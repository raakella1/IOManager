/************************************************************************
 * Copyright 2017-2019 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **************************************************************************/

// Implementation of the public socket_handle/net_op API (iomgr/net_op.hpp) over a dedicated per-reactor
// io_uring channel (interfaces/uring_net_interface.hpp). This is the ONLY place the public handle/awaiter
// meet io_uring/liburing details; nothing leaks into the public header.

#include <cerrno>
#include <chrono>
#include <cstring>
#include <system_error>
#include <utility>

extern "C" {
#include <fcntl.h>
#include <sys/socket.h>
}

#include <exec/async_scope.hpp>
#include <exec/task.hpp>
#include <sisl/logging/logging.h>
#include <iomgr/iomgr.hpp>
#include <iomgr/net_op.hpp>
#include "interfaces/uring_net_interface.hpp"
#include "reactor/reactor.hpp"

namespace iomgr {

namespace {

io_result try_recv(int fd, std::span< uint8_t > buf) {
    for (;;) {
        ssize_t const n = ::recv(fd, buf.data(), buf.size(), 0);
        if (n >= 0) { return io_result{static_cast< size_t >(n)}; }
        if (errno == EINTR) { continue; } // interrupted by a signal, not "no data" -- retry transparently
        return io_result{std::unexpected(std::error_code{errno, std::system_category()}.default_error_condition())};
    }
}

io_result try_send(int fd, std::span< uint8_t const > buf) {
    for (;;) {
        // MSG_NOSIGNAL: a peer that already RST/closed its read side must surface as an error result, not
        // SIGPIPE taking the process down.
        ssize_t const n = ::send(fd, buf.data(), buf.size(), MSG_NOSIGNAL);
        if (n >= 0) { return io_result{static_cast< size_t >(n)}; }
        if (errno == EINTR) { continue; }
        return io_result{std::unexpected(std::error_code{errno, std::system_category()}.default_error_condition())};
    }
}

accept_result try_accept(int fd) {
    for (;;) {
        int const r = ::accept(fd, nullptr, nullptr);
        if (r >= 0) { return accept_result{r}; }
        // ECONNABORTED: the peer reset the connection between its SYN completing and accept() picking it
        // up -- retry rather than fail the whole listener over one aborted connection.
        if (errno == EINTR || errno == ECONNABORTED) { continue; }
        return accept_result{std::unexpected(std::error_code{errno, std::system_category()}.default_error_condition())};
    }
}

template < typename T >
bool is_retryable(std::expected< T, std::error_condition > const& r) {
    return !r.has_value() &&
        (r.error() == std::errc::resource_unavailable_try_again || r.error() == std::errc::operation_would_block);
}

void set_nonblocking(int fd) {
    int const flags = ::fcntl(fd, F_GETFL, 0);
    RELEASE_ASSERT_GE(flags, 0, "fcntl(F_GETFL) failed on fd {}: {}", fd, strerror(errno));
    int const r = ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    RELEASE_ASSERT_EQ(r, 0, "fcntl(F_SETFL, O_NONBLOCK) failed on fd {}: {}", fd, strerror(errno));
}

// Runs fn() on `reactor` -- inline if already there, else dispatched via run_on_wait (blocking). Lets
// socket_handle's construction/destruction be called from any thread without making every recv/send pay
// for a dispatch too.
void run_on_reactor(IOReactor* reactor, const auto& fn) {
    if (iomanager.this_reactor() == reactor) {
        fn();
    } else {
        iomanager.run_on_wait(reactor, fn);
    }
}

io_result res_to_io_result(int res) {
    if (res < 0) {
        return io_result{std::unexpected(std::error_code{-res, std::system_category()}.default_error_condition())};
    }
    return io_result{static_cast< size_t >(res)};
}

// io_uring_scheduler::async_submit's sender completes via set_error(exception_ptr), not a negative `res`,
// when the ring is exhausted even after its internal flush-and-retry. exec::async_scope::spawn isn't
// built to handle a spawned sender that fails this way, so every submission coroutine below wraps its
// co_await in try/catch(std::exception const&) and converts through this instead of letting it propagate.
std::error_condition exception_to_error_condition(std::exception const& e) {
    if (auto const* se = dynamic_cast< std::system_error const* >(&e)) { return se->code().default_error_condition(); }
    return std::make_error_condition(std::errc::not_enough_memory);
}

// Outstanding accounting + tight-poll while a net op is in flight, mirroring UringDriveInterface's
// io_started()/io_finished(); see IOReactor::enter_tight_poll for why this goes through the reactor's
// shared refcount instead of a local save/restore.
void net_io_started() {
    auto* const ch = UringNetInterface::this_channel();
    if (ch->m_outstanding++ == 0) { iomanager.this_reactor()->enter_tight_poll(); }
}

void net_io_finished() {
    auto* const ch = UringNetInterface::this_channel();
    if (--ch->m_outstanding == 0) { iomanager.this_reactor()->exit_tight_poll(); }
}

// Constructed first (incremented first) / destroyed last (decremented last) relative to each coroutine's
// own cancel_guard, so m_outstanding only reaches zero once the op is fully resolved -- including the
// cancellation-path resolution a cancel_guard's own destructor may still need to run.
struct net_io_scope {
    net_io_scope() { net_io_started(); }
    net_io_scope(net_io_scope const&) = delete;
    ~net_io_scope() { net_io_finished(); }
};

} // namespace

// ----- socket_handle --------------------------------------------------------------------------------
struct socket_handle::impl {
    int fd{-1};
    IOReactor* reactor{nullptr};
    // Scoped per-socket, not per-op: teardown cancels everything still in flight on this socket (recv AND
    // send together) by stopping this one scope, rather than tracking per-op stop sources by hand.
    exec::async_scope scope;
    net_op::impl* parked_recv{nullptr};
    net_op::impl* parked_send{nullptr};
};

// ----- net_op: opaque awaitable wrapping a single recv or send ----------------------------------------
struct net_op::impl {
    socket_handle::impl* owner{nullptr};
    bool is_send{false};
    std::span< uint8_t > recv_buf;       // valid when !is_send
    std::span< uint8_t const > send_buf; // valid when is_send
    io_result result{0};
    bool done{false};
    std::coroutine_handle<> waiter{};

    io_result try_once() const {
        return is_send ? try_send(owner->fd, send_buf) : try_recv(owner->fd, recv_buf);
    }

    ~impl() {
        RELEASE_ASSERT(owner->parked_recv != this && owner->parked_send != this,
                       "net_op destroyed while still parked waiting on its socket_handle -- would leave a "
                       "dangling pointer the next completion would touch");
    }
};

// ----- listener_handle -------------------------------------------------------------------------------
struct listener_handle::impl {
    int fd{-1};
    IOReactor* reactor{nullptr};
    exec::async_scope scope;
    accept_op::impl* parked_accept{nullptr};
};

// ----- accept_op: opaque awaitable wrapping a single accept -------------------------------------------
struct accept_op::impl {
    listener_handle::impl* owner{nullptr};
    accept_result result{-1};
    bool done{false};
    std::coroutine_handle<> waiter{};

    accept_result try_once() const { return try_accept(owner->fd); }

    ~impl() {
        RELEASE_ASSERT(owner->parked_accept != this,
                       "accept_op destroyed while still parked waiting on its listener_handle -- would leave "
                       "a dangling pointer the next completion would touch");
    }
};

// ----- connect_op: opaque awaitable wrapping a single outbound connect ---------------------------------
struct connect_op::impl {
    using connect_result = std::expected< void, std::error_condition >;

    int fd{-1};
    sockaddr_storage addr{};
    socklen_t addrlen{0};
    std::optional< std::chrono::milliseconds > timeout;

    connect_result result{};
    bool done{false};
    std::coroutine_handle<> waiter{};
};

namespace {

// Clears the parked slot and resumes its waiter with `r`. Shared by net_op::impl (io_result) and
// accept_op::impl (accept_result) -- both have the same owner/result/done/waiter shape.
template < typename Impl, typename Result >
void complete_parked(Impl*& slot, Result r) {
    auto* const op = std::exchange(slot, nullptr);
    op->result = std::move(r);
    op->done = true;
    if (auto w = op->waiter) { w.resume(); }
}

// Submits the one-shot recv/send SQE for `self` and, once reaped, completes it, unless teardown already
// claimed it via cancellation. Spawned into the owning socket_handle's async_scope, so a stop request
// cancels only this socket's ops. If the stop token fires mid-flight, co_await on the submit sender
// completes via the stopped channel instead of resuming past it, skipping the completion below -- the
// cancel_guard's destructor covers that unwind and resolves the op as cancelled if nothing else did.
exec::task< void > run_uring_op(net_op::impl* self) {
    net_io_scope io_scope;
    struct cancel_guard {
        net_op::impl* self;
        bool completed{false};
        ~cancel_guard() {
            if (completed) { return; }
            net_op::impl*& slot = self->is_send ? self->owner->parked_send : self->owner->parked_recv;
            if (slot == self) {
                complete_parked(slot, io_result{std::unexpected(std::make_error_condition(std::errc::connection_aborted))});
            }
        }
    } guard{self};

    auto& sched = UringNetInterface::this_channel()->m_sched;
    int res = 0;
    for (;;) {
        try {
            res = co_await sched.async_submit([self](::io_uring_sqe* sqe) {
                if (self->is_send) {
                    ::io_uring_prep_send(sqe, self->owner->fd, self->send_buf.data(), self->send_buf.size(),
                                         MSG_NOSIGNAL);
                } else {
                    ::io_uring_prep_recv(sqe, self->owner->fd, self->recv_buf.data(), self->recv_buf.size(), 0);
                }
            });
        } catch (std::exception const& e) {
            net_op::impl*& slot = self->is_send ? self->owner->parked_send : self->owner->parked_recv;
            if (slot == self) { complete_parked(slot, io_result{std::unexpected(exception_to_error_condition(e))}); }
            guard.completed = true;
            co_return;
        }
        if (res != -EINTR) { break; } // interrupted by a signal, not "no data" -- retry transparently
    }

    net_op::impl*& slot = self->is_send ? self->owner->parked_send : self->owner->parked_recv;
    if (slot == self) { complete_parked(slot, res_to_io_result(res)); }
    guard.completed = true;
}

// Cancels and waits for anything still parked on `s`, then closes the fd. Runs on the owning reactor
// thread, so waiting is a bounded manual poll_once loop rather than a blocking await -- no other thread
// can drive this reactor's net channel to reap the cancellation. Unlike disk I/O, a parked accept/recv can
// hang forever if the peer never sends/connects, so this actively cancels instead of passively waiting for
// "nothing outstanding" the way drive's drain does.
void teardown(socket_handle::impl& s) {
    if (s.parked_recv || s.parked_send) {
        s.scope.request_stop();
        auto* const ch = UringNetInterface::this_channel();
        auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
        while (s.parked_recv || s.parked_send) {
            ch->m_sched.poll_once(std::chrono::milliseconds{1});
            if (std::chrono::steady_clock::now() > deadline) {
                LOGERRORMOD(iomgr, "Timed out cancelling a parked net op during socket teardown, fd={}", s.fd);
                break;
            }
        }
    }
    ::close(s.fd);
}

// Mirrors run_uring_op's shape for a single one-shot IORING_OP_ACCEPT.
exec::task< void > run_uring_accept(accept_op::impl* self) {
    net_io_scope io_scope;
    struct cancel_guard {
        accept_op::impl* self;
        bool completed{false};
        ~cancel_guard() {
            if (completed) { return; }
            accept_op::impl*& slot = self->owner->parked_accept;
            if (slot == self) {
                complete_parked(slot,
                                accept_result{std::unexpected(std::make_error_condition(std::errc::connection_aborted))});
            }
        }
    } guard{self};

    auto& sched = UringNetInterface::this_channel()->m_sched;
    int res = 0;
    for (;;) {
        try {
            res = co_await sched.async_submit(
                [self](::io_uring_sqe* sqe) { ::io_uring_prep_accept(sqe, self->owner->fd, nullptr, nullptr, 0); });
        } catch (std::exception const& e) {
            accept_op::impl*& slot = self->owner->parked_accept;
            if (slot == self) {
                complete_parked(slot, accept_result{std::unexpected(exception_to_error_condition(e))});
            }
            guard.completed = true;
            co_return;
        }
        if (res != -EINTR && res != -ECONNABORTED) { break; } // see try_accept for why ECONNABORTED retries
    }

    accept_op::impl*& slot = self->owner->parked_accept;
    if (slot == self) {
        if (res < 0) {
            complete_parked(slot, accept_result{std::unexpected(
                                      std::error_code{-res, std::system_category()}.default_error_condition())});
        } else {
            complete_parked(slot, accept_result{res});
        }
    }
    guard.completed = true;
}

// Mirrors teardown(socket_handle::impl&) for the single parked-accept case.
void teardown(listener_handle::impl& s) {
    if (s.parked_accept) {
        s.scope.request_stop();
        auto* const ch = UringNetInterface::this_channel();
        auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
        while (s.parked_accept) {
            ch->m_sched.poll_once(std::chrono::milliseconds{1});
            if (std::chrono::steady_clock::now() > deadline) {
                LOGERRORMOD(iomgr, "Timed out cancelling a parked accept during listener teardown, fd={}", s.fd);
                break;
            }
        }
    }
    ::close(s.fd);
}

// One-shot IORING_OP_CONNECT, optionally linked (IOSQE_IO_LINK) to an IORING_OP_LINK_TIMEOUT so the
// kernel cancels the connect if it fires first, rather than racing a second sender against `self` after
// its connect_op may already be gone. The timeout SQE's user_data is left 0 ("unmanaged"), same convention
// as elsewhere, so poll_once's reap loop ignores its CQE.
//
// addr/ts are coroutine-frame locals, not references to caller-owned storage: the kernel reads them at
// actual submission time (deferred to the next poll_once), and the frame stays alive across the suspend.
exec::task< void > run_uring_connect(connect_op::impl* self, int fd, sockaddr_storage addr, socklen_t addrlen,
                                     std::optional< std::chrono::milliseconds > timeout) {
    net_io_scope io_scope;
    // The kernel-side linked timeout above is the only source of cancellation for a connect_op in this
    // design (nothing ever calls self->scope.request_stop()) -- so if co_await below completes via the
    // stopped channel instead of resuming past it, it can only be that timeout firing first.
    struct cancel_guard {
        connect_op::impl* self;
        bool completed{false};
        ~cancel_guard() {
            if (completed) { return; }
            self->result = std::unexpected(std::make_error_condition(std::errc::timed_out));
            self->done = true;
            if (auto w = self->waiter) { w.resume(); }
        }
    } guard{self};

    auto* const ch = UringNetInterface::this_channel();
    __kernel_timespec ts{};
    if (timeout) {
        auto const ns = std::chrono::duration_cast< std::chrono::nanoseconds >(*timeout).count();
        ts.tv_sec = static_cast< __kernel_time64_t >(ns / 1'000'000'000);
        ts.tv_nsec = static_cast< long long >(ns % 1'000'000'000);
    }
    int res = 0;
    try {
        res = co_await ch->m_sched.async_submit([&](::io_uring_sqe* sqe) {
            ::io_uring_prep_connect(sqe, fd, reinterpret_cast< sockaddr const* >(&addr), addrlen);
            if (timeout) {
                if (auto* const timeout_sqe = ::io_uring_get_sqe(ch->m_sched.ring())) {
                    sqe->flags |= IOSQE_IO_LINK;
                    ::io_uring_prep_link_timeout(timeout_sqe, &ts, 0);
                    ::io_uring_sqe_set_data64(timeout_sqe, 0);
                } else {
                    LOGWARNMOD(iomgr, "net uring ring full: connect timeout not armed for this call");
                }
            }
        });
    } catch (std::exception const& e) {
        // Not retried: a second connect() on an already-mid-handshake fd risks EALREADY; see
        // connect_op::await_ready below for why there's no raw-syscall fast path either.
        if (!self->done) {
            self->result = std::unexpected(exception_to_error_condition(e));
            self->done = true;
            if (auto w = self->waiter) { w.resume(); }
        }
        guard.completed = true;
        co_return;
    }

    if (!self->done) {
        self->result = (res < 0)
            ? std::unexpected(std::error_code{-res, std::system_category()}.default_error_condition())
            : connect_op::impl::connect_result{};
        self->done = true;
        if (auto w = self->waiter) { w.resume(); }
    }
    guard.completed = true;
}

} // namespace

socket_handle::socket_handle(int fd, thread_specifier const& reactor) : _impl{std::make_unique< impl >()} {
    RELEASE_ASSERT(std::holds_alternative< IOReactor* >(reactor),
                   "socket_handle must be pinned to a specific reactor, not a reactor_regex");
    auto* const target = std::get< IOReactor* >(reactor);
    set_nonblocking(fd);
    impl* const p = _impl.get();
    run_on_reactor(target, [fd, target, p]() {
        p->fd = fd;
        p->reactor = target;
    });
}

// Must run before the owning reactor stops; see net_op.hpp's class comment.
socket_handle::~socket_handle() {
    if (!_impl) { return; } // moved-from
    impl* const p = _impl.get();
    run_on_reactor(p->reactor, [p]() { teardown(*p); });
}

socket_handle::socket_handle(socket_handle&&) noexcept = default;

socket_handle& socket_handle::operator=(socket_handle&& o) noexcept {
    if (this != &o) {
        if (_impl) {
            impl* const p = _impl.get();
            run_on_reactor(p->reactor, [p]() { teardown(*p); });
        }
        _impl = std::move(o._impl);
    }
    return *this;
}

int socket_handle::fd() const noexcept { return _impl->fd; }

listener_handle::listener_handle(int listen_fd, thread_specifier const& reactor) : _impl{std::make_unique< impl >()} {
    RELEASE_ASSERT(std::holds_alternative< IOReactor* >(reactor),
                   "listener_handle must be pinned to a specific reactor, not a reactor_regex");
    auto* const target = std::get< IOReactor* >(reactor);
    set_nonblocking(listen_fd);
    impl* const p = _impl.get();
    run_on_reactor(target, [listen_fd, target, p]() {
        p->fd = listen_fd;
        p->reactor = target;
    });
}

// Same "must run before the owning reactor stops" requirement as socket_handle::~socket_handle(); see
// that destructor's comment.
listener_handle::~listener_handle() {
    if (!_impl) { return; } // moved-from
    impl* const p = _impl.get();
    run_on_reactor(p->reactor, [p]() { teardown(*p); });
}

listener_handle::listener_handle(listener_handle&&) noexcept = default;

listener_handle& listener_handle::operator=(listener_handle&& o) noexcept {
    if (this != &o) {
        if (_impl) {
            impl* const p = _impl.get();
            run_on_reactor(p->reactor, [p]() { teardown(*p); });
        }
        _impl = std::move(o._impl);
    }
    return *this;
}

int listener_handle::fd() const noexcept { return _impl->fd; }

// ----- accept_op -----------------------------------------------------------------------------------
accept_op::accept_op(std::unique_ptr< impl > p) noexcept : _impl{std::move(p)} {}
accept_op::accept_op(accept_op&&) noexcept = default;
accept_op& accept_op::operator=(accept_op&&) noexcept = default;
accept_op::~accept_op() = default;

bool accept_op::await_ready() const noexcept {
    impl* const p = _impl.get();
    RELEASE_ASSERT(iomanager.this_reactor() == p->owner->reactor,
                   "accept_op driven from a thread other than its listener_handle's owning reactor");
    auto r = p->try_once();
    if (is_retryable(r)) { return false; }
    p->result = std::move(r);
    p->done = true;
    return true;
}

void accept_op::await_suspend(std::coroutine_handle<> h) noexcept {
    impl* const p = _impl.get();
    RELEASE_ASSERT(p->owner->parked_accept == nullptr,
                   "a second accept is already parked on this listener_handle -- callers must serialize "
                   "their own accept loop");
    p->waiter = h;
    p->owner->parked_accept = p;
    p->owner->scope.spawn(run_uring_accept(p));
}

accept_result accept_op::await_resume() const noexcept { return _impl->result; }

accept_op async_accept(listener_handle& listener) {
    auto p = std::make_unique< accept_op::impl >();
    p->owner = listener._impl.get();
    return accept_op{std::move(p)};
}

// ----- connect_op -----------------------------------------------------------------------------------
connect_op::connect_op(std::unique_ptr< impl > p) noexcept : _impl{std::move(p)} {}
connect_op::connect_op(connect_op&&) noexcept = default;
connect_op& connect_op::operator=(connect_op&&) noexcept = default;
connect_op::~connect_op() = default;

// Unlike recv/send/accept, connect has no safe fast path: a raw non-blocking ::connect() would itself
// start the handshake (EINPROGRESS), and a second connect() on an already-connecting fd -- including
// IORING_OP_CONNECT's own internal one -- is rejected with EALREADY. So IORING_OP_CONNECT must be the
// only connect() call ever made on this fd; go straight to uring submission every time.
bool connect_op::await_ready() const noexcept { return false; }

void connect_op::await_suspend(std::coroutine_handle<> h) noexcept {
    impl* const p = _impl.get();
    p->waiter = h;
    UringNetInterface::this_channel()->m_scope.spawn(run_uring_connect(p, p->fd, p->addr, p->addrlen, p->timeout));
}

std::expected< void, std::error_condition > connect_op::await_resume() const noexcept { return _impl->result; }

connect_op async_connect(int fd, sockaddr const* addr, socklen_t addrlen, thread_specifier const& reactor,
                         std::optional< std::chrono::milliseconds > timeout) {
    RELEASE_ASSERT(std::holds_alternative< IOReactor* >(reactor),
                   "async_connect must be called with a specific reactor, not a reactor_regex");
    RELEASE_ASSERT(iomanager.this_reactor() == std::get< IOReactor* >(reactor),
                   "async_connect driven from a thread other than the reactor it was called with");
    RELEASE_ASSERT(static_cast< size_t >(addrlen) <= sizeof(sockaddr_storage),
                   "addrlen {} exceeds sizeof(sockaddr_storage)", addrlen);
    set_nonblocking(fd);
    auto p = std::make_unique< connect_op::impl >();
    p->fd = fd;
    std::memcpy(&p->addr, addr, addrlen);
    p->addrlen = addrlen;
    p->timeout = timeout;
    return connect_op{std::move(p)};
}

// ----- net_op -------------------------------------------------------------------------------------
net_op::net_op(std::unique_ptr< impl > p) noexcept : _impl{std::move(p)} {}
net_op::net_op(net_op&&) noexcept = default;
net_op& net_op::operator=(net_op&&) noexcept = default;
net_op::~net_op() = default;

bool net_op::await_ready() const noexcept {
    impl* const p = _impl.get();
    RELEASE_ASSERT(iomanager.this_reactor() == p->owner->reactor,
                   "net_op driven from a thread other than its socket_handle's owning reactor");
    auto r = p->try_once();
    if (is_retryable(r)) { return false; } // park in await_suspend
    p->result = std::move(r);
    p->done = true;
    return true;
}

void net_op::await_suspend(std::coroutine_handle<> h) noexcept {
    impl* const p = _impl.get();
    net_op::impl*& slot = p->is_send ? p->owner->parked_send : p->owner->parked_recv;
    RELEASE_ASSERT(slot == nullptr,
                   "a second {} is already parked on this socket_handle -- callers must serialize their own "
                   "{} loop",
                   p->is_send ? "send" : "recv", p->is_send ? "send" : "recv");
    p->waiter = h;
    slot = p;
    p->owner->scope.spawn(run_uring_op(p));
}

io_result net_op::await_resume() const noexcept { return _impl->result; }

net_op async_recv(socket_handle& sock, std::span< uint8_t > buf) {
    RELEASE_ASSERT(!buf.empty(),
                   "async_recv called with an empty buffer -- a zero-byte recv() return is indistinguishable "
                   "from EOF");
    auto p = std::make_unique< net_op::impl >();
    p->owner = sock._impl.get();
    p->recv_buf = buf;
    return net_op{std::move(p)};
}

net_op async_send(socket_handle& sock, std::span< uint8_t const > buf) {
    RELEASE_ASSERT(!buf.empty(), "async_send called with an empty buffer");
    auto p = std::make_unique< net_op::impl >();
    p->owner = sock._impl.get();
    p->is_send = true;
    p->send_buf = buf;
    return net_op{std::move(p)};
}

// ----- fire-and-forget launcher for non-coroutine callers --------------------------------------------
namespace {
struct ff_task {
    struct promise_type {
        ff_task get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() { std::terminate(); }
    };
};
// op and on_done are coroutine PARAMETERS (copied into the self-owning frame), so nothing dangles.
ff_task run_detached(net_op op, std::function< void(io_result) > on_done) { on_done(co_await std::move(op)); }
} // namespace

void detach(net_op op, std::function< void(io_result) > on_done) { run_detached(std::move(op), std::move(on_done)); }

// ----- full-buffer composition helpers --------------------------------------------------------------
sisl::async::light_task< io_result > recv_exact(socket_handle& sock, std::span< uint8_t > buf) {
    size_t total{0};
    while (total < buf.size()) {
        auto r = co_await async_recv(sock, buf.subspan(total));
        if (!r) { co_return std::move(r); }
        if (*r == 0) { co_return io_result{std::unexpected(std::make_error_condition(std::errc::connection_aborted))}; }
        total += *r;
    }
    co_return io_result{total};
}

sisl::async::light_task< io_result > send_all(socket_handle& sock, std::span< uint8_t const > buf) {
    size_t total{0};
    while (total < buf.size()) {
        auto r = co_await async_send(sock, buf.subspan(total));
        if (!r) { co_return std::move(r); }
        if (*r == 0) { co_return io_result{std::unexpected(std::make_error_condition(std::errc::connection_aborted))}; }
        total += *r;
    }
    co_return io_result{total};
}

} // namespace iomgr
