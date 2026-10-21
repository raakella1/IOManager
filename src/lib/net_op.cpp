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

bool is_retryable(io_result const& r) {
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

namespace {

// Clears the parked slot and resumes its waiter with `r`.
void complete_parked(net_op::impl*& slot, io_result r) {
    auto* const op = std::exchange(slot, nullptr);
    op->result = std::move(r);
    op->done = true;
    if (auto w = op->waiter) { w.resume(); }
}

// Submits the one-shot recv/send SQE for `self` and, once reaped, completes it -- unless teardown already
// claimed it via cancellation. Spawned into the owning socket_handle's async_scope, which is what gives
// teardown a stop token to cancel this specific op (and only this socket's ops) with.
//
// If the scope's stop token fires mid-flight, co_await-ing the uring submit sender completes this
// coroutine via the stopped channel rather than resuming past the co_await -- so the completion below
// never runs. The RAII guard below covers that path: its destructor runs during that unwind the same way
// it would during a normal stack unwind, and resolves the parked op as cancelled if nothing else already
// did.
exec::task< void > run_uring_op(net_op::impl* self) {
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
    int const res = co_await sched.async_submit([self](::io_uring_sqe* sqe) {
        if (self->is_send) {
            ::io_uring_prep_send(sqe, self->owner->fd, self->send_buf.data(), self->send_buf.size(), MSG_NOSIGNAL);
        } else {
            ::io_uring_prep_recv(sqe, self->owner->fd, self->recv_buf.data(), self->recv_buf.size(), 0);
        }
    });

    net_op::impl*& slot = self->is_send ? self->owner->parked_send : self->owner->parked_recv;
    if (slot == self) { complete_parked(slot, res_to_io_result(res)); }
    guard.completed = true;
}

// Cancels and waits for anything still parked on `s`, then closes the fd. Waiting is a bounded manual
// poll_once loop (same shape as UringDriveInterface's drain_outstanding_ios), not a blocking sender await:
// this runs synchronously on the owning reactor thread, and reaping the cancellation's completion is only
// possible by driving that same thread's net channel -- there's no other thread that could do it instead.
// Unlike disk I/O, a parked accept/recv can hang forever if the peer never sends/connects, so this must
// actively cancel rather than passively wait for "nothing outstanding" the way drive's drain does.
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

// Must run before the owning reactor stops: run_on_reactor's dispatch has nothing to deliver the message
// to once that reactor's thread has exited, and would block forever.
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
