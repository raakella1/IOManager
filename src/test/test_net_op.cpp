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

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

extern "C" {
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
}

#include <sisl/async/light_task.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <gtest/gtest.h>

#include <iomgr/io_environment.hpp>
#include <iomgr/iomgr.hpp>
#include <iomgr/net_op.hpp>

using namespace iomgr;

SISL_LOGGING_INIT(IOMGR_LOG_MODS, flip)
SISL_OPTIONS_ENABLE(logging, iomgr, config)

namespace {

std::pair< int, int > make_connected_pair() {
    int fds[2];
    RELEASE_ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0, "socketpair() failed: {}", strerror(errno));
    return {fds[0], fds[1]};
}

// A bound, listening AF_UNIX socket -- distinct from make_connected_pair(): listen() requires a socket
// that was bound and never connected, which a socketpair() endpoint already is not.
std::pair< int, std::string > make_listening_socket(int backlog = 1) {
    int const fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    RELEASE_ASSERT_GE(fd, 0, "socket() failed: {}", strerror(errno));
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::string const path = "/tmp/iomgr_net_op_test_" + std::to_string(::getpid()) + "_" + std::to_string(fd);
    RELEASE_ASSERT_LT(path.size(), sizeof(addr.sun_path), "unix socket path too long: {}", path);
    ::unlink(path.c_str());
    std::copy(path.begin(), path.end(), addr.sun_path);
    RELEASE_ASSERT_EQ(::bind(fd, reinterpret_cast< sockaddr* >(&addr), sizeof(addr)), 0, "bind() failed: {}",
                      strerror(errno));
    RELEASE_ASSERT_EQ(::listen(fd, backlog), 0, "listen() failed: {}", strerror(errno));
    return {fd, path};
}

sockaddr_un make_addr(std::string const& path) {
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::copy(path.begin(), path.end(), addr.sun_path);
    return addr;
}

IOReactor* pick_a_worker_reactor() {
    IOReactor* reactor{nullptr};
    iomanager.run_on_wait(reactor_regex::random_worker, [&reactor]() { reactor = iomanager.this_reactor(); });
    return reactor;
}

std::vector< IOReactor* > two_distinct_worker_reactors() {
    std::mutex m;
    std::vector< IOReactor* > reactors;
    iomanager.run_on_wait(reactor_regex::all_worker, [&]() {
        std::lock_guard< std::mutex > lg(m);
        reactors.push_back(iomanager.this_reactor());
    });
    return reactors;
}

// Dispatches fn() (an awaitable-returning callable) onto `reactor` via run_on_forget -- so a genuine
// suspension inside it doesn't block that reactor from later resuming it -- and returns a future for the
// result. The caller decides whether to poll it with wait_for or block on get().
template < typename T, typename Fn >
std::future< T > start(IOReactor* reactor, Fn&& fn) {
    auto done = std::make_shared< std::promise< T > >();
    auto fut = done->get_future();
    iomanager.run_on_forget(reactor, [fn = std::forward< Fn >(fn), done]() mutable {
        [](auto fn, std::shared_ptr< std::promise< T > > done) -> sisl::async::light_task< void > {
            done->set_value(co_await fn());
        }(std::move(fn), std::move(done))
                                                                      .detach();
    });
    return fut;
}

} // namespace

class NetOpTest : public ::testing::Test {
public:
    void SetUp() override { ioenvironment.with_iomgr(iomgr_params{.num_threads = 2}); }
    void TearDown() override { iomanager.stop(); }
};

// Confirms iomgr_params::net_uring_qdepth is actually wired through to the net channel's ring (not just
// that the field exists), and that an explicit, intentionally-small-but-reasonable override doesn't break
// ordinary operation. This does NOT attempt to exercise genuine ring exhaustion (an unflushable "still
// full after acquire_sqe's own flush-and-retry" condition): acquire_sqe's retry runs per-op, sequentially,
// so an ordinary burst of fast local socket I/O self-heals via that retry before ever reaching the
// exhaustion path -- forcing the real thing would need the completion side backed up too, not just a
// small SQ depth. That fix (net_op.cpp's try/catch around each uring submission) remains verified by
// reading the code, not by a passing test here.
TEST(NetOpCustomQdepthTest, ExplicitOverrideStillWorks) {
    ioenvironment.with_iomgr(iomgr_params{.num_threads = 2, .net_uring_qdepth = 8});
    auto const [fd0, fd1] = make_connected_pair();
    std::array< uint8_t, 5 > const payload{'h', 'e', 'l', 'l', 'o'};
    std::array< uint8_t, 5 > read_buf{};
    io_result send_result{0};
    io_result recv_result{0};

    iomanager.run_on_wait(reactor_regex::random_worker, [&]() {
        socket_handle a(fd0, thread_specifier{iomanager.this_reactor()});
        socket_handle b(fd1, thread_specifier{iomanager.this_reactor()});
        send_result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t const > buf) -> sisl::async::light_task< io_result > {
                co_return co_await async_send(s, buf);
            }(a, std::span< uint8_t const >(payload)));
        recv_result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t > buf) -> sisl::async::light_task< io_result > {
                co_return co_await async_recv(s, buf);
            }(b, std::span< uint8_t >(read_buf)));
    });

    ASSERT_TRUE(send_result.has_value());
    ASSERT_TRUE(recv_result.has_value());
    EXPECT_EQ(read_buf, payload);
    iomanager.stop();
}

// A deliberately adversarial stress case for the queue-depth knob itself: 100 sockets concurrently
// submitting into a ring configured for a depth of only 2. Confirmed empirically (not just by reading
// io_uring_scheduler's acquire_sqe) that this does NOT reproduce genuine ring exhaustion: acquire_sqe's
// own flush-and-retry runs per op, sequentially, so each new submission's retry already makes room before
// the next one is even requested -- an ordinary burst self-heals this way regardless of how small the
// configured depth is. Kept as a permanent regression check that an extreme depth/concurrency mismatch
// at least never regresses into a crash or a hang, even though it can't exercise the true-exhaustion
// error path (that remains verified by code reading only; see net_op.cpp's exception_to_error_condition).
TEST(NetOpRingPressureTest, ManyConcurrentOpsOnATinyRingAllResolve) {
    ioenvironment.with_iomgr(iomgr_params{.num_threads = 1, .net_uring_qdepth = 2});
    auto* const reactor = pick_a_worker_reactor();

    constexpr int kNumSockets = 100;
    std::vector< int > peer_fds(kNumSockets);
    std::vector< std::optional< socket_handle > > socks(kNumSockets);
    std::vector< std::vector< uint8_t > > read_bufs(kNumSockets, std::vector< uint8_t >(16));

    for (int i = 0; i < kNumSockets; ++i) {
        auto const [fd0, fd1] = make_connected_pair();
        peer_fds[i] = fd1;
        socks[i].emplace(fd0, thread_specifier{reactor});
    }

    std::vector< std::future< io_result > > futs;
    futs.reserve(kNumSockets);
    for (int i = 0; i < kNumSockets; ++i) {
        futs.push_back(start< io_result >(
            reactor, [&, i]() { return async_recv(*socks[i], std::span< uint8_t >(read_bufs[i])); }));
    }
    for (int i = 0; i < kNumSockets; ++i) {
        uint8_t const b = static_cast< uint8_t >(i);
        ASSERT_EQ(::write(peer_fds[i], &b, 1), 1);
    }

    for (int i = 0; i < kNumSockets; ++i) {
        auto const r = futs[i].get();
        ASSERT_TRUE(r.has_value()) << "socket " << i << ": " << r.error().message();
    }

    // socket_handle destruction must happen before the reactor stops (see socket_handle's class
    // comment) -- explicit here rather than relying on scope exit, since that would run after the
    // iomanager.stop() below.
    socks.clear();
    for (int fd : peer_fds) {
        ::close(fd);
    }
    iomanager.stop();
}

TEST_F(NetOpTest, RecvSendRoundTrip) {
    auto const [fd0, fd1] = make_connected_pair();
    std::array< uint8_t, 5 > const payload{'h', 'e', 'l', 'l', 'o'};
    std::array< uint8_t, 5 > read_buf{};
    io_result send_result{0};
    io_result recv_result{0};

    iomanager.run_on_wait(reactor_regex::random_worker, [&]() {
        socket_handle a(fd0, thread_specifier{iomanager.this_reactor()});
        socket_handle b(fd1, thread_specifier{iomanager.this_reactor()});
        send_result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t const > buf) -> sisl::async::light_task< io_result > {
                co_return co_await async_send(s, buf);
            }(a, std::span< uint8_t const >(payload)));
        recv_result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t > buf) -> sisl::async::light_task< io_result > {
                co_return co_await async_recv(s, buf);
            }(b, std::span< uint8_t >(read_buf)));
    });

    ASSERT_TRUE(send_result.has_value());
    EXPECT_EQ(*send_result, payload.size());
    ASSERT_TRUE(recv_result.has_value());
    EXPECT_EQ(*recv_result, payload.size());
    EXPECT_EQ(read_buf, payload);
}

TEST_F(NetOpTest, RecvZeroIsEof) {
    auto const [fd0, fd1] = make_connected_pair();
    ::close(fd1); // peer gone before the recv is even attempted
    std::array< uint8_t, 5 > read_buf{};
    io_result result{0};

    iomanager.run_on_wait(reactor_regex::random_worker, [&]() {
        socket_handle sock(fd0, thread_specifier{iomanager.this_reactor()});
        result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t > buf) -> sisl::async::light_task< io_result > {
                co_return co_await async_recv(s, buf);
            }(sock, std::span< uint8_t >(read_buf)));
    });

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 0u);
}

// Complements RecvZeroIsEof: that one covers the read-side EOF signal (a clean 0-byte recv). This covers
// the write side failing outright once the peer is gone -- MSG_NOSIGNAL turns what would otherwise be a
// process-killing SIGPIPE into an ordinary error result, same as the raw-syscall fast path.
TEST_F(NetOpTest, SendFailsWhenPeerClosed) {
    auto const [fd0, fd1] = make_connected_pair();
    ::close(fd1); // peer gone before the send is even attempted
    std::array< uint8_t, 5 > const payload{'h', 'e', 'l', 'l', 'o'};
    io_result result{0};

    iomanager.run_on_wait(reactor_regex::random_worker, [&]() {
        socket_handle sock(fd0, thread_specifier{iomanager.this_reactor()});
        result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t const > buf) -> sisl::async::light_task< io_result > {
                co_return co_await async_send(s, buf);
            }(sock, std::span< uint8_t const >(payload)));
    });

    ASSERT_FALSE(result.has_value());
}

TEST_F(NetOpTest, RecvSuspendsUntilDataArrives) {
    auto const [fd0, fd1] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();
    socket_handle sock(fd0, thread_specifier{reactor});
    std::array< uint8_t, 5 > read_buf{};

    auto fut = start< io_result >(reactor, [&]() { return async_recv(sock, std::span< uint8_t >(read_buf)); });
    ASSERT_EQ(fut.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    std::array< uint8_t, 5 > const payload{'h', 'e', 'l', 'l', 'o'};
    ASSERT_EQ(::write(fd1, payload.data(), payload.size()), static_cast< ssize_t >(payload.size()));

    auto const result = fut.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, payload.size());
    EXPECT_EQ(read_buf, payload);
    ::close(fd1);
}

TEST_F(NetOpTest, SendSuspendsUntilPeerDrains) {
    auto const [fd0, fd1] = make_connected_pair();
    // Shrink both ends of the pipe's effective capacity so a send_all of a comfortably larger payload is
    // guaranteed to exhaust it and genuinely park, regardless of this host's default socket buffer sizes.
    int const small_buf_size = 4096;
    ::setsockopt(fd0, SOL_SOCKET, SO_SNDBUF, &small_buf_size, sizeof(small_buf_size));
    ::setsockopt(fd1, SOL_SOCKET, SO_RCVBUF, &small_buf_size, sizeof(small_buf_size));

    auto* const reactor = pick_a_worker_reactor();
    socket_handle sock(fd0, thread_specifier{reactor});

    std::vector< uint8_t > payload(1024 * 1024);
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast< uint8_t >(i);
    }

    auto fut = start< io_result >(reactor, [&]() { return send_all(sock, std::span< uint8_t const >(payload)); });
    ASSERT_EQ(fut.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);

    std::vector< uint8_t > received(payload.size());
    size_t total_read{0};
    while (total_read < received.size()) {
        ssize_t const n = ::read(fd1, received.data() + total_read, received.size() - total_read);
        ASSERT_GT(n, 0);
        total_read += static_cast< size_t >(n);
    }

    auto const result = fut.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, payload.size());
    EXPECT_EQ(received, payload);
    ::close(fd1);
}

TEST_F(NetOpTest, SocketTeardownFailsParkedRecv) {
    auto const [fd0, fd1] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();
    std::optional< socket_handle > sock;
    sock.emplace(fd0, thread_specifier{reactor});

    io_result result{0};
    bool completed{false};
    std::array< uint8_t, 5 > read_buf{};

    iomanager.run_on_wait(reactor, [&]() {
        [](socket_handle& s, std::span< uint8_t > buf, io_result& out, bool& done) -> sisl::async::light_task< void > {
            out = co_await async_recv(s, buf);
            done = true;
        }(*sock, std::span< uint8_t >(read_buf), result, completed)
                                                                                          .detach();
    });
    ASSERT_FALSE(completed); // nothing written yet; the recv is parked

    sock.reset(); // destroyed from this (non-owning) thread -- exercises the forced-close path

    ASSERT_TRUE(completed);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_condition(std::errc::connection_aborted));
    ::close(fd1);
}

TEST_F(NetOpTest, MoveAssignTearsDownPreviousConnection) {
    auto const [fd0, fd1] = make_connected_pair();
    auto const [fd2, fd3] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();

    socket_handle old_sock(fd0, thread_specifier{reactor});
    socket_handle new_sock(fd2, thread_specifier{reactor});

    io_result old_result{0};
    bool old_completed{false};
    std::array< uint8_t, 5 > old_read_buf{};

    iomanager.run_on_wait(reactor, [&]() {
        [](socket_handle& s, std::span< uint8_t > buf, io_result& out, bool& done) -> sisl::async::light_task< void > {
            out = co_await async_recv(s, buf);
            done = true;
        }(old_sock, std::span< uint8_t >(old_read_buf), old_result, old_completed)
                                                                                          .detach();
    });
    ASSERT_FALSE(old_completed); // nothing written to fd0's peer yet; the recv is parked

    // Move-assigning over old_sock while its recv is still parked must tear down its previous connection
    // (fail the parked waiter, close fd0) before taking ownership of new_sock's.
    old_sock = std::move(new_sock);

    ASSERT_TRUE(old_completed);
    ASSERT_FALSE(old_result.has_value());
    EXPECT_EQ(old_result.error(), std::make_error_condition(std::errc::connection_aborted));

    // old_sock now owns what was new_sock's connection and should work normally.
    std::array< uint8_t, 4 > const payload{'p', 'i', 'n', 'g'};
    ASSERT_EQ(::write(fd3, payload.data(), payload.size()), static_cast< ssize_t >(payload.size()));

    std::array< uint8_t, 4 > read_buf{};
    io_result recv_result{0};
    iomanager.run_on_wait(reactor, [&]() {
        recv_result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t > buf) -> sisl::async::light_task< io_result > {
                co_return co_await recv_exact(s, buf);
            }(old_sock, std::span< uint8_t >(read_buf)));
    });

    ASSERT_TRUE(recv_result.has_value());
    EXPECT_EQ(read_buf, payload);
    ::close(fd1);
    ::close(fd3);
}

TEST_F(NetOpTest, ListenerTeardownFailsParkedAccept) {
    auto const [fd0, path] = make_listening_socket();
    auto* const reactor = pick_a_worker_reactor();
    std::optional< listener_handle > listener;
    listener.emplace(fd0, thread_specifier{reactor});

    accept_result result{-1};
    bool completed{false};

    iomanager.run_on_wait(reactor, [&]() {
        [](listener_handle& l, accept_result& out, bool& done) -> sisl::async::light_task< void > {
            out = co_await async_accept(l);
            done = true;
        }(*listener, result, completed)
                                                                      .detach();
    });
    ASSERT_FALSE(completed);

    listener.reset();

    ASSERT_TRUE(completed);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_condition(std::errc::connection_aborted));
    ::unlink(path.c_str());
}

TEST_F(NetOpTest, RecvExactAcrossTwoWrites) {
    auto const [fd0, fd1] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();
    socket_handle sock(fd0, thread_specifier{reactor});
    std::array< uint8_t, 10 > read_buf{};

    auto fut = start< io_result >(reactor, [&]() { return recv_exact(sock, std::span< uint8_t >(read_buf)); });

    std::array< uint8_t, 4 > const chunk1{'a', 'b', 'c', 'd'};
    ASSERT_EQ(::write(fd1, chunk1.data(), chunk1.size()), static_cast< ssize_t >(chunk1.size()));
    ASSERT_EQ(fut.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    std::array< uint8_t, 6 > const chunk2{'e', 'f', 'g', 'h', 'i', 'j'};
    ASSERT_EQ(::write(fd1, chunk2.data(), chunk2.size()), static_cast< ssize_t >(chunk2.size()));

    auto const result = fut.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, read_buf.size());
    std::array< uint8_t, 10 > const expected{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};
    EXPECT_EQ(read_buf, expected);
    ::close(fd1);
}

TEST_F(NetOpTest, RecvExactShortEofIsError) {
    auto const [fd0, fd1] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();
    socket_handle sock(fd0, thread_specifier{reactor});
    std::array< uint8_t, 10 > read_buf{};

    auto fut = start< io_result >(reactor, [&]() { return recv_exact(sock, std::span< uint8_t >(read_buf)); });

    std::array< uint8_t, 4 > const chunk{'a', 'b', 'c', 'd'};
    ASSERT_EQ(::write(fd1, chunk.data(), chunk.size()), static_cast< ssize_t >(chunk.size()));
    ::close(fd1); // peer gone before the remaining 6 bytes ever arrive

    auto const result = fut.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_condition(std::errc::connection_aborted));
}

TEST_F(NetOpTest, SendAllThenRecvExactRoundTrip) {
    auto const [fd0, fd1] = make_connected_pair();
    std::vector< uint8_t > payload(4096);
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast< uint8_t >(i);
    }
    std::vector< uint8_t > read_buf(payload.size());
    io_result result{0};

    iomanager.run_on_wait(reactor_regex::random_worker, [&]() {
        socket_handle a(fd0, thread_specifier{iomanager.this_reactor()});
        socket_handle b(fd1, thread_specifier{iomanager.this_reactor()});
        result = sisl::async::sync_get([](socket_handle& snd, socket_handle& rcv, std::span< uint8_t const > out,
                                          std::span< uint8_t > in) -> sisl::async::light_task< io_result > {
            auto sr = co_await send_all(snd, out);
            if (!sr) { co_return sr; }
            co_return co_await recv_exact(rcv, in);
        }(a, b, std::span< uint8_t const >(payload), std::span< uint8_t >(read_buf)));
    });

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, payload.size());
    EXPECT_EQ(read_buf, payload);
}

TEST_F(NetOpTest, AcceptThenPlaceOnDifferentReactor) {
    auto reactors = two_distinct_worker_reactors();
    ASSERT_GE(reactors.size(), 2u);
    IOReactor* const accepting_reactor = reactors[0];
    IOReactor* const placement_reactor = reactors[1];

    auto const [listen_sock, path] = make_listening_socket();
    sockaddr_un addr = make_addr(path);

    listener_handle listener(listen_sock, thread_specifier{accepting_reactor});
    auto accepted_fut = start< accept_result >(accepting_reactor, [&]() { return async_accept(listener); });

    int const client_sock = ::socket(AF_UNIX, SOCK_STREAM, 0);
    ASSERT_GE(client_sock, 0);
    ASSERT_EQ(::connect(client_sock, reinterpret_cast< sockaddr* >(&addr), sizeof(addr)), 0);

    auto const accept_result_v = accepted_fut.get();
    ASSERT_TRUE(accept_result_v.has_value());
    int const server_side_fd = *accept_result_v;

    // Place the accepted connection on a reactor other than the one that accepted it: construction may
    // run from any thread, including the accepting reactor.
    socket_handle placed(server_side_fd, thread_specifier{placement_reactor});

    std::array< uint8_t, 4 > const payload{'p', 'i', 'n', 'g'};
    ASSERT_EQ(::write(client_sock, payload.data(), payload.size()), static_cast< ssize_t >(payload.size()));

    std::array< uint8_t, 4 > read_buf{};
    io_result recv_result{0};
    iomanager.run_on_wait(placement_reactor, [&]() {
        recv_result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t > buf) -> sisl::async::light_task< io_result > {
                co_return co_await recv_exact(s, buf);
            }(placed, std::span< uint8_t >(read_buf)));
    });

    ASSERT_TRUE(recv_result.has_value());
    EXPECT_EQ(read_buf, payload);

    ::close(client_sock);
    ::unlink(path.c_str());
}

TEST_F(NetOpTest, ConnectSucceedsAndRoundTrips) {
    auto const [listen_fd, path] = make_listening_socket();
    auto* const reactor = pick_a_worker_reactor();
    listener_handle listener(listen_fd, thread_specifier{reactor});
    sockaddr_un addr = make_addr(path);

    int const client_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);

    std::expected< void, std::error_condition > connect_result;
    accept_result accept_res{-1};

    // connect() can complete as soon as the kernel enqueues the pending connection into the listener's
    // backlog -- it doesn't need to race an in-flight accept, so issuing them sequentially is deterministic.
    iomanager.run_on_wait(reactor, [&]() {
        [](listener_handle& l, int cfd, sockaddr_un a, IOReactor* r,
           std::expected< void, std::error_condition >& cres, accept_result& ares) -> sisl::async::light_task< void > {
            cres = co_await async_connect(cfd, reinterpret_cast< sockaddr* >(&a), sizeof(a), thread_specifier{r});
            ares = co_await async_accept(l);
        }(listener, client_fd, addr, reactor, connect_result, accept_res)
                                                                      .detach();
    });

    ASSERT_TRUE(connect_result.has_value());
    ASSERT_TRUE(accept_res.has_value());

    socket_handle client(client_fd, thread_specifier{reactor});
    socket_handle server(*accept_res, thread_specifier{reactor});

    std::array< uint8_t, 4 > const payload{'p', 'i', 'n', 'g'};
    io_result rt_result{0};
    iomanager.run_on_wait(reactor, [&]() {
        rt_result =
            sisl::async::sync_get([](socket_handle& snd, socket_handle& rcv,
                                     std::span< uint8_t const > out) -> sisl::async::light_task< io_result > {
                auto sr = co_await send_all(snd, out);
                if (!sr) { co_return sr; }
                std::array< uint8_t, 4 > in{};
                auto rr = co_await recv_exact(rcv, std::span< uint8_t >(in));
                if (!rr) { co_return rr; }
                co_return io_result{std::equal(in.begin(), in.end(), out.begin()) ? *rr : 0};
            }(client, server, std::span< uint8_t const >(payload)));
    });

    ASSERT_TRUE(rt_result.has_value());
    EXPECT_EQ(*rt_result, payload.size());
    ::unlink(path.c_str());
}

TEST_F(NetOpTest, ConnectToNonexistentPathFails) {
    auto* const reactor = pick_a_worker_reactor();
    sockaddr_un addr = make_addr("/tmp/iomgr_net_op_test_no_such_socket_" + std::to_string(::getpid()));

    int const client_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);

    auto fut = start< std::expected< void, std::error_condition > >(reactor, [&]() {
        return async_connect(client_fd, reinterpret_cast< sockaddr* >(&addr), sizeof(addr), thread_specifier{reactor});
    });

    auto const result = fut.get();
    ASSERT_FALSE(result.has_value());
    ::close(client_fd);
}

TEST_F(NetOpTest, ConnectTimesOut) {
    // 192.0.2.0/24 is RFC 5737 TEST-NET-1: reserved strictly for documentation/examples, guaranteed never
    // assigned or routable. A non-blocking connect() to it starts a real TCP handshake (genuine
    // EINPROGRESS, properly poll-awaitable -- unlike ConnectTimesOut's previous AF_UNIX-backlog approach,
    // which turned out to report EAGAIN in a way IORING_OP_CONNECT doesn't actually wait on) that will
    // never get a SYN-ACK or RST, so it sits pending until something gives up -- deterministic without
    // depending on any peer/listener behavior.
    auto* const reactor = pick_a_worker_reactor();
    int const client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(80);
    ASSERT_EQ(::inet_pton(AF_INET, "192.0.2.1", &addr.sin_addr), 1);

    auto fut = start< std::expected< void, std::error_condition > >(reactor, [&]() {
        return async_connect(client_fd, reinterpret_cast< sockaddr* >(&addr), sizeof(addr), thread_specifier{reactor},
                             std::chrono::milliseconds(50));
    });

    auto const result = fut.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_condition(std::errc::timed_out));

    ::close(client_fd);
}

// All prior tests exercise one or two sockets at a time. This exercises many socket_handles pinned to the
// SAME reactor recv'ing concurrently, to build confidence the shared per-reactor ring actually
// disambiguates completions correctly by user_data under real multiplexing rather than just in isolation
// -- the scenario the whole "hundreds of connections on one reactor" design point is about.
TEST_F(NetOpTest, ManyConcurrentRecvsOnOneReactorDontCrossTalk) {
    constexpr int kNumSockets = 32;
    auto* const reactor = pick_a_worker_reactor();

    std::vector< int > peer_fds(kNumSockets);
    std::vector< std::optional< socket_handle > > socks(kNumSockets);
    std::vector< std::vector< uint8_t > > payloads(kNumSockets);
    std::vector< std::vector< uint8_t > > read_bufs(kNumSockets);

    for (int i = 0; i < kNumSockets; ++i) {
        auto const [fd0, fd1] = make_connected_pair();
        peer_fds[i] = fd1;
        socks[i].emplace(fd0, thread_specifier{reactor});
        payloads[i] = std::vector< uint8_t >(256, static_cast< uint8_t >(i));
        read_bufs[i] = std::vector< uint8_t >(256);
    }

    std::vector< std::future< io_result > > futs;
    futs.reserve(kNumSockets);
    for (int i = 0; i < kNumSockets; ++i) {
        futs.push_back(start< io_result >(
            reactor, [&, i]() { return recv_exact(*socks[i], std::span< uint8_t >(read_bufs[i])); }));
    }
    // All kNumSockets recvs are now parked concurrently on the same reactor's ring before any data
    // arrives. Writing from the peer side afterward (order doesn't matter -- the kernel buffers each
    // independently) is what actually forces all of them to complete via uring, not the fast path.
    for (int i = 0; i < kNumSockets; ++i) {
        ASSERT_EQ(::write(peer_fds[i], payloads[i].data(), payloads[i].size()),
                  static_cast< ssize_t >(payloads[i].size()));
    }

    for (int i = 0; i < kNumSockets; ++i) {
        auto const r = futs[i].get();
        ASSERT_TRUE(r.has_value()) << "socket " << i;
        EXPECT_EQ(read_bufs[i], payloads[i]) << "socket " << i << " got cross-talked data";
    }

    for (int fd : peer_fds) {
        ::close(fd);
    }
}

// Deliberately not a NetOpTest fixture: this exercises what happens when a caller does NOT follow
// socket_handle's documented contract ("destruction must run before the owning reactor stops"), so it
// needs to control iomanager's start/stop itself rather than share NetOpTest::TearDown's single stop().
TEST(NetOpReactorStopTest, CancelsOutstandingRecvInstead) {
    ioenvironment.with_iomgr(iomgr_params{.num_threads = 2});
    auto const [fd0, fd1] = make_connected_pair();
    IOReactor* reactor{nullptr};
    iomanager.run_on_wait(reactor_regex::random_worker, [&reactor]() { reactor = iomanager.this_reactor(); });

    // Heap-allocated and deliberately leaked: its destructor would try to dispatch onto `reactor`, which
    // is about to stop -- exactly the hang the documented contract warns about. This test is specifically
    // about the reactor-stop drain path, not about socket_handle's own (already covered) teardown path.
    auto* const sock = new socket_handle(fd0, thread_specifier{reactor});

    io_result result{0};
    bool completed{false};
    std::array< uint8_t, 5 > read_buf{};

    iomanager.run_on_wait(reactor, [&]() {
        [](socket_handle& s, std::span< uint8_t > buf, io_result& out, bool& done) -> sisl::async::light_task< void > {
            out = co_await async_recv(s, buf);
            done = true;
        }(*sock, std::span< uint8_t >(read_buf), result, completed)
                                                                                          .detach();
    });
    ASSERT_FALSE(completed); // parked; nothing written

    // Must not hang despite the still-outstanding recv: the net channel's reactor-stop drain has to
    // actively cancel it (a peer write that will never come isn't something passive draining can wait
    // out, unlike disk I/O always eventually completing on its own).
    iomanager.stop();

    EXPECT_TRUE(completed);
    EXPECT_FALSE(result.has_value());
    ::close(fd1);
    ::close(fd0);
}

// Same drain path as CancelsOutstandingRecvInstead, but for a parked accept and a parked (no-timeout)
// connect -- drain_outstanding_ops() cancels the whole ring in one shot rather than per-handle, so it
// needs to actually cover every op kind, not just recv.
TEST(NetOpReactorStopTest, CancelsOutstandingAcceptAndConnectInstead) {
    ioenvironment.with_iomgr(iomgr_params{.num_threads = 2});
    IOReactor* reactor{nullptr};
    iomanager.run_on_wait(reactor_regex::random_worker, [&reactor]() { reactor = iomanager.this_reactor(); });

    auto const [listen_fd, path] = make_listening_socket();
    // Heap-allocated and deliberately leaked, same reasoning as CancelsOutstandingRecvInstead: its
    // destructor would try to dispatch onto `reactor`, which is about to stop.
    auto* const listener = new listener_handle(listen_fd, thread_specifier{reactor});

    accept_result accept_res{-1};
    bool accept_completed{false};
    iomanager.run_on_wait(reactor, [&]() {
        [](listener_handle& l, accept_result& out, bool& done) -> sisl::async::light_task< void > {
            out = co_await async_accept(l);
            done = true;
        }(*listener, accept_res, accept_completed)
                                                                      .detach();
    });
    ASSERT_FALSE(accept_completed);

    // Same TEST-NET-1 target as ConnectTimesOut, with no timeout: genuinely still pending (not resolved
    // by its own timeout) by the time stop() runs below.
    int const client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(80);
    ASSERT_EQ(::inet_pton(AF_INET, "192.0.2.1", &addr.sin_addr), 1);

    std::expected< void, std::error_condition > connect_res;
    bool connect_completed{false};
    iomanager.run_on_wait(reactor, [&]() {
        [](int fd, sockaddr_in a, IOReactor* r, std::expected< void, std::error_condition >& out,
           bool& done) -> sisl::async::light_task< void > {
            out = co_await async_connect(fd, reinterpret_cast< sockaddr* >(&a), sizeof(a), thread_specifier{r});
            done = true;
        }(client_fd, addr, reactor, connect_res, connect_completed)
                                                                      .detach();
    });
    ASSERT_FALSE(connect_completed);

    iomanager.stop(); // must cancel both, not hang

    EXPECT_TRUE(accept_completed);
    EXPECT_FALSE(accept_res.has_value());
    EXPECT_TRUE(connect_completed);
    EXPECT_FALSE(connect_res.has_value());

    ::close(client_fd);
    ::unlink(path.c_str());
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging, iomgr, config);
    sisl::logging::SetLogger("test_net_op");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");
    return RUN_ALL_TESTS();
}
