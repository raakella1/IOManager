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
#pragma once

#include <cstdint>
#include <string>

/// NOTE: These are defined here to prevent inclusion of liburing/compat.h which conflicts
/// with system header /usr/include/futex.h
#define LIBURING_COMPAT_H
#define BLOCK_URING_CMD_DISCARD _IO(0x12, 0)
///

#include "open_how_compat.hpp" // complete struct open_how before <liburing.h> (Ubuntu 22.04 / gcc 13)
#include <liburing.h>
#include <sys/eventfd.h>

#include <exec/async_scope.hpp>
#include <sisl/async/io_uring_scheduler.hpp>

#include "io_interface.hpp" // IOInterface (internal)
#include <iomgr/iomgr_types.hpp>

namespace iomgr {

// Per-reactor io_uring channel dedicated to net_op's socket ops (recv/send/accept/connect). Kept
// separate from UringDriveInterface's own per-reactor ring: a socket_handle can be pinned to a reactor
// with no drive interface attached at all, and disk I/O (few, large ops) and socket I/O (many, small,
// short-lived ops) want independently sized queues rather than sharing one ring's SQE/CQE budget.
class UringNetInterface;
struct uring_net_channel {
    ::io_uring m_ring{};
    sisl::async::io_uring_scheduler m_sched; // wraps &m_ring; non-movable
    io_device_ptr m_ring_ev_iodev;
    uint32_t m_sentinel_idx{0};
    uint64_t m_outstanding{0}; // in-flight net ops on this reactor
    // connect_op has no owning handle of its own (unlike socket_handle/listener_handle) to host a scope
    // that outlives any single call, so its run_uring_connect task spawns into this channel-level one
    // instead -- long-lived for the reactor's whole life, so resuming a connect's waiter (which may
    // immediately destroy the connect_op) can never race the scope's own self-destruction the way a
    // per-call scope owned by connect_op::impl itself would (confirmed by hitting exec::async_scope's
    // "__active_ == 0" destructor assertion with that approach).
    exec::async_scope m_scope;

    uring_net_channel(UringNetInterface* iface);
    ~uring_net_channel();
};

class UringNetInterface : public IOInterface {
public:
    UringNetInterface() = default;
    ~UringNetInterface() override = default;
    std::string name() const override { return "uring_net_interface"; }

    void on_event_notification(IODevice* iodev, void* cookie, int event);

    // Reactor sentinel: flush queued SQEs + reap completions (resuming suspended coroutines).
    void poll_completions();

    static uring_net_channel* this_channel() { return t_uring_net_ch; }

private:
    void init_iface_reactor_context(IOReactor*) override;
    void clear_iface_reactor_context(IOReactor*) override;
    // Reactor stop: actively cancel everything still in flight on this channel's ring (unlike disk I/O, a
    // parked accept/recv/connect can wait forever for an external event that will never come once this
    // reactor is stopping, so passively waiting for "outstanding == 0" the way drive's drain does is not
    // safe here), then drive poll_once until every cancellation has landed.
    void drain_outstanding_ops();

private:
    static thread_local uring_net_channel* t_uring_net_ch;
};
} // namespace iomgr
