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
#include "interfaces/uring_net_interface.hpp"
#include <iomgr/iomgr.hpp>

#include <system_error>

#ifdef __linux__
#include <sys/epoll.h>
#endif

#include <sisl/logging/logging.h>
#include "iomgr_config.hpp"
#include "reactor/reactor.hpp"

namespace iomgr {
thread_local uring_net_channel* UringNetInterface::t_uring_net_ch{nullptr};

uring_net_channel::uring_net_channel(UringNetInterface* iface) : m_sched{&m_ring} {
    auto const qdepth = IM_DYNAMIC_CONFIG(net.uring_per_thread_qdepth);

    // See uring_drive_channel's constructor for the rationale on these flags -- same per-reactor,
    // single-submitter story applies here.
    io_uring_params params{};
#ifdef IORING_SETUP_SINGLE_ISSUER
    if (IM_DYNAMIC_CONFIG(net.uring_single_issuer) != 0) { params.flags |= IORING_SETUP_SINGLE_ISSUER; }
#endif
#ifdef IORING_SETUP_CQSIZE
    if (auto const cq_mult = IM_DYNAMIC_CONFIG(net.uring_cq_size_multiple); cq_mult > 0) {
        params.flags |= IORING_SETUP_CQSIZE;
        params.cq_entries = qdepth * cq_mult;
    }
#endif

    int ret = io_uring_queue_init_params(qdepth, &m_ring, &params);
    if ((ret == -EINVAL) && (params.flags != 0)) {
        LOGWARN("io_uring setup flags {:#x} unsupported by this kernel; retrying net ring with defaults",
                params.flags);
        params = io_uring_params{};
        ret = io_uring_queue_init_params(qdepth, &m_ring, &params);
    }
    if (ret) {
        throw std::system_error{-ret, std::system_category(),
                                fmt::format("Unable to create net uring queue ret={}", ret)};
    }

    int ev_fd = eventfd(0, EFD_NONBLOCK);
    if (ev_fd == -1) {
        throw std::system_error{errno, std::system_category(),
                                "Unable to create eventfd to listen for net uring queue events"};
    }

    ret = io_uring_register_eventfd(&m_ring, ev_fd);
    if (ret == -1) {
        throw std::system_error{errno, std::system_category(), "Unable to register event fd to net uring queue"};
    }

    // Create io device and add it to the local thread; the sentinel reaps completions every loop.
    using namespace std::placeholders;
    m_ring_ev_iodev = iomanager.generic_interface()->make_io_device(
        ev_fd, EPOLLIN, 0, nullptr, true, std::bind(&UringNetInterface::on_event_notification, iface, _1, _2, _3));
    m_sentinel_idx = iomanager.this_reactor()->attach_iomgr_sentinel_cb([iface]() { iface->poll_completions(); });
}

uring_net_channel::~uring_net_channel() {
    io_uring_queue_exit(&m_ring);
    if (m_ring_ev_iodev != nullptr) {
        iomanager.this_reactor()->detach_iomgr_sentinel_cb(m_sentinel_idx);
        iomanager.generic_interface()->remove_io_device(m_ring_ev_iodev, true /* wait_to_remove */);
    }
}

void UringNetInterface::init_iface_reactor_context(IOReactor*) {
    if (t_uring_net_ch == nullptr) { t_uring_net_ch = new uring_net_channel(this); }
}

void UringNetInterface::clear_iface_reactor_context(IOReactor*) {
    // No net ops exist yet (scaffolding-only chunk), so there is nothing outstanding to drain here.
    // Once socket/listener/connect ops land, this needs to actively cancel anything still in flight
    // before tearing the ring down -- unlike disk I/O, a parked accept/recv can wait forever if the
    // peer never sends/connects, so passively waiting for "outstanding == 0" is not safe here.
    if (t_uring_net_ch != nullptr) {
        delete t_uring_net_ch;
        t_uring_net_ch = nullptr;
    }
}

void UringNetInterface::poll_completions() {
    // Flush any SQEs queued since the last poll and reap available completions, resuming the suspended
    // coroutines inline on this reactor thread. Non-blocking: the reactor's epoll_wait (woken by the
    // ring eventfd) provides the actual wait.
    t_uring_net_ch->m_sched.poll_once(std::chrono::nanoseconds{0});
}

void UringNetInterface::on_event_notification(IODevice* iodev, [[maybe_unused]] void* cookie,
                                              [[maybe_unused]] int event) {
    uint64_t temp = 0;
    [[maybe_unused]] auto rsize = read(iodev->fd(), &temp, sizeof(uint64_t));
    poll_completions();
}
} // namespace iomgr
