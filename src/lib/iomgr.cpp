//
// Created by Rishabh Mittal on 04/20/2018
//

#include "iomgr.hpp"

extern "C" {
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/types.h>

#include <spdk/log.h>
#include <spdk/env.h>
#include <spdk/thread.h>
#include <spdk/bdev.h>
#include <spdk/env_dpdk.h>
#include <rte_errno.h>
}

#include <sds_logging/logging.h>

#include <cerrno>
#include <chrono>
#include <ctime>
#include <functional>
#include <thread>
#include <vector>

#include "include/aio_drive_interface.hpp"
#include "include/spdk_drive_interface.hpp"
#include "include/iomgr.hpp"
#include "include/reactor_epoll.hpp"
#include "include/reactor_spdk.hpp"
#include <utility/thread_factory.hpp>
#include <fds/obj_allocator.hpp>
#include <experimental/random>

namespace iomgr {

IOManager::IOManager() : m_thread_idx_reserver(max_io_threads) {
    m_iface_list.wlock()->reserve(inbuilt_interface_count + 5);
}

IOManager::~IOManager() = default;

void IOManager::start(size_t const num_threads, bool is_spdk, const thread_state_notifier_t& notifier,
                      const interface_adder_t& iface_adder) {
    if (get_state() == iomgr_state::running) {
        LOGWARN("WARNING: IOManager is asked to start, but it is already in running state. Ignoring the start request");
        return;
    }

    LOGINFO("Starting IOManager with {} threads", num_threads);
    m_is_spdk = is_spdk;
    // m_expected_ifaces += expected_custom_ifaces;
    m_yet_to_start_nreactors.set(num_threads);
    m_worker_reactors.reserve(num_threads * 2); // Have preallocate for iomgr slots

    // One common module and other internal handler
    m_common_thread_state_notifier = notifier;
    m_internal_msg_module_id = register_msg_module([this](iomgr_msg* msg) { this_reactor()->handle_msg(msg); });

    // Start the SPDK
    if (is_spdk) { start_spdk(); }

    // Create all in-built interfaces here
    set_state(iomgr_state::interface_init);
    m_default_general_iface = std::make_shared< GenericIOInterface >();
    add_interface(m_default_general_iface);

    // If caller wants to add the interface by themselves, allow to do so, else add drive interface by ourselves
    if (iface_adder) {
        iface_adder();
    } else {
        add_drive_interface(
            is_spdk ? std::dynamic_pointer_cast< iomgr::DriveInterface >(std::make_shared< SpdkDriveInterface >())
                    : std::dynamic_pointer_cast< iomgr::DriveInterface >(std::make_shared< AioDriveInterface >()),
            true);
    }

    // Start all reactor threads
    set_state(iomgr_state::reactor_init);
    for (auto i = 0u; i < num_threads; i++) {
        m_worker_reactors.push_back(reactor_info_t(
            sisl::thread_factory("iomgr_thread", &IOManager::_run_io_loop, this, (int)i, m_is_spdk, nullptr, nullptr),
            nullptr));
        LOGTRACEMOD(iomgr, "Created iomanager worker reactor thread {}...", i);
        // t.detach();
    }
    wait_for_state(iomgr_state::sys_init);

    // Start the global timer
    m_global_user_timer = std::make_unique< timer_epoll >(thread_regex::all_user);
    m_global_worker_timer = is_spdk ? std::unique_ptr< timer >(new timer_spdk(thread_regex::all_worker))
                                    : std::unique_ptr< timer >(new timer_epoll(thread_regex::all_worker));

    if (is_spdk && !m_is_spdk_inited_externally) {
        LOGINFO("Initializing bdev subsystem");
        iomanager.run_on(thread_regex::least_busy_worker,
                         [this](io_thread_addr_t taddr) {
                             spdk_bdev_initialize(
                                 [](void* cb_arg, int rc) {
                                     IOManager* pthis = (IOManager*)cb_arg;
                                     pthis->set_state_and_notify(iomgr_state::running);
                                 },
                                 (void*)this);
                         },
                         false /* wait_for_completion */);
        wait_for_state(iomgr_state::running);
    } else {
        set_state(iomgr_state::running);
    }

    // Notify all the reactors that they are ready to make callback about thread started
    iomanager.run_on(thread_regex::all_io,
                     [this](io_thread_addr_t taddr) { iomanager.this_reactor()->notify_thread_state(true); },
                     false /* wait_for_completion */);
}

void IOManager::start_spdk() {
    // Initialize if spdk has still not been initialized
    m_is_spdk_inited_externally = !spdk_env_dpdk_external_init();
    if (!m_is_spdk_inited_externally) {
        struct spdk_env_opts opts;
        spdk_env_opts_init(&opts);
        opts.name = "hs_code";
        opts.shm_id = -1;
        //    opts.mem_size = 512;

        int rc = spdk_env_init(&opts);
        if (rc != 0) { throw std::runtime_error("SPDK Iniitalization failed"); }

        spdk_unaffinitize_thread();

        // TODO: Do spdk_thread_lib_init_ext to handle spdk thread switching etc..
        rc = spdk_thread_lib_init(NULL, 0);
        if (rc != 0) {
            LOGERROR("Thread lib init returned rte_errno = {} {}", rte_errno, rte_strerror(rte_errno));
            throw std::runtime_error("SPDK Thread Lib Init failed");
        }
    }
    // Set the sisl::allocator with spdk allocator, so that all sisl libraries start to use spdk for aligned allocations
    sisl::AlignedAllocator::instance().set_allocator(std::move(new SpdkAlignedAllocImpl()));
}

void IOManager::stop() {
    LOGINFO("Stopping IOManager");
    set_state(iomgr_state::stopping);

    // Increment stopping threads by 1 and then decrement after sending message to prevent case where there are no
    // IO threads, which hangs the iomanager stop
    m_yet_to_stop_nreactors.increment();

    // Free up and unregister fds for global timer
    m_global_user_timer.reset(nullptr);
    m_global_worker_timer.reset(nullptr);

    // Send all the threads to reliquish its io thread status
    multicast_msg(thread_regex::all_io,
                  iomgr_msg::create(iomgr_msg_type::RELINQUISH_IO_THREAD, m_internal_msg_module_id));

    // Now decrement and check if all io threads have already reliquished the io thread status.
    if (m_yet_to_stop_nreactors.decrement_testz()) {
        set_state(iomgr_state::stopped);
    } else {
        // Few threads are still in process of coming out io loop, wait for them.
        wait_for_state(iomgr_state::stopped);
    }

    LOGINFO("All IO threads have stopped and hence IOManager is moved to stopped state, joining any iomanager threads");

    try {
        // Join all the iomanager threads
        for (auto& t : m_worker_reactors) {
            if (t.first.joinable()) t.first.join();
        }
    } catch (const std::exception& e) { LOGCRITICAL_AND_FLUSH("Caught exception {} during thread join", e.what()); }

    try {
        m_worker_reactors.clear();
        m_yet_to_start_nreactors.set(0);
        // m_expected_ifaces = inbuilt_interface_count;
        m_drive_ifaces.wlock()->clear();
        m_iface_list.wlock()->clear();
    } catch (const std::exception& e) { LOGCRITICAL_AND_FLUSH("Caught exception {} during clear lists", e.what()); }
    assert(get_state() == iomgr_state::stopped);

    LOGINFO("IOManager Stopped and all IO threads are relinquished");
}

void IOManager::add_drive_interface(std::shared_ptr< DriveInterface > iface, bool default_iface) {
    add_interface(std::dynamic_pointer_cast< IOInterface >(iface));
    m_drive_ifaces.wlock()->push_back(iface);
    if (default_iface) m_default_drive_iface = iface;
}

void IOManager::add_interface(std::shared_ptr< IOInterface > iface) {
    auto iface_list = m_iface_list.wlock();

    // Setup the reactor io threads to do any registration for interface specific registration
    iomanager.run_on(thread_regex::all_io,
                     [this, iface](io_thread_addr_t taddr) {
                         iface->on_io_thread_start(iomanager.this_reactor()->addr_to_thread(taddr));
                     },
                     true /* wait_for_completion */);

    iface_list->push_back(iface);
}

void IOManager::_run_io_loop(int iomgr_slot_num, bool is_tloop_reactor, const iodev_selector_t& iodev_selector,
                             const thread_state_notifier_t& addln_notifier) {
    if (is_tloop_reactor) {
        *(m_reactors.get()) = std::make_shared< IOReactorSPDK >();
    } else {
        *(m_reactors.get()) = std::make_shared< IOReactorEPoll >();
    }
    this_reactor()->run(iomgr_slot_num, iodev_selector, addln_notifier);
}

void IOManager::stop_io_loop() { this_reactor()->stop(); }

void IOManager::reactor_started(std::shared_ptr< IOReactor > reactor) {
    m_yet_to_stop_nreactors.increment();
    if (reactor->is_worker()) {
        m_worker_reactors[reactor->m_worker_slot_num].second = reactor;

        // All iomgr created reactors are initialized, move iomgr to sys init (next phase of start)
        if (m_yet_to_start_nreactors.decrement_testz()) {
            LOGINFO("All IOMgr reactors started, moving iomanager to sys_init state");
            set_state_and_notify(iomgr_state::sys_init);
        }
    }
}

void IOManager::reactor_stopped() {
    if (m_yet_to_stop_nreactors.decrement_testz()) { set_state_and_notify(iomgr_state::stopped); }
}

void IOManager::device_reschedule(const io_device_ptr& iodev, int event) {
    multicast_msg(thread_regex::least_busy_worker,
                  iomgr_msg::create(iomgr_msg_type::RESCHEDULE, m_internal_msg_module_id, iodev, event));
}

static bool match_regex(thread_regex r, const io_thread_t& thr) {
    if ((r == thread_regex::all_io) || (r == thread_regex::least_busy_io)) { return true; }
    if (thr->reactor->is_worker()) {
        return ((r == thread_regex::all_worker) || (r == thread_regex::least_busy_worker) ||
                (r == thread_regex::random_worker));
    } else {
        return ((r == thread_regex::all_user) || (r == thread_regex::least_busy_user));
    }
}

int IOManager::run_on(const io_thread_t& thread, spdk_msg_signature_t fn, void* context) {
    assert(thread->reactor->is_tight_loop_reactor());
    spdk_thread_send_msg(thread->spdk_thread_impl(), fn, context);
    return 1;
}

int IOManager::multicast_msg(thread_regex r, iomgr_msg* msg) {
    int sent_to = 0;
    bool cloned = false;
    int64_t min_cnt = INTMAX_MAX;
    io_thread_addr_t min_thread = -1U;
    IOReactor* min_reactor = nullptr;

    if (r == thread_regex::random_worker) {
        // Send to any random iomgr created io thread
        auto& reactor = m_worker_reactors[std::experimental::randint(0, (int)m_worker_reactors.size() - 1)].second;
        sent_to = reactor->deliver_msg(reactor->select_thread()->thread_idx, msg);
    } else {
        _pick_reactors(r, [&](IOReactor* reactor, bool is_last_thread) {
            if (reactor && reactor->is_io_reactor()) {
                for (auto& thr : reactor->io_threads()) {
                    if (match_regex(r, thr)) {
                        if ((r == thread_regex::least_busy_worker) || (r == thread_regex::least_busy_user)) {
                            if (thr->m_metrics->outstanding_ops < min_cnt) {
                                min_thread = thr->thread_addr;
                                min_cnt = thr->m_metrics->outstanding_ops;
                                min_reactor = reactor;
                            }
                        } else {
                            auto new_msg = msg->clone();
                            reactor->deliver_msg(thr->thread_addr, new_msg);
                            cloned = true;
                            ++sent_to;
                        }
                    }
                }
            }

            if (is_last_thread && min_reactor) {
                if (send_msg(min_reactor->addr_to_thread(min_thread), msg)) ++sent_to;
            }
        });
    }

    if (cloned || !sent_to) { iomgr_msg::free(msg); }
    return sent_to;
}

void IOManager::_pick_reactors(thread_regex r, const auto& cb) {
    if ((r == thread_regex::all_worker) || (r == thread_regex::least_busy_worker)) {
        for (auto i = 0u; i < m_worker_reactors.size(); ++i) {
            cb(m_worker_reactors[i].second.get(), (i == (m_worker_reactors.size() - 1)));
        }
    } else {
        all_reactors(cb);
    }
}

int IOManager::multicast_msg_and_wait(thread_regex r, sync_iomgr_msg& smsg) {
    auto sent_to = multicast_msg(r, smsg.base_msg);
    if (sent_to != 0) smsg.wait();
    return sent_to;
}

bool IOManager::send_msg(const io_thread_t& to_thread, iomgr_msg* msg) {
    bool ret = false;
    msg->m_dest_thread = to_thread->thread_addr;

    if (std::holds_alternative< spdk_thread* >(to_thread->thread_impl)) {
        // Shortcut to deliver the message without taking reactor list lock.
        IOReactorSPDK::deliver_msg_direct(std::get< spdk_thread* >(to_thread->thread_impl), msg);
        ret = true;
    } else {
        specific_reactor(std::get< reactor_idx_t >(to_thread->thread_impl), [&](IOReactor* reactor) {
            if (reactor && reactor->is_io_reactor() && reactor->deliver_msg(to_thread->thread_addr, msg)) {
                ret = true;
            }
        });
    }

    if (!ret) { iomgr_msg::free(msg); }
    return ret;
}

bool IOManager::send_msg_and_wait(const io_thread_t& to_thread, sync_iomgr_msg& smsg) {
    auto sent = send_msg(to_thread, smsg.base_msg);
    if (sent) smsg.wait();
    return sent;
}

timer_handle_t IOManager::schedule_thread_timer(uint64_t nanos_after, bool recurring, void* cookie,
                                                timer_callback_t&& timer_fn) {
    return this_reactor()->m_thread_timer->schedule(nanos_after, recurring, cookie, std::move(timer_fn));
}

timer_handle_t IOManager::schedule_global_timer(uint64_t nanos_after, bool recurring, void* cookie, thread_regex r,
                                                timer_callback_t&& timer_fn) {
    timer* t = nullptr;
    if (r == thread_regex::all_worker) {
        t = m_global_worker_timer.get();
    } else if (r == thread_regex::all_user) {
        t = m_global_user_timer.get();
    } else {
        LOGMSG_ASSERT(0, "Setting timer with invalid regex {}", enum_name(r));
        return null_timer_handle;
    }

    return t->schedule(nanos_after, recurring, cookie, std::move(timer_fn));
}

void IOManager::foreach_interface(const auto& iface_cb) {
    m_iface_list.withRLock([&](auto& iface_list) {
        for (auto iface : iface_list) {
            iface_cb(iface.get());
        }
    });
}

IOReactor* IOManager::this_reactor() const { return m_reactors.get()->get(); }
void IOManager::all_reactors(const auto& cb) {
    m_reactors.access_all_threads(
        [&cb](std::shared_ptr< IOReactor >* preactor, bool is_last_thread) { cb(preactor->get(), is_last_thread); });
}

void IOManager::specific_reactor(int thread_num, const auto& cb) {
    m_reactors.access_specific_thread(thread_num,
                                      [&cb](std::shared_ptr< IOReactor >* preactor) { cb(preactor->get()); });
}

msg_module_id_t IOManager::register_msg_module(const msg_handler_t& handler) {
    std::unique_lock lk(m_msg_hdlrs_mtx);
    DEBUG_ASSERT_LT(m_msg_handlers_count, m_msg_handlers.size(), "More than expected msg modules registered");
    m_msg_handlers[m_msg_handlers_count++] = handler;
    return m_msg_handlers_count - 1;
}

io_thread_t IOManager::make_io_thread(IOReactor* reactor) {
    io_thread_t t = std::make_shared< io_thread >(reactor);
    t->thread_idx = m_thread_idx_reserver.reserve();
    if (t->thread_idx >= max_io_threads) {
        throw std::system_error(errno, std::generic_category(), "Running IO Threads exceeds limit");
    }
    return t;
}

// It is ok not to take a lock to get msg modules, since we don't support unregister a module. Taking a lock
// here defeats the purpose of per thread messages here.
msg_handler_t& IOManager::get_msg_module(msg_module_id_t id) { return m_msg_handlers[id]; }

const io_thread_t& IOManager::iothread_self() const { return this_reactor()->iothread_self(); };

/****** IODevice related ********/
IODevice::IODevice() { m_thread_local_ctx.reserve(IOManager::max_io_threads); }

std::string IODevice::dev_id() {
    if (std::holds_alternative< int >(dev)) {
        return std::to_string(fd());
    } else if (std::holds_alternative< spdk_bdev_desc* >(dev)) {
        return spdk_bdev_get_name(bdev());
    } else {
        return "";
    }
}

spdk_bdev_desc* IODevice::bdev_desc() { return std::get< spdk_bdev_desc* >(dev); }
spdk_bdev* IODevice::bdev() { return spdk_bdev_desc_get_bdev(bdev_desc()); }
spdk_nvmf_qpair* IODevice::nvmf_qp() const { return std::get< spdk_nvmf_qpair* >(dev); }

bool IODevice::is_global() const { return (!std::holds_alternative< io_thread_t >(thread_scope)); }
bool IODevice::is_my_thread_scope() const {
    return (!is_global() && (per_thread_scope() == iomanager.iothread_self()));
}

void IODevice::clear() {
    dev = -1;
    io_interface = nullptr;
    tinfo = nullptr;
    cookie = nullptr;
    m_thread_local_ctx.clear();
}

uint8_t* IOManager::iobuf_alloc(size_t align, size_t size) {
    size = sisl::round_up(size, align);
    return m_is_spdk ? (uint8_t*)spdk_malloc(size, align, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA)
                     : (uint8_t*)std::aligned_alloc(align, size);
}

void IOManager::iobuf_free(uint8_t* buf) { m_is_spdk ? spdk_free((void*)buf) : std::free(buf); }

uint8_t* IOManager::iobuf_realloc(uint8_t* buf, size_t align, size_t new_size) {
    return m_is_spdk ? (uint8_t*)spdk_realloc((void*)buf, new_size, align) : sisl_aligned_realloc(buf, align, new_size);
}

/* SpkdAllocator Implementaion */
uint8_t* SpdkAlignedAllocImpl::aligned_alloc(size_t align, size_t size) {
    return (uint8_t*)spdk_malloc(size, align, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
}

void SpdkAlignedAllocImpl::aligned_free(uint8_t* b) { spdk_free((void*)b); }

uint8_t* SpdkAlignedAllocImpl::aligned_realloc(uint8_t* old_buf, size_t align, size_t new_sz, size_t old_sz) {
    return (uint8_t*)spdk_realloc((void*)old_buf, new_sz, align);
}

} // namespace iomgr
