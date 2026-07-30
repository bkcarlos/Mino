// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_SUBSCRIBER_LEASE_H_
#define MINO_RUNTIME_SUBSCRIBER_LEASE_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/shm/channel/broadcast_channel.h"

namespace mino {

enum class SubscriberLeaseState : uint32_t {
    kFree = 0,
    kRegistering = 1,
    kActive = 2,
    kEvicting = 3,
    kEvicted = 4,
};

// One cache-line record per Broadcast subscriber id. subscriber_generation
// binds ACK ownership; lease_epoch is an independent Coordinator grant epoch.
struct alignas(64) SubscriberLeaseRecord {
    std::atomic<uint32_t> state{0};
    uint32_t subscriber_id = 0;
    std::atomic<uint64_t> subscriber_generation{0};
    ProcessIdentity owner;
    std::atomic<uint64_t> lease_epoch{0};
    std::atomic<uint64_t> heartbeat_ns{0};
};

static_assert(sizeof(SubscriberLeaseRecord) == 64);
static_assert(alignof(SubscriberLeaseRecord) == 64);
static_assert(std::is_standard_layout_v<SubscriberLeaseRecord>);

struct SubscriberLeaseHandle {
    BroadcastChannel::SubscriberHandle subscriber;
    uint64_t lease_epoch = 0;
};

struct SubscriberLeaseSnapshot {
    SubscriberLeaseHandle handle;
    ProcessIdentity owner;
    uint64_t heartbeat_ns = 0;
    SubscriberLeaseState state = SubscriberLeaseState::kFree;
};

// Non-owning process-local facade over a shared SubscriberLeaseRecord table.
class SubscriberLeaseTable {
public:
    static constexpr uint32_t kMaxSubscribers =
        BroadcastChannel::kMaxSubscribers;
    static constexpr uint64_t kMagic = 0x4D49'4E4F'4C45'4153ULL;  // MINOLEAS
    static constexpr uint32_t kLayoutVersion = 1;

    struct alignas(64) ControlBlock {
        std::atomic<uint64_t> magic{0};
        std::atomic<uint32_t> layout_version{0};
        uint32_t max_subscribers = 0;
        unsigned char padding[64 - 8 - 4 - 4] = {};
    };

    static_assert(sizeof(ControlBlock) == 64);

    static constexpr uint64_t RequiredSize() noexcept {
        return sizeof(ControlBlock) +
               kMaxSubscribers * sizeof(SubscriberLeaseRecord);
    }

    static Result<SubscriberLeaseTable> Init(void* shm_base);
    static Result<SubscriberLeaseTable> Attach(void* shm_base);

    Result<SubscriberLeaseHandle> Register(
        BroadcastChannel::SubscriberHandle subscriber,
        const ProcessIdentity& owner, uint64_t now_ns) noexcept;

    Status Heartbeat(SubscriberLeaseHandle handle,
                     uint64_t now_ns) noexcept;

    Result<SubscriberLeaseSnapshot> Read(uint32_t subscriber_id) const noexcept;

    Result<SubscriberLeaseSnapshot> BeginEviction(
        SubscriberLeaseHandle handle, uint64_t now_ns, uint64_t lease_ns,
        bool require_expired) noexcept;

    Status CancelEviction(SubscriberLeaseHandle handle) noexcept;
    Status FinishEviction(SubscriberLeaseHandle handle) noexcept;

    SubscriberLeaseState State(uint32_t subscriber_id) const noexcept;

private:
    explicit SubscriberLeaseTable(SubscriberLeaseRecord* records) noexcept
        : records_(records) {}

    static SubscriberLeaseRecord* RecordsOf(void* shm_base) noexcept {
        return reinterpret_cast<SubscriberLeaseRecord*>(
            static_cast<unsigned char*>(shm_base) + sizeof(ControlBlock));
    }

    static bool Matches(const SubscriberLeaseRecord& record,
                        SubscriberLeaseHandle handle) noexcept;

    SubscriberLeaseRecord* records_ = nullptr;
};

class SubscriberLeaseCoordinator {
public:
    using IdentityProbe = bool (*)(const ProcessIdentity&, void*) noexcept;
    using PinCleanup = void (*)(const ProcessIdentity&, void*) noexcept;

    SubscriberLeaseCoordinator(BroadcastChannel& channel,
                               SubscriberLeaseTable& leases,
                               IdentityProbe identity_probe = nullptr,
                               void* identity_probe_context = nullptr,
                               PinCleanup pin_cleanup = nullptr,
                               void* pin_cleanup_context = nullptr) noexcept;

    Result<SubscriberLeaseHandle> Register(
        SubscriberId id,
        const ProcessIdentity& owner = ProcessIdentity::Current(),
        uint64_t now_ns = BroadcastChannel::MonotonicNowNs()) noexcept;

    Status Heartbeat(SubscriberLeaseHandle handle,
                     uint64_t now_ns = BroadcastChannel::MonotonicNowNs())
        noexcept;

    Status Unregister(SubscriberLeaseHandle handle) noexcept;

    // Evicts expired leases whose exact ProcessIdentity is no longer alive.
    // Returns the number of completed evictions.
    uint64_t EvictExpired(uint64_t now_ns, uint64_t lease_ns) noexcept;

private:
    static bool DefaultIdentityProbe(const ProcessIdentity& identity,
                                     void*) noexcept {
        return IsProcessIdentityAlive(identity);
    }

    void CleanupPins(const ProcessIdentity& owner) noexcept {
        if (pin_cleanup_ != nullptr) {
            pin_cleanup_(owner, pin_cleanup_context_);
        }
    }

    BroadcastChannel* channel_;
    SubscriberLeaseTable* leases_;
    IdentityProbe identity_probe_;
    void* identity_probe_context_;
    PinCleanup pin_cleanup_;
    void* pin_cleanup_context_;
};

}  // namespace mino

#endif  // MINO_RUNTIME_SUBSCRIBER_LEASE_H_
