// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_JOURNAL_CHANNEL_RECOVERY_H_
#define MINO_RUNTIME_JOURNAL_CHANNEL_RECOVERY_H_

#include <cstdint>
#include <optional>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/runtime/allocation_journal.h"
#include "mino/shm/channel/broadcast_channel.h"
#include "mino/shm/channel/mpsc_channel.h"
#include "mino/shm/channel/spsc_channel.h"

namespace mino {

// Read-only publication observer for BroadcastChannel shared memory. This is a
// separate adapter so recovery can inspect broadcast publication state without
// changing BroadcastChannel's public facade or shared-memory ABI.
class BroadcastPublicationView {
public:
    enum class PublicationVisibility : uint32_t {
        kNotVisible = 0,
        kVisible = 1,
        kIndeterminate = 2,
    };

    static Result<BroadcastPublicationView> Attach(
        const void* shm_base) noexcept;

    PublicationVisibility InspectPublication(
        uint64_t sequence, ShmHandle payload) const noexcept;

private:
    BroadcastPublicationView(const BroadcastChannel::ControlBlock* control,
                             const IndexSlot* slots,
                             uint64_t capacity) noexcept
        : control_(control), slots_(slots), mask_(capacity - 1) {}

    const BroadcastChannel::ControlBlock* control_ = nullptr;
    const IndexSlot* slots_ = nullptr;
    uint64_t mask_ = 0;
};

// Coordinates AllocationJournal orphan recovery with the concrete channel
// that owns each durable PublicationBinding. Registered channel views and their
// backing shared memory must outlive this coordinator.
class JournalChannelRecoveryCoordinator {
public:
    using IdentityProbe = AllocationJournal::IdentityProbe;

    explicit JournalChannelRecoveryCoordinator(
        AllocationJournal& journal) noexcept : journal_(&journal) {}

    // channel_id is a stable process-independent identity. Zero is reserved
    // for an absent/legacy binding and is rejected.
    Status RegisterChannel(uint64_t channel_id,
                           const SpscChannel& channel);
    Status RegisterChannel(uint64_t channel_id,
                           const MpscChannel& channel);
    Status RegisterChannel(uint64_t channel_id,
                           const BroadcastPublicationView& channel);

    // Uses AllocationJournal's tri-state liveness contract. kAlive and
    // kUnknown owners are never resolved; only kDead owners reach channel
    // inspection. Unknown channel IDs, type mismatches, and indeterminate
    // publication state are conservatively deferred.
    uint32_t RecoverOrphans(
        IdentityProbe identity_probe = nullptr,
        void* identity_probe_context = nullptr) noexcept;

private:
    struct Registration {
        uint64_t channel_id = 0;
        PublicationChannelKind channel_kind = PublicationChannelKind::kNone;
        const SpscChannel* spsc = nullptr;
        const MpscChannel* mpsc = nullptr;
        std::optional<BroadcastPublicationView> broadcast;
    };

    Status Register(Registration registration);
    const Registration* Find(uint64_t channel_id) const noexcept;

    static AllocationJournal::CommittedOrphanAction ResolveCommittedOrphan(
        const AllocationTransaction& transaction,
        const PublicationBinding& binding, void* context) noexcept;
    AllocationJournal::CommittedOrphanAction Resolve(
        const PublicationBinding& binding) const noexcept;

    AllocationJournal* journal_ = nullptr;
    std::vector<Registration> registrations_;
};

}  // namespace mino

#endif  // MINO_RUNTIME_JOURNAL_CHANNEL_RECOVERY_H_
