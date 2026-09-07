// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/deployment/shared_host_domain.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>

#include <signal.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include "mino/common/checked_arithmetic.h"
#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/platform/shared_memory.h"
#include "mino/runtime/allocation_journal.h"
#include "mino/runtime/journal_channel_recovery.h"
#include "mino/runtime/shm_shared_ptr.h"
#include "mino/shm/allocator/bitmap.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/allocator/class_table.h"
#include "mino/shm/allocator/slab_header.h"
#include "mino/shm/channel/broadcast_channel.h"
#include "mino/shm/channel/index_slot.h"
#include "mino/shm/channel/mpsc_channel.h"

namespace mino::deployment {
namespace {

constexpr uint64_t kCacheLine = 64;
constexpr uint64_t kMagic = 0x4D494E4F53484433ull;  // "MINOSHD3"
constexpr uint32_t kVersion = 3;
constexpr uint32_t kMaxTopicName = 63;
constexpr uint64_t kMinLeaseNs = 1'000'000ull;
constexpr uint64_t kMarkerSlackBytes = 256ull << 10;

constexpr uint32_t kPeerEmpty = 0;
constexpr uint32_t kPeerActive = 1;
constexpr uint32_t kTopicEmpty = 0;
constexpr uint32_t kTopicReady = 1;

constexpr uint64_t kLeaseStateMask = 0x3ull;
constexpr uint64_t kLeaseGenerationMask = 0x3fffffffull;
constexpr uint64_t kLeaseClaiming = 1;
constexpr uint64_t kLeaseActive = 2;

struct alignas(kCacheLine) EndpointLease {
    std::atomic<uint64_t> control{0};
    std::atomic<uint64_t> owner_node_id{0};
    std::atomic<uint64_t> owner_process_id{0};
    std::atomic<uint64_t> owner_process_epoch{0};
    std::atomic<uint64_t> owner_start_time_ns{0};
    unsigned char pad[24]{};
};
static_assert(sizeof(EndpointLease) == kCacheLine);
static_assert(std::is_standard_layout_v<EndpointLease>);

struct SchemaPod {
    std::array<std::byte, 32> digest{};
    uint64_t short_id = 0;
    uint32_t schema_version = 0;
    uint32_t layout_version = 0;
};
static_assert(std::is_trivially_copyable_v<SchemaPod>);

struct alignas(kCacheLine) PeerSlot {
    std::atomic<uint32_t> state{kPeerEmpty};
    uint32_t reserved0 = 0;
    uint64_t node_id = 0;
    ProcessIdentity identity{};
    std::atomic<uint64_t> last_heartbeat_ns{0};
    EndpointLease lease;
    unsigned char pad[16]{};
};
static_assert(alignof(PeerSlot) == kCacheLine);
static_assert(std::is_standard_layout_v<PeerSlot>);

struct alignas(kCacheLine) TopicSlot {
    std::atomic<uint32_t> state{kTopicEmpty};
    uint32_t max_payload_bytes = 0;
    uint32_t capacity = 0;
    uint32_t max_subscribers = 0;
    uint32_t max_publishers = 0;
    uint32_t mode = 0;
    uint32_t queue_full_policy = 0;
    uint32_t sample_rate = 0;
    char name[64]{};
    SchemaPod schema{};
    uint64_t channel_offset = 0;
    uint64_t channel_extent = 0;
    uint64_t channel_id = 0;
    uint64_t reserved0 = 0;
    EndpointLease publishers[kSharedHostMaxPublishersPerTopic];
    EndpointLease subscriber;
};
static_assert(alignof(TopicSlot) == kCacheLine);
static_assert(std::is_standard_layout_v<TopicSlot>);

struct alignas(kCacheLine) DomainHeader {
    std::atomic<uint64_t> magic{0};
    uint32_t version = 0;
    uint32_t header_size = 0;
    uint64_t total_size = 0;
    uint32_t peer_slots = 0;
    uint32_t topic_slots = 0;
    uint32_t queue_depth = 0;
    uint32_t max_subscribers = 0;
    uint32_t max_publishers_per_topic = 0;
    uint32_t max_payload_bytes = 0;
    uint64_t peer_lease_ns = 0;
    uint64_t allocator_offset = 0;
    uint64_t allocator_extent = 0;
    uint64_t journal_offset = 0;
    uint64_t journal_extent = 0;
    uint64_t pins_offset = 0;
    uint64_t pins_extent = 0;
    uint64_t channels_offset = 0;
    uint64_t channels_extent = 0;
    uint64_t channel_extent = 0;
    std::atomic<uint64_t> publisher_sequence{0};
    uint64_t reserved1 = 0;
    EndpointLease directory_lock;
    PeerSlot peers[kSharedHostMaxPeerSlots];
    TopicSlot topics[kSharedHostMaxTopicSlots];
};
static_assert(alignof(DomainHeader) == kCacheLine);
static_assert(std::is_standard_layout_v<DomainHeader>);
static_assert(offsetof(DomainHeader, magic) == 0);

struct SegmentLayout {
    uint64_t total_size = 0;
    uint64_t allocator_offset = 0;
    uint64_t allocator_extent = 0;
    uint64_t journal_offset = 0;
    uint64_t journal_extent = 0;
    uint64_t pins_offset = 0;
    uint64_t pins_extent = 0;
    uint64_t channels_offset = 0;
    uint64_t channels_extent = 0;
    uint64_t channel_extent = 0;
    uint32_t allocator_slot_count = 0;
    uint32_t journal_capacity = 0;
};

struct DomainState {
    std::optional<SharedMemorySegment> segment;
    std::optional<CentralSlabAllocator> allocator;
    std::optional<AllocationJournal> journal;
    std::optional<ShmPinTable> pins;
    DomainHeader* header = nullptr;
    std::string name;
    std::optional<uint32_t> peer_index;
    uint64_t peer_token = 0;
};

using ChannelVariant = std::variant<BroadcastChannel, MpscChannel>;

uint64_t MonotonicNowNs() noexcept {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

uint64_t HostPageSize() {
    const long page = ::sysconf(_SC_PAGESIZE);
    return page > 0 ? static_cast<uint64_t>(page) : 4096;
}

bool CheckedAddU64(uint64_t a, uint64_t b, uint64_t* out) noexcept {
    if (a > std::numeric_limits<uint64_t>::max() - b) return false;
    *out = a + b;
    return true;
}

bool CheckedMulU64(uint64_t a, uint64_t b, uint64_t* out) noexcept {
    if (a != 0 && b > std::numeric_limits<uint64_t>::max() / a) return false;
    *out = a * b;
    return true;
}

bool CheckedAlignUpU64(uint64_t value, uint64_t align, uint64_t* out) noexcept {
    if (align == 0 || (align & (align - 1)) != 0) return false;
    const uint64_t mask = align - 1;
    if (value > std::numeric_limits<uint64_t>::max() - mask) return false;
    *out = (value + mask) & ~mask;
    return true;
}

Status ValidateName(std::string_view name) {
    if (name.size() < 2 || name.front() != '/') {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm name must begin with '/' and a token");
    }
    if (name.find('/', 1) != std::string_view::npos) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm name must not contain additional '/'");
    }
    return Status::Ok();
}

Status ValidateTopic(std::string_view topic) {
    if (topic.empty() || topic.size() > kMaxTopicName) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "topic name must be 1..63 bytes");
    }
    if (topic.find('\0') != std::string_view::npos) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "topic name must not contain a NUL");
    }
    return Status::Ok();
}

Status ValidateTopicOptions(const SharedHostTopicOptions& options,
                            uint32_t queue_depth) {
    if (options.mode != SharedHostTopicMode::kBroadcast &&
        options.mode != SharedHostTopicMode::kMpsc) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "unsupported SharedHostTopicMode");
    }
    if (options.mode == SharedHostTopicMode::kMpsc && queue_depth < 64) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "MPSC topics require queue_depth >= 64");
    }
    if (options.sample_rate == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "sample_rate must be non-zero");
    }
    switch (options.queue_full_policy) {
        case QueueFullPolicy::kBlock:
        case QueueFullPolicy::kFail:
        case QueueFullPolicy::kDropNewest:
        case QueueFullPolicy::kDropOldest:
        case QueueFullPolicy::kSample:
            break;
        default:
            return Status::Error(StatusCode::kInvalidArgument,
                                 "unsupported QueueFullPolicy");
    }
    return Status::Ok();
}

Result<SharedHostDomainOptions> NormalizeOptions(
    SharedHostDomainOptions options) {
    if (options.peer_slots == 0 ||
        options.peer_slots > kSharedHostMaxPeerSlots) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "peer_slots must be in [1, 64]");
    }
    if (options.topic_slots == 0 ||
        options.topic_slots > kSharedHostMaxTopicSlots) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "topic_slots must be in [1, 64]");
    }
    if (options.queue_depth < 2 ||
        options.queue_depth > kSharedHostMaxQueueDepth ||
        (options.queue_depth & (options.queue_depth - 1)) != 0) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "queue_depth must be a power of two in [2, 1024]");
    }
    if (options.max_subscribers == 0 ||
        options.max_subscribers > kSharedHostMaxSubscribers) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "max_subscribers is out of range");
    }
    if (options.max_publishers_per_topic == 0 ||
        options.max_publishers_per_topic > kSharedHostMaxPublishersPerTopic) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "max_publishers_per_topic must be in [1, 16]");
    }
    if (options.max_payload_bytes == 0 ||
        options.max_payload_bytes > kSharedHostMaxPayloadBytes) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "max_payload_bytes must be in [1, 1 MiB]");
    }
    if (options.peer_lease_ns < kMinLeaseNs) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "peer_lease_ns must be at least 1 ms");
    }
    return options;
}

Result<uint64_t> AllocatorExtent(uint32_t slot_size, uint32_t slot_count) {
    ClassTableConfig config;
    config.classes = {{.slot_size = slot_size, .slot_count = slot_count}};
    MINO_ASSIGN_OR_RETURN(ClassTable table, ClassTable::Create(config));
    const uint32_t total_slots = table.total_slot_count();
    const uint32_t class_count = table.class_count();
    const uint32_t bitmap_words =
        (total_slots + kBitmapShardBits - 1) / kBitmapShardBits;

    uint64_t off = CentralSlabAllocator::kMetadataHeaderSize;
    uint64_t step = 0;
    if (!CheckedMulU64(sizeof(ClassDescriptor), class_count, &step) ||
        !CheckedAddU64(off, step, &off) ||
        !CheckedAlignUpU64(off, alignof(std::atomic<uint64_t>), &off) ||
        !CheckedMulU64(sizeof(std::atomic<uint64_t>), bitmap_words, &step) ||
        !CheckedAddU64(off, step, &off) ||
        !CheckedAlignUpU64(off, alignof(std::atomic<uint32_t>), &off) ||
        !CheckedMulU64(sizeof(std::atomic<uint32_t>), total_slots, &step) ||
        !CheckedAddU64(off, step, &off) ||
        !CheckedMulU64(sizeof(std::atomic<uint32_t>), class_count, &step) ||
        !CheckedAddU64(off, step, &off)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator metadata overflows");
    }
    uint64_t stride = 0;
    if (!CheckedAddU64(sizeof(SlabHeader), table.max_object_size(), &stride) ||
        !CheckedAlignUpU64(stride, alignof(SlabHeader), &stride)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator slot stride overflows");
    }
    uint64_t metadata = 0;
    uint64_t slot_bytes = 0;
    uint64_t extent = 0;
    if (!CheckedAlignUpU64(off, alignof(SlabHeader), &metadata) ||
        !CheckedMulU64(stride, total_slots, &slot_bytes) ||
        !CheckedAddU64(metadata, slot_bytes, &extent) ||
        !CheckedAlignUpU64(extent, kCacheLine, &extent)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "allocator extent overflows");
    }
    return extent;
}

Result<SegmentLayout> ComputeLayout(const SharedHostDomainOptions& options) {
    uint64_t journal_capacity = 0;
    uint64_t allocator_slots = 0;
    if (!CheckedMulU64(options.topic_slots, options.max_publishers_per_topic,
                       &journal_capacity) ||
        !CheckedMulU64(options.topic_slots, options.queue_depth,
                       &allocator_slots) ||
        !CheckedAddU64(allocator_slots, journal_capacity, &allocator_slots) ||
        journal_capacity > std::numeric_limits<uint32_t>::max() ||
        allocator_slots > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SharedHostDomain recovery capacity overflows");
    }

    SegmentLayout layout;
    layout.journal_capacity = static_cast<uint32_t>(journal_capacity);
    layout.allocator_slot_count = static_cast<uint32_t>(allocator_slots);
    MINO_ASSIGN_OR_RETURN(
        layout.allocator_extent,
        AllocatorExtent(options.max_payload_bytes, layout.allocator_slot_count));
    layout.journal_extent = AllocationJournal::RequiredSize(
        layout.journal_capacity, /*handles_per_transaction=*/1);
    layout.pins_extent = ShmPinTable::RequiredSize();
    layout.channel_extent = BroadcastChannel::RequiredSize(options.queue_depth);
    if (options.queue_depth >= 64) {
        layout.channel_extent =
            std::max(layout.channel_extent,
                     MpscChannel::RequiredSize(options.queue_depth));
    }
    if (!CheckedAlignUpU64(layout.channel_extent, kCacheLine,
                           &layout.channel_extent)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel extent overflows");
    }

    layout.allocator_offset = sizeof(DomainHeader);
    uint64_t next = 0;
    auto append = [&](uint64_t offset, uint64_t extent,
                      uint64_t* following) -> bool {
        return CheckedAddU64(offset, extent, &next) &&
               CheckedAlignUpU64(next, kCacheLine, following);
    };
    if (!append(layout.allocator_offset, layout.allocator_extent,
                &layout.journal_offset) ||
        !append(layout.journal_offset, layout.journal_extent,
                &layout.pins_offset) ||
        !append(layout.pins_offset, layout.pins_extent,
                &layout.channels_offset) ||
        !CheckedMulU64(layout.channel_extent, options.topic_slots,
                       &layout.channels_extent) ||
        !CheckedAddU64(layout.channels_offset, layout.channels_extent, &next) ||
        !CheckedAlignUpU64(next, HostPageSize(), &layout.total_size)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SharedHostDomain layout overflows");
    }
    return layout;
}

Status CheckShmBudget(uint64_t data_bytes) {
    uint64_t needed = 0;
    if (!CheckedAddU64(data_bytes, kMarkerSlackBytes, &needed)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shared-memory size overflows");
    }
    struct statvfs vfs;
    if (::statvfs("/dev/shm", &vfs) != 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot query /dev/shm capacity");
    }
    const uint64_t block = vfs.f_frsize != 0 ? vfs.f_frsize : vfs.f_bsize;
    uint64_t available = 0;
    if (block != 0 &&
        vfs.f_bavail <= std::numeric_limits<uint64_t>::max() / block) {
        available = static_cast<uint64_t>(vfs.f_bavail) * block;
    }
    if (needed > available) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "requested SHM segment exceeds free /dev/shm");
    }
    return Status::Ok();
}

std::byte* BytesOf(SharedMemorySegment& segment) {
    return static_cast<std::byte*>(segment.base());
}

std::byte* BytesOf(DomainState& state) {
    return BytesOf(*state.segment);
}

SchemaPod ToPod(const schema::SchemaIdentity& schema) {
    return SchemaPod{
        .digest = schema.canonical_digest(),
        .short_id = schema.short_id(),
        .schema_version = schema.schema_version(),
        .layout_version = schema.layout_version(),
    };
}

schema::SchemaIdentity FromPod(const SchemaPod& pod) {
    return schema::SchemaIdentity(pod.short_id, pod.digest, pod.schema_version,
                                  pod.layout_version);
}

bool SameSchema(const SchemaPod& lhs, const SchemaPod& rhs) noexcept {
    return lhs.short_id == rhs.short_id &&
           lhs.schema_version == rhs.schema_version &&
           lhs.layout_version == rhs.layout_version &&
           std::memcmp(lhs.digest.data(), rhs.digest.data(),
                       lhs.digest.size()) == 0;
}

bool TopicEquals(const TopicSlot& slot, std::string_view topic) {
    return std::strncmp(slot.name, topic.data(), topic.size()) == 0 &&
           slot.name[topic.size()] == '\0';
}

void WriteTopicName(TopicSlot* slot, std::string_view topic) {
    std::memset(slot->name, 0, sizeof(slot->name));
    std::memcpy(slot->name, topic.data(), topic.size());
}

bool SameTopicConfiguration(const TopicSlot& slot,
                            const SharedHostTopicOptions& options,
                            const SchemaPod& schema,
                            const DomainHeader& header) noexcept {
    return SameSchema(slot.schema, schema) &&
           slot.mode == static_cast<uint32_t>(options.mode) &&
           slot.queue_full_policy ==
               static_cast<uint32_t>(options.queue_full_policy) &&
           slot.sample_rate == options.sample_rate &&
           slot.capacity == header.queue_depth &&
           slot.max_subscribers == header.max_subscribers &&
           slot.max_publishers == header.max_publishers_per_topic &&
           slot.max_payload_bytes == header.max_payload_bytes;
}

ProcessIdentity LoadOwner(const EndpointLease& lease) noexcept {
    return ProcessIdentity{
        .node_id = lease.owner_node_id.load(std::memory_order_acquire),
        .process_id = lease.owner_process_id.load(std::memory_order_acquire),
        .process_epoch =
            lease.owner_process_epoch.load(std::memory_order_acquire),
        .start_time_ns =
            lease.owner_start_time_ns.load(std::memory_order_acquire),
    };
}

void StoreOwner(EndpointLease& lease, const ProcessIdentity& owner) noexcept {
    lease.owner_node_id.store(owner.node_id, std::memory_order_relaxed);
    lease.owner_process_id.store(owner.process_id, std::memory_order_relaxed);
    lease.owner_process_epoch.store(owner.process_epoch,
                                    std::memory_order_relaxed);
    lease.owner_start_time_ns.store(owner.start_time_ns,
                                    std::memory_order_relaxed);
}

uint64_t LeaseToken(uint32_t pid, uint64_t generation,
                    uint64_t state) noexcept {
    return (static_cast<uint64_t>(pid) << 32) |
           ((generation & kLeaseGenerationMask) << 2) | state;
}

bool LeaseIsActive(const EndpointLease& lease) noexcept {
    const uint64_t control = lease.control.load(std::memory_order_acquire);
    return (control & kLeaseStateMask) == kLeaseActive;
}

bool RecoverLease(EndpointLease& lease,
                  ProcessIdentity* dead_owner = nullptr) noexcept {
    uint64_t observed = lease.control.load(std::memory_order_acquire);
    if (observed == 0) return false;
    const uint64_t state = observed & kLeaseStateMask;
    if (state != kLeaseClaiming && state != kLeaseActive) return false;
    const uint32_t pid = static_cast<uint32_t>(observed >> 32);
    const ProcessIdentity owner = LoadOwner(lease);
    bool dead = false;
    if (!owner.IsZero() && owner.process_id == pid) {
        dead = ProbeProcessIdentity(owner) == ProcessIdentityLiveness::kDead;
    } else if (pid != 0) {
        errno = 0;
        const int rc = ::kill(static_cast<pid_t>(pid), 0);
        dead = rc != 0 && errno == ESRCH;
    }
    if (!dead) return false;
    const uint64_t generation = (observed >> 2) & kLeaseGenerationMask;
    const uint64_t cleared = LeaseToken(0, generation + 1, 0);
    if (!lease.control.compare_exchange_strong(observed, cleared,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
        return false;
    }
    if (dead_owner != nullptr) *dead_owner = owner;
    return true;
}

Result<uint64_t> TryClaimLease(EndpointLease& lease) {
    const ProcessIdentity owner = ProcessIdentity::Current();
    const uint32_t pid = static_cast<uint32_t>(owner.process_id);
    for (;;) {
        uint64_t observed = lease.control.load(std::memory_order_acquire);
        const uint64_t state = observed & kLeaseStateMask;
        if (state == kLeaseActive || state == kLeaseClaiming) {
            (void)RecoverLease(lease);
            observed = lease.control.load(std::memory_order_acquire);
            if ((observed & kLeaseStateMask) != 0) {
                return Status::Error(StatusCode::kAlreadyExists,
                                     "endpoint lease is held");
            }
        }
        const uint64_t generation = (observed >> 2) & kLeaseGenerationMask;
        const uint64_t claiming = LeaseToken(pid, generation, kLeaseClaiming);
        if (!lease.control.compare_exchange_weak(observed, claiming,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
            continue;
        }
        StoreOwner(lease, owner);
        const uint64_t active = LeaseToken(pid, generation, kLeaseActive);
        lease.control.store(active, std::memory_order_release);
        return active;
    }
}

Result<uint64_t> ClaimLeaseUntil(EndpointLease& lease, Deadline deadline) {
    for (;;) {
        Result<uint64_t> claimed = TryClaimLease(lease);
        if (claimed.ok()) return *claimed;
        if (claimed.status().code() != StatusCode::kAlreadyExists) {
            return claimed.status();
        }
        if (deadline.expired()) {
            return Status::Error(StatusCode::kTimeout,
                                 "timed out claiming endpoint lease");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void ReleaseLease(EndpointLease* lease, uint64_t token) noexcept {
    if (lease == nullptr || token == 0) return;
    uint64_t expected = token;
    const uint64_t generation = (token >> 2) & kLeaseGenerationMask;
    const uint64_t cleared = LeaseToken(0, generation + 1, 0);
    (void)lease->control.compare_exchange_strong(expected, cleared,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire);
}

struct LeaseGuard {
    LeaseGuard(EndpointLease* lease, uint64_t token) noexcept
        : lease_(lease), token_(token) {}
    ~LeaseGuard() { ReleaseLease(lease_, token_); }
    LeaseGuard(const LeaseGuard&) = delete;
    LeaseGuard& operator=(const LeaseGuard&) = delete;
    void Disarm() noexcept {
        lease_ = nullptr;
        token_ = 0;
    }
    EndpointLease* lease_;
    uint64_t token_;
};

Status WaitForMagic(DomainHeader* header, Deadline deadline) {
    while (header->magic.load(std::memory_order_acquire) != kMagic) {
        if (deadline.expired()) {
            return Status::Error(
                StatusCode::kTimeout,
                "timed out waiting for SharedHostDomain Create");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return Status::Ok();
}

Status ValidateHeader(const SharedMemorySegment& segment,
                      const DomainHeader& header) {
    if (header.version != kVersion ||
        header.header_size != sizeof(DomainHeader)) {
        return Status::Error(StatusCode::kCorruption,
                             "SharedHostDomain header version/size mismatch");
    }
    if (header.total_size != segment.size() || header.peer_slots == 0 ||
        header.peer_slots > kSharedHostMaxPeerSlots ||
        header.topic_slots == 0 ||
        header.topic_slots > kSharedHostMaxTopicSlots ||
        header.queue_depth < 2 ||
        (header.queue_depth & (header.queue_depth - 1)) != 0 ||
        header.max_subscribers == 0 || header.max_payload_bytes == 0 ||
        header.max_publishers_per_topic == 0 ||
        header.max_publishers_per_topic > kSharedHostMaxPublishersPerTopic) {
        return Status::Error(StatusCode::kCorruption,
                             "SharedHostDomain header fields are invalid");
    }
    SharedHostDomainOptions options;
    options.peer_slots = header.peer_slots;
    options.topic_slots = header.topic_slots;
    options.queue_depth = header.queue_depth;
    options.max_subscribers = header.max_subscribers;
    options.max_publishers_per_topic = header.max_publishers_per_topic;
    options.max_payload_bytes = header.max_payload_bytes;
    options.peer_lease_ns = header.peer_lease_ns;
    MINO_ASSIGN_OR_RETURN(SharedHostDomainOptions normalized,
                          NormalizeOptions(options));
    MINO_ASSIGN_OR_RETURN(SegmentLayout expected, ComputeLayout(normalized));
    if (header.allocator_offset != expected.allocator_offset ||
        header.allocator_extent != expected.allocator_extent ||
        header.journal_offset != expected.journal_offset ||
        header.journal_extent != expected.journal_extent ||
        header.pins_offset != expected.pins_offset ||
        header.pins_extent != expected.pins_extent ||
        header.channels_offset != expected.channels_offset ||
        header.channels_extent != expected.channels_extent ||
        header.channel_extent != expected.channel_extent ||
        header.total_size < expected.total_size) {
        return Status::Error(StatusCode::kCorruption,
                             "SharedHostDomain layout offsets mismatch");
    }
    return Status::Ok();
}

void ClearPeer(PeerSlot& peer) noexcept {
    peer.state.store(kPeerEmpty, std::memory_order_release);
    peer.node_id = 0;
    peer.identity = {};
    peer.last_heartbeat_ns.store(0, std::memory_order_release);
}

uint32_t CountActivePublishers(const TopicSlot& topic) noexcept {
    uint32_t count = 0;
    const uint32_t limit =
        std::min(topic.max_publishers, kSharedHostMaxPublishersPerTopic);
    for (uint32_t i = 0; i < limit; ++i) {
        if (LeaseIsActive(topic.publishers[i])) ++count;
    }
    return count;
}

Result<ChannelVariant> AttachTopicChannel(
    const std::shared_ptr<DomainState>& state, const TopicSlot& slot) {
    if (state == nullptr || !state->pins.has_value()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SharedHostDomain pin table is not bound");
    }
    std::byte* base = BytesOf(*state);
    void* channel_base = base + slot.channel_offset;
    const auto mode = static_cast<SharedHostTopicMode>(slot.mode);
    if (mode == SharedHostTopicMode::kMpsc) {
        MINO_ASSIGN_OR_RETURN(MpscChannel channel,
                              MpscChannel::Attach(channel_base));
        if (channel.capacity() != slot.capacity) {
            return Status::Error(StatusCode::kCorruption,
                                 "MPSC capacity mismatch");
        }
        channel.SetPayloadRetireObserver(&ShmPinTable::RetirePayloadCallback,
                                         &*state->pins);
        return ChannelVariant(std::move(channel));
    }
    MINO_ASSIGN_OR_RETURN(BroadcastChannel channel,
                          BroadcastChannel::Attach(channel_base));
    if (channel.capacity() != slot.capacity) {
        return Status::Error(StatusCode::kCorruption,
                             "Broadcast capacity mismatch");
    }
    channel.SetPayloadRetireObserver(&ShmPinTable::RetirePayloadCallback,
                                     &*state->pins);
    return ChannelVariant(std::move(channel));
}

Status RecoverState(const std::shared_ptr<DomainState>& state) {
    if (state == nullptr || state->header == nullptr ||
        !state->segment.has_value() || !state->allocator.has_value() ||
        !state->journal.has_value() || !state->pins.has_value()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SharedHostDomain state is not bound");
    }
    DomainHeader* header = state->header;
    (void)RecoverLease(header->directory_lock);
    const uint64_t now = MonotonicNowNs();
    for (uint32_t i = 0; i < header->peer_slots; ++i) {
        PeerSlot& peer = header->peers[i];
        if (peer.state.load(std::memory_order_acquire) != kPeerActive) continue;
        ProcessIdentity dead_peer;
        if (RecoverLease(peer.lease, &dead_peer)) {
            if (!dead_peer.IsZero()) {
                (void)state->pins->CleanupOwner(dead_peer);
            }
            ClearPeer(peer);
            continue;
        }
        const ProcessIdentity owner = LoadOwner(peer.lease);
        if (!owner.IsZero() &&
            ProbeProcessIdentity(owner) == ProcessIdentityLiveness::kDead) {
            if (RecoverLease(peer.lease, &dead_peer) && !dead_peer.IsZero()) {
                (void)state->pins->CleanupOwner(dead_peer);
            }
            ClearPeer(peer);
            continue;
        }
        const uint64_t heartbeat =
            peer.last_heartbeat_ns.load(std::memory_order_acquire);
        if (heartbeat != 0 && now > heartbeat &&
            now - heartbeat > header->peer_lease_ns && !owner.IsZero() &&
            ProbeProcessIdentity(owner) == ProcessIdentityLiveness::kDead) {
            if (RecoverLease(peer.lease, &dead_peer) && !dead_peer.IsZero()) {
                (void)state->pins->CleanupOwner(dead_peer);
            }
            ClearPeer(peer);
        }
    }

    std::optional<MpscChannel> mpsc[kSharedHostMaxTopicSlots];
    std::optional<BroadcastPublicationView> broadcast[kSharedHostMaxTopicSlots];
    JournalChannelRecoveryCoordinator journal_recovery(*state->journal);

    for (uint32_t i = 0; i < header->topic_slots; ++i) {
        TopicSlot& topic = header->topics[i];
        if (topic.state.load(std::memory_order_acquire) != kTopicReady) continue;
        const uint32_t pub_limit =
            std::min(topic.max_publishers, kSharedHostMaxPublishersPerTopic);
        for (uint32_t p = 0; p < pub_limit; ++p) {
            (void)RecoverLease(topic.publishers[p]);
        }
        ProcessIdentity dead_subscriber;
        if (RecoverLease(topic.subscriber, &dead_subscriber) &&
            !dead_subscriber.IsZero()) {
            (void)state->pins->CleanupOwner(dead_subscriber);
        }

        MINO_ASSIGN_OR_RETURN(ChannelVariant channel,
                              AttachTopicChannel(state, topic));
        if (static_cast<SharedHostTopicMode>(topic.mode) ==
            SharedHostTopicMode::kMpsc) {
            mpsc[i] = std::get<MpscChannel>(channel);
            (void)mpsc[i]->AbortOrphanedReservations(now);
            MINO_RETURN_IF_ERROR(
                journal_recovery.RegisterChannel(topic.channel_id, *mpsc[i]));
        } else {
            auto& broadcast_channel = std::get<BroadcastChannel>(channel);
            // Require proven-dead ProcessIdentity (SimpleNode pattern): do not
            // heartbeat-evict a live slow subscriber under ASAN/clock quirks.
            (void)broadcast_channel.EvictStaleSubscribers(
                now, header->peer_lease_ns, /*require_dead_owner=*/true);
            std::byte* base = BytesOf(*state);
            MINO_ASSIGN_OR_RETURN(
                broadcast[i],
                BroadcastPublicationView::Attach(base + topic.channel_offset));
            MINO_RETURN_IF_ERROR(journal_recovery.RegisterChannel(
                topic.channel_id, *broadcast[i]));
        }
    }
    (void)journal_recovery.RecoverOrphans();
    return Status::Ok();
}

Status EnsureJoined(const std::shared_ptr<DomainState>& state) {
    if (!state->peer_index.has_value()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SharedHostDomain peer has not Join()'d");
    }
    return Status::Ok();
}

Status InitializeTopic(const std::shared_ptr<DomainState>& state,
                       TopicSlot& slot, std::string_view topic,
                       const SchemaPod& schema,
                       const SharedHostTopicOptions& options) {
    DomainHeader* header = state->header;
    std::byte* base = BytesOf(*state);
    std::memset(base + slot.channel_offset, 0,
                static_cast<size_t>(slot.channel_extent));
    if (options.mode == SharedHostTopicMode::kMpsc) {
        MINO_RETURN_IF_ERROR(
            MpscChannel::Init(base + slot.channel_offset, header->queue_depth)
                .status());
    } else {
        MINO_RETURN_IF_ERROR(
            BroadcastChannel::Init(base + slot.channel_offset,
                                   header->queue_depth)
                .status());
    }
    WriteTopicName(&slot, topic);
    slot.schema = schema;
    slot.mode = static_cast<uint32_t>(options.mode);
    slot.queue_full_policy =
        static_cast<uint32_t>(options.queue_full_policy);
    slot.sample_rate = options.sample_rate;
    slot.max_payload_bytes = header->max_payload_bytes;
    slot.capacity = header->queue_depth;
    slot.max_subscribers = header->max_subscribers;
    slot.max_publishers = header->max_publishers_per_topic;
    slot.state.store(kTopicReady, std::memory_order_release);
    return Status::Ok();
}

void FillIndexSlot(IndexSlot* slot, const TopicSlot& topic, ShmHandle handle,
                   uint32_t payload_len) {
    slot->msg_type = static_cast<uint32_t>(topic.schema.short_id & 0xffffffffu);
    slot->schema_version = topic.schema.schema_version;
    slot->schema_short_id = topic.schema.short_id;
    slot->schema_layout_version = topic.schema.layout_version;
    slot->reserved0 = 0;
    slot->timestamp_ns = MonotonicNowNs();
    slot->payload = handle;
    slot->payload_len = payload_len;
    slot->flags = 0;
}

}  // namespace

struct SharedHostPublisher::Impl {
    std::shared_ptr<DomainState> state;
    TopicSlot* slot = nullptr;
    EndpointLease* lease = nullptr;
    uint64_t token = 0;
    uint64_t channel_id = 0;
    std::optional<ChannelVariant> channel;
    SharedHostTopicOptions options{};
    MpscChannel::ProducerIdentity mpsc_identity{};

    ~Impl() { ReleaseLease(lease, token); }
};

struct SharedHostSubscriber::Impl {
    std::shared_ptr<DomainState> state;
    TopicSlot* slot = nullptr;
    std::optional<ChannelVariant> channel;
    SharedHostTopicMode mode = SharedHostTopicMode::kBroadcast;
    BroadcastChannel::SubscriberHandle broadcast_handle{};
    EndpointLease* subscriber_lease = nullptr;
    uint64_t subscriber_token = 0;
    ProcessIdentity owner{};
    std::atomic<bool> borrow_active{false};

    ~Impl() {
        if (!channel.has_value()) return;
        if (mode == SharedHostTopicMode::kBroadcast) {
            if (auto* broadcast = std::get_if<BroadcastChannel>(&*channel)) {
                if (broadcast_handle.generation != 0) {
                    (void)broadcast->UnregisterSubscriber(
                        broadcast_handle.id, broadcast_handle.generation);
                }
            }
        } else {
            ReleaseLease(subscriber_lease, subscriber_token);
        }
    }
};

struct SharedHostBorrowedBytes::Impl {
    std::shared_ptr<DomainState> state;
    std::atomic<bool>* borrow_active = nullptr;
    std::variant<BroadcastChannel::Borrow, MpscChannel::Borrow> borrow;
    ShmPinToken pin;
    const std::byte* data = nullptr;
    uint32_t size = 0;
    ShmHandle handle{};
    bool payload_cleanup_by_channel = false;
    bool active = false;
};

struct SharedHostDomain::Impl {
    std::shared_ptr<DomainState> state;
};

SharedHostBorrowedBytes::SharedHostBorrowedBytes() noexcept = default;
SharedHostBorrowedBytes::SharedHostBorrowedBytes(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SharedHostBorrowedBytes::SharedHostBorrowedBytes(
    SharedHostBorrowedBytes&& other) noexcept = default;
SharedHostBorrowedBytes& SharedHostBorrowedBytes::operator=(
    SharedHostBorrowedBytes&& other) noexcept {
    if (this != &other) {
        if (impl_ != nullptr && impl_->active) {
            (void)std::move(*this).Release();
        }
        impl_ = std::move(other.impl_);
    }
    return *this;
}
SharedHostBorrowedBytes::~SharedHostBorrowedBytes() {
    if (impl_ != nullptr && impl_->active) {
        (void)std::move(*this).Release();
    }
}

bool SharedHostBorrowedBytes::active() const noexcept {
    return impl_ != nullptr && impl_->active;
}

std::span<const std::byte> SharedHostBorrowedBytes::bytes() const noexcept {
    if (impl_ == nullptr || !impl_->active || impl_->data == nullptr) {
        return {};
    }
    return {impl_->data, impl_->size};
}

Status SharedHostBorrowedBytes::Release() && noexcept {
    if (impl_ == nullptr || !impl_->active) return Status::Ok();
    impl_->active = false;
    Status ack = Status::Ok();
    std::visit([&](auto& borrow) { ack = std::move(borrow).Ack(); },
               impl_->borrow);
    Status retire = Status::Ok();
    if (ack.ok() && !impl_->payload_cleanup_by_channel &&
        impl_->state != nullptr && impl_->state->pins.has_value() &&
        !impl_->handle.IsNull()) {
        retire = impl_->state->pins->RetirePayload(impl_->handle);
    }
    const Status release_pin = impl_->pin.Release();
    impl_->data = nullptr;
    if (impl_->borrow_active != nullptr) {
        impl_->borrow_active->store(false, std::memory_order_release);
        impl_->borrow_active = nullptr;
    }
    impl_->state.reset();
    impl_.reset();
    if (!ack.ok()) return ack;
    if (!retire.ok()) return retire;
    return release_pin;
}

SharedHostPublisher::SharedHostPublisher(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SharedHostPublisher::SharedHostPublisher(SharedHostPublisher&&) noexcept =
    default;
SharedHostPublisher& SharedHostPublisher::operator=(
    SharedHostPublisher&&) noexcept = default;
SharedHostPublisher::~SharedHostPublisher() = default;

bool SharedHostPublisher::active() const noexcept {
    return impl_ != nullptr && impl_->slot != nullptr && impl_->token != 0;
}

SharedHostTopicMode SharedHostPublisher::mode() const noexcept {
    return impl_ == nullptr ? SharedHostTopicMode::kBroadcast
                            : impl_->options.mode;
}

Status SharedHostPublisher::Publish(std::span<const std::byte> payload) {
    return Publish(payload, Deadline::FromNow(std::chrono::seconds(5)));
}

Status SharedHostPublisher::PublishTyped(std::span<const std::byte> payload,
                                         Deadline deadline) {
    return Publish(payload, deadline);
}

Status SharedHostPublisher::Publish(std::span<const std::byte> payload,
                                    Deadline deadline) {
    if (!active()) {
        return Status::Error(StatusCode::kUnavailable, "publisher is closed");
    }
    if (payload.empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "payload must not be empty");
    }
    if (payload.size() > impl_->slot->max_payload_bytes) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "payload exceeds topic max_payload_bytes");
    }
    if (deadline.expired()) {
        return Status::Error(StatusCode::kTimeout, "publish deadline expired");
    }
    const std::shared_ptr<DomainState> state = impl_->state;
    MINO_RETURN_IF_ERROR(RecoverState(state));

    AllocationJournal& journal = *state->journal;
    const ProcessIdentity owner = ProcessIdentity::Current();
    MINO_ASSIGN_OR_RETURN(AllocationTransaction transaction,
                          journal.Begin(owner));
    AllocationRequest request;
    request.object_size = static_cast<uint32_t>(payload.size());
    request.type_id = TypeId{
        static_cast<uint32_t>(impl_->slot->schema.short_id & 0xffffffffu)};
    request.schema = SchemaIdentity{
        .short_id = impl_->slot->schema.short_id,
        .layout_version = impl_->slot->schema.layout_version,
    };
    request.alignment = 1;
    Result<ShmHandle> allocated = journal.AllocateRoot(transaction, request);
    if (!allocated.ok()) {
        (void)journal.Abort(transaction);
        return allocated.status();
    }
    const ShmHandle handle = *allocated;
    Result<MutableBuildView> build = state->allocator->BeginBuild(handle);
    if (!build.ok()) {
        (void)journal.Abort(transaction);
        return build.status();
    }
    if (build->data == nullptr || build->object_size != payload.size() ||
        build->capacity < payload.size()) {
        (void)journal.Abort(transaction);
        return Status::Error(StatusCode::kCorruption,
                             "allocator returned an invalid payload view");
    }
    std::memcpy(build->data, payload.data(), payload.size());

    auto finish = [&](auto reservation, PublicationChannelKind kind,
                      auto& channel) -> Status {
        if (!reservation.ok()) {
            (void)journal.Abort(transaction);
            return reservation.status();
        }
        const Status published = journal.PublishGraph(transaction);
        if (!published.ok()) {
            (void)journal.Abort(transaction);
            return published;
        }
        FillIndexSlot(reservation->slot(), *impl_->slot, handle,
                      static_cast<uint32_t>(payload.size()));
        const uint64_t sequence =
            reservation->slot()->sequence_num.load(std::memory_order_relaxed);
        const PublicationBinding binding{
            .channel_kind = kind,
            .channel_id = impl_->channel_id,
            .sequence = sequence,
            .payload = handle,
        };
        const Status journal_commit = journal.Commit(transaction, binding);
        if (!journal_commit.ok()) {
            (void)journal.Abort(transaction);
            return journal_commit;
        }
        const Status channel_commit = std::move(*reservation).Commit();
        if (!channel_commit.ok()) {
            const Status rollback = journal.RollbackCommitted(transaction);
            return rollback.ok() ? channel_commit : rollback;
        }
        (void)journal.FinalizeCommit(transaction);
        if constexpr (std::is_same_v<std::remove_reference_t<decltype(channel)>,
                                     BroadcastChannel>) {
            channel.CollectGarbage();
        }
        return Status::Ok();
    };

    const auto policy = impl_->options.queue_full_policy;
    const uint32_t sample_rate = impl_->options.sample_rate;
    if (impl_->options.mode == SharedHostTopicMode::kMpsc) {
        auto& channel = std::get<MpscChannel>(*impl_->channel);
        Result<MpscChannel::Reservation> reservation =
            Status::Error(StatusCode::kWouldBlock, "not reserved");
        if (policy != QueueFullPolicy::kBlock &&
            policy != QueueFullPolicy::kSample) {
            reservation =
                channel.Reserve(impl_->mpsc_identity, policy, sample_rate);
        } else {
            for (;;) {
                reservation = channel.TryReserve(impl_->mpsc_identity);
                if (reservation.ok()) break;
                const StatusCode code = reservation.status().code();
                if (code != StatusCode::kWouldBlock &&
                    code != StatusCode::kResourceExhausted) {
                    break;
                }
                if (policy == QueueFullPolicy::kSample &&
                    channel.next_sequence() % sample_rate != 0) {
                    reservation = Status::Error(
                        StatusCode::kDegraded,
                        "MPSC queue full: message sampled out");
                    break;
                }
                (void)channel.AbortOrphanedReservations(MonotonicNowNs());
                if (deadline.expired()) break;
                std::this_thread::yield();
            }
            if (!reservation.ok() && deadline.expired() &&
                reservation.status().code() != StatusCode::kDegraded) {
                (void)journal.Abort(transaction);
                return Status::Error(StatusCode::kTimeout,
                                     "publish blocked until deadline");
            }
        }
        return finish(std::move(reservation), PublicationChannelKind::kMpsc,
                      channel);
    }

    auto& channel = std::get<BroadcastChannel>(*impl_->channel);
    Result<BroadcastChannel::Reservation> reservation =
        Status::Error(StatusCode::kWouldBlock, "not reserved");
    if (policy != QueueFullPolicy::kBlock &&
        policy != QueueFullPolicy::kSample) {
        reservation = channel.Reserve(policy, sample_rate);
    } else {
        for (;;) {
            reservation = channel.TryReserve();
            if (reservation.ok() ||
                reservation.status().code() != StatusCode::kWouldBlock) {
                break;
            }
            if (policy == QueueFullPolicy::kSample &&
                channel.next_sequence() % sample_rate != 0) {
                reservation = Status::Error(
                    StatusCode::kDegraded,
                    "Broadcast queue full: message sampled out");
                break;
            }
            if (deadline.expired()) break;
            std::this_thread::yield();
        }
        if (!reservation.ok() && deadline.expired() &&
            reservation.status().code() != StatusCode::kDegraded) {
            (void)journal.Abort(transaction);
            return Status::Error(StatusCode::kTimeout,
                                 "publish blocked until deadline");
        }
    }
    return finish(std::move(reservation), PublicationChannelKind::kBroadcast,
                  channel);
}

SharedHostSubscriber::SharedHostSubscriber(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SharedHostSubscriber::SharedHostSubscriber(SharedHostSubscriber&&) noexcept =
    default;
SharedHostSubscriber& SharedHostSubscriber::operator=(
    SharedHostSubscriber&&) noexcept = default;
SharedHostSubscriber::~SharedHostSubscriber() = default;

bool SharedHostSubscriber::active() const noexcept {
    return impl_ != nullptr && impl_->slot != nullptr;
}

SharedHostTopicMode SharedHostSubscriber::mode() const noexcept {
    return impl_ == nullptr ? SharedHostTopicMode::kBroadcast : impl_->mode;
}

Result<SharedHostBorrowedBytes> SharedHostSubscriber::TryPollBorrow() {
    if (!active()) {
        return Status::Error(StatusCode::kUnavailable, "subscriber is closed");
    }
    // Renew the broadcast lease before Recover so a concurrent coordinator
    // cannot treat this live subscriber as expired while we are polling.
    if (impl_->mode == SharedHostTopicMode::kBroadcast &&
        impl_->channel.has_value()) {
        auto& channel = std::get<BroadcastChannel>(*impl_->channel);
        const Status pre_hb =
            channel.Heartbeat(impl_->broadcast_handle, MonotonicNowNs());
        if (!pre_hb.ok()) return pre_hb;
    }
    MINO_RETURN_IF_ERROR(RecoverState(impl_->state));
    bool expected = false;
    if (!impl_->borrow_active.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "subscriber already has an active borrow");
    }

    auto fail = [&](Status status) -> Result<SharedHostBorrowedBytes> {
        impl_->borrow_active.store(false, std::memory_order_release);
        return status;
    };

    auto finish = [&](auto polled,
                      bool payload_cleanup_by_channel)
        -> Result<SharedHostBorrowedBytes> {
        if (!polled.ok()) return fail(polled.status());
        auto borrow = std::move(*polled);
        const IndexSlotSnapshot& snapshot = *borrow;
        if (snapshot.payload_len == 0 ||
            snapshot.payload_len > impl_->slot->max_payload_bytes ||
            snapshot.payload.IsNull() ||
            snapshot.schema_short_id != impl_->slot->schema.short_id ||
            snapshot.schema_version != impl_->slot->schema.schema_version ||
            snapshot.schema_layout_version !=
                impl_->slot->schema.layout_version) {
            const Status ack = std::move(borrow).Ack();
            if (ack.ok() && !payload_cleanup_by_channel) {
                (void)impl_->state->pins->RetirePayload(snapshot.payload);
            }
            return fail(Status::Error(StatusCode::kCorruption,
                                      "shared topic payload metadata invalid"));
        }
        const ShmPinContract contract{
            .type_id = TypeId{static_cast<uint32_t>(
                impl_->slot->schema.short_id & 0xffffffffu)},
            .schema_short_id = impl_->slot->schema.short_id,
            .layout_version = impl_->slot->schema.layout_version,
            .object_size = snapshot.payload_len,
        };
        Result<ShmPinToken> pin = impl_->state->pins->Pin(
            snapshot.payload, contract, impl_->owner);
        if (!pin.ok()) {
            const Status ack = std::move(borrow).Ack();
            if (ack.ok() && !payload_cleanup_by_channel) {
                (void)impl_->state->pins->RetirePayload(snapshot.payload);
            }
            return fail(pin.status());
        }
        auto borrowed = std::make_unique<SharedHostBorrowedBytes::Impl>();
        borrowed->state = impl_->state;
        borrowed->borrow_active = &impl_->borrow_active;
        borrowed->borrow = std::move(borrow);
        borrowed->data = static_cast<const std::byte*>(pin->data());
        borrowed->size = snapshot.payload_len;
        borrowed->handle = snapshot.payload;
        borrowed->payload_cleanup_by_channel = payload_cleanup_by_channel;
        borrowed->pin = std::move(*pin);
        borrowed->active = true;
        return SharedHostBorrowedBytes(std::move(borrowed));
    };

    if (impl_->mode == SharedHostTopicMode::kMpsc) {
        auto& channel = std::get<MpscChannel>(*impl_->channel);
        Result<MpscChannel::Borrow> polled = channel.Poll();
        if (!polled.ok() &&
            polled.status().code() == StatusCode::kWouldBlock) {
            (void)channel.AbortOrphanedReservations(MonotonicNowNs());
            polled = channel.Poll();
        }
        return finish(std::move(polled), false);
    }

    auto& channel = std::get<BroadcastChannel>(*impl_->channel);
    const Status heartbeat =
        channel.Heartbeat(impl_->broadcast_handle, MonotonicNowNs());
    if (!heartbeat.ok()) return fail(heartbeat);
    return finish(channel.Poll(impl_->broadcast_handle, impl_->owner), true);
}

Result<SharedHostBorrowedBytes> SharedHostSubscriber::PollBorrow(
    Deadline deadline) {
    for (;;) {
        Result<SharedHostBorrowedBytes> polled = TryPollBorrow();
        if (polled.ok()) return polled;
        if (polled.status().code() != StatusCode::kWouldBlock) {
            return polled.status();
        }
        if (deadline.expired()) {
            return Status::Error(StatusCode::kTimeout,
                                 "timed out polling SharedHostSubscriber");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

Result<std::vector<std::byte>> SharedHostSubscriber::TryPoll() {
    Result<SharedHostBorrowedBytes> borrowed = TryPollBorrow();
    if (!borrowed.ok()) return borrowed.status();
    const std::span<const std::byte> view = borrowed->bytes();
    std::vector<std::byte> out(view.begin(), view.end());
    MINO_RETURN_IF_ERROR(std::move(*borrowed).Release());
    return out;
}

Result<std::vector<std::byte>> SharedHostSubscriber::Poll(Deadline deadline) {
    for (;;) {
        Result<std::vector<std::byte>> polled = TryPoll();
        if (polled.ok()) return polled;
        if (polled.status().code() != StatusCode::kWouldBlock) {
            return polled.status();
        }
        if (deadline.expired()) {
            return Status::Error(StatusCode::kTimeout,
                                 "timed out polling SharedHostSubscriber");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

SharedHostDomain::SharedHostDomain(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SharedHostDomain::SharedHostDomain(SharedHostDomain&&) noexcept = default;
SharedHostDomain& SharedHostDomain::operator=(SharedHostDomain&&) noexcept =
    default;
SharedHostDomain::~SharedHostDomain() {
    if (impl_ != nullptr && impl_->state != nullptr) {
        (void)Leave();
    }
}

const std::string& SharedHostDomain::name() const noexcept {
    return impl_->state->name;
}

uint64_t SharedHostDomain::size_bytes() const noexcept {
    return impl_->state->segment->size();
}

Result<uint64_t> SharedHostDomain::RequiredBytes(
    const SharedHostDomainOptions& options) {
    MINO_ASSIGN_OR_RETURN(SharedHostDomainOptions normalized,
                          NormalizeOptions(options));
    MINO_ASSIGN_OR_RETURN(SegmentLayout layout, ComputeLayout(normalized));
    return layout.total_size;
}

Result<SharedHostDomain> SharedHostDomain::Create(
    std::string_view name, SharedHostDomainOptions options) {
    MINO_RETURN_IF_ERROR(ValidateName(name));
    MINO_ASSIGN_OR_RETURN(SharedHostDomainOptions normalized,
                          NormalizeOptions(options));
    MINO_ASSIGN_OR_RETURN(SegmentLayout layout, ComputeLayout(normalized));
    MINO_RETURN_IF_ERROR(CheckShmBudget(layout.total_size));

    SharedMemoryCreateOptions create;
    create.name = std::string(name);
    create.size = layout.total_size;
    create.use_huge_pages = false;
    MINO_ASSIGN_OR_RETURN(SharedMemorySegment segment,
                          SharedMemorySegment::Create(create));
    std::memset(segment.base(), 0, static_cast<size_t>(segment.size()));
    auto* header =
        std::construct_at(static_cast<DomainHeader*>(segment.base()));
    header->version = kVersion;
    header->header_size = sizeof(DomainHeader);
    header->total_size = segment.size();
    header->peer_slots = normalized.peer_slots;
    header->topic_slots = normalized.topic_slots;
    header->queue_depth = normalized.queue_depth;
    header->max_subscribers = normalized.max_subscribers;
    header->max_publishers_per_topic = normalized.max_publishers_per_topic;
    header->max_payload_bytes = normalized.max_payload_bytes;
    header->peer_lease_ns = normalized.peer_lease_ns;
    header->allocator_offset = layout.allocator_offset;
    header->allocator_extent = layout.allocator_extent;
    header->journal_offset = layout.journal_offset;
    header->journal_extent = layout.journal_extent;
    header->pins_offset = layout.pins_offset;
    header->pins_extent = layout.pins_extent;
    header->channels_offset = layout.channels_offset;
    header->channels_extent = layout.channels_extent;
    header->channel_extent = layout.channel_extent;
    for (uint32_t i = 0; i < normalized.topic_slots; ++i) {
        TopicSlot& slot = header->topics[i];
        slot.channel_offset =
            layout.channels_offset + layout.channel_extent * i;
        slot.channel_extent = layout.channel_extent;
        slot.channel_id = i + 1;
        slot.max_publishers = normalized.max_publishers_per_topic;
    }

    std::byte* base = BytesOf(segment);
    ClassTableConfig allocator_config;
    allocator_config.classes = {
        {.slot_size = normalized.max_payload_bytes,
         .slot_count = layout.allocator_slot_count},
    };
    // Journal/pin table store raw pointers to the allocator facade. Bind them
    // only after the allocator lives in DomainState so moves cannot leave
    // dangling allocator_ pointers.
    auto state = std::make_shared<DomainState>();
    state->segment.emplace(std::move(segment));
    state->header = header;
    state->name = std::string(name);
    MINO_ASSIGN_OR_RETURN(
        state->allocator,
        CentralSlabAllocator::Create(base + layout.allocator_offset,
                                     layout.allocator_extent,
                                     allocator_config));
    MINO_ASSIGN_OR_RETURN(
        state->journal,
        AllocationJournal::Init(base + layout.journal_offset,
                                layout.journal_extent,
                                layout.journal_capacity,
                                /*handles_per_transaction=*/1,
                                *state->allocator));
    MINO_ASSIGN_OR_RETURN(
        state->pins,
        ShmPinTable::Init(base + layout.pins_offset, layout.pins_extent,
                          *state->allocator));
    header->magic.store(kMagic, std::memory_order_release);
    auto impl = std::make_unique<Impl>();
    impl->state = std::move(state);
    return SharedHostDomain(std::move(impl));
}

Result<SharedHostDomain> SharedHostDomain::Open(std::string_view name) {
    MINO_RETURN_IF_ERROR(ValidateName(name));
    SharedMemoryOpenOptions open;
    open.name = std::string(name);
    open.read_only = false;
    open.creating_wait_timeout_ms = 2000;
    MINO_ASSIGN_OR_RETURN(SharedMemorySegment segment,
                          SharedMemorySegment::Open(open));
    if (segment.base() == nullptr ||
        segment.size() < sizeof(DomainHeader) ||
        reinterpret_cast<uintptr_t>(segment.base()) % kCacheLine != 0) {
        return Status::Error(StatusCode::kCorruption,
                             "shared-memory mapping cannot hold domain");
    }
    auto* header = static_cast<DomainHeader*>(segment.base());
    MINO_RETURN_IF_ERROR(
        WaitForMagic(header, Deadline::FromNow(std::chrono::seconds(2))));
    MINO_RETURN_IF_ERROR(ValidateHeader(segment, *header));
    std::byte* base = BytesOf(segment);
    auto state = std::make_shared<DomainState>();
    state->segment.emplace(std::move(segment));
    state->header = header;
    state->name = std::string(name);
    MINO_ASSIGN_OR_RETURN(
        state->allocator,
        CentralSlabAllocator::Attach(base + header->allocator_offset,
                                     header->allocator_extent));
    MINO_ASSIGN_OR_RETURN(
        state->journal,
        AllocationJournal::Attach(base + header->journal_offset,
                                  header->journal_extent, *state->allocator));
    MINO_ASSIGN_OR_RETURN(
        state->pins,
        ShmPinTable::Attach(base + header->pins_offset, header->pins_extent,
                            *state->allocator));
    MINO_RETURN_IF_ERROR(RecoverState(state));
    auto impl = std::make_unique<Impl>();
    impl->state = std::move(state);
    return SharedHostDomain(std::move(impl));
}

Status SharedHostDomain::Unlink(std::string_view name) {
    MINO_RETURN_IF_ERROR(ValidateName(name));
    return SharedMemorySegment::Unlink(std::string(name));
}

Status SharedHostDomain::Join(NodeId node_id) {
    if (impl_ == nullptr || impl_->state == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SharedHostDomain is not bound");
    }
    auto state = impl_->state;
    MINO_RETURN_IF_ERROR(RecoverState(state));
    if (state->peer_index.has_value()) {
        PeerSlot& peer = state->header->peers[*state->peer_index];
        peer.node_id = node_id.value;
        peer.last_heartbeat_ns.store(MonotonicNowNs(),
                                     std::memory_order_release);
        return Status::Ok();
    }
    DomainHeader* header = state->header;
    const ProcessIdentity self = ProcessIdentity::Current();
    MINO_ASSIGN_OR_RETURN(
        uint64_t directory_token,
        ClaimLeaseUntil(header->directory_lock,
                        Deadline::FromNow(std::chrono::seconds(2))));
    LeaseGuard directory_guard(&header->directory_lock, directory_token);

    for (uint32_t i = 0; i < header->peer_slots; ++i) {
        PeerSlot& peer = header->peers[i];
        if (peer.state.load(std::memory_order_acquire) == kPeerActive &&
            peer.identity == self) {
            state->peer_index = i;
            state->peer_token =
                peer.lease.control.load(std::memory_order_acquire);
            peer.node_id = node_id.value;
            peer.last_heartbeat_ns.store(MonotonicNowNs(),
                                         std::memory_order_release);
            return Status::Ok();
        }
    }
    for (uint32_t i = 0; i < header->peer_slots; ++i) {
        PeerSlot& peer = header->peers[i];
        if (peer.state.load(std::memory_order_acquire) != kPeerEmpty) continue;
        MINO_ASSIGN_OR_RETURN(uint64_t token, TryClaimLease(peer.lease));
        peer.identity = self;
        peer.node_id = node_id.value;
        peer.last_heartbeat_ns.store(MonotonicNowNs(),
                                     std::memory_order_release);
        peer.state.store(kPeerActive, std::memory_order_release);
        state->peer_index = i;
        state->peer_token = token;
        return Status::Ok();
    }
    return Status::Error(StatusCode::kResourceExhausted,
                         "peer directory is full");
}

Status SharedHostDomain::Heartbeat() {
    MINO_RETURN_IF_ERROR(EnsureJoined(impl_->state));
    PeerSlot& peer = impl_->state->header->peers[*impl_->state->peer_index];
    peer.last_heartbeat_ns.store(MonotonicNowNs(), std::memory_order_release);
    return Status::Ok();
}

Status SharedHostDomain::Leave() {
    if (impl_ == nullptr || impl_->state == nullptr ||
        !impl_->state->peer_index.has_value()) {
        return Status::Ok();
    }
    PeerSlot& peer = impl_->state->header->peers[*impl_->state->peer_index];
    ReleaseLease(&peer.lease, impl_->state->peer_token);
    ClearPeer(peer);
    impl_->state->peer_index.reset();
    impl_->state->peer_token = 0;
    return Status::Ok();
}

Result<std::vector<SharedPeerInfo>> SharedHostDomain::ListPeers() {
    MINO_RETURN_IF_ERROR(RecoverState(impl_->state));
    DomainHeader* header = impl_->state->header;
    std::vector<SharedPeerInfo> out;
    out.reserve(header->peer_slots);
    for (uint32_t i = 0; i < header->peer_slots; ++i) {
        PeerSlot& peer = header->peers[i];
        if (peer.state.load(std::memory_order_acquire) != kPeerActive) continue;
        out.push_back(SharedPeerInfo{
            .node_id = NodeId{peer.node_id},
            .identity = peer.identity,
            .last_heartbeat_ns =
                peer.last_heartbeat_ns.load(std::memory_order_acquire),
        });
    }
    return out;
}

Result<std::vector<SharedTopicInfo>> SharedHostDomain::ListTopics() {
    MINO_RETURN_IF_ERROR(RecoverState(impl_->state));
    DomainHeader* header = impl_->state->header;
    std::vector<SharedTopicInfo> out;
    for (uint32_t i = 0; i < header->topic_slots; ++i) {
        TopicSlot& slot = header->topics[i];
        if (slot.state.load(std::memory_order_acquire) != kTopicReady) continue;
        const uint32_t active = CountActivePublishers(slot);
        out.push_back(SharedTopicInfo{
            .name = std::string(slot.name),
            .schema = FromPod(slot.schema),
            .mode = static_cast<SharedHostTopicMode>(slot.mode),
            .capacity = slot.capacity,
            .max_subscribers = slot.max_subscribers,
            .max_publishers = slot.max_publishers,
            .max_payload_bytes = slot.max_payload_bytes,
            .active_publishers = active,
            .publisher_active = active > 0,
            .subscriber_active = LeaseIsActive(slot.subscriber),
        });
    }
    return out;
}

Result<SharedHostPublisher> SharedHostDomain::Advertise(
    std::string_view topic, const schema::SchemaIdentity& schema,
    SharedHostTopicOptions options) {
    MINO_RETURN_IF_ERROR(ValidateTopic(topic));
    MINO_RETURN_IF_ERROR(EnsureJoined(impl_->state));
    auto state = impl_->state;
    DomainHeader* header = state->header;
    MINO_RETURN_IF_ERROR(ValidateTopicOptions(options, header->queue_depth));
    const SchemaPod wanted = ToPod(schema);
    if (wanted.short_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "schema short_id must be non-zero");
    }
    MINO_RETURN_IF_ERROR(RecoverState(state));
    MINO_ASSIGN_OR_RETURN(
        uint64_t directory_token,
        ClaimLeaseUntil(header->directory_lock,
                        Deadline::FromNow(std::chrono::seconds(2))));
    LeaseGuard directory_guard(&header->directory_lock, directory_token);

    TopicSlot* found = nullptr;
    TopicSlot* empty = nullptr;
    for (uint32_t i = 0; i < header->topic_slots; ++i) {
        TopicSlot& slot = header->topics[i];
        const uint32_t slot_state = slot.state.load(std::memory_order_acquire);
        if (slot_state == kTopicReady && TopicEquals(slot, topic)) {
            found = &slot;
            break;
        }
        if (slot_state == kTopicEmpty && empty == nullptr) empty = &slot;
    }
    if (found == nullptr) {
        if (empty == nullptr) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "topic directory is full");
        }
        MINO_RETURN_IF_ERROR(
            InitializeTopic(state, *empty, topic, wanted, options));
        found = empty;
    } else if (!SameTopicConfiguration(*found, options, wanted, *header)) {
        return Status::Error(
            StatusCode::kSchemaMismatch,
            "topic already exists with different schema/QoS/mode");
    }
    directory_guard.Disarm();
    ReleaseLease(&header->directory_lock, directory_token);

    EndpointLease* claimed_lease = nullptr;
    uint64_t pub_token = 0;
    if (options.mode == SharedHostTopicMode::kBroadcast) {
        MINO_ASSIGN_OR_RETURN(
            pub_token,
            ClaimLeaseUntil(found->publishers[0],
                            Deadline::FromNow(std::chrono::seconds(2))));
        claimed_lease = &found->publishers[0];
    } else {
        const uint32_t limit =
            std::min(found->max_publishers, kSharedHostMaxPublishersPerTopic);
        Status last = Status::Error(StatusCode::kResourceExhausted,
                                    "publisher lease table is full");
        for (uint32_t i = 0; i < limit; ++i) {
            Result<uint64_t> token = TryClaimLease(found->publishers[i]);
            if (token.ok()) {
                pub_token = *token;
                claimed_lease = &found->publishers[i];
                break;
            }
            last = token.status();
        }
        if (claimed_lease == nullptr) return last;
    }

    MINO_ASSIGN_OR_RETURN(ChannelVariant channel,
                          AttachTopicChannel(state, *found));
    auto impl = std::make_unique<SharedHostPublisher::Impl>();
    impl->state = state;
    impl->slot = found;
    impl->lease = claimed_lease;
    impl->token = pub_token;
    impl->channel_id = found->channel_id;
    impl->channel.emplace(std::move(channel));
    impl->options = options;
    const ProcessIdentity owner = ProcessIdentity::Current();
    impl->mpsc_identity = MpscChannel::ProducerIdentity{
        .owner = owner,
        .publisher_id =
            header->publisher_sequence.fetch_add(1, std::memory_order_relaxed) +
            1,
    };
    return SharedHostPublisher(std::move(impl));
}

Result<SharedHostSubscriber> SharedHostDomain::Subscribe(
    std::string_view topic, const schema::SchemaIdentity& schema,
    Deadline deadline) {
    MINO_RETURN_IF_ERROR(ValidateTopic(topic));
    MINO_RETURN_IF_ERROR(EnsureJoined(impl_->state));
    auto state = impl_->state;
    DomainHeader* header = state->header;
    const SchemaPod wanted = ToPod(schema);
    TopicSlot* found = nullptr;
    for (;;) {
        MINO_RETURN_IF_ERROR(RecoverState(state));
        for (uint32_t i = 0; i < header->topic_slots; ++i) {
            TopicSlot& slot = header->topics[i];
            if (slot.state.load(std::memory_order_acquire) == kTopicReady &&
                TopicEquals(slot, topic)) {
                found = &slot;
                break;
            }
        }
        if (found != nullptr) break;
        if (deadline.expired()) {
            return Status::Error(StatusCode::kTimeout,
                                 "timed out waiting for topic advertisement");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!SameSchema(found->schema, wanted)) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "subscribed schema does not match advertised topic");
    }
    MINO_ASSIGN_OR_RETURN(ChannelVariant channel,
                          AttachTopicChannel(state, *found));
    auto impl = std::make_unique<SharedHostSubscriber::Impl>();
    impl->state = state;
    impl->slot = found;
    impl->channel.emplace(std::move(channel));
    impl->mode = static_cast<SharedHostTopicMode>(found->mode);
    impl->owner = ProcessIdentity::Current();

    if (impl->mode == SharedHostTopicMode::kMpsc) {
        MINO_ASSIGN_OR_RETURN(
            uint64_t token,
            ClaimLeaseUntil(found->subscriber,
                            Deadline::FromNow(std::chrono::seconds(2))));
        impl->subscriber_lease = &found->subscriber;
        impl->subscriber_token = token;
    } else {
        auto& broadcast = std::get<BroadcastChannel>(*impl->channel);
        BroadcastChannel::SubscriberHandle handle{};
        bool registered = false;
        for (uint32_t id = 0; id < found->max_subscribers; ++id) {
            Result<BroadcastChannel::SubscriberHandle> claimed =
                broadcast.RegisterSubscriber(SubscriberId{id},
                                             ProcessIdentity::Current(),
                                             MonotonicNowNs());
            if (claimed.ok()) {
                handle = *claimed;
                registered = true;
                break;
            }
            if (claimed.status().code() != StatusCode::kAlreadyExists) {
                return claimed.status();
            }
        }
        if (!registered) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "topic subscriber capacity is exhausted");
        }
        impl->broadcast_handle = handle;
    }
    return SharedHostSubscriber(std::move(impl));
}

Status SharedHostDomain::Recover() {
    return RecoverState(impl_->state);
}

}  // namespace mino::deployment
