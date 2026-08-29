// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/simple_node.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>

#include <csignal>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>

#include "mino/common/checked_arithmetic.h"
#include "mino/common/ids.h"
#include "mino/platform/process_identity.h"
#include "mino/platform/shared_memory.h"
#include "mino/runtime/allocation_journal.h"
#include "mino/runtime/journal_channel_recovery.h"
#include "mino/runtime/shm_shared_ptr.h"
#include "mino/runtime/subscriber_lease.h"
#include "mino/shm/allocator/bitmap.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/allocator/class_table.h"
#include "mino/shm/allocator/slab_header.h"
#include "mino/shm/channel/broadcast_channel.h"
#include "mino/shm/channel/mpsc_channel.h"
#include "mino/shm/channel/spsc_channel.h"

namespace mino {
namespace {

constexpr uint64_t kCacheLine = 64;
constexpr uint64_t kMagic = 0x4D494E4F534D5031ull;  // "MINOSMP1"
constexpr uint32_t kVersion = 3;
constexpr uint32_t kMaxTopicSlots = 16;
constexpr uint32_t kMaxTopicName = 63;
constexpr uint32_t kMinQueueDepth = 2;
constexpr uint32_t kMaxQueueDepth = 1024;
constexpr uint32_t kMaxPayloadBytes = 1024u * 1024u;
constexpr uint32_t kMaxPublishersPerTopic = 64;
constexpr uint64_t kMarkerSlackBytes = 8192;
constexpr uint64_t kMinLeaseNs = 1000 * 1000;  // 1 ms

constexpr uint32_t kSimpleMsgType = 0x53424C42u;  // "SBLB"
constexpr uint32_t kSimpleSchemaVersion = (1u << 16);
constexpr uint64_t kSimpleSchemaShortId = 0x4D494E4F42594453ull;
constexpr uint32_t kSimpleLayoutVersion = 1;

constexpr uint32_t kTopicEmpty = 0;
constexpr uint32_t kTopicReady = 1;
constexpr uint64_t kLeaseStateMask = 0x3;
constexpr uint64_t kLeaseClaiming = 1;
constexpr uint64_t kLeaseActive = 2;
constexpr uint64_t kLeaseGenerationMask = 0x3FFF'FFFFull;

struct alignas(kCacheLine) EndpointLease {
    std::atomic<uint64_t> control{0};
    std::atomic<uint64_t> next_generation{0};
    std::atomic<uint64_t> owner_node_id{0};
    std::atomic<uint64_t> owner_process_id{0};
    std::atomic<uint64_t> owner_process_epoch{0};
    std::atomic<uint64_t> owner_start_time_ns{0};
    unsigned char pad[16]{};
};

static_assert(sizeof(EndpointLease) == kCacheLine);
static_assert(alignof(EndpointLease) == kCacheLine);
static_assert(std::is_standard_layout_v<EndpointLease>);

struct alignas(kCacheLine) TopicSlot {
    std::atomic<uint32_t> state{0};
    uint32_t mode = 0;
    uint32_t queue_full_policy = 0;
    uint32_t sample_rate = 0;
    char name[64]{};
    SimpleTypeDescriptor type;
    uint64_t channel_offset = 0;
    uint64_t channel_extent = 0;
    uint64_t lease_offset = 0;
    uint64_t lease_extent = 0;
    uint64_t capacity = 0;
    uint64_t channel_id = 0;
    EndpointLease publisher;
    EndpointLease subscriber;
    unsigned char pad[16]{};
};

static_assert(sizeof(TopicSlot) % kCacheLine == 0);
static_assert(alignof(TopicSlot) == kCacheLine);
static_assert(std::is_standard_layout_v<TopicSlot>);

struct alignas(kCacheLine) ManifestHeader {
    std::atomic<uint64_t> magic{0};
    uint32_t version = 0;
    uint32_t header_size = 0;
    uint64_t total_size = 0;
    uint32_t topic_slots = 0;
    uint32_t queue_depth = 0;
    uint32_t max_payload_bytes = 0;
    uint32_t max_publishers_per_topic = 0;
    uint64_t subscriber_lease_ns = 0;
    uint64_t allocator_offset = 0;
    uint64_t allocator_extent = 0;
    uint64_t journal_offset = 0;
    uint64_t journal_extent = 0;
    uint64_t pins_offset = 0;
    uint64_t pins_extent = 0;
    uint64_t channels_offset = 0;
    uint64_t channels_extent = 0;
    uint64_t leases_offset = 0;
    uint64_t leases_extent = 0;
    std::atomic<uint64_t> publisher_sequence{0};
    uint64_t reserved0 = 0;
    EndpointLease directory_lock;
    TopicSlot topics[kMaxTopicSlots];
};

static_assert(alignof(ManifestHeader) == kCacheLine);
static_assert(std::is_standard_layout_v<ManifestHeader>);
static_assert(offsetof(ManifestHeader, magic) == 0);
static_assert(offsetof(ManifestHeader, topics) % kCacheLine == 0);

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
    uint64_t leases_offset = 0;
    uint64_t leases_extent = 0;
    uint64_t lease_extent = 0;
    uint32_t allocator_slot_count = 0;
    uint32_t journal_capacity = 0;
};

using Channel = std::variant<SpscChannel, MpscChannel, BroadcastChannel>;
using ChannelBorrow = std::variant<SpscChannel::Borrow, MpscChannel::Borrow,
                                   BroadcastChannel::Borrow>;

Status Errorf(StatusCode code, std::string_view prefix, uint64_t a,
              uint64_t b) {
    return Status::Error(code, std::string(prefix) + " (" +
                                   std::to_string(a) + " > " +
                                   std::to_string(b) + ")");
}

uint64_t MonotonicNowNs() noexcept {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

uint64_t HostPageSize() {
    const long page = ::sysconf(_SC_PAGESIZE);
    return page > 0 ? static_cast<uint64_t>(page) : 4096;
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

Status ValidateTopicOptions(const SimpleTopicOptions& options,
                            uint32_t queue_depth) {
    if (options.mode != SimpleTopicMode::kSpsc &&
        options.mode != SimpleTopicMode::kMpsc &&
        options.mode != SimpleTopicMode::kBroadcast) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SimpleTopicMode is invalid");
    }
    if (static_cast<uint32_t>(options.queue_full_policy) >
        static_cast<uint32_t>(QueueFullPolicy::kSample)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "QueueFullPolicy is invalid");
    }
    if (options.sample_rate == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "sample_rate must be at least 1");
    }
    if (options.mode == SimpleTopicMode::kMpsc && queue_depth < 64) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "MPSC topics require queue_depth >= 64");
    }
    return Status::Ok();
}

Status ValidateType(const SimpleTypeDescriptor& type,
                    uint32_t max_payload_bytes) {
    if (type.type_id == 0 || type.message_type == 0 ||
        type.schema_short_id == 0 || type.alignment == 0 ||
        (type.alignment & (type.alignment - 1)) != 0 ||
        type.alignment > kCacheLine || type.reserved0 != 0 ||
        type.fixed_size > max_payload_bytes) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SimpleNode type descriptor is invalid");
    }
    return Status::Ok();
}

bool SameType(const SimpleTypeDescriptor& lhs,
              const SimpleTypeDescriptor& rhs) noexcept {
    return lhs.type_id == rhs.type_id &&
           lhs.message_type == rhs.message_type &&
           lhs.schema_version == rhs.schema_version &&
           lhs.index_flags == rhs.index_flags &&
           lhs.schema_short_id == rhs.schema_short_id &&
           lhs.layout_version == rhs.layout_version &&
           lhs.alignment == rhs.alignment &&
           lhs.fixed_size == rhs.fixed_size && lhs.reserved0 == 0 &&
           rhs.reserved0 == 0;
}

bool TopicEquals(const TopicSlot& slot, std::string_view topic) {
    return std::strncmp(slot.name, topic.data(), topic.size()) == 0 &&
           slot.name[topic.size()] == '\0';
}

void WriteTopicName(TopicSlot* slot, std::string_view topic) {
    std::memset(slot->name, 0, sizeof(slot->name));
    std::memcpy(slot->name, topic.data(), topic.size());
}

Result<SimpleNodeOptions> NormalizeOptions(SimpleNodeOptions options) {
    if (options.topic_slots == 0 || options.topic_slots > kMaxTopicSlots) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "topic_slots must be in [1, 16]");
    }
    if (options.queue_depth < kMinQueueDepth ||
        options.queue_depth > kMaxQueueDepth ||
        (options.queue_depth & (options.queue_depth - 1)) != 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "queue_depth must be a power of two in [2, 1024]");
    }
    if (options.max_payload_bytes == 0 ||
        options.max_payload_bytes > kMaxPayloadBytes) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "max_payload_bytes must be in [1, 1 MiB]");
    }
    if (options.max_publishers_per_topic == 0 ||
        options.max_publishers_per_topic > kMaxPublishersPerTopic) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "max_publishers_per_topic must be in [1, 64]");
    }
    if (options.subscriber_lease_ns < kMinLeaseNs) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "subscriber_lease_ns must be at least 1 ms");
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

Result<SegmentLayout> ComputeLayout(const SimpleNodeOptions& options) {
    uint64_t journal_capacity = 0;
    uint64_t allocator_slots = 0;
    if (!CheckedMulU64(options.topic_slots,
                       options.max_publishers_per_topic,
                       &journal_capacity) ||
        !CheckedMulU64(options.topic_slots, options.queue_depth,
                       &allocator_slots) ||
        !CheckedAddU64(allocator_slots, journal_capacity, &allocator_slots) ||
        journal_capacity > std::numeric_limits<uint32_t>::max() ||
        allocator_slots > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SimpleNode recovery capacity overflows");
    }

    SegmentLayout layout;
    layout.journal_capacity = static_cast<uint32_t>(journal_capacity);
    layout.allocator_slot_count = static_cast<uint32_t>(allocator_slots);
    MINO_ASSIGN_OR_RETURN(
        layout.allocator_extent,
        AllocatorExtent(options.max_payload_bytes,
                        layout.allocator_slot_count));
    layout.journal_extent = AllocationJournal::RequiredSize(
        layout.journal_capacity, /*handles_per_transaction=*/1);
    layout.pins_extent = ShmPinTable::RequiredSize();
    layout.channel_extent = std::max(
        SpscChannel::RequiredSize(options.queue_depth),
        BroadcastChannel::RequiredSize(options.queue_depth));
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
    layout.lease_extent = SubscriberLeaseTable::RequiredSize();

    layout.allocator_offset = sizeof(ManifestHeader);
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
        !append(layout.channels_offset, layout.channels_extent,
                &layout.leases_offset) ||
        !CheckedMulU64(layout.lease_extent, options.topic_slots,
                       &layout.leases_extent) ||
        !CheckedAddU64(layout.leases_offset, layout.leases_extent, &next) ||
        !CheckedAlignUpU64(next, HostPageSize(), &layout.total_size)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SimpleNode segment layout overflows");
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
        return Errorf(StatusCode::kResourceExhausted,
                      "requested SHM segment exceeds free /dev/shm", needed,
                      available);
    }
    struct rlimit limit;
    if (::getrlimit(RLIMIT_FSIZE, &limit) == 0 &&
        limit.rlim_cur != RLIM_INFINITY &&
        needed > static_cast<uint64_t>(limit.rlim_cur)) {
        return Errorf(StatusCode::kResourceExhausted,
                      "requested SHM segment exceeds RLIMIT_FSIZE", needed,
                      static_cast<uint64_t>(limit.rlim_cur));
    }
    if (::getrlimit(RLIMIT_AS, &limit) == 0 &&
        limit.rlim_cur != RLIM_INFINITY &&
        needed > static_cast<uint64_t>(limit.rlim_cur)) {
        return Errorf(StatusCode::kResourceExhausted,
                      "requested SHM segment exceeds RLIMIT_AS", needed,
                      static_cast<uint64_t>(limit.rlim_cur));
    }
    return Status::Ok();
}

Status WaitForMagic(ManifestHeader* header, Deadline deadline) {
    while (header->magic.load(std::memory_order_acquire) != kMagic) {
        if (deadline.expired()) {
            return Status::Error(StatusCode::kTimeout,
                                 "timed out waiting for SimpleNode Create");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return Status::Ok();
}

std::byte* BytesOf(SharedMemorySegment& segment) {
    return static_cast<std::byte*>(segment.base());
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

bool PidIsProvenDead(uint32_t pid) noexcept {
    if (pid == 0) return false;
    errno = 0;
    const int rc = ::kill(static_cast<pid_t>(pid), 0);
    return rc != 0 && errno == ESRCH;
}

bool RecoverLease(EndpointLease& lease,
                  ProcessIdentity* recovered_owner = nullptr) noexcept {
    const uint64_t observed = lease.control.load(std::memory_order_acquire);
    if (observed == 0) return false;
    const uint64_t state = observed & kLeaseStateMask;
    if (state != kLeaseClaiming && state != kLeaseActive) return false;
    const uint32_t pid = static_cast<uint32_t>(observed >> 32);
    const ProcessIdentity owner = LoadOwner(lease);
    bool dead = false;
    if (!owner.IsZero() && owner.process_id == pid) {
        dead = ProbeProcessIdentity(owner) == ProcessIdentityLiveness::kDead;
    } else if (state == kLeaseClaiming) {
        dead = PidIsProvenDead(pid);
    }
    if (!dead) return false;
    uint64_t expected = observed;
    if (!lease.control.compare_exchange_strong(
            expected, 0, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return false;
    }
    if (recovered_owner != nullptr) *recovered_owner = owner;
    return true;
}

Result<uint64_t> TryClaimLease(EndpointLease& lease) {
    (void)RecoverLease(lease);
    const ProcessIdentity owner = ProcessIdentity::Current();
    if (owner.IsZero() || owner.process_id == 0 ||
        owner.process_id > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kUnavailable,
                             "current process identity cannot own an endpoint");
    }
    uint64_t generation =
        lease.next_generation.fetch_add(1, std::memory_order_relaxed) + 1;
    generation &= kLeaseGenerationMask;
    if (generation == 0) {
        generation =
            lease.next_generation.fetch_add(1, std::memory_order_relaxed) + 1;
        generation &= kLeaseGenerationMask;
    }
    const uint64_t claiming = LeaseToken(
        static_cast<uint32_t>(owner.process_id), generation, kLeaseClaiming);
    uint64_t expected = 0;
    if (!lease.control.compare_exchange_strong(
            expected, claiming, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "endpoint is already claimed");
    }
    StoreOwner(lease, owner);
    const uint64_t active = (claiming & ~kLeaseStateMask) | kLeaseActive;
    expected = claiming;
    if (!lease.control.compare_exchange_strong(
            expected, active, std::memory_order_release,
            std::memory_order_acquire)) {
        return Status::Error(StatusCode::kUnavailable,
                             "endpoint claim was recovered during registration");
    }
    return active;
}

Result<uint64_t> ClaimLeaseUntil(EndpointLease& lease, Deadline deadline) {
    for (;;) {
        Result<uint64_t> claimed = TryClaimLease(lease);
        if (claimed.ok()) return claimed;
        if (claimed.status().code() != StatusCode::kAlreadyExists) {
            return claimed.status();
        }
        if (deadline.expired()) return claimed.status();
        std::this_thread::yield();
    }
}

void ReleaseLease(EndpointLease* lease, uint64_t token) noexcept {
    if (lease == nullptr || token == 0) return;
    uint64_t expected = token;
    lease->control.compare_exchange_strong(expected, 0,
                                           std::memory_order_release,
                                           std::memory_order_relaxed);
}

class LeaseGuard {
public:
    LeaseGuard(EndpointLease* lease, uint64_t token) noexcept
        : lease_(lease), token_(token) {}
    ~LeaseGuard() { ReleaseLease(lease_, token_); }
    LeaseGuard(const LeaseGuard&) = delete;
    LeaseGuard& operator=(const LeaseGuard&) = delete;

private:
    EndpointLease* lease_;
    uint64_t token_;
};

SimpleNodeOptions OptionsFromHeader(const ManifestHeader& header) {
    return SimpleNodeOptions{
        .topic_slots = header.topic_slots,
        .queue_depth = header.queue_depth,
        .max_payload_bytes = header.max_payload_bytes,
        .max_publishers_per_topic = header.max_publishers_per_topic,
        .subscriber_lease_ns = header.subscriber_lease_ns,
        .segment_bytes = header.total_size,
    };
}

Status ValidateHeader(const SharedMemorySegment& segment,
                      const ManifestHeader& header) {
    if (header.version != kVersion ||
        header.header_size != sizeof(ManifestHeader)) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "SimpleNode manifest version/size mismatch");
    }
    if (header.total_size != segment.size() || header.reserved0 != 0) {
        return Status::Error(StatusCode::kCorruption,
                             "SimpleNode manifest size/reserved mismatch");
    }
    MINO_ASSIGN_OR_RETURN(SimpleNodeOptions options,
                          NormalizeOptions(OptionsFromHeader(header)));
    options.segment_bytes = 0;
    MINO_ASSIGN_OR_RETURN(SegmentLayout expected, ComputeLayout(options));
    if (header.total_size < expected.total_size ||
        header.allocator_offset != expected.allocator_offset ||
        header.allocator_extent != expected.allocator_extent ||
        header.journal_offset != expected.journal_offset ||
        header.journal_extent != expected.journal_extent ||
        header.pins_offset != expected.pins_offset ||
        header.pins_extent != expected.pins_extent ||
        header.channels_offset != expected.channels_offset ||
        header.channels_extent != expected.channels_extent ||
        header.leases_offset != expected.leases_offset ||
        header.leases_extent != expected.leases_extent) {
        return Status::Error(StatusCode::kCorruption,
                             "SimpleNode manifest extents mismatch");
    }
    for (uint32_t i = 0; i < header.topic_slots; ++i) {
        const TopicSlot& slot = header.topics[i];
        uint64_t channel_delta = 0;
        uint64_t lease_delta = 0;
        if (!CheckedMulU64(expected.channel_extent, i, &channel_delta) ||
            !CheckedMulU64(expected.lease_extent, i, &lease_delta) ||
            slot.channel_offset != header.channels_offset + channel_delta ||
            slot.channel_extent != expected.channel_extent ||
            slot.lease_offset != header.leases_offset + lease_delta ||
            slot.lease_extent != expected.lease_extent ||
            slot.capacity != header.queue_depth || slot.channel_id != i + 1) {
            return Status::Error(StatusCode::kCorruption,
                                 "SimpleNode topic extent mismatch");
        }
        const uint32_t state = slot.state.load(std::memory_order_acquire);
        if (state != kTopicEmpty && state != kTopicReady) {
            return Status::Error(StatusCode::kCorruption,
                                 "SimpleNode topic state is invalid");
        }
        if (state == kTopicReady) {
            if (slot.name[0] == '\0' ||
                std::memchr(slot.name, '\0', sizeof(slot.name)) == nullptr) {
                return Status::Error(StatusCode::kCorruption,
                                     "SimpleNode topic name is invalid");
            }
            const SimpleTopicOptions topic_options{
                .mode = static_cast<SimpleTopicMode>(slot.mode),
                .queue_full_policy =
                    static_cast<QueueFullPolicy>(slot.queue_full_policy),
                .sample_rate = slot.sample_rate,
            };
            MINO_RETURN_IF_ERROR(
                ValidateTopicOptions(topic_options, header.queue_depth));
            MINO_RETURN_IF_ERROR(
                ValidateType(slot.type, header.max_payload_bytes));
        }
    }
    return Status::Ok();
}

struct SimpleNodeState {
    std::optional<SharedMemorySegment> segment;
    std::optional<CentralSlabAllocator> allocator;
    std::optional<AllocationJournal> journal;
    std::optional<ShmPinTable> pins;
    ManifestHeader* header = nullptr;
    std::string name;
};

Result<Channel> AttachChannel(const std::shared_ptr<SimpleNodeState>& state,
                              const TopicSlot& slot) {
    std::byte* base = BytesOf(*state->segment);
    void* channel_base = base + slot.channel_offset;
    const auto mode = static_cast<SimpleTopicMode>(slot.mode);
    if (mode == SimpleTopicMode::kSpsc) {
        MINO_ASSIGN_OR_RETURN(SpscChannel channel,
                              SpscChannel::Attach(channel_base));
        if (channel.capacity() != slot.capacity) {
            return Status::Error(StatusCode::kCorruption,
                                 "SPSC capacity mismatch");
        }
        channel.SetPayloadRetireObserver(&ShmPinTable::RetirePayloadCallback,
                                         &*state->pins);
        return Channel(channel);
    }
    if (mode == SimpleTopicMode::kMpsc) {
        MINO_ASSIGN_OR_RETURN(MpscChannel channel,
                              MpscChannel::Attach(channel_base));
        if (channel.capacity() != slot.capacity) {
            return Status::Error(StatusCode::kCorruption,
                                 "MPSC capacity mismatch");
        }
        channel.SetPayloadRetireObserver(&ShmPinTable::RetirePayloadCallback,
                                         &*state->pins);
        return Channel(channel);
    }
    MINO_ASSIGN_OR_RETURN(BroadcastChannel channel,
                          BroadcastChannel::Attach(channel_base));
    if (channel.capacity() != slot.capacity) {
        return Status::Error(StatusCode::kCorruption,
                             "Broadcast capacity mismatch");
    }
    channel.SetPayloadRetireObserver(&ShmPinTable::RetirePayloadCallback,
                                     &*state->pins);
    return Channel(channel);
}

Result<SubscriberLeaseTable> AttachLeaseTable(
    const std::shared_ptr<SimpleNodeState>& state, const TopicSlot& slot) {
    std::byte* base = BytesOf(*state->segment);
    return SubscriberLeaseTable::Attach(base + slot.lease_offset);
}

Status RecoverState(const std::shared_ptr<SimpleNodeState>& state,
                    bool immediate_broadcast_recovery = false) {
    if (state == nullptr || state->header == nullptr ||
        !state->segment.has_value() || !state->allocator.has_value() ||
        !state->journal.has_value() || !state->pins.has_value()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SimpleNode state is not bound");
    }
    ManifestHeader* header = state->header;
    (void)RecoverLease(header->directory_lock);

    std::optional<SpscChannel> spsc[kMaxTopicSlots];
    std::optional<MpscChannel> mpsc[kMaxTopicSlots];
    std::optional<BroadcastPublicationView> broadcast[kMaxTopicSlots];
    JournalChannelRecoveryCoordinator journal_recovery(*state->journal);

    for (uint32_t i = 0; i < header->topic_slots; ++i) {
        TopicSlot& slot = header->topics[i];
        if (slot.state.load(std::memory_order_acquire) != kTopicReady) continue;

        ProcessIdentity dead_subscriber;
        if (RecoverLease(slot.subscriber, &dead_subscriber) &&
            !dead_subscriber.IsZero()) {
            (void)state->pins->CleanupOwner(dead_subscriber);
        }
        (void)RecoverLease(slot.publisher);

        MINO_ASSIGN_OR_RETURN(Channel channel, AttachChannel(state, slot));
        const auto mode = static_cast<SimpleTopicMode>(slot.mode);
        if (mode == SimpleTopicMode::kSpsc) {
            spsc[i] = std::get<SpscChannel>(channel);
            MINO_RETURN_IF_ERROR(
                journal_recovery.RegisterChannel(slot.channel_id, *spsc[i]));
        } else if (mode == SimpleTopicMode::kMpsc) {
            mpsc[i] = std::get<MpscChannel>(channel);
            (void)mpsc[i]->AbortOrphanedReservations(MonotonicNowNs());
            MINO_RETURN_IF_ERROR(
                journal_recovery.RegisterChannel(slot.channel_id, *mpsc[i]));
        } else {
            BroadcastChannel broadcast_channel =
                std::get<BroadcastChannel>(channel);
            MINO_ASSIGN_OR_RETURN(SubscriberLeaseTable leases,
                                  AttachLeaseTable(state, slot));
            SubscriberLeaseCoordinator coordinator(
                broadcast_channel, leases, nullptr, nullptr,
                &ShmPinTable::CleanupOwnerCallback, &*state->pins);
            // The coordinator still requires exact-owner death. Explicit
            // recovery may skip the heartbeat grace period; hot-path recovery
            // honors the configured lease before doing destructive cleanup.
            (void)coordinator.EvictExpired(
                MonotonicNowNs(), immediate_broadcast_recovery
                                      ? 0
                                      : header->subscriber_lease_ns);
            std::byte* base = BytesOf(*state->segment);
            MINO_ASSIGN_OR_RETURN(
                broadcast[i],
                BroadcastPublicationView::Attach(base + slot.channel_offset));
            MINO_RETURN_IF_ERROR(journal_recovery.RegisterChannel(
                slot.channel_id, *broadcast[i]));
        }
    }
    (void)journal_recovery.RecoverOrphans();
    return Status::Ok();
}

struct PublisherClaim {
    std::shared_ptr<SimpleNodeState> node;
    EndpointLease* lease = nullptr;
    uint64_t token = 0;

    ~PublisherClaim() { ReleaseLease(lease, token); }
};

struct SubscriberClaim {
    std::shared_ptr<SimpleNodeState> node;
    Channel channel;
    EndpointLease* lease = nullptr;
    uint64_t token = 0;
    std::optional<SubscriberLeaseTable> lease_table;
    std::optional<SubscriberLeaseHandle> broadcast_handle;
    ProcessIdentity owner;
    SimpleTypeDescriptor type;
    SimpleTopicMode mode = SimpleTopicMode::kSpsc;
    std::atomic<bool> borrow_active{false};

    ~SubscriberClaim() {
        if (mode == SimpleTopicMode::kBroadcast && lease_table.has_value() &&
            broadcast_handle.has_value()) {
            BroadcastChannel& broadcast = std::get<BroadcastChannel>(channel);
            SubscriberLeaseCoordinator coordinator(
                broadcast, *lease_table, nullptr, nullptr,
                &ShmPinTable::CleanupOwnerCallback, &*node->pins);
            (void)coordinator.Unregister(*broadcast_handle);
        } else {
            ReleaseLease(lease, token);
        }
    }
};

Status InitializeTopic(const std::shared_ptr<SimpleNodeState>& state,
                       TopicSlot& slot, std::string_view topic,
                       const SimpleTopicOptions& options,
                       const SimpleTypeDescriptor& type) {
    std::byte* base = BytesOf(*state->segment);
    std::memset(base + slot.channel_offset, 0,
                static_cast<size_t>(slot.channel_extent));
    if (options.mode == SimpleTopicMode::kSpsc) {
        MINO_RETURN_IF_ERROR(
            SpscChannel::Init(base + slot.channel_offset, slot.capacity)
                .status());
    } else if (options.mode == SimpleTopicMode::kMpsc) {
        MINO_RETURN_IF_ERROR(
            MpscChannel::Init(base + slot.channel_offset, slot.capacity)
                .status());
    } else {
        MINO_RETURN_IF_ERROR(
            BroadcastChannel::Init(base + slot.channel_offset, slot.capacity)
                .status());
        std::memset(base + slot.lease_offset, 0,
                    static_cast<size_t>(slot.lease_extent));
        MINO_RETURN_IF_ERROR(
            SubscriberLeaseTable::Init(base + slot.lease_offset).status());
    }
    WriteTopicName(&slot, topic);
    slot.mode = static_cast<uint32_t>(options.mode);
    slot.queue_full_policy =
        static_cast<uint32_t>(options.queue_full_policy);
    slot.sample_rate = options.sample_rate;
    slot.type = type;
    slot.state.store(kTopicReady, std::memory_order_release);
    return Status::Ok();
}

bool SameTopicConfiguration(const TopicSlot& slot,
                            const SimpleTopicOptions& options,
                            const SimpleTypeDescriptor& type) noexcept {
    return slot.mode == static_cast<uint32_t>(options.mode) &&
           slot.queue_full_policy ==
               static_cast<uint32_t>(options.queue_full_policy) &&
           slot.sample_rate == options.sample_rate && SameType(slot.type, type);
}

}  // namespace

SimpleTypeDescriptor SimpleTypeDescriptor::Bytes() noexcept {
    return SimpleTypeDescriptor{
        .type_id = kSimpleMsgType,
        .message_type = kSimpleMsgType,
        .schema_version = kSimpleSchemaVersion,
        .index_flags = 0,
        .schema_short_id = kSimpleSchemaShortId,
        .layout_version = kSimpleLayoutVersion,
        .alignment = 1,
        .fixed_size = 0,
    };
}

struct BorrowedBytes::Impl {
    std::shared_ptr<SubscriberClaim> claim;
    ChannelBorrow borrow;
    ShmPinToken pin;
    const std::byte* data = nullptr;
    uint32_t size = 0;
    ShmHandle handle;
    SimpleTypeDescriptor type;
    bool payload_cleanup_by_channel = false;
    bool active = false;
};

BorrowedBytes::BorrowedBytes() noexcept = default;
BorrowedBytes::BorrowedBytes(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
BorrowedBytes::BorrowedBytes(BorrowedBytes&& other) noexcept = default;
BorrowedBytes& BorrowedBytes::operator=(BorrowedBytes&& other) noexcept {
    if (this != &other) {
        if (impl_ != nullptr && impl_->active) {
            (void)std::move(*this).Release();
        }
        impl_ = std::move(other.impl_);
    }
    return *this;
}
BorrowedBytes::~BorrowedBytes() {
    if (impl_ != nullptr && impl_->active) {
        (void)std::move(*this).Release();
    }
}

std::span<const std::byte> BorrowedBytes::bytes() const noexcept {
    if (impl_ == nullptr || !impl_->active || impl_->data == nullptr) return {};
    return {impl_->data, impl_->size};
}

const SimpleTypeDescriptor& BorrowedBytes::type() const noexcept {
    static const SimpleTypeDescriptor kEmpty;
    return impl_ == nullptr ? kEmpty : impl_->type;
}

bool BorrowedBytes::Matches(const SimpleTypeDescriptor& expected) const noexcept {
    return impl_ != nullptr && impl_->active && SameType(impl_->type, expected);
}

bool BorrowedBytes::active() const noexcept {
    return impl_ != nullptr && impl_->active;
}

Status BorrowedBytes::Release() && noexcept {
    if (impl_ == nullptr || !impl_->active) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "borrowed bytes are not active");
    }
    impl_->active = false;
    const Status ack = std::visit(
        [](auto& borrow) { return std::move(borrow).Ack(); }, impl_->borrow);
    Status retire = Status::Ok();
    if (ack.ok() && !impl_->payload_cleanup_by_channel &&
        impl_->claim != nullptr && impl_->claim->node != nullptr &&
        !impl_->handle.IsNull()) {
        retire = impl_->claim->node->pins->RetirePayload(impl_->handle);
    }
    const Status release_pin = impl_->pin.Release();
    impl_->data = nullptr;
    if (impl_->claim != nullptr) {
        impl_->claim->borrow_active.store(false, std::memory_order_release);
        impl_->claim.reset();
    }
    if (!ack.ok()) return ack;
    if (!retire.ok()) return retire;
    return release_pin;
}

struct SimplePublisher::Impl {
    std::shared_ptr<PublisherClaim> claim;
    Channel channel;
    SimpleTypeDescriptor type;
    SimpleTopicOptions options;
    MpscChannel::ProducerIdentity mpsc_identity;
    uint64_t channel_id = 0;
    uint32_t max_payload_bytes = 0;
};

SimplePublisher::SimplePublisher(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SimplePublisher::SimplePublisher(SimplePublisher&& other) noexcept = default;
SimplePublisher& SimplePublisher::operator=(SimplePublisher&& other) noexcept =
    default;
SimplePublisher::~SimplePublisher() = default;

SimpleTopicMode SimplePublisher::mode() const noexcept {
    return impl_ == nullptr ? SimpleTopicMode::kSpsc : impl_->options.mode;
}

const SimpleTypeDescriptor& SimplePublisher::type() const noexcept {
    static const SimpleTypeDescriptor kEmpty;
    return impl_ == nullptr ? kEmpty : impl_->type;
}

Status SimplePublisher::Publish(std::span<const std::byte> payload,
                                Deadline deadline) {
    return PublishTyped(payload, SimpleTypeDescriptor::Bytes(), deadline);
}

Status SimplePublisher::PublishTyped(std::span<const std::byte> payload,
                                     const SimpleTypeDescriptor& type,
                                     Deadline deadline) {
    if (impl_ == nullptr || impl_->claim == nullptr ||
        impl_->claim->node == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "publisher is not bound");
    }
    if (!SameType(type, impl_->type)) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "publisher type does not match topic schema");
    }
    if (payload.empty() || payload.size() > impl_->max_payload_bytes ||
        (type.fixed_size != 0 && payload.size() != type.fixed_size)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "payload size is invalid for this topic");
    }
    if (deadline.expired()) {
        return Status::Error(StatusCode::kTimeout,
                             "publish deadline expired");
    }
    const std::shared_ptr<SimpleNodeState> state = impl_->claim->node;
    MINO_RETURN_IF_ERROR(RecoverState(state));

    AllocationJournal& journal = *state->journal;
    const ProcessIdentity owner = ProcessIdentity::Current();
    MINO_ASSIGN_OR_RETURN(AllocationTransaction transaction,
                          journal.Begin(owner));
    AllocationRequest request;
    request.object_size = static_cast<uint32_t>(payload.size());
    request.type_id = TypeId{type.type_id};
    request.schema = SchemaIdentity{
        .short_id = type.schema_short_id,
        .layout_version = type.layout_version,
    };
    request.alignment = type.alignment;
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
        IndexSlot* slot = reservation->slot();
        slot->msg_type = type.message_type;
        slot->schema_version = type.schema_version;
        slot->schema_short_id = type.schema_short_id;
        slot->schema_layout_version = type.layout_version;
        slot->reserved0 = 0;
        slot->timestamp_ns = MonotonicNowNs();
        slot->payload = handle;
        slot->payload_len = static_cast<uint32_t>(payload.size());
        slot->flags = type.index_flags;
        const uint64_t sequence =
            slot->sequence_num.load(std::memory_order_relaxed);
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
    if (impl_->options.mode == SimpleTopicMode::kSpsc) {
        SpscChannel& channel = std::get<SpscChannel>(impl_->channel);
        Result<SpscChannel::Reservation> reservation =
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
                        "SPSC queue full: message sampled out");
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
        return finish(std::move(reservation), PublicationChannelKind::kSpsc,
                      channel);
    }
    if (impl_->options.mode == SimpleTopicMode::kMpsc) {
        MpscChannel& channel = std::get<MpscChannel>(impl_->channel);
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
    BroadcastChannel& channel = std::get<BroadcastChannel>(impl_->channel);
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

struct SimpleSubscriber::Impl {
    std::shared_ptr<SubscriberClaim> claim;
};

SimpleSubscriber::SimpleSubscriber(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SimpleSubscriber::SimpleSubscriber(SimpleSubscriber&& other) noexcept = default;
SimpleSubscriber& SimpleSubscriber::operator=(
    SimpleSubscriber&& other) noexcept = default;
SimpleSubscriber::~SimpleSubscriber() = default;

SimpleTopicMode SimpleSubscriber::mode() const noexcept {
    return impl_ == nullptr || impl_->claim == nullptr
               ? SimpleTopicMode::kSpsc
               : impl_->claim->mode;
}

const SimpleTypeDescriptor& SimpleSubscriber::type() const noexcept {
    static const SimpleTypeDescriptor kEmpty;
    return impl_ == nullptr || impl_->claim == nullptr ? kEmpty
                                                       : impl_->claim->type;
}

Result<BorrowedBytes> SimpleSubscriber::TryPoll() {
    if (impl_ == nullptr || impl_->claim == nullptr ||
        impl_->claim->node == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "subscriber is not bound");
    }
    const std::shared_ptr<SubscriberClaim> claim = impl_->claim;
    MINO_RETURN_IF_ERROR(RecoverState(claim->node));
    bool expected = false;
    if (!claim->borrow_active.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return Status::Error(StatusCode::kWouldBlock,
                             "subscriber already has an active borrow");
    }

    if (claim->mode == SimpleTopicMode::kBroadcast) {
        BroadcastChannel& channel = std::get<BroadcastChannel>(claim->channel);
        SubscriberLeaseCoordinator coordinator(
            channel, *claim->lease_table, nullptr, nullptr,
            &ShmPinTable::CleanupOwnerCallback, &*claim->node->pins);
        const Status heartbeat = coordinator.Heartbeat(
            *claim->broadcast_handle, MonotonicNowNs());
        if (!heartbeat.ok()) {
            claim->borrow_active.store(false, std::memory_order_release);
            return heartbeat;
        }
    }

    auto finish = [&](auto polled,
                      bool payload_cleanup_by_channel) -> Result<BorrowedBytes> {
        if (!polled.ok()) {
            claim->borrow_active.store(false, std::memory_order_release);
            return polled.status();
        }
        auto borrow = std::move(*polled);
        const IndexSlotSnapshot& slot = *borrow.slot();
        const SimpleTypeDescriptor& type = claim->type;
        if (slot.msg_type != type.message_type ||
            slot.schema_version != type.schema_version ||
            slot.schema_short_id != type.schema_short_id ||
            slot.schema_layout_version != type.layout_version ||
            slot.payload.IsNull() ||
            (slot.flags & kIndexSlotFlagReservedMask) != 0 ||
            slot.flags != type.index_flags ||
            (type.fixed_size != 0 && slot.payload_len != type.fixed_size)) {
            const Status ack = std::move(borrow).Ack();
            if (ack.ok() && !payload_cleanup_by_channel) {
                (void)claim->node->pins->RetirePayload(slot.payload);
            }
            claim->borrow_active.store(false, std::memory_order_release);
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "message does not match topic schema");
        }
        const ShmPinContract contract{
            .type_id = TypeId{type.type_id},
            .schema_short_id = type.schema_short_id,
            .layout_version = type.layout_version,
            .object_size = slot.payload_len,
        };
        Result<ShmPinToken> pin = claim->node->pins->Pin(
            slot.payload, contract, claim->owner);
        if (!pin.ok()) {
            const Status ack = std::move(borrow).Ack();
            if (ack.ok() && !payload_cleanup_by_channel) {
                (void)claim->node->pins->RetirePayload(slot.payload);
            }
            claim->borrow_active.store(false, std::memory_order_release);
            return pin.status();
        }
        auto result = std::unique_ptr<BorrowedBytes::Impl>(
            new BorrowedBytes::Impl());
        result->claim = claim;
        result->borrow = std::move(borrow);
        result->data = static_cast<const std::byte*>(pin->data());
        result->size = slot.payload_len;
        result->handle = slot.payload;
        result->type = type;
        result->payload_cleanup_by_channel = payload_cleanup_by_channel;
        result->pin = std::move(*pin);
        result->active = true;
        return BorrowedBytes(std::move(result));
    };

    if (claim->mode == SimpleTopicMode::kSpsc) {
        return finish(std::get<SpscChannel>(claim->channel).Poll(), false);
    }
    if (claim->mode == SimpleTopicMode::kMpsc) {
        MpscChannel& channel = std::get<MpscChannel>(claim->channel);
        Result<MpscChannel::Borrow> polled = channel.Poll();
        if (!polled.ok() &&
            polled.status().code() == StatusCode::kWouldBlock) {
            (void)channel.AbortOrphanedReservations(MonotonicNowNs());
            polled = channel.Poll();
        }
        return finish(std::move(polled), false);
    }
    return finish(
        std::get<BroadcastChannel>(claim->channel)
            .Poll(claim->broadcast_handle->subscriber, claim->owner),
        true);
}

Result<BorrowedBytes> SimpleSubscriber::Poll(Deadline deadline) {
    for (;;) {
        Result<BorrowedBytes> message = TryPoll();
        if (message.ok()) return message;
        if (message.status().code() != StatusCode::kWouldBlock) {
            return message.status();
        }
        if (deadline.expired()) {
            return Status::Error(StatusCode::kTimeout,
                                 "subscriber poll deadline expired");
        }
        std::this_thread::yield();
    }
}

struct SimpleNode::Impl {
    std::shared_ptr<SimpleNodeState> state;
};

SimpleNode::SimpleNode(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SimpleNode::SimpleNode(SimpleNode&& other) noexcept = default;
SimpleNode& SimpleNode::operator=(SimpleNode&& other) noexcept = default;
SimpleNode::~SimpleNode() = default;

Result<uint64_t> SimpleNode::RequiredBytes(const SimpleNodeOptions& options) {
    MINO_ASSIGN_OR_RETURN(SimpleNodeOptions normalized,
                          NormalizeOptions(options));
    MINO_ASSIGN_OR_RETURN(SegmentLayout layout, ComputeLayout(normalized));
    if (normalized.segment_bytes == 0) return layout.total_size;
    if (normalized.segment_bytes < layout.total_size) {
        return Errorf(StatusCode::kInvalidArgument,
                      "segment_bytes is smaller than RequiredBytes",
                      layout.total_size, normalized.segment_bytes);
    }
    uint64_t rounded = 0;
    if (!CheckedAlignUpU64(normalized.segment_bytes, HostPageSize(), &rounded)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "segment_bytes alignment overflows");
    }
    return rounded;
}

Result<SimpleNode> SimpleNode::Create(std::string_view name,
                                      SimpleNodeOptions options) {
    MINO_RETURN_IF_ERROR(ValidateName(name));
    MINO_ASSIGN_OR_RETURN(SimpleNodeOptions normalized,
                          NormalizeOptions(options));
    MINO_ASSIGN_OR_RETURN(uint64_t bytes, RequiredBytes(normalized));
    MINO_RETURN_IF_ERROR(CheckShmBudget(bytes));
    normalized.segment_bytes = 0;
    MINO_ASSIGN_OR_RETURN(SegmentLayout layout, ComputeLayout(normalized));

    SharedMemoryCreateOptions create;
    create.name = std::string(name);
    create.size = bytes;
    create.use_huge_pages = false;
    MINO_ASSIGN_OR_RETURN(SharedMemorySegment segment,
                          SharedMemorySegment::Create(create));
    std::memset(segment.base(), 0, static_cast<size_t>(segment.size()));
    auto* header = std::construct_at(
        static_cast<ManifestHeader*>(segment.base()));
    header->version = kVersion;
    header->header_size = sizeof(ManifestHeader);
    header->total_size = segment.size();
    header->topic_slots = normalized.topic_slots;
    header->queue_depth = normalized.queue_depth;
    header->max_payload_bytes = normalized.max_payload_bytes;
    header->max_publishers_per_topic =
        normalized.max_publishers_per_topic;
    header->subscriber_lease_ns = normalized.subscriber_lease_ns;
    header->allocator_offset = layout.allocator_offset;
    header->allocator_extent = layout.allocator_extent;
    header->journal_offset = layout.journal_offset;
    header->journal_extent = layout.journal_extent;
    header->pins_offset = layout.pins_offset;
    header->pins_extent = layout.pins_extent;
    header->channels_offset = layout.channels_offset;
    header->channels_extent = layout.channels_extent;
    header->leases_offset = layout.leases_offset;
    header->leases_extent = layout.leases_extent;

    std::byte* base = BytesOf(segment);
    ClassTableConfig allocator_config;
    allocator_config.classes = {
        {.slot_size = normalized.max_payload_bytes,
         .slot_count = layout.allocator_slot_count},
    };
    MINO_ASSIGN_OR_RETURN(
        CentralSlabAllocator allocator,
        CentralSlabAllocator::Create(base + layout.allocator_offset,
                                     layout.allocator_extent,
                                     allocator_config));
    MINO_ASSIGN_OR_RETURN(
        AllocationJournal journal,
        AllocationJournal::Init(base + layout.journal_offset,
                                layout.journal_extent,
                                layout.journal_capacity,
                                /*handles_per_transaction=*/1, allocator));
    MINO_ASSIGN_OR_RETURN(
        ShmPinTable pins,
        ShmPinTable::Init(base + layout.pins_offset, layout.pins_extent,
                          allocator));

    for (uint32_t i = 0; i < normalized.topic_slots; ++i) {
        TopicSlot& slot = header->topics[i];
        slot.channel_offset =
            layout.channels_offset + layout.channel_extent * i;
        slot.channel_extent = layout.channel_extent;
        slot.lease_offset = layout.leases_offset + layout.lease_extent * i;
        slot.lease_extent = layout.lease_extent;
        slot.capacity = normalized.queue_depth;
        slot.channel_id = i + 1;
    }
    header->magic.store(kMagic, std::memory_order_release);

    auto state = std::make_shared<SimpleNodeState>();
    state->segment = std::move(segment);
    state->allocator = std::move(allocator);
    state->journal = std::move(journal);
    state->pins = std::move(pins);
    state->header = header;
    state->name = std::string(name);
    auto impl = std::unique_ptr<Impl>(new Impl());
    impl->state = std::move(state);
    return SimpleNode(std::move(impl));
}

Result<SimpleNode> SimpleNode::Open(std::string_view name) {
    MINO_RETURN_IF_ERROR(ValidateName(name));
    SharedMemoryOpenOptions open;
    open.name = std::string(name);
    open.read_only = false;
    open.creating_wait_timeout_ms = 2000;
    MINO_ASSIGN_OR_RETURN(SharedMemorySegment segment,
                          SharedMemorySegment::Open(open));
    if (segment.base() == nullptr ||
        segment.size() < sizeof(ManifestHeader) ||
        reinterpret_cast<uintptr_t>(segment.base()) % kCacheLine != 0) {
        return Status::Error(StatusCode::kCorruption,
                             "shared-memory mapping cannot hold SimpleNode");
    }
    auto* header = static_cast<ManifestHeader*>(segment.base());
    MINO_RETURN_IF_ERROR(
        WaitForMagic(header, Deadline::FromNow(std::chrono::seconds(2))));
    MINO_RETURN_IF_ERROR(ValidateHeader(segment, *header));
    std::byte* base = BytesOf(segment);
    MINO_ASSIGN_OR_RETURN(
        CentralSlabAllocator allocator,
        CentralSlabAllocator::Attach(base + header->allocator_offset,
                                     header->allocator_extent));
    MINO_ASSIGN_OR_RETURN(
        AllocationJournal journal,
        AllocationJournal::Attach(base + header->journal_offset,
                                  header->journal_extent, allocator));
    MINO_ASSIGN_OR_RETURN(
        ShmPinTable pins,
        ShmPinTable::Attach(base + header->pins_offset, header->pins_extent,
                            allocator));

    auto state = std::make_shared<SimpleNodeState>();
    state->segment = std::move(segment);
    state->allocator = std::move(allocator);
    state->journal = std::move(journal);
    state->pins = std::move(pins);
    state->header = header;
    state->name = std::string(name);
    MINO_RETURN_IF_ERROR(RecoverState(state));
    auto impl = std::unique_ptr<Impl>(new Impl());
    impl->state = std::move(state);
    return SimpleNode(std::move(impl));
}

Status SimpleNode::Unlink(std::string_view name) {
    MINO_RETURN_IF_ERROR(ValidateName(name));
    return SharedMemorySegment::Unlink(std::string(name));
}

Result<SimplePublisher> SimpleNode::Advertise(
    std::string_view topic, SimpleTopicOptions options) {
    return AdvertiseTyped(topic, options, SimpleTypeDescriptor::Bytes());
}

Result<SimplePublisher> SimpleNode::AdvertiseTyped(
    std::string_view topic, SimpleTopicOptions options,
    const SimpleTypeDescriptor& type) {
    MINO_RETURN_IF_ERROR(ValidateTopic(topic));
    if (impl_ == nullptr || impl_->state == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SimpleNode is not bound");
    }
    const std::shared_ptr<SimpleNodeState> state = impl_->state;
    ManifestHeader* header = state->header;
    MINO_RETURN_IF_ERROR(ValidateTopicOptions(options, header->queue_depth));
    MINO_RETURN_IF_ERROR(ValidateType(type, header->max_payload_bytes));
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
        const Status initialized =
            InitializeTopic(state, *empty, topic, options, type);
        if (!initialized.ok()) {
            empty->state.store(kTopicEmpty, std::memory_order_release);
            return initialized;
        }
        found = empty;
    } else if (!SameTopicConfiguration(*found, options, type)) {
        return Status::Error(
            StatusCode::kSchemaMismatch,
            "topic already exists with different mode, QoS, or schema");
    }

    auto claim = std::make_shared<PublisherClaim>();
    claim->node = state;
    if (options.mode != SimpleTopicMode::kMpsc) {
        MINO_ASSIGN_OR_RETURN(uint64_t token,
                              TryClaimLease(found->publisher));
        claim->lease = &found->publisher;
        claim->token = token;
    }
    MINO_ASSIGN_OR_RETURN(Channel channel, AttachChannel(state, *found));

    auto publisher =
        std::unique_ptr<SimplePublisher::Impl>(new SimplePublisher::Impl());
    publisher->claim = std::move(claim);
    publisher->channel = std::move(channel);
    publisher->type = type;
    publisher->options = options;
    publisher->channel_id = found->channel_id;
    publisher->max_payload_bytes = header->max_payload_bytes;
    const ProcessIdentity owner = ProcessIdentity::Current();
    publisher->mpsc_identity = MpscChannel::ProducerIdentity{
        .owner = owner,
        .publisher_id =
            header->publisher_sequence.fetch_add(1,
                                                 std::memory_order_relaxed) + 1,
    };
    return SimplePublisher(std::move(publisher));
}

Result<SimpleSubscriber> SimpleNode::Subscribe(std::string_view topic,
                                               Deadline deadline) {
    return SubscribeTyped(topic, SimpleTypeDescriptor::Bytes(), deadline);
}

Result<SimpleSubscriber> SimpleNode::SubscribeTyped(
    std::string_view topic, const SimpleTypeDescriptor& type,
    Deadline deadline) {
    MINO_RETURN_IF_ERROR(ValidateTopic(topic));
    if (impl_ == nullptr || impl_->state == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SimpleNode is not bound");
    }
    const std::shared_ptr<SimpleNodeState> state = impl_->state;
    MINO_RETURN_IF_ERROR(RecoverState(state));
    ManifestHeader* header = state->header;
    TopicSlot* found = nullptr;
    for (;;) {
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
            return Status::Error(StatusCode::kNotFound,
                                 "topic is not advertised");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        MINO_RETURN_IF_ERROR(RecoverState(state));
    }
    if (!SameType(found->type, type)) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "subscriber type does not match topic schema");
    }

    auto claim = std::make_shared<SubscriberClaim>();
    claim->node = state;
    claim->owner = ProcessIdentity::Current();
    claim->type = type;
    claim->mode = static_cast<SimpleTopicMode>(found->mode);
    MINO_ASSIGN_OR_RETURN(claim->channel, AttachChannel(state, *found));

    if (claim->mode == SimpleTopicMode::kBroadcast) {
        MINO_ASSIGN_OR_RETURN(SubscriberLeaseTable leases,
                              AttachLeaseTable(state, *found));
        BroadcastChannel& channel =
            std::get<BroadcastChannel>(claim->channel);
        SubscriberLeaseCoordinator coordinator(
            channel, leases, nullptr, nullptr,
            &ShmPinTable::CleanupOwnerCallback, &*state->pins);
        (void)coordinator.EvictExpired(MonotonicNowNs(),
                                       header->subscriber_lease_ns);
        std::optional<SubscriberLeaseHandle> handle;
        for (uint32_t id = 0;
             id < SubscriberLeaseTable::kMaxSubscribers; ++id) {
            Result<SubscriberLeaseHandle> registered = coordinator.Register(
                SubscriberId{id}, claim->owner, MonotonicNowNs());
            if (registered.ok()) {
                handle = *registered;
                break;
            }
            if (registered.status().code() != StatusCode::kAlreadyExists) {
                return registered.status();
            }
        }
        if (!handle.has_value()) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "broadcast subscriber table is full");
        }
        claim->lease_table = leases;
        claim->broadcast_handle = *handle;
    } else {
        MINO_ASSIGN_OR_RETURN(uint64_t token,
                              TryClaimLease(found->subscriber));
        claim->lease = &found->subscriber;
        claim->token = token;
    }

    auto subscriber =
        std::unique_ptr<SimpleSubscriber::Impl>(new SimpleSubscriber::Impl());
    subscriber->claim = std::move(claim);
    return SimpleSubscriber(std::move(subscriber));
}

Status SimpleNode::Recover() {
    if (impl_ == nullptr || impl_->state == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "SimpleNode is not bound");
    }
    return RecoverState(impl_->state,
                        /*immediate_broadcast_recovery=*/true);
}

uint64_t SimpleNode::size_bytes() const noexcept {
    return impl_ == nullptr || impl_->state == nullptr ||
                   !impl_->state->segment.has_value()
               ? 0
               : impl_->state->segment->size();
}

const std::string& SimpleNode::name() const noexcept {
    static const std::string kEmpty;
    return impl_ == nullptr || impl_->state == nullptr ? kEmpty
                                                       : impl_->state->name;
}

}  // namespace mino
