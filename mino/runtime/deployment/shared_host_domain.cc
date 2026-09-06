// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/deployment/shared_host_domain.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <new>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>

#include <signal.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include "mino/common/result.h"
#include "mino/platform/shared_memory.h"
#include "mino/shm/channel/broadcast_channel.h"
#include "mino/shm/channel/index_slot.h"

namespace mino::deployment {
namespace {

constexpr uint64_t kCacheLine = 64;
constexpr uint64_t kMagic = 0x4D494E4F53484431ull;  // "MINOSHD1"
constexpr uint32_t kVersion = 1;
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
    char name[64]{};
    SchemaPod schema{};
    uint64_t channel_offset = 0;
    uint64_t channel_extent = 0;
    uint64_t payload_offset = 0;
    uint64_t payload_stride = 0;
    uint64_t channel_id = 0;
    EndpointLease publisher;
    unsigned char pad[32]{};
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
    uint32_t max_payload_bytes = 0;
    uint32_t reserved0 = 0;
    uint64_t peer_lease_ns = 0;
    uint64_t channels_offset = 0;
    uint64_t channels_extent = 0;
    uint64_t payloads_offset = 0;
    uint64_t payloads_extent = 0;
    uint64_t channel_extent = 0;
    uint64_t payload_extent = 0;
    uint64_t payload_stride = 0;
    EndpointLease directory_lock;
    PeerSlot peers[kSharedHostMaxPeerSlots];
    TopicSlot topics[kSharedHostMaxTopicSlots];
};
static_assert(alignof(DomainHeader) == kCacheLine);
static_assert(std::is_standard_layout_v<DomainHeader>);
static_assert(offsetof(DomainHeader, magic) == 0);

struct SegmentLayout {
    uint64_t total_size = 0;
    uint64_t channels_offset = 0;
    uint64_t channels_extent = 0;
    uint64_t payloads_offset = 0;
    uint64_t payloads_extent = 0;
    uint64_t channel_extent = 0;
    uint64_t payload_extent = 0;
    uint64_t payload_stride = 0;
};

struct DomainState {
    std::optional<SharedMemorySegment> segment;
    DomainHeader* header = nullptr;
    std::string name;
    std::optional<uint32_t> peer_index;
    uint64_t peer_token = 0;
};

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

Result<SegmentLayout> ComputeLayout(const SharedHostDomainOptions& options) {
    SegmentLayout layout;
    layout.channel_extent = BroadcastChannel::RequiredSize(options.queue_depth);
    if (!CheckedAlignUpU64(layout.channel_extent, kCacheLine,
                           &layout.channel_extent)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel extent overflows");
    }
    if (!CheckedAlignUpU64(options.max_payload_bytes, kCacheLine,
                           &layout.payload_stride)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "payload stride overflows");
    }
    if (!CheckedMulU64(layout.payload_stride, options.queue_depth,
                       &layout.payload_extent) ||
        !CheckedAlignUpU64(layout.payload_extent, kCacheLine,
                           &layout.payload_extent)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "payload extent overflows");
    }
    layout.channels_offset = sizeof(DomainHeader);
    uint64_t next = 0;
    if (!CheckedMulU64(layout.channel_extent, options.topic_slots,
                       &layout.channels_extent) ||
        !CheckedAddU64(layout.channels_offset, layout.channels_extent, &next) ||
        !CheckedAlignUpU64(next, kCacheLine, &layout.payloads_offset) ||
        !CheckedMulU64(layout.payload_extent, options.topic_slots,
                       &layout.payloads_extent) ||
        !CheckedAddU64(layout.payloads_offset, layout.payloads_extent, &next) ||
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

bool RecoverLease(EndpointLease& lease) noexcept {
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
    return lease.control.compare_exchange_strong(observed, cleared,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire);
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
        header.max_subscribers == 0 || header.max_payload_bytes == 0) {
        return Status::Error(StatusCode::kCorruption,
                             "SharedHostDomain header fields are invalid");
    }
    return Status::Ok();
}

void ClearPeer(PeerSlot& peer) noexcept {
    peer.state.store(kPeerEmpty, std::memory_order_release);
    peer.node_id = 0;
    peer.identity = {};
    peer.last_heartbeat_ns.store(0, std::memory_order_release);
}

Status RecoverState(const std::shared_ptr<DomainState>& state) {
    DomainHeader* header = state->header;
    (void)RecoverLease(header->directory_lock);
    const uint64_t now = MonotonicNowNs();
    for (uint32_t i = 0; i < header->peer_slots; ++i) {
        PeerSlot& peer = header->peers[i];
        if (peer.state.load(std::memory_order_acquire) != kPeerActive) continue;
        if (RecoverLease(peer.lease)) {
            ClearPeer(peer);
            continue;
        }
        const ProcessIdentity owner = LoadOwner(peer.lease);
        if (!owner.IsZero() &&
            ProbeProcessIdentity(owner) == ProcessIdentityLiveness::kDead) {
            (void)RecoverLease(peer.lease);
            ClearPeer(peer);
            continue;
        }
        const uint64_t heartbeat =
            peer.last_heartbeat_ns.load(std::memory_order_acquire);
        if (heartbeat != 0 && now > heartbeat &&
            now - heartbeat > header->peer_lease_ns && !owner.IsZero() &&
            ProbeProcessIdentity(owner) == ProcessIdentityLiveness::kDead) {
            (void)RecoverLease(peer.lease);
            ClearPeer(peer);
        }
    }
    for (uint32_t i = 0; i < header->topic_slots; ++i) {
        TopicSlot& topic = header->topics[i];
        if (topic.state.load(std::memory_order_acquire) != kTopicReady) continue;
        (void)RecoverLease(topic.publisher);
    }
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
                       const SchemaPod& schema) {
    DomainHeader* header = state->header;
    std::byte* base = BytesOf(*state);
    std::memset(base + slot.channel_offset, 0,
                static_cast<size_t>(slot.channel_extent));
    std::memset(base + slot.payload_offset, 0,
                static_cast<size_t>(header->payload_extent));
    MINO_RETURN_IF_ERROR(
        BroadcastChannel::Init(base + slot.channel_offset, header->queue_depth)
            .status());
    WriteTopicName(&slot, topic);
    slot.schema = schema;
    slot.max_payload_bytes = header->max_payload_bytes;
    slot.capacity = header->queue_depth;
    slot.max_subscribers = header->max_subscribers;
    slot.payload_stride = header->payload_stride;
    slot.state.store(kTopicReady, std::memory_order_release);
    return Status::Ok();
}

}  // namespace

struct SharedHostPublisher::Impl {
    std::shared_ptr<DomainState> state;
    TopicSlot* slot = nullptr;
    uint64_t token = 0;
    std::optional<BroadcastChannel> channel;

    ~Impl() {
        if (slot != nullptr && token != 0) {
            ReleaseLease(&slot->publisher, token);
        }
    }
};

struct SharedHostSubscriber::Impl {
    std::shared_ptr<DomainState> state;
    TopicSlot* slot = nullptr;
    std::optional<BroadcastChannel> channel;
    BroadcastChannel::SubscriberHandle handle{};

    ~Impl() {
        if (channel.has_value() && handle.generation != 0) {
            (void)channel->UnregisterSubscriber(handle.id, handle.generation);
        }
    }
};

struct SharedHostDomain::Impl {
    std::shared_ptr<DomainState> state;
};

SharedHostPublisher::SharedHostPublisher(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SharedHostPublisher::SharedHostPublisher(SharedHostPublisher&&) noexcept =
    default;
SharedHostPublisher& SharedHostPublisher::operator=(
    SharedHostPublisher&&) noexcept = default;
SharedHostPublisher::~SharedHostPublisher() = default;

bool SharedHostPublisher::active() const noexcept {
    return impl_ != nullptr && impl_->channel.has_value() && impl_->token != 0;
}

Status SharedHostPublisher::Publish(std::span<const std::byte> payload) {
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
    MINO_RETURN_IF_ERROR(RecoverState(impl_->state));
    MINO_ASSIGN_OR_RETURN(BroadcastChannel::Reservation reserved,
                          impl_->channel->TryReserve());
    const uint64_t sequence = reserved.sequence();
    const uint64_t physical = sequence & (impl_->slot->capacity - 1u);
    const uint64_t generation64 = sequence / impl_->slot->capacity + 1u;
    if (generation64 > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "payload generation space is exhausted");
    }
    std::byte* base = BytesOf(*impl_->state);
    const uint64_t offset =
        impl_->slot->payload_offset + physical * impl_->slot->payload_stride;
    std::memcpy(base + offset, payload.data(), payload.size());
    IndexSlot* slot = reserved.slot();
    slot->msg_type = static_cast<uint32_t>(impl_->slot->schema.short_id);
    slot->schema_version = impl_->slot->schema.schema_version;
    slot->schema_short_id = impl_->slot->schema.short_id;
    slot->schema_layout_version = impl_->slot->schema.layout_version;
    slot->reserved0 = 0;
    slot->timestamp_ns = MonotonicNowNs();
    slot->payload = ShmHandle{
        .offset = offset,
        .generation = static_cast<uint32_t>(generation64),
        .region_id = 1,
    };
    slot->payload_len = static_cast<uint32_t>(payload.size());
    slot->flags = 0;
    return std::move(reserved).Commit();
}

SharedHostSubscriber::SharedHostSubscriber(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SharedHostSubscriber::SharedHostSubscriber(SharedHostSubscriber&&) noexcept =
    default;
SharedHostSubscriber& SharedHostSubscriber::operator=(
    SharedHostSubscriber&&) noexcept = default;
SharedHostSubscriber::~SharedHostSubscriber() = default;

bool SharedHostSubscriber::active() const noexcept {
    return impl_ != nullptr && impl_->channel.has_value();
}

Result<std::vector<std::byte>> SharedHostSubscriber::TryPoll() {
    if (!active()) {
        return Status::Error(StatusCode::kUnavailable, "subscriber is closed");
    }
    MINO_RETURN_IF_ERROR(RecoverState(impl_->state));
    MINO_RETURN_IF_ERROR(
        impl_->channel->Heartbeat(impl_->handle, MonotonicNowNs()));
    MINO_ASSIGN_OR_RETURN(BroadcastChannel::Borrow borrowed,
                          impl_->channel->Poll(impl_->handle));
    const IndexSlotSnapshot& snapshot = *borrowed;
    if (snapshot.payload_len == 0 ||
        snapshot.payload_len > impl_->slot->max_payload_bytes) {
        return Status::Error(StatusCode::kCorruption,
                             "shared topic payload length is invalid");
    }
    std::byte* base = BytesOf(*impl_->state);
    const std::byte* src = base + snapshot.payload.offset;
    std::vector<std::byte> out(snapshot.payload_len);
    std::memcpy(out.data(), src, snapshot.payload_len);
    MINO_RETURN_IF_ERROR(std::move(borrowed).Ack());
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
    header->max_payload_bytes = normalized.max_payload_bytes;
    header->peer_lease_ns = normalized.peer_lease_ns;
    header->channels_offset = layout.channels_offset;
    header->channels_extent = layout.channels_extent;
    header->payloads_offset = layout.payloads_offset;
    header->payloads_extent = layout.payloads_extent;
    header->channel_extent = layout.channel_extent;
    header->payload_extent = layout.payload_extent;
    header->payload_stride = layout.payload_stride;
    for (uint32_t i = 0; i < normalized.topic_slots; ++i) {
        TopicSlot& slot = header->topics[i];
        slot.channel_offset =
            layout.channels_offset + layout.channel_extent * i;
        slot.channel_extent = layout.channel_extent;
        slot.payload_offset =
            layout.payloads_offset + layout.payload_extent * i;
        slot.payload_stride = layout.payload_stride;
        slot.channel_id = i + 1;
    }
    header->magic.store(kMagic, std::memory_order_release);

    auto state = std::make_shared<DomainState>();
    state->segment.emplace(std::move(segment));
    state->header = header;
    state->name = std::string(name);
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
    auto state = std::make_shared<DomainState>();
    state->segment.emplace(std::move(segment));
    state->header = header;
    state->name = std::string(name);
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
        const uint64_t pub =
            slot.publisher.control.load(std::memory_order_acquire);
        out.push_back(SharedTopicInfo{
            .name = std::string(slot.name),
            .schema = FromPod(slot.schema),
            .capacity = slot.capacity,
            .max_subscribers = slot.max_subscribers,
            .max_payload_bytes = slot.max_payload_bytes,
            .publisher_active = (pub & kLeaseStateMask) == kLeaseActive,
        });
    }
    return out;
}

Result<SharedHostPublisher> SharedHostDomain::Advertise(
    std::string_view topic, const schema::SchemaIdentity& schema) {
    MINO_RETURN_IF_ERROR(ValidateTopic(topic));
    MINO_RETURN_IF_ERROR(EnsureJoined(impl_->state));
    auto state = impl_->state;
    DomainHeader* header = state->header;
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
        MINO_RETURN_IF_ERROR(InitializeTopic(state, *empty, topic, wanted));
        found = empty;
    } else if (!SameSchema(found->schema, wanted) ||
               found->capacity != header->queue_depth ||
               found->max_subscribers != header->max_subscribers ||
               found->max_payload_bytes != header->max_payload_bytes) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "topic already exists with different schema/QoS");
    }
    directory_guard.Disarm();
    ReleaseLease(&header->directory_lock, directory_token);

    MINO_ASSIGN_OR_RETURN(
        uint64_t pub_token,
        ClaimLeaseUntil(found->publisher,
                        Deadline::FromNow(std::chrono::seconds(2))));
    std::byte* base = BytesOf(*state);
    MINO_ASSIGN_OR_RETURN(BroadcastChannel channel,
                          BroadcastChannel::Attach(base + found->channel_offset));
    auto impl = std::make_unique<SharedHostPublisher::Impl>();
    impl->state = state;
    impl->slot = found;
    impl->token = pub_token;
    impl->channel = std::move(channel);
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
    std::byte* base = BytesOf(*state);
    MINO_ASSIGN_OR_RETURN(BroadcastChannel channel,
                          BroadcastChannel::Attach(base + found->channel_offset));
    BroadcastChannel::SubscriberHandle handle{};
    bool registered = false;
    for (uint32_t id = 0; id < found->max_subscribers; ++id) {
        Result<BroadcastChannel::SubscriberHandle> claimed =
            channel.RegisterSubscriber(SubscriberId{id},
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
    auto impl = std::make_unique<SharedHostSubscriber::Impl>();
    impl->state = state;
    impl->slot = found;
    impl->channel = std::move(channel);
    impl->handle = handle;
    return SharedHostSubscriber(std::move(impl));
}

Status SharedHostDomain::Recover() {
    return RecoverState(impl_->state);
}

}  // namespace mino::deployment
