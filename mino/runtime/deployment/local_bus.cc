// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/deployment/local_bus.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <exception>
#include <limits>
#include <mutex>
#include <new>
#include <span>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>

#include "mino/platform/process_identity.h"
#include "mino/registry/coordinator.h"
#include "mino/registry/id_allocator.h"
#include "mino/registry/metadata.h"
#include "mino/shm/channel/broadcast_channel.h"
#include "mino/transport/transport_driver.h"
#include "mino/transport/transport_switcher.h"
#include "mino/upgrade/routing_catalog.h"

namespace mino::deployment {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

uint64_t MonotonicNowNs() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

uint64_t AlignUp64(uint64_t value) noexcept {
    return (value + 63u) & ~uint64_t{63u};
}

bool CompleteSchema(const schema::SchemaIdentity& identity) noexcept {
    return identity.short_id() != 0 && identity.schema_version() != 0 &&
           identity.layout_version() != 0 &&
           std::any_of(identity.canonical_digest().begin(),
                       identity.canonical_digest().end(),
                       [](std::byte value) { return value != std::byte{0}; });
}

Status ValidateConfig(const LocalBusConfig& config) {
    if (config.node_id.value == 0 || config.security_domain_id.value == 0 ||
        config.lease_epoch == 0 ||
        config.lease_duration_ns == 0 || config.region_id == 0 ||
        config.region_bytes == 0 ||
        config.region_bytes > kMaximumLocalRegionBytes ||
        config.topic_id_state_path.empty() ||
        config.topic_id_state_path.string().size() > 4096 ||
        config.topics.empty() ||
        config.topics.size() > kMaximumLocalDeploymentTopics) {
        return Invalid("local Bus deployment configuration is incomplete or out of bounds");
    }
    uint64_t charged = 0;
    std::vector<std::string_view> names;
    names.reserve(config.topics.size());
    for (const LocalTopicConfig& topic : config.topics) {
        if (topic.name.empty() ||
            topic.name.size() > registry::kMaxTopicNameBytes ||
            topic.name.find('\0') != std::string::npos ||
            !CompleteSchema(topic.schema) || topic.channel_capacity < 2 ||
            topic.channel_capacity > kMaximumLocalChannelCapacity ||
            (topic.channel_capacity & (topic.channel_capacity - 1)) != 0 ||
            topic.max_subscribers == 0 ||
            topic.max_subscribers > BroadcastChannel::kMaxSubscribers ||
            topic.max_payload_bytes == 0 ||
            topic.max_payload_bytes > kMaxBusCanonicalPayloadBytes) {
            return Invalid("local Bus topic configuration is incomplete or out of bounds");
        }
        const uint64_t stride = AlignUp64(topic.max_payload_bytes);
        const uint64_t channel =
            AlignUp64(BroadcastChannel::RequiredSize(topic.channel_capacity));
        if (stride > std::numeric_limits<uint64_t>::max() /
                         topic.channel_capacity ||
            channel > std::numeric_limits<uint64_t>::max() -
                          stride * topic.channel_capacity ||
            charged > std::numeric_limits<uint64_t>::max() - channel -
                          stride * topic.channel_capacity) {
            return Exhausted("local Bus region size overflows");
        }
        charged += channel + stride * topic.channel_capacity;
        names.push_back(topic.name);
    }
    std::sort(names.begin(), names.end());
    if (std::adjacent_find(names.begin(), names.end()) != names.end()) {
        return Invalid("local Bus topic names must be unique");
    }
    if (charged > config.region_bytes) {
        return Exhausted("configured topics exceed the local region byte budget");
    }
    return Status::Ok();
}

class DurableFileIdAllocator final : public registry::IdAllocator {
public:
    static Result<std::shared_ptr<DurableFileIdAllocator>> Open(
        const std::filesystem::path& path) {
        const std::filesystem::path parent =
            path.parent_path().empty() ? std::filesystem::path(".")
                                       : path.parent_path();
        const int directory_fd =
            ::open(parent.c_str(), O_RDONLY | O_CLOEXEC | O_DIRECTORY);
        if (directory_fd < 0) {
            return Status::Error(StatusCode::kNotFound,
                                 "cannot open topic ID state directory");
        }
        const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC |
                                               O_NOFOLLOW,
                              0600);
        if (fd < 0) {
            ::close(directory_fd);
            return Status::Error(errno == ELOOP ? StatusCode::kPermissionDenied
                                               : StatusCode::kUnavailable,
                                 "cannot open durable topic ID state");
        }
        if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
            ::close(fd);
            ::close(directory_fd);
            return Status::Error(StatusCode::kWouldBlock,
                                 "durable topic ID state is already in use");
        }
        struct stat state {};
        if (::fstat(fd, &state) != 0 || !S_ISREG(state.st_mode) ||
            state.st_nlink != 1) {
            ::close(fd);
            ::close(directory_fd);
            return Status::Error(StatusCode::kPermissionDenied,
                                 "topic ID state must be a single-link regular file");
        }
        uint32_t high_watermark = 0;
        if (state.st_size != 0) {
            if (state.st_size != static_cast<off_t>(kStateBytes)) {
                ::close(fd);
                ::close(directory_fd);
                return Status::Error(StatusCode::kCorruption,
                                     "topic ID state has an invalid size");
            }
            std::array<std::byte, kStateBytes> bytes{};
            const ssize_t read = ::pread(fd, bytes.data(), bytes.size(), 0);
            if (read != static_cast<ssize_t>(bytes.size()) ||
                std::memcmp(bytes.data(), kMagic.data(), kMagic.size()) != 0) {
                ::close(fd);
                ::close(directory_fd);
                return Status::Error(StatusCode::kCorruption,
                                     "topic ID state header is corrupt");
            }
            high_watermark = ReadU32(bytes, 8);
            if (ReadU32(bytes, 12) != (high_watermark ^ kFence)) {
                ::close(fd);
                ::close(directory_fd);
                return Status::Error(StatusCode::kCorruption,
                                     "topic ID state fence is corrupt");
            }
        } else if (::fsync(directory_fd) != 0) {
            ::close(fd);
            ::close(directory_fd);
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot durably create topic ID state");
        }
        return std::shared_ptr<DurableFileIdAllocator>(
            new DurableFileIdAllocator(fd, directory_fd, high_watermark));
    }

    ~DurableFileIdAllocator() override {
        if (fd_ >= 0) {
            static_cast<void>(::flock(fd_, LOCK_UN));
            ::close(fd_);
        }
        if (directory_fd_ >= 0) ::close(directory_fd_);
    }

    registry::IdAllocatorDurability durability() const noexcept override {
        return registry::IdAllocatorDurability::kDurable;
    }

    Result<TopicId> AllocateTopicId() override {
        std::lock_guard lock(mutex_);
        if (high_watermark_ == std::numeric_limits<uint32_t>::max()) {
            return Exhausted("topic ID space is exhausted");
        }
        const uint32_t next = high_watermark_ + 1;
        std::array<std::byte, kStateBytes> bytes{};
        std::memcpy(bytes.data(), kMagic.data(), kMagic.size());
        WriteU32(&bytes, 8, next);
        WriteU32(&bytes, 12, next ^ kFence);
        const ssize_t written = ::pwrite(fd_, bytes.data(), bytes.size(), 0);
        if (written != static_cast<ssize_t>(bytes.size()) ||
            ::ftruncate(fd_, static_cast<off_t>(bytes.size())) != 0) {
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot durably advance topic ID state");
        }
#if defined(__APPLE__)
        const int sync_result = ::fsync(fd_);
#else
        const int sync_result = ::fdatasync(fd_);
#endif
        if (sync_result != 0) {
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot durably advance topic ID state");
        }
        high_watermark_ = next;
        return TopicId{next};
    }

private:
    static constexpr size_t kStateBytes = 16;
    static constexpr std::array<char, 8> kMagic = {'M', 'I', 'N', 'O',
                                                   'I', 'D', '1', '\0'};
    static constexpr uint32_t kFence = 0xa59c3f17u;

    DurableFileIdAllocator(int fd, int directory_fd,
                           uint32_t high_watermark) noexcept
        : fd_(fd), directory_fd_(directory_fd),
          high_watermark_(high_watermark) {}

    static uint32_t ReadU32(
        const std::array<std::byte, kStateBytes>& bytes, size_t offset) noexcept {
        uint32_t value = 0;
        for (size_t index = 0; index < 4; ++index) {
            value |= static_cast<uint32_t>(
                         static_cast<uint8_t>(bytes[offset + index]))
                     << (index * 8u);
        }
        return value;
    }

    static void WriteU32(std::array<std::byte, kStateBytes>* bytes,
                         size_t offset, uint32_t value) noexcept {
        for (size_t index = 0; index < 4; ++index) {
            (*bytes)[offset + index] =
                static_cast<std::byte>(value >> (index * 8u));
        }
    }

    int fd_ = -1;
    int directory_fd_ = -1;
    uint32_t high_watermark_ = 0;
    std::mutex mutex_;
};

class MonotonicParticipantIds final : public ParticipantIdAllocator {
public:
    Result<PublisherParticipantIdentity> AllocatePublisher() override {
        const uint64_t value = next_publisher_.fetch_add(1);
        const uint64_t generation = next_generation_.fetch_add(1);
        if (value == 0 || generation == 0) {
            return Exhausted("publisher participant ID space is exhausted");
        }
        return PublisherParticipantIdentity{PublisherId{value}, generation};
    }

    Result<SubscriberParticipantIdentity> AllocateSubscriber() override {
        const uint32_t value = next_subscriber_.fetch_add(1);
        const uint64_t generation = next_generation_.fetch_add(1);
        if (value == 0 || generation == 0) {
            return Exhausted("subscriber participant ID space is exhausted");
        }
        return SubscriberParticipantIdentity{SubscriberId{value}, generation};
    }

private:
    std::atomic<uint64_t> next_publisher_{1};
    std::atomic<uint32_t> next_subscriber_{1};
    std::atomic<uint64_t> next_generation_{1};
};

class LocalAccessValidator final : public transport::RouteAccessValidator {
public:
    explicit LocalAccessValidator(NodeId node_id) noexcept : node_id_(node_id) {}
    uint64_t version() const noexcept override { return 1; }
    Status Validate(const registry::TopicMetadata&, NodeId source,
                    NodeId target) const override {
        return source == node_id_ && target == node_id_
                   ? Status::Ok()
                   : Status::Error(StatusCode::kPermissionDenied,
                                   "local deployment rejects non-local routes");
    }

private:
    NodeId node_id_;
};

class ExactSchemaValidator final : public transport::SchemaRouteValidator {
public:
    uint64_t version() const noexcept override { return 1; }
    Status Validate(
        const registry::TopicMetadata& topic, NodeId,
        const schema::SchemaIdentity& publisher_schema) const override {
        return registry::SchemaIdentityEqual(topic.schema, publisher_schema)
                   ? Status::Ok()
                   : Status::Error(StatusCode::kSchemaMismatch,
                                   "publisher schema differs from the local topic");
    }
};

class LocalOnlyDispatcher final : public BridgeDispatcher {
public:
    Status Dispatch(const BridgeDispatchRequest& request) override {
        if (request.route == nullptr) {
            return Invalid("local dispatch route is missing");
        }
        for (const transport::TargetRoute& target : request.route->targets()) {
            if (!std::holds_alternative<transport::LocalTargetRoute>(
                    target.transport)) {
                return Status::Error(StatusCode::kUnsupported,
                                     "local deployment has no remote bridge");
            }
        }
        return Status::Ok();
    }
};

struct SlotSourceMetadata {
    bridge::SourceIdentity source;
    uint8_t priority = 0;
};

class LocalTopicBinding;

class LocalPublisherEndpoint final : public BusLocalPublisherEndpoint {
public:
    LocalPublisherEndpoint(std::shared_ptr<LocalTopicBinding> binding,
                           registry::PublisherRegistration registration) noexcept;
    ~LocalPublisherEndpoint() override;
    Result<LocalPublication> Publish(std::span<const std::byte> payload,
                                     uint8_t priority) override;

private:
    std::shared_ptr<LocalTopicBinding> binding_;
    registry::PublisherRegistration registration_;
};

class LocalSubscriberEndpoint final : public BusLocalSubscriberEndpoint {
public:
    LocalSubscriberEndpoint(std::shared_ptr<LocalTopicBinding> binding,
                            BroadcastChannel::SubscriberHandle handle) noexcept;
    ~LocalSubscriberEndpoint() override;
    Result<CanonicalMessage> TryPoll() override;

private:
    std::shared_ptr<LocalTopicBinding> binding_;
    BroadcastChannel::SubscriberHandle handle_;
};

class LocalTopicBinding final
    : public transport::LocalPublicationBinding,
      public std::enable_shared_from_this<LocalTopicBinding> {
public:
    static Result<std::shared_ptr<LocalTopicBinding>> Create(
        const registry::TopicMetadata& metadata, uint32_t region_id,
        size_t max_payload_bytes, bool publisher_fenced) {
        try {
            const uint64_t channel_bytes =
                AlignUp64(BroadcastChannel::RequiredSize(metadata.capacity));
            const uint64_t payload_stride = AlignUp64(max_payload_bytes);
            const uint64_t total_bytes =
                channel_bytes + payload_stride * metadata.capacity;
            void* memory = ::operator new(static_cast<size_t>(total_bytes),
                                          std::align_val_t{64});
            std::memset(memory, 0, static_cast<size_t>(total_bytes));
            Result<BroadcastChannel> channel =
                BroadcastChannel::Init(memory, metadata.capacity);
            if (!channel.ok()) {
                ::operator delete(memory, std::align_val_t{64});
                return channel.status();
            }
            return std::shared_ptr<LocalTopicBinding>(new LocalTopicBinding(
                metadata, region_id, max_payload_bytes, channel_bytes,
                payload_stride, total_bytes, memory, std::move(*channel),
                publisher_fenced));
        } catch (const std::bad_alloc&) {
            return Exhausted("cannot allocate local topic region");
        }
    }

    ~LocalTopicBinding() override {
        ::operator delete(memory_, std::align_val_t{64});
    }

    Result<std::shared_ptr<BusLocalPublisherEndpoint>> OpenPublisher(
        const registry::PublisherRegistration& registration) {
        std::lock_guard lock(lifecycle_mutex_);
        if (publisher_fenced_) {
            return Status::Error(StatusCode::kUnavailable,
                                 "local publisher creation is fenced");
        }
        if (publisher_active_) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "local broadcast topic already has a publisher");
        }
        publisher_active_ = true;
        active_publisher_id_ = registration.publisher_id;
        try {
            return std::shared_ptr<BusLocalPublisherEndpoint>(
                new LocalPublisherEndpoint(shared_from_this(), registration));
        } catch (const std::bad_alloc&) {
            publisher_active_ = false;
            active_publisher_id_ = {};
            return Exhausted("cannot allocate local publisher endpoint");
        }
    }

    void ClosePublisher(
        const registry::PublisherRegistration& registration) noexcept {
        std::lock_guard lock(lifecycle_mutex_);
        if (publisher_active_ && active_publisher_id_ == registration.publisher_id) {
            publisher_active_ = false;
            active_publisher_id_ = {};
        }
    }

    Result<std::shared_ptr<BusLocalSubscriberEndpoint>> OpenSubscriber() {
        std::lock_guard lock(lifecycle_mutex_);
        for (uint32_t index = 0; index < metadata_.max_subscribers; ++index) {
            Result<BroadcastChannel::SubscriberHandle> registered =
                channel_.RegisterSubscriber(SubscriberId{index});
            if (!registered.ok()) {
                if (registered.status().code() == StatusCode::kAlreadyExists) {
                    continue;
                }
                return registered.status();
            }
            try {
                return std::shared_ptr<BusLocalSubscriberEndpoint>(
                    new LocalSubscriberEndpoint(shared_from_this(), *registered));
            } catch (const std::bad_alloc&) {
                static_cast<void>(channel_.UnregisterSubscriber(
                    registered->id, registered->generation));
                return Exhausted("cannot allocate local subscriber endpoint");
            }
        }
        return Exhausted("local topic subscriber capacity is exhausted");
    }

    void CloseSubscriber(BroadcastChannel::SubscriberHandle handle) noexcept {
        std::lock_guard lock(lifecycle_mutex_);
        static_cast<void>(
            channel_.UnregisterSubscriber(handle.id, handle.generation));
    }

    Result<LocalPublication> Publish(
        const registry::PublisherRegistration& registration,
        std::span<const std::byte> payload, uint8_t priority) {
        if (payload.empty()) {
            return Invalid("local Bus canonical payload must not be empty");
        }
        if (payload.size() > max_payload_bytes_) {
            return Exhausted("canonical payload exceeds the configured topic bound");
        }
        std::lock_guard lock(publish_mutex_);
        {
            std::lock_guard lifecycle_lock(lifecycle_mutex_);
            if (!publisher_active_ || publisher_fenced_) {
                return Status::Error(StatusCode::kUnavailable,
                                     "local publisher endpoint is closed or fenced");
            }
            active_publisher_id_ = registration.publisher_id;
        }
        Result<BroadcastChannel::Reservation> reserved = channel_.TryReserve();
        if (!reserved.ok()) {
            if (reserved.status().code() == StatusCode::kWouldBlock) {
                queue_full_events_.fetch_add(1, std::memory_order_relaxed);
                queue_dropped_.fetch_add(1, std::memory_order_relaxed);
            }
            return reserved.status();
        }
        const uint64_t sequence = reserved->sequence();
        const uint64_t physical = sequence & (metadata_.capacity - 1u);
        const uint64_t generation64 = sequence / metadata_.capacity + 1u;
        if (generation64 > std::numeric_limits<uint32_t>::max()) {
            return Exhausted("local payload generation space is exhausted");
        }
        const uint64_t offset = payload_offset_ + physical * payload_stride_;
        std::memcpy(static_cast<std::byte*>(memory_) + offset, payload.data(),
                    payload.size());
        slot_sources_[physical] = SlotSourceMetadata{
            .source = bridge::SourceIdentity{
                registration.owner.node_id.value,
                registration.publisher_id.value,
                registration.generation,
            },
            .priority = priority,
        };
        const uint64_t timestamp_ns = MonotonicNowNs();
        IndexSlot* slot = reserved->slot();
        slot->msg_type = static_cast<uint32_t>(metadata_.schema.short_id());
        slot->schema_version = metadata_.schema.schema_version();
        slot->schema_short_id = metadata_.schema.short_id();
        slot->schema_layout_version = metadata_.schema.layout_version();
        slot->reserved0 = 0;
        slot->timestamp_ns = timestamp_ns;
        slot->payload = ShmHandle{
            .offset = offset,
            .generation = static_cast<uint32_t>(generation64),
            .region_id = region_id_,
        };
        slot->payload_len = static_cast<uint32_t>(payload.size());
        slot->flags = 0;
        const Status committed = std::move(*reserved).Commit();
        if (!committed.ok()) return committed;
        last_published_sequence_.store(sequence + 1, std::memory_order_release);
        published_samples_.fetch_add(1, std::memory_order_relaxed);
        return LocalPublication{
            .source = slot_sources_[physical].source,
            .sequence_num = sequence + 1,
            .timestamp_ns = timestamp_ns,
            .message_type = slot->msg_type,
        };
    }

    Result<CanonicalMessage> Poll(
        BroadcastChannel::SubscriberHandle handle) {
        MINO_RETURN_IF_ERROR(channel_.Heartbeat(handle, MonotonicNowNs()));
        Result<BroadcastChannel::Borrow> borrowed = channel_.Poll(handle);
        if (!borrowed.ok()) {
            if (borrowed.status().code() == StatusCode::kDegraded ||
                borrowed.status().code() == StatusCode::kCorruption) {
                unexplained_loss_count_.fetch_add(1, std::memory_order_relaxed);
            }
            return borrowed.status();
        }
        outstanding_borrows_.fetch_add(1, std::memory_order_acq_rel);
        struct BorrowCounterGuard final {
            std::atomic<uint64_t>* count;
            ~BorrowCounterGuard() {
                count->fetch_sub(1, std::memory_order_acq_rel);
            }
        } borrow_guard{&outstanding_borrows_};
        const IndexSlotSnapshot snapshot = **borrowed;
        if (snapshot.payload.region_id != region_id_ ||
            snapshot.payload_len == 0 ||
            snapshot.payload_len > max_payload_bytes_ ||
            snapshot.payload.offset < payload_offset_ ||
            (snapshot.payload.offset - payload_offset_) % payload_stride_ != 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "local canonical payload handle is invalid");
        }
        const uint64_t physical =
            (snapshot.payload.offset - payload_offset_) / payload_stride_;
        const uint64_t expected_physical =
            snapshot.sequence_num & (metadata_.capacity - 1u);
        const uint64_t expected_generation =
            snapshot.sequence_num / metadata_.capacity + 1u;
        if (physical != expected_physical || physical >= metadata_.capacity ||
            expected_generation > std::numeric_limits<uint32_t>::max() ||
            snapshot.payload.generation != expected_generation ||
            snapshot.payload.offset + snapshot.payload_len > total_bytes_) {
            return Status::Error(StatusCode::kCorruption,
                                 "local canonical payload generation is stale");
        }
        CanonicalMessage message{
            .schema = metadata_.schema,
            .publication = LocalPublication{
                .source = slot_sources_[physical].source,
                .sequence_num = snapshot.sequence_num + 1,
                .timestamp_ns = snapshot.timestamp_ns,
                .message_type = snapshot.msg_type,
            },
            .priority = slot_sources_[physical].priority,
            .payload = {},
        };
        message.payload.resize(snapshot.payload_len);
        std::memcpy(message.payload.data(),
                    static_cast<const std::byte*>(memory_) +
                        snapshot.payload.offset,
                    snapshot.payload_len);
        const Status acked = std::move(*borrowed).Ack();
        if (!acked.ok()) return acked;
        last_consumed_sequence_.store(message.publication.sequence_num,
                                      std::memory_order_release);
        observed_samples_.fetch_add(1, std::memory_order_relaxed);
        return message;
    }

    const registry::TopicMetadata& metadata() const noexcept { return metadata_; }
    uint32_t region_id() const noexcept { return region_id_; }

    Status FencePublisher() noexcept {
        std::lock_guard lock(lifecycle_mutex_);
        publisher_fenced_ = true;
        return Status::Ok();
    }

    Status UnfencePublisher() noexcept {
        std::lock_guard lock(lifecycle_mutex_);
        publisher_fenced_ = false;
        return Status::Ok();
    }

    LocalBusUpgradeTopicStats UpgradeStats(uint64_t now_ns) const noexcept {
        const BroadcastChannel::OperationalStats channel =
            channel_.operational_stats(now_ns);
        bool publisher_active = false;
        bool publisher_fenced = false;
        {
            std::lock_guard lock(lifecycle_mutex_);
            publisher_active = publisher_active_;
            publisher_fenced = publisher_fenced_;
        }
        return LocalBusUpgradeTopicStats{
            .topic_id = metadata_.topic_id,
            .region_id = region_id_,
            .publisher_creation_fenced = publisher_fenced,
            .local_publishers = publisher_active ? 1u : 0u,
            .local_readers = channel.active_subscribers,
            .outstanding_receipts = 0,
            .outstanding_borrows =
                outstanding_borrows_.load(std::memory_order_acquire),
            .queue_depth = channel.depth,
            .last_published_sequence =
                last_published_sequence_.load(std::memory_order_acquire),
            .last_consumed_sequence =
                last_consumed_sequence_.load(std::memory_order_acquire),
            .observed_samples =
                observed_samples_.load(std::memory_order_relaxed),
            .duplicate_count =
                duplicate_count_.load(std::memory_order_relaxed),
            .unexplained_loss_count =
                unexplained_loss_count_.load(std::memory_order_relaxed),
        };
    }

    LocalBusOperationalStats OperationalStats(uint64_t now_ns) const noexcept {
        const BroadcastChannel::OperationalStats channel =
            channel_.operational_stats(now_ns);
        return LocalBusOperationalStats{
            .queue_depth = channel.depth,
            .queue_capacity = channel.capacity,
            .queue_full_events =
                queue_full_events_.load(std::memory_order_relaxed),
            .queue_dropped = queue_dropped_.load(std::memory_order_relaxed),
            .lease_expirations =
                lease_expirations_.load(std::memory_order_relaxed),
            .oldest_heartbeat_age_ns = channel.oldest_heartbeat_age_ns,
        };
    }

    uint64_t SweepExpired(uint64_t now_ns, uint64_t lease_ns) noexcept {
        const uint64_t expired =
            channel_.EvictStaleSubscribers(now_ns, lease_ns);
        lease_expirations_.fetch_add(expired, std::memory_order_relaxed);
        return expired;
    }

private:
    LocalTopicBinding(registry::TopicMetadata metadata, uint32_t region_id,
                      size_t max_payload_bytes, uint64_t payload_offset,
                      uint64_t payload_stride, uint64_t total_bytes,
                      void* memory, BroadcastChannel channel,
                      bool publisher_fenced)
        : metadata_(std::move(metadata)),
          region_id_(region_id),
          max_payload_bytes_(max_payload_bytes),
          payload_offset_(payload_offset),
          payload_stride_(payload_stride),
          total_bytes_(total_bytes),
          memory_(memory),
          channel_(std::move(channel)),
          slot_sources_(metadata_.capacity),
          publisher_fenced_(publisher_fenced) {}

    registry::TopicMetadata metadata_;
    uint32_t region_id_ = 0;
    size_t max_payload_bytes_ = 0;
    uint64_t payload_offset_ = 0;
    uint64_t payload_stride_ = 0;
    uint64_t total_bytes_ = 0;
    void* memory_ = nullptr;
    BroadcastChannel channel_;
    std::vector<SlotSourceMetadata> slot_sources_;
    mutable std::mutex lifecycle_mutex_;
    std::mutex publish_mutex_;
    std::atomic<uint64_t> queue_full_events_{0};
    std::atomic<uint64_t> queue_dropped_{0};
    std::atomic<uint64_t> lease_expirations_{0};
    std::atomic<uint64_t> outstanding_borrows_{0};
    std::atomic<uint64_t> last_published_sequence_{0};
    std::atomic<uint64_t> last_consumed_sequence_{0};
    std::atomic<uint64_t> published_samples_{0};
    std::atomic<uint64_t> observed_samples_{0};
    std::atomic<uint64_t> duplicate_count_{0};
    std::atomic<uint64_t> unexplained_loss_count_{0};
    bool publisher_active_ = false;
    bool publisher_fenced_ = false;
    PublisherId active_publisher_id_{};
};

LocalPublisherEndpoint::LocalPublisherEndpoint(
    std::shared_ptr<LocalTopicBinding> binding,
    registry::PublisherRegistration registration) noexcept
    : binding_(std::move(binding)), registration_(registration) {}

LocalPublisherEndpoint::~LocalPublisherEndpoint() {
    if (binding_ != nullptr) binding_->ClosePublisher(registration_);
}

Result<LocalPublication> LocalPublisherEndpoint::Publish(
    std::span<const std::byte> payload, uint8_t priority) {
    return binding_->Publish(registration_, payload, priority);
}

LocalSubscriberEndpoint::LocalSubscriberEndpoint(
    std::shared_ptr<LocalTopicBinding> binding,
    BroadcastChannel::SubscriberHandle handle) noexcept
    : binding_(std::move(binding)), handle_(handle) {}

LocalSubscriberEndpoint::~LocalSubscriberEndpoint() {
    if (binding_ != nullptr) binding_->CloseSubscriber(handle_);
}

Result<CanonicalMessage> LocalSubscriberEndpoint::TryPoll() {
    return binding_->Poll(handle_);
}

class LocalEndpointProvider final : public BusLocalEndpointProvider,
                                    public transport::LocalRouteProvider {
public:
    uint64_t version() const noexcept override {
        std::filesystem::path path;
        {
            std::lock_guard lock(mutex_);
            path = routing_catalog_path_;
        }
        if (path.empty()) return 1;
        auto snapshot = upgrade::LoadRegionRoutingSnapshot(path);
        return snapshot.ok() ? snapshot->generation : 0;
    }

    Status Install(const registry::TopicMetadata& metadata, uint32_t region_id,
                   size_t max_payload_bytes, bool publisher_fenced) {
        Result<std::shared_ptr<LocalTopicBinding>> binding =
            LocalTopicBinding::Create(metadata, region_id, max_payload_bytes,
                                      publisher_fenced);
        if (!binding.ok()) return binding.status();
        std::lock_guard lock(mutex_);
        try {
            if (!topics_.emplace(metadata.topic_id, std::move(*binding)).second) {
                return Status::Error(StatusCode::kAlreadyExists,
                                     "local topic binding already exists");
            }
            return Status::Ok();
        } catch (const std::bad_alloc&) {
            return Exhausted("cannot install local topic binding");
        }
    }

    Result<std::shared_ptr<const transport::LocalPublicationBinding>> Resolve(
        const registry::TopicMetadata& topic) const override {
        Result<std::shared_ptr<LocalTopicBinding>> binding = Find(topic);
        if (!binding.ok()) return binding.status();
        return std::shared_ptr<const transport::LocalPublicationBinding>(*binding);
    }

    Result<BusLocalPublisherResources> OpenPublisher(
        const registry::TopicMetadata& topic,
        const registry::PublisherRegistration& registration) override {
        Result<std::shared_ptr<LocalTopicBinding>> binding = Find(topic);
        if (!binding.ok()) return binding.status();
        Result<std::shared_ptr<BusLocalPublisherEndpoint>> endpoint =
            (*binding)->OpenPublisher(registration);
        if (!endpoint.ok()) return endpoint.status();
        return BusLocalPublisherResources{
            .binding = ResourceBinding(topic, *binding),
            .endpoint = std::move(*endpoint),
        };
    }

    Result<BusLocalSubscriberResources> OpenSubscriber(
        const registry::TopicMetadata& topic,
        const registry::SubscriberRegistration&) override {
        Result<std::shared_ptr<LocalTopicBinding>> binding = Find(topic);
        if (!binding.ok()) return binding.status();
        Result<std::shared_ptr<BusLocalSubscriberEndpoint>> endpoint =
            (*binding)->OpenSubscriber();
        if (!endpoint.ok()) return endpoint.status();
        return BusLocalSubscriberResources{
            .binding = ResourceBinding(topic, *binding),
            .endpoint = std::move(*endpoint),
        };
    }

    LocalBusOperationalStats OperationalStats(uint64_t now_ns) const noexcept {
        std::lock_guard lock(mutex_);
        LocalBusOperationalStats aggregate;
        for (const auto& [topic_id, binding] : topics_) {
            static_cast<void>(topic_id);
            const LocalBusOperationalStats current =
                binding->OperationalStats(now_ns);
            aggregate.queue_depth += current.queue_depth;
            aggregate.queue_capacity += current.queue_capacity;
            aggregate.queue_full_events += current.queue_full_events;
            aggregate.queue_dropped += current.queue_dropped;
            aggregate.lease_expirations += current.lease_expirations;
            aggregate.oldest_heartbeat_age_ns = std::max(
                aggregate.oldest_heartbeat_age_ns,
                current.oldest_heartbeat_age_ns);
        }
        return aggregate;
    }

    Status BindRoutingCatalog(std::filesystem::path path) {
        MINO_ASSIGN_OR_RETURN(const auto snapshot,
                              upgrade::LoadRegionRoutingSnapshot(path));
        static_cast<void>(snapshot);
        std::lock_guard lock(mutex_);
        routing_catalog_path_ = std::move(path);
        return Status::Ok();
    }

    Result<LocalBusUpgradeTopicStats> UpgradeStats(TopicId topic_id) const {
        std::shared_ptr<LocalTopicBinding> binding;
        {
            std::lock_guard lock(mutex_);
            const auto found = topics_.find(topic_id);
            if (found == topics_.end()) {
                return Status::Error(StatusCode::kNotFound,
                                     "local upgrade topic binding is not installed");
            }
            binding = found->second;
        }
        return binding->UpgradeStats(MonotonicNowNs());
    }

    Status FencePublisher(TopicId topic_id) {
        MINO_ASSIGN_OR_RETURN(auto binding, FindInstalled(topic_id));
        return binding->FencePublisher();
    }

    Status UnfencePublisher(TopicId topic_id) {
        MINO_ASSIGN_OR_RETURN(auto binding, FindInstalled(topic_id));
        return binding->UnfencePublisher();
    }

    uint64_t SweepExpired(uint64_t now_ns, uint64_t lease_ns) noexcept {
        std::lock_guard lock(mutex_);
        uint64_t expired = 0;
        for (const auto& [topic_id, binding] : topics_) {
            static_cast<void>(topic_id);
            expired += binding->SweepExpired(now_ns, lease_ns);
        }
        return expired;
    }

private:
    Result<std::shared_ptr<LocalTopicBinding>> FindInstalled(
        TopicId topic_id) const {
        std::lock_guard lock(mutex_);
        const auto found = topics_.find(topic_id);
        if (found == topics_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "local topic binding is not installed");
        }
        return found->second;
    }

    Result<std::shared_ptr<LocalTopicBinding>> Find(
        const registry::TopicMetadata& topic) const {
        std::shared_ptr<LocalTopicBinding> binding;
        std::filesystem::path catalog_path;
        {
            std::lock_guard lock(mutex_);
            const auto found = topics_.find(topic.topic_id);
            if (found == topics_.end()) {
                return Status::Error(StatusCode::kNotFound,
                                     "local topic binding is not installed");
            }
            binding = found->second;
            catalog_path = routing_catalog_path_;
        }
        const registry::TopicMetadata& installed = binding->metadata();
        if (installed.region_version != topic.region_version ||
            installed.channel_version != topic.channel_version ||
            installed.acl_version != topic.acl_version) {
            return Status::Error(StatusCode::kUnavailable,
                                 "local topic binding version is stale");
        }
        if (!catalog_path.empty()) {
            MINO_ASSIGN_OR_RETURN(
                const auto route,
                upgrade::LoadRegionRoutingSnapshot(catalog_path));
            if (binding->region_id() != route.active_region.region_id) {
                return Status::Error(StatusCode::kUnavailable,
                                     "local endpoint Region is fenced by the durable routing catalog");
            }
        }
        return binding;
    }

    static BusLocalResourceBinding ResourceBinding(
        const registry::TopicMetadata& topic,
        const std::shared_ptr<LocalTopicBinding>& binding) {
        return BusLocalResourceBinding{
            .topic_id = topic.topic_id,
            .region_version = topic.region_version,
            .channel_version = topic.channel_version,
            .acl_version = topic.acl_version,
            .publication =
                std::shared_ptr<const transport::LocalPublicationBinding>(binding),
        };
    }

    mutable std::mutex mutex_;
    std::unordered_map<TopicId, std::shared_ptr<LocalTopicBinding>> topics_;
    std::filesystem::path routing_catalog_path_;
};

}  // namespace

class LocalBusDeployment::Impl final {
public:
    uint64_t lease_duration_ns = 0;
    std::shared_ptr<DurableFileIdAllocator> ids;
    std::shared_ptr<registry::Coordinator> coordinator;
    std::shared_ptr<LocalEndpointProvider> endpoints;
    std::shared_ptr<transport::TransportSwitcher> switcher;
    std::shared_ptr<MonotonicParticipantIds> participant_ids;
    std::shared_ptr<LocalOnlyDispatcher> dispatcher;
    std::unique_ptr<Bus> bus;
};

Result<std::unique_ptr<LocalBusDeployment>> LocalBusDeployment::Create(
    LocalBusConfig config) noexcept {
    try {
        MINO_RETURN_IF_ERROR(ValidateConfig(config));
        Result<std::shared_ptr<DurableFileIdAllocator>> ids =
            DurableFileIdAllocator::Open(config.topic_id_state_path);
        if (!ids.ok()) return ids.status();
        Result<std::unique_ptr<registry::Coordinator>> coordinator_created =
            registry::Coordinator::Create({}, *ids);
        if (!coordinator_created.ok()) return coordinator_created.status();
        auto coordinator = std::shared_ptr<registry::Coordinator>(
            std::move(*coordinator_created));

        ProcessIdentity identity = ProcessIdentity::Current();
        identity.node_id = config.node_id.value;
        Result<transport::EndpointDescriptor> endpoint =
            transport::EndpointDescriptor::SharedFabric(config.region_id, 1);
        if (!endpoint.ok()) return endpoint.status();
        const registry::NodeRegistration node{
            .node_id = config.node_id,
            .process_identity = identity,
            .endpoints = {*endpoint},
            .security_domain_id = config.security_domain_id,
            .trust_domain = "local",
            .health = registry::NodeHealth::kHealthy,
            .lease_epoch = config.lease_epoch,
            .lease_duration_ns = config.lease_duration_ns,
            .config_version = 1,
        };
        Result<registry::NodeRegistrationOutcome> registered =
            coordinator->RegisterNode(node, MonotonicNowNs());
        if (!registered.ok()) return registered.status();
        const registry::NodeLeaseOwner owner{
            .node_id = config.node_id,
            .process_identity = identity,
            .lease_epoch = config.lease_epoch,
        };

        auto endpoints = std::make_shared<LocalEndpointProvider>();
        auto access = std::make_shared<LocalAccessValidator>(config.node_id);
        auto schemas = std::make_shared<ExactSchemaValidator>();
        Result<std::unique_ptr<transport::TransportSwitcher>> switcher_created =
            transport::TransportSwitcher::Create(config.node_id, coordinator.get(),
                                                  access, schemas, endpoints);
        if (!switcher_created.ok()) return switcher_created.status();
        auto switcher = std::shared_ptr<transport::TransportSwitcher>(
            std::move(*switcher_created));

        for (const LocalTopicConfig& configured : config.topics) {
            registry::TopicMetadata candidate{
                .topic_id = {},
                .name = configured.name,
                .channel_kind = registry::ChannelKind::kBroadcast,
                .delivery = registry::DeliveryPolicy{
                    .reliability = registry::Reliability::kBestEffort,
                    .allow_drop = false,
                },
                .queue_full_policy = QueueFullPolicy::kFail,
                .schema = configured.schema,
                .accepted_schemas = {},
                .route_policy = registry::RoutePolicy::kStatic,
                .static_routes = {registry::StaticRouteEntry{
                    .target_node = config.node_id,
                    .preferred_transport = std::nullopt,
                }},
                .route_set_version = 0,
                .capacity = configured.channel_capacity,
                .max_publishers = 1,
                .max_subscribers = configured.max_subscribers,
                .partition_count = 1,
                .record_topology =
                    registry::RecordBackpressureTopology::kIsolated,
                .acl = registry::TopicAcl{
                    .entries = {{.node_id = config.node_id,
                                 .security_domain_id = config.security_domain_id,
                                 .permissions = registry::kAllTopicPermissions}},
                },
                .region_version = 1,
                .channel_version = 1,
                .acl_version = 1,
                .config_version = 0,
                .state = registry::TopicState::kCreating,
            };
            Result<std::shared_ptr<const registry::TopicSnapshot>> topic =
                coordinator->CreateTopic(std::move(candidate));
            if (!topic.ok()) return topic.status();
            const uint32_t topic_region_id =
                configured.region_id == 0 ? config.region_id
                                          : configured.region_id;
            MINO_RETURN_IF_ERROR(endpoints->Install(
                (*topic)->metadata, topic_region_id,
                configured.max_payload_bytes, !configured.activate));
            if (!configured.activate) continue;
            const registry::ActivationReadinessProof proof{
                .topic_id = (*topic)->metadata.topic_id,
                .config_version = (*topic)->metadata.config_version,
                .schema = (*topic)->metadata.schema,
                .region_version = (*topic)->metadata.region_version,
                .channel_version = (*topic)->metadata.channel_version,
                .acl_version = (*topic)->metadata.acl_version,
                .schema_ready = true,
                .region_ready = true,
                .channel_ready = true,
                .acl_ready = true,
            };
            MINO_RETURN_IF_ERROR(
                coordinator->ActivateTopic((*topic)->metadata.topic_id, proof));
        }

        auto participant_ids = std::make_shared<MonotonicParticipantIds>();
        auto dispatcher = std::make_shared<LocalOnlyDispatcher>();
        Result<std::unique_ptr<Bus>> bus =
            Bus::Create(owner, coordinator, switcher, participant_ids, endpoints,
                        dispatcher);
        if (!bus.ok()) return bus.status();

        auto impl = std::make_unique<Impl>();
        impl->lease_duration_ns = config.lease_duration_ns;
        impl->ids = std::move(*ids);
        impl->coordinator = std::move(coordinator);
        impl->endpoints = std::move(endpoints);
        impl->switcher = std::move(switcher);
        impl->participant_ids = std::move(participant_ids);
        impl->dispatcher = std::move(dispatcher);
        impl->bus = std::move(*bus);
        return std::unique_ptr<LocalBusDeployment>(
            new LocalBusDeployment(std::move(impl)));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate local Bus deployment");
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal, error.what());
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "local Bus deployment failed unexpectedly");
    }
}

LocalBusDeployment::LocalBusDeployment(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

LocalBusDeployment::~LocalBusDeployment() = default;

Bus& LocalBusDeployment::bus() noexcept { return *impl_->bus; }

const Bus& LocalBusDeployment::bus() const noexcept { return *impl_->bus; }

LocalBusOperationalStats LocalBusDeployment::OperationalStats(
    uint64_t now_ns) const noexcept {
    return impl_->endpoints->OperationalStats(now_ns);
}

uint64_t LocalBusDeployment::SweepExpiredSubscribers(uint64_t now_ns) noexcept {
    return impl_->endpoints->SweepExpired(now_ns, impl_->lease_duration_ns);
}

Status LocalBusDeployment::BindRoutingCatalog(
    std::filesystem::path catalog_path) {
    return impl_->endpoints->BindRoutingCatalog(std::move(catalog_path));
}

Result<LocalBusUpgradeTopicStats> LocalBusDeployment::UpgradeTopicStats(
    TopicId topic_id) const {
    return impl_->endpoints->UpgradeStats(topic_id);
}

Status LocalBusDeployment::FenceUpgradePublisher(TopicId topic_id) {
    return impl_->endpoints->FencePublisher(topic_id);
}

Status LocalBusDeployment::UnfenceUpgradePublisher(TopicId topic_id) {
    return impl_->endpoints->UnfencePublisher(topic_id);
}

Status LocalBusDeployment::RefreshUpgradeRoute(TopicId topic_id) {
    MINO_RETURN_IF_ERROR(impl_->switcher->InvalidateTopic(topic_id));
    return impl_->switcher->RefreshTopic(topic_id);
}

registry::Coordinator& LocalBusDeployment::coordinator() noexcept {
    return *impl_->coordinator;
}

}  // namespace mino::deployment
