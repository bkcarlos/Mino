// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_DEPLOYMENT_SHARED_HOST_DOMAIN_H_
#define MINO_RUNTIME_DEPLOYMENT_SHARED_HOST_DOMAIN_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/runtime/deadline.h"
#include "mino/runtime/message_traits.h"
#include "mino/schema/descriptor.h"
#include "mino/shm/channel/queue_full_policy.h"

namespace mino::deployment {

// Same-host multi-process topology domain backed by one POSIX SHM segment.
// Independent processes Create or Open the domain by name, Join as peers, and
// Advertise/Subscribe topics without a pre-baked peer or channel manifest.
//
// LocalBusDeployment remains the in-process Bus assembly (heap BroadcastChannel
// + Coordinator). SharedHostDomain is the production same-host dynamic path that
// replaces benchmark-static SHM manifests for discoverable local topologies.
// Schema, topic mode, and queue shape are immutable after first advertisement;
// mismatches fail closed. Dead peers and endpoint leases are reclaimable via
// Recover(). MPSC topics also abort orphaned reservations on Recover().
//
// Layout ABI version 2 (magic MINOSHD2) is incompatible with v1 domains.
//
// Capabilities in this revision:
// - Topic modes: kBroadcast (1 publisher, N subscribers) and kMpsc (N
//   publishers, 1 subscriber). MPSC requires queue_depth >= 64.
// - Multi-publisher leases per topic with proven-dead Recover() reclamation.
// - Zero-copy borrow poll path over the fixed per-topic payload ring.
// - Optional typed Advertise/Subscribe/Publish via StaticMessageTraits<T>.
//
// Remaining limits (intentional / deferred):
// - Canonical payloads stay in fixed per-topic rings. CentralSlab +
//   AllocationJournal + ShmPinTable are **deferred**: wiring them needs an ABI
//   bump (MINOSHD2→v3), journal/pin recovery, and essentially duplicates
//   SimpleNode's segment layout. SharedHostDomain's role is discoverable
//   POD/bytes topology; SimpleNode remains the CentralSlab reference path.
// - LocalBus/Coordinator stay in-process; no hybrid cross-host ZC / PTP / RDMA.
// - Optional static LocalBusConfig::topics manifests remain valid for the
//   in-process LocalBusDeployment path.

inline constexpr uint32_t kSharedHostMaxPeerSlots = 64;
inline constexpr uint32_t kSharedHostMaxTopicSlots = 64;
inline constexpr uint32_t kSharedHostMaxQueueDepth = 1024;
inline constexpr uint32_t kSharedHostMaxPayloadBytes = 1u << 20;
inline constexpr uint32_t kSharedHostMaxSubscribers = 64;
inline constexpr uint32_t kSharedHostMaxPublishersPerTopic = 16;

enum class SharedHostTopicMode : uint32_t {
    kBroadcast = 1,
    kMpsc = 2,
};

struct SharedHostTopicOptions {
    SharedHostTopicMode mode = SharedHostTopicMode::kBroadcast;
    QueueFullPolicy queue_full_policy = QueueFullPolicy::kBlock;
    uint32_t sample_rate = 1;
};

struct SharedHostDomainOptions {
    uint32_t peer_slots = 8;
    uint32_t topic_slots = 8;
    uint32_t queue_depth = 64;  // power of two; MPSC topics require >= 64
    uint32_t max_subscribers = 16;
    uint32_t max_publishers_per_topic = 8;
    uint32_t max_payload_bytes = 4096;
    uint64_t peer_lease_ns = 30ull * 1000 * 1000 * 1000;
};

struct SharedPeerInfo {
    NodeId node_id{};
    ProcessIdentity identity{};
    uint64_t last_heartbeat_ns = 0;
};

struct SharedTopicInfo {
    std::string name;
    schema::SchemaIdentity schema{0, {}, 0, 0};
    SharedHostTopicMode mode = SharedHostTopicMode::kBroadcast;
    uint32_t capacity = 0;
    uint32_t max_subscribers = 0;
    uint32_t max_publishers = 0;
    uint32_t max_payload_bytes = 0;
    uint32_t active_publishers = 0;
    bool publisher_active = false;  // active_publishers > 0
    bool subscriber_active = false;
};

// Zero-copy view into a published payload. Lifetime is bound to the channel
// borrow; Release()/destructor Ack the slot. Only one borrow may be active per
// subscriber at a time.
class SharedHostBorrowedBytes {
public:
    SharedHostBorrowedBytes() noexcept;
    SharedHostBorrowedBytes(SharedHostBorrowedBytes&& other) noexcept;
    SharedHostBorrowedBytes& operator=(SharedHostBorrowedBytes&& other) noexcept;
    ~SharedHostBorrowedBytes();

    SharedHostBorrowedBytes(const SharedHostBorrowedBytes&) = delete;
    SharedHostBorrowedBytes& operator=(const SharedHostBorrowedBytes&) = delete;

    std::span<const std::byte> bytes() const noexcept;
    bool active() const noexcept;
    Status Release() && noexcept;

    template <typename T>
    Result<const T*> As() const noexcept {
        static_assert(kHasStaticMessageTraits<T>,
                      "StaticMessageTraits<T> must be specialized");
        static_assert(std::is_standard_layout_v<T> &&
                          std::is_trivially_copyable_v<T> &&
                          std::is_trivially_destructible_v<T>,
                      "SharedHostDomain typed messages must be SHM-safe POD");
        const std::span<const std::byte> payload = bytes();
        if (payload.size() != sizeof(T) || payload.data() == nullptr ||
            reinterpret_cast<uintptr_t>(payload.data()) % alignof(T) != 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "typed payload size or alignment is invalid");
        }
        return reinterpret_cast<const T*>(payload.data());
    }

private:
    friend class SharedHostSubscriber;
    struct Impl;
    explicit SharedHostBorrowedBytes(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

template <typename T>
class SharedHostBorrowedValue {
public:
    SharedHostBorrowedValue(SharedHostBorrowedValue&&) noexcept = default;
    SharedHostBorrowedValue& operator=(SharedHostBorrowedValue&&) noexcept =
        default;
    SharedHostBorrowedValue(const SharedHostBorrowedValue&) = delete;
    SharedHostBorrowedValue& operator=(const SharedHostBorrowedValue&) = delete;

    const T* get() const noexcept { return value_; }
    const T* operator->() const noexcept { return value_; }
    const T& operator*() const noexcept { return *value_; }
    bool active() const noexcept { return bytes_.active(); }
    Status Release() && noexcept { return std::move(bytes_).Release(); }

private:
    friend class SharedHostSubscriber;
    SharedHostBorrowedValue(SharedHostBorrowedBytes&& bytes,
                            const T* value) noexcept
        : bytes_(std::move(bytes)), value_(value) {}

    SharedHostBorrowedBytes bytes_;
    const T* value_ = nullptr;
};

class SharedHostPublisher {
public:
    SharedHostPublisher(SharedHostPublisher&& other) noexcept;
    SharedHostPublisher& operator=(SharedHostPublisher&& other) noexcept;
    ~SharedHostPublisher();

    SharedHostPublisher(const SharedHostPublisher&) = delete;
    SharedHostPublisher& operator=(const SharedHostPublisher&) = delete;

    Status Publish(std::span<const std::byte> payload);
    Status Publish(std::span<const std::byte> payload, Deadline deadline);

    template <typename T>
    Status Publish(
        const T& value,
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5)))
        requires kHasStaticMessageTraits<T> {
        static_assert(std::is_standard_layout_v<T> &&
                          std::is_trivially_copyable_v<T> &&
                          std::is_trivially_destructible_v<T>,
                      "SharedHostDomain typed messages must be SHM-safe POD");
        const Status validation = StaticMessageTraits<T>::Validate(value);
        if (!validation.ok()) return validation;
        return PublishTyped(std::as_bytes(std::span<const T>(&value, 1)),
                            deadline);
    }

    SharedHostTopicMode mode() const noexcept;
    bool active() const noexcept;

private:
    friend class SharedHostDomain;
    struct Impl;
    explicit SharedHostPublisher(std::unique_ptr<Impl> impl) noexcept;
    Status PublishTyped(std::span<const std::byte> payload, Deadline deadline);
    std::unique_ptr<Impl> impl_;
};

class SharedHostSubscriber {
public:
    SharedHostSubscriber(SharedHostSubscriber&& other) noexcept;
    SharedHostSubscriber& operator=(SharedHostSubscriber&& other) noexcept;
    ~SharedHostSubscriber();

    SharedHostSubscriber(const SharedHostSubscriber&) = delete;
    SharedHostSubscriber& operator=(const SharedHostSubscriber&) = delete;

    // Copying poll path (allocates a vector). Prefer TryPollBorrow for
    // zero-copy when the caller can keep the borrow alive.
    Result<std::vector<std::byte>> TryPoll();
    Result<std::vector<std::byte>> Poll(
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5)));

    Result<SharedHostBorrowedBytes> TryPollBorrow();
    Result<SharedHostBorrowedBytes> PollBorrow(
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5)));

    template <typename T>
    Result<SharedHostBorrowedValue<T>> TryPoll() {
        Result<SharedHostBorrowedBytes> message = TryPollBorrow();
        if (!message.ok()) return message.status();
        Result<const T*> value = message->template As<T>();
        if (!value.ok()) {
            const Status error = value.status();
            (void)std::move(*message).Release();
            return error;
        }
        return SharedHostBorrowedValue<T>(std::move(*message), *value);
    }

    template <typename T>
    Result<SharedHostBorrowedValue<T>> Poll(
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5))) {
        for (;;) {
            Result<SharedHostBorrowedValue<T>> message = TryPoll<T>();
            if (message.ok()) return message;
            if (message.status().code() != StatusCode::kWouldBlock) {
                return message.status();
            }
            if (deadline.expired()) {
                return Status::Error(StatusCode::kTimeout,
                                     "timed out polling SharedHostSubscriber");
            }
        }
    }

    SharedHostTopicMode mode() const noexcept;
    bool active() const noexcept;

private:
    friend class SharedHostDomain;
    struct Impl;
    explicit SharedHostSubscriber(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

class SharedHostDomain {
public:
    static Result<uint64_t> RequiredBytes(
        const SharedHostDomainOptions& options = {});

    static Result<SharedHostDomain> Create(
        std::string_view name, SharedHostDomainOptions options = {});
    static Result<SharedHostDomain> Open(std::string_view name);
    static Status Unlink(std::string_view name);

    SharedHostDomain(SharedHostDomain&& other) noexcept;
    SharedHostDomain& operator=(SharedHostDomain&& other) noexcept;
    ~SharedHostDomain();

    SharedHostDomain(const SharedHostDomain&) = delete;
    SharedHostDomain& operator=(const SharedHostDomain&) = delete;

    // Registers this process in the peer table. Required before Advertise /
    // Subscribe. Idempotent for the current ProcessIdentity.
    Status Join(NodeId node_id);
    Status Heartbeat();
    Status Leave();

    Result<std::vector<SharedPeerInfo>> ListPeers();
    Result<std::vector<SharedTopicInfo>> ListTopics();

    Result<SharedHostPublisher> Advertise(
        std::string_view topic, const schema::SchemaIdentity& schema,
        SharedHostTopicOptions options = {});
    Result<SharedHostSubscriber> Subscribe(
        std::string_view topic, const schema::SchemaIdentity& schema,
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5)));

    template <typename T>
    Result<SharedHostPublisher> Advertise(
        std::string_view topic, SharedHostTopicOptions options = {}) {
        static_assert(kHasStaticMessageTraits<T>,
                      "StaticMessageTraits<T> must be specialized");
        static_assert(std::is_standard_layout_v<T> &&
                          std::is_trivially_copyable_v<T> &&
                          std::is_trivially_destructible_v<T>,
                      "SharedHostDomain typed messages must be SHM-safe POD");
        schema::CanonicalDigest digest{};
        const uint64_t short_id = StaticMessageTraits<T>::schema_short_id;
        digest[0] = static_cast<std::byte>(short_id & 0xff);
        digest[1] = static_cast<std::byte>((short_id >> 8) & 0xff);
        digest[2] = static_cast<std::byte>((short_id >> 16) & 0xff);
        digest[3] = static_cast<std::byte>((short_id >> 24) & 0xff);
        digest[31] = static_cast<std::byte>(
            StaticMessageTraits<T>::layout_version & 0xff);
        const schema::SchemaIdentity schema(
            short_id, digest, StaticMessageTraits<T>::schema_version,
            StaticMessageTraits<T>::layout_version);
        return Advertise(topic, schema, options);
    }

    template <typename T>
    Result<SharedHostSubscriber> Subscribe(
        std::string_view topic,
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5))) {
        static_assert(kHasStaticMessageTraits<T>,
                      "StaticMessageTraits<T> must be specialized");
        schema::CanonicalDigest digest{};
        const uint64_t short_id = StaticMessageTraits<T>::schema_short_id;
        digest[0] = static_cast<std::byte>(short_id & 0xff);
        digest[1] = static_cast<std::byte>((short_id >> 8) & 0xff);
        digest[2] = static_cast<std::byte>((short_id >> 16) & 0xff);
        digest[3] = static_cast<std::byte>((short_id >> 24) & 0xff);
        digest[31] = static_cast<std::byte>(
            StaticMessageTraits<T>::layout_version & 0xff);
        const schema::SchemaIdentity schema(
            short_id, digest, StaticMessageTraits<T>::schema_version,
            StaticMessageTraits<T>::layout_version);
        return Subscribe(topic, schema, deadline);
    }

    // Reclaims peer slots and topic endpoint leases whose ProcessIdentity is
    // proven dead. Also aborts orphaned MPSC reservations. Survivors are never
    // wedged by unknown/live owners.
    Status Recover();

    const std::string& name() const noexcept;
    uint64_t size_bytes() const noexcept;

private:
    struct Impl;
    explicit SharedHostDomain(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::deployment

#endif  // MINO_RUNTIME_DEPLOYMENT_SHARED_HOST_DOMAIN_H_
