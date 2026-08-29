// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_SIMPLE_NODE_H_
#define MINO_RUNTIME_SIMPLE_NODE_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/runtime/deadline.h"
#include "mino/runtime/message_traits.h"
#include "mino/shm/channel/queue_full_policy.h"

namespace mino {

// Compact multi-process SHM pub/sub. Discovery, allocator recovery metadata,
// endpoint ownership, channels, subscriber leases, and payload Pins all live in
// one POSIX shm object; no coordinator process is required.
//
// The default byte API remains one-publisher/one-subscriber SPSC. A topic may
// instead be advertised as kMpsc (many publishers, one subscriber) or
// kBroadcast (one publisher, up to 64 subscribers). MPSC requires queue_depth
// >= 64. Topic mode, QoS, and schema are immutable after first advertisement.
//
// Endpoints and borrowed messages retain the mapping. Normal destruction and
// proven-dead process recovery release endpoint claims. MPSC reservations,
// allocation transactions, Broadcast subscriber leases/borrows, and Pins are
// recovered automatically by public operations and explicitly by Recover().

enum class SimpleTopicMode : uint32_t {
    kSpsc = 1,
    kMpsc = 2,
    kBroadcast = 3,
};

struct SimpleTopicOptions {
    SimpleTopicMode mode = SimpleTopicMode::kSpsc;
    QueueFullPolicy queue_full_policy = QueueFullPolicy::kBlock;
    uint32_t sample_rate = 1;
};

struct SimpleNodeOptions {
    uint32_t topic_slots = 8;
    uint32_t queue_depth = 32;        // power of two, >= 2; MPSC requires >= 64
    uint32_t max_payload_bytes = 256;
    uint32_t max_publishers_per_topic = 8;
    uint64_t subscriber_lease_ns = 30ull * 1000 * 1000 * 1000;
    uint64_t segment_bytes = 0;       // 0 = exact RequiredBytes()
};

// Stable schema identity stored in the topic directory and every message.
// fixed_size == 0 denotes the variable-length raw byte API.
struct SimpleTypeDescriptor {
    uint32_t type_id = 0;
    uint32_t message_type = 0;
    uint32_t schema_version = 0;
    uint32_t index_flags = 0;
    uint64_t schema_short_id = 0;
    uint32_t layout_version = 0;
    uint32_t alignment = 1;
    uint32_t fixed_size = 0;
    uint32_t reserved0 = 0;

    static SimpleTypeDescriptor Bytes() noexcept;

    template <typename T>
    static constexpr SimpleTypeDescriptor For() noexcept {
        static_assert(kHasStaticMessageTraits<T>,
                      "StaticMessageTraits<T> must be specialized");
        static_assert(std::is_standard_layout_v<T> &&
                          std::is_trivially_copyable_v<T> &&
                          std::is_trivially_destructible_v<T>,
                      "SimpleNode typed messages must be SHM-safe POD types");
        if constexpr (requires {
                          StaticMessageTraits<T>::kOwnedGraphCollectionSupported;
                      }) {
            static_assert(
                !StaticMessageTraits<T>::kOwnedGraphCollectionSupported,
                "SimpleNode typed copy API does not support owned child slabs; "
                "use Publisher<T> for generated owned graphs");
        }
        return SimpleTypeDescriptor{
            .type_id = StaticMessageTraits<T>::type_id.value,
            .message_type = StaticMessageTraits<T>::message_type,
            .schema_version = StaticMessageTraits<T>::schema_version,
            .index_flags = StaticMessageTraits<T>::index_flags,
            .schema_short_id = StaticMessageTraits<T>::schema_short_id,
            .layout_version = StaticMessageTraits<T>::layout_version,
            .alignment = alignof(T),
            .fixed_size = sizeof(T),
        };
    }
};

class BorrowedBytes {
public:
    BorrowedBytes() noexcept;
    BorrowedBytes(BorrowedBytes&& other) noexcept;
    BorrowedBytes& operator=(BorrowedBytes&& other) noexcept;
    ~BorrowedBytes();

    BorrowedBytes(const BorrowedBytes&) = delete;
    BorrowedBytes& operator=(const BorrowedBytes&) = delete;

    std::span<const std::byte> bytes() const noexcept;
    const SimpleTypeDescriptor& type() const noexcept;
    bool active() const noexcept;
    Status Release() && noexcept;

    template <typename T>
    Result<const T*> As() const noexcept {
        const SimpleTypeDescriptor expected = SimpleTypeDescriptor::For<T>();
        if (!Matches(expected)) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "borrowed payload type does not match T");
        }
        const std::span<const std::byte> payload = bytes();
        if (payload.size() != sizeof(T) || payload.data() == nullptr ||
            reinterpret_cast<uintptr_t>(payload.data()) % alignof(T) != 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "typed payload size or alignment is invalid");
        }
        return reinterpret_cast<const T*>(payload.data());
    }

private:
    friend class SimpleSubscriber;
    struct Impl;
    explicit BorrowedBytes(std::unique_ptr<Impl> impl) noexcept;
    bool Matches(const SimpleTypeDescriptor& expected) const noexcept;
    std::unique_ptr<Impl> impl_;
};

template <typename T>
class BorrowedValue {
public:
    BorrowedValue(BorrowedValue&&) noexcept = default;
    BorrowedValue& operator=(BorrowedValue&&) noexcept = default;
    BorrowedValue(const BorrowedValue&) = delete;
    BorrowedValue& operator=(const BorrowedValue&) = delete;

    const T* get() const noexcept { return value_; }
    const T* operator->() const noexcept { return value_; }
    const T& operator*() const noexcept { return *value_; }
    bool active() const noexcept { return bytes_.active(); }
    Status Release() && noexcept { return std::move(bytes_).Release(); }

private:
    friend class SimpleSubscriber;
    BorrowedValue(BorrowedBytes&& bytes, const T* value) noexcept
        : bytes_(std::move(bytes)), value_(value) {}

    BorrowedBytes bytes_;
    const T* value_ = nullptr;
};

class SimplePublisher {
public:
    SimplePublisher(SimplePublisher&& other) noexcept;
    SimplePublisher& operator=(SimplePublisher&& other) noexcept;
    ~SimplePublisher();

    SimplePublisher(const SimplePublisher&) = delete;
    SimplePublisher& operator=(const SimplePublisher&) = delete;

    Status Publish(
        std::span<const std::byte> payload,
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5)));

    template <typename T>
    Status Publish(
        const T& value,
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5))) {
        const Status validation = StaticMessageTraits<T>::Validate(value);
        if (!validation.ok()) return validation;
        return PublishTyped(
            std::as_bytes(std::span<const T>(&value, 1)),
            SimpleTypeDescriptor::For<T>(), deadline);
    }

    SimpleTopicMode mode() const noexcept;
    const SimpleTypeDescriptor& type() const noexcept;

private:
    friend class SimpleNode;
    struct Impl;
    explicit SimplePublisher(std::unique_ptr<Impl> impl) noexcept;
    Status PublishTyped(std::span<const std::byte> payload,
                        const SimpleTypeDescriptor& type, Deadline deadline);
    std::unique_ptr<Impl> impl_;
};

class SimpleSubscriber {
public:
    SimpleSubscriber(SimpleSubscriber&& other) noexcept;
    SimpleSubscriber& operator=(SimpleSubscriber&& other) noexcept;
    ~SimpleSubscriber();

    SimpleSubscriber(const SimpleSubscriber&) = delete;
    SimpleSubscriber& operator=(const SimpleSubscriber&) = delete;

    Result<BorrowedBytes> TryPoll();
    Result<BorrowedBytes> Poll(
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5)));

    template <typename T>
    Result<BorrowedValue<T>> TryPoll() {
        Result<BorrowedBytes> message = TryPoll();
        if (!message.ok()) return message.status();
        Result<const T*> value = message->template As<T>();
        if (!value.ok()) {
            const Status error = value.status();
            (void)std::move(*message).Release();
            return error;
        }
        return BorrowedValue<T>(std::move(*message), *value);
    }

    template <typename T>
    Result<BorrowedValue<T>> Poll(
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5))) {
        for (;;) {
            Result<BorrowedValue<T>> message = TryPoll<T>();
            if (message.ok()) return message;
            if (message.status().code() != StatusCode::kWouldBlock) {
                return message.status();
            }
            if (deadline.expired()) {
                return Status::Error(StatusCode::kTimeout,
                                     "subscriber poll deadline expired");
            }
        }
    }

    SimpleTopicMode mode() const noexcept;
    const SimpleTypeDescriptor& type() const noexcept;

private:
    friend class SimpleNode;
    struct Impl;
    explicit SimpleSubscriber(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

class SimpleNode {
public:
    static Result<uint64_t> RequiredBytes(
        const SimpleNodeOptions& options = {});

    static Result<SimpleNode> Create(std::string_view name,
                                     SimpleNodeOptions options = {});
    static Result<SimpleNode> Open(std::string_view name);
    static Status Unlink(std::string_view name);

    SimpleNode(SimpleNode&& other) noexcept;
    SimpleNode& operator=(SimpleNode&& other) noexcept;
    ~SimpleNode();

    SimpleNode(const SimpleNode&) = delete;
    SimpleNode& operator=(const SimpleNode&) = delete;

    Result<SimplePublisher> Advertise(
        std::string_view topic, SimpleTopicOptions options = {});
    Result<SimpleSubscriber> Subscribe(
        std::string_view topic,
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5)));

    template <typename T>
    Result<SimplePublisher> Advertise(
        std::string_view topic, SimpleTopicOptions options = {}) {
        return AdvertiseTyped(topic, options, SimpleTypeDescriptor::For<T>());
    }

    template <typename T>
    Result<SimpleSubscriber> Subscribe(
        std::string_view topic,
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5))) {
        return SubscribeTyped(topic, SimpleTypeDescriptor::For<T>(), deadline);
    }

    // Runs endpoint, reservation, subscriber lease/borrow, Pin, and allocation
    // journal recovery. Only owners proven dead are reclaimed.
    Status Recover();

    uint64_t size_bytes() const noexcept;
    const std::string& name() const noexcept;

private:
    struct Impl;
    explicit SimpleNode(std::unique_ptr<Impl> impl) noexcept;
    Result<SimplePublisher> AdvertiseTyped(
        std::string_view topic, SimpleTopicOptions options,
        const SimpleTypeDescriptor& type);
    Result<SimpleSubscriber> SubscribeTyped(
        std::string_view topic, const SimpleTypeDescriptor& type,
        Deadline deadline);
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino

#endif  // MINO_RUNTIME_SIMPLE_NODE_H_
