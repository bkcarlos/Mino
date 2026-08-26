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

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/runtime/deadline.h"

namespace mino {

// Compact multi-process SHM pub/sub. Discovery is a fixed topic directory
// inside the same POSIX shm object that holds the slab and SPSC channels.
// There is no coordinator, Region supervisor, or extra SHM manager process.
//
//   auto node = SimpleNode::Create("/mino_demo");
//   auto pub = node->Advertise("camera");
//   pub->Publish(std::as_bytes(std::span{payload}));
//
//   auto node = SimpleNode::Open("/mino_demo");
//   auto sub = node->Subscribe("camera");
//   auto msg = sub->TryPoll();  // borrow into SHM, no payload copy
//
// Default Create() size is derived from (topic_slots, queue_depth,
// max_payload_bytes). A 256 B / depth-32 demo fits in a few hundred KiB.

struct SimpleNodeOptions {
    uint32_t topic_slots = 8;
    uint32_t queue_depth = 32;        // power of two, >= 2
    uint32_t max_payload_bytes = 256;
    uint64_t segment_bytes = 0;       // 0 = exact RequiredBytes()
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
    bool active() const noexcept;
    Status Release() && noexcept;

private:
    friend class SimpleSubscriber;
    struct Impl;
    explicit BorrowedBytes(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
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

private:
    friend class SimpleNode;
    struct Impl;
    explicit SimplePublisher(std::unique_ptr<Impl> impl) noexcept;
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

    Result<SimplePublisher> Advertise(std::string_view topic);
    Result<SimpleSubscriber> Subscribe(
        std::string_view topic,
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5)));

    uint64_t size_bytes() const noexcept;
    const std::string& name() const noexcept;

private:
    struct Impl;
    explicit SimpleNode(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino

#endif  // MINO_RUNTIME_SIMPLE_NODE_H_
