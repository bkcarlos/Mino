// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/simple_node.h"

#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace mino {
namespace {

constexpr int kChildOk = 0;
constexpr int kChildOpen = 1;
constexpr int kChildSubscribe = 2;
constexpr int kChildPoll = 3;
constexpr int kChildPayload = 4;
constexpr int kChildCount = 5;

std::string UniqueName(const char* tag) {
    static std::atomic<uint32_t> sequence{0};
    return std::string("/mnp") + std::to_string(::getpid()) + "_" +
           std::to_string(sequence.fetch_add(1) + 1) + "_" + tag;
}

class SimpleNodeTest : public ::testing::Test {
protected:
    void TearDown() override {
        for (const auto& name : created_) {
            (void)SimpleNode::Unlink(name);
        }
    }

    std::string MakeName(const char* tag) {
        std::string name = UniqueName(tag);
        created_.push_back(name);
        return name;
    }

    std::vector<std::string> created_;
};

TEST_F(SimpleNodeTest, DefaultLayoutFitsWellUnderAFewMiB) {
    SimpleNodeOptions options;
    options.topic_slots = 8;
    options.queue_depth = 32;
    options.max_payload_bytes = 256;
    auto bytes = SimpleNode::RequiredBytes(options);
    ASSERT_TRUE(bytes.ok()) << bytes.status().ToString();
    EXPECT_LT(*bytes, 2ull << 20) << "256B x depth 32 should be << 2 MiB";
    EXPECT_GT(*bytes, 64u * 1024u);
}

TEST_F(SimpleNodeTest, CreateFailsClosedWhenSegmentExceedsShmBudget) {
    const std::string name = MakeName("huge");
    SimpleNodeOptions options;
    options.segment_bytes = 16ull << 30;  // 16 GiB: above a 64 MiB tmpfs
    auto node = SimpleNode::Create(name, options);
    ASSERT_FALSE(node.ok());
    EXPECT_EQ(node.status().code(), StatusCode::kResourceExhausted)
        << node.status().ToString();
    EXPECT_NE(node.status().message().find("/dev/shm"), std::string_view::npos)
        << node.status().ToString();
}

TEST_F(SimpleNodeTest, SameNodePublishThenPollBorrowsShmBytes) {
    const std::string name = MakeName("local");
    auto created = SimpleNode::Create(name);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SimpleNode node = std::move(*created);
    EXPECT_LT(node.size_bytes(), 2ull << 20);

    auto pub = node.Advertise("camera");
    ASSERT_TRUE(pub.ok()) << pub.status().ToString();
    auto sub = node.Subscribe("camera");
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();

    const char payload[] = "hello-shm";
    const Status published = pub->Publish(std::as_bytes(std::span{
        reinterpret_cast<const std::byte*>(payload), sizeof(payload) - 1}));
    ASSERT_TRUE(published.ok()) << published.ToString();

    auto message = sub->TryPoll();
    ASSERT_TRUE(message.ok()) << message.status().ToString();
    ASSERT_EQ(message->bytes().size(), sizeof(payload) - 1);
    EXPECT_EQ(std::memcmp(message->bytes().data(), payload, sizeof(payload) - 1),
              0);
    const Status released = std::move(*message).Release();
    EXPECT_TRUE(released.ok()) << released.ToString();
}

TEST_F(SimpleNodeTest, EndpointsRetainMappingAfterNodeDestruction) {
    const std::string name = MakeName("endpoint_lifetime");
    std::optional<SimplePublisher> publisher;
    std::optional<SimpleSubscriber> subscriber;
    {
        auto created = SimpleNode::Create(name);
        ASSERT_TRUE(created.ok()) << created.status().ToString();
        auto advertised = created->Advertise("camera");
        ASSERT_TRUE(advertised.ok()) << advertised.status().ToString();
        publisher.emplace(std::move(*advertised));
        auto subscribed = created->Subscribe("camera");
        ASSERT_TRUE(subscribed.ok()) << subscribed.status().ToString();
        subscriber.emplace(std::move(*subscribed));
    }

    const char payload[] = "mapping-stays-live";
    const Status published = publisher->Publish(std::as_bytes(std::span{
        reinterpret_cast<const std::byte*>(payload), sizeof(payload) - 1}));
    ASSERT_TRUE(published.ok()) << published.ToString();
    auto message = subscriber->TryPoll();
    ASSERT_TRUE(message.ok()) << message.status().ToString();
    ASSERT_EQ(message->bytes().size(), sizeof(payload) - 1);
    EXPECT_EQ(std::memcmp(message->bytes().data(), payload, sizeof(payload) - 1),
              0);
    EXPECT_TRUE(std::move(*message).Release().ok());
}

TEST_F(SimpleNodeTest, DestroyedEndpointsCanBeRecreated) {
    const std::string name = MakeName("recreate");
    auto created = SimpleNode::Create(name);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SimpleNode node = std::move(*created);

    {
        auto publisher = node.Advertise("camera");
        ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
    }
    auto publisher = node.Advertise("camera");
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();

    {
        auto subscriber = node.Subscribe("camera");
        ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();
    }
    auto subscriber = node.Subscribe("camera");
    ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();
}

TEST_F(SimpleNodeTest, BorrowDelaysSubscriberReleaseAndRejectsSecondPoll) {
    const std::string name = MakeName("borrow_lifetime");
    std::optional<BorrowedBytes> borrowed;
    {
        auto created = SimpleNode::Create(name);
        ASSERT_TRUE(created.ok()) << created.status().ToString();
        auto publisher = created->Advertise("camera");
        ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
        auto subscriber = created->Subscribe("camera");
        ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();

        const char payload[] = "borrow-outlives-subscriber";
        ASSERT_TRUE(publisher->Publish(std::as_bytes(std::span{
            reinterpret_cast<const std::byte*>(payload),
            sizeof(payload) - 1})).ok());
        auto message = subscriber->TryPoll();
        ASSERT_TRUE(message.ok()) << message.status().ToString();
        borrowed.emplace(std::move(*message));

        auto duplicate = subscriber->TryPoll();
        ASSERT_FALSE(duplicate.ok());
        EXPECT_EQ(duplicate.status().code(), StatusCode::kWouldBlock);
    }

    auto opened = SimpleNode::Open(name);
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    auto blocked = opened->Subscribe("camera");
    ASSERT_FALSE(blocked.ok());
    EXPECT_EQ(blocked.status().code(), StatusCode::kAlreadyExists);

    ASSERT_TRUE(borrowed->active());
    EXPECT_FALSE(borrowed->bytes().empty());
    EXPECT_TRUE(std::move(*borrowed).Release().ok());
    auto recreated = opened->Subscribe("camera");
    EXPECT_TRUE(recreated.ok()) << recreated.status().ToString();
}

TEST_F(SimpleNodeTest, ConcurrentAdvertiseCreatesOnePublisherClaim) {
    const std::string name = MakeName("advertise_race");
    auto created = SimpleNode::Create(name);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SimpleNode node = std::move(*created);

    std::atomic<uint32_t> ready{0};
    std::atomic<bool> start{false};
    std::atomic<uint32_t> codes[2];
    std::optional<SimplePublisher> publishers[2];
    auto advertise = [&](uint32_t index) {
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        auto result = node.Advertise("camera");
        if (result.ok()) {
            publishers[index].emplace(std::move(*result));
            codes[index].store(static_cast<uint32_t>(StatusCode::kOk),
                               std::memory_order_release);
        } else {
            codes[index].store(static_cast<uint32_t>(result.status().code()),
                               std::memory_order_release);
        }
    };

    std::thread first(advertise, 0);
    std::thread second(advertise, 1);
    while (ready.load(std::memory_order_acquire) != 2) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    first.join();
    second.join();

    uint32_t successes = 0;
    uint32_t duplicates = 0;
    for (uint32_t i = 0; i < 2; ++i) {
        const auto code = static_cast<StatusCode>(
            codes[i].load(std::memory_order_acquire));
        successes += code == StatusCode::kOk ? 1 : 0;
        duplicates += code == StatusCode::kAlreadyExists ? 1 : 0;
    }
    EXPECT_EQ(successes, 1u);
    EXPECT_EQ(duplicates, 1u);
}

[[noreturn]] void SubscriberChild(const std::string& name) {
    auto opened = SimpleNode::Open(name);
    if (!opened.ok()) _exit(kChildOpen);
    auto sub = opened->Subscribe("camera");
    if (!sub.ok()) _exit(kChildSubscribe);
    for (uint32_t i = 0; i < 8; ++i) {
        auto message = sub->Poll(Deadline::FromNow(std::chrono::seconds(3)));
        if (!message.ok()) _exit(kChildPoll);
        const std::string expected = "frame-" + std::to_string(i);
        if (message->bytes().size() != expected.size() ||
            std::memcmp(message->bytes().data(), expected.data(),
                        expected.size()) != 0) {
            _exit(kChildPayload);
        }
        if (!std::move(*message).Release().ok()) _exit(kChildPoll);
    }
    auto extra = sub->TryPoll();
    if (extra.ok()) _exit(kChildCount);
    if (extra.status().code() != StatusCode::kWouldBlock) _exit(kChildPoll);
    _exit(kChildOk);
}

TEST_F(SimpleNodeTest, TwoProcessPubSubBorrowsWithoutCopy) {
    const std::string name = MakeName("xproc");
    auto created = SimpleNode::Create(name);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SimpleNode node = std::move(*created);
    auto pub = node.Advertise("camera");
    ASSERT_TRUE(pub.ok()) << pub.status().ToString();

    const pid_t child = fork();
    ASSERT_NE(child, -1) << "fork failed";
    if (child == 0) {
        SubscriberChild(name);
    }

    for (uint32_t i = 0; i < 8; ++i) {
        const std::string payload = "frame-" + std::to_string(i);
        const Status published = pub->Publish(std::as_bytes(std::span{
            reinterpret_cast<const std::byte*>(payload.data()),
            payload.size()}));
        ASSERT_TRUE(published.ok()) << published.ToString();
    }

    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status)) << "subscriber child did not exit";
    EXPECT_EQ(WEXITSTATUS(status), kChildOk)
        << "subscriber child exit " << WEXITSTATUS(status);
}

}  // namespace
}  // namespace mino
