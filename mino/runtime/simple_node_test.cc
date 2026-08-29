// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/simple_node.h"

#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <csignal>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace mino {

struct SimpleTypedFrame {
    uint64_t sequence = 0;
    uint32_t value = 0;
    uint32_t reserved = 0;
};

template <>
struct StaticMessageTraits<SimpleTypedFrame> {
    static constexpr bool kIsSpecialized = true;
    static constexpr TypeId type_id{0x54595045u};
    static constexpr uint32_t message_type = 0x54595045u;
    static constexpr uint32_t schema_version = 0x00010000u;
    static constexpr uint64_t schema_short_id = 0x53494D504C455459ull;
    static constexpr uint32_t layout_version = 1;
    static constexpr uint32_t index_flags = 0;
    static Status Validate(const SimpleTypedFrame& value) noexcept {
        return value.reserved == 0
                   ? Status::Ok()
                   : Status::Error(StatusCode::kInvalidArgument,
                                   "reserved field must be zero");
    }
};

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

TEST_F(SimpleNodeTest, DefaultRecoveryLayoutFitsUnderEightMiB) {
    SimpleNodeOptions options;
    options.topic_slots = 8;
    options.queue_depth = 32;
    options.max_payload_bytes = 256;
    auto bytes = SimpleNode::RequiredBytes(options);
    ASSERT_TRUE(bytes.ok()) << bytes.status().ToString();
    EXPECT_LT(*bytes, 8ull << 20)
        << "default recovery metadata should remain compact";
    EXPECT_GT(*bytes, 1u << 20)
        << "the layout includes the fixed crash-safe Pin table";
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
    EXPECT_LT(node.size_bytes(), 8ull << 20);

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

TEST_F(SimpleNodeTest, TypedPublishAndSubscribeValidateSchema) {
    const std::string name = MakeName("typed");
    SimpleNodeOptions node_options;
    node_options.max_payload_bytes = 256;
    auto created = SimpleNode::Create(name, node_options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();

    auto publisher = created->Advertise<SimpleTypedFrame>("telemetry");
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
    auto subscriber = created->Subscribe<SimpleTypedFrame>("telemetry");
    ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();

    const SimpleTypedFrame sent{.sequence = 42, .value = 7};
    ASSERT_TRUE(publisher->Publish(sent).ok());
    auto received = subscriber->TryPoll<SimpleTypedFrame>();
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    EXPECT_EQ(received->get()->sequence, 42u);
    EXPECT_EQ(received->get()->value, 7u);
    EXPECT_TRUE(std::move(*received).Release().ok());

    auto raw_subscriber = created->Subscribe("telemetry");
    ASSERT_FALSE(raw_subscriber.ok());
    EXPECT_EQ(raw_subscriber.status().code(), StatusCode::kSchemaMismatch);
}

TEST_F(SimpleNodeTest, MpscAllowsMultiplePublishersAndOneSubscriber) {
    const std::string name = MakeName("mpsc");
    SimpleNodeOptions node_options;
    node_options.queue_depth = 64;
    node_options.max_publishers_per_topic = 4;
    auto created = SimpleNode::Create(name, node_options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();

    SimpleTopicOptions topic_options;
    topic_options.mode = SimpleTopicMode::kMpsc;
    topic_options.queue_full_policy = QueueFullPolicy::kFail;
    auto first = created->Advertise("jobs", topic_options);
    auto second = created->Advertise("jobs", topic_options);
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    auto subscriber = created->Subscribe("jobs");
    ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();

    const char one[] = "one";
    const char two[] = "two";
    ASSERT_TRUE(first->Publish(std::as_bytes(std::span(one, 3))).ok());
    ASSERT_TRUE(second->Publish(std::as_bytes(std::span(two, 3))).ok());
    auto received_one = subscriber->TryPoll();
    ASSERT_TRUE(received_one.ok()) << received_one.status().ToString();
    EXPECT_EQ(std::memcmp(received_one->bytes().data(), one, 3), 0);
    EXPECT_TRUE(std::move(*received_one).Release().ok());
    auto received_two = subscriber->TryPoll();
    ASSERT_TRUE(received_two.ok()) << received_two.status().ToString();
    EXPECT_EQ(std::memcmp(received_two->bytes().data(), two, 3), 0);
    EXPECT_TRUE(std::move(*received_two).Release().ok());
}

TEST_F(SimpleNodeTest, BroadcastDeliversToEverySubscriber) {
    const std::string name = MakeName("broadcast");
    auto created = SimpleNode::Create(name);
    ASSERT_TRUE(created.ok()) << created.status().ToString();

    SimpleTopicOptions topic_options;
    topic_options.mode = SimpleTopicMode::kBroadcast;
    topic_options.queue_full_policy = QueueFullPolicy::kFail;
    auto publisher = created->Advertise("events", topic_options);
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
    auto first = created->Subscribe("events");
    auto second = created->Subscribe("events");
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();

    const char payload[] = "fanout";
    ASSERT_TRUE(publisher->Publish(
        std::as_bytes(std::span(payload, sizeof(payload) - 1))).ok());
    auto first_message = first->TryPoll();
    auto second_message = second->TryPoll();
    ASSERT_TRUE(first_message.ok()) << first_message.status().ToString();
    ASSERT_TRUE(second_message.ok()) << second_message.status().ToString();
    EXPECT_EQ(std::memcmp(first_message->bytes().data(), payload,
                          sizeof(payload) - 1), 0);
    EXPECT_EQ(std::memcmp(second_message->bytes().data(), payload,
                          sizeof(payload) - 1), 0);
    EXPECT_TRUE(std::move(*first_message).Release().ok());
    EXPECT_TRUE(std::move(*second_message).Release().ok());
}

TEST_F(SimpleNodeTest, DropNewestReportsDegradedWithoutBlocking) {
    const std::string name = MakeName("drop_newest");
    SimpleNodeOptions node_options;
    node_options.queue_depth = 2;
    auto created = SimpleNode::Create(name, node_options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();

    SimpleTopicOptions topic_options;
    topic_options.queue_full_policy = QueueFullPolicy::kDropNewest;
    auto publisher = created->Advertise("camera", topic_options);
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
    const char payload[] = "x";
    const auto bytes = std::as_bytes(std::span(payload, 1));
    ASSERT_TRUE(publisher->Publish(bytes).ok());
    ASSERT_TRUE(publisher->Publish(bytes).ok());
    const Status dropped = publisher->Publish(bytes);
    EXPECT_EQ(dropped.code(), StatusCode::kDegraded) << dropped.ToString();
}

TEST_F(SimpleNodeTest, SampleDropsOrTimesOutWithoutUnboundedBlocking) {
    const std::string name = MakeName("sample");
    SimpleNodeOptions node_options;
    node_options.queue_depth = 2;
    node_options.topic_slots = 2;
    auto created = SimpleNode::Create(name, node_options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    const char payload[] = "x";
    const auto bytes = std::as_bytes(std::span(payload, 1));

    SimpleTopicOptions dropped_options;
    dropped_options.queue_full_policy = QueueFullPolicy::kSample;
    dropped_options.sample_rate = 3;
    auto dropped_publisher = created->Advertise("dropped", dropped_options);
    ASSERT_TRUE(dropped_publisher.ok())
        << dropped_publisher.status().ToString();
    ASSERT_TRUE(dropped_publisher->Publish(bytes).ok());
    ASSERT_TRUE(dropped_publisher->Publish(bytes).ok());
    const Status sampled_out = dropped_publisher->Publish(bytes);
    EXPECT_EQ(sampled_out.code(), StatusCode::kDegraded)
        << sampled_out.ToString();

    SimpleTopicOptions admitted_options;
    admitted_options.queue_full_policy = QueueFullPolicy::kSample;
    admitted_options.sample_rate = 2;
    auto admitted_publisher = created->Advertise("admitted", admitted_options);
    ASSERT_TRUE(admitted_publisher.ok())
        << admitted_publisher.status().ToString();
    ASSERT_TRUE(admitted_publisher->Publish(bytes).ok());
    ASSERT_TRUE(admitted_publisher->Publish(bytes).ok());
    const Status timed_out = admitted_publisher->Publish(
        bytes, Deadline::FromNow(std::chrono::milliseconds(2)));
    EXPECT_EQ(timed_out.code(), StatusCode::kTimeout)
        << timed_out.ToString();
}

TEST_F(SimpleNodeTest, DropOldestKeepsOutstandingBorrowPinned) {
    const std::string name = MakeName("drop_oldest");
    SimpleNodeOptions node_options;
    node_options.queue_depth = 2;
    auto created = SimpleNode::Create(name, node_options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();

    SimpleTopicOptions topic_options;
    topic_options.queue_full_policy = QueueFullPolicy::kDropOldest;
    auto publisher = created->Advertise("camera", topic_options);
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();
    auto subscriber = created->Subscribe("camera");
    ASSERT_TRUE(subscriber.ok()) << subscriber.status().ToString();

    const char first[] = "first";
    const char second[] = "second";
    const char third[] = "third";
    ASSERT_TRUE(publisher->Publish(std::as_bytes(std::span(first, 5))).ok());
    auto borrowed = subscriber->TryPoll();
    ASSERT_TRUE(borrowed.ok()) << borrowed.status().ToString();
    ASSERT_TRUE(publisher->Publish(std::as_bytes(std::span(second, 6))).ok());
    ASSERT_TRUE(publisher->Publish(std::as_bytes(std::span(third, 5))).ok());

    EXPECT_EQ(std::memcmp(borrowed->bytes().data(), first, 5), 0)
        << "dropped payload must remain pinned while borrowed";
    const Status late_release = std::move(*borrowed).Release();
    EXPECT_EQ(late_release.code(), StatusCode::kNotFound)
        << late_release.ToString();

    auto next = subscriber->TryPoll();
    ASSERT_TRUE(next.ok()) << next.status().ToString();
    EXPECT_EQ(std::memcmp(next->bytes().data(), second, 6), 0);
    EXPECT_TRUE(std::move(*next).Release().ok());
    auto last = subscriber->TryPoll();
    ASSERT_TRUE(last.ok()) << last.status().ToString();
    EXPECT_EQ(std::memcmp(last->bytes().data(), third, 5), 0);
    EXPECT_TRUE(std::move(*last).Release().ok());
}

TEST_F(SimpleNodeTest, DeadPublisherClaimCanBeRecovered) {
    const std::string name = MakeName("dead_publisher");
    auto created = SimpleNode::Create(name);
    ASSERT_TRUE(created.ok()) << created.status().ToString();

    int ready[2];
    ASSERT_EQ(pipe(ready), 0);
    const pid_t child = fork();
    ASSERT_NE(child, -1);
    if (child == 0) {
        close(ready[0]);
        auto opened = SimpleNode::Open(name);
        if (!opened.ok()) _exit(1);
        auto publisher = opened->Advertise("camera");
        if (!publisher.ok()) _exit(2);
        const char marker = 'R';
        if (write(ready[1], &marker, 1) != 1) _exit(3);
        pause();
        _exit(4);
    }
    close(ready[1]);
    char marker = 0;
    ASSERT_EQ(read(ready[0], &marker, 1), 1);
    ASSERT_EQ(marker, 'R');
    ASSERT_EQ(kill(child, SIGKILL), 0);
    int child_status = 0;
    ASSERT_EQ(waitpid(child, &child_status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(child_status));

    ASSERT_TRUE(created->Recover().ok());
    auto replacement = created->Advertise("camera");
    EXPECT_TRUE(replacement.ok()) << replacement.status().ToString();
}

TEST_F(SimpleNodeTest, DeadBroadcastBorrowIsEvictedAndCanResubscribe) {
    const std::string name = MakeName("dead_broadcast_borrow");
    auto created = SimpleNode::Create(name);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SimpleTopicOptions topic_options;
    topic_options.mode = SimpleTopicMode::kBroadcast;
    auto publisher = created->Advertise("events", topic_options);
    ASSERT_TRUE(publisher.ok()) << publisher.status().ToString();

    int parent_to_child[2];
    int child_to_parent[2];
    ASSERT_EQ(pipe(parent_to_child), 0);
    ASSERT_EQ(pipe(child_to_parent), 0);
    const pid_t child = fork();
    ASSERT_NE(child, -1);
    if (child == 0) {
        close(parent_to_child[1]);
        close(child_to_parent[0]);
        auto opened = SimpleNode::Open(name);
        if (!opened.ok()) _exit(1);
        auto subscriber = opened->Subscribe("events");
        if (!subscriber.ok()) _exit(2);
        const char subscribed = 'S';
        if (write(child_to_parent[1], &subscribed, 1) != 1) _exit(3);
        char publish_ready = 0;
        if (read(parent_to_child[0], &publish_ready, 1) != 1 ||
            publish_ready != 'P') {
            _exit(4);
        }
        auto borrowed = subscriber->Poll(
            Deadline::FromNow(std::chrono::seconds(3)));
        if (!borrowed.ok()) _exit(5);
        const char borrowed_ready = 'B';
        if (write(child_to_parent[1], &borrowed_ready, 1) != 1) _exit(6);
        pause();
        _exit(7);
    }
    close(parent_to_child[0]);
    close(child_to_parent[1]);
    char marker = 0;
    ASSERT_EQ(read(child_to_parent[0], &marker, 1), 1);
    ASSERT_EQ(marker, 'S');
    const char first[] = "before-crash";
    ASSERT_TRUE(publisher->Publish(
        std::as_bytes(std::span(first, sizeof(first) - 1))).ok());
    const char publish_ready = 'P';
    ASSERT_EQ(write(parent_to_child[1], &publish_ready, 1), 1);
    ASSERT_EQ(read(child_to_parent[0], &marker, 1), 1);
    ASSERT_EQ(marker, 'B');
    ASSERT_EQ(kill(child, SIGKILL), 0);
    int child_status = 0;
    ASSERT_EQ(waitpid(child, &child_status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(child_status));

    ASSERT_TRUE(created->Recover().ok());
    auto replacement = created->Subscribe("events");
    ASSERT_TRUE(replacement.ok()) << replacement.status().ToString();
    const char second[] = "after-crash";
    ASSERT_TRUE(publisher->Publish(
        std::as_bytes(std::span(second, sizeof(second) - 1))).ok());
    auto received = replacement->Poll(
        Deadline::FromNow(std::chrono::seconds(3)));
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    EXPECT_EQ(std::memcmp(received->bytes().data(), second,
                          sizeof(second) - 1), 0);
    EXPECT_TRUE(std::move(*received).Release().ok());
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
