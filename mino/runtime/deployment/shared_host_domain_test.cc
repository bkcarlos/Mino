// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/deployment/shared_host_domain.h"

#include "mino/runtime/message_traits.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

namespace mino::deployment {

struct SharedHostTypedFrame {
    uint32_t sequence = 0;
    uint32_t value = 0;
    uint32_t reserved = 0;
};

}  // namespace mino::deployment

namespace mino {
template <>
struct StaticMessageTraits<deployment::SharedHostTypedFrame> {
    static constexpr bool kIsSpecialized = true;
    static constexpr TypeId type_id{0x53484454u};
    static constexpr uint32_t message_type = 0x53484454u;
    static constexpr uint32_t schema_version = 0x00010000u;
    static constexpr uint64_t schema_short_id = 0x5348445459504544ull;
    static constexpr uint32_t layout_version = 1;
    static constexpr uint32_t index_flags = 0;
    static Status Validate(
        const deployment::SharedHostTypedFrame& value) noexcept {
        return value.reserved == 0
                   ? Status::Ok()
                   : Status::Error(StatusCode::kInvalidArgument,
                                   "reserved field must be zero");
    }
};
}  // namespace mino

namespace mino::deployment {
namespace {

std::string UniqueName(const char* tag) {
    static std::atomic<uint32_t> sequence{0};
    return std::string("/shd") + std::to_string(::getpid()) + "_" +
           std::to_string(sequence.fetch_add(1) + 1) + "_" + tag;
}

schema::SchemaIdentity MakeSchema(uint64_t short_id, uint32_t version = 1) {
    schema::CanonicalDigest digest{};
    digest[0] = static_cast<std::byte>(short_id & 0xff);
    digest[1] = static_cast<std::byte>((short_id >> 8) & 0xff);
    digest[31] = static_cast<std::byte>(version);
    return schema::SchemaIdentity(short_id, digest, version, version);
}

class SharedHostDomainTest : public ::testing::Test {
protected:
    void SetUp() override { name_ = UniqueName("unit"); }
    void TearDown() override { (void)SharedHostDomain::Unlink(name_); }
    std::string name_;
};

TEST_F(SharedHostDomainTest, DynamicAdvertiseSubscribeWithoutStaticPeerList) {
    SharedHostDomainOptions options;
    options.peer_slots = 4;
    options.topic_slots = 4;
    options.queue_depth = 8;
    options.max_payload_bytes = 64;

    auto created = SharedHostDomain::Create(name_, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SharedHostDomain creator = std::move(*created);
    ASSERT_TRUE(creator.Join(NodeId{1}).ok());

    // Second handle in the same process reuses the ProcessIdentity peer slot.
    auto opened = SharedHostDomain::Open(name_);
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    SharedHostDomain joiner = std::move(*opened);
    ASSERT_TRUE(joiner.Join(NodeId{2}).ok());
    auto peers = joiner.ListPeers();
    ASSERT_TRUE(peers.ok()) << peers.status().ToString();
    ASSERT_EQ(peers->size(), 1u);

    const auto schema = MakeSchema(42);
    auto pub = creator.Advertise("telemetry", schema);
    ASSERT_TRUE(pub.ok()) << pub.status().ToString();
    auto sub = joiner.Subscribe("telemetry", schema);
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();

    const char* payload = "hello-domain";
    ASSERT_TRUE(
        pub->Publish(std::as_bytes(std::span(payload, std::strlen(payload))))
            .ok());
    auto message = sub->Poll(Deadline::FromNow(std::chrono::seconds(2)));
    ASSERT_TRUE(message.ok()) << message.status().ToString();
    ASSERT_EQ(message->size(), std::strlen(payload));
    EXPECT_EQ(std::memcmp(message->data(), payload, message->size()), 0);
}

TEST_F(SharedHostDomainTest, LateJoinerAttachesToExistingTopic) {
    SharedHostDomainOptions options;
    options.queue_depth = 8;
    options.max_payload_bytes = 32;
    auto created = SharedHostDomain::Create(name_, options);
    ASSERT_TRUE(created.ok());
    SharedHostDomain first = std::move(*created);
    ASSERT_TRUE(first.Join(NodeId{10}).ok());
    const auto schema = MakeSchema(7);
    auto pub = first.Advertise("late", schema);
    ASSERT_TRUE(pub.ok());
    ASSERT_TRUE(pub->Publish(std::as_bytes(std::span("ab", 2))).ok());

    auto opened = SharedHostDomain::Open(name_);
    ASSERT_TRUE(opened.ok());
    SharedHostDomain late = std::move(*opened);
    ASSERT_TRUE(late.Join(NodeId{11}).ok());
    auto topics = late.ListTopics();
    ASSERT_TRUE(topics.ok());
    ASSERT_EQ(topics->size(), 1u);
    EXPECT_EQ(topics->front().name, "late");

    auto sub = late.Subscribe("late", schema,
                              Deadline::FromNow(std::chrono::seconds(2)));
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();
    // Prior message may or may not still be in the ring; publish a fresh one.
    ASSERT_TRUE(pub->Publish(std::as_bytes(std::span("cd", 2))).ok());
    auto message = sub->Poll(Deadline::FromNow(std::chrono::seconds(2)));
    ASSERT_TRUE(message.ok()) << message.status().ToString();
    ASSERT_EQ(message->size(), 2u);
    EXPECT_EQ((*message)[0], std::byte{'c'});
    EXPECT_EQ((*message)[1], std::byte{'d'});
}

TEST_F(SharedHostDomainTest, SchemaMismatchFailsClosed) {
    auto created = SharedHostDomain::Create(name_);
    ASSERT_TRUE(created.ok());
    SharedHostDomain domain = std::move(*created);
    ASSERT_TRUE(domain.Join(NodeId{1}).ok());
    ASSERT_TRUE(domain.Advertise("q", MakeSchema(1)).ok());
    auto mismatch = domain.Subscribe("q", MakeSchema(2));
    ASSERT_FALSE(mismatch.ok());
    EXPECT_EQ(mismatch.status().code(), StatusCode::kSchemaMismatch);
    auto bad_advertise = domain.Advertise("q", MakeSchema(3));
    ASSERT_FALSE(bad_advertise.ok());
    EXPECT_EQ(bad_advertise.status().code(), StatusCode::kSchemaMismatch);
}

TEST_F(SharedHostDomainTest, DeadPeerRecoveredWithoutWedgingSurvivor) {
    SharedHostDomainOptions options;
    options.peer_slots = 4;
    options.topic_slots = 2;
    options.queue_depth = 8;
    options.max_payload_bytes = 32;
    auto created = SharedHostDomain::Create(name_, options);
    ASSERT_TRUE(created.ok());
    SharedHostDomain survivor = std::move(*created);
    ASSERT_TRUE(survivor.Join(NodeId{1}).ok());
    const auto schema = MakeSchema(99);
    auto pub = survivor.Advertise("alive", schema);
    ASSERT_TRUE(pub.ok());

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        auto opened = SharedHostDomain::Open(name_);
        if (!opened.ok()) _exit(11);
        SharedHostDomain peer = std::move(*opened);
        if (!peer.Join(NodeId{2}).ok()) _exit(12);
        auto sub = peer.Subscribe("alive", schema,
                                  Deadline::FromNow(std::chrono::seconds(2)));
        if (!sub.ok()) _exit(13);
        // Stay joined until killed.
        for (;;) {
            (void)peer.Heartbeat();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    // Wait until the child peer appears.
    for (int i = 0; i < 200; ++i) {
        auto peers = survivor.ListPeers();
        ASSERT_TRUE(peers.ok());
        if (peers->size() >= 2) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    {
        auto peers = survivor.ListPeers();
        ASSERT_TRUE(peers.ok());
        ASSERT_GE(peers->size(), 2u);
    }

    ASSERT_EQ(::kill(child, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);

    // Poll until the kernel reports the child dead, then Recover.
    for (int i = 0; i < 200; ++i) {
        ASSERT_TRUE(survivor.Recover().ok());
        auto peers = survivor.ListPeers();
        ASSERT_TRUE(peers.ok());
        if (peers->size() == 1) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    auto peers = survivor.ListPeers();
    ASSERT_TRUE(peers.ok());
    ASSERT_EQ(peers->size(), 1u);
    EXPECT_EQ(peers->front().node_id.value, 1u);

    // Survivor continues to publish.
    auto sub2_domain = SharedHostDomain::Open(name_);
    ASSERT_TRUE(sub2_domain.ok());
    SharedHostDomain replacement = std::move(*sub2_domain);
    ASSERT_TRUE(replacement.Join(NodeId{3}).ok());
    auto sub = replacement.Subscribe("alive", schema);
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();
    ASSERT_TRUE(pub->Publish(std::as_bytes(std::span("ok", 2))).ok());
    auto message = sub->Poll(Deadline::FromNow(std::chrono::seconds(2)));
    ASSERT_TRUE(message.ok()) << message.status().ToString();
    ASSERT_EQ(message->size(), 2u);
}

TEST_F(SharedHostDomainTest, DeadPublisherRecoverAllowsReadvertise) {
    SharedHostDomainOptions options;
    options.peer_slots = 4;
    options.topic_slots = 2;
    options.queue_depth = 8;
    options.max_payload_bytes = 32;
    auto created = SharedHostDomain::Create(name_, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SharedHostDomain survivor = std::move(*created);
    ASSERT_TRUE(survivor.Join(NodeId{1}).ok());
    const auto schema = MakeSchema(55);

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        auto opened = SharedHostDomain::Open(name_);
        if (!opened.ok()) _exit(21);
        SharedHostDomain peer = std::move(*opened);
        if (!peer.Join(NodeId{2}).ok()) _exit(22);
        auto pub = peer.Advertise("recover-pub", schema);
        if (!pub.ok()) _exit(23);
        for (;;) {
            (void)peer.Heartbeat();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    bool saw_publisher = false;
    for (int i = 0; i < 200; ++i) {
        ASSERT_TRUE(survivor.Recover().ok());
        auto topics = survivor.ListTopics();
        ASSERT_TRUE(topics.ok()) << topics.status().ToString();
        for (const auto& topic : *topics) {
            if (topic.name == "recover-pub" && topic.publisher_active) {
                saw_publisher = true;
                break;
            }
        }
        if (saw_publisher) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(saw_publisher);

    ASSERT_EQ(::kill(child, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);

    bool reclaimed = false;
    for (int i = 0; i < 200; ++i) {
        ASSERT_TRUE(survivor.Recover().ok());
        auto topics = survivor.ListTopics();
        ASSERT_TRUE(topics.ok());
        bool active = false;
        for (const auto& topic : *topics) {
            if (topic.name == "recover-pub") active = topic.publisher_active;
        }
        if (!active) {
            reclaimed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(reclaimed);

    auto pub = survivor.Advertise("recover-pub", schema);
    ASSERT_TRUE(pub.ok()) << pub.status().ToString();
    auto sub = survivor.Subscribe("recover-pub", schema);
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();
    ASSERT_TRUE(pub->Publish(std::as_bytes(std::span("xy", 2))).ok());
    auto message = sub->Poll(Deadline::FromNow(std::chrono::seconds(2)));
    ASSERT_TRUE(message.ok()) << message.status().ToString();
    ASSERT_EQ(message->size(), 2u);
    EXPECT_EQ((*message)[0], std::byte{'x'});
    EXPECT_EQ((*message)[1], std::byte{'y'});
}

TEST_F(SharedHostDomainTest, SinglePublisherFailClosedThenReadvertise) {
    SharedHostDomainOptions options;
    options.queue_depth = 8;
    options.max_payload_bytes = 32;
    auto created = SharedHostDomain::Create(name_, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SharedHostDomain domain = std::move(*created);
    ASSERT_TRUE(domain.Join(NodeId{1}).ok());
    const auto schema = MakeSchema(11);

    {
        auto first = domain.Advertise("solo", schema);
        ASSERT_TRUE(first.ok()) << first.status().ToString();

        // Second live Advertise fails closed (lease claim times out).
        auto second = domain.Advertise("solo", schema);
        ASSERT_FALSE(second.ok());
        EXPECT_EQ(second.status().code(), StatusCode::kTimeout);
    }  // publisher dtor releases the topic lease

    auto again = domain.Advertise("solo", schema);
    ASSERT_TRUE(again.ok()) << again.status().ToString();
    auto sub = domain.Subscribe("solo", schema);
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();
    ASSERT_TRUE(again->Publish(std::as_bytes(std::span("z", 1))).ok());
    auto message = sub->Poll(Deadline::FromNow(std::chrono::seconds(2)));
    ASSERT_TRUE(message.ok()) << message.status().ToString();
    ASSERT_EQ(message->size(), 1u);
    EXPECT_EQ((*message)[0], std::byte{'z'});
}

TEST_F(SharedHostDomainTest, ExplicitLeaveAndHeartbeat) {
    SharedHostDomainOptions options;
    options.peer_slots = 4;
    auto created = SharedHostDomain::Create(name_, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SharedHostDomain domain = std::move(*created);
    ASSERT_TRUE(domain.Join(NodeId{7}).ok());

    auto peers = domain.ListPeers();
    ASSERT_TRUE(peers.ok()) << peers.status().ToString();
    ASSERT_EQ(peers->size(), 1u);
    EXPECT_EQ(peers->front().node_id.value, 7u);
    const uint64_t hb0 = peers->front().last_heartbeat_ns;
    ASSERT_NE(hb0, 0u);

    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    ASSERT_TRUE(domain.Heartbeat().ok());
    peers = domain.ListPeers();
    ASSERT_TRUE(peers.ok());
    ASSERT_EQ(peers->size(), 1u);
    EXPECT_GT(peers->front().last_heartbeat_ns, hb0);

    ASSERT_TRUE(domain.Leave().ok());
    peers = domain.ListPeers();
    ASSERT_TRUE(peers.ok());
    EXPECT_EQ(peers->size(), 0u);

    // Can Join again after Leave.
    ASSERT_TRUE(domain.Join(NodeId{8}).ok());
    peers = domain.ListPeers();
    ASSERT_TRUE(peers.ok());
    ASSERT_EQ(peers->size(), 1u);
    EXPECT_EQ(peers->front().node_id.value, 8u);
}

TEST_F(SharedHostDomainTest, DualSubscriberBroadcast) {
    SharedHostDomainOptions options;
    options.queue_depth = 8;
    options.max_payload_bytes = 64;
    options.max_subscribers = 8;
    auto created = SharedHostDomain::Create(name_, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SharedHostDomain domain = std::move(*created);
    ASSERT_TRUE(domain.Join(NodeId{1}).ok());
    const auto schema = MakeSchema(77);

    auto pub = domain.Advertise("fanout", schema);
    ASSERT_TRUE(pub.ok()) << pub.status().ToString();
    auto sub_a = domain.Subscribe("fanout", schema);
    ASSERT_TRUE(sub_a.ok()) << sub_a.status().ToString();
    auto sub_b = domain.Subscribe("fanout", schema);
    ASSERT_TRUE(sub_b.ok()) << sub_b.status().ToString();

    const char* payload = "broadcast-me";
    ASSERT_TRUE(
        pub->Publish(std::as_bytes(std::span(payload, std::strlen(payload))))
            .ok());

    auto msg_a = sub_a->Poll(Deadline::FromNow(std::chrono::seconds(2)));
    ASSERT_TRUE(msg_a.ok()) << msg_a.status().ToString();
    auto msg_b = sub_b->Poll(Deadline::FromNow(std::chrono::seconds(2)));
    ASSERT_TRUE(msg_b.ok()) << msg_b.status().ToString();
    ASSERT_EQ(msg_a->size(), std::strlen(payload));
    ASSERT_EQ(msg_b->size(), std::strlen(payload));
    EXPECT_EQ(std::memcmp(msg_a->data(), payload, msg_a->size()), 0);
    EXPECT_EQ(std::memcmp(msg_b->data(), payload, msg_b->size()), 0);
}


TEST_F(SharedHostDomainTest, MpscAllowsMultiplePublishers) {
    SharedHostDomainOptions options;
    options.peer_slots = 4;
    options.topic_slots = 2;
    options.queue_depth = 64;
    options.max_publishers_per_topic = 4;
    options.max_payload_bytes = 64;
    auto created = SharedHostDomain::Create(name_, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SharedHostDomain domain = std::move(*created);
    ASSERT_TRUE(domain.Join(NodeId{1}).ok());
    const auto schema = MakeSchema(21);

    SharedHostTopicOptions topic_options;
    topic_options.mode = SharedHostTopicMode::kMpsc;
    topic_options.queue_full_policy = QueueFullPolicy::kFail;
    auto first = domain.Advertise("jobs", schema, topic_options);
    auto second = domain.Advertise("jobs", schema, topic_options);
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    EXPECT_EQ(first->mode(), SharedHostTopicMode::kMpsc);
    EXPECT_EQ(second->mode(), SharedHostTopicMode::kMpsc);

    auto topics = domain.ListTopics();
    ASSERT_TRUE(topics.ok());
    ASSERT_EQ(topics->size(), 1u);
    EXPECT_EQ(topics->front().mode, SharedHostTopicMode::kMpsc);
    EXPECT_EQ(topics->front().active_publishers, 2u);
    EXPECT_TRUE(topics->front().publisher_active);

    auto sub = domain.Subscribe("jobs", schema);
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();
    ASSERT_TRUE(first->Publish(std::as_bytes(std::span("one", 3))).ok());
    ASSERT_TRUE(second->Publish(std::as_bytes(std::span("two", 3))).ok());

    auto msg1 = sub->Poll(Deadline::FromNow(std::chrono::seconds(2)));
    ASSERT_TRUE(msg1.ok()) << msg1.status().ToString();
    ASSERT_EQ(msg1->size(), 3u);
    EXPECT_EQ(std::memcmp(msg1->data(), "one", 3), 0);
    auto msg2 = sub->Poll(Deadline::FromNow(std::chrono::seconds(2)));
    ASSERT_TRUE(msg2.ok()) << msg2.status().ToString();
    ASSERT_EQ(msg2->size(), 3u);
    EXPECT_EQ(std::memcmp(msg2->data(), "two", 3), 0);

    // Mode/QoS mismatch fails closed.
    SharedHostTopicOptions broadcast = topic_options;
    broadcast.mode = SharedHostTopicMode::kBroadcast;
    auto mismatch = domain.Advertise("jobs", schema, broadcast);
    ASSERT_FALSE(mismatch.ok());
    EXPECT_EQ(mismatch.status().code(), StatusCode::kSchemaMismatch);
}

TEST_F(SharedHostDomainTest, DeadMpscPublisherLeaseRecovered) {
    SharedHostDomainOptions options;
    options.peer_slots = 4;
    options.topic_slots = 2;
    options.queue_depth = 64;
    options.max_publishers_per_topic = 2;
    options.max_payload_bytes = 32;
    auto created = SharedHostDomain::Create(name_, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SharedHostDomain survivor = std::move(*created);
    ASSERT_TRUE(survivor.Join(NodeId{1}).ok());
    const auto schema = MakeSchema(88);
    SharedHostTopicOptions topic_options;
    topic_options.mode = SharedHostTopicMode::kMpsc;

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        auto opened = SharedHostDomain::Open(name_);
        if (!opened.ok()) _exit(31);
        SharedHostDomain peer = std::move(*opened);
        if (!peer.Join(NodeId{2}).ok()) _exit(32);
        auto pub = peer.Advertise("mpsc-recover", schema, topic_options);
        if (!pub.ok()) _exit(33);
        for (;;) {
            (void)peer.Heartbeat();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    bool saw = false;
    for (int i = 0; i < 200; ++i) {
        ASSERT_TRUE(survivor.Recover().ok());
        auto topics = survivor.ListTopics();
        ASSERT_TRUE(topics.ok());
        for (const auto& topic : *topics) {
            if (topic.name == "mpsc-recover" && topic.active_publishers >= 1) {
                saw = true;
                break;
            }
        }
        if (saw) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(saw);

    ASSERT_EQ(::kill(child, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);

    bool reclaimed = false;
    for (int i = 0; i < 200; ++i) {
        ASSERT_TRUE(survivor.Recover().ok());
        auto topics = survivor.ListTopics();
        ASSERT_TRUE(topics.ok());
        for (const auto& topic : *topics) {
            if (topic.name == "mpsc-recover" && topic.active_publishers == 0) {
                reclaimed = true;
                break;
            }
        }
        if (reclaimed) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(reclaimed);

    auto pub = survivor.Advertise("mpsc-recover", schema, topic_options);
    ASSERT_TRUE(pub.ok()) << pub.status().ToString();
    auto sub = survivor.Subscribe("mpsc-recover", schema);
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();
    ASSERT_TRUE(pub->Publish(std::as_bytes(std::span("ok", 2))).ok());
    auto message = sub->Poll(Deadline::FromNow(std::chrono::seconds(2)));
    ASSERT_TRUE(message.ok()) << message.status().ToString();
    ASSERT_EQ(message->size(), 2u);
}

TEST_F(SharedHostDomainTest, BorrowPathIsZeroCopy) {
    SharedHostDomainOptions options;
    options.queue_depth = 8;
    options.max_payload_bytes = 64;
    auto created = SharedHostDomain::Create(name_, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SharedHostDomain domain = std::move(*created);
    ASSERT_TRUE(domain.Join(NodeId{1}).ok());
    const auto schema = MakeSchema(33);
    auto pub = domain.Advertise("borrow", schema);
    ASSERT_TRUE(pub.ok()) << pub.status().ToString();
    auto sub = domain.Subscribe("borrow", schema);
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();

    const char* payload = "zero-copy";
    ASSERT_TRUE(
        pub->Publish(std::as_bytes(std::span(payload, std::strlen(payload))))
            .ok());
    auto borrowed = sub->TryPollBorrow();
    ASSERT_TRUE(borrowed.ok()) << borrowed.status().ToString();
    ASSERT_TRUE(borrowed->active());
    ASSERT_EQ(borrowed->bytes().size(), std::strlen(payload));
    EXPECT_EQ(std::memcmp(borrowed->bytes().data(), payload,
                          borrowed->bytes().size()),
              0);
    // Second borrow while first is live fails closed.
    auto duplicate = sub->TryPollBorrow();
    ASSERT_FALSE(duplicate.ok());
    EXPECT_EQ(duplicate.status().code(), StatusCode::kWouldBlock);
    ASSERT_TRUE(std::move(*borrowed).Release().ok());
}

TEST_F(SharedHostDomainTest, FailClosedSuite) {
    // RequiredBytes is sensible for default and scaled options.
    auto default_bytes = SharedHostDomain::RequiredBytes({});
    ASSERT_TRUE(default_bytes.ok()) << default_bytes.status().ToString();
    EXPECT_GT(*default_bytes, sizeof(uint64_t));

    SharedHostDomainOptions tiny;
    tiny.peer_slots = 2;
    tiny.topic_slots = 2;
    tiny.queue_depth = 4;
    tiny.max_subscribers = 2;
    tiny.max_payload_bytes = 64;
    auto tiny_bytes = SharedHostDomain::RequiredBytes(tiny);
    ASSERT_TRUE(tiny_bytes.ok()) << tiny_bytes.status().ToString();
    EXPECT_LT(*tiny_bytes, *default_bytes);

    SharedHostDomainOptions bad_depth = tiny;
    bad_depth.queue_depth = 3;  // not power of two
    auto bad_req = SharedHostDomain::RequiredBytes(bad_depth);
    ASSERT_FALSE(bad_req.ok());
    EXPECT_EQ(bad_req.status().code(), StatusCode::kInvalidArgument);
    auto bad_create = SharedHostDomain::Create(name_ + "_badq", bad_depth);
    ASSERT_FALSE(bad_create.ok());
    EXPECT_EQ(bad_create.status().code(), StatusCode::kInvalidArgument);

    // Unlink then Open fails.
    auto created = SharedHostDomain::Create(name_, tiny);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SharedHostDomain domain = std::move(*created);
    ASSERT_TRUE(SharedHostDomain::Unlink(name_).ok());
    auto reopened = SharedHostDomain::Open(name_);
    ASSERT_FALSE(reopened.ok());
    EXPECT_EQ(reopened.status().code(), StatusCode::kNotFound);

    // Fresh domain for payload / poll fail-closed checks.
    const std::string name2 = UniqueName("fail2");
    auto created2 = SharedHostDomain::Create(name2, tiny);
    ASSERT_TRUE(created2.ok()) << created2.status().ToString();
    SharedHostDomain domain2 = std::move(*created2);
    ASSERT_TRUE(domain2.Join(NodeId{1}).ok());
    const auto schema = MakeSchema(3);
    auto pub = domain2.Advertise("fc", schema);
    ASSERT_TRUE(pub.ok()) << pub.status().ToString();
    auto sub = domain2.Subscribe("fc", schema);
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();

    auto empty = pub->Publish({});
    ASSERT_FALSE(empty.ok());
    EXPECT_EQ(empty.code(), StatusCode::kInvalidArgument);

    std::vector<std::byte> oversized(tiny.max_payload_bytes + 1);
    auto too_big = pub->Publish(oversized);
    ASSERT_FALSE(too_big.ok());
    EXPECT_EQ(too_big.code(), StatusCode::kResourceExhausted);

    auto try_empty = sub->TryPoll();
    ASSERT_FALSE(try_empty.ok());
    EXPECT_EQ(try_empty.status().code(), StatusCode::kWouldBlock);

    auto poll_timeout =
        sub->Poll(Deadline::FromNow(std::chrono::milliseconds(30)));
    ASSERT_FALSE(poll_timeout.ok());
    EXPECT_EQ(poll_timeout.status().code(), StatusCode::kTimeout);

    auto sub_timeout = domain2.Subscribe(
        "missing-topic", schema, Deadline::FromNow(std::chrono::milliseconds(30)));
    ASSERT_FALSE(sub_timeout.ok());
    EXPECT_EQ(sub_timeout.status().code(), StatusCode::kTimeout);

    (void)SharedHostDomain::Unlink(name2);
}

TEST_F(SharedHostDomainTest, TypedAdvertiseSubscribePublish) {
    SharedHostDomainOptions options;
    options.queue_depth = 8;
    options.max_payload_bytes = 64;
    auto created = SharedHostDomain::Create(name_, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SharedHostDomain domain = std::move(*created);
    ASSERT_TRUE(domain.Join(NodeId{1}).ok());

    auto pub = domain.Advertise<SharedHostTypedFrame>("typed");
    ASSERT_TRUE(pub.ok()) << pub.status().ToString();
    auto sub = domain.Subscribe<SharedHostTypedFrame>("typed");
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();

    SharedHostTypedFrame frame{.sequence = 7, .value = 42, .reserved = 0};
    ASSERT_TRUE(pub->Publish(frame).ok());
    auto received = sub->TryPoll<SharedHostTypedFrame>();
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    EXPECT_EQ(received->get()->sequence, 7u);
    EXPECT_EQ(received->get()->value, 42u);
    ASSERT_TRUE(std::move(*received).Release().ok());
}


TEST_F(SharedHostDomainTest, CentralSlabBorrowAndReclaim) {
    SharedHostDomainOptions options;
    options.peer_slots = 2;
    options.topic_slots = 2;
    options.queue_depth = 8;
    options.max_payload_bytes = 128;
    auto created = SharedHostDomain::Create(name_, options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    SharedHostDomain domain = std::move(*created);
    ASSERT_TRUE(domain.Join(NodeId{1}).ok());
    const auto schema = MakeSchema(77);
    auto pub = domain.Advertise("slab", schema);
    ASSERT_TRUE(pub.ok()) << pub.status().ToString();
    auto sub = domain.Subscribe("slab", schema);
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();

    // Fill beyond queue depth with release between polls so slab slots reclaim.
    for (uint32_t i = 0; i < 32; ++i) {
        char payload[64];
        const int n = std::snprintf(payload, sizeof(payload), "msg-%u", i);
        ASSERT_GT(n, 0);
        ASSERT_TRUE(pub->Publish(std::as_bytes(std::span(payload, n))).ok())
            << "publish " << i;
        auto borrowed = sub->PollBorrow(Deadline::FromNow(std::chrono::seconds(2)));
        ASSERT_TRUE(borrowed.ok()) << borrowed.status().ToString();
        ASSERT_EQ(borrowed->bytes().size(), static_cast<size_t>(n));
        EXPECT_EQ(std::memcmp(borrowed->bytes().data(), payload, n), 0);
        ASSERT_TRUE(std::move(*borrowed).Release().ok());
    }
    ASSERT_TRUE(domain.Recover().ok());
}

}  // namespace
}  // namespace mino::deployment

