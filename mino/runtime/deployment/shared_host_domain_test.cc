// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/deployment/shared_host_domain.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

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

}  // namespace
}  // namespace mino::deployment
