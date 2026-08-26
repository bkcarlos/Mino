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
#include <span>
#include <string>
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
