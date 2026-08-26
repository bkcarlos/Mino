// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/mp_stress_harness.h"
#include "mino/runtime/simple_node.h"

#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>

namespace mino {
namespace {

constexpr uint32_t kTopicSlots = 4;
constexpr uint32_t kHeaderAllowanceBytes = 2048;
constexpr uint64_t kShmBudgetBytes = 64ull << 20;

class SimpleNodeBusinessMpStressTest : public ::testing::Test {
protected:
    void TearDown() override {
        for (const auto& name : created_) {
            (void)SimpleNode::Unlink(name);
        }
    }

    std::string MakeName(const char* tag) {
        std::string name = mp_stress::UniqueShmName(tag);
        created_.push_back(name);
        return name;
    }

    void RunProfile(uint32_t payload_bytes, uint64_t messages,
                    uint32_t queue_depth, const char* tag) {
        SimpleNodeOptions options;
        options.topic_slots = kTopicSlots;
        options.queue_depth = queue_depth;
        options.max_payload_bytes = payload_bytes + kHeaderAllowanceBytes;
        auto required = SimpleNode::RequiredBytes(options);
        ASSERT_TRUE(required.ok()) << required.status().ToString();
        ASSERT_LT(*required, kShmBudgetBytes)
            << "profile " << tag << " needs " << *required
            << " bytes; must fit in a 64 MiB /dev/shm";

        const std::filesystem::path worker =
            mp_stress::FindWorker("examples/simple_mp_business_stress");
        ASSERT_TRUE(std::filesystem::exists(worker)) << worker;
        const std::filesystem::path descriptor = mp_stress::FindWorker(
            "benchmarks/pipeline_comparison/mino_generated/"
            "autonomy_pipeline.descriptor");
        ASSERT_TRUE(std::filesystem::exists(descriptor)) << descriptor;

        const std::string name = MakeName(tag);
        const std::string messages_s = std::to_string(messages);
        const std::string payload_s = std::to_string(payload_bytes);
        const std::string depth_s = std::to_string(queue_depth);
        const std::vector<std::string> extra = {
            "--messages", messages_s, "--payload-bytes", payload_s,
            "--queue-depth", depth_s, "--schema-descriptor",
            descriptor.string(),
        };
        std::vector<std::string> sub_args = {"sub", name};
        sub_args.insert(sub_args.end(), extra.begin(), extra.end());
        std::vector<std::string> pub_args = {"pub", name};
        pub_args.insert(pub_args.end(), extra.begin(), extra.end());

        mp_stress::Child sub;
        mp_stress::Child pub;
        ASSERT_EQ(mp_stress::SpawnWorker(
                      worker, sub_args,
                      mp_stress::TmpPath(std::string(tag) + ".sub.err"), &sub),
                  0)
            << "posix_spawn sub failed";
        ASSERT_EQ(mp_stress::SpawnWorker(
                      worker, pub_args,
                      mp_stress::TmpPath(std::string(tag) + ".pub.err"), &pub),
                  0)
            << "posix_spawn pub failed";

        int sub_exit = -1;
        int pub_exit = -1;
        const bool pub_ok =
            mp_stress::WaitChild(&pub, &pub_exit, mp_stress::kChildTimeout);
        const bool sub_ok =
            mp_stress::WaitChild(&sub, &sub_exit, mp_stress::kChildTimeout);
        const std::string json = mp_stress::ReadFd(sub.stdout_rd);
        const std::string sub_err = mp_stress::ReadFile(sub.err_path);
        const std::string pub_err = mp_stress::ReadFile(pub.err_path);
        if (sub.stdout_rd >= 0) {
            ::close(sub.stdout_rd);
            sub.stdout_rd = -1;
        }
        if (pub.stdout_rd >= 0) {
            ::close(pub.stdout_rd);
            pub.stdout_rd = -1;
        }

        std::cout << "transport=simplenode-shm codec=business profile=" << tag
                  << " payload_bytes=" << payload_bytes
                  << " messages=" << messages
                  << " queue_depth=" << queue_depth
                  << " required_bytes=" << *required << "\n"
                  << "json=" << json << "\n"
                  << "pub_err=" << pub_err << "\n"
                  << "sub_err=" << sub_err << "\n";

        ASSERT_TRUE(pub_ok) << "publisher did not exit cleanly, code " << pub_exit
                            << "\n"
                            << pub_err;
        ASSERT_TRUE(sub_ok) << "subscriber did not exit cleanly, code " << sub_exit
                            << "\n"
                            << sub_err;
        EXPECT_EQ(pub_exit, 0) << pub_err;
        EXPECT_EQ(sub_exit, 0) << sub_err << "\n" << json;

        std::string codec;
        uint64_t received = 0;
        uint64_t lost = 0;
        uint64_t expected = 0;
        uint64_t p50_ns = 0;
        uint64_t p95_ns = 0;
        double msgs_per_s = 0.0;
        ASSERT_TRUE(mp_stress::ExtractQuoted(json, "codec", &codec)) << json;
        ASSERT_TRUE(mp_stress::ExtractU64(json, "received", &received)) << json;
        ASSERT_TRUE(mp_stress::ExtractU64(json, "lost", &lost)) << json;
        ASSERT_TRUE(mp_stress::ExtractU64(json, "expected", &expected)) << json;
        ASSERT_TRUE(mp_stress::ExtractU64(json, "p50_ns", &p50_ns)) << json;
        ASSERT_TRUE(mp_stress::ExtractU64(json, "p95_ns", &p95_ns)) << json;
        ASSERT_TRUE(mp_stress::ExtractF64(json, "msgs_per_s", &msgs_per_s))
            << json;
        EXPECT_EQ(codec, "business");
        EXPECT_EQ(expected, messages);
        EXPECT_EQ(received, messages);
        EXPECT_EQ(lost, 0u);
        EXPECT_GT(msgs_per_s, 0.0);
        RecordProperty("payload_bytes", static_cast<int>(payload_bytes));
        RecordProperty("messages", static_cast<int>(messages));
        RecordProperty("p50_ns", std::to_string(p50_ns));
        RecordProperty("p95_ns", std::to_string(p95_ns));
        RecordProperty("msgs_per_s", std::to_string(msgs_per_s));
    }

    std::vector<std::string> created_;
};

TEST_F(SimpleNodeBusinessMpStressTest, SmallPayloadIndependentProcesses) {
    RunProfile(256, 20000, 32, "s256");
}

TEST_F(SimpleNodeBusinessMpStressTest, MediumPayloadIndependentProcesses) {
    RunProfile(64u * 1024u, 2000, 32, "m64k");
}

}  // namespace
}  // namespace mino
