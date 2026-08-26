// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/mp_stress_harness.h"

#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>

namespace mino {
namespace {

class ZmqIpcBusinessMpStressTest : public ::testing::Test {
protected:
    void RunProfile(uint32_t payload_bytes, uint64_t messages,
                    uint32_t queue_depth, const char* tag,
                    uint32_t hwm = 0) {
        if (hwm == 0) hwm = queue_depth;
        const std::filesystem::path worker =
            mp_stress::FindWorker("examples/zmq_ipc_business_stress");
        ASSERT_TRUE(std::filesystem::exists(worker)) << worker;

        const std::filesystem::path path = mp_stress::UniqueIpcPath(tag);
        const std::string messages_s = std::to_string(messages);
        const std::string payload_s = std::to_string(payload_bytes);
        const std::string depth_s = std::to_string(queue_depth);
        const std::string hwm_s = std::to_string(hwm);
        const std::vector<std::string> sub_args = {
            "sub", path.string(), "--messages", messages_s, "--payload-bytes",
            payload_s, "--queue-depth", depth_s, "--hwm", hwm_s,
        };
        const std::vector<std::string> pub_args = {
            "pub", path.string(), "--messages", messages_s, "--payload-bytes",
            payload_s, "--queue-depth", depth_s, "--hwm", hwm_s,
        };

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

        std::cout << "transport=zmq-ipc socket=pubsub codec=business profile=" << tag
                  << " payload_bytes=" << payload_bytes
                  << " messages=" << messages
                  << " queue_depth=" << queue_depth
                  << " hwm=" << hwm << "\n"
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
        std::string socket;
        uint64_t received = 0;
        uint64_t lost = 0;
        uint64_t expected = 0;
        uint64_t p50_ns = 0;
        uint64_t p95_ns = 0;
        double msgs_per_s = 0.0;
        ASSERT_TRUE(mp_stress::ExtractQuoted(json, "codec", &codec)) << json;
        ASSERT_TRUE(mp_stress::ExtractQuoted(json, "socket", &socket)) << json;
        ASSERT_TRUE(mp_stress::ExtractU64(json, "received", &received)) << json;
        ASSERT_TRUE(mp_stress::ExtractU64(json, "lost", &lost)) << json;
        ASSERT_TRUE(mp_stress::ExtractU64(json, "expected", &expected)) << json;
        ASSERT_TRUE(mp_stress::ExtractU64(json, "p50_ns", &p50_ns)) << json;
        ASSERT_TRUE(mp_stress::ExtractU64(json, "p95_ns", &p95_ns)) << json;
        ASSERT_TRUE(mp_stress::ExtractF64(json, "msgs_per_s", &msgs_per_s))
            << json;
        uint64_t json_hwm = 0;
        ASSERT_TRUE(mp_stress::ExtractU64(json, "hwm", &json_hwm)) << json;
        EXPECT_EQ(json_hwm, hwm);
        EXPECT_EQ(codec, "business");
        EXPECT_EQ(socket, "pubsub");
        EXPECT_EQ(expected, messages);
        EXPECT_EQ(received + lost, messages);
        EXPECT_GT(received, 0u);
        EXPECT_GT(msgs_per_s, 0.0);
        uint64_t first_seq = 0;
        if (mp_stress::ExtractU64(json, "first_seq", &first_seq)) {
            RecordProperty("first_seq", std::to_string(first_seq));
            EXPECT_EQ(first_seq, 0u)
                << "slow-joiner dropped the start of the stream";
        }
        if (lost != 0) {
            std::cout << "NOTE: ZMQ PUB mute-dropped lost=" << lost
                      << " received=" << received
                      << " first_seq=" << first_seq
                      << " (HWM=" << hwm
                      << " queue_depth=" << queue_depth << ")\n";
        }
        RecordProperty("payload_bytes", static_cast<int>(payload_bytes));
        RecordProperty("messages", static_cast<int>(messages));
        RecordProperty("hwm", static_cast<int>(hwm));
        RecordProperty("lost", std::to_string(lost));
        RecordProperty("p50_ns", std::to_string(p50_ns));
        RecordProperty("p95_ns", std::to_string(p95_ns));
        RecordProperty("msgs_per_s", std::to_string(msgs_per_s));
    }
};

TEST_F(ZmqIpcBusinessMpStressTest, SmallPayloadIndependentProcesses) {
    RunProfile(256, 20000, 32, "s256");
}

TEST_F(ZmqIpcBusinessMpStressTest, MediumPayloadIndependentProcesses) {
    RunProfile(64u * 1024u, 2000, 32, "m64k");
}

TEST_F(ZmqIpcBusinessMpStressTest, SmallPayloadHwm1024) {
    RunProfile(256, 20000, 32, "s256h1k", 1024);
}

TEST_F(ZmqIpcBusinessMpStressTest, MediumPayloadHwm1024) {
    RunProfile(64u * 1024u, 2000, 32, "m64kh1k", 1024);
}

TEST_F(ZmqIpcBusinessMpStressTest, SmallPayloadHwmMessages) {
    RunProfile(256, 20000, 32, "s256hn", 20000);
}

TEST_F(ZmqIpcBusinessMpStressTest, MediumPayloadHwmMessages) {
    RunProfile(64u * 1024u, 2000, 32, "m64khn", 2000);
}

}  // namespace
}  // namespace mino
