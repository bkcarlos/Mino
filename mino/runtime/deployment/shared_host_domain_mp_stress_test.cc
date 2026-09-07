// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/deployment/shared_host_domain.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "mino/runtime/mp_stress_harness.h"

namespace mino::deployment {
namespace {

using mp_stress::Child;
using mp_stress::ExtractU64;
using mp_stress::FindWorker;
using mp_stress::ReadFd;
using mp_stress::ReadFile;
using mp_stress::SpawnWorker;
using mp_stress::TmpPath;
using mp_stress::UniqueShmName;
using mp_stress::WaitChild;
using mp_stress::kChildTimeout;

TEST(SharedHostDomainMpStressTest, IndependentProcessesDiscoverAndExchange) {
    const std::string shm = UniqueShmName("shd");
    const auto worker = FindWorker("examples/shared_host_domain_stress");
    ASSERT_TRUE(std::filesystem::exists(worker)) << worker;

    Child publisher;
    Child subscriber;
    ASSERT_EQ(SpawnWorker(worker, {"sub", shm, "2", "16"},
                          TmpPath("shd_sub.err"), &subscriber),
              0);
    // Brief head-start so Open/Join can begin before Create under slow ASAN.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_EQ(SpawnWorker(worker, {"pub", shm, "1", "16"},
                          TmpPath("shd_pub.err"), &publisher),
              0);

    int pub_exit = -1;
    int sub_exit = -1;
    const bool pub_ok = WaitChild(&publisher, &pub_exit, kChildTimeout);
    const bool sub_ok = WaitChild(&subscriber, &sub_exit, kChildTimeout);
    const std::string pub_out = ReadFd(publisher.stdout_rd);
    const std::string sub_out = ReadFd(subscriber.stdout_rd);
    const std::string pub_err = ReadFile(publisher.err_path);
    const std::string sub_err = ReadFile(subscriber.err_path);
    if (publisher.stdout_rd >= 0) {
        ::close(publisher.stdout_rd);
        publisher.stdout_rd = -1;
    }
    if (subscriber.stdout_rd >= 0) {
        ::close(subscriber.stdout_rd);
        subscriber.stdout_rd = -1;
    }

    ASSERT_TRUE(pub_ok) << "publisher exit=" << pub_exit << "\n" << pub_err;
    ASSERT_TRUE(sub_ok) << "subscriber exit=" << sub_exit << "\n" << sub_err;
    ASSERT_EQ(pub_exit, 0) << pub_err << pub_out;
    ASSERT_EQ(sub_exit, 0) << sub_err << sub_out;

    uint64_t sent = 0;
    uint64_t received = 0;
    ASSERT_TRUE(ExtractU64(pub_out, "sent", &sent)) << pub_out;
    ASSERT_TRUE(ExtractU64(sub_out, "received", &received)) << sub_out;
    EXPECT_EQ(sent, 16u);
    EXPECT_EQ(received, 16u);

    (void)SharedHostDomain::Unlink(shm);
}

TEST(SharedHostDomainMpStressTest, LateJoinerSeesExistingPeer) {
    const std::string shm = UniqueShmName("shd_late");
    const auto worker = FindWorker("examples/shared_host_domain_stress");
    ASSERT_TRUE(std::filesystem::exists(worker)) << worker;

    Child publisher;
    ASSERT_EQ(SpawnWorker(worker, {"pub", shm, "1", "1"},
                          TmpPath("shd_late_pub.err"), &publisher),
              0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    Child late;
    ASSERT_EQ(SpawnWorker(worker, {"join-wait", shm, "9"},
                          TmpPath("shd_late_join.err"), &late),
              0);

    int late_exit = -1;
    int pub_exit = -1;
    const bool late_ok = WaitChild(&late, &late_exit, kChildTimeout);
    const bool pub_ok = WaitChild(&publisher, &pub_exit, kChildTimeout);
    const std::string late_out = ReadFd(late.stdout_rd);
    const std::string late_err = ReadFile(late.err_path);
    if (late.stdout_rd >= 0) {
        ::close(late.stdout_rd);
        late.stdout_rd = -1;
    }
    if (publisher.stdout_rd >= 0) {
        ::close(publisher.stdout_rd);
        publisher.stdout_rd = -1;
    }

    ASSERT_TRUE(late_ok) << late_err << late_out;
    ASSERT_TRUE(pub_ok);
    ASSERT_EQ(late_exit, 0) << late_err << late_out;
    uint64_t peers = 0;
    ASSERT_TRUE(ExtractU64(late_out, "peers", &peers)) << late_out;
    EXPECT_GE(peers, 2u);
    (void)SharedHostDomain::Unlink(shm);
}

}  // namespace
}  // namespace mino::deployment
