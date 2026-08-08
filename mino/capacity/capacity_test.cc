// Copyright 2026 The Mino Authors

#include "mino/capacity/capacity.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mino::capacity {
namespace {

std::shared_ptr<CapacityController> Controller(NodeBudget budget) {
    auto created = CapacityController::Create(std::move(budget));
    EXPECT_TRUE(created.ok()) << created.status().ToString();
    return created.ok() ? std::move(*created) : nullptr;
}

TEST(CapacityTest, ReserveCommitLeaseReleaseAndSnapshot) {
    NodeBudget budget;
    budget.limit.shm_bytes = 100;
    budget.limit.topics = 4;
    auto controller = Controller(budget);

    ResourceVector resources;
    resources.shm_bytes = 40;
    resources.topics = 1;
    auto reserved = controller->Reserve({resources, ResourceScope::kTopic,
                                        AdmissionClass::kDataPlane, "alpha"});
    ASSERT_TRUE(reserved.ok()) << reserved.status().ToString();
    EXPECT_EQ(controller->Snapshot().pending, resources);

    auto lease = reserved->Commit();
    ASSERT_TRUE(lease.ok()) << lease.status().ToString();
    EXPECT_TRUE(lease->valid());
    EXPECT_EQ(controller->Snapshot().committed, resources);
    EXPECT_TRUE(controller->Snapshot().pending.empty());

    lease->Reset();
    EXPECT_TRUE(controller->Snapshot().committed.empty());
    EXPECT_EQ(controller->Snapshot().data_plane_headroom, budget.limit);
}

TEST(CapacityTest, DestructionAndExplicitRollbackNeverLeakPendingCharge) {
    NodeBudget budget;
    budget.limit.recorder_buffer_bytes = 64;
    auto controller = Controller(budget);
    ResourceVector charge;
    charge.recorder_buffer_bytes = 64;

    {
        auto first = controller->Reserve({charge, ResourceScope::kRecorder,
                                          AdmissionClass::kDataPlane, "first"});
        ASSERT_TRUE(first.ok());
        auto blocked = controller->Reserve({charge, ResourceScope::kRecorder,
                                            AdmissionClass::kDataPlane, "second"});
        EXPECT_FALSE(blocked.ok());
        EXPECT_EQ(blocked.status().code(), StatusCode::kResourceExhausted);
    }
    EXPECT_TRUE(controller->Snapshot().pending.empty());

    auto third = controller->Reserve({charge, ResourceScope::kRecorder,
                                      AdmissionClass::kDataPlane, "third"});
    ASSERT_TRUE(third.ok());
    EXPECT_TRUE(third->Rollback().ok());
    EXPECT_TRUE(controller->Snapshot().pending.empty());
}

TEST(CapacityTest, MultiModuleRequestRejectsAtomicallyWithTypedReason) {
    NodeBudget budget;
    budget.limit.shm_bytes = 1024;
    budget.limit.bridge_connections = 2;
    budget.limit.bridge_egress_bytes = 100;
    budget.limit.schema_buffer_bytes = 32;
    auto controller = Controller(budget);

    ResourceVector request;
    request.shm_bytes = 512;
    request.bridge_connections = 1;
    request.bridge_egress_bytes = 101;
    request.schema_buffer_bytes = 16;
    AdmissionRejection rejection;
    auto denied = controller->Reserve(
        {request, ResourceScope::kBridge, AdmissionClass::kDataPlane, "peer-7"},
        &rejection);
    ASSERT_FALSE(denied.ok());
    EXPECT_EQ(rejection.dimension, ResourceDimension::kBridgeEgressBytes);
    EXPECT_EQ(rejection.requested, 101u);
    EXPECT_EQ(rejection.available, 100u);
    EXPECT_TRUE(controller->Snapshot().pending.empty());
    EXPECT_TRUE(controller->Snapshot().committed.empty());

    auto json = AdmissionRejectionToJson(rejection);
    ASSERT_TRUE(json.ok());
    EXPECT_NE(json->find("\"dimension\":\"bridge_egress_bytes\""),
              std::string::npos);
}

TEST(CapacityTest, EmergencyReserveIsDataPlaneHeadroomOnly) {
    NodeBudget budget;
    budget.limit.threads = 10;
    budget.emergency_reserve.threads = 2;
    auto controller = Controller(budget);
    ResourceVector data;
    data.threads = 8;
    auto data_reservation = controller->Reserve(
        {data, ResourceScope::kOther, AdmissionClass::kDataPlane, "workers"});
    ASSERT_TRUE(data_reservation.ok());
    auto data_lease = data_reservation->Commit();
    ASSERT_TRUE(data_lease.ok());

    ResourceVector one;
    one.threads = 1;
    EXPECT_EQ(controller
                  ->Reserve({one, ResourceScope::kOther,
                             AdmissionClass::kDataPlane, "extra worker"})
                  .status()
                  .code(),
              StatusCode::kResourceExhausted);
    auto control = controller->Reserve(
        {one, ResourceScope::kOther, AdmissionClass::kControlPlane, "recovery"});
    ASSERT_TRUE(control.ok());
    auto control_lease = control->Commit();
    ASSERT_TRUE(control_lease.ok());

    const CapacitySnapshot snapshot = controller->Snapshot();
    EXPECT_EQ(snapshot.data_plane_headroom.threads, 0u);
    EXPECT_EQ(snapshot.control_plane_headroom.threads, 1u);
}

TEST(CapacityTest, ConcurrentReservationsCannotOversubscribe) {
    NodeBudget budget;
    budget.limit.topics = 17;
    auto controller = Controller(budget);
    constexpr size_t kContenders = 64;
    std::atomic<size_t> accepted{0};
    std::mutex leases_mutex;
    std::vector<CapacityLease> leases;
    leases.reserve(kContenders);
    std::vector<std::thread> threads;
    threads.reserve(kContenders);

    for (size_t index = 0; index < kContenders; ++index) {
        threads.emplace_back([&] {
            ResourceVector one;
            one.topics = 1;
            auto reserved = controller->Reserve(
                {one, ResourceScope::kTopic, AdmissionClass::kDataPlane,
                 "concurrent topic"});
            if (!reserved.ok()) return;
            auto lease = reserved->Commit();
            if (!lease.ok()) return;
            accepted.fetch_add(1, std::memory_order_relaxed);
            std::lock_guard lock(leases_mutex);
            leases.push_back(std::move(*lease));
        });
    }
    for (auto& thread : threads) thread.join();

    EXPECT_EQ(accepted.load(), 17u);
    const CapacitySnapshot full = controller->Snapshot();
    EXPECT_EQ(full.committed.topics, 17u);
    EXPECT_EQ(full.data_plane_headroom.topics, 0u);
    leases.clear();
    EXPECT_EQ(controller->Snapshot().committed.topics, 0u);
}

TEST(CapacityTest, SlabClassBoundaryAndBudgetValidationAreExact) {
    NodeBudget budget;
    budget.limit.slab_bytes[255] = std::numeric_limits<uint64_t>::max();
    budget.emergency_reserve.slab_bytes[255] = 1;
    auto controller = Controller(budget);
    ResourceVector maximum_data;
    maximum_data.slab_bytes[255] =
        std::numeric_limits<uint64_t>::max() - 1;
    auto reserved = controller->Reserve(
        {maximum_data, ResourceScope::kTopic, AdmissionClass::kDataPlane,
         "largest slab class"});
    ASSERT_TRUE(reserved.ok());
    auto lease = reserved->Commit();
    ASSERT_TRUE(lease.ok());
    EXPECT_EQ(controller->Snapshot().committed.slab_bytes[255],
              std::numeric_limits<uint64_t>::max() - 1);

    NodeBudget invalid;
    invalid.emergency_reserve.file_descriptors = 1;
    EXPECT_EQ(CapacityController::Create(invalid).status().code(),
              StatusCode::kInvalidArgument);
}

TEST(CapacityTest, CheckedArithmeticRejectsEveryOverflowWithoutWrapping) {
    ResourceVector maximum;
    maximum.shm_bytes = std::numeric_limits<uint64_t>::max();
    ResourceVector one;
    one.shm_bytes = 1;
    EXPECT_EQ(CheckedAdd(maximum, one).status().code(),
              StatusCode::kInvalidArgument);

    ResourceVector half;
    half.recorder_buffer_bytes =
        std::numeric_limits<uint64_t>::max() / 2 + 1;
    EXPECT_EQ(CheckedScale(half, 2).status().code(),
              StatusCode::kInvalidArgument);

    ResourceVector safe;
    safe.bridge_connections = 3;
    auto scaled = CheckedScale(safe, 7);
    ASSERT_TRUE(scaled.ok());
    EXPECT_EQ(scaled->bridge_connections, 21u);
}

TEST(CapacityTest, SnapshotJsonContainsBudgetUsageAndHeadroom) {
    NodeBudget budget;
    budget.limit.topics = 3;
    budget.emergency_reserve.topics = 1;
    auto controller = Controller(budget);
    ResourceVector one;
    one.topics = 1;
    auto reservation = controller->Reserve(
        {one, ResourceScope::kTopic, AdmissionClass::kDataPlane, "json"});
    ASSERT_TRUE(reservation.ok());
    auto lease = reservation->Commit();
    ASSERT_TRUE(lease.ok());

    auto json = CapacitySnapshotToJson(controller->Snapshot());
    ASSERT_TRUE(json.ok()) << json.status().ToString();
    EXPECT_NE(json->find("\"committed\""), std::string::npos);
    EXPECT_NE(json->find("\"data_plane_headroom\""), std::string::npos);
    EXPECT_NE(json->find("\"topics\":1"), std::string::npos);
    EXPECT_NE(json->find("\"rejected_reservations\":0"),
              std::string::npos);
}

}  // namespace
}  // namespace mino::capacity
