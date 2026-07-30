// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/subscriber_lease.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>

namespace mino {
namespace {

struct AlignedDeleter {
    void operator()(std::byte* p) const {
        ::operator delete[](p, std::align_val_t(64));
    }
};
using AlignedBytes = std::unique_ptr<std::byte[], AlignedDeleter>;

AlignedBytes AllocateAligned(uint64_t bytes) {
    AlignedBytes memory(
        new (std::align_val_t(64)) std::byte[static_cast<size_t>(bytes)]);
    std::memset(memory.get(), 0, static_cast<size_t>(bytes));
    return memory;
}

ProcessIdentity Owner(uint64_t epoch) {
    return ProcessIdentity{
        .node_id = 1,
        .process_id = 100 + epoch,
        .process_epoch = epoch,
        .start_time_ns = 1000 + epoch,
    };
}

bool AlwaysAlive(const ProcessIdentity&, void*) noexcept { return true; }
bool AlwaysDead(const ProcessIdentity&, void*) noexcept { return false; }

void CountPinCleanup(const ProcessIdentity& owner, void* context) noexcept {
    auto* count = static_cast<uint64_t*>(context);
    *count += owner.process_epoch;
}

void Publish(BroadcastChannel& channel, uint32_t tag) {
    auto reservation = channel.Reserve();
    ASSERT_TRUE(reservation.ok()) << reservation.status().ToString();
    reservation->slot()->msg_type = tag;
    reservation->slot()->schema_version = 1;
    reservation->slot()->schema_short_id = tag;
    reservation->slot()->schema_layout_version = 1;
    reservation->slot()->payload_len = 0;
    ASSERT_TRUE(std::move(*reservation).Commit().ok());
}

class SubscriberLeaseTest : public ::testing::Test {
protected:
    static constexpr uint64_t kCapacity = 4;
    static constexpr uint64_t kT0 = 1'000'000;
    static constexpr uint64_t kLease = 10'000;

    AlignedBytes channel_memory_;
    AlignedBytes lease_memory_;
    std::optional<BroadcastChannel> channel_;
    std::optional<SubscriberLeaseTable> leases_;

    void SetUp() override {
        channel_memory_ =
            AllocateAligned(BroadcastChannel::RequiredSize(kCapacity));
        auto channel =
            BroadcastChannel::Init(channel_memory_.get(), kCapacity);
        ASSERT_TRUE(channel.ok()) << channel.status().ToString();
        channel_.emplace(*channel);

        lease_memory_ = AllocateAligned(SubscriberLeaseTable::RequiredSize());
        auto leases = SubscriberLeaseTable::Init(lease_memory_.get());
        ASSERT_TRUE(leases.ok()) << leases.status().ToString();
        leases_.emplace(*leases);
    }
};

TEST_F(SubscriberLeaseTest, AttachAndRegistrationKeepEpochIndependent) {
    auto attached = SubscriberLeaseTable::Attach(lease_memory_.get());
    ASSERT_TRUE(attached.ok()) << attached.status().ToString();

    SubscriberLeaseCoordinator coordinator(*channel_, *leases_, &AlwaysAlive);
    auto first = coordinator.Register(SubscriberId{2}, Owner(7), kT0);
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    EXPECT_EQ(first->subscriber.generation, 1u);
    EXPECT_EQ(first->lease_epoch, 1u);

    ASSERT_TRUE(coordinator.Unregister(*first).ok());
    EXPECT_EQ(leases_->State(2), SubscriberLeaseState::kEvicted);

    auto second = coordinator.Register(SubscriberId{2}, Owner(8), kT0 + 1);
    ASSERT_TRUE(second.ok());
    EXPECT_GT(second->subscriber.generation, first->subscriber.generation);
    EXPECT_GT(second->lease_epoch, first->lease_epoch);

    EXPECT_EQ(coordinator.Heartbeat(*first, kT0 + 2).code(),
              StatusCode::kNotFound);
    EXPECT_TRUE(coordinator.Heartbeat(*second, kT0 + 2).ok());
}

TEST_F(SubscriberLeaseTest, LiveOwnerIsNotEvictedAfterTimeout) {
    SubscriberLeaseCoordinator coordinator(*channel_, *leases_, &AlwaysAlive);
    auto lease = coordinator.Register(SubscriberId{0}, Owner(3), kT0);
    ASSERT_TRUE(lease.ok());

    EXPECT_EQ(coordinator.EvictExpired(kT0 + 100 * kLease, kLease), 0u);
    EXPECT_EQ(leases_->State(0), SubscriberLeaseState::kActive);
    EXPECT_TRUE(channel_->Heartbeat(lease->subscriber, kT0 + 100 * kLease).ok());
}

TEST_F(SubscriberLeaseTest, FreshHeartbeatPreventsEviction) {
    SubscriberLeaseCoordinator coordinator(*channel_, *leases_, &AlwaysDead);
    auto lease = coordinator.Register(SubscriberId{1}, Owner(4), kT0);
    ASSERT_TRUE(lease.ok());

    ASSERT_TRUE(coordinator.Heartbeat(*lease, kT0 + kLease).ok());
    EXPECT_EQ(coordinator.EvictExpired(kT0 + kLease, kLease), 0u);
    EXPECT_EQ(leases_->State(1), SubscriberLeaseState::kActive);
}

TEST_F(SubscriberLeaseTest, DeadExpiredOwnerIsEvictedAndUnblocksPublisher) {
    uint64_t cleaned_epoch_sum = 0;
    SubscriberLeaseCoordinator coordinator(
        *channel_, *leases_, &AlwaysDead, nullptr, &CountPinCleanup,
        &cleaned_epoch_sum);
    auto lease = coordinator.Register(SubscriberId{0}, Owner(5), kT0);
    ASSERT_TRUE(lease.ok());

    for (uint32_t i = 0; i < kCapacity; ++i) {
        Publish(*channel_, i + 1);
    }
    EXPECT_TRUE(channel_->IsFull());

    EXPECT_EQ(coordinator.EvictExpired(kT0 + kLease, kLease), 1u);
    EXPECT_EQ(leases_->State(0), SubscriberLeaseState::kEvicted);
    EXPECT_EQ(cleaned_epoch_sum, 5u);
    EXPECT_FALSE(channel_->IsFull());
    EXPECT_EQ(channel_->Poll(lease->subscriber).status().code(),
              StatusCode::kNotFound);

    auto replacement = coordinator.Register(SubscriberId{0}, Owner(6),
                                            kT0 + kLease);
    ASSERT_TRUE(replacement.ok());
    EXPECT_GT(replacement->subscriber.generation,
              lease->subscriber.generation);
    EXPECT_GT(replacement->lease_epoch, lease->lease_epoch);
}

TEST_F(SubscriberLeaseTest, NormalUnregisterRunsPinCleanup) {
    uint64_t cleaned_epoch_sum = 0;
    SubscriberLeaseCoordinator coordinator(
        *channel_, *leases_, &AlwaysAlive, nullptr, &CountPinCleanup,
        &cleaned_epoch_sum);
    auto lease = coordinator.Register(SubscriberId{3}, Owner(9), kT0);
    ASSERT_TRUE(lease.ok());

    EXPECT_TRUE(coordinator.Unregister(*lease).ok());
    EXPECT_EQ(cleaned_epoch_sum, 9u);
    EXPECT_EQ(leases_->State(3), SubscriberLeaseState::kEvicted);
}

}  // namespace
}  // namespace mino
