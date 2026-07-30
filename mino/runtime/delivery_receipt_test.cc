// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/delivery_receipt.h"

#include <gtest/gtest.h>

#include <chrono>
#include <vector>

namespace mino {
namespace {

PublisherReceiptIdentity Publisher(uint64_t id = 1) {
    return PublisherReceiptIdentity{
        .process = ProcessIdentity{
            .node_id = 10,
            .process_id = 20,
            .process_epoch = 30,
            .start_time_ns = 40,
        },
        .publisher_id = PublisherId{id},
    };
}

DeliveryRequirement Requirement(DeliveryStage stage,
                                CompletionPolicy completion,
                                uint32_t quorum = 0) {
    return DeliveryRequirement{
        .stage = stage,
        .completion = completion,
        .quorum = quorum,
        .deadline = Deadline::Infinite(),
    };
}

TEST(DeliveryReceiptTest, LocalPublishedCompletesImmediately) {
    OutstandingReceiptTable table;
    const std::vector<DeliveryTarget> targets = {
        {DeliveryTargetKind::kNode, 1},
        {DeliveryTargetKind::kRecorder, 2},
    };
    auto receipt = table.Create(
        Publisher(), 7, targets,
        Requirement(DeliveryStage::kLocalPublished, CompletionPolicy::kAll));
    ASSERT_TRUE(receipt.ok()) << receipt.status().ToString();
    EXPECT_EQ(table.outstanding(), 0u);

    auto statuses = receipt->Wait(Deadline::Infinite());
    ASSERT_TRUE(statuses.ok());
    ASSERT_EQ(statuses->size(), 2u);
    EXPECT_TRUE((*statuses)[0].status.ok());
    EXPECT_TRUE((*statuses)[1].status.ok());
}

TEST(DeliveryReceiptTest, AllWaitsForFrozenTargetSnapshot) {
    OutstandingReceiptTable table;
    std::vector<DeliveryTarget> targets = {
        {DeliveryTargetKind::kNode, 1},
        {DeliveryTargetKind::kNode, 2},
    };
    auto receipt = table.Create(
        Publisher(), 8, targets,
        Requirement(DeliveryStage::kRemoteAccepted, CompletionPolicy::kAll));
    ASSERT_TRUE(receipt.ok());
    targets[0].id = 999;

    ASSERT_TRUE(table.Acknowledge(
        receipt->id(), {DeliveryTargetKind::kNode, 1},
        DeliveryStage::kRemoteAccepted).ok());
    auto pending = receipt->Wait(
        Deadline::FromNow(std::chrono::milliseconds(1)));
    ASSERT_FALSE(pending.ok());
    EXPECT_EQ(pending.status().code(), StatusCode::kTimeout);

    ASSERT_TRUE(table.Acknowledge(
        receipt->id(), {DeliveryTargetKind::kNode, 2},
        DeliveryStage::kRemoteAccepted).ok());
    auto complete = receipt->Wait(Deadline::Infinite());
    ASSERT_TRUE(complete.ok());
    EXPECT_EQ((*complete)[0].target.id, 1u);
    EXPECT_EQ(table.outstanding(), 0u);
}

TEST(DeliveryReceiptTest, AnyAndQuorumUseOnlySuccessfulTargets) {
    const std::vector<DeliveryTarget> targets = {
        {DeliveryTargetKind::kRecorder, 1},
        {DeliveryTargetKind::kRecorder, 2},
        {DeliveryTargetKind::kRecorder, 3},
    };

    OutstandingReceiptTable any_table;
    auto any = any_table.Create(
        Publisher(), 1, targets,
        Requirement(DeliveryStage::kStorageWritten, CompletionPolicy::kAny));
    ASSERT_TRUE(any.ok());
    ASSERT_TRUE(any_table.Acknowledge(
        any->id(), targets[0], DeliveryStage::kRecorderBuffered,
        Status::Error(StatusCode::kUnavailable, "recorder failed")).ok());
    EXPECT_EQ(any_table.outstanding(), 1u);
    ASSERT_TRUE(any_table.Acknowledge(
        any->id(), targets[1], DeliveryStage::kStorageDurable).ok());
    EXPECT_TRUE(any->Wait(Deadline::Infinite()).ok());

    OutstandingReceiptTable quorum_table;
    auto quorum = quorum_table.Create(
        Publisher(), 2, targets,
        Requirement(DeliveryStage::kStorageWritten,
                    CompletionPolicy::kQuorum, 2));
    ASSERT_TRUE(quorum.ok());
    ASSERT_TRUE(quorum_table.Acknowledge(
        quorum->id(), targets[0], DeliveryStage::kStorageWritten).ok());
    ASSERT_TRUE(quorum_table.Acknowledge(
        quorum->id(), targets[1], DeliveryStage::kStorageDurable).ok());
    EXPECT_TRUE(quorum->Wait(Deadline::Infinite()).ok());
}

TEST(DeliveryReceiptTest, RemoteAndStorageBranchesCannotImpersonateEachOther) {
    OutstandingReceiptTable table;
    const DeliveryTarget recorder{DeliveryTargetKind::kRecorder, 7};
    const std::vector<DeliveryTarget> targets = {recorder};
    auto receipt = table.Create(
        Publisher(), 3, targets,
        Requirement(DeliveryStage::kStorageDurable, CompletionPolicy::kAll));
    ASSERT_TRUE(receipt.ok());

    const Status wrong = table.Acknowledge(
        receipt->id(), recorder, DeliveryStage::kRemoteAccepted);
    ASSERT_FALSE(wrong.ok());
    EXPECT_EQ(wrong.code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(table.outstanding(), 1u);
}

TEST(DeliveryReceiptTest, LimitsRejectWithoutGrowingState) {
    OutstandingReceiptTable table({.max_outstanding = 2,
                                   .max_per_publisher = 1});
    const std::vector<DeliveryTarget> targets = {
        {DeliveryTargetKind::kNode, 1},
    };
    const auto requirement = Requirement(DeliveryStage::kRemoteAccepted,
                                         CompletionPolicy::kAll);
    ASSERT_TRUE(table.Create(Publisher(1), 1, targets, requirement).ok());
    auto same_publisher = table.Create(Publisher(1), 2, targets, requirement);
    ASSERT_FALSE(same_publisher.ok());
    EXPECT_EQ(same_publisher.status().code(), StatusCode::kResourceExhausted);
    ASSERT_TRUE(table.Create(Publisher(2), 1, targets, requirement).ok());
    auto table_full = table.Create(Publisher(3), 1, targets, requirement);
    ASSERT_FALSE(table_full.ok());
    EXPECT_EQ(table_full.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(table.outstanding(), 2u);
}

TEST(DeliveryReceiptTest, CancelAndPublisherCleanupOnlyStopWaiting) {
    OutstandingReceiptTable table;
    const std::vector<DeliveryTarget> targets = {
        {DeliveryTargetKind::kNode, 1},
    };
    const auto requirement = Requirement(DeliveryStage::kRemoteAccepted,
                                         CompletionPolicy::kAll);

    auto canceled = table.Create(Publisher(1), 1, targets, requirement);
    ASSERT_TRUE(canceled.ok());
    canceled->CancelWait();
    EXPECT_EQ(canceled->Wait(Deadline::Infinite()).status().code(),
              StatusCode::kUnavailable);
    EXPECT_EQ(table.outstanding(), 1u);

    auto orphaned = table.Create(Publisher(2), 1, targets, requirement);
    ASSERT_TRUE(orphaned.ok());
    EXPECT_EQ(table.CleanupPublisher(Publisher(2)), 1u);
    EXPECT_EQ(orphaned->Wait(Deadline::Infinite()).status().code(),
              StatusCode::kUnavailable);
}

TEST(DeliveryReceiptTest, InvalidQuorumIsRejected) {
    OutstandingReceiptTable table;
    const std::vector<DeliveryTarget> targets = {
        {DeliveryTargetKind::kNode, 1},
        {DeliveryTargetKind::kNode, 2},
    };
    auto zero = table.Create(
        Publisher(), 1, targets,
        Requirement(DeliveryStage::kRemoteAccepted,
                    CompletionPolicy::kQuorum, 0));
    EXPECT_EQ(zero.status().code(), StatusCode::kInvalidArgument);
    auto too_many = table.Create(
        Publisher(), 1, targets,
        Requirement(DeliveryStage::kRemoteAccepted,
                    CompletionPolicy::kQuorum, 3));
    EXPECT_EQ(too_many.status().code(), StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino
