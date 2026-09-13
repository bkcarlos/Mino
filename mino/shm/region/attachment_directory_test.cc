// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/attachment_directory.h"

#include <cstdint>
#include <cstring>
#include <vector>

#include <gtest/gtest.h>

#include "mino/platform/process_identity.h"

namespace mino {
namespace {

class AttachmentDirectoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::memset(storage_, 0, sizeof(storage_));
    ASSERT_TRUE(
        InitializeAttachmentDirectory(storage_, sizeof(storage_)).ok());
  }

  ProcessIdentity FakeIdentity(uint64_t pid) const {
    ProcessIdentity id = ProcessIdentity::Current();
    id.process_id = pid;
    id.process_epoch ^= (pid << 8) | 0xA5u;
    id.start_time_ns ^= (pid << 16) | 0x5Au;
    return id;
  }

  alignas(64) std::byte storage_[kAttachmentDirectoryMinimumSize];
};

TEST_F(AttachmentDirectoryTest, ClaimReleaseAndHeartbeat) {
  const ProcessIdentity id = ProcessIdentity::Current();
  auto claimed = ClaimAttachmentSlot(storage_, sizeof(storage_),
                                     AttachmentRole::kSupervisor, id,
                                     /*attach_service_epoch=*/1);
  ASSERT_TRUE(claimed.ok()) << claimed.status().ToString();
  EXPECT_TRUE(HeartbeatAttachmentSlot(storage_, sizeof(storage_),
                                      claimed->slot_index, claimed->generation,
                                      id, /*heartbeat_ns=*/42)
                  .ok());
  EXPECT_TRUE(ReleaseAttachmentSlot(storage_, sizeof(storage_),
                                    claimed->slot_index, claimed->generation, id)
                  .ok());
}

TEST_F(AttachmentDirectoryTest, SubordinatePolicyCapsWriters) {
  const ProcessIdentity supervisor = ProcessIdentity::Current();
  ASSERT_TRUE(ClaimAttachmentSlot(storage_, sizeof(storage_),
                                  AttachmentRole::kSupervisor, supervisor,
                                  /*attach_service_epoch=*/1)
                  .ok());

  std::vector<AttachmentRegistration> regs;
  for (uint32_t i = 0; i < kMaxSubordinateWritableAttachments; ++i) {
    // Same process identity is fine for unit policy counting; production
    // attachments come from distinct processes.
    auto claimed = ClaimAttachmentSlot(
        storage_, sizeof(storage_), AttachmentRole::kSubordinateWritable,
        supervisor, /*attach_service_epoch=*/1);
    ASSERT_TRUE(claimed.ok()) << claimed.status().ToString();
    regs.push_back(*claimed);
  }
  auto denied = ClaimAttachmentSlot(
      storage_, sizeof(storage_), AttachmentRole::kSubordinateWritable,
      supervisor, /*attach_service_epoch=*/1);
  ASSERT_FALSE(denied.ok());
  EXPECT_EQ(denied.status().code(), StatusCode::kResourceExhausted);
  EXPECT_EQ(CountLiveSubordinateWritableSlots(storage_, sizeof(storage_)),
            kMaxSubordinateWritableAttachments);
}

TEST_F(AttachmentDirectoryTest, PrepareRecoveryReclaimsDeadIdentity) {
  ProcessIdentity dead = FakeIdentity(424242);
  ASSERT_EQ(ProbeProcessIdentity(dead), ProcessIdentityLiveness::kDead);

  auto claimed = ClaimAttachmentSlot(
      storage_, sizeof(storage_), AttachmentRole::kSubordinateWritable,
      dead, /*attach_service_epoch=*/3);
  ASSERT_TRUE(claimed.ok()) << claimed.status().ToString();
  EXPECT_EQ(CountLiveSubordinateWritableSlots(storage_, sizeof(storage_)),
            1u);

  ASSERT_TRUE(PrepareAttachmentDirectoryForSupervisorRecovery(storage_,
                                                              sizeof(storage_))
                  .ok());
  EXPECT_EQ(CountLiveSubordinateWritableSlots(storage_, sizeof(storage_)),
            0u);
}

TEST_F(AttachmentDirectoryTest, PrepareRecoveryRefusesLiveSubordinate) {
  const ProcessIdentity live = ProcessIdentity::Current();
  ASSERT_TRUE(ClaimAttachmentSlot(storage_, sizeof(storage_),
                                  AttachmentRole::kSubordinateWritable, live,
                                  /*attach_service_epoch=*/1)
                  .ok());
  auto prepared = PrepareAttachmentDirectoryForSupervisorRecovery(
      storage_, sizeof(storage_));
  ASSERT_FALSE(prepared.ok());
  EXPECT_EQ(prepared.code(), StatusCode::kWouldBlock);
}

TEST(AttachmentDirectoryAlignmentTest, RejectsMisalignedBase) {
  alignas(64) std::byte storage[kAttachmentDirectoryMinimumSize + 64];
  void* misaligned = storage + 32;
  EXPECT_EQ(InitializeAttachmentDirectory(misaligned,
                                          kAttachmentDirectoryMinimumSize)
                .code(),
            StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino
