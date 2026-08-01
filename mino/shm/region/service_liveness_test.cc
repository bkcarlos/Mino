// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/region.h"

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#if defined(__unix__) || defined(__APPLE__)
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "mino/platform/process_identity.h"
#include "mino/platform/shared_memory.h"
#include "mino/shm/region/recovery.h"
#include "mino/shm/region/superblock.h"

namespace mino {
namespace {

#if defined(__unix__) || defined(__APPLE__)

template <typename T>
bool WriteValue(int fd, const T& value) {
  const auto* data = reinterpret_cast<const char*>(&value);
  size_t written = 0;
  while (written != sizeof(T)) {
    const ssize_t rc = ::write(fd, data + written, sizeof(T) - written);
    if (rc <= 0) {
      return false;
    }
    written += static_cast<size_t>(rc);
  }
  return true;
}

template <typename T>
bool ReadValue(int fd, T* value) {
  auto* data = reinterpret_cast<char*>(value);
  size_t read = 0;
  while (read != sizeof(T)) {
    const ssize_t rc = ::read(fd, data + read, sizeof(T) - read);
    if (rc <= 0) {
      return false;
    }
    read += static_cast<size_t>(rc);
  }
  return true;
}

struct OwnerReady {
  uint32_t region_id;
  uint32_t reserved;
  uint64_t service_epoch;
  uint64_t recovery_epoch;
  ProcessIdentity identity;
};

pid_t SpawnLiveOwner(const std::string& name, int ready_fd) {
  const pid_t child = ::fork();
  if (child != 0) {
    return child;
  }

  RegionCreateOptions options;
  options.name = name;
  options.size_bytes = 1024 * 1024;
  auto region = SharedMemoryRegion::Create(options);
  if (!region.ok()) {
    ::_exit(10);
  }
  OwnerReady ready{
      .region_id = region->region_id(),
      .reserved = 0,
      .service_epoch = region->service_epoch(),
      .recovery_epoch = LoadRecoveryEpoch(*region->superblock()),
      .identity = ProcessIdentity::Current(),
  };
  if (!WriteValue(ready_fd, ready)) {
    ::_exit(11);
  }
  for (;;) {
    ::pause();
  }
}

class ServiceLivenessTest : public ::testing::Test {
 protected:
  std::string Name(const char* tag) {
    static uint32_t sequence = 0;
    return "/sl_" + std::to_string(::getpid()) + "_" +
           std::to_string(++sequence) + tag;
  }

  void TearDown() override {
    for (const std::string& name : names_) {
      (void)SharedMemorySegment::Unlink(name);
    }
  }

  std::string Track(const char* tag) {
    std::string name = Name(tag);
    names_.push_back(name);
    return name;
  }

  std::vector<std::string> names_;
};

TEST_F(ServiceLivenessTest, LiveOwnerRejectsRecoveryAndKilledOwnerIsRecovered) {
  const std::string name = Track("live");
  int ready_pipe[2];
  ASSERT_EQ(::pipe(ready_pipe), 0);
  const pid_t child = SpawnLiveOwner(name, ready_pipe[1]);
  ASSERT_GT(child, 0);
  ::close(ready_pipe[1]);

  OwnerReady ready{};
  ASSERT_TRUE(ReadValue(ready_pipe[0], &ready));
  ::close(ready_pipe[0]);

  RegionAttachOptions read_options;
  read_options.name = name;
  read_options.region_id = ready.region_id;
  read_options.read_only = true;
  auto reader = SharedMemoryRegion::Attach(read_options);
  ASSERT_TRUE(reader.ok()) << reader.status().ToString();
  EXPECT_EQ(LoadRegionState(*reader->superblock()), RegionState::kActive);
  EXPECT_EQ(LoadRecoveryEpoch(*reader->superblock()), ready.recovery_epoch);

  RegionAttachOptions write_options;
  write_options.name = name;
  write_options.region_id = ready.region_id;
  auto denied = SharedMemoryRegion::Attach(write_options);
  ASSERT_FALSE(denied.ok());
  EXPECT_EQ(denied.status().code(), StatusCode::kWouldBlock);
  EXPECT_EQ(LoadRegionState(*reader->superblock()), RegionState::kActive);
  EXPECT_EQ(LoadRecoveryEpoch(*reader->superblock()), ready.recovery_epoch);
  ASSERT_TRUE(reader->Detach().ok());

  ASSERT_EQ(::kill(child, SIGKILL), 0);
  int child_status = 0;
  ASSERT_EQ(::waitpid(child, &child_status, 0), child);
  ASSERT_TRUE(WIFSIGNALED(child_status));

  auto recovered = SharedMemoryRegion::Attach(write_options);
  ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
  EXPECT_TRUE(recovered->is_supervisor());
  EXPECT_TRUE(recovered->ValidateSupervisorFence().ok());
  EXPECT_EQ(LoadRegionState(*recovered->superblock()), RegionState::kActive);
  EXPECT_GT(recovered->service_epoch(), ready.service_epoch);
  EXPECT_GT(LoadRecoveryEpoch(*recovered->superblock()), ready.recovery_epoch);
  EXPECT_EQ(LoadServiceOwner(*recovered->superblock()),
            ProcessIdentity::Current());
  EXPECT_TRUE(recovered->Detach().ok());
}

TEST_F(ServiceLivenessTest, RecycledPidCannotKeepKilledOwnerAlive) {
  const std::string name = Track("pid");
  int ready_pipe[2];
  ASSERT_EQ(::pipe(ready_pipe), 0);
  const pid_t child = SpawnLiveOwner(name, ready_pipe[1]);
  ASSERT_GT(child, 0);
  ::close(ready_pipe[1]);

  OwnerReady ready{};
  ASSERT_TRUE(ReadValue(ready_pipe[0], &ready));
  ::close(ready_pipe[0]);
  ASSERT_EQ(::kill(child, SIGKILL), 0);
  int child_status = 0;
  ASSERT_EQ(::waitpid(child, &child_status, 0), child);

  // Simulate stale metadata after PID reuse: the numeric PID now names this
  // live process, but start_time/process_epoch still describe another
  // incarnation. Exact ProcessIdentity probing must classify it as dead.
  auto raw = SharedMemorySegment::Open(name, /*read_only=*/false);
  ASSERT_TRUE(raw.ok()) << raw.status().ToString();
  auto* sb = static_cast<SuperBlock*>(raw->base());
  ProcessIdentity recycled = ready.identity;
  recycled.node_id = ProcessIdentity::Current().node_id;
  recycled.process_id = ProcessIdentity::Current().process_id;
  recycled.start_time_ns = ProcessIdentity::Current().start_time_ns ^ 1u;
  recycled.process_epoch = ProcessIdentity::Current().process_epoch ^ 1u;
  ASSERT_EQ(ProbeProcessIdentity(recycled), ProcessIdentityLiveness::kDead);
  StoreServiceOwner(*sb, recycled);
  ASSERT_TRUE(raw->Close().ok());

  RegionAttachOptions options;
  options.name = name;
  options.region_id = ready.region_id;
  auto recovered = SharedMemoryRegion::Attach(options);
  ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
  EXPECT_GT(recovered->service_epoch(), ready.service_epoch);
  EXPECT_TRUE(recovered->ValidateSupervisorFence().ok());
  EXPECT_TRUE(recovered->Detach().ok());
}

TEST_F(ServiceLivenessTest, RecoveryLeaseTakeoverFencesOldProcessEpoch) {
  const std::string name = Track("fence");
  RegionCreateOptions create;
  create.name = name;
  create.size_bytes = 1024 * 1024;
  auto region = SharedMemoryRegion::Create(create);
  ASSERT_TRUE(region.ok()) << region.status().ToString();

  int ready_pipe[2];
  int resume_pipe[2];
  int result_pipe[2];
  ASSERT_EQ(::pipe(ready_pipe), 0);
  ASSERT_EQ(::pipe(resume_pipe), 0);
  ASSERT_EQ(::pipe(result_pipe), 0);

  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    ::close(ready_pipe[0]);
    ::close(resume_pipe[1]);
    ::close(result_pipe[0]);
    auto acquired = RecoveryOwner::TryAcquire(
        *region, ProcessIdentity::Current(), /*lease_duration_ms=*/20);
    if (!acquired.ok()) {
      ::_exit(20);
    }
    RecoveryOwner stale = std::move(*acquired);
    const uint64_t active_fence = LoadRecoveryFence(*region->superblock());
    if (!stale.ClaimRecoveryFence(active_fence).ok()) {
      ::_exit(21);
    }
    StoreState(*region->superblock(), RegionState::kRecovering);
    const uint64_t old_fence = LoadRecoveryFence(*region->superblock());
    if (!WriteValue(ready_pipe[1], old_fence)) {
      ::_exit(22);
    }
    uint8_t resume = 0;
    if (!ReadValue(resume_pipe[0], &resume)) {
      ::_exit(23);
    }
    const int code = static_cast<int>(
        stale.CommitRecoveryFence(RecoveryFencePhase::kActive).code());
    if (!WriteValue(result_pipe[1], code)) {
      ::_exit(24);
    }
    ::_exit(0);
  }

  ::close(ready_pipe[1]);
  ::close(resume_pipe[0]);
  ::close(result_pipe[1]);
  uint64_t old_fence = 0;
  ASSERT_TRUE(ReadValue(ready_pipe[0], &old_fence));
  ::close(ready_pipe[0]);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  auto acquired = RecoveryOwner::TryAcquire(
      *region, ProcessIdentity::Current(), /*lease_duration_ms=*/1000);
  ASSERT_TRUE(acquired.ok()) << acquired.status().ToString();
  RecoveryOwner replacement = std::move(*acquired);
  ASSERT_TRUE(replacement.ClaimRecoveryFence(old_fence).ok());
  EXPECT_NE(LoadRecoveryFence(*region->superblock()), old_fence);

  const uint8_t resume = 1;
  ASSERT_TRUE(WriteValue(resume_pipe[1], resume));
  ::close(resume_pipe[1]);
  int stale_code = 0;
  ASSERT_TRUE(ReadValue(result_pipe[0], &stale_code));
  ::close(result_pipe[0]);
  EXPECT_EQ(static_cast<StatusCode>(stale_code), StatusCode::kUnavailable);

  ASSERT_TRUE(
      replacement.CommitRecoveryFence(RecoveryFencePhase::kActive).ok());
  StoreRegionEpoch(*region->superblock(), replacement.epoch());
  StoreCleanShutdown(*region->superblock(), false);
  StoreState(*region->superblock(), RegionState::kActive);
  EXPECT_TRUE(replacement.Release().ok());

  int child_status = 0;
  ASSERT_EQ(::waitpid(child, &child_status, 0), child);
  ASSERT_TRUE(WIFEXITED(child_status));
  EXPECT_EQ(WEXITSTATUS(child_status), 0);
  EXPECT_TRUE(region->ValidateSupervisorFence().ok());
}

#else

TEST(ServiceLivenessTest, RequiresPosixProcesses) {
  GTEST_SKIP() << "requires POSIX shared memory, fork, and process locks";
}

#endif

}  // namespace
}  // namespace mino
