// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.gnu.org/licenses/lgpl-3.0.txt
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

// V-11: Region Recovery Ownership -- double-recovery and owner-kill tests.
//
// These tests place the RecoveryOwnerState control block in MAP_SHARED
// anonymous memory and fork() real processes, verifying the detailed design
// 6.5 protocol across process boundaries:
//   - two processes racing TryAcquire -> exactly one wins;
//   - the loser observes kAlreadyExists and can only wait or run read-only
//     diagnostics;
//   - after the owner is SIGKILLed and its lease expires, a new process
//     takes over and the epoch increments;
//   - release while alive lets a contender in without waiting for expiry.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <thread>

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mino/shm/recovery/scanner.h"

namespace mino::shm::recovery {
namespace {

// Shared control block for one test case. Beyond the recovery owner state it
// carries result slots so child processes can report what they observed.
struct SharedBlock {
    RecoveryOwnerState owner_state;
    std::atomic<int> acquire_results[2];  // Per-child TryAcquire outcome code.
    std::atomic<uint64_t> acquire_epoch[2];
    std::atomic<uint64_t> acquired_pid[2];
    std::atomic<int> barrier;
    std::atomic<uint64_t> takeover_epoch;
};

class DoubleOwnerTest : public ::testing::Test {
protected:
    void SetUp() override {
        void* mapped = mmap(nullptr, sizeof(SharedBlock),
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(mapped, MAP_FAILED) << "mmap failed: " << strerror(errno);
        block_ = new (mapped) SharedBlock();
        RecoveryOwner::Initialize(&block_->owner_state);
        for (int i = 0; i < 2; ++i) {
            block_->acquire_results[i].store(-1, std::memory_order_relaxed);
            block_->acquire_epoch[i].store(0, std::memory_order_relaxed);
            block_->acquired_pid[i].store(0, std::memory_order_relaxed);
        }
        block_->barrier.store(0, std::memory_order_relaxed);
        block_->takeover_epoch.store(0, std::memory_order_relaxed);
    }

    void TearDown() override {
        if (block_ != nullptr) {
            munmap(block_, sizeof(SharedBlock));
            block_ = nullptr;
        }
    }

    // Waits for a child and asserts a clean exit(0).
    void WaitChild(pid_t pid) {
        int status = 0;
        ASSERT_EQ(waitpid(pid, &status, 0), pid);
        EXPECT_TRUE(WIFEXITED(status)) << "child did not exit normally";
        if (WIFEXITED(status)) {
            EXPECT_EQ(WEXITSTATUS(status), 0) << "child reported failure";
        }
    }

    // Sleeps until `state`'s lease has definitely expired.
    static void WaitLeaseExpiry() {
        const uint64_t deadline = RecoveryOwner::NowNs() +
                                  RecoveryOwner::kLeaseDurationNs +
                                  100'000'000;  // +100 ms slack.
        while (RecoveryOwner::NowNs() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }

    SharedBlock* block_ = nullptr;
};

TEST_F(DoubleOwnerTest, SingleProcessAcquireRelease) {
    RecoveryOwner owner(&block_->owner_state, static_cast<uint64_t>(getpid()));
    ASSERT_TRUE(owner.TryAcquire().ok());
    EXPECT_TRUE(owner.IsOwner());
    EXPECT_EQ(owner.Epoch(), 1u);
    owner.Release();
    EXPECT_FALSE(owner.IsOwner());
    EXPECT_TRUE(owner.IsIdle());
}

TEST_F(DoubleOwnerTest, TwoProcessesRaceExactlyOneWins) {
    // Both children block on the barrier, then race TryAcquire exactly once.
    for (int i = 0; i < 2; ++i) {
        const pid_t pid = fork();
        ASSERT_NE(pid, -1) << "fork failed";
        if (pid == 0) {
            while (block_->barrier.load(std::memory_order_acquire) == 0) {
            }
            RecoveryOwner contender(&block_->owner_state,
                                    static_cast<uint64_t>(getpid()));
            Status st = contender.TryAcquire();
            block_->acquire_results[i].store(
                st.ok() ? 0 : static_cast<int>(st.code()),
                std::memory_order_release);
            if (st.ok()) {
                block_->acquire_epoch[i].store(contender.Epoch(),
                                               std::memory_order_release);
                block_->acquired_pid[i].store(
                    static_cast<uint64_t>(getpid()), std::memory_order_release);
            }
            _exit(0);
        }
    }

    block_->barrier.store(1, std::memory_order_release);

    // Reap whatever order they finish in: wait for both children.
    for (int i = 0; i < 2; ++i) {
        int status = 0;
        const pid_t done = waitpid(-1, &status, 0);
        ASSERT_GT(done, 0);
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 0);
    }

    const int r0 = block_->acquire_results[0].load(std::memory_order_acquire);
    const int r1 = block_->acquire_results[1].load(std::memory_order_acquire);

    // CAS guarantees exactly one winner; the loser sees kAlreadyExists.
    const int wins = (r0 == 0 ? 1 : 0) + (r1 == 0 ? 1 : 0);
    ASSERT_EQ(wins, 1) << "r0=" << r0 << " r1=" << r1;
    const int loser = (r0 == 0) ? 1 : 0;
    EXPECT_EQ(block_->acquire_results[loser].load(std::memory_order_acquire),
              static_cast<int>(StatusCode::kAlreadyExists));

    // The winner observed epoch 1 (first acquisition ever).
    const int winner = (r0 == 0) ? 0 : 1;
    EXPECT_EQ(block_->acquire_epoch[winner].load(std::memory_order_acquire),
              1u);
}

TEST_F(DoubleOwnerTest, LoserWaitsForIdleWhileOwnerHoldsLease) {
    RecoveryOwner owner(&block_->owner_state, static_cast<uint64_t>(getpid()));
    ASSERT_TRUE(owner.TryAcquire().ok());

    // A second scanner (contender) must fail acquisition and observe the
    // lease as busy; WaitForIdle with a short timeout must time out.
    RecoveryOwner contender(&block_->owner_state, /*pid=*/999999);
    Status st = contender.TryAcquire();
    ASSERT_EQ(st.code(), StatusCode::kAlreadyExists);
    EXPECT_FALSE(contender.IsIdle());
    EXPECT_EQ(contender.CurrentOwner(), static_cast<uint64_t>(getpid()));

    st = contender.WaitForIdle(/*timeout_ns=*/20'000'000);  // 20 ms.
    EXPECT_EQ(st.code(), StatusCode::kTimeout);

    // After the owner releases, the contender proceeds without waiting for
    // lease expiry (epoch increments on takeover).
    owner.Release();
    st = contender.WaitForIdle(/*timeout_ns=*/1'000'000'000);
    ASSERT_TRUE(st.ok()) << st.ToString();
    ASSERT_TRUE(contender.TryAcquire().ok());
    EXPECT_EQ(contender.Epoch(), 2u);
}

TEST_F(DoubleOwnerTest, OwnerKilledNewProcessTakesOverWithNewEpoch) {
    // Child 1 acquires and then simulates a crash: SIGKILL, never releases.
    const pid_t child = fork();
    ASSERT_NE(child, -1);
    if (child == 0) {
        RecoveryOwner owner(&block_->owner_state,
                            static_cast<uint64_t>(getpid()));
        if (!owner.TryAcquire().ok()) {
            _exit(1);
        }
        block_->acquire_epoch[0].store(owner.Epoch(),
                                       std::memory_order_release);
        _exit(0);  // Exit without Release(): process death = crash.
    }
    WaitChild(child);
    ASSERT_EQ(block_->acquire_epoch[0].load(std::memory_order_acquire), 1u);

    // Immediately after death the lease is still valid: takeover must fail.
    RecoveryOwner contender(&block_->owner_state,
                            static_cast<uint64_t>(getpid()));
    Status st = contender.TryAcquire();
    ASSERT_EQ(st.code(), StatusCode::kAlreadyExists)
        << "takeover before lease expiry must be refused";

    // After the lease expires the contender takes over; epoch increments.
    WaitLeaseExpiry();
    st = contender.TryAcquire();
    ASSERT_TRUE(st.ok()) << st.ToString();
    EXPECT_TRUE(contender.IsOwner());
    EXPECT_EQ(contender.Epoch(), 2u)
        << "new recovery owner must increment the epoch (6.5 step 5)";

    // Cleanup: release so the shared block is idle for TearDown.
    contender.Release();
}

TEST_F(DoubleOwnerTest, ConcurrentTakeoverAfterLeaseExpiryHasSingleWinner) {
    // Owner acquires and dies (exits without release).
    const pid_t child = fork();
    ASSERT_NE(child, -1);
    if (child == 0) {
        RecoveryOwner owner(&block_->owner_state,
                            static_cast<uint64_t>(getpid()));
        if (!owner.TryAcquire().ok()) {
            _exit(1);
        }
        _exit(0);
    }
    WaitChild(child);
    WaitLeaseExpiry();

    // Two contenders race the takeover; exactly one must win and the epoch
    // must be incremented exactly once (from 1 to 2).
    for (int i = 0; i < 2; ++i) {
        const pid_t pid = fork();
        ASSERT_NE(pid, -1);
        if (pid == 0) {
            while (block_->barrier.load(std::memory_order_acquire) == 0) {
            }
            RecoveryOwner contender(&block_->owner_state,
                                    static_cast<uint64_t>(getpid()));
            Status st = contender.TryAcquire();
            block_->acquire_results[i].store(
                st.ok() ? 0 : static_cast<int>(st.code()),
                std::memory_order_release);
            if (st.ok()) {
                block_->takeover_epoch.store(contender.Epoch(),
                                             std::memory_order_release);
            }
            _exit(0);
        }
    }
    block_->barrier.store(1, std::memory_order_release);

    for (int i = 0; i < 2; ++i) {
        int status = 0;
        const pid_t done = waitpid(-1, &status, 0);
        ASSERT_GT(done, 0);
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 0);
    }

    const int r0 = block_->acquire_results[0].load(std::memory_order_acquire);
    const int r1 = block_->acquire_results[1].load(std::memory_order_acquire);
    const int wins = (r0 == 0 ? 1 : 0) + (r1 == 0 ? 1 : 0);
    ASSERT_EQ(wins, 1) << "r0=" << r0 << " r1=" << r1;
    EXPECT_EQ(block_->takeover_epoch.load(std::memory_order_acquire), 2u)
        << "two racing takeovers must not double-increment the epoch";
}

TEST_F(DoubleOwnerTest, ReadOnlyDiagnosticsWhileOwnerActive) {
    // The loser of an ownership race must still be able to run the
    // non-destructive inspection path (detailed design 6.5 step 4).
    RecoveryOwner owner(&block_->owner_state, static_cast<uint64_t>(getpid()));
    ASSERT_TRUE(owner.TryAcquire().ok());

    RecoveryOwner contender(&block_->owner_state, /*pid=*/888888);
    ASSERT_EQ(contender.TryAcquire().code(), StatusCode::kAlreadyExists);

    // The contender can still observe ownership metadata read-only.
    EXPECT_EQ(contender.CurrentOwner(), static_cast<uint64_t>(getpid()));
    EXPECT_EQ(contender.Epoch(), 1u);
    EXPECT_GT(contender.LeaseDeadlineNs(), 0u);

    owner.Release();
}

}  // namespace
}  // namespace mino::shm::recovery
