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

// V-26: cross-process MPMC skeleton tests (design doc 9.9).
//
// mpmc_ring_test.cc exercises the ring with std::thread inside one process;
// these tests place the ring (control block + slots) in MAP_SHARED anonymous
// memory and fork() real processes, verifying across process boundaries:
//   - conservation: 2 producer + 2 consumer processes move 2 x 5000 uniquely
//     tagged messages through a 64-slot ring (~156 wrap cycles) with no
//     message lost and no message delivered twice;
//   - full: with no consumer running, a producer that has filled the ring
//     observes kResourceExhausted on the next TryEnqueue, and a new producer
//     can enqueue again after another process drains a slot;
//   - empty: a consumer on an empty ring observes kWouldBlock.
//
// Children never use gtest (its state is not fork-safe); they report through
// _exit() codes and shared atomics. All assertions run in the parent.

#include "mino/shm/channel/mpmc_ring.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <thread>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino {
namespace {

constexpr uint64_t kCapacity = 64;
constexpr size_t kRingBytes = static_cast<size_t>(
    MpmcRing<uint64_t>::RequiredSize(kCapacity, sizeof(uint64_t),
                                     alignof(uint64_t)));

// Conservation test topology.
constexpr int kProducers = 2;
constexpr int kConsumers = 2;
constexpr uint64_t kPerProducer = 5000;
constexpr uint64_t kTotal = kPerProducer * kProducers;

// Child exit codes (children report failures through _exit(), not gtest).
constexpr int kChildOk = 0;
constexpr int kChildApiError = 1;   // Unexpected Result/Status failure.
constexpr int kChildWrongCode = 2;  // Wrong StatusCode on a probe.
constexpr int kChildBadToken = 3;   // Message payload out of range.

// Shared state for one test case: the ring storage plus the cross-process
// bookkeeping. No raw pointers: only atomics and plain byte storage, so the
// layout is independent of where each process maps it.
struct SharedBlock {
    // Ring storage: control block + slots. The ring requires a 64-byte
    // aligned region base; the mmap base is page aligned and this member
    // sits at offset 0 with 64-byte alignment.
    alignas(kMpmcRingCacheLineSize) unsigned char ring_storage[kRingBytes];

    // Start barrier: children wait until the parent releases them together.
    std::atomic<int> barrier;
    // Number of messages consumed so far (conservation test).
    std::atomic<uint64_t> consumed;
    // claimed[p][i]: how many times message i of producer p was delivered.
    std::atomic<uint32_t> claimed[kProducers][kPerProducer];
    // Probe outcomes reported by children: a StatusCode value, an
    // IsFull/IsEmpty snapshot (0/1) and a dequeued payload.
    std::atomic<int> probe_code;
    std::atomic<int> probe_snapshot;
    std::atomic<uint64_t> probe_value;
};

// Producer body for the conservation test. Attaches to the shared ring and
// publishes kPerProducer uniquely tagged messages, retrying while the ring
// is full. Never returns: reports through _exit().
[[noreturn]] void ProducerChildMain(SharedBlock* shared, int id) {
    auto ring = MpmcRing<uint64_t>::Attach(shared->ring_storage);
    if (!ring.ok()) {
        _exit(kChildApiError);
    }
    while (shared->barrier.load(std::memory_order_acquire) == 0) {
    }
    for (uint64_t i = 0; i < kPerProducer; ++i) {
        const uint64_t token = (static_cast<uint64_t>(id) << 32) | i;
        for (;;) {
            auto seq = ring->TryEnqueue();
            if (seq.ok()) {
                if (!ring->CommitEnqueue(*seq, token).ok()) {
                    _exit(kChildApiError);
                }
                break;
            }
            if (seq.status().code() != StatusCode::kResourceExhausted) {
                _exit(kChildWrongCode);
            }
            std::this_thread::yield();
        }
    }
    _exit(kChildOk);
}

// Consumer body for the conservation test. Drains messages until `consumed`
// reaches kTotal and records every delivery in the shared claim matrix.
// Never returns: reports through _exit().
[[noreturn]] void ConsumerChildMain(SharedBlock* shared) {
    auto ring = MpmcRing<uint64_t>::Attach(shared->ring_storage);
    if (!ring.ok()) {
        _exit(kChildApiError);
    }
    while (shared->barrier.load(std::memory_order_acquire) == 0) {
    }
    for (;;) {
        if (shared->consumed.load(std::memory_order_acquire) >= kTotal) {
            _exit(kChildOk);
        }
        auto got = ring->TryDequeue();
        if (!got.ok()) {
            if (got.status().code() != StatusCode::kWouldBlock) {
                _exit(kChildWrongCode);
            }
            std::this_thread::yield();
            continue;
        }
        auto value = ring->ReadSlot(*got);
        if (!value.ok()) {
            _exit(kChildApiError);
        }
        if (!ring->CommitDequeue(*got).ok()) {
            _exit(kChildApiError);
        }
        const uint64_t producer_id = *value >> 32;
        const uint64_t index = *value & 0xFFFFFFFFULL;
        if (producer_id >= static_cast<uint64_t>(kProducers) ||
            index >= kPerProducer) {
            _exit(kChildBadToken);
        }
        shared->claimed[producer_id][index].fetch_add(
            1, std::memory_order_acq_rel);
        shared->consumed.fetch_add(1, std::memory_order_acq_rel);
    }
}

class MpmcRingXprocTest : public ::testing::Test {
protected:
    void SetUp() override {
        void* mapped = mmap(nullptr, sizeof(SharedBlock),
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(mapped, MAP_FAILED) << "mmap failed: " << strerror(errno);
        shared_ = new (mapped) SharedBlock();
        shared_->barrier.store(0, std::memory_order_relaxed);
        shared_->consumed.store(0, std::memory_order_relaxed);
        for (int p = 0; p < kProducers; ++p) {
            for (uint64_t i = 0; i < kPerProducer; ++i) {
                shared_->claimed[p][i].store(0, std::memory_order_relaxed);
            }
        }
        shared_->probe_code.store(-1, std::memory_order_relaxed);
        shared_->probe_snapshot.store(-1, std::memory_order_relaxed);
        shared_->probe_value.store(0, std::memory_order_relaxed);

        auto ring = MpmcRing<uint64_t>::Init(shared_->ring_storage, kCapacity,
                                             sizeof(uint64_t),
                                             alignof(uint64_t));
        ASSERT_TRUE(ring.ok()) << ring.status().ToString();
        ring_ = *ring;
    }

    void TearDown() override {
        if (shared_ != nullptr) {
            munmap(shared_, sizeof(SharedBlock));
            shared_ = nullptr;
        }
    }

    // Waits for a child and asserts a clean exit(kChildOk).
    void WaitChild(pid_t pid) {
        int status = 0;
        ASSERT_EQ(waitpid(pid, &status, 0), pid);
        ASSERT_TRUE(WIFEXITED(status)) << "child did not exit normally";
        EXPECT_EQ(WEXITSTATUS(status), 0) << "child reported failure";
    }

    SharedBlock* shared_ = nullptr;
    MpmcRing<uint64_t> ring_;
};

// 2 producer + 2 consumer processes exchange kTotal tagged messages through
// a 64-slot ring. kTotal / kCapacity = ~156 wrap cycles per slot, so the
// wraparound path is exercised heavily. Afterwards every (producer, seq)
// pair must have been claimed exactly once: no loss, no duplication.
TEST_F(MpmcRingXprocTest, ConservationAcrossProcessesNoLossNoDuplication) {
    pid_t children[kProducers + kConsumers];
    for (int p = 0; p < kProducers; ++p) {
        const pid_t pid = fork();
        ASSERT_NE(pid, -1) << "fork failed";
        if (pid == 0) {
            ProducerChildMain(shared_, p);
        }
        children[p] = pid;
    }
    for (int c = 0; c < kConsumers; ++c) {
        const pid_t pid = fork();
        ASSERT_NE(pid, -1) << "fork failed";
        if (pid == 0) {
            ConsumerChildMain(shared_);
        }
        children[kProducers + c] = pid;
    }
    shared_->barrier.store(1, std::memory_order_release);

    for (int i = 0; i < kProducers + kConsumers; ++i) {
        WaitChild(children[i]);
    }

    EXPECT_EQ(shared_->consumed.load(std::memory_order_acquire), kTotal);
    for (int p = 0; p < kProducers; ++p) {
        for (uint64_t i = 0; i < kPerProducer; ++i) {
            EXPECT_EQ(
                shared_->claimed[p][i].load(std::memory_order_acquire), 1u)
                << "producer " << p << " message " << i
                << " lost or duplicated";
        }
    }
    EXPECT_TRUE(ring_.IsEmpty());
}

// With no consumer running, a producer process fills the ring and the next
// TryEnqueue reports kResourceExhausted. After a consumer process drains a
// slot, a new producer process can enqueue again: the full/roomy state
// propagates across process boundaries.
TEST_F(MpmcRingXprocTest, FullRefusesEnqueueAcrossProcessesThenDrains) {
    // Phase 1: a producer process fills the ring; the 65th TryEnqueue must
    // report "full".
    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        auto ring = MpmcRing<uint64_t>::Attach(shared_->ring_storage);
        if (!ring.ok()) {
            _exit(kChildApiError);
        }
        for (uint64_t i = 0; i < kCapacity; ++i) {
            auto seq = ring->TryEnqueue();
            if (!seq.ok()) {
                _exit(kChildApiError);
            }
            if (!ring->CommitEnqueue(*seq, 1000 + i).ok()) {
                _exit(kChildApiError);
            }
        }
        auto blocked = ring->TryEnqueue();
        if (blocked.ok()) {
            _exit(kChildWrongCode);  // A full ring accepted an enqueue.
        }
        shared_->probe_code.store(static_cast<int>(blocked.status().code()),
                                  std::memory_order_release);
        shared_->probe_snapshot.store(ring->IsFull() ? 1 : 0,
                                      std::memory_order_release);
        _exit(kChildOk);
    }
    WaitChild(pid);
    EXPECT_EQ(shared_->probe_code.load(std::memory_order_acquire),
              static_cast<int>(StatusCode::kResourceExhausted));
    EXPECT_EQ(shared_->probe_snapshot.load(std::memory_order_acquire), 1);
    EXPECT_TRUE(ring_.IsFull());

    // Phase 2: a consumer process drains exactly one slot. The first
    // committed message must come back first (FIFO over positions).
    pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        auto ring = MpmcRing<uint64_t>::Attach(shared_->ring_storage);
        if (!ring.ok()) {
            _exit(kChildApiError);
        }
        auto got = ring->TryDequeue();
        if (!got.ok()) {
            _exit(kChildApiError);
        }
        auto value = ring->ReadSlot(*got);
        if (!value.ok()) {
            _exit(kChildApiError);
        }
        shared_->probe_value.store(*value, std::memory_order_release);
        if (!ring->CommitDequeue(*got).ok()) {
            _exit(kChildApiError);
        }
        _exit(kChildOk);
    }
    WaitChild(pid);
    EXPECT_EQ(shared_->probe_value.load(std::memory_order_acquire), 1000u);
    EXPECT_FALSE(ring_.IsFull());

    // Phase 3: with a slot freed by another process, a new producer process
    // can enqueue again.
    pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        auto ring = MpmcRing<uint64_t>::Attach(shared_->ring_storage);
        if (!ring.ok()) {
            _exit(kChildApiError);
        }
        auto seq = ring->TryEnqueue();
        if (!seq.ok()) {
            _exit(kChildApiError);
        }
        if (!ring->CommitEnqueue(*seq, 2000).ok()) {
            _exit(kChildApiError);
        }
        _exit(kChildOk);
    }
    WaitChild(pid);

    // The parent drains the rest: the remaining phase-1 messages in order,
    // then the phase-3 message.
    for (uint64_t i = 1; i < kCapacity; ++i) {
        auto got = ring_.TryDequeue();
        ASSERT_TRUE(got.ok()) << "i=" << i;
        auto value = ring_.ReadSlot(*got);
        ASSERT_TRUE(value.ok());
        EXPECT_EQ(*value, 1000 + i);
        ASSERT_TRUE(ring_.CommitDequeue(*got).ok());
    }
    auto got = ring_.TryDequeue();
    ASSERT_TRUE(got.ok());
    EXPECT_EQ(ring_.ReadSlot(*got).value(), 2000u);
    ASSERT_TRUE(ring_.CommitDequeue(*got).ok());
    EXPECT_TRUE(ring_.IsEmpty());
}

// A consumer process attaching to an empty ring observes IsEmpty() and
// kWouldBlock from TryDequeue.
TEST_F(MpmcRingXprocTest, EmptyRefusesDequeueAcrossProcesses) {
    const pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        auto ring = MpmcRing<uint64_t>::Attach(shared_->ring_storage);
        if (!ring.ok()) {
            _exit(kChildApiError);
        }
        shared_->probe_snapshot.store(ring->IsEmpty() ? 1 : 0,
                                      std::memory_order_release);
        auto blocked = ring->TryDequeue();
        if (blocked.ok()) {
            _exit(kChildWrongCode);  // An empty ring produced a message.
        }
        shared_->probe_code.store(static_cast<int>(blocked.status().code()),
                                  std::memory_order_release);
        _exit(kChildOk);
    }
    WaitChild(pid);
    EXPECT_EQ(shared_->probe_code.load(std::memory_order_acquire),
              static_cast<int>(StatusCode::kWouldBlock));
    EXPECT_EQ(shared_->probe_snapshot.load(std::memory_order_acquire), 1);
    EXPECT_TRUE(ring_.IsEmpty());
}

}  // namespace
}  // namespace mino
