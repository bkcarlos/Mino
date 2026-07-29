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

// D2-05 (cross-process): broadcast channel tests across a real process
// boundary (design doc 9.6, dev plan D2 DoD).
//
// broadcast_channel_test.cc exercises the channel with std::thread inside
// one process; these tests place the channel (control block + slots +
// sidecars + subscriber slots) in MAP_SHARED anonymous memory and fork()
// real processes, verifying across process boundaries:
//   - fan-out conservation: 1 publisher process + 3 subscriber processes
//     move 10000 messages through a 64-slot channel (~156 wrap cycles);
//     every subscriber receives every message exactly once, in order;
//   - publisher crash: a publisher that dies via _exit() with a live
//     Reservation leaves a kWriting slot; stamping the ABORTED tombstone
//     lets subscribers skip it transparently (no /proc dependency: the
//     publisher is single-writer, so the tombstone is plain protocol).
//
// Children never use gtest (its state is not fork-safe); they report through
// _exit() codes and shared atomics. All assertions run in the parent.

#include "mino/shm/channel/broadcast_channel.h"

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
#include <optional>
#include <thread>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/shm/channel/index_slot.h"
#include "mino/shm/channel/queue_full_policy.h"

namespace mino {
namespace {

constexpr uint64_t kCapacity = 64;
constexpr uint64_t kTotal = 10000;  // ~156 wraps of the 64-slot ring.
constexpr size_t kChannelBytes =
    static_cast<size_t>(BroadcastChannel::RequiredSize(kCapacity));

// Fan-out test topology.
constexpr int kSubscribers = 3;

// Child exit codes (children report failures through _exit(), not gtest).
constexpr int kChildOk = 0;
constexpr int kChildApiError = 1;   // Unexpected Result/Status failure.
constexpr int kChildWrongCode = 2;  // Wrong StatusCode on a probe.
constexpr int kChildOrder = 3;      // Sequence/payload out of order.

// Shared state: the channel storage plus a start barrier. No raw pointers —
// only atomics and plain byte storage, so the layout is independent of where
// each process maps it. The channel requires a 64-byte aligned region base;
// the mmap base is page aligned and this member sits at offset 0.
struct SharedBlock {
    alignas(BroadcastChannel::kCacheLineSize)
        unsigned char channel_storage[kChannelBytes];
    std::atomic<int> barrier;
    // Per-subscriber progress counters, written by subscriber children so
    // the parent can verify every subscriber drained the full stream.
    std::atomic<uint64_t> consumed[kSubscribers];
};

// Fills the reserved slot with deterministic content derived from `tag`.
// payload_len carries the message index so subscribers can verify order.
void FillSlot(BroadcastChannel::Reservation& res, uint64_t index) {
    const uint32_t tag = static_cast<uint32_t>(index & 0xFFFF);
    res->msg_type = 0x5000 + tag;
    res->schema_version = (1u << 16) | 0u;
    res->schema_short_id = 0xABCDEF00ULL + tag;
    res->schema_layout_version = 1;
    res->timestamp_ns = 1'000'000 + tag;
    res->payload.offset = 0x9000 + 16ULL * tag;
    res->payload.generation = 1;
    res->payload.region_id = 1;
    res->payload_len = static_cast<uint32_t>(index);
    res->flags = 0;
}

// Publisher body: attaches, registers nothing, waits for the barrier and
// publishes kTotal messages with kBlock backpressure. Never returns:
// reports through _exit().
[[noreturn]] void PublisherChildMain(SharedBlock* shared) {
    auto ch = BroadcastChannel::Attach(shared->channel_storage);
    if (!ch.ok()) {
        _exit(kChildApiError);
    }
    while (shared->barrier.load(std::memory_order_acquire) == 0) {
    }
    for (uint64_t i = 0; i < kTotal; ++i) {
        auto res = ch->Reserve(QueueFullPolicy::kBlock);
        if (!res.ok()) {
            _exit(kChildApiError);
        }
        FillSlot(res.value(), i);
        if (!std::move(res.value()).Commit().ok()) {
            _exit(kChildApiError);
        }
    }
    _exit(kChildOk);
}

// Subscriber body: attaches, re-attaches its registration handle (the ids
// are pre-registered by the parent before the fork), waits for the barrier
// and polls kTotal messages verifying strict per-subscriber order. Never
// returns: reports through _exit().
[[noreturn]] void SubscriberChildMain(SharedBlock* shared, uint32_t id,
                                      uint64_t generation) {
    auto ch = BroadcastChannel::Attach(shared->channel_storage);
    if (!ch.ok()) {
        _exit(kChildApiError);
    }
    const BroadcastChannel::SubscriberHandle handle{SubscriberId{id},
                                                    generation};
    while (shared->barrier.load(std::memory_order_acquire) == 0) {
    }
    for (uint64_t i = 0; i < kTotal; ++i) {
        auto borrow = ch->Poll(handle);
        while (!borrow.ok()) {
            if (borrow.status().code() != StatusCode::kWouldBlock) {
                // kCorruption or anything unexpected is a hard failure.
                _exit(kChildWrongCode);
            }
            borrow = ch->Poll(handle);
        }
        if (borrow.value()->sequence_num != i ||
            borrow.value()->payload_len != static_cast<uint32_t>(i)) {
            _exit(kChildOrder);
        }
        if (!std::move(borrow.value()).Ack().ok()) {
            _exit(kChildApiError);
        }
        shared->consumed[id].store(i + 1, std::memory_order_release);
    }
    _exit(kChildOk);
}

class BroadcastChannelXprocTest : public ::testing::Test {
protected:
    void SetUp() override {
        void* mapped = mmap(nullptr, sizeof(SharedBlock),
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(mapped, MAP_FAILED) << "mmap failed: " << strerror(errno);
        shared_ = new (mapped) SharedBlock();
        shared_->barrier.store(0, std::memory_order_relaxed);
        for (int s = 0; s < kSubscribers; ++s) {
            shared_->consumed[s].store(0, std::memory_order_relaxed);
        }

        auto ch = BroadcastChannel::Init(shared_->channel_storage, kCapacity);
        ASSERT_TRUE(ch.ok()) << ch.status().ToString();
        channel_.emplace(*ch);
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

    // Reaps a child that is expected to have exited; returns its exit
    // status word for custom assertions.
    static int ReapRaw(pid_t pid) {
        int status = 0;
        EXPECT_EQ(waitpid(pid, &status, 0), pid);
        return status;
    }

    SharedBlock* shared_ = nullptr;
    // BroadcastChannel is a view with no default constructor, so the
    // fixture holds it lazily in an optional populated by SetUp().
    std::optional<BroadcastChannel> channel_;
};

// 1 publisher process fans out to 3 subscriber processes through one
// 64-slot channel. kTotal / kCapacity = ~156 wrap cycles per slot. Every
// subscriber must observe every message exactly once, in order: no loss,
// no duplication, no reordering, independent cursor progress.
TEST_F(BroadcastChannelXprocTest, FanOutConservationAcrossProcesses) {
    // Pre-register the three subscribers in the parent so the children only
    // need their handles (registration is fork-safe state in SHM).
    BroadcastChannel::SubscriberHandle handles[kSubscribers];
    for (uint32_t s = 0; s < kSubscribers; ++s) {
        auto sub = channel_->RegisterSubscriber(SubscriberId{s});
        ASSERT_TRUE(sub.ok()) << sub.status().ToString();
        handles[s] = *sub;
    }

    const pid_t publisher = fork();
    ASSERT_NE(publisher, -1) << "fork failed";
    if (publisher == 0) {
        PublisherChildMain(shared_);  // never returns
    }

    pid_t subscribers[kSubscribers];
    for (uint32_t s = 0; s < kSubscribers; ++s) {
        const pid_t pid = fork();
        ASSERT_NE(pid, -1) << "fork failed";
        if (pid == 0) {
            SubscriberChildMain(shared_, s, handles[s].generation);
        }
        subscribers[s] = pid;
    }

    // Release everyone together.
    shared_->barrier.store(1, std::memory_order_release);

    WaitChild(publisher);
    for (int s = 0; s < kSubscribers; ++s) {
        WaitChild(subscribers[s]);
    }

    // Every subscriber drained the full stream.
    for (int s = 0; s < kSubscribers; ++s) {
        EXPECT_EQ(shared_->consumed[s].load(std::memory_order_acquire),
                  kTotal)
            << "subscriber " << s << " lost messages";
    }
    EXPECT_EQ(channel_->Size(), kTotal);
    // All ACKs collected: the last era of every physical slot retired.
    channel_->CollectGarbage();
    const auto* slots = reinterpret_cast<const IndexSlot*>(
        shared_->channel_storage + BroadcastChannel::SlotsOffset());
    for (uint64_t i = 0; i < kCapacity; ++i) {
        EXPECT_EQ(slots[i].state.load(std::memory_order_acquire),
                  static_cast<uint32_t>(SlotState::kRetired))
            << "slot " << i;
    }
}

// A publisher that Reserve()s and then dies via _exit() never runs its
// Reservation destructor, so the slot stays kWriting with the publisher
// cursor un-advanced. The parent stamps the ABORTED tombstone and advances
// the cursor (single-writer protocol state, no /proc needed); subscribers
// must then skip the tombstone transparently and see the next message.
TEST_F(BroadcastChannelXprocTest, PublisherCrashTombstoneSkipped) {
    auto sub = channel_->RegisterSubscriber(SubscriberId{0});
    ASSERT_TRUE(sub.ok()) << sub.status().ToString();

    // Phase 1: the publisher commits one good message (seq 0), then
    // Reserve()s seq 1 and dies via _exit() without committing.
    const pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        auto ch = BroadcastChannel::Attach(shared_->channel_storage);
        if (!ch.ok()) {
            _exit(kChildApiError);
        }
        {
            auto res = ch->Reserve();
            if (!res.ok()) {
                _exit(kChildApiError);
            }
            FillSlot(res.value(), 0xAAAA);
            if (!std::move(res.value()).Commit().ok()) {
                _exit(kChildApiError);
            }
        }
        {
            auto res = ch->Reserve();
            if (!res.ok()) {
                _exit(kChildApiError);
            }
            FillSlot(res.value(), 0xBBBB);
            // Deliberately destroy the process while the Reservation is
            // live: no Commit, no Abort, no destructor.
            _exit(kChildOk);
        }
    }
    const int status = ReapRaw(pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), kChildOk);

    // Phase 2: the subscriber sees the good message, then nothing (the
    // publisher cursor never advanced past the orphaned reservation).
    {
        auto borrow = channel_->Poll(*sub);
        ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
        EXPECT_EQ(borrow.value()->sequence_num, 0u);
        EXPECT_EQ(borrow.value()->payload_len, 0xAAAAu);
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    {
        auto blocked = channel_->Poll(*sub);
        ASSERT_FALSE(blocked.ok());
        EXPECT_EQ(blocked.status().code(), StatusCode::kWouldBlock)
            << "orphaned reservation is invisible to subscribers";
    }

    // Phase 3: the parent reclaims the orphaned reservation: stamp the
    // ABORTED tombstone, clear the (empty) delivery bitmap and advance the
    // publisher cursor — exactly what AbortSlot does, driven here through a
    // fresh Reservation on the same channel (single-writer ownership now
    // belongs to the parent).
    //
    // The orphaned slot is the current head: Reserve() hands it back to us
    // (publisher_cursor still points at it) and we abort it immediately.
    {
        auto res = channel_->Reserve();
        ASSERT_TRUE(res.ok()) << res.status().ToString();
        EXPECT_EQ(res->sequence(), 1u);
        ASSERT_TRUE(std::move(res.value()).Abort().ok());
    }

    // Phase 4: publish one more message; the subscriber must skip the
    // tombstone transparently and deliver seq 2.
    {
        auto res = channel_->Reserve();
        ASSERT_TRUE(res.ok());
        FillSlot(res.value(), 0xCCCC);
        ASSERT_TRUE(std::move(res.value()).Commit().ok());
    }
    {
        auto borrow = channel_->Poll(*sub);
        ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
        EXPECT_EQ(borrow.value()->sequence_num, 2u);
        EXPECT_EQ(borrow.value()->payload_len, 0xCCCCu);
        ASSERT_TRUE(std::move(borrow.value()).Ack().ok());
    }
    auto empty = channel_->Poll(*sub);
    ASSERT_FALSE(empty.ok());
    EXPECT_EQ(empty.status().code(), StatusCode::kWouldBlock);
}

}  // namespace
}  // namespace mino
