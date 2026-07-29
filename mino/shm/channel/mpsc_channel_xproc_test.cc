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

// V-03 (part): cross-process MPSC channel tests (design doc 9.5 / 12.3).
//
// mpsc_channel_test.cc exercises the channel with std::thread inside one
// process; these tests place the channel (control block + slots + sidecars)
// in MAP_SHARED anonymous memory and fork() real processes, verifying across
// process boundaries:
//   - conservation: 2 producer processes + the parent consumer move 2 x 2000
//     uniquely tagged messages through a 64-slot channel (~62 wrap cycles)
//     with no message lost, none delivered twice, and the consumer observing
//     a strictly contiguous sequence (INV-17 ordered prefix);
//   - crash recovery: a producer that Reserve()s and then dies via _exit()
//     leaves an orphaned kWriting reservation; after the parent reaps it,
//     AbortOrphanedReservations() stamps an ABORTED tombstone and the
//     consumer skips it transparently instead of wedging (Linux only — the
//     liveness probe needs /proc, see IsOwnerAlive).
//
// Children never use gtest (its state is not fork-safe); they report through
// _exit() codes and shared atomics. All assertions run in the parent.

#include "mino/shm/channel/mpsc_channel.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <optional>
#include <thread>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/shm/channel/index_slot.h"

namespace mino {
namespace {

constexpr uint64_t kCapacity = 64;
constexpr size_t kChannelBytes =
    static_cast<size_t>(MpscChannel::RequiredSize(kCapacity));

// Conservation test topology.
constexpr int kProducers = 2;
constexpr uint64_t kPerProducer = 2000;
constexpr uint64_t kTotal = kPerProducer * kProducers;

// Child exit codes (children report failures through _exit(), not gtest).
constexpr int kChildOk = 0;
constexpr int kChildApiError = 1;   // Unexpected Result/Status failure.
constexpr int kChildWrongCode = 2;  // Wrong StatusCode on a probe.

// Shared state for one test case: the channel storage plus the cross-process
// bookkeeping. No raw pointers: only atomics and plain byte storage, so the
// layout is independent of where each process maps it. The channel requires
// a 64-byte aligned region base; the mmap base is page aligned and this
// member sits at offset 0 with 64-byte alignment.
struct SharedBlock {
    alignas(64) unsigned char channel_storage[kChannelBytes];

    // Start barrier: children wait until the parent releases them together.
    std::atomic<int> barrier;
    // Number of messages consumed so far (conservation test).
    std::atomic<uint64_t> consumed;
    // claimed[p][i]: how many times message i of producer p was delivered.
    std::atomic<uint32_t> claimed[kProducers][kPerProducer];
    // Recovery probes reported by children / scanned by the parent.
    std::atomic<uint64_t> aborted_count;
};

// Builds a producer identity for the calling process. publisher_id is the
// Registry-style ID stamped into the sidecar.
MpscChannel::ProducerIdentity MakeIdentity(uint64_t publisher_id) {
    MpscChannel::ProducerIdentity id;
    id.owner = ProcessIdentity::Current();
    id.publisher_id = publisher_id;
    return id;
}

// Fills the reserved slot with a deterministic, CRC-sealed payload derived
// from the producer tag and per-producer index.
void FillSlot(MpscChannel::Reservation& res, uint32_t tag) {
    res->msg_type = 0x3000 + tag;
    res->schema_version = (1u << 16) | 0u;
    res->schema_short_id = 0xABCDEF00ULL + tag;
    res->schema_layout_version = 1;
    res->timestamp_ns = 1'000'000 + tag;
    res->payload.offset = 0x8000 + 16ULL * tag;
    res->payload.generation = 1;
    res->payload.region_id = 1;
    res->payload_len = 96 + tag;
    res->flags = 0;
}

// Monotonic clock matching MpscChannel's internal MonotonicNowNs(): both use
// steady_clock nanoseconds, so a parent-computed `now_ns` is comparable to
// the reservation timestamps a child stamped.
// Used only by the Linux-only crash-recovery test, hence [[maybe_unused]].
[[maybe_unused]] uint64_t NowNs() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

// Producer body for the conservation test. Publishes kPerProducer uniquely
// tagged messages, retrying while the channel is full. The tag encodes the
// producer id in the high bits and the per-producer index in the low bits so
// the consumer can build the claim matrix. Never returns: _exit().
[[noreturn]] void ProducerChildMain(SharedBlock* shared, int id) {
    auto ch = MpscChannel::Attach(shared->channel_storage);
    if (!ch.ok()) {
        _exit(kChildApiError);
    }
    const MpscChannel::ProducerIdentity owner =
        MakeIdentity(static_cast<uint64_t>(id) + 1);
    while (shared->barrier.load(std::memory_order_acquire) == 0) {
    }
    for (uint64_t i = 0; i < kPerProducer; ++i) {
        const uint32_t tag =
            static_cast<uint32_t>((id << 24) | static_cast<uint32_t>(i));
        for (;;) {
            auto res = ch->Reserve(owner);
            if (res.ok()) {
                FillSlot(*res, tag);
                if (!std::move(*res).Commit().ok()) {
                    _exit(kChildApiError);
                }
                break;
            }
            const StatusCode code = res.status().code();
            if (code != StatusCode::kResourceExhausted &&
                code != StatusCode::kWouldBlock) {
                _exit(kChildWrongCode);
            }
            std::this_thread::yield();
        }
    }
    _exit(kChildOk);
}

class MpscChannelXprocTest : public ::testing::Test {
protected:
    void SetUp() override {
        void* mapped = mmap(nullptr, sizeof(SharedBlock),
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(mapped, MAP_FAILED) << "mmap failed: " << strerror(errno);
        shared_ = new (mapped) SharedBlock();
        shared_->barrier.store(0, std::memory_order_relaxed);
        shared_->consumed.store(0, std::memory_order_relaxed);
        shared_->aborted_count.store(0, std::memory_order_relaxed);
        for (int p = 0; p < kProducers; ++p) {
            for (uint64_t i = 0; i < kPerProducer; ++i) {
                shared_->claimed[p][i].store(0, std::memory_order_relaxed);
            }
        }

        auto ch = MpscChannel::Init(shared_->channel_storage, kCapacity);
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

    // Reaps a child that is expected to have exited; returns its exit status
    // word for custom assertions (crash-recovery test inspects the code).
    static int ReapRaw(pid_t pid) {
        int status = 0;
        EXPECT_EQ(waitpid(pid, &status, 0), pid);
        return status;
    }

    SharedBlock* shared_ = nullptr;
    // MpscChannel is a move-only view with no default constructor, so the
    // fixture holds it lazily in an optional populated by SetUp().
    std::optional<MpscChannel> channel_;
};

// 2 producer processes publish into one 64-slot channel while the parent
// consumes. kTotal / kCapacity = ~62 wrap cycles per slot, so the wrap path
// is exercised heavily. The consumer must observe a strictly contiguous
// sequence (0..kTotal-1) and every (producer, index) pair claimed exactly
// once: no loss, no duplication, no reordering gaps (INV-17).
TEST_F(MpscChannelXprocTest, ConservationAcrossProcessesNoLossNoDuplication) {
    pid_t children[kProducers];
    for (int p = 0; p < kProducers; ++p) {
        const pid_t pid = fork();
        ASSERT_NE(pid, -1) << "fork failed";
        if (pid == 0) {
            ProducerChildMain(shared_, p);
        }
        children[p] = pid;
    }
    shared_->barrier.store(1, std::memory_order_release);

    // Single consumer (the parent). Poll until all kTotal messages arrive;
    // the ordered prefix guarantees a contiguous sequence with no gaps.
    uint64_t expected_seq = 0;
    while (shared_->consumed.load(std::memory_order_acquire) < kTotal) {
        auto borrow = channel_->Poll();
        if (!borrow.ok()) {
            std::this_thread::yield();
            continue;
        }
        const IndexSlotSnapshot* snap = borrow->slot();
        EXPECT_EQ(snap->sequence_num, expected_seq)
            << "gap or reorder at consumed=" << expected_seq;
        const uint32_t tag = snap->msg_type - 0x3000u;
        const int producer_id = static_cast<int>(tag >> 24);
        const uint64_t index = tag & 0xFFFFFFu;
        ASSERT_LT(producer_id, kProducers) << "tag out of range";
        ASSERT_LT(index, kPerProducer) << "index out of range";
        shared_->claimed[producer_id][index].fetch_add(
            1, std::memory_order_acq_rel);
        shared_->consumed.fetch_add(1, std::memory_order_acq_rel);
        ASSERT_TRUE(std::move(*borrow).Ack().ok());
        ++expected_seq;
    }

    for (int p = 0; p < kProducers; ++p) {
        WaitChild(children[p]);
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
    EXPECT_TRUE(channel_->IsEmpty());
}

// A producer that Reserve()s and then dies via _exit() never runs its
// Reservation destructor, so the slot stays kWriting: an orphaned reservation
// that would wedge the queue (the consumer cannot pass the unfinished
// sequence). After the parent reaps the child, AbortOrphanedReservations()
// must detect the dead owner (via /proc) + expired lease, stamp an ABORTED
// tombstone, and let the consumer skip it transparently.
//
// Linux-only: IsOwnerAlive() has no /proc on other platforms and always
// reports the owner alive, so the tombstone is never stamped there.
TEST_F(MpscChannelXprocTest, ProducerCrashLeavesOrphanThenRecoveryReclaims) {
#if !defined(__linux__)
    GTEST_SKIP() << "IsOwnerAlive needs /proc; only Linux reclaims a dead "
                    "owner (other platforms report alive and rely on lease)";
#else
    // Phase 1: a producer commits one good message (seq 0), then Reserve()s
    // seq 1 and dies via _exit() without committing. The reservation is
    // orphaned in kWriting.
    const pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        auto ch = MpscChannel::Attach(shared_->channel_storage);
        if (!ch.ok()) {
            _exit(kChildApiError);
        }
        const MpscChannel::ProducerIdentity owner = MakeIdentity(1);
        // seq 0: a complete, consumable message.
        {
            auto res = ch->Reserve(owner);
            if (!res.ok()) {
                _exit(kChildApiError);
            }
            FillSlot(*res, 0xAA);
            if (!std::move(*res).Commit().ok()) {
                _exit(kChildApiError);
            }
        }
        // seq 1: reserved but never committed; _exit skips the RAII Abort.
        {
            auto res = ch->Reserve(owner);
            if (!res.ok()) {
                _exit(kChildApiError);
            }
            FillSlot(*res, 0xBB);
            // Deliberately destroy the process while the Reservation is
            // live: no Commit, no Abort, no destructor.
            _exit(kChildOk);
        }
    }
    const int status = ReapRaw(pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), kChildOk);

    // Phase 2: before recovery, seq 0 is consumable but seq 1 wedges the
    // queue: the consumer sees the good message, then kWouldBlock behind the
    // orphaned reservation.
    {
        auto borrow = channel_->Poll();
        ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
        EXPECT_EQ(borrow->slot()->sequence_num, 0u);
        EXPECT_EQ(borrow->slot()->msg_type, 0x3000u + 0xAAu);
        ASSERT_TRUE(std::move(*borrow).Ack().ok());
    }
    {
        auto blocked = channel_->Poll();
        ASSERT_FALSE(blocked.ok());
        EXPECT_EQ(blocked.status().code(), StatusCode::kWouldBlock)
            << "orphaned reservation should wedge the consumer";
    }

    // Phase 3: the parent scans with an expired lease. The child is reaped
    // (its PID no longer exists), so IsOwnerAlive() is false and the
    // reservation is far older than a 1ns lease: exactly one tombstone.
    const uint64_t aborted =
        channel_->AbortOrphanedReservations(NowNs(), /*lease_ns=*/1);
    EXPECT_EQ(aborted, 1u) << "expected exactly one orphaned reservation";

    // Phase 4: the consumer transparently retires and skips the tombstone;
    // the queue drains to empty instead of staying wedged.
    for (int i = 0; i < 8; ++i) {
        auto blocked = channel_->Poll();
        if (!blocked.ok() &&
            blocked.status().code() == StatusCode::kWouldBlock) {
            break;
        }
        std::this_thread::yield();
    }
    EXPECT_TRUE(channel_->IsEmpty())
        << "tombstone must be skipped, leaving the queue empty";
#endif
}

// A PAUSED (SIGSTOP) but still-alive producer must NEVER be reclaimed, no
// matter how far past the lease its reservation ages (design doc 9.5: "暂停但
// 仍有效的 Producer 不得被误回收"; recovery must not judge a crash by timeout
// alone). IsOwnerAlive() reports a stopped process as alive (its /proc entry
// still exists on Linux; non-Linux probes conservatively report alive), so the
// scanner skips it. Once SIGCONT resumes the producer it Commits normally and
// the queue flows again — proof the slot was left intact.
TEST_F(MpscChannelXprocTest, PausedProducerIsNeverReclaimedThenResumes) {
    // Child: Reserve seq 0, then park until the parent signals it via the
    // shared barrier. While parked (first stopped with SIGSTOP, then continued
    // but still pre-Commit) its reservation sits in kWriting.
    const pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        auto ch = MpscChannel::Attach(shared_->channel_storage);
        if (!ch.ok()) {
            _exit(kChildApiError);
        }
        const MpscChannel::ProducerIdentity owner = MakeIdentity(1);
        auto res = ch->Reserve(owner);
        if (!res.ok()) {
            _exit(kChildApiError);
        }
        FillSlot(*res, 0xCC);
        // Tell the parent the reservation is held, then raise SIGSTOP on
        // itself: the parent will SIGCONT after its recovery scan. The
        // Reservation stays live across the stop/continue (no destructor).
        shared_->barrier.store(1, std::memory_order_release);
        raise(SIGSTOP);
        // Resumed: finish the publication and exit cleanly.
        if (!std::move(*res).Commit().ok()) {
            _exit(kChildApiError);
        }
        _exit(kChildOk);
    }

    // Wait until the child holds the reservation, then give it a moment to
    // actually stop itself (SIGSTOP delivery is asynchronous).
    while (shared_->barrier.load(std::memory_order_acquire) == 0) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Recovery scan with a long-expired lease: the child is alive (merely
    // stopped), so IsOwnerAlive() is true on every platform and the scanner
    // must reclaim NOTHING.
    const uint64_t aborted =
        channel_->AbortOrphanedReservations(NowNs(), /*lease_ns=*/1);
    EXPECT_EQ(aborted, 0u)
        << "a paused-but-alive producer must never be reclaimed";
    // The consumer is still wedged behind the live reservation: no tombstone
    // was stamped, so the ordered prefix cannot advance.
    {
        auto blocked = channel_->Poll();
        ASSERT_FALSE(blocked.ok());
        EXPECT_EQ(blocked.status().code(), StatusCode::kWouldBlock);
    }

    // Resume the child; it Commits seq 0 and exits. The queue now flows,
    // proving the slot was never reclaimed out from under the live owner.
    ASSERT_EQ(kill(pid, SIGCONT), 0) << "SIGCONT failed: " << strerror(errno);
    WaitChild(pid);
    {
        auto borrow = channel_->Poll();
        ASSERT_TRUE(borrow.ok()) << borrow.status().ToString();
        EXPECT_EQ(borrow->slot()->sequence_num, 0u);
        EXPECT_EQ(borrow->slot()->msg_type, 0x3000u + 0xCCu);
        ASSERT_TRUE(std::move(*borrow).Ack().ok());
    }
    EXPECT_TRUE(channel_->IsEmpty());
}

}  // namespace
}  // namespace mino
