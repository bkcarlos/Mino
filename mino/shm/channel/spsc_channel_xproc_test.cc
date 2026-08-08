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

// INV-01 (cross-process): SPSC long-wrap with no ABA, no loss, no duplication
// across a real process boundary (design doc 9.4).
//
// spsc_channel_test.cc exercises the channel with std::thread inside one
// process; this test places the channel (control block + slots) in MAP_SHARED
// anonymous memory and fork()s real processes: one producer process, one
// consumer process. Across the boundary it verifies:
//   - FIFO order and exact sequence numbers over 20000 messages through a
//     64-slot ring (~312 full wrap cycles);
//   - queue-full backpressure (kBlock) works across processes;
//   - each message is delivered exactly once.
//
// Children never use gtest (its state is not fork-safe); they report through
// _exit() codes. All assertions run in the parent.

#include "mino/shm/channel/spsc_channel.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/shm/channel/queue_full_policy.h"

namespace mino {
namespace {

constexpr uint64_t kCapacity = 64;
constexpr uint64_t kTotal = 20000;  // ~312 wraps of the 64-slot ring.
constexpr size_t kChannelBytes =
    static_cast<size_t>(SpscChannel::RequiredSize(kCapacity));

// Child exit codes (children report failures through _exit(), not gtest).
constexpr int kChildOk = 0;
constexpr int kChildApiError = 1;   // Unexpected Result/Status failure.
constexpr int kChildWrongCode = 2;  // Wrong StatusCode on a probe.
constexpr int kChildOrder = 3;      // Sequence/payload out of order.

// Shared state: the channel storage plus a start barrier. No raw pointers —
// only atomics and plain byte storage, so the layout is independent of where
// each process maps it.
struct SharedBlock {
    alignas(SpscChannel::kCacheLineSize) unsigned char channel_storage[kChannelBytes];
    std::atomic<int> barrier;
};

// Producer body: attaches, waits for the barrier, publishes kTotal messages
// with kBlock backpressure. payload_len carries the monotonic index so the
// consumer can verify order. Never returns: reports through _exit().
[[noreturn]] void ProducerChildMain(SharedBlock* shared) {
    auto ch = SpscChannel::Attach(shared->channel_storage);
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
        res.value()->msg_type = 0x2000;
        res.value()->schema_version = (1u << 16);
        res.value()->schema_short_id = 0xABCD;
        res.value()->schema_layout_version = 1;
        res.value()->timestamp_ns = i;
        res.value()->payload.offset = 0x4000;
        res.value()->payload.generation = 1;
        res.value()->payload.region_id = 1;
        res.value()->payload_len = static_cast<uint32_t>(i);
        res.value()->flags = 0;
        if (!std::move(res.value()).Commit().ok()) {
            _exit(kChildApiError);
        }
    }
    _exit(kChildOk);
}

// Consumer body: attaches, waits for the barrier, polls kTotal messages and
// verifies strict FIFO order (sequence_num and payload_len both == index).
// Never returns: reports through _exit().
[[noreturn]] void ConsumerChildMain(SharedBlock* shared) {
    auto ch = SpscChannel::Attach(shared->channel_storage);
    if (!ch.ok()) {
        _exit(kChildApiError);
    }
    while (shared->barrier.load(std::memory_order_acquire) == 0) {
    }
    for (uint64_t i = 0; i < kTotal; ++i) {
        auto borrow = ch->Poll();
        while (!borrow.ok()) {
            if (borrow.status().code() != StatusCode::kWouldBlock) {
                // kCorruption or anything unexpected is a hard failure.
                _exit(kChildWrongCode);
            }
            borrow = ch->Poll();
        }
        if (borrow.value()->sequence_num != i ||
            borrow.value()->payload_len != static_cast<uint32_t>(i)) {
            _exit(kChildOrder);
        }
        if (!std::move(borrow.value()).Ack().ok()) {
            _exit(kChildApiError);
        }
    }
    _exit(kChildOk);
}

TEST(SpscXprocTest, LongWrapAcrossProcesses) {
    const size_t bytes = sizeof(SharedBlock);
    void* map = mmap(nullptr, bytes, PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(map, MAP_FAILED) << "mmap failed: " << std::strerror(errno);
    auto* shared = static_cast<SharedBlock*>(map);
    std::memset(map, 0, bytes);

    // Init the channel in the parent before forking.
    auto ch = SpscChannel::Init(shared->channel_storage, kCapacity);
    ASSERT_TRUE(ch.ok()) << ch.status().ToString();
    shared->barrier.store(0, std::memory_order_relaxed);

    const pid_t producer = fork();
    ASSERT_NE(producer, -1) << "fork failed";
    if (producer == 0) {
        ProducerChildMain(shared);  // never returns
    }

    const pid_t consumer = fork();
    ASSERT_NE(consumer, -1) << "fork failed";
    if (consumer == 0) {
        ConsumerChildMain(shared);  // never returns
    }

    // Release both children together.
    shared->barrier.store(1, std::memory_order_release);

    int status = 0;
    ASSERT_EQ(waitpid(producer, &status, 0), producer);
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), kChildOk)
        << "producer failed (exit code " << WEXITSTATUS(status) << ")";

    status = 0;
    ASSERT_EQ(waitpid(consumer, &status, 0), consumer);
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), kChildOk)
        << "consumer failed (exit code " << WEXITSTATUS(status) << ")";

    // After both finish the queue must be drained.
    EXPECT_TRUE(ch->IsEmpty());
    EXPECT_EQ(ch->Size(), 0u);

    munmap(map, bytes);
}

}  // namespace
}  // namespace mino
