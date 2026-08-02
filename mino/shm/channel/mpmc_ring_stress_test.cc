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

// Stress test for the generic MPMC skeleton.
//
// This target is tagged `manual` in Bazel: it is excluded from the default
// CI run and meant to be executed explicitly (ideally for minutes, and under
// --config=tsan). Run it with:
//
//   bazel test //mino/shm/channel:mpmc_ring_stress_test
//       --test_timeout=3600 --test_env=MPMC_STRESS_DURATION_SEC=60
//
// Configuration via environment variables:
//   MPMC_STRESS_DURATION_SEC   wall-clock seconds for the timed phase
//                              (default: 10)
//   MPMC_STRESS_PRODUCERS      producer thread count        (default: 8)
//   MPMC_STRESS_CONSUMERS      consumer thread count        (default: 8)
//   MPMC_STRESS_CAPACITY_LOG2  log2 of the ring capacity    (default: 10)
//   MPMC_STRESS_TOKENS         tokens per producer in the fixed-count
//                              conservation phase           (default: 1 << 20)
//
// Invariants checked:
//   - Sequence conservation (fixed-count phase): the multiset of dequeued
//     sequence numbers equals the multiset of reserved ones — every position
//     is reserved exactly once and consumed exactly once.
//   - No data loss / no duplication (both phases): every enqueued token is
//     consumed exactly once.
//   - Data integrity (both phases): the token read from a slot is exactly the
//     token the reserving producer wrote into it.

#include "mino/shm/channel/mpmc_ring.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <new>
#include <thread>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino {
namespace {

uint64_t EnvOr(const char* name, uint64_t fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    return std::strtoull(value, nullptr, 10);
}

// Book-keeping for one stress run: per-producer multisets of consumed token
// counters. Tokens are (producer_id << 56) | counter; verifying each
// producer's multiset equals [0, its enqueued count) proves no loss and no
// duplication.
struct TokenLedger {
    explicit TokenLedger(uint64_t producers)
        : consumed(producers), consumed_mu(producers) {}

    void Record(uint64_t token) {
        const uint64_t producer = token >> 56;
        const uint64_t counter = token & 0x00FFFFFFFFFFFFFFULL;
        std::lock_guard<std::mutex> lock(consumed_mu[producer]);
        consumed[producer].push_back(counter);
    }

    // total_enqueued[p] is the number of tokens producer p committed.
    void Verify(const std::vector<uint64_t>& total_enqueued) {
        uint64_t grand_total = 0;
        for (uint64_t p = 0; p < consumed.size(); ++p) {
            auto& tokens = consumed[p];
            grand_total += tokens.size();
            ASSERT_EQ(tokens.size(), total_enqueued[p])
                << "producer " << p << ": lost or duplicated tokens";
            std::sort(tokens.begin(), tokens.end());
            for (uint64_t i = 0; i < tokens.size(); ++i) {
                ASSERT_EQ(tokens[i], i)
                    << "producer " << p << ": gap or duplicate at counter "
                    << i;
            }
        }
        uint64_t expected_total = 0;
        for (uint64_t n : total_enqueued) {
            expected_total += n;
        }
        EXPECT_EQ(grand_total, expected_total);
    }

    std::vector<std::vector<uint64_t>> consumed;
    std::vector<std::mutex> consumed_mu;
};

// One shared ring plus per-run state, torn down with the fixture.
struct StressRing {
    explicit StressRing(uint64_t capacity)
        : bytes(MpmcRing<uint64_t>::RequiredSize(capacity, sizeof(uint64_t),
                                                 alignof(uint64_t))),
          storage(static_cast<unsigned char*>(
              ::operator new(bytes, std::align_val_t(64)))) {
        std::memset(storage, 0, bytes);
        auto init = MpmcRing<uint64_t>::Init(storage, capacity,
                                             sizeof(uint64_t),
                                             alignof(uint64_t));
        EXPECT_TRUE(init.ok()) << init.status().ToString();
        ring = *init;
    }
    ~StressRing() { ::operator delete(storage, bytes, std::align_val_t(64)); }

    StressRing(const StressRing&) = delete;
    StressRing& operator=(const StressRing&) = delete;

    const uint64_t bytes;
    unsigned char* storage;
    MpmcRing<uint64_t> ring;
};

// ---------------------------------------------------------------------------
// Phase 1: fixed-count conservation under contention
// ---------------------------------------------------------------------------

TEST(MpmcRingStressTest, SequenceConservation) {
    const uint64_t num_producers = EnvOr("MPMC_STRESS_PRODUCERS", 8);
    const uint64_t num_consumers = EnvOr("MPMC_STRESS_CONSUMERS", 8);
    const uint64_t capacity = uint64_t{1}
                              << EnvOr("MPMC_STRESS_CAPACITY_LOG2", 10);
    const uint64_t tokens_per_producer =
        EnvOr("MPMC_STRESS_TOKENS", uint64_t{1} << 20);
    ASSERT_GE(capacity, 2u);
    ASSERT_GE(num_producers, 1u);
    ASSERT_GE(num_consumers, 1u);
    ASSERT_LE(tokens_per_producer, uint64_t{1} << 48)
        << "token counter field is 56 bits; keep totals representable";

    const uint64_t total = num_producers * tokens_per_producer;
    StressRing env(capacity);
    const MpmcRing<uint64_t> ring = env.ring;

    // Per-sequence counters: correct behavior sets every entry to exactly 1.
    std::vector<std::atomic<uint32_t>> reserved(total);
    std::vector<std::atomic<uint32_t>> consumed(total);
    for (uint64_t i = 0; i < total; ++i) {
        reserved[i].store(0, std::memory_order_relaxed);
        consumed[i].store(0, std::memory_order_relaxed);
    }

    TokenLedger token_ledger(num_producers);
    std::vector<uint64_t> enqueued_per_producer(num_producers, 0);

    std::atomic<bool> start{false};
    std::atomic<uint64_t> consumed_count{0};

    auto producer = [&](uint64_t id) {
        while (!start.load(std::memory_order_acquire)) {
        }
        uint64_t counter = 0;
        while (counter < tokens_per_producer) {
            auto seq = ring.TryEnqueue();
            if (!seq.ok()) {
                ASSERT_EQ(seq.status().code(), StatusCode::kResourceExhausted);
                std::this_thread::yield();
                continue;
            }
            ASSERT_LT(*seq, total);
            // Exactly one producer may win each reservation.
            ASSERT_EQ(reserved[*seq].fetch_add(1, std::memory_order_acq_rel),
                      0u)
                << "sequence " << *seq << " reserved more than once";
            const uint64_t token = (id << 56) | counter;
            ASSERT_TRUE(ring.CommitEnqueue(*seq, token).ok());
            ++counter;
        }
        enqueued_per_producer[id] = counter;
    };

    auto consumer = [&]() {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (;;) {
            if (consumed_count.load(std::memory_order_acquire) >= total) {
                return;
            }
            auto got = ring.TryDequeue();
            if (!got.ok()) {
                ASSERT_EQ(got.status().code(), StatusCode::kWouldBlock);
                std::this_thread::yield();
                continue;
            }
            ASSERT_LT(*got, total);
            ASSERT_EQ(consumed[*got].fetch_add(1, std::memory_order_acq_rel),
                      0u)
                << "sequence " << *got << " consumed more than once";
            auto value = ring.ReadSlot(*got);
            ASSERT_TRUE(value.ok());
            ASSERT_TRUE(ring.CommitDequeue(*got).ok());
            token_ledger.Record(*value);
            consumed_count.fetch_add(1, std::memory_order_acq_rel);
        }
    };

    std::vector<std::thread> threads;
    for (uint64_t p = 0; p < num_producers; ++p) {
        threads.emplace_back(producer, p);
    }
    for (uint64_t c = 0; c < num_consumers; ++c) {
        threads.emplace_back(consumer);
    }
    start.store(true, std::memory_order_release);
    for (auto& t : threads) {
        t.join();
    }

    // Conservation: every sequence in [0, total) reserved exactly once and
    // consumed exactly once.
    for (uint64_t seq = 0; seq < total; ++seq) {
        EXPECT_EQ(reserved[seq].load(std::memory_order_relaxed), 1u)
            << "sequence " << seq;
        EXPECT_EQ(consumed[seq].load(std::memory_order_relaxed), 1u)
            << "sequence " << seq;
    }
    EXPECT_EQ(consumed_count.load(), total);

    token_ledger.Verify(enqueued_per_producer);
    EXPECT_TRUE(ring.IsEmpty().value());
}

// ---------------------------------------------------------------------------
// Phase 2: timed high-contention run
// ---------------------------------------------------------------------------

TEST(MpmcRingStressTest, TimedHighContention) {
    const uint64_t duration_sec = EnvOr("MPMC_STRESS_DURATION_SEC", 10);
    const uint64_t num_producers = EnvOr("MPMC_STRESS_PRODUCERS", 8);
    const uint64_t num_consumers = EnvOr("MPMC_STRESS_CONSUMERS", 8);
    const uint64_t capacity = uint64_t{1}
                              << EnvOr("MPMC_STRESS_CAPACITY_LOG2", 10);
    ASSERT_GE(capacity, 2u);
    ASSERT_GE(num_producers, 1u);
    ASSERT_GE(num_consumers, 1u);

    StressRing env(capacity);
    const MpmcRing<uint64_t> ring = env.ring;

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(duration_sec);

    std::atomic<bool> start{false};
    std::atomic<bool> producers_done{false};
    std::atomic<uint64_t> full_events{0};
    std::atomic<uint64_t> empty_events{0};

    TokenLedger token_ledger(num_producers);
    std::vector<std::atomic<uint64_t>> enqueued_per_producer(num_producers);
    for (auto& counter : enqueued_per_producer) {
        counter.store(0, std::memory_order_relaxed);
    }

    auto producer = [&](uint64_t id) {
        while (!start.load(std::memory_order_acquire)) {
        }
        uint64_t counter = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            auto seq = ring.TryEnqueue();
            if (!seq.ok()) {
                ASSERT_EQ(seq.status().code(), StatusCode::kResourceExhausted);
                full_events.fetch_add(1, std::memory_order_relaxed);
                std::this_thread::yield();
                continue;
            }
            const uint64_t token =
                (id << 56) | (counter & 0x00FFFFFFFFFFFFFFULL);
            ASSERT_TRUE(ring.CommitEnqueue(*seq, token).ok());
            ++counter;
        }
        enqueued_per_producer[id].store(counter, std::memory_order_relaxed);
    };

    auto consumer = [&]() {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (;;) {
            if (producers_done.load(std::memory_order_acquire) &&
                ring.IsEmpty().value()) {
                return;
            }
            auto got = ring.TryDequeue();
            if (!got.ok()) {
                ASSERT_EQ(got.status().code(), StatusCode::kWouldBlock);
                empty_events.fetch_add(1, std::memory_order_relaxed);
                std::this_thread::yield();
                continue;
            }
            auto value = ring.ReadSlot(*got);
            ASSERT_TRUE(value.ok());
            ASSERT_TRUE(ring.CommitDequeue(*got).ok());
            token_ledger.Record(*value);
        }
    };

    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    for (uint64_t p = 0; p < num_producers; ++p) {
        producers.emplace_back(producer, p);
    }
    for (uint64_t c = 0; c < num_consumers; ++c) {
        consumers.emplace_back(consumer);
    }
    start.store(true, std::memory_order_release);
    for (auto& t : producers) {
        t.join();
    }
    producers_done.store(true, std::memory_order_release);
    for (auto& t : consumers) {
        t.join();
    }

    std::vector<uint64_t> totals(num_producers);
    uint64_t grand_total = 0;
    for (uint64_t p = 0; p < num_producers; ++p) {
        totals[p] = enqueued_per_producer[p].load(std::memory_order_relaxed);
        grand_total += totals[p];
    }

    std::cout << "[ MPMC-STRESS ] duration=" << duration_sec << "s"
              << " producers=" << num_producers
              << " consumers=" << num_consumers << " capacity=" << capacity
              << " tokens=" << grand_total
              << " throughput="
              << (duration_sec > 0 ? grand_total / duration_sec : grand_total)
              << " ops/s"
              << " full_events=" << full_events.load()
              << " empty_events=" << empty_events.load() << std::endl;

    token_ledger.Verify(totals);
    EXPECT_TRUE(ring.IsEmpty().value());
}

}  // namespace
}  // namespace mino
