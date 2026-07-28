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

#include "mino/shm/allocator/bitmap.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <set>
#include <thread>
#include <vector>

#include "mino/common/status.h"

namespace mino {
namespace {

class BitmapTest : public ::testing::Test {
protected:
    static constexpr uint32_t kShards = 4;

    // 4 shards = 256 bits, backed by cache-line-sized storage as in the
    // shared-memory layout. std::atomic is non-copyable, so a plain array
    // is used instead of std::vector.
    std::atomic<uint64_t> storage_[kShards];
    ShardedBitmap bitmap_;

    void SetUp() override {
        for (auto& word : storage_) {
            word.store(0, std::memory_order_relaxed);
        }
        auto result = ShardedBitmap::Create(storage_, kShards);
        ASSERT_TRUE(result.ok());
        bitmap_ = result.value();
    }
};

TEST_F(BitmapTest, CreateRejectsNullStorage) {
    auto result = ShardedBitmap::Create(nullptr, 4);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(BitmapTest, CreateRejectsZeroShards) {
    auto result = ShardedBitmap::Create(storage_, 0);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(BitmapTest, FindAndSetClaimsUniqueBitsInOrder) {
    std::set<uint32_t> claimed;
    for (int i = 0; i < 128; ++i) {
        auto bit = bitmap_.FindAndSetFreeBit(/*shard_hint=*/0);
        ASSERT_TRUE(bit.ok()) << bit.status().ToString();
        EXPECT_TRUE(bitmap_.IsSet(bit.value()));
        EXPECT_TRUE(claimed.insert(bit.value()).second) << "bit claimed twice";
    }
    // With hint 0 and no concurrency the scan starts at shard 0, so bits are
    // claimed in increasing order.
    EXPECT_EQ(*claimed.begin(), 0u);
    EXPECT_EQ(*claimed.rbegin(), 127u);
}

TEST_F(BitmapTest, FindAndSetHonorsShardHint) {
    auto bit = bitmap_.FindAndSetFreeBit(/*shard_hint=*/2);
    ASSERT_TRUE(bit.ok());
    EXPECT_GE(bit.value(), 2 * kBitmapShardBits);
    EXPECT_LT(bit.value(), 3 * kBitmapShardBits);
}

TEST_F(BitmapTest, FindAndSetWrapsAroundShardHint) {
    // Fill shard 0 completely, then a hint of 0 must spill into shard 1.
    for (uint32_t i = 0; i < kBitmapShardBits; ++i) {
        auto bit = bitmap_.FindAndSetFreeBit(0);
        ASSERT_TRUE(bit.ok());
        EXPECT_LT(bit.value(), kBitmapShardBits);
    }
    auto spill = bitmap_.FindAndSetFreeBit(0);
    ASSERT_TRUE(spill.ok());
    EXPECT_GE(spill.value(), kBitmapShardBits);
    EXPECT_LT(spill.value(), 2 * kBitmapShardBits);
}

TEST_F(BitmapTest, ExhaustionReturnsResourceExhausted) {
    for (uint32_t i = 0; i < bitmap_.bit_count(); ++i) {
        auto bit = bitmap_.FindAndSetFreeBit(0);
        ASSERT_TRUE(bit.ok()) << "i=" << i;
    }
    auto full = bitmap_.FindAndSetFreeBit(0);
    ASSERT_FALSE(full.ok());
    EXPECT_EQ(full.status().code(), StatusCode::kResourceExhausted);
}

TEST_F(BitmapTest, ClearBitFreesSlot) {
    auto bit = bitmap_.FindAndSetFreeBit(0);
    ASSERT_TRUE(bit.ok());
    EXPECT_TRUE(bitmap_.IsSet(bit.value()));

    ASSERT_TRUE(bitmap_.ClearBit(bit.value()).ok());
    EXPECT_FALSE(bitmap_.IsSet(bit.value()));

    // The freed bit can be claimed again.
    auto reclaimed = bitmap_.FindAndSetFreeBit(0);
    ASSERT_TRUE(reclaimed.ok());
    EXPECT_EQ(reclaimed.value(), bit.value());
}

TEST_F(BitmapTest, ClearBitRejectsDoubleClear) {
    auto bit = bitmap_.FindAndSetFreeBit(0);
    ASSERT_TRUE(bit.ok());
    ASSERT_TRUE(bitmap_.ClearBit(bit.value()).ok());

    const Status second = bitmap_.ClearBit(bit.value());
    ASSERT_FALSE(second.ok());
    EXPECT_EQ(second.code(), StatusCode::kNotFound);
}

TEST_F(BitmapTest, ClearBitRejectsOutOfRange) {
    const Status status = bitmap_.ClearBit(bitmap_.bit_count());
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
}

TEST_F(BitmapTest, IsSetRejectsOutOfRange) {
    EXPECT_FALSE(bitmap_.IsSet(bitmap_.bit_count()));
    EXPECT_FALSE(bitmap_.IsSet(UINT32_MAX));
}

TEST_F(BitmapTest, SetRangeMarksBits) {
    bitmap_.SetRange(10, 20);
    for (uint32_t i = 0; i < bitmap_.bit_count(); ++i) {
        EXPECT_EQ(bitmap_.IsSet(i), i >= 10 && i < 20) << "i=" << i;
    }
}

TEST_F(BitmapTest, ConcurrentClaimsAreUnique) {
    constexpr int kThreads = 8;
    constexpr int kClaimsPerThread = 16;

    std::vector<std::vector<uint32_t>> per_thread(kThreads);
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([this, t, &per_thread]() {
            for (int i = 0; i < kClaimsPerThread; ++i) {
                auto bit = bitmap_.FindAndSetFreeBit(static_cast<uint32_t>(t));
                ASSERT_TRUE(bit.ok());
                per_thread[t].push_back(bit.value());
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }

    std::set<uint32_t> all;
    for (const auto& vec : per_thread) {
        for (uint32_t bit : vec) {
            EXPECT_TRUE(all.insert(bit).second) << "duplicate bit " << bit;
        }
    }
    EXPECT_EQ(all.size(), static_cast<size_t>(kThreads) * kClaimsPerThread);
}

}  // namespace
}  // namespace mino
