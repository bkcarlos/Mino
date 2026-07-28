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

#include "mino/shm/allocator/generation_array.h"

#include <gtest/gtest.h>

#include <atomic>
#include <limits>
#include <vector>

#include "mino/common/status.h"

namespace mino {
namespace {

class GenerationArrayTest : public ::testing::Test {
protected:
    static constexpr uint32_t kSlots = 8;

    // std::atomic is non-copyable, so a plain array is used instead of
    // std::vector.
    std::atomic<uint32_t> storage_[kSlots];
    GenerationArray array_;

    void SetUp() override {
        for (auto& cell : storage_) {
            cell.store(0, std::memory_order_relaxed);
        }
        auto result = GenerationArray::Create(storage_, kSlots);
        ASSERT_TRUE(result.ok());
        array_ = result.value();
    }
};

TEST_F(GenerationArrayTest, CreateRejectsNullStorage) {
    auto result = GenerationArray::Create(nullptr, 4);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(GenerationArrayTest, CreateRejectsZeroSlots) {
    auto result = GenerationArray::Create(storage_, 0);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(GenerationArrayTest, IncrementReturnsNewValue) {
    EXPECT_EQ(array_.Increment(3), 1u);
    EXPECT_EQ(array_.Increment(3), 2u);
    EXPECT_EQ(array_.Increment(3), 3u);
    EXPECT_EQ(array_.Get(3), 3u);
}

TEST_F(GenerationArrayTest, SlotsAreIndependent) {
    EXPECT_EQ(array_.Increment(0), 1u);
    EXPECT_EQ(array_.Increment(1), 1u);
    EXPECT_EQ(array_.Increment(1), 2u);
    EXPECT_EQ(array_.Get(0), 1u);
    EXPECT_EQ(array_.Get(1), 2u);
    EXPECT_EQ(array_.Get(2), 0u);
}

TEST_F(GenerationArrayTest, GetOutOfRangeReturnsZero) {
    EXPECT_EQ(array_.Get(8), 0u);
    EXPECT_EQ(array_.Get(UINT32_MAX), 0u);
}

TEST_F(GenerationArrayTest, IncrementOutOfRangeReturnsDraining) {
    EXPECT_EQ(array_.Increment(8), kGenerationDraining);
    EXPECT_EQ(array_.Increment(UINT32_MAX), kGenerationDraining);
}

TEST_F(GenerationArrayTest, ReachingMaxReturnsDrainingAndDoesNotWrap) {
    storage_[5].store(std::numeric_limits<uint32_t>::max() - 1,
                      std::memory_order_relaxed);

    // Last valid increment reaches UINT32_MAX.
    EXPECT_EQ(array_.Increment(5), std::numeric_limits<uint32_t>::max());
    EXPECT_EQ(array_.Get(5), std::numeric_limits<uint32_t>::max());

    // Next increment must refuse the wrap and report DRAINING.
    EXPECT_EQ(array_.Increment(5), kGenerationDraining);
    EXPECT_EQ(array_.Get(5), std::numeric_limits<uint32_t>::max());
}

TEST_F(GenerationArrayTest, DrainingMarkerEqualsUint32Max) {
    // The DRAINING marker is exactly UINT32_MAX (design doc 8.3 step 6).
    EXPECT_EQ(kGenerationDraining, std::numeric_limits<uint32_t>::max());
}

}  // namespace
}  // namespace mino
