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

#include "mino/shm/allocator/class_table.h"

#include <gtest/gtest.h>

#include "mino/common/status.h"

namespace mino {
namespace {

ClassTableConfig TwoClassConfig() {
    ClassTableConfig config;
    config.classes = {
        {.slot_size = 64, .slot_count = 10},
        {.slot_size = 256, .slot_count = 5},
    };
    return config;
}

TEST(ClassTableTest, DefaultConfigHasDesignDocClasses) {
    const ClassTableConfig config = DefaultClassTableConfig();
    ASSERT_EQ(config.classes.size(), 4u);
    EXPECT_EQ(config.classes[0].slot_size, 64u);
    EXPECT_EQ(config.classes[1].slot_size, 256u);
    EXPECT_EQ(config.classes[2].slot_size, 2u * 1024u);
    EXPECT_EQ(config.classes[3].slot_size, 64u * 1024u);
}

TEST(ClassTableTest, CreateAssignsSequentialIdsAndShardAlignedOffsets) {
    auto result = ClassTable::Create(TwoClassConfig());
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    const ClassTable& table = result.value();

    ASSERT_EQ(table.class_count(), 2u);
    EXPECT_EQ(table.GetClass(0).class_id, 0u);
    EXPECT_EQ(table.GetClass(0).bitmap_shard_offset, 0u);
    EXPECT_EQ(table.GetClass(1).class_id, 1u);
    // Class 1 begins on the next 64-bit shard boundary (shard 1).
    EXPECT_EQ(table.GetClass(1).bitmap_shard_offset, 64u);
    // Total slots = 2 shards = 128 bits (56 bits of shard 0 and 60 bits of
    // shard 1 are reserved padding).
    EXPECT_EQ(table.total_slot_count(), 128u);
    EXPECT_EQ(table.max_object_size(), 256u);
}

TEST(ClassTableTest, FindClassSelectsSmallestFitting) {
    auto result = ClassTable::Create(TwoClassConfig());
    ASSERT_TRUE(result.ok());
    const ClassTable& table = result.value();

    EXPECT_EQ(table.FindClass(1).value(), 0u);
    EXPECT_EQ(table.FindClass(64).value(), 0u);   // exact fit class 0
    EXPECT_EQ(table.FindClass(65).value(), 1u);   // spills into class 1
    EXPECT_EQ(table.FindClass(256).value(), 1u);  // exact fit class 1
}

TEST(ClassTableTest, FindClassRejectsZeroSize) {
    auto result = ClassTable::Create(TwoClassConfig());
    ASSERT_TRUE(result.ok());
    const ClassTable& table = result.value();

    auto find = table.FindClass(0);
    ASSERT_FALSE(find.ok());
    EXPECT_EQ(find.status().code(), StatusCode::kInvalidArgument);
}

TEST(ClassTableTest, FindClassReturnsNotFoundWhenTooLarge) {
    auto result = ClassTable::Create(TwoClassConfig());
    ASSERT_TRUE(result.ok());
    const ClassTable& table = result.value();

    auto find = table.FindClass(257);
    ASSERT_FALSE(find.ok());
    EXPECT_EQ(find.status().code(), StatusCode::kNotFound);
}

TEST(ClassTableTest, CreateRejectsEmptyConfig) {
    auto result = ClassTable::Create(ClassTableConfig{});
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST(ClassTableTest, CreateRejectsZeroSlotSize) {
    ClassTableConfig config;
    config.classes = {{.slot_size = 0, .slot_count = 4}};
    auto result = ClassTable::Create(config);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST(ClassTableTest, CreateRejectsZeroSlotCount) {
    ClassTableConfig config;
    config.classes = {{.slot_size = 64, .slot_count = 0}};
    auto result = ClassTable::Create(config);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST(ClassTableTest, CreateRejectsNonIncreasingSizes) {
    ClassTableConfig config;
    config.classes = {
        {.slot_size = 256, .slot_count = 4},
        {.slot_size = 256, .slot_count = 4},  // equal, not strictly increasing
    };
    auto result = ClassTable::Create(config);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);

    ClassTableConfig decreasing;
    decreasing.classes = {
        {.slot_size = 256, .slot_count = 4},
        {.slot_size = 64, .slot_count = 4},
    };
    auto result2 = ClassTable::Create(decreasing);
    ASSERT_FALSE(result2.ok());
    EXPECT_EQ(result2.status().code(), StatusCode::kInvalidArgument);
}

TEST(ClassTableTest, CreateRejectsTooManyClasses) {
    ClassTableConfig config;
    for (uint32_t i = 0; i < kMaxClassCount + 1; ++i) {
        config.classes.push_back({.slot_size = 64 + i, .slot_count = 1});
    }
    auto result = ClassTable::Create(config);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST(ClassTableTest, SingleClassTable) {
    ClassTableConfig config;
    config.classes = {{.slot_size = 128, .slot_count = 8}};
    auto result = ClassTable::Create(config);
    ASSERT_TRUE(result.ok());
    const ClassTable& table = result.value();

    EXPECT_EQ(table.class_count(), 1u);
    EXPECT_EQ(table.FindClass(128).value(), 0u);
    EXPECT_FALSE(table.FindClass(129).ok());
    // One class occupies a single 64-bit shard.
    EXPECT_EQ(table.total_slot_count(), 64u);
}

}  // namespace
}  // namespace mino
