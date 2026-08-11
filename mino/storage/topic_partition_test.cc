// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/topic_partition.h"

#include <array>

#include "gtest/gtest.h"

namespace mino::storage {
namespace {

TEST(TopicPartitionTest, StableHashAndStrategiesAreDeterministic) {
    constexpr std::array<std::byte, 7> key = {
        std::byte{'h'}, std::byte{'o'}, std::byte{'t'}, std::byte{'-'},
        std::byte{'k'}, std::byte{'e'}, std::byte{'y'},
    };
    const uint64_t first = StablePartitionHash(key, 17);
    EXPECT_EQ(first, 15722575754017373447ull);
    EXPECT_EQ(StablePartitionHash(key, 17), first);
    EXPECT_NE(StablePartitionHash(key, 18), first);

    TopicPartitionMap map{
        .map_version = 4,
        .generation = 2,
        .partition_count = 16,
        .strategy = TopicPartitionStrategy::kKey,
        .state = TopicPartitionMapState::kActive,
        .hash_algorithm_version = kStablePartitionHashVersion,
        .hash_seed = 17,
    };
    TopicPartitionRouteInput input{
        .key = key, .hash = std::nullopt,
        .manual_partition_id = std::nullopt, .source = {}};
    auto selected = SelectTopicPartition(map, input);
    ASSERT_TRUE(selected.ok()) << selected.status().ToString();
    EXPECT_EQ(*selected, first % map.partition_count);

    map.strategy = TopicPartitionStrategy::kSource;
    input = TopicPartitionRouteInput{
        .key = {},
        .hash = std::nullopt,
        .manual_partition_id = std::nullopt,
        .source = MessageSource{.node_id = 9,
                                .publisher_id = 11,
                                .publisher_epoch = 3,
                                .source_sequence = 1},
    };
    const auto source_one = SelectTopicPartition(map, input);
    ASSERT_TRUE(source_one.ok()) << source_one.status().ToString();
    input.source.source_sequence = 999;
    EXPECT_EQ(SelectTopicPartition(map, input).value(), *source_one);
}

TEST(TopicPartitionTest, ManualAndHotKeyRemainInOnePartition) {
    TopicPartitionMap manual{
        .map_version = 1,
        .generation = 1,
        .partition_count = 4,
        .strategy = TopicPartitionStrategy::kManual,
        .state = TopicPartitionMapState::kActive,
    };
    EXPECT_EQ(SelectTopicPartition(
                  manual, TopicPartitionRouteInput{
                              .key = {}, .hash = std::nullopt,
                              .manual_partition_id = 3, .source = {}})
                  .value(),
              3u);
    EXPECT_EQ(SelectTopicPartition(
                  manual, TopicPartitionRouteInput{
                              .key = {}, .hash = std::nullopt,
                              .manual_partition_id = 4, .source = {}})
                  .status()
                  .code(),
              StatusCode::kInvalidArgument);

    constexpr std::array<std::byte, 3> hot = {
        std::byte{'h'}, std::byte{'o'}, std::byte{'t'}};
    manual.strategy = TopicPartitionStrategy::kKey;
    const auto expected = SelectTopicPartition(
        manual, TopicPartitionRouteInput{
                    .key = hot, .hash = std::nullopt,
                    .manual_partition_id = std::nullopt, .source = {}});
    ASSERT_TRUE(expected.ok());
    for (size_t index = 0; index < 1000; ++index) {
        EXPECT_EQ(SelectTopicPartition(
                      manual, TopicPartitionRouteInput{
                                  .key = hot, .hash = std::nullopt,
                                  .manual_partition_id = std::nullopt,
                                  .source = {}})
                      .value(),
                  *expected);
    }
}

TEST(TopicPartitionTest, RepartitionRequiresNewGenerationAndCompleteDrain) {
    TopicPartitionMap current{
        .map_version = 7,
        .generation = 2,
        .partition_count = 2,
        .strategy = TopicPartitionStrategy::kSource,
        .state = TopicPartitionMapState::kActive,
    };
    TopicPartitionMap prepared{
        .map_version = 8,
        .generation = 3,
        .partition_count = 4,
        .strategy = TopicPartitionStrategy::kSource,
        .state = TopicPartitionMapState::kPrepared,
    };
    EXPECT_TRUE(ValidatePartitionMapPrepare(current, prepared).ok());
    TopicPartitionMap in_place = prepared;
    in_place.generation = current.generation;
    EXPECT_EQ(ValidatePartitionMapPrepare(current, in_place).code(),
              StatusCode::kInvalidArgument);

    current.state = TopicPartitionMapState::kDraining;
    PartitionDrainProof incomplete;
    EXPECT_EQ(ValidatePartitionMapCutover(current, prepared, incomplete).code(),
              StatusCode::kWouldBlock);
    PartitionDrainProof complete{
        .old_routes_fenced = true,
        .queued_records = 0,
        .reserved_records = 0,
        .active_writers = 0,
        .last_admitted_sequence = 42,
        .last_persisted_sequence = 42,
    };
    EXPECT_TRUE(ValidatePartitionMapCutover(current, prepared, complete).ok());
}

}  // namespace
}  // namespace mino::storage
