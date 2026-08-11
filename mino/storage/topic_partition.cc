// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/topic_partition.h"

#include <array>
#include <limits>

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

bool ValidStrategy(TopicPartitionStrategy strategy) noexcept {
    switch (strategy) {
        case TopicPartitionStrategy::kKey:
        case TopicPartitionStrategy::kHash:
        case TopicPartitionStrategy::kSource:
        case TopicPartitionStrategy::kManual:
            return true;
    }
    return false;
}

bool ValidState(TopicPartitionMapState state) noexcept {
    switch (state) {
        case TopicPartitionMapState::kPrepared:
        case TopicPartitionMapState::kActive:
        case TopicPartitionMapState::kDraining:
        case TopicPartitionMapState::kRetired:
            return true;
    }
    return false;
}

void StoreLe64(uint64_t value, std::byte* output) noexcept {
    for (size_t index = 0; index < sizeof(value); ++index) {
        output[index] = static_cast<std::byte>(value & 0xffu);
        value >>= 8u;
    }
}

uint64_t Finalize(uint64_t value) noexcept {
    value ^= value >> 30u;
    value *= 0xbf58476d1ce4e5b9ull;
    value ^= value >> 27u;
    value *= 0x94d049bb133111ebull;
    value ^= value >> 31u;
    return value;
}

}  // namespace

std::string_view TopicPartitionStrategyName(
    TopicPartitionStrategy strategy) noexcept {
    switch (strategy) {
        case TopicPartitionStrategy::kKey: return "key";
        case TopicPartitionStrategy::kHash: return "hash";
        case TopicPartitionStrategy::kSource: return "source";
        case TopicPartitionStrategy::kManual: return "manual";
    }
    return "unknown";
}

Result<TopicPartitionStrategy> ParseTopicPartitionStrategy(
    std::string_view value) noexcept {
    if (value == "key") return TopicPartitionStrategy::kKey;
    if (value == "hash") return TopicPartitionStrategy::kHash;
    if (value == "source") return TopicPartitionStrategy::kSource;
    if (value == "manual") return TopicPartitionStrategy::kManual;
    return Invalid("partition strategy must be key, hash, source, or manual");
}

std::string_view TopicPartitionMapStateName(
    TopicPartitionMapState state) noexcept {
    switch (state) {
        case TopicPartitionMapState::kPrepared: return "prepared";
        case TopicPartitionMapState::kActive: return "active";
        case TopicPartitionMapState::kDraining: return "draining";
        case TopicPartitionMapState::kRetired: return "retired";
    }
    return "unknown";
}

Status ValidateTopicPartitionMap(const TopicPartitionMap& map) noexcept {
    if (map.map_version == 0 || map.generation == 0 ||
        map.partition_count == 0 ||
        map.partition_count > kMaximumTopicPartitions ||
        !ValidStrategy(map.strategy) || !ValidState(map.state)) {
        return Invalid("topic partition map identity is invalid");
    }
    if (map.hash_algorithm_version != kStablePartitionHashVersion) {
        return Status::Error(StatusCode::kUnsupported,
                             "topic partition hash version is unsupported");
    }
    return Status::Ok();
}

uint64_t StablePartitionHash(std::span<const std::byte> bytes,
                             uint64_t seed) noexcept {
    constexpr uint64_t kFnvOffset = 14695981039346656037ull;
    constexpr uint64_t kFnvPrime = 1099511628211ull;
    uint64_t value = kFnvOffset;
    std::array<std::byte, sizeof(seed)> encoded_seed{};
    StoreLe64(seed, encoded_seed.data());
    for (std::byte byte : encoded_seed) {
        value ^= static_cast<uint8_t>(byte);
        value *= kFnvPrime;
    }
    for (std::byte byte : bytes) {
        value ^= static_cast<uint8_t>(byte);
        value *= kFnvPrime;
    }
    return Finalize(value);
}

uint64_t StableSourcePartitionHash(const MessageSource& source,
                                   uint64_t seed) noexcept {
    std::array<std::byte, 3 * sizeof(uint64_t)> encoded{};
    StoreLe64(source.node_id, encoded.data());
    StoreLe64(source.publisher_id, encoded.data() + sizeof(uint64_t));
    StoreLe64(source.publisher_epoch, encoded.data() + 2 * sizeof(uint64_t));
    return StablePartitionHash(encoded, seed);
}

Result<uint32_t> SelectTopicPartition(
    const TopicPartitionMap& map,
    const TopicPartitionRouteInput& input) noexcept {
    MINO_RETURN_IF_ERROR(ValidateTopicPartitionMap(map));
    if (map.state != TopicPartitionMapState::kActive) {
        return Status::Error(StatusCode::kUnavailable,
                             "topic partition map is not active");
    }

    uint64_t hash = 0;
    switch (map.strategy) {
        case TopicPartitionStrategy::kKey:
            if (input.key.empty()) {
                return Invalid("key partition strategy requires a non-empty key");
            }
            hash = StablePartitionHash(input.key, map.hash_seed);
            break;
        case TopicPartitionStrategy::kHash: {
            if (!input.hash.has_value()) {
                return Invalid("hash partition strategy requires a stable hash");
            }
            std::array<std::byte, sizeof(uint64_t)> encoded{};
            StoreLe64(*input.hash, encoded.data());
            hash = StablePartitionHash(encoded, map.hash_seed);
            break;
        }
        case TopicPartitionStrategy::kSource:
            if (input.source.node_id == 0 || input.source.publisher_id == 0 ||
                input.source.publisher_epoch == 0) {
                return Invalid("source partition strategy requires source identity");
            }
            hash = StableSourcePartitionHash(input.source, map.hash_seed);
            break;
        case TopicPartitionStrategy::kManual:
            if (!input.manual_partition_id.has_value()) {
                return Invalid("manual partition strategy requires partition_id");
            }
            if (*input.manual_partition_id >= map.partition_count) {
                return Invalid("manual partition_id exceeds partition count");
            }
            return *input.manual_partition_id;
    }
    return static_cast<uint32_t>(hash % map.partition_count);
}

Status ValidatePartitionMapPrepare(const TopicPartitionMap& current,
                                   const TopicPartitionMap& next) noexcept {
    MINO_RETURN_IF_ERROR(ValidateTopicPartitionMap(current));
    MINO_RETURN_IF_ERROR(ValidateTopicPartitionMap(next));
    if (current.state != TopicPartitionMapState::kActive ||
        next.state != TopicPartitionMapState::kPrepared) {
        return Invalid("repartition prepare requires active and prepared maps");
    }
    if (current.map_version == std::numeric_limits<uint64_t>::max() ||
        current.generation == std::numeric_limits<uint64_t>::max() ||
        next.map_version <= current.map_version ||
        next.generation <= current.generation) {
        return Invalid("repartition requires a newer map version and generation");
    }
    return Status::Ok();
}

Status ValidatePartitionMapBeginDrain(const TopicPartitionMap& current,
                                      const TopicPartitionMap& prepared) noexcept {
    MINO_RETURN_IF_ERROR(ValidatePartitionMapPrepare(current, prepared));
    return Status::Ok();
}

Status ValidatePartitionMapCutover(const TopicPartitionMap& draining,
                                   const TopicPartitionMap& prepared,
                                   const PartitionDrainProof& proof) noexcept {
    MINO_RETURN_IF_ERROR(ValidateTopicPartitionMap(draining));
    MINO_RETURN_IF_ERROR(ValidateTopicPartitionMap(prepared));
    if (draining.state != TopicPartitionMapState::kDraining ||
        prepared.state != TopicPartitionMapState::kPrepared ||
        prepared.map_version <= draining.map_version ||
        prepared.generation <= draining.generation) {
        return Invalid("repartition cutover maps are not drain-compatible");
    }
    if (!proof.complete()) {
        return Status::Error(StatusCode::kWouldBlock,
                             "repartition cutover requires a complete drain proof");
    }
    return Status::Ok();
}

}  // namespace mino::storage
