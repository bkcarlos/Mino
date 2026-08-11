// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_TOPIC_PARTITION_H_
#define MINO_STORAGE_TOPIC_PARTITION_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/storage/recording_types.h"

namespace mino::storage {

inline constexpr uint16_t kStablePartitionHashVersion = 1;
inline constexpr uint64_t kDefaultPartitionHashSeed = 0x6d696e6f2d703031ull;
inline constexpr uint32_t kMaximumTopicPartitions = 65536;

enum class TopicPartitionStrategy : uint8_t {
    // StableHash(explicit business key, seed) % partition_count.
    kKey = 1,
    // Fold a caller-supplied stable 64-bit hash with the configured seed.
    kHash = 2,
    // Hash (node_id, publisher_id, publisher_epoch); source_sequence is excluded
    // so one source cannot move between partitions while a map is active.
    kSource = 3,
    // The caller must provide an explicit partition ID.
    kManual = 4,
};

std::string_view TopicPartitionStrategyName(
    TopicPartitionStrategy strategy) noexcept;
Result<TopicPartitionStrategy> ParseTopicPartitionStrategy(
    std::string_view value) noexcept;

enum class TopicPartitionMapState : uint8_t {
    kPrepared = 1,
    kActive = 2,
    kDraining = 3,
    kRetired = 4,
};

std::string_view TopicPartitionMapStateName(
    TopicPartitionMapState state) noexcept;

struct TopicPartitionMap {
    // map_version is a topic configuration/CAS identity. generation identifies
    // an immutable on-disk writer generation. Neither may be reused.
    uint64_t map_version = 1;
    uint64_t generation = 1;
    uint32_t partition_count = 1;
    TopicPartitionStrategy strategy = TopicPartitionStrategy::kManual;
    TopicPartitionMapState state = TopicPartitionMapState::kActive;
    uint16_t hash_algorithm_version = kStablePartitionHashVersion;
    uint64_t hash_seed = kDefaultPartitionHashSeed;

    bool operator==(const TopicPartitionMap&) const = default;
};

Status ValidateTopicPartitionMap(const TopicPartitionMap& map) noexcept;

struct TopicPartitionRouteInput {
    std::span<const std::byte> key;
    std::optional<uint64_t> hash;
    std::optional<uint32_t> manual_partition_id;
    MessageSource source;
};

// Cross-platform deterministic hash. Version 1 is FNV-1a over the seed and
// bytes followed by a fixed SplitMix64 finalizer. Inputs are serialized
// explicitly little-endian; native object layouts are never hashed.
uint64_t StablePartitionHash(std::span<const std::byte> bytes,
                             uint64_t seed) noexcept;
uint64_t StableSourcePartitionHash(const MessageSource& source,
                                   uint64_t seed) noexcept;
Result<uint32_t> SelectTopicPartition(
    const TopicPartitionMap& map,
    const TopicPartitionRouteInput& input) noexcept;

// Deliberately mirrors the conservation fields exposed by rolling-upgrade drain
// observations without depending on the upgrade package. This keeps Storage
// usable independently and makes the cutover adapter mechanical.
struct PartitionDrainProof {
    bool old_routes_fenced = false;
    uint64_t queued_records = 0;
    uint64_t reserved_records = 0;
    uint64_t active_writers = 0;
    uint64_t last_admitted_sequence = 0;
    uint64_t last_persisted_sequence = 0;

    bool complete() const noexcept {
        return old_routes_fenced && queued_records == 0 &&
               reserved_records == 0 && active_writers == 0 &&
               last_persisted_sequence >= last_admitted_sequence;
    }
};

// Pure lifecycle gate. A partition-count/strategy/seed change is represented by
// a new immutable generation; in-place mutation has no valid transition.
Status ValidatePartitionMapPrepare(const TopicPartitionMap& current,
                                   const TopicPartitionMap& next) noexcept;
Status ValidatePartitionMapBeginDrain(const TopicPartitionMap& current,
                                      const TopicPartitionMap& prepared) noexcept;
Status ValidatePartitionMapCutover(const TopicPartitionMap& draining,
                                   const TopicPartitionMap& prepared,
                                   const PartitionDrainProof& proof) noexcept;

}  // namespace mino::storage

#endif  // MINO_STORAGE_TOPIC_PARTITION_H_
