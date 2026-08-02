// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_RECORDING_MANIFEST_H_
#define MINO_STORAGE_RECORDING_MANIFEST_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/result.h"

namespace mino::storage {

inline constexpr uint16_t kRecordingManifestFormatVersion = 1;
inline constexpr uint16_t kPartitionManifestFormatVersion = 1;
inline constexpr size_t kSchemaDigestSize = 32;

enum class ManifestFaultPoint : uint8_t {
    kAfterTempWrite,
    kAfterTempDataSync,
    kAfterRename,
    kAfterParentDirectorySync,
    kAfterOrphanRename,
    kAfterOrphanDirectorySync,
};

using ManifestFaultHook =
    Status (*)(ManifestFaultPoint point, void* context) noexcept;

struct ManifestLimits {
    size_t max_manifest_bytes = 64u * 1024u * 1024u;
    size_t max_topics = 65536;
    size_t max_schemas_per_topic = 65536;
    size_t max_segments = 1u << 20;
    size_t max_topic_name_bytes = 1024;
    size_t max_relative_path_bytes = 4096;
};

struct ManifestOptions {
    ManifestLimits limits{};
    // Recovery watermarks supplied by a trusted lease/coordinator. A persisted
    // value below any non-zero watermark is rejected as a rollback.
    uint64_t minimum_generation = 0;
    uint64_t minimum_config_version = 0;
    uint64_t minimum_durable_sequence = 0;
    ManifestFaultHook fault_hook = nullptr;
    void* fault_hook_context = nullptr;
};

struct SchemaRefSnapshot {
    uint32_t schema_ref = 0;
    uint32_t schema_version = 0;
    uint32_t layout_version = 0;
    std::array<std::byte, kSchemaDigestSize> canonical_digest{};
    // Session-root-relative, canonical form: schemas/<digest>.schema.
    std::filesystem::path descriptor_path;

    bool operator==(const SchemaRefSnapshot&) const = default;
};

struct TopicTableEntry {
    uint32_t topic_id = 0;
    std::string topic_name;
    uint64_t config_version = 0;
    std::vector<SchemaRefSnapshot> schema_snapshot;

    bool operator==(const TopicTableEntry&) const = default;
};

struct RecordingSessionMetadata {
    uint64_t recording_id = 0;
    uint64_t created_at_ns = 0;
    uint64_t owner_id = 0;
    uint64_t owner_epoch = 0;
    uint64_t config_version = 0;

    bool operator==(const RecordingSessionMetadata&) const = default;
};

struct RecordingManifestSnapshot {
    uint64_t generation = 0;
    RecordingSessionMetadata session;
    std::vector<TopicTableEntry> topics;

    bool operator==(const RecordingManifestSnapshot&) const = default;
};

enum class SegmentPersistentState : uint8_t {
    kCreating = 1,
    kOpen = 2,
    kSealed = 3,
    kIndexed = 4,
    kRetained = 5,
    kDeleted = 6,
};

struct SegmentManifestEntry {
    uint64_t segment_id = 0;
    SegmentPersistentState state = SegmentPersistentState::kCreating;
    uint64_t first_ingestion_sequence = 0;
    uint64_t last_ingestion_sequence = 0;
    uint64_t created_at_ns = 0;
    uint64_t sealed_at_ns = 0;
    uint64_t size_bytes = 0;
    // Partition-root-relative, canonical form: segments/<name>.mino.
    std::filesystem::path relative_path;

    bool operator==(const SegmentManifestEntry&) const = default;
};

struct DurableCheckpoint {
    uint64_t segment_id = 0;
    uint64_t durable_offset = 0;
    uint64_t durable_sequence = 0;

    bool operator==(const DurableCheckpoint&) const = default;
};

struct PartitionMetadata {
    uint64_t recording_id = 0;
    uint32_t topic_id = 0;
    uint32_t partition_id = 0;
    uint64_t writer_id = 0;
    uint64_t owner_epoch = 0;
    uint64_t config_version = 0;

    bool operator==(const PartitionMetadata&) const = default;
};

struct PartitionManifestSnapshot {
    uint64_t generation = 0;
    PartitionMetadata partition;
    std::optional<DurableCheckpoint> checkpoint;
    std::vector<SegmentManifestEntry> segments;

    bool operator==(const PartitionManifestSnapshot&) const = default;
};

// Pure codecs are exposed for golden vectors, bounded-parser fuzzing, and
// offline recovery tools. Integers are encoded explicitly little-endian; the
// CRC32C field is treated as zero while computing the checksum.
Result<std::vector<std::byte>> EncodeRecordingManifest(
    const RecordingManifestSnapshot& snapshot,
    const ManifestLimits& limits = {}) noexcept;
Result<RecordingManifestSnapshot> DecodeRecordingManifest(
    std::span<const std::byte> encoded,
    const ManifestLimits& limits = {}) noexcept;
Result<std::vector<std::byte>> EncodePartitionManifest(
    const PartitionManifestSnapshot& snapshot,
    const ManifestLimits& limits = {}) noexcept;
Result<PartitionManifestSnapshot> DecodePartitionManifest(
    std::span<const std::byte> encoded,
    const ManifestLimits& limits = {}) noexcept;

// Owns <session_root>/manifest and a non-blocking advisory owner lock. The
// object is deliberately not thread-safe. Every successful mutation is durable
// before return (temp write, fdatasync, rename, fsync parent).
class RecordingManifest final {
public:
    static Result<std::unique_ptr<RecordingManifest>> Create(
        const std::filesystem::path& session_root,
        const RecordingSessionMetadata& metadata,
        const ManifestOptions& options = {}) noexcept;
    static Result<std::unique_ptr<RecordingManifest>> Open(
        const std::filesystem::path& session_root,
        const ManifestOptions& options = {}) noexcept;

    ~RecordingManifest();
    RecordingManifest(const RecordingManifest&) = delete;
    RecordingManifest& operator=(const RecordingManifest&) = delete;
    RecordingManifest(RecordingManifest&&) = delete;
    RecordingManifest& operator=(RecordingManifest&&) = delete;

    Status AddTopic(TopicTableEntry topic) noexcept;
    // Topic ID/name mapping is immutable. Config version and schema snapshot
    // may only move forward; schema refs already present may not change.
    Status UpdateTopic(TopicTableEntry topic) noexcept;
    Status UpdateSessionConfigVersion(uint64_t config_version) noexcept;

    Result<TopicTableEntry> FindTopic(uint32_t topic_id) const noexcept;
    Result<TopicTableEntry> FindTopic(std::string_view topic_name) const noexcept;
    const RecordingManifestSnapshot& snapshot() const noexcept { return snapshot_; }
    bool poisoned() const noexcept { return poisoned_; }

private:
    RecordingManifest(std::filesystem::path root, ManifestOptions options,
                      int lock_fd, RecordingManifestSnapshot snapshot) noexcept;
    Status Commit(RecordingManifestSnapshot next) noexcept;

    std::filesystem::path root_;
    ManifestOptions options_;
    int lock_fd_ = -1;
    bool poisoned_ = false;
    RecordingManifestSnapshot snapshot_;
};

// Owns <partition_root>/manifest and its owner lock. The same lock also
// serializes sealed-orphan adoption/quarantine with manifest commits.
class PartitionManifest final {
public:
    static Result<std::unique_ptr<PartitionManifest>> Create(
        const std::filesystem::path& partition_root,
        const PartitionMetadata& metadata,
        const ManifestOptions& options = {}) noexcept;
    static Result<std::unique_ptr<PartitionManifest>> Open(
        const std::filesystem::path& partition_root,
        const ManifestOptions& options = {}) noexcept;

    ~PartitionManifest();
    PartitionManifest(const PartitionManifest&) = delete;
    PartitionManifest& operator=(const PartitionManifest&) = delete;
    PartitionManifest(PartitionManifest&&) = delete;
    PartitionManifest& operator=(PartitionManifest&&) = delete;

    Status AddSegment(SegmentManifestEntry segment) noexcept;
    Status UpdateSegment(SegmentManifestEntry segment) noexcept;
    Status UpdateCheckpoint(DurableCheckpoint checkpoint) noexcept;

    // The caller performs full Segment record/CRC validation first. Adoption
    // verifies that the candidate is an untracked regular non-symlink file and
    // persists it directly in SEALED state. Quarantine atomically renames an
    // untracked candidate to <name>.orphan and fsyncs the segments directory.
    Status AdoptSealedOrphan(SegmentManifestEntry sealed_segment) noexcept;
    Result<std::filesystem::path> QuarantineOrphan(
        const std::filesystem::path& relative_path) noexcept;

    Result<SegmentManifestEntry> FindSegment(uint64_t segment_id) const noexcept;
    const PartitionManifestSnapshot& snapshot() const noexcept { return snapshot_; }
    bool poisoned() const noexcept { return poisoned_; }

private:
    PartitionManifest(std::filesystem::path root, ManifestOptions options,
                      int lock_fd, PartitionManifestSnapshot snapshot) noexcept;
    Status Commit(PartitionManifestSnapshot next) noexcept;

    std::filesystem::path root_;
    ManifestOptions options_;
    int lock_fd_ = -1;
    bool poisoned_ = false;
    PartitionManifestSnapshot snapshot_;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_RECORDING_MANIFEST_H_
