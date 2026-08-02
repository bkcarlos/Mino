// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_SESSION_RECOVERY_COORDINATOR_H_
#define MINO_STORAGE_SESSION_RECOVERY_COORDINATOR_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <vector>

#include "mino/common/result.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/schema_store.h"
#include "mino/storage/segment_recovery.h"

namespace mino::storage {

using SessionRecoveryDirectorySyncHook =
    int (*)(int directory_fd, void* context) noexcept;

struct SessionRecoveryOptions {
    ManifestOptions manifest_options{};
    SchemaStoreOptions schema_store_options{};
    SegmentRecoveryOptions segment_recovery_options{};
    SegmentRepairOptions segment_repair_options{};
    SessionRecoveryDirectorySyncHook directory_sync_hook = nullptr;
    void* directory_sync_hook_context = nullptr;
    // Zero selects the current system-clock timestamp. This is persisted as the
    // seal time of an adopted/recovered segment.
    uint64_t recovery_timestamp_ns = 0;
    size_t max_partitions = 65536;
    size_t max_segment_candidates_per_partition = 1u << 20;
};

struct DurableBoundaryReport {
    uint32_t topic_id = 0;
    uint32_t partition_id = 0;
    uint64_t manifest_generation = 0;
    uint64_t durable_segment_id = 0;
    uint64_t durable_offset = 0;
    uint64_t durable_sequence = 0;
    size_t tracked_segments_scanned = 0;
    size_t orphan_candidates_scanned = 0;
    size_t repaired_segments = 0;
    size_t adopted_sealed_orphans = 0;
    size_t quarantined_orphans = 0;
};

struct SessionRecoveryReport {
    uint64_t recording_id = 0;
    uint64_t manifest_generation = 0;
    size_t schema_refs_validated = 0;
    size_t partitions_recovered = 0;
    size_t segments_scanned = 0;
    size_t repaired_segments = 0;
    size_t adopted_sealed_orphans = 0;
    size_t quarantined_orphans = 0;
    std::vector<DurableBoundaryReport> durable_boundaries;
};

// Single-owner startup coordinator for an existing recording session. Recovery
// is intentionally explicit and mutating: every successful repair, adoption,
// quarantine, and checkpoint update is durable before Recover returns.
class SessionRecoveryCoordinator final {
public:
    static Result<std::unique_ptr<SessionRecoveryCoordinator>> Open(
        const std::filesystem::path& session_root,
        SessionRecoveryOptions options = {}) noexcept;

    ~SessionRecoveryCoordinator();
    SessionRecoveryCoordinator(const SessionRecoveryCoordinator&) = delete;
    SessionRecoveryCoordinator& operator=(const SessionRecoveryCoordinator&) = delete;
    SessionRecoveryCoordinator(SessionRecoveryCoordinator&&) = delete;
    SessionRecoveryCoordinator& operator=(SessionRecoveryCoordinator&&) = delete;

    Result<SessionRecoveryReport> Recover() noexcept;

private:
    class Impl;
    explicit SessionRecoveryCoordinator(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_SESSION_RECOVERY_COORDINATOR_H_
