// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_SNAPSHOT_STORE_H_
#define MINO_STORAGE_SNAPSHOT_STORE_H_

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/storage/segment_format.h"

namespace mino::storage {

enum class SnapshotStoreFaultPoint : uint8_t {
    kAfterTempWrite,
    kAfterTempDataSync,
    kAfterRename,
    kAfterParentDirectorySync,
};

using SnapshotStoreFaultHook =
    Status (*)(SnapshotStoreFaultPoint point, void* context) noexcept;

struct SnapshotStoreIdentity {
    uint64_t recording_id = 0;
    uint32_t topic_id = 0;
    uint32_t partition_id = 0;
    uint64_t writer_id = 0;
    std::vector<uint32_t> schema_refs;
};

struct SnapshotStoreOptions {
    SegmentFormatLimits format_limits{};
    SnapshotStoreFaultHook fault_hook = nullptr;
    void* fault_hook_context = nullptr;
};

// Durable last-value storage for one Topic/Partition. The canonical file is a
// SegmentHeader followed by exactly one EncodeRecord envelope, so normal segment
// recovery and replay readers can consume it. Put never appends: it writes a
// complete temporary file, fdatasyncs it, atomically renames it over
// snapshot.mino, then fsyncs the partition directory.
class SnapshotStore final {
public:
    static Result<std::unique_ptr<SnapshotStore>> Open(
        const std::filesystem::path& partition_root,
        SnapshotStoreIdentity identity,
        const SnapshotStoreOptions& options = {}) noexcept;

    ~SnapshotStore();

    SnapshotStore(const SnapshotStore&) = delete;
    SnapshotStore& operator=(const SnapshotStore&) = delete;
    SnapshotStore(SnapshotStore&&) = delete;
    SnapshotStore& operator=(SnapshotStore&&) = delete;

    // Assigns the next ingestion sequence, preserving every other header field
    // and the payload. On success the complete replacement is durable.
    Result<uint64_t> Put(Record record) noexcept;

    const std::filesystem::path& path() const noexcept { return path_; }
    bool has_snapshot() const noexcept { return latest_record_.has_value(); }
    uint64_t latest_ingestion_sequence() const noexcept {
        return latest_record_.has_value()
                   ? latest_record_->header.ingestion_sequence
                   : 0;
    }
    uint64_t next_ingestion_sequence() const noexcept {
        return next_ingestion_sequence_;
    }
    const std::optional<Record>& latest_record() const noexcept {
        return latest_record_;
    }

private:
    SnapshotStore(std::filesystem::path partition_root,
                  SnapshotStoreIdentity identity, SnapshotStoreOptions options,
                  int owner_lock_fd, std::optional<Record> latest_record,
                  uint64_t next_ingestion_sequence) noexcept;

    std::filesystem::path partition_root_;
    std::filesystem::path path_;
    SnapshotStoreIdentity identity_;
    SnapshotStoreOptions options_;
    int owner_lock_fd_ = -1;
    std::optional<Record> latest_record_;
    uint64_t next_ingestion_sequence_ = 1;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_SNAPSHOT_STORE_H_
