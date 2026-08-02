// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_RETENTION_H_
#define MINO_STORAGE_RETENTION_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>

#include "mino/common/result.h"
#include "mino/storage/recording_manifest.h"

namespace mino::storage {

inline constexpr uint64_t kDefaultSegmentPinTtlNs =
    5ull * 60ull * 1000ull * 1000ull * 1000ull;

struct RetentionPolicy {
    // A missing limit disables that policy. max_age_ns is evaluated against the
    // latest ingestion timestamp in the segment, not its seal timestamp.
    std::optional<uint64_t> max_age_ns;
    std::optional<uint64_t> max_total_bytes;
    std::optional<size_t> max_segment_count;
    bool delete_archived_segments = false;
};

struct RetentionSegment {
    SegmentManifestEntry manifest;
    uint64_t latest_ingestion_timestamp_ns = 0;
    bool archive_complete = false;
};

enum class RetentionReason : uint8_t {
    kNone = 0,
    kAge = 1u << 0,
    kTotalBytes = 1u << 1,
    kSegmentCount = 1u << 2,
    kArchiveComplete = 1u << 3,
};

constexpr RetentionReason operator|(RetentionReason left,
                                    RetentionReason right) noexcept {
    return static_cast<RetentionReason>(static_cast<uint8_t>(left) |
                                        static_cast<uint8_t>(right));
}

constexpr RetentionReason& operator|=(RetentionReason& left,
                                      RetentionReason right) noexcept {
    left = left | right;
    return left;
}

constexpr bool HasRetentionReason(RetentionReason reasons,
                                  RetentionReason reason) noexcept {
    return (static_cast<uint8_t>(reasons) & static_cast<uint8_t>(reason)) != 0;
}

struct RetentionDecision {
    uint64_t segment_id = 0;
    RetentionReason reasons = RetentionReason::kNone;

    bool operator==(const RetentionDecision&) const = default;
};

struct RetentionPlan {
    // Ordered oldest first, then by segment ID. Only SEALED, INDEXED, and
    // RETAINED segments can appear here.
    std::vector<RetentionDecision> deletions;
    uint64_t remaining_total_bytes = 0;
    size_t remaining_segment_count = 0;
    bool byte_limit_satisfied = true;
    bool segment_limit_satisfied = true;
};

class RetentionPlanner final {
public:
    static Result<RetentionPlan> Plan(
        std::span<const RetentionSegment> segments,
        const RetentionPolicy& policy, uint64_t now_ns) noexcept;
};

using RetentionNowFunction = uint64_t (*)(void* context) noexcept;

struct SegmentPinManagerOptions {
    uint64_t default_ttl_ns = kDefaultSegmentPinTtlNs;
    uint64_t max_ttl_ns = 24ull * 60ull * 60ull * 1000ull * 1000ull * 1000ull;
    RetentionNowFunction now = nullptr;
    void* now_context = nullptr;
};

struct SegmentPinLease {
    uint64_t pin_id = 0;
    uint64_t segment_id = 0;
    uint64_t expires_at_ns = 0;

    bool operator==(const SegmentPinLease&) const = default;
};

// Thread-safe serialization point for pin acquisition and retention's
// manifest transition. Other users must not mutate the same PartitionManifest
// concurrently outside this manager/executor pair.
class SegmentPinManager final {
public:
    explicit SegmentPinManager(
        PartitionManifest& manifest,
        SegmentPinManagerOptions options = {}) noexcept;

    Result<SegmentPinLease> Acquire(uint64_t segment_id) noexcept;
    Result<SegmentPinLease> Acquire(uint64_t segment_id,
                                    uint64_t ttl_ns) noexcept;
    Result<SegmentPinLease> Renew(uint64_t pin_id) noexcept;
    Result<SegmentPinLease> Renew(uint64_t pin_id, uint64_t ttl_ns) noexcept;
    // Release is idempotent, including for an already expired lease.
    Status Release(uint64_t pin_id) noexcept;

    size_t PurgeExpired() noexcept;
    size_t ActivePinCount(uint64_t segment_id) noexcept;

private:
    friend class RetentionExecutor;

    struct BeginDeletionResult {
        SegmentManifestEntry segment;
        bool transitioned = false;
    };

    uint64_t NowNs() const noexcept;
    size_t PurgeExpiredLocked(uint64_t now_ns) noexcept;
    Result<SegmentPinLease> AcquireLocked(uint64_t segment_id,
                                          uint64_t ttl_ns);
    Result<SegmentPinLease> RenewLocked(uint64_t pin_id,
                                        uint64_t ttl_ns);
    Result<BeginDeletionResult> BeginDeletion(uint64_t segment_id) noexcept;

    PartitionManifest* manifest_;
    SegmentPinManagerOptions options_;
    std::mutex mutex_;
    uint64_t next_pin_id_ = 1;
    std::unordered_map<uint64_t, SegmentPinLease> leases_;
};

enum class RetentionFaultPoint : uint8_t {
    kAfterManifestDeleted,
    kBeforeUnlink,
    kAfterUnlink,
    kAfterParentDirectorySync,
};

using RetentionFaultHook =
    Status (*)(RetentionFaultPoint point, uint64_t segment_id,
               void* context) noexcept;

struct RetentionExecutorOptions {
    RetentionFaultHook fault_hook = nullptr;
    void* fault_hook_context = nullptr;
};

struct RetentionExecutionReport {
    std::vector<uint64_t> marked_deleted;
    std::vector<uint64_t> pending_pins;
    std::vector<uint64_t> unlinked;
    std::vector<uint64_t> already_missing;
};

// Retention completion is represented durably by the segment's DELETED state.
// Keeping the tombstone in the manifest makes every phase safely retryable.
class RetentionExecutor final {
public:
    static Result<std::unique_ptr<RetentionExecutor>> Create(
        const std::filesystem::path& partition_root,
        PartitionManifest& manifest, SegmentPinManager& pins,
        RetentionExecutorOptions options = {}) noexcept;

    ~RetentionExecutor();
    RetentionExecutor(const RetentionExecutor&) = delete;
    RetentionExecutor& operator=(const RetentionExecutor&) = delete;
    RetentionExecutor(RetentionExecutor&&) = delete;
    RetentionExecutor& operator=(RetentionExecutor&&) = delete;

    Result<RetentionExecutionReport> Execute(
        const RetentionPlan& plan) noexcept;
    // Resumes every DELETED tombstone after a process crash. Missing files are
    // treated as an idempotent post-unlink retry and the parent is fsynced.
    Result<RetentionExecutionReport> RecoverPendingDeletions() noexcept;

private:
    RetentionExecutor(std::filesystem::path partition_root,
                      PartitionManifest& manifest, SegmentPinManager& pins,
                      RetentionExecutorOptions options, int root_fd,
                      int segments_fd) noexcept;

    Result<RetentionExecutionReport> ExecuteIds(
        std::span<const uint64_t> segment_ids) noexcept;
    Result<bool> UnlinkSegment(const SegmentManifestEntry& segment) noexcept;
    Status RunFaultHook(RetentionFaultPoint point,
                        uint64_t segment_id) const noexcept;

    std::filesystem::path partition_root_;
    PartitionManifest* manifest_;
    SegmentPinManager* pins_;
    RetentionExecutorOptions options_;
    int root_fd_ = -1;
    int segments_fd_ = -1;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_RETENTION_H_
