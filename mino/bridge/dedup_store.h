// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_DEDUP_STORE_H_
#define MINO_BRIDGE_DEDUP_STORE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "mino/bridge/dedup_window.h"
#include "mino/bridge/source_identity.h"
#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino::bridge {

// Durable host-local source HWMs and per-publisher retired-epoch boundaries.
// Only the cumulative HWM is persisted (matching SessionHello); in-flight gap
// bitmap state remains memory-only and may be re-accepted after restart
// (at-least-once). Crash-safe updates use write + fsync + rename + directory
// fsync. Open fails closed on truncated or corrupt payloads.
enum class DedupStoreIoStage {
    kWrite, kFileSync, kRename, kDirectorySync, kRecoveryDirectorySync
};

// Deterministic fault/crash injection for tests. Called during Open or while
// the store mutex is held; must outlive the store and must not call back into it. Returning an
// error before an operation skips that operation; after=true runs on success.
// Production callers leave this null.
class DedupStoreTestHooks {
public:
    virtual ~DedupStoreTestHooks() = default;
    virtual Status Checkpoint(DedupStoreIoStage stage, bool after) const noexcept = 0;
};

struct DedupStoreOptions {
    // Required absolute or relative path to the durable snapshot file. The
    // parent directory is created with mode 0700 when missing.
    std::string path;
    // Bounds active source HWMs plus retired-publisher boundaries. Each boundary
    // consumes one slot regardless of the number of epochs it fences.
    size_t max_sources = 1024;
    const DedupStoreTestHooks* test_hooks = nullptr;
};

struct DedupStoreStats {
    // Total retained records (active HWMs plus retirement boundaries).
    size_t sources = 0;
    size_t capacity = 0;
    size_t retired_publishers = 0;
    uint64_t persistence_count = 0;
    uint64_t persistence_failures = 0;
    uint64_t persistence_total_ns = 0;
    uint64_t persistence_max_ns = 0;
    uint64_t capacity_rejections = 0;
};

class DedupStore {
public:
    // Share one owning instance across lanes. A nonblocking exclusive lock on
    // path + ".lock" rejects a second opener with kAlreadyExists. Do not unlink
    // the lock file while any process uses the store (snapshots are renamed).
    // Opens an existing snapshot or creates an empty store. A missing file is
    // treated as empty (not corruption). Truncation, bad magic/version, or a
    // CRC mismatch returns kCorruption.
    static Result<std::unique_ptr<DedupStore>> Open(
        DedupStoreOptions options) noexcept;

    ~DedupStore();
    DedupStore(const DedupStore&) = delete;
    DedupStore& operator=(const DedupStore&) = delete;

    // Active HWMs only, sorted like DedupWindow::SnapshotAccepted. Retirement
    // boundaries are excluded so they cannot be advertised as Accepted ACKs.
    Result<std::vector<DedupResumeEntry>> Load() const noexcept;

    // O(1) durable lookup, including sources evicted from the memory window.
    // Zero means no durable accepted prefix. Does not read the filesystem.
    uint64_t HighestAccepted(const SourceIdentity& source) const noexcept;
    // Retirement is a rejection boundary, never an accepted-prefix ACK.
    bool IsRetired(const SourceIdentity& source) const noexcept;

    // Offline only: destroy all attached pipelines, stop the publisher epochs
    // being retired, and drain their reliable retransmissions first. Atomically
    // removes HWMs for this node/publisher through last_retired.publisher_epoch
    // and persists a monotonic rejection boundary. This is an explicit operator
    // decision, not proof inferred from epoch ordering, elapsed time or a new peer.
    // Boundaries survive ReplaceAll/reopen and cannot be removed by this API.
    // First retirement upgrades the snapshot to v2; old binaries fail to open it.
    // If persistence fails, close/reopen before any further use: rename may have
    // succeeded even when the directory durability barrier failed.
    Status RetireEpochsThrough(const SourceIdentity& last_retired) noexcept;
    DedupStoreStats stats() const noexcept;

    // Monotonic upsert of the durable HWM for one source. Persists before
    // returning OK. Incomplete identities and zero sequences are rejected.
    Status RecordAccepted(const SourceIdentity& source,
                          uint64_t highest_contiguous_sequence) noexcept;
    // One atomic durable commit for a group of monotonic updates. Duplicate
    // sources are coalesced to their largest HWM. Failure restores memory.
    Status RecordAcceptedBatch(std::span<const DedupResumeEntry> entries) noexcept;
    uint64_t persistence_count() const noexcept {
        std::lock_guard lock(mutex_);
        return persistence_count_;
    }

    // Offline maintenance only: stop all pipelines and fence retired publishers
    // and their retransmissions before removing entries. Age or a newer epoch
    // alone is not a retirement proof. Empty is allowed. Replacement is atomic;
    // existing retirement boundaries are preserved. Attached pipelines reject it.
    Status ReplaceAll(const std::vector<DedupResumeEntry>& entries) noexcept;

    const std::string& path() const noexcept { return path_; }
    // Total capacity slots used, including retirement boundaries.
    size_t size() const noexcept {
        std::lock_guard lock(mutex_);
        return entries_.size() + retired_.size();
    }
    size_t max_sources() const noexcept { return max_sources_; }

private:
    friend class BridgePipeline;
    Status AttachPipeline() noexcept;
    void DetachPipeline() noexcept;

    DedupStore(std::string path, size_t max_sources) noexcept;

    Status AcquireLock();
    Status PersistLocked() noexcept;
    Status LoadFromDisk() noexcept;

    std::string path_;
    size_t max_sources_ = 0;
    // One store can be shared by the pipelines in a multi-lane deployment.
    mutable std::mutex mutex_;
    int lock_fd_ = -1;
    const DedupStoreTestHooks* test_hooks_ = nullptr;
    uint64_t persistence_count_ = 0;
    DedupStoreStats stats_;
    size_t attached_pipelines_ = 0;
    bool retirement_failed_ = false;
    // Key uses epoch zero; value fences all epochs <= its nonzero boundary.
    std::unordered_map<SourceIdentity, uint64_t, SourceIdentityHash> retired_;
    std::unordered_map<SourceIdentity, uint64_t, SourceIdentityHash> entries_;
};

// Seeds only sources assigned to this lane. The window session must already be
// active. A SeedAccepted failure can leave a partial restore; treat it as fatal.
Status SeedDedupWindowFromStore(DedupWindow* window, DedupStore* store,
                                uint64_t peer_session_epoch,
                                uint64_t now_ns, uint16_t lane_index = 0,
                                uint16_t lane_count = 1) noexcept;

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_DEDUP_STORE_H_
