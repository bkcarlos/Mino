// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_DEDUP_STORE_H_
#define MINO_BRIDGE_DEDUP_STORE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "mino/bridge/dedup_window.h"
#include "mino/bridge/source_identity.h"
#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino::bridge {

// Durable host-local map of SourceIdentity -> highest_contiguous_sequence.
// Only the cumulative HWM is persisted (matching SessionHello); in-flight gap
// bitmap state remains memory-only and may be re-accepted after restart
// (at-least-once). Crash-safe updates use write + fsync + rename + directory
// fsync. Open fails closed on truncated or corrupt payloads.
struct DedupStoreOptions {
    // Required absolute or relative path to the durable snapshot file. The
    // parent directory is created with mode 0700 when missing.
    std::string path;
    // Bounds the number of retained sources. Exceeding the limit fails closed.
    size_t max_sources = 1024;
};

class DedupStore {
public:
    // Opens an existing snapshot or creates an empty store. A missing file is
    // treated as empty (not corruption). Truncation, bad magic/version, or a
    // CRC mismatch returns kCorruption.
    static Result<std::unique_ptr<DedupStore>> Open(
        DedupStoreOptions options) noexcept;

    DedupStore(const DedupStore&) = delete;
    DedupStore& operator=(const DedupStore&) = delete;

    // Sorted snapshot matching DedupWindow::SnapshotAccepted ordering.
    Result<std::vector<DedupResumeEntry>> Load() const noexcept;

    // Monotonic upsert of the durable HWM for one source. Persists before
    // returning OK. Incomplete identities and zero sequences are rejected.
    Status RecordAccepted(const SourceIdentity& source,
                          uint64_t highest_contiguous_sequence) noexcept;

    // Atomically replaces the entire durable snapshot. Empty is allowed.
    Status ReplaceAll(const std::vector<DedupResumeEntry>& entries) noexcept;

    const std::string& path() const noexcept { return path_; }
    size_t size() const noexcept { return entries_.size(); }
    size_t max_sources() const noexcept { return max_sources_; }

private:
    DedupStore(std::string path, size_t max_sources) noexcept;

    Status PersistLocked() noexcept;
    Status LoadFromDisk() noexcept;

    std::string path_;
    size_t max_sources_ = 0;
    std::unordered_map<SourceIdentity, uint64_t, SourceIdentityHash> entries_;
};

// Seeds a DedupWindow from a durable store. The window session must already be
// active. Failures leave the window unchanged only when SeedAccepted itself
// fails mid-loop (caller should treat that as fatal).
Status SeedDedupWindowFromStore(DedupWindow* window, DedupStore* store,
                                uint64_t peer_session_epoch,
                                uint64_t now_ns) noexcept;

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_DEDUP_STORE_H_
