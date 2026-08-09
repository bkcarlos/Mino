// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_DEDUP_WINDOW_H_
#define MINO_BRIDGE_DEDUP_WINDOW_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "mino/bridge/source_identity.h"
#include "mino/common/result.h"

namespace mino::bridge {

struct DedupWindowOptions {
    size_t max_sources = 1024;
    // Bounds logical retained per-source state, including each source's gap
    // bitmap. It excludes container spare capacity, the lookup index, allocator
    // metadata, and transient allocation while replacing an entry.
    size_t max_bytes = 1024u * 1024u;
    uint64_t max_sequence_distance = 4096;
    uint64_t max_source_age_ns = 60ull * 1000ull * 1000ull * 1000ull;
};

enum class DedupDecision : uint8_t {
    kAccept = 0,
    kDuplicateAccepted = 1,
    kNackWithHighest = 2,
    kStaleSession = 3,
};

struct DedupCheckResult {
    DedupDecision decision = DedupDecision::kAccept;
    std::optional<uint64_t> highest_contiguous_sequence;
};

struct DedupResumeEntry {
    SourceIdentity source;
    uint64_t highest_contiguous_sequence = 0;
};

struct DedupWindowStats {
    uint64_t accepted_checks = 0;
    uint64_t duplicate_checks = 0;
    uint64_t nack_checks = 0;
    uint64_t stale_session_checks = 0;
    uint64_t gap_events = 0;
    uint64_t source_evictions = 0;
    uint64_t age_evictions = 0;
    uint64_t session_switches = 0;
};

// Single-owner receiver-side deduplication cache. Check never records a frame
// as accepted; CommitAccepted must be called only after the remote publish path
// has taken ownership. The class is intentionally not internally synchronized.
class DedupWindow {
public:
    static Result<std::unique_ptr<DedupWindow>> Create(
        DedupWindowOptions options = {}) noexcept;

    DedupWindow(const DedupWindow&) = delete;
    DedupWindow& operator=(const DedupWindow&) = delete;

    // A changed peer epoch fences old traffic. State is discarded by default;
    // a normal transport reconnect may explicitly preserve it, while receiver
    // restart must leave preserve_state false and advertise degraded delivery.
    // Repeating the active epoch is idempotent.
    void BeginSession(uint64_t peer_session_epoch, uint64_t now_ns,
                      bool preserve_state = false) noexcept;

    Result<DedupCheckResult> Check(uint64_t peer_session_epoch,
                                  const SourceIdentity& source,
                                  uint64_t sequence,
                                  uint64_t now_ns) noexcept;

    // Preallocates a zero-HWM source state before the local publication
    // linearization point, so CommitAccepted cannot first allocate afterward.
    Status PrepareSource(uint64_t peer_session_epoch,
                         const SourceIdentity& source,
                         uint64_t now_ns) noexcept;
    bool HasSource(const SourceIdentity& source) const noexcept;

    Status CommitAccepted(uint64_t peer_session_epoch,
                          const SourceIdentity& source, uint64_t sequence,
                          uint64_t now_ns) noexcept;

    // Seeds
    // New sources begin at cumulative HWM zero; accepted out-of-order frames
    // are retained in the gap bitmap until the prefix beginning at one closes.
    // Seeds a cumulative HWM obtained from a trusted handshake or persistent
    // dedup store. The supplied epoch must be the active session.
    Status SeedAccepted(uint64_t peer_session_epoch,
                        const SourceIdentity& source,
                        uint64_t highest_contiguous_sequence,
                        uint64_t now_ns) noexcept;

    Result<std::vector<DedupResumeEntry>> SnapshotAccepted() const noexcept;
    size_t PurgeExpired(uint64_t now_ns) noexcept;

    bool session_active() const noexcept { return session_active_; }
    uint64_t peer_session_epoch() const noexcept { return peer_session_epoch_; }
    size_t source_count() const noexcept { return entries_.size(); }
    // Logical retained source-state bytes charged against max_bytes.
    size_t bytes() const noexcept { return bytes_; }
    const DedupWindowStats& stats() const noexcept { return stats_; }

private:
    struct SourceState {
        SourceIdentity source;
        uint64_t highest_contiguous_sequence = 0;
        uint64_t last_activity_ns = 0;
        std::vector<uint64_t> gap_words;
    };

    DedupWindow(DedupWindowOptions options, size_t bitmap_words,
                size_t source_bytes) noexcept;

    SourceState* Find(const SourceIdentity& source) noexcept;
    const SourceState* Find(const SourceIdentity& source) const noexcept;
    void EraseEntry(size_t index) noexcept;
    void RebuildIndex() noexcept;
    DedupCheckResult Classify(const SourceState& state,
                              uint64_t sequence) const noexcept;
    Status AddSource(const SourceIdentity& source, uint64_t highest,
                     uint64_t now_ns) noexcept;
    void SetGap(SourceState& state, uint64_t distance) noexcept;
    bool GapSet(const SourceState& state, uint64_t distance) const noexcept;
    void AdvanceContiguous(SourceState& state) noexcept;
    void EvictToLimits() noexcept;

    DedupWindowOptions options_;
    size_t bitmap_words_ = 0;
    size_t source_bytes_ = 0;
    bool session_active_ = false;
    uint64_t peer_session_epoch_ = 0;
    uint64_t session_started_ns_ = 0;
    std::vector<SourceState> entries_;
    std::unordered_map<SourceIdentity, size_t, SourceIdentityHash> source_index_;
    size_t bytes_ = 0;
    DedupWindowStats stats_;
};

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_DEDUP_WINDOW_H_
