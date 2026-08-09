// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/dedup_window.h"

#include <algorithm>
#include <limits>
#include <new>
#include <string_view>
#include <utility>

#include "mino/common/status.h"

namespace mino::bridge {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Resource(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

Status Stale() { return Status::Error(StatusCode::kUnavailable); }

bool IsExpired(uint64_t now_ns, uint64_t then_ns,
               uint64_t max_age_ns) noexcept {
    return now_ns >= then_ns && now_ns - then_ns > max_age_ns;
}

}  // namespace

DedupWindow::DedupWindow(DedupWindowOptions options, size_t bitmap_words,
                         size_t source_bytes) noexcept
    : options_(options),
      bitmap_words_(bitmap_words),
      source_bytes_(source_bytes) {}

Result<std::unique_ptr<DedupWindow>> DedupWindow::Create(
    DedupWindowOptions options) noexcept {
    try {
        if (options.max_sources == 0 || options.max_bytes == 0 ||
            options.max_sequence_distance == 0 ||
            options.max_source_age_ns == 0) {
            return Invalid("dedup limits must be nonzero");
        }
        const uint64_t words64 =
            (options.max_sequence_distance - 1) / 64 + 1;
        if (words64 > std::numeric_limits<size_t>::max()) {
            return Resource("dedup bitmap is not addressable");
        }
        const size_t words = static_cast<size_t>(words64);
        if (words >
            (std::numeric_limits<size_t>::max() - sizeof(SourceState)) /
                sizeof(uint64_t)) {
            return Resource("dedup source state size overflows");
        }
        const size_t source_bytes =
            sizeof(SourceState) + words * sizeof(uint64_t);
        if (source_bytes > options.max_bytes) {
            return Resource(
                "dedup byte limit cannot hold one logical source state");
        }
        auto window = std::unique_ptr<DedupWindow>(
            new DedupWindow(options, words, source_bytes));
        const size_t max_indexed_sources =
            std::min(options.max_sources, options.max_bytes / source_bytes);
        if (max_indexed_sources == std::numeric_limits<size_t>::max()) {
            return Resource("dedup source index size overflows");
        }
        // The index is bounded by logical source capacity but is not charged to
        // max_bytes. reserve() preallocates buckets, not per-source map nodes.
        // One transient slot lets AddSource insert before eviction so allocation
        // failure cannot change existing retained state.
        window->source_index_.reserve(max_indexed_sources + 1);
        return window;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

void DedupWindow::BeginSession(uint64_t peer_session_epoch,
                               uint64_t now_ns,
                               bool preserve_state) noexcept {
    if (session_active_ && peer_session_epoch_ == peer_session_epoch) return;
    if (session_active_) ++stats_.session_switches;
    if (!preserve_state) {
        entries_.clear();
        source_index_.clear();
        bytes_ = 0;
    }
    session_active_ = true;
    peer_session_epoch_ = peer_session_epoch;
    session_started_ns_ = now_ns;
}

DedupWindow::SourceState* DedupWindow::Find(
    const SourceIdentity& source) noexcept {
    const auto it = source_index_.find(source);
    return it == source_index_.end() ? nullptr : &entries_[it->second];
}

const DedupWindow::SourceState* DedupWindow::Find(
    const SourceIdentity& source) const noexcept {
    const auto it = source_index_.find(source);
    return it == source_index_.end() ? nullptr : &entries_[it->second];
}

void DedupWindow::RebuildIndex() noexcept {
    // Erasure shifts vector positions. Retained map nodes already exist, so
    // refreshing their indices cannot allocate or invalidate noexcept callers.
    for (size_t i = 0; i < entries_.size(); ++i) {
        const auto it = source_index_.find(entries_[i].source);
        if (it != source_index_.end()) it->second = i;
    }
}

void DedupWindow::EraseEntry(size_t index) noexcept {
    source_index_.erase(entries_[index].source);
    entries_.erase(entries_.begin() + static_cast<std::ptrdiff_t>(index));
    bytes_ -= source_bytes_;
    RebuildIndex();
}

bool DedupWindow::GapSet(const SourceState& state,
                         uint64_t distance) const noexcept {
    const uint64_t bit = distance - 1;
    const size_t word = static_cast<size_t>(bit / 64);
    const uint32_t offset = static_cast<uint32_t>(bit % 64);
    return (state.gap_words[word] & (uint64_t{1} << offset)) != 0;
}

void DedupWindow::SetGap(SourceState& state, uint64_t distance) noexcept {
    const uint64_t bit = distance - 1;
    const size_t word = static_cast<size_t>(bit / 64);
    const uint32_t offset = static_cast<uint32_t>(bit % 64);
    state.gap_words[word] |= uint64_t{1} << offset;
}

DedupCheckResult DedupWindow::Classify(const SourceState& state,
                                       uint64_t sequence) const noexcept {
    if (sequence <= state.highest_contiguous_sequence) {
        const uint64_t age = state.highest_contiguous_sequence - sequence;
        if (age < options_.max_sequence_distance) {
            return {DedupDecision::kDuplicateAccepted,
                    state.highest_contiguous_sequence};
        }
        return {DedupDecision::kNackWithHighest,
                state.highest_contiguous_sequence};
    }
    const uint64_t distance = sequence - state.highest_contiguous_sequence;
    if (distance > options_.max_sequence_distance) {
        return {DedupDecision::kNackWithHighest,
                state.highest_contiguous_sequence};
    }
    if (GapSet(state, distance)) {
        return {DedupDecision::kDuplicateAccepted,
                state.highest_contiguous_sequence};
    }
    return {DedupDecision::kAccept, state.highest_contiguous_sequence};
}

Result<DedupCheckResult> DedupWindow::Check(
    uint64_t peer_session_epoch, const SourceIdentity& source,
    uint64_t sequence, uint64_t now_ns) noexcept {
    try {
        if (!session_active_ || peer_session_epoch != peer_session_epoch_) {
            ++stats_.stale_session_checks;
            return DedupCheckResult{DedupDecision::kStaleSession, std::nullopt};
        }
        if (sequence == 0) return Invalid("sequence zero is not reliable data");
        PurgeExpired(now_ns);
        SourceState* state = Find(source);
        if (state == nullptr) {
            if (sequence > options_.max_sequence_distance) {
                ++stats_.nack_checks;
                ++stats_.gap_events;
                return DedupCheckResult{DedupDecision::kNackWithHighest, 0};
            }
            ++stats_.accepted_checks;
            if (sequence > 1) {
                ++stats_.gap_events;
            }
            return DedupCheckResult{DedupDecision::kAccept, 0};
        }
        state->last_activity_ns = now_ns;
        DedupCheckResult result = Classify(*state, sequence);
        switch (result.decision) {
            case DedupDecision::kAccept:
                ++stats_.accepted_checks;
                if (sequence > state->highest_contiguous_sequence + 1) {
                    ++stats_.gap_events;
                }
                break;
            case DedupDecision::kDuplicateAccepted:
                ++stats_.duplicate_checks;
                break;
            case DedupDecision::kNackWithHighest:
                ++stats_.nack_checks;
                ++stats_.gap_events;
                break;
            case DedupDecision::kStaleSession:
                break;
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Status DedupWindow::AddSource(const SourceIdentity& source, uint64_t highest,
                              uint64_t now_ns) noexcept {
    try {
        SourceState state;
        state.source = source;
        state.highest_contiguous_sequence = highest;
        state.last_activity_ns = now_ns;
        state.gap_words.assign(bitmap_words_, 0);
        entries_.push_back(std::move(state));
        try {
            const bool inserted =
                source_index_.emplace(source, entries_.size() - 1).second;
            if (!inserted) {
                entries_.pop_back();
                return Invalid("dedup source already exists");
            }
        } catch (const std::bad_alloc&) {
            entries_.pop_back();
            return Status::Error(StatusCode::kResourceExhausted);
        } catch (const std::length_error&) {
            entries_.pop_back();
            return Status::Error(StatusCode::kResourceExhausted);
        }
        EvictToLimits();
        bytes_ += source_bytes_;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

void DedupWindow::AdvanceContiguous(SourceState& state) noexcept {
    while ((state.gap_words[0] & uint64_t{1}) != 0) {
        for (size_t i = 0; i < state.gap_words.size(); ++i) {
            const uint64_t carry =
                i + 1 < state.gap_words.size()
                    ? (state.gap_words[i + 1] & uint64_t{1}) << 63
                    : 0;
            state.gap_words[i] = (state.gap_words[i] >> 1) | carry;
        }
        ++state.highest_contiguous_sequence;
    }
}

Status DedupWindow::PrepareSource(uint64_t peer_session_epoch,
                                  const SourceIdentity& source,
                                  uint64_t now_ns) noexcept {
    if (!session_active_ || peer_session_epoch != peer_session_epoch_) {
        return Stale();
    }
    PurgeExpired(now_ns);
    SourceState* state = Find(source);
    if (state != nullptr) {
        state->last_activity_ns = now_ns;
        return Status::Ok();
    }
    return AddSource(source, 0, now_ns);
}

bool DedupWindow::HasSource(const SourceIdentity& source) const noexcept {
    return Find(source) != nullptr;
}

Status DedupWindow::CommitAccepted(uint64_t peer_session_epoch,
                                   const SourceIdentity& source,
                                   uint64_t sequence,
                                   uint64_t now_ns) noexcept {
    try {
        if (!session_active_ || peer_session_epoch != peer_session_epoch_) {
            return Stale();
        }
        if (sequence == 0) return Invalid("sequence zero is not reliable data");
        PurgeExpired(now_ns);
        SourceState* state = Find(source);
        if (state == nullptr) {
            if (sequence > options_.max_sequence_distance) {
                return Invalid("sequence is outside the dedup window");
            }
            const Status added = AddSource(source, 0, now_ns);
            if (!added.ok()) {
                return added;
            }
            state = Find(source);
            SetGap(*state, sequence);
            AdvanceContiguous(*state);
            return Status::Ok();
        }

        const DedupCheckResult check = Classify(*state, sequence);
        if (check.decision == DedupDecision::kDuplicateAccepted) {
            state->last_activity_ns = now_ns;
            return Status::Ok();
        }
        if (check.decision != DedupDecision::kAccept) {
            return Invalid("sequence is outside the dedup window");
        }
        const uint64_t distance = sequence - state->highest_contiguous_sequence;
        SetGap(*state, distance);
        AdvanceContiguous(*state);
        state->last_activity_ns = now_ns;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Status DedupWindow::SeedAccepted(uint64_t peer_session_epoch,
                                 const SourceIdentity& source,
                                 uint64_t highest_contiguous_sequence,
                                 uint64_t now_ns) noexcept {
    try {
        if (!session_active_ || peer_session_epoch != peer_session_epoch_) {
            return Stale();
        }
        PurgeExpired(now_ns);
        SourceState* state = Find(source);
        if (state == nullptr) {
            return AddSource(source, highest_contiguous_sequence, now_ns);
        }
        state->highest_contiguous_sequence = highest_contiguous_sequence;
        std::fill(state->gap_words.begin(), state->gap_words.end(), 0);
        state->last_activity_ns = now_ns;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<std::vector<DedupResumeEntry>>
DedupWindow::SnapshotAccepted() const noexcept {
    try {
        std::vector<DedupResumeEntry> snapshot;
        snapshot.reserve(entries_.size());
        for (const SourceState& state : entries_) {
            snapshot.push_back(DedupResumeEntry{
                .source = state.source,
                .highest_contiguous_sequence =
                    state.highest_contiguous_sequence,
            });
        }
        std::sort(snapshot.begin(), snapshot.end(),
                  [](const DedupResumeEntry& left,
                     const DedupResumeEntry& right) {
                      if (left.source.node_id != right.source.node_id) {
                          return left.source.node_id < right.source.node_id;
                      }
                      if (left.source.publisher_id !=
                          right.source.publisher_id) {
                          return left.source.publisher_id <
                                 right.source.publisher_id;
                      }
                      return left.source.publisher_epoch <
                             right.source.publisher_epoch;
                  });
        return snapshot;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

size_t DedupWindow::PurgeExpired(uint64_t now_ns) noexcept {
    size_t removed = 0;
    for (size_t i = 0; i < entries_.size();) {
        if (!IsExpired(now_ns, entries_[i].last_activity_ns,
                       options_.max_source_age_ns)) {
            ++i;
            continue;
        }
        EraseEntry(i);
        ++removed;
        ++stats_.age_evictions;
    }
    return removed;
}

void DedupWindow::EvictToLimits() noexcept {
    // The last entry has been allocated but is not yet included in bytes_. Make
    // room using subtraction-based checks, then AddSource can account it without
    // any size_t addition overflow.
    while (entries_.size() > options_.max_sources ||
           bytes_ > options_.max_bytes - source_bytes_) {
        // Evict the least recently active old entry so a successful commit
        // always retains the frame just accepted.
        size_t oldest = 0;
        const size_t old_count = entries_.size() - 1;
        for (size_t i = 1; i < old_count; ++i) {
            if (entries_[i].last_activity_ns <
                entries_[oldest].last_activity_ns) {
                oldest = i;
            }
        }
        EraseEntry(oldest);
        ++stats_.source_evictions;
    }
}

}  // namespace mino::bridge
