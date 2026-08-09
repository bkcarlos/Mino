// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/retransmit_window.h"

#include <algorithm>
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

bool IsExpired(uint64_t now_ns, uint64_t then_ns,
               uint64_t max_age_ns) noexcept {
    return now_ns >= then_ns && now_ns - then_ns > max_age_ns;
}

}  // namespace

Result<std::unique_ptr<RetransmitWindow>> RetransmitWindow::Create(
    RetransmitWindowOptions options) noexcept {
    try {
        if (options.max_entries == 0 || options.max_bytes == 0 ||
            options.max_age_ns == 0) {
            return Invalid("retransmit limits must be nonzero");
        }
        auto window = std::unique_ptr<RetransmitWindow>(
            new RetransmitWindow(options));
        window->entries_.reserve(options.max_entries);
        window->entry_index_.reserve(options.max_entries);
        window->source_index_.reserve(options.max_entries);
        return window;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

size_t RetransmitWindow::BeginSession(uint64_t local_session_epoch,
                                      uint64_t remote_session_epoch,
                                      uint64_t now_ns) noexcept {
    if (session_active_ && local_session_epoch_ == local_session_epoch &&
        remote_session_epoch_ == remote_session_epoch) {
        return 0;
    }
    if (session_active_) {
        ++stats_.session_switches;
    }
    // Encoded frames are owned independently of a transport connection and
    // intentionally survive reconnect. Resume ACK state trims them after the
    // new session handshake; discarding here would create a data-loss window.
    session_active_ = true;
    local_session_epoch_ = local_session_epoch;
    remote_session_epoch_ = remote_session_epoch;
    session_started_ns_ = now_ns;
    return 0;
}

Status RetransmitWindow::Add(const SourceIdentity& source, uint64_t sequence,
                             std::span<const std::byte> encoded_frame,
                             uint64_t now_ns) noexcept {
    try {
        if (!session_active_) return Invalid("retransmit session is not active");
        if (sequence == 0) return Invalid("sequence zero is not reliable data");
        if (encoded_frame.empty()) return Invalid("encoded frame is empty");
        if (encoded_frame.size() > options_.max_bytes) {
            return Resource("encoded frame exceeds retransmit byte limit");
        }
        const EntryKey key{source, sequence};
        if (entry_index_.contains(key)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "retransmit entry already exists");
        }
        if (entries_.size() >= options_.max_entries) {
            return Resource("retransmit entry limit reached");
        }
        if (bytes_ > options_.max_bytes - encoded_frame.size()) {
            return Resource("retransmit byte limit reached");
        }

        RetransmitEntry entry;
        entry.source = source;
        entry.sequence = sequence;
        entry.enqueued_ns = now_ns;
        entry.frame.assign(encoded_frame.begin(), encoded_frame.end());

        auto source_position = source_index_.end();
        bool source_created = false;
        bool sequence_inserted = false;
        bool key_inserted = false;
        try {
            auto source_result = source_index_.try_emplace(source);
            source_position = source_result.first;
            source_created = source_result.second;
            sequence_inserted =
                source_position->second.emplace(sequence, entries_.size())
                    .second;
            if (!sequence_inserted) {
                if (source_created) source_index_.erase(source_position);
                return Status::Error(StatusCode::kAlreadyExists,
                                     "retransmit entry already exists");
            }
            key_inserted =
                entry_index_.emplace(key, entries_.size()).second;
            if (!key_inserted) {
                source_position->second.erase(sequence);
                if (source_created) source_index_.erase(source_position);
                return Status::Error(StatusCode::kAlreadyExists,
                                     "retransmit entry already exists");
            }
            entries_.push_back(std::move(entry));
        } catch (const std::bad_alloc&) {
            if (key_inserted) entry_index_.erase(key);
            if (sequence_inserted) {
                source_position->second.erase(sequence);
            }
            if (source_created && source_position != source_index_.end() &&
                source_position->second.empty()) {
                source_index_.erase(source_position);
            }
            return Status::Error(StatusCode::kResourceExhausted);
        } catch (const std::length_error&) {
            if (key_inserted) entry_index_.erase(key);
            if (sequence_inserted) {
                source_position->second.erase(sequence);
            }
            if (source_created && source_position != source_index_.end() &&
                source_position->second.empty()) {
                source_index_.erase(source_position);
            }
            return Status::Error(StatusCode::kResourceExhausted);
        }
        bytes_ += encoded_frame.size();
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<RetransmitAckResult> RetransmitWindow::ApplyAck(
    const AckPayload& ack) noexcept {
    try {
        if (!session_active_ ||
            ack.receiver_session_epoch != local_session_epoch_ ||
            ack.sender_session_epoch != remote_session_epoch_) {
            ++stats_.stale_acks;
            return Status::Error(StatusCode::kUnavailable);
        }
        if (ack.disposition != AckDisposition::kAccepted &&
            ack.disposition != AckDisposition::kNackWithHighest) {
            return Invalid("ACK disposition is unknown");
        }
        if (ack.disposition == AckDisposition::kNackWithHighest &&
            !ack.highest_contiguous_sequence.has_value()) {
            return Invalid("NACK_WITH_HIGHEST requires highest sequence");
        }

        RetransmitAckResult result;
        const uint64_t highest = ack.highest_contiguous_sequence.value_or(0);
        if (ack.highest_contiguous_sequence.has_value()) {
            while (true) {
                const auto source = source_index_.find(ack.source);
                if (source == source_index_.end() ||
                    source->second.empty() ||
                    source->second.begin()->first > highest) {
                    break;
                }
                RemoveAt(source->second.begin()->second, &result);
            }
        }
        if (ack.disposition == AckDisposition::kAccepted &&
            ack.observed_sequence > highest) {
            const auto observed = entry_index_.find(
                EntryKey{ack.source, ack.observed_sequence});
            if (observed != entry_index_.end()) {
                RemoveAt(observed->second, &result);
            }
        }
        if (ack.disposition == AckDisposition::kAccepted) {
            ++stats_.accepted_acks;
        } else {
            ++stats_.nack_acks;
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

void RetransmitWindow::RemoveAt(size_t index,
                                RetransmitAckResult* result) noexcept {
    const EntryKey removed_key{entries_[index].source, entries_[index].sequence};
    const size_t removed_bytes = entries_[index].frame.size();
    bytes_ -= removed_bytes;
    if (result != nullptr) {
        ++result->removed_entries;
        result->removed_bytes += removed_bytes;
    }

    entry_index_.erase(removed_key);
    const auto removed_source = source_index_.find(removed_key.source);
    if (removed_source != source_index_.end()) {
        removed_source->second.erase(removed_key.sequence);
        if (removed_source->second.empty()) {
            source_index_.erase(removed_source);
        }
    }

    const size_t last = entries_.size() - 1;
    if (index != last) {
        entries_[index] = std::move(entries_[last]);
        const EntryKey moved_key{entries_[index].source,
                                 entries_[index].sequence};
        const auto moved = entry_index_.find(moved_key);
        if (moved != entry_index_.end()) moved->second = index;
        const auto moved_source = source_index_.find(moved_key.source);
        if (moved_source != source_index_.end()) {
            const auto moved_sequence =
                moved_source->second.find(moved_key.sequence);
            if (moved_sequence != moved_source->second.end()) {
                moved_sequence->second = index;
            }
        }
    }
    entries_.pop_back();
}

size_t RetransmitWindow::PurgeExpired(uint64_t now_ns) noexcept {
    size_t removed = 0;
    for (size_t i = 0; i < entries_.size();) {
        if (!IsExpired(now_ns, entries_[i].enqueued_ns,
                       options_.max_age_ns)) {
            ++i;
            continue;
        }
        RemoveAt(i);
        ++removed;
        ++stats_.expired_entries;
    }
    return removed;
}

const RetransmitEntry* RetransmitWindow::Find(
    const SourceIdentity& source, uint64_t sequence) const noexcept {
    const auto found = entry_index_.find(EntryKey{source, sequence});
    return found == entry_index_.end() ? nullptr : &entries_[found->second];
}

}  // namespace mino::bridge
