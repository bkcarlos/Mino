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
        return std::unique_ptr<RetransmitWindow>(
            new RetransmitWindow(options));
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
        if (Find(source, sequence) != nullptr) {
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
        entries_.push_back(std::move(entry));
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
        for (size_t i = 0; i < entries_.size();) {
            const RetransmitEntry& entry = entries_[i];
            const bool same_source = entry.source == ack.source;
            const bool cumulative =
                ack.highest_contiguous_sequence.has_value() &&
                entry.sequence <= highest;
            const bool accepted_observed =
                ack.disposition == AckDisposition::kAccepted &&
                entry.sequence == ack.observed_sequence;
            if (!same_source || (!cumulative && !accepted_observed)) {
                ++i;
                continue;
            }
            result.removed_bytes += entry.frame.size();
            ++result.removed_entries;
            bytes_ -= entry.frame.size();
            entries_.erase(entries_.begin() +
                           static_cast<std::ptrdiff_t>(i));
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

size_t RetransmitWindow::PurgeExpired(uint64_t now_ns) noexcept {
    size_t removed = 0;
    for (size_t i = 0; i < entries_.size();) {
        if (!IsExpired(now_ns, entries_[i].enqueued_ns,
                       options_.max_age_ns)) {
            ++i;
            continue;
        }
        bytes_ -= entries_[i].frame.size();
        entries_.erase(entries_.begin() + static_cast<std::ptrdiff_t>(i));
        ++removed;
        ++stats_.expired_entries;
    }
    return removed;
}

const RetransmitEntry* RetransmitWindow::Find(
    const SourceIdentity& source, uint64_t sequence) const noexcept {
    const auto it = std::find_if(
        entries_.begin(), entries_.end(),
        [&source, sequence](const RetransmitEntry& entry) {
            return entry.source == source && entry.sequence == sequence;
        });
    return it == entries_.end() ? nullptr : &*it;
}

}  // namespace mino::bridge
