// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_RETRANSMIT_WINDOW_H_
#define MINO_BRIDGE_RETRANSMIT_WINDOW_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "mino/bridge/control_payload.h"
#include "mino/bridge/source_identity.h"
#include "mino/common/result.h"

namespace mino::bridge {

struct RetransmitWindowOptions {
    size_t max_entries = 4096;
    size_t max_bytes = 64u * 1024u * 1024u;
    uint64_t max_age_ns = 30ull * 1000ull * 1000ull * 1000ull;
};

struct RetransmitEntry {
    SourceIdentity source;
    uint64_t sequence = 0;
    uint64_t enqueued_ns = 0;
    std::vector<std::byte> frame;
};

struct RetransmitAckResult {
    size_t removed_entries = 0;
    size_t removed_bytes = 0;
};

struct RetransmitWindowStats {
    uint64_t accepted_acks = 0;
    uint64_t nack_acks = 0;
    uint64_t stale_acks = 0;
    uint64_t expired_entries = 0;
    uint64_t session_switches = 0;
    uint64_t session_discarded_entries = 0;
};

// Single-owner sender-side bounded window. Add copies the complete encoded
// frame, so retransmission never borrows caller or SHM storage.
class RetransmitWindow {
public:
    static Result<std::unique_ptr<RetransmitWindow>> Create(
        RetransmitWindowOptions options = {}) noexcept;

    RetransmitWindow(const RetransmitWindow&) = delete;
    RetransmitWindow& operator=(const RetransmitWindow&) = delete;

    // Returns the number of old-session entries discarded.
    size_t BeginSession(uint64_t local_session_epoch,
                        uint64_t remote_session_epoch,
                        uint64_t now_ns) noexcept;

    Status Add(const SourceIdentity& source, uint64_t sequence,
               std::span<const std::byte> encoded_frame,
               uint64_t now_ns) noexcept;

    // Accepted ACKs remove the observed frame and the cumulative prefix.
    // NACK_WITH_HIGHEST removes only the cumulative prefix; the rejected
    // observed frame remains eligible for retry until expiry.
    Result<RetransmitAckResult> ApplyAck(const AckPayload& ack) noexcept;

    size_t PurgeExpired(uint64_t now_ns) noexcept;
    const RetransmitEntry* Find(const SourceIdentity& source,
                                uint64_t sequence) const noexcept;

    bool session_active() const noexcept { return session_active_; }
    uint64_t local_session_epoch() const noexcept {
        return local_session_epoch_;
    }
    uint64_t remote_session_epoch() const noexcept {
        return remote_session_epoch_;
    }
    size_t size() const noexcept { return entries_.size(); }
    size_t bytes() const noexcept { return bytes_; }
    const std::vector<RetransmitEntry>& entries() const noexcept {
        return entries_;
    }
    const RetransmitWindowStats& stats() const noexcept { return stats_; }

private:
    explicit RetransmitWindow(RetransmitWindowOptions options) noexcept
        : options_(options) {}

    RetransmitWindowOptions options_;
    bool session_active_ = false;
    uint64_t local_session_epoch_ = 0;
    uint64_t remote_session_epoch_ = 0;
    uint64_t session_started_ns_ = 0;
    std::vector<RetransmitEntry> entries_;
    size_t bytes_ = 0;
    RetransmitWindowStats stats_;
};

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_RETRANSMIT_WINDOW_H_
