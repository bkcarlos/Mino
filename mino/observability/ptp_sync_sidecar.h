// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.
//
// pmc/ptp4l-oriented sync-quality publisher for PtpClockClient::PublishSync.
// Documented file interface (fail-closed when sync is absent/stale/unparseable).
//
// File contract (text, UTF-8), schema mino.ptp_sync_quality.v1:
//   First non-comment line must be the schema token, then key=value lines:
//     state=synchronized|unsynchronized|degraded|synchronizing
//     offset_ns=<int64>
//     uncertainty_ns=<uint64>
//     clock_domain_id=<uint32>   # optional; 0 or omitted => client domain
//   '#' starts a comment. Blank lines ignored.
//
// Compact single-line form (also accepted):
//   mino.ptp_sync_quality.v1 state=synchronized offset_ns=-12 uncertainty_ns=40
//
// Operators typically wrap `pmc -u -b 0 'GET TIME_STATUS_NP'` / ptp4l logs into
// this file atomically (temp + rename). Raw pmc text alone never enables
// reporting: TryParsePtpOffsetLine extracts offset magnitude but leaves state
// unsynchronized unless the documented schema marks synchronized with a bound.

#ifndef MINO_OBSERVABILITY_PTP_SYNC_SIDECAR_H_
#define MINO_OBSERVABILITY_PTP_SYNC_SIDECAR_H_

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "mino/common/status.h"
#include "mino/observability/clock.h"
#include "mino/observability/ptp_clock.h"

namespace mino::observability {

inline constexpr std::string_view kPtpSyncQualitySchemaV1 =
    "mino.ptp_sync_quality.v1";

struct PtpSyncQualitySample {
    int64_t estimated_offset_ns = 0;
    uint64_t uncertainty_ns = std::numeric_limits<uint64_t>::max();
    ClockSyncState state = ClockSyncState::kUnsynchronized;
    // Zero means "unspecified": ApplySample uses the bound client's domain.
    uint32_t clock_domain_id = 0;
};

struct PtpSyncSidecarOptions {
    // Absolute path to the quality file written by an operator / pmc wrapper.
    std::string quality_path;
    // Reject samples whose file mtime is older than this (vs CLOCK_REALTIME).
    uint64_t maximum_file_age_ns = 2'000'000'000ull;  // 2s
    // On missing/stale/unparseable input, PublishSync(..., kUnsynchronized).
    bool publish_unsynchronized_on_failure = true;
};

// Parses documented quality text. Returns false without mutating *out on
// failure (fail-closed). Does not consult the filesystem.
bool TryParsePtpSyncQualityText(std::string_view text,
                                PtpSyncQualitySample* out) noexcept;

// Maps common state tokens; unknown tokens return nullopt-equivalent false.
bool TryParseClockSyncStateToken(std::string_view token,
                                 ClockSyncState* out) noexcept;

// Non-owning publisher: reads quality_path and feeds client->PublishSync.
// Missing path, stale mtime, domain mismatch, or parse failure fail closed.
class PtpSyncSidecar {
public:
    PtpSyncSidecar(PtpClockClient* client,
                   PtpSyncSidecarOptions options) noexcept;

    PtpClockClient* client() const noexcept { return client_; }
    const PtpSyncSidecarOptions& options() const noexcept { return options_; }

    // Apply a pre-parsed / injected sample (unit tests; in-process feeds).
    // Domain mismatch or null client => error and no publish.
    Status ApplySample(const PtpSyncQualitySample& sample) noexcept;

    // Read options_.quality_path once and ApplySample / fail-closed.
    Status PollOnce() noexcept;

    // Last successfully applied sample (including fail-closed unsync publishes).
    bool has_last_sample() const noexcept { return has_last_sample_; }
    const PtpSyncQualitySample& last_sample() const noexcept {
        return last_sample_;
    }

private:
    Status FailClosedUnsynchronized(std::string_view reason) noexcept;

    PtpClockClient* client_ = nullptr;
    PtpSyncSidecarOptions options_{};
    bool has_last_sample_ = false;
    PtpSyncQualitySample last_sample_{};
};

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_PTP_SYNC_SIDECAR_H_
