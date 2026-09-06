// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.
//
// PTP/PHC (or configured clock_gettime) client for cross-host one-way latency.
// Fail-closed: without a configured, synchronized quality publication, reporting
// stays disabled and CrossNodeLatencyRecorder rejects samples as uncertain.

#ifndef MINO_OBSERVABILITY_PTP_CLOCK_H_
#define MINO_OBSERVABILITY_PTP_CLOCK_H_

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/observability/clock.h"

namespace mino::observability {

// How the client obtains timestamps. PHC device paths use Linux FD_TO_CLOCKID.
enum class PtpClockBackend : uint8_t {
    kUnconfigured = 0,
    kClockGettime = 1,
    kPhcDevice = 2,
};

struct PtpClockClientOptions {
    // Absolute path such as "/dev/ptp0". Empty selects clock_gettime backend
    // when clock_id is set, otherwise Create fails closed.
    std::string phc_device_path;
    // Used when phc_device_path is empty. Must be a valid POSIX clock id
    // (for example CLOCK_REALTIME). Zero with empty path is invalid.
    // CLOCK_REALTIME is typically 0 on Linux; set clock_id_explicit when using 0.
    int clock_id = 0;
    bool clock_id_explicit = false;

    uint32_t clock_domain_id = 0;
    // Samples older than this age (vs last sync publication) are uncertain.
    uint64_t maximum_sync_age_ns = 1'000'000'000ull;  // 1s
    // Upper bound accepted by CrossNodeLatencyRecorder consumers.
    uint64_t maximum_uncertainty_ns = 1'000'000ull;  // 1ms

    // Optional operator-provided bound when an out-of-process ptp4l/pmc feed is
    // not yet attached. Remains unsynchronized until PublishSync() or until
    // this is set with assume_synchronized=true (lab-only; not a PTP contract).
    uint64_t initial_uncertainty_ns =
        std::numeric_limits<uint64_t>::max();
    bool assume_synchronized = false;
    int64_t assumed_offset_ns = 0;
};

// Reads a configured PHC or clockid and publishes AtomicClockQuality. Cross-host
// one-way latency reporting is enabled only while quality is kSynchronized,
// domain matches, and uncertainty/age thresholds hold.
class PtpClockClient {
public:
    static Result<PtpClockClient> Create(
        const PtpClockClientOptions& options) noexcept;

    PtpClockClient(PtpClockClient&& other) noexcept;
    PtpClockClient& operator=(PtpClockClient&& other) noexcept;
    PtpClockClient(const PtpClockClient&) = delete;
    PtpClockClient& operator=(const PtpClockClient&) = delete;
    ~PtpClockClient();

    PtpClockBackend backend() const noexcept { return backend_; }
    const std::string& provenance() const noexcept { return provenance_; }
    uint32_t clock_domain_id() const noexcept { return options_.clock_domain_id; }

    // Sample the configured clock into wall_ns. Does not change sync state.
    Result<uint64_t> NowWallNs() const noexcept;
    Result<uint64_t> NowMonotonicNs() const noexcept;

    // Operator / ptp4l-sidecar feed. Empty PublishSync with unsynchronized state
    // disables reporting. Refresh() samples the clock and refreshes last_sync
    // age bookkeeping when already synchronized.
    Status PublishSync(int64_t estimated_offset_ns, uint64_t uncertainty_ns,
                       ClockSyncState state) noexcept;
    Status Refresh() noexcept;

    bool TryLoadQuality(ClockQuality* quality,
                        uint32_t max_attempts = 4) const noexcept;
    ClockQuality LatestQualityOrUncertain() const noexcept;

    // True only when quality is synchronized within configured thresholds for
    // the client's clock_domain_id. Callers must still pass the same domain in
    // TraceContext when recording.
    bool AllowsCrossNodeOneWayReporting() const noexcept;

    // Convenience: Record only when AllowsCrossNodeOneWayReporting(); otherwise
    // returns kClockUncertain without mutating the histogram (still counts
    // uncertain via Record's own path when quality is loaded).
    template <size_t Shards = kMetricShards>
    LatencySampleDecision TryRecord(
        CrossNodeLatencyRecorder<Shards>* recorder, const TraceContext& context,
        size_t shard) const noexcept {
        if (recorder == nullptr) {
            return LatencySampleDecision::kClockUncertain;
        }
        const ClockQuality quality = LatestQualityOrUncertain();
        const auto wall = NowWallNs();
        const auto mono = NowMonotonicNs();
        if (!wall.ok() || !mono.ok()) {
            return LatencySampleDecision::kClockUncertain;
        }
        return recorder->Record(context, *wall, *mono, quality, shard);
    }

private:
    PtpClockClient(PtpClockClientOptions options, PtpClockBackend backend,
                   int clock_fd, int clock_id, std::string provenance) noexcept;

    Status SampleClockNs(int clock_id, uint64_t* out_ns) const noexcept;
    void StoreQuality(const ClockQuality& quality) noexcept;

    PtpClockClientOptions options_{};
    PtpClockBackend backend_ = PtpClockBackend::kUnconfigured;
    int clock_fd_ = -1;
    int clock_id_ = 0;
    std::string provenance_;
    AtomicClockQuality quality_{};
};

// Parses a small subset of pmc/ptp4l-style "offset / rms" text into sync fields.
// Accepts lines containing "offset" and an integer nanosecond value. Returns
// false without mutating outputs when parsing fails (fail-closed).
bool TryParsePtpOffsetLine(std::string_view line, int64_t* offset_ns,
                           uint64_t* uncertainty_ns) noexcept;

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_PTP_CLOCK_H_
