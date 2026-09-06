// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/ptp_clock.h"

#include <fcntl.h>
#include <time.h>
#include <unistd.h>

#include <cctype>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <utility>

#if defined(__linux__)
#include <linux/ptp_clock.h>
#include <sys/ioctl.h>
#endif

namespace mino::observability {
namespace {

#if defined(__linux__)
#ifndef CLOCKFD
#define CLOCKFD 3
#endif
#ifndef FD_TO_CLOCKID
#define FD_TO_CLOCKID(fd) \
    ((clockid_t)((((unsigned int)~(fd)) << 3) | CLOCKFD))
#endif
#endif

Status Unavailable(std::string_view message) {
    return Status::Error(StatusCode::kUnavailable, message);
}
Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status SampleTimespec(clockid_t clock_id, uint64_t* out_ns) {
    if (out_ns == nullptr) return Invalid("clock sample output is null");
    timespec ts{};
    if (::clock_gettime(clock_id, &ts) != 0) {
        return Unavailable("clock_gettime failed for configured clock");
    }
    if (ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= 1'000'000'000L) {
        return Status::Error(StatusCode::kCorruption,
                             "clock_gettime returned an invalid timespec");
    }
    const uint64_t sec = static_cast<uint64_t>(ts.tv_sec);
    const uint64_t nsec = static_cast<uint64_t>(ts.tv_nsec);
    if (sec > (UINT64_MAX - nsec) / 1'000'000'000ull) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "clock sample overflowed uint64 nanoseconds");
    }
    *out_ns = sec * 1'000'000'000ull + nsec;
    return Status::Ok();
}

bool PathIsAbsolute(std::string_view path) {
    return !path.empty() && path.front() == '/';
}

}  // namespace

Result<PtpClockClient> PtpClockClient::Create(
    const PtpClockClientOptions& options) noexcept {
    try {
        if (options.clock_domain_id == 0) {
            return Invalid("PTP clock_domain_id must be non-zero");
        }
        if (options.maximum_uncertainty_ns == 0 ||
            options.maximum_sync_age_ns == 0) {
            return Invalid("PTP quality thresholds must be non-zero");
        }

        PtpClockBackend backend = PtpClockBackend::kUnconfigured;
        int clock_fd = -1;
        int clock_id = 0;
        std::string provenance;

        if (!options.phc_device_path.empty()) {
#if defined(__linux__)
            if (!PathIsAbsolute(options.phc_device_path)) {
                return Invalid("PHC device path must be absolute");
            }
            clock_fd =
                ::open(options.phc_device_path.c_str(), O_RDONLY | O_CLOEXEC);
            if (clock_fd < 0) {
                return Unavailable(
                    "configured PHC device could not be opened");
            }
#if defined(PTP_CLOCK_GETCAPS)
            ptp_clock_caps caps{};
            if (::ioctl(clock_fd, PTP_CLOCK_GETCAPS, &caps) != 0) {
                ::close(clock_fd);
                return Unavailable(
                    "configured path is not a usable PTP clock device");
            }
#endif
            clock_id = static_cast<int>(FD_TO_CLOCKID(clock_fd));
            uint64_t probe = 0;
            const Status probe_status = SampleTimespec(clock_id, &probe);
            if (!probe_status.ok()) {
                ::close(clock_fd);
                return probe_status;
            }
            backend = PtpClockBackend::kPhcDevice;
            provenance = "phc:" + options.phc_device_path;
#else
            return Status::Error(StatusCode::kUnsupported,
                                 "PHC device backend requires Linux");
#endif
        } else if (options.clock_id_explicit) {
            clock_id = options.clock_id;
            uint64_t probe = 0;
            const Status probe_status =
                SampleTimespec(static_cast<clockid_t>(clock_id), &probe);
            if (!probe_status.ok()) return probe_status;
            backend = PtpClockBackend::kClockGettime;
            provenance = "clock_gettime:id=" + std::to_string(clock_id);
        } else {
            return Invalid(
                "PTP client requires phc_device_path or explicit clock_id");
        }

        PtpClockClient client(options, backend, clock_fd, clock_id,
                              std::move(provenance));
        if (options.assume_synchronized) {
            if (options.initial_uncertainty_ns >
                options.maximum_uncertainty_ns) {
                return Invalid(
                    "assumed PTP uncertainty exceeds maximum_uncertainty_ns");
            }
            const Status sync = client.PublishSync(
                options.assumed_offset_ns, options.initial_uncertainty_ns,
                ClockSyncState::kSynchronized);
            if (!sync.ok()) return sync;
        } else {
            ClockQuality quality;
            quality.clock_domain_id = options.clock_domain_id;
            quality.uncertainty_ns = options.initial_uncertainty_ns;
            quality.state = ClockSyncState::kUnsynchronized;
            client.StoreQuality(quality);
        }
        return client;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "PTP client allocation failed");
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "PTP client construction failed");
    }
}

PtpClockClient::PtpClockClient(PtpClockClientOptions options,
                               PtpClockBackend backend, int clock_fd,
                               int clock_id, std::string provenance) noexcept
    : options_(std::move(options)),
      backend_(backend),
      clock_fd_(clock_fd),
      clock_id_(clock_id),
      provenance_(std::move(provenance)) {}

PtpClockClient::PtpClockClient(PtpClockClient&& other) noexcept
    : options_(std::move(other.options_)),
      backend_(other.backend_),
      clock_fd_(other.clock_fd_),
      clock_id_(other.clock_id_),
      provenance_(std::move(other.provenance_)) {
    ClockQuality quality = other.LatestQualityOrUncertain();
    StoreQuality(quality);
    other.clock_fd_ = -1;
    other.backend_ = PtpClockBackend::kUnconfigured;
}

PtpClockClient& PtpClockClient::operator=(PtpClockClient&& other) noexcept {
    if (this == &other) return *this;
    if (clock_fd_ >= 0) ::close(clock_fd_);
    options_ = std::move(other.options_);
    backend_ = other.backend_;
    clock_fd_ = other.clock_fd_;
    clock_id_ = other.clock_id_;
    provenance_ = std::move(other.provenance_);
    StoreQuality(other.LatestQualityOrUncertain());
    other.clock_fd_ = -1;
    other.backend_ = PtpClockBackend::kUnconfigured;
    return *this;
}

PtpClockClient::~PtpClockClient() {
    if (clock_fd_ >= 0) ::close(clock_fd_);
}

Status PtpClockClient::SampleClockNs(int clock_id,
                                     uint64_t* out_ns) const noexcept {
    return SampleTimespec(static_cast<clockid_t>(clock_id), out_ns);
}

void PtpClockClient::StoreQuality(const ClockQuality& quality) noexcept {
    quality_.Store(quality);
}

Result<uint64_t> PtpClockClient::NowWallNs() const noexcept {
    if (backend_ == PtpClockBackend::kUnconfigured) {
        return Unavailable("PTP clock client is not configured");
    }
    uint64_t value = 0;
    const Status status = SampleClockNs(clock_id_, &value);
    if (!status.ok()) return status;
    return value;
}

Result<uint64_t> PtpClockClient::NowMonotonicNs() const noexcept {
    uint64_t value = 0;
    const Status status = SampleTimespec(CLOCK_MONOTONIC, &value);
    if (!status.ok()) return status;
    return value;
}

Status PtpClockClient::PublishSync(int64_t estimated_offset_ns,
                                   uint64_t uncertainty_ns,
                                   ClockSyncState state) noexcept {
    if (backend_ == PtpClockBackend::kUnconfigured) {
        return Unavailable("PTP clock client is not configured");
    }
    uint64_t now = 0;
    MINO_RETURN_IF_ERROR(SampleClockNs(clock_id_, &now));
    ClockQuality quality;
    quality.clock_domain_id = options_.clock_domain_id;
    quality.estimated_offset_ns = estimated_offset_ns;
    quality.uncertainty_ns = uncertainty_ns;
    quality.last_sync_time_ns = now;
    quality.state = state;
    if (state == ClockSyncState::kSynchronized &&
        uncertainty_ns > options_.maximum_uncertainty_ns) {
        quality.state = ClockSyncState::kDegraded;
    }
    StoreQuality(quality);
    return Status::Ok();
}

Status PtpClockClient::Refresh() noexcept {
    ClockQuality current = LatestQualityOrUncertain();
    if (current.state != ClockSyncState::kSynchronized &&
        current.state != ClockSyncState::kDegraded &&
        current.state != ClockSyncState::kSynchronizing) {
        return Status::Ok();
    }
    return PublishSync(current.estimated_offset_ns, current.uncertainty_ns,
                       current.state);
}

bool PtpClockClient::TryLoadQuality(ClockQuality* quality,
                                    uint32_t max_attempts) const noexcept {
    return quality_.TryLoad(quality, max_attempts);
}

ClockQuality PtpClockClient::LatestQualityOrUncertain() const noexcept {
    ClockQuality quality;
    if (!TryLoadQuality(&quality)) {
        quality.clock_domain_id = options_.clock_domain_id;
        quality.uncertainty_ns = std::numeric_limits<uint64_t>::max();
        quality.state = ClockSyncState::kUnsynchronized;
    }
    return quality;
}

bool PtpClockClient::AllowsCrossNodeOneWayReporting() const noexcept {
    if (backend_ == PtpClockBackend::kUnconfigured) return false;
    const ClockQuality quality = LatestQualityOrUncertain();
    if (quality.state != ClockSyncState::kSynchronized) return false;
    if (quality.clock_domain_id != options_.clock_domain_id) return false;
    if (quality.uncertainty_ns > options_.maximum_uncertainty_ns) return false;
    const auto now = NowWallNs();
    if (!now.ok()) return false;
    if (*now < quality.last_sync_time_ns) return false;
    if ((*now - quality.last_sync_time_ns) > options_.maximum_sync_age_ns) {
        return false;
    }
    return true;
}

bool TryParsePtpOffsetLine(std::string_view line, int64_t* offset_ns,
                           uint64_t* uncertainty_ns) noexcept {
    if (offset_ns == nullptr || uncertainty_ns == nullptr) return false;
    const auto lower = [](char c) {
        return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    };
    std::string haystack(line);
    for (char& c : haystack) c = lower(c);
    const auto offset_pos = haystack.find("offset");
    if (offset_pos == std::string::npos) return false;
    size_t index = offset_pos + 6;
    while (index < haystack.size() &&
           (haystack[index] == ' ' || haystack[index] == '=' ||
            haystack[index] == ':')) {
        ++index;
    }
    if (index >= haystack.size()) return false;
    errno = 0;
    char* end = nullptr;
    const long long parsed =
        std::strtoll(haystack.c_str() + index, &end, 10);
    if (end == haystack.c_str() + index || errno == ERANGE) return false;
    *offset_ns = static_cast<int64_t>(parsed);
    // Without an explicit rms/error token, treat |offset| as a lower bound on
    // uncertainty and require callers to PublishSync with a real bound for
    // production. Parsing alone never marks the clock synchronized.
    const uint64_t magnitude =
        parsed < 0 ? static_cast<uint64_t>(-parsed)
                   : static_cast<uint64_t>(parsed);
    *uncertainty_ns = magnitude == 0 ? 1 : magnitude;
    return true;
}

}  // namespace mino::observability
