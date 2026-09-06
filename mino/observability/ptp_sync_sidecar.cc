// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/ptp_sync_sidecar.h"

#include <sys/stat.h>
#include <time.h>

#include <cctype>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

namespace mino::observability {
namespace {

Status Unavailable(std::string_view message) {
    return Status::Error(StatusCode::kUnavailable, message);
}

std::string_view TrimView(std::string_view value) {
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
    }
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back()))) {
        value.remove_suffix(1);
    }
    return value;
}

std::string ToLower(std::string_view input) {
    std::string out(input);
    for (char& c : out) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return out;
}

bool ParseInt64Token(std::string_view token, int64_t* out) {
    if (out == nullptr || token.empty()) return false;
    errno = 0;
    char* end = nullptr;
    const std::string owned(token);
    const long long parsed = std::strtoll(owned.c_str(), &end, 10);
    if (end == owned.c_str() || *end != '\0' || errno == ERANGE) return false;
    *out = static_cast<int64_t>(parsed);
    return true;
}

bool ParseUint64Token(std::string_view token, uint64_t* out) {
    if (out == nullptr || token.empty() || token.front() == '-') return false;
    errno = 0;
    char* end = nullptr;
    const std::string owned(token);
    const unsigned long long parsed = std::strtoull(owned.c_str(), &end, 10);
    if (end == owned.c_str() || *end != '\0' || errno == ERANGE) return false;
    *out = static_cast<uint64_t>(parsed);
    return true;
}

bool ParseUint32Token(std::string_view token, uint32_t* out) {
    uint64_t wide = 0;
    if (!ParseUint64Token(token, &wide)) return false;
    if (wide > 0xffffffffull) return false;
    *out = static_cast<uint32_t>(wide);
    return true;
}

bool NowRealtimeNs(uint64_t* out_ns) {
    if (out_ns == nullptr) return false;
    timespec ts{};
    if (::clock_gettime(CLOCK_REALTIME, &ts) != 0) return false;
    if (ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= 1'000'000'000L) {
        return false;
    }
    *out_ns = static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull +
              static_cast<uint64_t>(ts.tv_nsec);
    return true;
}

bool PathIsAbsolute(std::string_view path) {
    return !path.empty() && path.front() == '/';
}

std::vector<std::string_view> SplitLines(std::string_view text) {
    std::vector<std::string_view> lines;
    size_t begin = 0;
    while (begin <= text.size()) {
        const size_t end = text.find('\n', begin);
        if (end == std::string_view::npos) {
            lines.push_back(text.substr(begin));
            break;
        }
        lines.push_back(text.substr(begin, end - begin));
        begin = end + 1;
    }
    return lines;
}

}  // namespace

bool TryParseClockSyncStateToken(std::string_view token,
                                 ClockSyncState* out) noexcept {
    if (out == nullptr) return false;
    const std::string lower = ToLower(TrimView(token));
    if (lower == "synchronized" || lower == "sync" || lower == "s2") {
        *out = ClockSyncState::kSynchronized;
        return true;
    }
    if (lower == "unsynchronized" || lower == "unsync" || lower == "idle") {
        *out = ClockSyncState::kUnsynchronized;
        return true;
    }
    if (lower == "degraded") {
        *out = ClockSyncState::kDegraded;
        return true;
    }
    if (lower == "synchronizing" || lower == "s1" || lower == "s0") {
        *out = ClockSyncState::kSynchronizing;
        return true;
    }
    return false;
}

bool TryParsePtpSyncQualityText(std::string_view text,
                                PtpSyncQualitySample* out) noexcept {
    if (out == nullptr) return false;
    PtpSyncQualitySample sample;
    bool saw_schema = false;
    bool saw_state = false;
    bool saw_offset = false;
    bool saw_uncertainty = false;

    for (std::string_view raw_line : SplitLines(text)) {
        if (!raw_line.empty() && raw_line.back() == '\r') {
            raw_line.remove_suffix(1);
        }
        std::string_view line = TrimView(raw_line);
        if (line.empty() || line.front() == '#') continue;

        if (!saw_schema) {
            // Allow schema alone on the first payload line, or schema as the
            // first token of a compact single-line record.
            if (line == kPtpSyncQualitySchemaV1) {
                saw_schema = true;
                continue;
            }
            if (line.size() > kPtpSyncQualitySchemaV1.size() &&
                line.substr(0, kPtpSyncQualitySchemaV1.size()) ==
                    kPtpSyncQualitySchemaV1 &&
                std::isspace(static_cast<unsigned char>(
                    line[kPtpSyncQualitySchemaV1.size()]))) {
                saw_schema = true;
                line = TrimView(
                    line.substr(kPtpSyncQualitySchemaV1.size()));
                if (line.empty()) continue;
            } else {
                return false;
            }
        }

        // Support multiple key=value tokens on one line.
        while (!line.empty()) {
            const size_t next_space = line.find_first_of(" \t");
            const std::string_view token = TrimView(line.substr(
                0, next_space == std::string_view::npos ? line.size()
                                                        : next_space));
            line = next_space == std::string_view::npos
                       ? std::string_view{}
                       : TrimView(line.substr(next_space + 1));
            if (token.empty()) continue;
            const size_t eq = token.find('=');
            if (eq == std::string_view::npos) return false;
            const std::string key = ToLower(TrimView(token.substr(0, eq)));
            const std::string_view value = TrimView(token.substr(eq + 1));
            if (key == "state") {
                if (!TryParseClockSyncStateToken(value, &sample.state)) {
                    return false;
                }
                saw_state = true;
            } else if (key == "offset_ns" || key == "offset") {
                if (!ParseInt64Token(value, &sample.estimated_offset_ns)) {
                    return false;
                }
                saw_offset = true;
            } else if (key == "uncertainty_ns" || key == "rms_ns" ||
                       key == "rms") {
                if (!ParseUint64Token(value, &sample.uncertainty_ns)) {
                    return false;
                }
                saw_uncertainty = true;
            } else if (key == "clock_domain_id" || key == "domain") {
                if (!ParseUint32Token(value, &sample.clock_domain_id)) {
                    return false;
                }
            } else if (key == "schema") {
                if (value != kPtpSyncQualitySchemaV1) return false;
            } else {
                // Unknown keys fail closed so operators cannot silently drop
                // required fields by typo.
                return false;
            }
        }
    }

    if (!saw_schema || !saw_state || !saw_offset || !saw_uncertainty) {
        return false;
    }
    if (sample.state == ClockSyncState::kSynchronized &&
        sample.uncertainty_ns == std::numeric_limits<uint64_t>::max()) {
        return false;
    }
    *out = sample;
    return true;
}

PtpSyncSidecar::PtpSyncSidecar(PtpClockClient* client,
                               PtpSyncSidecarOptions options) noexcept
    : client_(client), options_(std::move(options)) {}

Status PtpSyncSidecar::FailClosedUnsynchronized(
    std::string_view reason) noexcept {
    (void)reason;
    if (client_ == nullptr) {
        return Unavailable("PTP sync sidecar has no bound PtpClockClient");
    }
    if (!options_.publish_unsynchronized_on_failure) {
        return Unavailable("PTP sync quality unavailable");
    }
    const Status status = client_->PublishSync(
        0, std::numeric_limits<uint64_t>::max(),
        ClockSyncState::kUnsynchronized);
    if (status.ok()) {
        last_sample_ = PtpSyncQualitySample{};
        last_sample_.state = ClockSyncState::kUnsynchronized;
        last_sample_.uncertainty_ns = std::numeric_limits<uint64_t>::max();
        last_sample_.clock_domain_id = client_->clock_domain_id();
        has_last_sample_ = true;
    }
    return status;
}

Status PtpSyncSidecar::ApplySample(
    const PtpSyncQualitySample& sample) noexcept {
    if (client_ == nullptr) {
        return Unavailable("PTP sync sidecar has no bound PtpClockClient");
    }
    if (sample.clock_domain_id != 0 &&
        sample.clock_domain_id != client_->clock_domain_id()) {
        return FailClosedUnsynchronized("clock domain mismatch");
    }
    if (sample.state == ClockSyncState::kSynchronized &&
        sample.uncertainty_ns == 0) {
        return FailClosedUnsynchronized("synchronized sample needs uncertainty");
    }
    const Status status = client_->PublishSync(
        sample.estimated_offset_ns, sample.uncertainty_ns, sample.state);
    if (!status.ok()) return status;
    last_sample_ = sample;
    if (last_sample_.clock_domain_id == 0) {
        last_sample_.clock_domain_id = client_->clock_domain_id();
    }
    has_last_sample_ = true;
    return Status::Ok();
}

Status PtpSyncSidecar::PollOnce() noexcept {
    if (client_ == nullptr) {
        return Unavailable("PTP sync sidecar has no bound PtpClockClient");
    }
    if (!PathIsAbsolute(options_.quality_path)) {
        return FailClosedUnsynchronized("quality path must be absolute");
    }

    struct stat st {};
    if (::stat(options_.quality_path.c_str(), &st) != 0) {
        return FailClosedUnsynchronized("quality file missing");
    }
    if (!S_ISREG(st.st_mode)) {
        return FailClosedUnsynchronized("quality path is not a regular file");
    }

    uint64_t now_ns = 0;
    if (!NowRealtimeNs(&now_ns)) {
        return FailClosedUnsynchronized("CLOCK_REALTIME unavailable");
    }
#if defined(__linux__)
    const uint64_t mtime_ns =
        static_cast<uint64_t>(st.st_mtim.tv_sec) * 1'000'000'000ull +
        static_cast<uint64_t>(st.st_mtim.tv_nsec);
#else
    const uint64_t mtime_ns =
        static_cast<uint64_t>(st.st_mtime) * 1'000'000'000ull;
#endif
    if (options_.maximum_file_age_ns != 0) {
        if (now_ns < mtime_ns ||
            (now_ns - mtime_ns) > options_.maximum_file_age_ns) {
            return FailClosedUnsynchronized("quality file stale");
        }
    }

    std::ifstream input(options_.quality_path);
    if (!input) {
        return FailClosedUnsynchronized("quality file unreadable");
    }
    std::string content((std::istreambuf_iterator<char>(input)),
                        std::istreambuf_iterator<char>());
    PtpSyncQualitySample sample;
    if (!TryParsePtpSyncQualityText(content, &sample)) {
        return FailClosedUnsynchronized("quality file parse failed");
    }
    return ApplySample(sample);
}

}  // namespace mino::observability
