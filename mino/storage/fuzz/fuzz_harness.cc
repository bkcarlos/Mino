// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/storage/fuzz/fuzz_harness.h"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <new>
#include <span>
#include <string_view>

#include <unistd.h>

#include "mino/storage/segment_format.h"
#include "mino/storage/segment_recovery.h"

namespace mino::storage::fuzz {
namespace {

SegmentFormatLimits BoundedLimits() {
    return SegmentFormatLimits{
        .max_payload_size = kMaxSegmentFuzzInputBytes,
        .max_encoded_record_size = kMaxSegmentFuzzInputBytes + 4096u,
    };
}

Status Internal(std::string_view message) {
    return Status::Error(StatusCode::kInternal, message);
}

uint64_t ReadLe64(std::span<const std::byte> input) {
    uint64_t value = 0;
    const size_t count = std::min<size_t>(input.size(), 8);
    for (size_t index = 0; index < count; ++index) {
        value |= static_cast<uint64_t>(static_cast<uint8_t>(input[index]))
                 << (8u * index);
    }
    return value;
}

class TemporarySegment {
public:
    TemporarySegment() {
        char name[] = "/tmp/mino-segment-fuzz-XXXXXX";
        fd_ = ::mkstemp(name);
        if (fd_ >= 0) path_ = name;
    }

    TemporarySegment(const TemporarySegment&) = delete;
    TemporarySegment& operator=(const TemporarySegment&) = delete;

    ~TemporarySegment() {
        if (fd_ >= 0) ::close(fd_);
        if (!path_.empty()) ::unlink(path_.c_str());
    }

    bool valid() const noexcept { return fd_ >= 0; }
    const std::filesystem::path& path() const noexcept { return path_; }

    bool Write(std::span<const std::byte> bytes) noexcept {
        size_t offset = 0;
        while (offset < bytes.size()) {
            const ssize_t written = ::write(fd_, bytes.data() + offset,
                                            bytes.size() - offset);
            if (written < 0 && errno == EINTR) continue;
            if (written <= 0) return false;
            offset += static_cast<size_t>(written);
        }
        return true;
    }

private:
    int fd_ = -1;
    std::filesystem::path path_;
};

}  // namespace

SegmentFuzzSelector SelectSegmentHarness(
    std::span<const std::byte> input) noexcept {
    if (input.empty()) return SegmentFuzzSelector::kFormat;
    return static_cast<SegmentFuzzSelector>(
        static_cast<uint8_t>(input.front()) % 2u);
}

Status FuzzSegmentFormat(std::span<const std::byte> input) noexcept {
    try {
        if (input.size() > kMaxSegmentFuzzInputBytes) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        const uint8_t selector =
            input.empty() ? 0 : static_cast<uint8_t>(input.front()) % 3u;
        const auto payload = input.empty() ? input : input.subspan(1);
        const SegmentFormatLimits limits = BoundedLimits();
        if (selector == 0) {
            auto decoded = DecodeSegmentHeader(payload);
            if (!decoded.ok()) return decoded.status();
            auto encoded = EncodeSegmentHeader(*decoded);
            if (!encoded.ok() || !std::equal(encoded->begin(), encoded->end(),
                                              payload.begin(), payload.end())) {
                return Internal("segment header round trip is unstable");
            }
            return Status::Ok();
        }
        if (selector == 1) {
            auto decoded = DecodeRecord(payload, limits);
            if (!decoded.ok()) return decoded.status();
            auto encoded = EncodeRecord(*decoded, limits);
            if (!encoded.ok() || !std::equal(encoded->begin(), encoded->end(),
                                              payload.begin(), payload.end())) {
                return Internal("record round trip is unstable");
            }
            return Status::Ok();
        }
        const auto size = EncodedRecordSize(ReadLe64(payload), limits);
        return size.ok() ? Status::Ok() : size.status();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Internal("segment format harness threw an exception");
    }
}

Status FuzzSegmentScanner(std::span<const std::byte> input) noexcept {
    try {
        if (input.size() > kMaxSegmentFuzzInputBytes) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        TemporarySegment segment;
        if (!segment.valid() || !segment.Write(input)) {
            return Status::Error(StatusCode::kUnavailable,
                                 "could not create bounded scanner input");
        }
        SegmentRecoveryOptions options;
        options.format_limits = BoundedLimits();
        if (!input.empty() &&
            (static_cast<uint8_t>(input.front()) & 1u) != 0) {
            options.checkpoint = SegmentRecoveryCheckpoint{
                .offset = ReadLe64(input) %
                          (static_cast<uint64_t>(input.size()) + 1u),
                .ingestion_sequence = ReadLe64(input.last(
                    std::min<size_t>(input.size(), 8))),
            };
        }
        auto report = ScanSegment(segment.path(), options);
        if (!report.ok()) return report.status();
        if (report->file_size != input.size() ||
            report->records.size() >
                input.size() / kMinimumEncodedRecordSize + 1u) {
            return Internal("scanner report exceeded input bounds");
        }
        for (const SegmentRecordOffset& record : report->records) {
            if (record.record_offset > input.size() ||
                record.encoded_size > input.size() - record.record_offset) {
                return Internal("scanner returned an out-of-bounds record");
            }
            const auto encoded = input.subspan(
                static_cast<size_t>(record.record_offset),
                static_cast<size_t>(record.encoded_size));
            if (!DecodeRecord(encoded, options.format_limits).ok()) {
                return Internal("scanner accepted a record rejected by format");
            }
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Internal("segment scanner harness threw an exception");
    }
}

Status FuzzOneInput(std::span<const std::byte> input) noexcept {
    const SegmentFuzzSelector selector = SelectSegmentHarness(input);
    const auto payload = input.empty() ? input : input.subspan(1);
    if (selector == SegmentFuzzSelector::kFormat) {
        return FuzzSegmentFormat(payload);
    }
    return FuzzSegmentScanner(payload);
}

}  // namespace mino::storage::fuzz
