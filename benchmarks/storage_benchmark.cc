// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "mino/common/ids.h"
#include "mino/common/status.h"
#include "mino/storage/recorder_buffer_pool.h"
#include "mino/storage/segment_format.h"
#include "mino/storage/segment_recovery.h"
#include "mino/storage/segment_writer.h"

namespace mino::storage {
namespace {

using Clock = std::chrono::steady_clock;

constexpr uint64_t kDefaultRecords = 20'000;
constexpr size_t kDefaultPayloadBytes = 1024;
constexpr uint64_t kWriterBatchRecords = 128;
constexpr uint64_t kRecoverySamples = 5;
constexpr uint64_t kRecoveryWarmupScans = 1;
constexpr size_t kMpscProducers = 4;
constexpr size_t kMaximumBufferPayloadBytes = 16u * 1024u * 1024u;
constexpr size_t kTargetBufferPoolBytes = 64u * 1024u * 1024u;
constexpr uint64_t kPayloadSeed = 0x4d494e4f44353134ULL;
constexpr std::string_view kDirectoryPrefix = "mino-storage-benchmark-";
constexpr std::string_view kOwnershipMarker = ".mino-storage-benchmark-owned";
constexpr std::string_view kOwnershipMarkerContents =
    "Mino storage-sla benchmark temporary directory\n";

std::atomic<uint64_t> g_benchmark_sink{0};

struct Config {
    uint64_t records = kDefaultRecords;
    size_t payload_bytes = kDefaultPayloadBytes;
    SegmentSyncPolicy sync_policy = SegmentSyncPolicy::kPerBatch;
    std::string sync_policy_name = "per-batch";
    std::optional<std::filesystem::path> output_json;
    std::optional<std::filesystem::path> directory;
};

struct Distribution {
    size_t count = 0;
    uint64_t p50 = 0;
    uint64_t p95 = 0;
    uint64_t p99 = 0;
    uint64_t p999 = 0;
    uint64_t maximum = 0;
};

struct EncodeMetrics {
    uint64_t warmup_records = 0;
    uint64_t operations_attempted = 0;
    uint64_t errors = 0;
    Distribution latency_ns;
};

struct WriterMetrics {
    uint64_t warmup_records = 0;
    uint64_t records = 0;
    uint64_t encoded_bytes = 0;
    uint64_t append_elapsed_ns = 0;
    uint64_t end_to_end_elapsed_ns = 0;
    double append_records_per_second = 0.0;
    double write_mebibytes_per_second = 0.0;
    uint64_t operations_attempted = 0;
    uint64_t errors = 0;
    uint64_t fdatasync_attempts = 0;
    uint64_t fdatasync_errors = 0;
    Distribution flush_latency_ns;
    Distribution fdatasync_latency_ns;
};

struct RecoveryMetrics {
    uint64_t warmup_scans = kRecoveryWarmupScans;
    uint64_t scan_attempts = 0;
    uint64_t errors = 0;
    uint64_t records_per_scan = 0;
    uint64_t bytes_per_scan = 0;
    uint64_t total_elapsed_ns = 0;
    double records_per_second = 0.0;
    double mebibytes_per_second = 0.0;
    Distribution scan_time_ns;
};

struct BufferMetrics {
    uint64_t warmup_records = 0;
    uint64_t records = 0;
    size_t producers = kMpscProducers;
    size_t queue_capacity = 0;
    uint64_t elapsed_ns = 0;
    double records_per_second = 0.0;
    double payload_mebibytes_per_second = 0.0;
    uint64_t operations_attempted = 0;
    uint64_t errors = 0;
    uint64_t internal_errors = 0;
    std::string error_detail;
};

uint64_t DurationNs(Clock::time_point begin, Clock::time_point end) {
    const auto value =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
    return value <= 0 ? 0 : static_cast<uint64_t>(value);
}

double ErrorRate(uint64_t errors, uint64_t attempts) {
    if (attempts == 0) return 0.0;
    return static_cast<double>(errors) / static_cast<double>(attempts);
}

double PerSecond(uint64_t count, uint64_t elapsed_ns) {
    if (elapsed_ns == 0) return 0.0;
    return static_cast<double>(count) * 1'000'000'000.0 /
           static_cast<double>(elapsed_ns);
}

double MebibytesPerSecond(uint64_t bytes, uint64_t elapsed_ns) {
    if (elapsed_ns == 0) return 0.0;
    constexpr double kMebibyte = 1024.0 * 1024.0;
    return static_cast<double>(bytes) * 1'000'000'000.0 /
           (kMebibyte * static_cast<double>(elapsed_ns));
}

Distribution Summarize(std::vector<uint64_t> samples) {
    Distribution result;
    result.count = samples.size();
    if (samples.empty()) return result;
    std::sort(samples.begin(), samples.end());
    const auto nearest_rank = [&](double percentile) {
        const double raw_rank =
            std::ceil(percentile * static_cast<double>(samples.size()));
        size_t rank = static_cast<size_t>(raw_rank);
        rank = std::max<size_t>(1, std::min(rank, samples.size()));
        return samples[rank - 1];
    };
    result.p50 = nearest_rank(0.50);
    result.p95 = nearest_rank(0.95);
    result.p99 = nearest_rank(0.99);
    result.p999 = nearest_rank(0.999);
    result.maximum = samples.back();
    return result;
}

uint64_t WarmupRecords(uint64_t records) {
    return std::min<uint64_t>(10'000, std::max<uint64_t>(1, records / 10));
}

std::string JsonEscape(std::string_view input) {
    std::ostringstream output;
    for (const unsigned char character : input) {
        switch (character) {
            case '"':
                output << "\\\"";
                break;
            case '\\':
                output << "\\\\";
                break;
            case '\b':
                output << "\\b";
                break;
            case '\f':
                output << "\\f";
                break;
            case '\n':
                output << "\\n";
                break;
            case '\r':
                output << "\\r";
                break;
            case '\t':
                output << "\\t";
                break;
            default:
                if (character < 0x20u) {
                    output << "\\u" << std::hex << std::setw(4)
                           << std::setfill('0')
                           << static_cast<unsigned int>(character) << std::dec
                           << std::setfill(' ');
                } else {
                    output << static_cast<char>(character);
                }
                break;
        }
    }
    return output.str();
}

uint64_t ParseUnsigned(std::string_view text, std::string_view option) {
    if (text.empty()) {
        throw std::runtime_error(std::string(option) + " requires a value");
    }
    uint64_t value = 0;
    const char* const begin = text.data();
    const char* const end = text.data() + text.size();
    const auto parsed = std::from_chars(begin, end, value);
    if (parsed.ec != std::errc{} || parsed.ptr != end) {
        throw std::runtime_error(std::string(option) +
                                 " requires an unsigned integer");
    }
    return value;
}

std::optional<std::string_view> InlineOption(std::string_view argument,
                                             std::string_view option) {
    if (argument == option) return std::string_view{};
    if (argument.size() > option.size() && argument.starts_with(option) &&
        argument[option.size()] == '=') {
        return argument.substr(option.size() + 1);
    }
    return std::nullopt;
}

std::string_view OptionValue(int* index, int argc, char** argv,
                             std::string_view option,
                             std::string_view inline_value) {
    if (inline_value.data() != nullptr) return inline_value;
    if (*index + 1 >= argc) {
        throw std::runtime_error(std::string(option) + " requires a value");
    }
    ++(*index);
    return argv[*index];
}

void PrintUsage(std::ostream& output, std::string_view program) {
    output
        << "Usage: " << program << " [options]\n"
        << "  --records N|--records=N              measured records (default "
        << kDefaultRecords << ")\n"
        << "  --payload-bytes N|--payload-bytes=N  payload bytes per record "
           "(default "
        << kDefaultPayloadBytes << ")\n"
        << "  --sync-policy P|--sync-policy=P      none, interval, per-batch, "
           "or per-record (default per-batch)\n"
        << "  --output-json PATH                   also write JSON to PATH\n"
        << "  --directory PATH                     base for an owned temporary "
           "subdirectory\n"
        << "  --help                               show this help\n";
}

Config ParseArguments(int argc, char** argv) {
    Config config;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--help") {
            PrintUsage(std::cout, argv[0]);
            std::exit(0);
        }
        if (const auto value = InlineOption(argument, "--records")) {
            config.records = ParseUnsigned(
                OptionValue(&index, argc, argv, "--records", *value),
                "--records");
            continue;
        }
        if (const auto value = InlineOption(argument, "--payload-bytes")) {
            const uint64_t parsed = ParseUnsigned(
                OptionValue(&index, argc, argv, "--payload-bytes", *value),
                "--payload-bytes");
            if (parsed > std::numeric_limits<size_t>::max()) {
                throw std::runtime_error("--payload-bytes exceeds size_t");
            }
            config.payload_bytes = static_cast<size_t>(parsed);
            continue;
        }
        if (const auto value = InlineOption(argument, "--sync-policy")) {
            const std::string_view policy = OptionValue(
                &index, argc, argv, "--sync-policy", *value);
            if (policy == "none") {
                config.sync_policy = SegmentSyncPolicy::kNone;
            } else if (policy == "interval") {
                config.sync_policy = SegmentSyncPolicy::kInterval;
            } else if (policy == "per-batch") {
                config.sync_policy = SegmentSyncPolicy::kPerBatch;
            } else if (policy == "per-record") {
                config.sync_policy = SegmentSyncPolicy::kPerRecord;
            } else {
                throw std::runtime_error(
                    "--sync-policy must be none, interval, per-batch, or "
                    "per-record");
            }
            config.sync_policy_name = std::string(policy);
            continue;
        }
        if (const auto value = InlineOption(argument, "--output-json")) {
            config.output_json = std::filesystem::path(OptionValue(
                &index, argc, argv, "--output-json", *value));
            continue;
        }
        if (const auto value = InlineOption(argument, "--directory")) {
            config.directory = std::filesystem::path(OptionValue(
                &index, argc, argv, "--directory", *value));
            continue;
        }
        throw std::runtime_error("unknown option: " + std::string(argument));
    }

    if (config.records == 0) {
        throw std::runtime_error("--records must be greater than zero");
    }
    if (config.records >
        std::numeric_limits<size_t>::max() -
            static_cast<size_t>(kWriterBatchRecords)) {
        throw std::runtime_error("--records is too large for benchmark samples");
    }
    if (config.payload_bytes > kMaximumBufferPayloadBytes) {
        throw std::runtime_error(
            "--payload-bytes exceeds recorder_buffer_pool's 16 MiB limit");
    }
    if (config.output_json.has_value() && config.output_json->empty()) {
        throw std::runtime_error("--output-json must not be empty");
    }
    if (config.directory.has_value() && config.directory->empty()) {
        throw std::runtime_error("--directory must not be empty");
    }
    return config;
}

class OwnedTemporaryDirectory final {
public:
    explicit OwnedTemporaryDirectory(
        const std::optional<std::filesystem::path>& requested_base) {
        std::error_code error;
        std::filesystem::path base =
            requested_base.has_value()
                ? *requested_base
                : std::filesystem::temp_directory_path(error);
        if (error) {
            throw std::runtime_error("cannot determine temporary directory: " +
                                     error.message());
        }
        std::filesystem::create_directories(base, error);
        if (error) {
            throw std::runtime_error("cannot create benchmark base directory '" +
                                     base.string() + "': " + error.message());
        }
        base_ = std::filesystem::canonical(base, error);
        if (error) {
            throw std::runtime_error("cannot canonicalize benchmark directory '" +
                                     base.string() + "': " + error.message());
        }

        const uint64_t nonce = static_cast<uint64_t>(
            Clock::now().time_since_epoch().count());
        for (uint64_t attempt = 0; attempt < 100; ++attempt) {
            const std::string name =
                std::string(kDirectoryPrefix) +
                std::to_string(static_cast<uint64_t>(::getpid())) + "-" +
                std::to_string(nonce) + "-" + std::to_string(attempt);
            const std::filesystem::path candidate = base_ / name;
            error.clear();
            if (!std::filesystem::create_directory(candidate, error)) {
                if (!error || error == std::errc::file_exists) continue;
                throw std::runtime_error(
                    "cannot create benchmark temporary directory '" +
                    candidate.string() + "': " + error.message());
            }

            const std::filesystem::path marker = candidate / kOwnershipMarker;
            std::ofstream output(marker, std::ios::binary | std::ios::out);
            output << kOwnershipMarkerContents;
            output.close();
            if (!output) {
                std::filesystem::remove(candidate, error);
                throw std::runtime_error(
                    "cannot create benchmark ownership marker in '" +
                    candidate.string() + "'");
            }
            path_ = candidate;
            owned_ = true;
            return;
        }
        throw std::runtime_error(
            "cannot allocate a unique benchmark temporary directory");
    }

    ~OwnedTemporaryDirectory() { Cleanup(); }

    OwnedTemporaryDirectory(const OwnedTemporaryDirectory&) = delete;
    OwnedTemporaryDirectory& operator=(const OwnedTemporaryDirectory&) = delete;

    const std::filesystem::path& path() const noexcept { return path_; }
    const std::filesystem::path& base() const noexcept { return base_; }

private:
    void Cleanup() noexcept {
        if (!owned_) return;
        std::error_code error;
        const auto status = std::filesystem::symlink_status(path_, error);
        if (error || status.type() != std::filesystem::file_type::directory ||
            path_.parent_path() != base_ ||
            !path_.filename().string().starts_with(kDirectoryPrefix)) {
            std::cerr << "warning: refused to clean unverified benchmark "
                         "directory '"
                      << path_.string() << "'\n";
            return;
        }

        std::ifstream marker(path_ / kOwnershipMarker, std::ios::binary);
        const std::string contents{std::istreambuf_iterator<char>(marker),
                                   std::istreambuf_iterator<char>()};
        if (!marker.is_open() || contents != kOwnershipMarkerContents) {
            std::cerr << "warning: refused to clean benchmark directory with "
                         "missing or invalid marker '"
                      << path_.string() << "'\n";
            return;
        }
        std::filesystem::remove_all(path_, error);
        if (error) {
            std::cerr << "warning: could not clean benchmark directory '"
                      << path_.string() << "': " << error.message() << '\n';
        }
    }

    std::filesystem::path base_;
    std::filesystem::path path_;
    bool owned_ = false;
};

SegmentHeader MakeSegmentHeader() {
    SegmentHeader header;
    header.recording_id = 0xd514;
    header.topic_id = 11;
    header.partition_id = 3;
    header.writer_id = 29;
    header.first_ingestion_sequence = 1;
    header.created_at_ns = 1;
    return header;
}

Record MakeRecord(size_t payload_bytes) {
    Record record;
    record.header.schema_ref = 1;
    record.header.schema_version = 0x00010000u;
    record.header.layout_version = 1;
    record.header.topic_id = 11;
    record.header.partition_id = 3;
    record.header.ingestion_sequence = 1;
    record.header.ingestion_timestamp_ns = 1001;
    record.header.node_id = 41;
    record.header.publisher_id = 43;
    record.header.publisher_epoch = 47;
    record.header.source_sequence = 1;
    record.header.observed_timestamp_ns = 2001;
    record.payload.resize(payload_bytes);

    uint64_t state = kPayloadSeed;
    for (std::byte& byte : record.payload) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        byte = static_cast<std::byte>(state & 0xffu);
    }
    return record;
}

void SetSequence(Record* record, uint64_t sequence) {
    record->header.ingestion_sequence = sequence;
    record->header.ingestion_timestamp_ns = 1000 + sequence;
    record->header.source_sequence = sequence;
    record->header.observed_timestamp_ns = 2000 + sequence;
}

void ConsumeEncoded(const std::vector<std::byte>& encoded, uint64_t* checksum) {
    *checksum ^= static_cast<uint64_t>(encoded.size());
    if (!encoded.empty()) {
        *checksum ^= static_cast<uint64_t>(
            std::to_integer<unsigned int>(encoded.front()));
        *checksum ^= static_cast<uint64_t>(
                         std::to_integer<unsigned int>(encoded.back()))
                     << 8;
    }
}

void RunEncodeWarmup(Record* record, uint64_t records) {
    uint64_t checksum = 0;
    for (uint64_t index = 0; index < records; ++index) {
        SetSequence(record, index + 1);
        auto encoded = EncodeRecord(*record);
        if (!encoded.ok()) {
            throw std::runtime_error("EncodeRecord warmup failed: " +
                                     encoded.status().ToString());
        }
        ConsumeEncoded(*encoded, &checksum);
    }
    g_benchmark_sink.fetch_xor(checksum, std::memory_order_relaxed);
}

EncodeMetrics BenchmarkEncode(Record* record, uint64_t records) {
    EncodeMetrics metrics;
    metrics.warmup_records = WarmupRecords(records);
    RunEncodeWarmup(record, metrics.warmup_records);

    std::vector<uint64_t> samples;
    samples.reserve(static_cast<size_t>(records));
    uint64_t checksum = 0;
    for (uint64_t index = 0; index < records; ++index) {
        SetSequence(record, index + 1);
        ++metrics.operations_attempted;
        const Clock::time_point begin = Clock::now();
        auto encoded = EncodeRecord(*record);
        const Clock::time_point end = Clock::now();
        if (!encoded.ok()) {
            ++metrics.errors;
            throw std::runtime_error("EncodeRecord failed: " +
                                     encoded.status().ToString());
        }
        samples.push_back(DurationNs(begin, end));
        ConsumeEncoded(*encoded, &checksum);
    }
    g_benchmark_sink.fetch_xor(checksum, std::memory_order_relaxed);
    metrics.latency_ns = Summarize(std::move(samples));
    return metrics;
}

struct SyncSamples {
    std::vector<uint64_t> latency_ns;
    uint64_t attempts = 0;
    uint64_t errors = 0;
};

int TimedDataSync(int fd, void* context) noexcept {
    auto* samples = static_cast<SyncSamples*>(context);
    const Clock::time_point begin = Clock::now();
#if defined(__APPLE__)
    // macOS does not expose fdatasync; use fsync for the durable benchmark
    // path so the timing still measures the configured sync boundary.
    const int result = ::fsync(fd);
#else
    const int result = ::fdatasync(fd);
#endif
    const int saved_errno = errno;
    const Clock::time_point end = Clock::now();
    ++samples->attempts;
    if (samples->latency_ns.size() < samples->latency_ns.capacity()) {
        samples->latency_ns.push_back(DurationNs(begin, end));
    }
    if (result != 0) ++samples->errors;
    errno = saved_errno;
    return result;
}

WriterMetrics RunWriter(const std::filesystem::path& path, Record* record,
                        uint64_t records, SegmentSyncPolicy sync_policy,
                        uint64_t warmup_records) {
    WriterMetrics metrics;
    metrics.warmup_records = warmup_records;
    metrics.records = records;

    const auto encoded_size = EncodedRecordSize(record->payload.size());
    if (!encoded_size.ok()) {
        throw std::runtime_error("cannot determine encoded record size: " +
                                 encoded_size.status().ToString());
    }
    if (records > std::numeric_limits<uint64_t>::max() /
                      static_cast<uint64_t>(*encoded_size)) {
        throw std::runtime_error("writer encoded byte count overflows uint64_t");
    }

    SyncSamples sync_samples;
    sync_samples.latency_ns.reserve(static_cast<size_t>(records) + 2);
    SegmentWriterOptions options;
    options.batch_bytes = 0;
    options.batch_records = 0;
    options.flush_interval_ns = 0;
    options.sync_policy = sync_policy;
    options.sync_interval_ns = 0;
    options.sync_interval_bytes =
        static_cast<uint64_t>(*encoded_size) * kWriterBatchRecords;
    options.data_sync_hook = TimedDataSync;
    options.io_hook_context = &sync_samples;

    auto created = SegmentWriter::Create(path, MakeSegmentHeader(), 1, options);
    if (!created.ok()) {
        throw std::runtime_error("SegmentWriter::Create failed: " +
                                 created.status().ToString());
    }
    std::unique_ptr<SegmentWriter> writer = std::move(*created);
    std::vector<uint64_t> flush_samples;
    flush_samples.reserve(static_cast<size_t>(
        (records + kWriterBatchRecords - 1) / kWriterBatchRecords));

    const Clock::time_point workload_begin = Clock::now();
    for (uint64_t index = 0; index < records; ++index) {
        const uint64_t sequence = index + 1;
        SetSequence(record, sequence);
        ++metrics.operations_attempted;
        const Clock::time_point append_begin = Clock::now();
        auto appended = writer->Append(*record, sequence + 1);
        const Clock::time_point append_end = Clock::now();
        metrics.append_elapsed_ns += DurationNs(append_begin, append_end);
        if (!appended.ok() || appended->records_accepted != 1 ||
            appended->rotate_needed) {
            ++metrics.errors;
            const std::string detail =
                appended.ok() ? "unexpected rotation or rejected record"
                              : appended.status().ToString();
            throw std::runtime_error("SegmentWriter::Append failed: " + detail);
        }

        if ((index + 1) % kWriterBatchRecords == 0 || index + 1 == records) {
            ++metrics.operations_attempted;
            const Clock::time_point flush_begin = Clock::now();
            const Status status = writer->Flush(sequence + 1);
            const Clock::time_point flush_end = Clock::now();
            flush_samples.push_back(DurationNs(flush_begin, flush_end));
            if (!status.ok()) {
                ++metrics.errors;
                throw std::runtime_error("SegmentWriter::Flush failed: " +
                                         status.ToString());
            }
        }
    }

    ++metrics.operations_attempted;
    const Status sealed = writer->Seal(records + 2);
    const Clock::time_point workload_end = Clock::now();
    if (!sealed.ok()) {
        ++metrics.errors;
        throw std::runtime_error("SegmentWriter::Seal failed: " +
                                 sealed.ToString());
    }

    metrics.encoded_bytes = writer->size_bytes() - kEncodedSegmentHeaderSize;
    metrics.end_to_end_elapsed_ns =
        DurationNs(workload_begin, workload_end);
    metrics.append_records_per_second =
        PerSecond(metrics.records, metrics.append_elapsed_ns);
    metrics.write_mebibytes_per_second =
        MebibytesPerSecond(metrics.encoded_bytes,
                           metrics.end_to_end_elapsed_ns);
    metrics.flush_latency_ns = Summarize(std::move(flush_samples));
    metrics.fdatasync_attempts = sync_samples.attempts;
    metrics.fdatasync_errors = sync_samples.errors;
    metrics.fdatasync_latency_ns =
        Summarize(std::move(sync_samples.latency_ns));
    return metrics;
}

void RunWriterWarmup(const std::filesystem::path& path, Record* record,
                     uint64_t records, SegmentSyncPolicy sync_policy) {
    static_cast<void>(RunWriter(path, record, records, sync_policy, records));
}

RecoveryMetrics BenchmarkRecovery(const std::filesystem::path& path,
                                  uint64_t expected_records,
                                  uint64_t expected_bytes) {
    for (uint64_t index = 0; index < kRecoveryWarmupScans; ++index) {
        auto report = ScanSegment(path);
        if (!report.ok() || !report->clean() ||
            report->records_scanned != expected_records) {
            const std::string detail =
                report.ok() ? report->reason_detail
                            : report.status().ToString();
            throw std::runtime_error("recovery warmup scan failed: " + detail);
        }
    }

    RecoveryMetrics metrics;
    metrics.records_per_scan = expected_records;
    metrics.bytes_per_scan = expected_bytes;
    std::vector<uint64_t> samples;
    samples.reserve(kRecoverySamples);
    for (uint64_t index = 0; index < kRecoverySamples; ++index) {
        ++metrics.scan_attempts;
        const Clock::time_point begin = Clock::now();
        auto report = ScanSegment(path);
        const Clock::time_point end = Clock::now();
        const uint64_t elapsed = DurationNs(begin, end);
        if (!report.ok() || !report->clean() ||
            report->records_scanned != expected_records) {
            ++metrics.errors;
            const std::string detail =
                report.ok() ? report->reason_detail
                            : report.status().ToString();
            throw std::runtime_error("recovery scan failed: " + detail);
        }
        samples.push_back(elapsed);
        metrics.total_elapsed_ns += elapsed;
    }

    const uint64_t total_records = expected_records * metrics.scan_attempts;
    const uint64_t total_bytes = expected_bytes * metrics.scan_attempts;
    metrics.records_per_second =
        PerSecond(total_records, metrics.total_elapsed_ns);
    metrics.mebibytes_per_second =
        MebibytesPerSecond(total_bytes, metrics.total_elapsed_ns);
    metrics.scan_time_ns = Summarize(std::move(samples));
    return metrics;
}

size_t BufferCharge(size_t payload_bytes) {
    if (payload_bytes == 0) return 0;
    if (payload_bytes <= kRecorderSmallBufferClassBytes) {
        return kRecorderSmallBufferClassBytes;
    }
    if (payload_bytes <= kRecorderMediumBufferClassBytes) {
        return kRecorderMediumBufferClassBytes;
    }
    if (payload_bytes <= kRecorderLargeBufferClassBytes) {
        return kRecorderLargeBufferClassBytes;
    }
    constexpr size_t kPage = 4096;
    return ((payload_bytes + kPage - 1) / kPage) * kPage;
}

size_t BufferQueueCapacity(uint64_t records, size_t charged_bytes) {
    constexpr size_t kMaximumCapacity = 4096;
    size_t capacity = kMaximumCapacity;
    if (charged_bytes != 0) {
        capacity = std::max<size_t>(
            kMpscProducers, kTargetBufferPoolBytes / charged_bytes);
        capacity = std::min(capacity, kMaximumCapacity);
    }
    return std::max<size_t>(
        1, std::min(capacity, static_cast<size_t>(records)));
}

class StartGate final {
public:
    explicit StartGate(size_t participants) : participants_(participants) {}

    void ArriveAndWait() {
        std::unique_lock lock(mutex_);
        ++arrived_;
        ready_.notify_one();
        start_.wait(lock, [this] { return started_; });
    }

    void WaitUntilReady() {
        std::unique_lock lock(mutex_);
        ready_.wait(lock, [this] { return arrived_ == participants_; });
    }

    void Start() {
        {
            std::lock_guard lock(mutex_);
            started_ = true;
        }
        start_.notify_all();
    }

private:
    const size_t participants_;
    size_t arrived_ = 0;
    bool started_ = false;
    std::mutex mutex_;
    std::condition_variable ready_;
    std::condition_variable start_;
};

BufferMetrics RunBuffer(uint64_t records, size_t payload_bytes,
                        uint64_t warmup_records) {
    BufferMetrics metrics;
    metrics.warmup_records = warmup_records;
    metrics.records = records;
    metrics.operations_attempted = records;

    const size_t charged_bytes = BufferCharge(payload_bytes);
    metrics.queue_capacity = BufferQueueCapacity(records, charged_bytes);
    RecorderBufferPoolOptions options;
    options.queue_capacity = metrics.queue_capacity;
    options.max_large_object_bytes = kMaximumBufferPayloadBytes;
    if (charged_bytes == 0) {
        options.global_byte_limit = 4096;
    } else {
        if (charged_bytes >
            std::numeric_limits<size_t>::max() / metrics.queue_capacity) {
            throw std::runtime_error("buffer pool byte limit overflows size_t");
        }
        options.global_byte_limit = charged_bytes * metrics.queue_capacity;
    }
    options.default_topic_byte_limit = options.global_byte_limit;

    auto created = RecorderBufferPool::Create(options);
    if (!created.ok()) {
        throw std::runtime_error("RecorderBufferPool::Create failed: " +
                                 created.status().ToString());
    }
    std::unique_ptr<RecorderBufferPool> pool = std::move(*created);
    StartGate gate(kMpscProducers + 1);
    std::atomic<bool> failed{false};
    std::atomic<uint64_t> internal_errors{0};
    std::mutex error_mutex;
    std::string error_detail;
    uint64_t consumed = 0;

    const auto fail = [&](std::string detail) {
        internal_errors.fetch_add(1, std::memory_order_relaxed);
        if (!failed.exchange(true, std::memory_order_relaxed)) {
            std::lock_guard lock(error_mutex);
            error_detail = std::move(detail);
        }
        pool->Close();
    };

    std::vector<std::thread> threads;
    threads.reserve(kMpscProducers + 1);
    try {
        threads.emplace_back([&] {
            try {
                gate.ArriveAndWait();
                uint64_t checksum = 0;
                while (consumed < records &&
                       !failed.load(std::memory_order_relaxed)) {
                    auto dequeued = pool->Dequeue();
                    if (!dequeued.ok()) {
                        if (!failed.load(std::memory_order_relaxed)) {
                            fail("RecorderBufferPool::Dequeue failed: " +
                                 dequeued.status().ToString());
                        }
                        break;
                    }
                    if (!dequeued->bytes().empty()) {
                        checksum += std::to_integer<unsigned int>(
                            dequeued->bytes().front());
                        checksum += std::to_integer<unsigned int>(
                            dequeued->bytes().back());
                    }
                    ++consumed;
                }
                g_benchmark_sink.fetch_xor(checksum,
                                           std::memory_order_relaxed);
            } catch (const std::exception& exception) {
                fail(std::string("buffer consumer exception: ") +
                     exception.what());
            } catch (...) {
                fail("buffer consumer unknown exception");
            }
        });

        for (size_t producer = 0; producer < kMpscProducers; ++producer) {
            threads.emplace_back([&, producer] {
                try {
                    gate.ArriveAndWait();
                    for (uint64_t index = producer; index < records;
                         index += kMpscProducers) {
                        if (failed.load(std::memory_order_relaxed)) break;
                        BufferReservationRequest request;
                        request.topic_id = TopicId{11};
                        request.payload_size = payload_bytes;
                        request.user_tag = index + 1;
                        request.full_policy = BufferFullPolicy::kBlock;
                        auto reserved = pool->Reserve(request);
                        if (!reserved.ok() || !reserved->accepted()) {
                            const std::string detail =
                                reserved.ok()
                                    ? "reservation was unexpectedly dropped"
                                    : reserved.status().ToString();
                            fail("RecorderBufferPool::Reserve failed: " +
                                 detail);
                            break;
                        }
                        const std::byte value = static_cast<std::byte>(
                            (index + producer) & 0xffu);
                        std::fill(reserved->reservation.bytes().begin(),
                                  reserved->reservation.bytes().end(), value);
                        const Status committed =
                            std::move(reserved->reservation).Commit();
                        if (!committed.ok()) {
                            fail("RecorderBufferReservation::Commit failed: " +
                                 committed.ToString());
                            break;
                        }
                    }
                } catch (const std::exception& exception) {
                    fail(std::string("buffer producer exception: ") +
                         exception.what());
                } catch (...) {
                    fail("buffer producer unknown exception");
                }
            });
        }
    } catch (...) {
        pool->Close();
        gate.Start();
        for (std::thread& thread : threads) {
            if (thread.joinable()) thread.join();
        }
        throw;
    }

    gate.WaitUntilReady();
    const Clock::time_point begin = Clock::now();
    gate.Start();
    for (std::thread& thread : threads) thread.join();
    const Clock::time_point end = Clock::now();
    pool->Close();

    metrics.elapsed_ns = DurationNs(begin, end);
    metrics.internal_errors = internal_errors.load(std::memory_order_relaxed);
    metrics.errors = records - consumed;
    metrics.error_detail = std::move(error_detail);
    metrics.records_per_second = PerSecond(consumed, metrics.elapsed_ns);
    if (payload_bytes != 0 &&
        consumed <= std::numeric_limits<uint64_t>::max() / payload_bytes) {
        metrics.payload_mebibytes_per_second = MebibytesPerSecond(
            consumed * static_cast<uint64_t>(payload_bytes), metrics.elapsed_ns);
    }
    return metrics;
}

void RunBufferWarmup(uint64_t records, size_t payload_bytes) {
    const BufferMetrics warmup = RunBuffer(records, payload_bytes, records);
    if (warmup.errors != 0) {
        throw std::runtime_error("buffer warmup failed: " +
                                 warmup.error_detail);
    }
}

void WriteDistributionJson(std::ostream& output,
                           const Distribution& distribution,
                           std::string_view indent) {
    output << indent << "{\n"
           << indent << "  \"samples\": " << distribution.count << ",\n"
           << indent << "  \"p50\": " << distribution.p50 << ",\n"
           << indent << "  \"p95\": " << distribution.p95 << ",\n"
           << indent << "  \"p99\": " << distribution.p99 << ",\n"
           << indent << "  \"p99_9\": " << distribution.p999 << ",\n"
           << indent << "  \"max\": " << distribution.maximum << '\n'
           << indent << '}';
}

std::string BuildJson(const Config& config,
                      const OwnedTemporaryDirectory& temporary,
                      const EncodeMetrics& encode,
                      const WriterMetrics& writer,
                      const RecoveryMetrics& recovery,
                      const BufferMetrics& buffer) {
    std::ostringstream output;
    output << std::setprecision(17);
    output << "{\n"
           << "  \"schema\": \"mino.storage_benchmark.v1\",\n"
           << "  \"benchmark\": \"storage-sla\",\n"
           << "  \"clock\": \"std::chrono::steady_clock\",\n"
           << "  \"configuration\": {\n"
           << "    \"records\": " << config.records << ",\n"
           << "    \"payload_bytes\": " << config.payload_bytes << ",\n"
           << "    \"sync_policy\": \""
           << JsonEscape(config.sync_policy_name) << "\",\n"
           << "    \"directory_base\": \""
           << JsonEscape(temporary.base().string()) << "\",\n"
           << "    \"writer_batch_records\": " << kWriterBatchRecords
           << ",\n"
           << "    \"mpsc_producers\": " << kMpscProducers << "\n"
           << "  },\n"
           << "  \"methodology\": {\n"
           << "    \"payload_seed\": " << kPayloadSeed << ",\n"
           << "    \"percentile_method\": \"nearest-rank\",\n"
           << "    \"latency_unit\": \"ns\",\n"
           << "    \"throughput_byte_unit\": \"MiB/s\",\n"
           << "    \"warmup\": {\n"
           << "      \"encode_records\": " << encode.warmup_records
           << ",\n"
           << "      \"writer_records\": " << writer.warmup_records
           << ",\n"
           << "      \"recovery_scans\": " << recovery.warmup_scans
           << ",\n"
           << "      \"buffer_records\": " << buffer.warmup_records
           << "\n"
           << "    }\n"
           << "  },\n"
           << "  \"results\": {\n"
           << "    \"encode_record\": {\n"
           << "      \"operations_attempted\": "
           << encode.operations_attempted << ",\n"
           << "      \"errors\": " << encode.errors << ",\n"
           << "      \"error_rate\": "
           << ErrorRate(encode.errors, encode.operations_attempted) << ",\n"
           << "      \"latency_ns\": ";
    WriteDistributionJson(output, encode.latency_ns, "      ");
    output << "\n    },\n"
           << "    \"segment_writer\": {\n"
           << "      \"records\": " << writer.records << ",\n"
           << "      \"encoded_bytes\": " << writer.encoded_bytes << ",\n"
           << "      \"append_elapsed_ns\": " << writer.append_elapsed_ns
           << ",\n"
           << "      \"end_to_end_elapsed_ns\": "
           << writer.end_to_end_elapsed_ns << ",\n"
           << "      \"append_records_per_second\": "
           << writer.append_records_per_second << ",\n"
           << "      \"write_mebibytes_per_second\": "
           << writer.write_mebibytes_per_second << ",\n"
           << "      \"operations_attempted\": "
           << writer.operations_attempted << ",\n"
           << "      \"errors\": " << writer.errors << ",\n"
           << "      \"error_rate\": "
           << ErrorRate(writer.errors, writer.operations_attempted) << ",\n"
           << "      \"flush_latency_ns\": ";
    WriteDistributionJson(output, writer.flush_latency_ns, "      ");
    output << ",\n"
           << "      \"fdatasync\": {\n"
           << "        \"operations_attempted\": "
           << writer.fdatasync_attempts << ",\n"
           << "        \"errors\": " << writer.fdatasync_errors << ",\n"
           << "        \"error_rate\": "
           << ErrorRate(writer.fdatasync_errors, writer.fdatasync_attempts)
           << ",\n"
           << "        \"latency_ns\": ";
    WriteDistributionJson(output, writer.fdatasync_latency_ns, "        ");
    output << "\n      }\n"
           << "    },\n"
           << "    \"recovery_scan\": {\n"
           << "      \"operations_attempted\": "
           << recovery.scan_attempts << ",\n"
           << "      \"errors\": " << recovery.errors << ",\n"
           << "      \"error_rate\": "
           << ErrorRate(recovery.errors, recovery.scan_attempts) << ",\n"
           << "      \"records_per_scan\": "
           << recovery.records_per_scan << ",\n"
           << "      \"bytes_per_scan\": " << recovery.bytes_per_scan
           << ",\n"
           << "      \"total_elapsed_ns\": "
           << recovery.total_elapsed_ns << ",\n"
           << "      \"records_per_second\": "
           << recovery.records_per_second << ",\n"
           << "      \"mebibytes_per_second\": "
           << recovery.mebibytes_per_second << ",\n"
           << "      \"scan_time_ns\": ";
    WriteDistributionJson(output, recovery.scan_time_ns, "      ");
    output << "\n    },\n"
           << "    \"buffer_mpsc\": {\n"
           << "      \"producers\": " << buffer.producers << ",\n"
           << "      \"queue_capacity\": " << buffer.queue_capacity
           << ",\n"
           << "      \"records\": " << buffer.records << ",\n"
           << "      \"elapsed_ns\": " << buffer.elapsed_ns << ",\n"
           << "      \"records_per_second\": "
           << buffer.records_per_second << ",\n"
           << "      \"payload_mebibytes_per_second\": "
           << buffer.payload_mebibytes_per_second << ",\n"
           << "      \"operations_attempted\": "
           << buffer.operations_attempted << ",\n"
           << "      \"errors\": " << buffer.errors << ",\n"
           << "      \"internal_errors\": " << buffer.internal_errors
           << ",\n"
           << "      \"error_rate\": "
           << ErrorRate(buffer.errors, buffer.operations_attempted) << ",\n"
           << "      \"error_detail\": \""
           << JsonEscape(buffer.error_detail) << "\"\n"
           << "    }\n"
           << "  }\n"
           << "}\n";
    return output.str();
}

void PrintDistribution(std::ostream& output,
                       const Distribution& distribution) {
    output << "p50=" << distribution.p50 << " ns, p95=" << distribution.p95
           << " ns, p99=" << distribution.p99
           << " ns, p99.9=" << distribution.p999
           << " ns, max=" << distribution.maximum
           << " ns (samples=" << distribution.count << ')';
}

void PrintHumanSummary(const Config& config, const EncodeMetrics& encode,
                       const WriterMetrics& writer,
                       const RecoveryMetrics& recovery,
                       const BufferMetrics& buffer) {
    std::cerr << std::fixed << std::setprecision(2);
    std::cerr << "Mino storage-sla benchmark\n"
              << "  config: records=" << config.records
              << ", payload=" << config.payload_bytes
              << " B, sync-policy=" << config.sync_policy_name << '\n'
              << "  clock: std::chrono::steady_clock; percentiles: "
                 "nearest-rank\n"
              << "  warmup: encode=" << encode.warmup_records
              << " records, writer=" << writer.warmup_records
              << " records, recovery=" << recovery.warmup_scans
              << " scan, buffer=" << buffer.warmup_records << " records\n"
              << "  EncodeRecord latency: ";
    PrintDistribution(std::cerr, encode.latency_ns);
    std::cerr << '\n'
              << "  SegmentWriter append: "
              << writer.append_records_per_second << " records/s\n"
              << "  SegmentWriter write (end-to-end): "
              << writer.write_mebibytes_per_second << " MiB/s\n"
              << "  Flush latency: ";
    PrintDistribution(std::cerr, writer.flush_latency_ns);
    std::cerr << '\n' << "  fdatasync latency: ";
    PrintDistribution(std::cerr, writer.fdatasync_latency_ns);
    std::cerr << '\n'
              << "  Recovery scan: " << recovery.mebibytes_per_second
              << " MiB/s, " << recovery.records_per_second
              << " records/s, total=" << recovery.total_elapsed_ns
              << " ns across " << recovery.scan_attempts << " samples\n"
              << "  Recovery scan time: ";
    PrintDistribution(std::cerr, recovery.scan_time_ns);
    std::cerr << '\n'
              << "  Buffer MPSC (" << buffer.producers
              << "P/1C): " << buffer.records_per_second << " records/s, "
              << buffer.payload_mebibytes_per_second << " payload MiB/s\n"
              << "  errors: encode="
              << ErrorRate(encode.errors, encode.operations_attempted)
              << ", writer="
              << ErrorRate(writer.errors, writer.operations_attempted)
              << ", fdatasync="
              << ErrorRate(writer.fdatasync_errors,
                           writer.fdatasync_attempts)
              << ", recovery="
              << ErrorRate(recovery.errors, recovery.scan_attempts)
              << ", buffer="
              << ErrorRate(buffer.errors, buffer.operations_attempted) << '\n';
}

void WriteJsonFile(const std::filesystem::path& path,
                   std::string_view json) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        throw std::runtime_error("cannot open JSON output '" + path.string() +
                                 "'");
    }
    output.write(json.data(), static_cast<std::streamsize>(json.size()));
    output.close();
    if (!output) {
        throw std::runtime_error("cannot write JSON output '" + path.string() +
                                 "'");
    }
}

}  // namespace
}  // namespace mino::storage

int main(int argc, char** argv) {
    try {
        const mino::storage::Config config =
            mino::storage::ParseArguments(argc, argv);
        mino::storage::OwnedTemporaryDirectory temporary(config.directory);
        mino::storage::Record record =
            mino::storage::MakeRecord(config.payload_bytes);
        const uint64_t warmup_records =
            mino::storage::WarmupRecords(config.records);

        const mino::storage::EncodeMetrics encode =
            mino::storage::BenchmarkEncode(&record, config.records);

        const std::filesystem::path warmup_segment =
            temporary.path() / "writer-warmup.mino";
        mino::storage::RunWriterWarmup(warmup_segment, &record, warmup_records,
                                       config.sync_policy);

        const std::filesystem::path measured_segment =
            temporary.path() / "writer-measured.mino";
        const mino::storage::WriterMetrics writer =
            mino::storage::RunWriter(measured_segment, &record, config.records,
                                     config.sync_policy, warmup_records);
        const mino::storage::RecoveryMetrics recovery =
            mino::storage::BenchmarkRecovery(
                measured_segment, config.records,
                writer.encoded_bytes + mino::storage::kEncodedSegmentHeaderSize);

        mino::storage::RunBufferWarmup(warmup_records, config.payload_bytes);
        const mino::storage::BufferMetrics buffer = mino::storage::RunBuffer(
            config.records, config.payload_bytes, warmup_records);

        const std::string json = mino::storage::BuildJson(
            config, temporary, encode, writer, recovery, buffer);
        mino::storage::PrintHumanSummary(config, encode, writer, recovery,
                                         buffer);
        std::cout << json;
        if (!std::cout) {
            throw std::runtime_error("cannot write JSON to stdout");
        }
        if (config.output_json.has_value()) {
            mino::storage::WriteJsonFile(*config.output_json, json);
        }
        return buffer.errors == 0 ? 0 : 2;
    } catch (const std::exception& exception) {
        std::cerr << "storage_benchmark: " << exception.what() << '\n';
        return 1;
    }
}
