// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/replay_engine.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "mino/common/status.h"

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

Status IoError(std::string_view operation,
               const std::filesystem::path& path) {
    const int error = errno;
    StatusCode code = StatusCode::kUnavailable;
    if (error == EACCES || error == EPERM || error == EROFS) {
        code = StatusCode::kPermissionDenied;
    } else if (error == ENOENT) {
        code = StatusCode::kNotFound;
    }
    return Status::Error(code, std::string(operation) + " '" + path.string() +
                                   "': " + std::strerror(error));
}



int ReadOnlyFlags() noexcept {
    int flags = O_RDONLY;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    return flags;
}

Status SetCloseOnExec(int fd) {
#ifdef O_CLOEXEC
    static_cast<void>(fd);
    return Status::Ok();
#else
    const int flags = ::fcntl(fd, F_GETFD);
    if (flags < 0 || ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot set replay segment close-on-exec flag");
    }
    return Status::Ok();
#endif
}

Result<bool> ReadExact(int fd, const std::filesystem::path& path,
                       uint64_t offset, std::span<std::byte> output) {
    size_t completed = 0;
    while (completed < output.size()) {
        if (offset > std::numeric_limits<uint64_t>::max() - completed) {
            return Corruption("replay segment read offset overflows uint64");
        }
        const uint64_t current = offset + completed;
        if (current >
            static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
            return Corruption("replay segment offset does not fit off_t");
        }
        const size_t request = std::min(
            output.size() - completed,
            static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
        const ssize_t count =
            ::pread(fd, output.data() + completed, request,
                    static_cast<off_t>(current));
        if (count < 0) {
            if (errno == EINTR) continue;
            return IoError("cannot read replay segment", path);
        }
        if (count == 0) return false;
        completed += static_cast<size_t>(count);
    }
    return true;
}

Status ValidateRange(const ReplayValueRange& range, std::string_view name) {
    if (range.minimum.has_value() && range.maximum.has_value() &&
        *range.minimum > *range.maximum) {
        return Invalid(std::string(name) + " minimum exceeds maximum");
    }
    return Status::Ok();
}

bool Contains(const std::set<uint32_t>& values, uint32_t value) noexcept {
    return values.empty() || values.contains(value);
}

bool Contains(const std::set<uint64_t>& values, uint64_t value) noexcept {
    return values.empty() || values.contains(value);
}

uint64_t PartitionGeneration(const std::filesystem::path& segment_path) noexcept {
    const std::filesystem::path partition_root =
        segment_path.filename() == "snapshot.mino"
            ? segment_path.parent_path()
            : segment_path.parent_path().parent_path();
    const std::filesystem::path partitions = partition_root.parent_path();
    if (partitions.filename() != "partitions") return 0;
    const std::filesystem::path owner = partitions.parent_path();
    if (owner.parent_path().filename() != "generations") return 1;
    const std::string text = owner.filename().string();
    uint64_t generation = 0;
    const auto [end, error] =
        std::from_chars(text.data(), text.data() + text.size(), generation);
    return error == std::errc{} && end == text.data() + text.size() &&
                   generation != 0
               ? generation
               : 0;
}

ReplayOrderKey OrderKey(const RecordHeader& header,
                        uint64_t partition_generation,
                        uint64_t writer_id) noexcept {
    return ReplayOrderKey{
        .ingestion_timestamp_ns = header.ingestion_timestamp_ns,
        .topic_id = header.topic_id,
        .partition_generation = partition_generation,
        .partition_id = header.partition_id,
        .ingestion_sequence = header.ingestion_sequence,
        .writer_id = writer_id,
        .node_id = header.node_id,
        .publisher_id = header.publisher_id,
        .publisher_epoch = header.publisher_epoch,
        .source_sequence = header.source_sequence,
        .observed_timestamp_ns = header.observed_timestamp_ns,
    };
}

struct SchemaKey {
    uint32_t topic_id = 0;
    uint32_t schema_ref = 0;
    uint32_t schema_version = 0;
    uint32_t layout_version = 0;

    bool operator<(const SchemaKey& other) const noexcept {
        return std::tie(topic_id, schema_ref, schema_version, layout_version) <
               std::tie(other.topic_id, other.schema_ref, other.schema_version,
                        other.layout_version);
    }
};

SchemaKey MakeSchemaKey(uint32_t topic_id,
                        const SegmentRecordOffset& record) noexcept {
    return SchemaKey{.topic_id = topic_id,
                     .schema_ref = record.schema_ref,
                     .schema_version = record.schema_version,
                     .layout_version = record.layout_version};
}

SchemaKey MakeSchemaKey(const RecordHeader& record) noexcept {
    return SchemaKey{.topic_id = record.topic_id,
                     .schema_ref = record.schema_ref,
                     .schema_version = record.schema_version,
                     .layout_version = record.layout_version};
}

bool SchemaMatches(const SchemaRefSnapshot& schema,
                   const SchemaKey& key) noexcept {
    return schema.schema_ref == key.schema_ref &&
           schema.schema_version == key.schema_version &&
           schema.layout_version == key.layout_version;
}

}  // namespace

bool ReplayValueRange::Contains(uint64_t value) const noexcept {
    return (!minimum.has_value() || value >= *minimum) &&
           (!maximum.has_value() || value <= *maximum);
}

uint64_t SystemReplayClock::NowNs() noexcept {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

Status SystemReplaySleeper::SleepFor(uint64_t duration_ns) noexcept {
    constexpr uint64_t kMaximumChunk =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    while (duration_ns != 0) {
        const uint64_t chunk = std::min(duration_ns, kMaximumChunk);
        std::this_thread::sleep_for(
            std::chrono::nanoseconds(static_cast<int64_t>(chunk)));
        duration_ns -= chunk;
    }
    return Status::Ok();
}

ManifestReplaySchemaResolver::ManifestReplaySchemaResolver(
    RecordingManifestSnapshot manifest) noexcept
    : manifest_(std::move(manifest)) {}

Result<SchemaRefSnapshot> ManifestReplaySchemaResolver::Resolve(
    uint32_t topic_id, uint32_t schema_ref, uint32_t schema_version,
    uint32_t layout_version) noexcept {
    try {
        for (const TopicTableEntry& topic : manifest_.topics) {
            if (topic.topic_id != topic_id) continue;
            for (const SchemaRefSnapshot& schema : topic.schema_snapshot) {
                if (schema.schema_ref == schema_ref &&
                    schema.schema_version == schema_version &&
                    schema.layout_version == layout_version) {
                    return schema;
                }
            }
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "record references an unknown topic schema");
        }
        return Status::Error(StatusCode::kNotFound,
                             "record references an unknown topic_id");
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

bool ReplayOrderLess::operator()(const ReplayOrderKey& left,
                                 const ReplayOrderKey& right) const noexcept {
    return std::tie(left.ingestion_timestamp_ns, left.topic_id,
                    left.partition_generation, left.partition_id,
                    left.ingestion_sequence, left.writer_id, left.node_id,
                    left.publisher_id, left.publisher_epoch,
                    left.source_sequence, left.observed_timestamp_ns) <
           std::tie(right.ingestion_timestamp_ns, right.topic_id,
                    right.partition_generation, right.partition_id,
                    right.ingestion_sequence, right.writer_id, right.node_id,
                    right.publisher_id, right.publisher_epoch,
                    right.source_sequence, right.observed_timestamp_ns);
}

SegmentReplayReader::SegmentReplayReader(std::filesystem::path path, int fd,
                                         SegmentRecoveryReport report,
                                         SegmentFormatLimits limits) noexcept
    : path_(std::move(path)),
      fd_(fd),
      report_(std::move(report)),
      limits_(limits) {}

SegmentReplayReader::~SegmentReplayReader() {
    if (fd_ >= 0) static_cast<void>(::close(fd_));
}

Result<std::unique_ptr<SegmentReplayReader>> SegmentReplayReader::Open(
    const std::filesystem::path& path, const SegmentFormatLimits& limits) {
    return OpenImpl(path, limits, nullptr, nullptr);
}

Result<std::unique_ptr<SegmentReplayReader>>
SegmentReplayReader::OpenForTesting(
    const std::filesystem::path& path, const SegmentFormatLimits& limits,
    PostScanHookForTesting post_scan_hook, void* hook_context) {
    return OpenImpl(path, limits, post_scan_hook, hook_context);
}

Result<std::unique_ptr<SegmentReplayReader>> SegmentReplayReader::OpenImpl(
    const std::filesystem::path& path, const SegmentFormatLimits& limits,
    PostScanHookForTesting post_scan_hook, void* hook_context) {
    try {
        SegmentRecoveryOptions recovery_options;
        recovery_options.format_limits = limits;
        Result<SegmentRecoveryReport> scanned =
            ScanSegment(path, recovery_options);
        if (!scanned.ok()) return scanned.status();
        if (!scanned->clean()) {
            return Corruption(std::string("replay rejects non-clean segment: ") +
                              scanned->reason_detail);
        }
        if (!scanned->metadata_is_complete) {
            return Corruption("replay requires complete segment metadata");
        }
        if (post_scan_hook != nullptr) {
            const Status hooked = post_scan_hook(path, hook_context);
            if (!hooked.ok()) return hooked;
        }

        const int fd = ::open(path.c_str(), ReadOnlyFlags());
        if (fd < 0) return IoError("cannot open replay segment", path);
        const Status close_on_exec = SetCloseOnExec(fd);
        if (!close_on_exec.ok()) {
            static_cast<void>(::close(fd));
            return close_on_exec;
        }

        struct stat attributes {};
        if (::fstat(fd, &attributes) != 0) {
            const Status status = IoError("cannot stat replay segment", path);
            static_cast<void>(::close(fd));
            return status;
        }
        if (!S_ISREG(attributes.st_mode) ||
            static_cast<uint64_t>(attributes.st_dev) != scanned->file_device ||
            static_cast<uint64_t>(attributes.st_ino) != scanned->file_inode) {
            static_cast<void>(::close(fd));
            return Status::Error(
                StatusCode::kUnavailable,
                "replay segment device/inode changed after validation");
        }
        if (attributes.st_size < 0 ||
            static_cast<uint64_t>(attributes.st_size) != scanned->file_size) {
            static_cast<void>(::close(fd));
            return Corruption("replay segment changed after validation");
        }

        std::array<std::byte, kEncodedSegmentHeaderSize> encoded_header{};
        Result<bool> header_read = ReadExact(fd, path, 0, encoded_header);
        if (!header_read.ok()) {
            static_cast<void>(::close(fd));
            return header_read.status();
        }
        Result<SegmentHeader> header = DecodeSegmentHeader(encoded_header);
        if (!*header_read || !header.ok() ||
            !(*header == scanned->segment_header)) {
            static_cast<void>(::close(fd));
            return Corruption("replay segment header changed after validation");
        }

        return std::unique_ptr<SegmentReplayReader>(new SegmentReplayReader(
            path, fd, std::move(*scanned), limits));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate replay segment reader");
    } catch (const std::length_error&) {
        return Exhausted("replay segment metadata is too large");
    }
}

Result<std::optional<Record>> SegmentReplayReader::Next() {
    try {
        if (next_record_ == report_.records.size()) {
            return std::optional<Record>{};
        }
        const SegmentRecordOffset& metadata = report_.records[next_record_];
        if (metadata.encoded_size > std::numeric_limits<size_t>::max()) {
            return Corruption("record envelope does not fit size_t");
        }
        std::vector<std::byte> envelope(
            static_cast<size_t>(metadata.encoded_size));
        Result<bool> read =
            ReadExact(fd_, path_, metadata.record_offset, envelope);
        if (!read.ok()) return read.status();
        if (!*read) return Corruption("record envelope changed or was truncated");

        Result<Record> decoded = DecodeRecord(envelope, limits_);
        if (!decoded.ok()) return decoded.status();
        const RecordHeader& header = decoded->header;
        if (header.topic_id != report_.segment_header.topic_id ||
            header.partition_id != report_.segment_header.partition_id ||
            header.flags != metadata.flags ||
            header.schema_ref != metadata.schema_ref ||
            header.schema_version != metadata.schema_version ||
            header.layout_version != metadata.layout_version ||
            header.ingestion_sequence != metadata.ingestion_sequence ||
            header.ingestion_timestamp_ns !=
                metadata.ingestion_timestamp_ns ||
            header.source_sequence != metadata.source_sequence ||
            header.observed_timestamp_ns !=
                metadata.observed_timestamp_ns) {
            return Corruption("decoded record disagrees with scanned metadata");
        }
        ++next_record_;
        return std::optional<Record>(std::move(*decoded));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate replay record envelope");
    } catch (const std::length_error&) {
        return Exhausted("replay record envelope is too large");
    }
}

struct ReplayEngine::Impl {
    struct PartitionStream {
        uint32_t topic_id = 0;
        uint64_t partition_generation = 0;
        uint32_t partition_id = 0;
        uint64_t writer_id = 0;
        std::string topic_name;
        std::vector<std::unique_ptr<SegmentReplayReader>> readers;
        size_t reader_index = 0;
    };

    struct Candidate {
        size_t stream_index = 0;
        ReplayOrderKey key;
        Record record;
    };

    struct CandidateLater {
        bool operator()(const Candidate& left,
                        const Candidate& right) const noexcept {
            const ReplayOrderLess less;
            if (less(right.key, left.key)) return true;
            if (less(left.key, right.key)) return false;
            return left.stream_index > right.stream_index;
        }
    };

    RecordingManifestSnapshot manifest;
    ReplayPublisherAdapter* publisher = nullptr;
    ReplayOptions options;
    std::unique_ptr<ManifestReplaySchemaResolver> default_schema_resolver;
    ReplaySchemaResolver* schema_resolver = nullptr;
    SystemReplayClock system_clock;
    SystemReplaySleeper system_sleeper;
    ReplayClock* clock = nullptr;
    ReplaySleeper* sleeper = nullptr;
    std::set<uint32_t> filtered_topics;
    std::set<uint32_t> filtered_partitions;
    std::set<uint64_t> filtered_generations;
    std::set<uint64_t> filtered_nodes;
    std::map<SchemaKey, SchemaRefSnapshot> schemas;
    std::vector<PartitionStream> streams;
    std::priority_queue<Candidate, std::vector<Candidate>, CandidateLater> ready;
    std::optional<Status> terminal_error;
    bool schedule_started = false;
    uint64_t first_record_time_ns = 0;
    uint64_t replay_start_time_ns = 0;

    bool Matches(const PartitionStream& stream,
                 const RecordHeader& header) const noexcept {
        return Contains(filtered_topics, header.topic_id) &&
               Contains(filtered_partitions, header.partition_id) &&
               Contains(filtered_generations,
                        stream.partition_generation) &&
               Contains(filtered_nodes, header.node_id) &&
               options.filter.ingestion_timestamp_ns.Contains(
                   header.ingestion_timestamp_ns) &&
               options.filter.observed_timestamp_ns.Contains(
                   header.observed_timestamp_ns) &&
               options.filter.source_sequence.Contains(header.source_sequence) &&
               options.filter.ingestion_sequence.Contains(
                   header.ingestion_sequence);
    }

    Result<std::optional<Record>> NextMatching(PartitionStream* stream) {
        while (stream->reader_index < stream->readers.size()) {
            Result<std::optional<Record>> next =
                stream->readers[stream->reader_index]->Next();
            if (!next.ok()) return next.status();
            if (!next->has_value()) {
                ++stream->reader_index;
                continue;
            }
            // Gap records describe a discontinuity in the recording and have no
            // application schema. They remain visible to inspect/verify tools but
            // are not republished as application messages.
            if ((next->value().header.flags & kRecordFlagGap) != 0) continue;
            if (Matches(*stream, next->value().header)) {
                return std::move(*next);
            }
        }
        return std::optional<Record>{};
    }

    Status PrimeStream(size_t stream_index) {
        Result<std::optional<Record>> next =
            NextMatching(&streams[stream_index]);
        if (!next.ok()) return next.status();
        if (!next->has_value()) return Status::Ok();
        Record record = std::move(next->value());
        ready.push(Candidate{
                             .stream_index = stream_index,
                             .key = OrderKey(record.header,
                                             streams[stream_index]
                                                 .partition_generation,
                                             streams[stream_index].writer_id),
                             .record = std::move(record)});
        return Status::Ok();
    }

    Result<uint64_t> Schedule(uint64_t record_time_ns) {
        if (options.playback.mode == ReplayPlaybackMode::kStep) {
            return clock->NowNs();
        }
        if (!schedule_started) {
            schedule_started = true;
            first_record_time_ns = record_time_ns;
            replay_start_time_ns = clock->NowNs();
            return replay_start_time_ns;
        }

        const uint64_t source_delta =
            record_time_ns >= first_record_time_ns
                ? record_time_ns - first_record_time_ns
                : 0;
        const long double scaled_value =
            static_cast<long double>(source_delta) /
            static_cast<long double>(options.playback.speed);
        const uint64_t scaled_delta =
            scaled_value >=
                    static_cast<long double>(
                        std::numeric_limits<uint64_t>::max())
                ? std::numeric_limits<uint64_t>::max()
                : static_cast<uint64_t>(scaled_value);
        const uint64_t target =
            scaled_delta > std::numeric_limits<uint64_t>::max() -
                               replay_start_time_ns
                ? std::numeric_limits<uint64_t>::max()
                : replay_start_time_ns + scaled_delta;
        const uint64_t now = clock->NowNs();
        if (now < target) {
            const Status slept = sleeper->SleepFor(target - now);
            if (!slept.ok()) return slept;
        }
        return clock->NowNs();
    }

    Result<bool> ReplayOne() {
        if (terminal_error.has_value()) return *terminal_error;
        if (publisher == nullptr) {
            return Status::Error(
                StatusCode::kUnsupported,
                "no normal replay publisher adapter is installed");
        }
        if (ready.empty()) return false;

        const Candidate& candidate = ready.top();
        const RecordHeader& header = candidate.record.header;
        const auto schema = schemas.find(MakeSchemaKey(header));
        if (schema == schemas.end()) {
            terminal_error = Status::Error(
                StatusCode::kSchemaMismatch,
                "schema disappeared after replay prevalidation");
            return *terminal_error;
        }

        Result<uint64_t> replay_time =
            Schedule(header.ingestion_timestamp_ns);
        if (!replay_time.ok()) {
            terminal_error = replay_time.status();
            return *terminal_error;
        }
        const PartitionStream& stream = streams[candidate.stream_index];
        const std::string& target_namespace =
            options.publish_target == ReplayPublishTarget::kLive
                ? options.live_namespace
                : options.replay_namespace;
        const ReplayMessageMetadata metadata{
            .message_origin = ReplayMessageOrigin::kReplay,
            .replay_session_id = options.replay_session_id,
            .original_ingestion_timestamp_ns =
                header.ingestion_timestamp_ns,
            .original_observed_timestamp_ns = header.observed_timestamp_ns,
            .replay_timestamp_ns = *replay_time,
            .publish_timestamp_ns =
                options.timestamp_mode ==
                        ReplayTimestampMode::kPreserveOriginal
                    ? header.ingestion_timestamp_ns
                    : *replay_time,
            .original_source_sequence = header.source_sequence,
            .original_ingestion_sequence = header.ingestion_sequence,
            .original_partition_generation = stream.partition_generation,
            .original_node_id = header.node_id,
            .original_publisher_id = header.publisher_id,
            .original_publisher_epoch = header.publisher_epoch,
            .record_flags = header.flags,
        };
        const ReplayPublishRequest request{
            .target_namespace = target_namespace,
            .topic_name = stream.topic_name,
            .topic_id = header.topic_id,
            .partition_id = header.partition_id,
            .schema = schema->second,
            .canonical_payload = candidate.record.payload,
            .metadata = metadata,
        };
        const Status published = publisher->Publish(request);
        if (!published.ok()) {
            terminal_error = published;
            return *terminal_error;
        }

        const size_t stream_index = candidate.stream_index;
        ready.pop();
        const Status primed = PrimeStream(stream_index);
        if (!primed.ok()) {
            // Publish already crossed the external side-effect boundary. Report
            // that success now and surface prefetch damage on the next advance.
            terminal_error = primed;
        }
        return true;
    }
};

ReplayEngine::ReplayEngine(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ReplayEngine::~ReplayEngine() = default;

Result<std::unique_ptr<ReplayEngine>> ReplayEngine::Create(
    std::vector<std::filesystem::path> segment_paths,
    RecordingManifestSnapshot manifest, ReplayPublisherAdapter* publisher,
    ReplayOptions options, ReplaySchemaResolver* schema_resolver,
    ReplayClock* clock, ReplaySleeper* sleeper) {
    try {
        if (!std::isfinite(options.playback.speed) ||
            options.playback.speed <= 0.0) {
            return Invalid("replay speed must be finite and greater than zero");
        }
        if (options.publish_target == ReplayPublishTarget::kLive &&
            !options.live_injection_authorized) {
            return Status::Error(
                StatusCode::kPermissionDenied,
                "live replay injection requires explicit authorization");
        }
        if (options.publish_target == ReplayPublishTarget::kReplayNamespace &&
            options.replay_namespace.empty()) {
            return Invalid("replay namespace is empty");
        }
        if (options.publish_target == ReplayPublishTarget::kLive &&
            options.live_namespace.empty()) {
            return Invalid("live namespace is empty");
        }
        MINO_RETURN_IF_ERROR(ValidateRange(
            options.filter.ingestion_timestamp_ns,
            "ingestion timestamp filter"));
        MINO_RETURN_IF_ERROR(ValidateRange(
            options.filter.observed_timestamp_ns,
            "observed timestamp filter"));
        MINO_RETURN_IF_ERROR(
            ValidateRange(options.filter.source_sequence,
                          "source sequence filter"));
        MINO_RETURN_IF_ERROR(
            ValidateRange(options.filter.ingestion_sequence,
                          "ingestion sequence filter"));

        auto impl = std::make_unique<Impl>();
        impl->manifest = std::move(manifest);
        impl->publisher = publisher;
        impl->options = std::move(options);
        impl->clock = clock == nullptr ? &impl->system_clock : clock;
        impl->sleeper = sleeper == nullptr ? &impl->system_sleeper : sleeper;
        if (schema_resolver == nullptr) {
            impl->default_schema_resolver =
                std::make_unique<ManifestReplaySchemaResolver>(impl->manifest);
            impl->schema_resolver = impl->default_schema_resolver.get();
        } else {
            impl->schema_resolver = schema_resolver;
        }

        std::map<uint32_t, const TopicTableEntry*> topics;
        std::map<std::string, uint32_t> topic_names;
        for (const TopicTableEntry& topic : impl->manifest.topics) {
            if (!topics.emplace(topic.topic_id, &topic).second ||
                !topic_names.emplace(topic.topic_name, topic.topic_id).second) {
                return Corruption("recording manifest has duplicate topics");
            }
        }
        for (uint32_t topic_id : impl->options.filter.topic_ids) {
            if (!topics.contains(topic_id)) {
                return Status::Error(StatusCode::kNotFound,
                                     "topic filter contains an unknown topic_id");
            }
            impl->filtered_topics.insert(topic_id);
        }
        for (const std::string& name : impl->options.filter.topic_names) {
            const auto found = topic_names.find(name);
            if (found == topic_names.end()) {
                return Status::Error(StatusCode::kNotFound,
                                     "topic filter contains an unknown name");
            }
            impl->filtered_topics.insert(found->second);
        }
        impl->filtered_partitions.insert(
            impl->options.filter.partition_ids.begin(),
            impl->options.filter.partition_ids.end());
        impl->filtered_generations.insert(
            impl->options.filter.partition_generations.begin(),
            impl->options.filter.partition_generations.end());
        impl->filtered_nodes.insert(impl->options.filter.node_ids.begin(),
                                    impl->options.filter.node_ids.end());

        using PartitionKey =
            std::tuple<uint32_t, uint64_t, uint32_t>;
        std::map<PartitionKey,
                 std::vector<std::unique_ptr<SegmentReplayReader>>>
            grouped;
        for (const std::filesystem::path& path : segment_paths) {
            Result<std::unique_ptr<SegmentReplayReader>> reader =
                SegmentReplayReader::Open(path, impl->options.format_limits);
            if (!reader.ok()) return reader.status();
            const SegmentHeader& header = (*reader)->segment_header();
            if (header.recording_id != impl->manifest.session.recording_id) {
                return Corruption("segment recording_id differs from manifest");
            }
            if (!topics.contains(header.topic_id)) {
                return Corruption("segment topic_id is absent from manifest");
            }
            const uint64_t generation = PartitionGeneration(path);
            const TopicTableEntry& topic = *topics.at(header.topic_id);
            if (!topic.partition_maps.empty()) {
                const auto map = std::find_if(
                    topic.partition_maps.begin(), topic.partition_maps.end(),
                    [generation](const TopicPartitionMap& candidate) {
                        return candidate.generation == generation;
                    });
                if (map == topic.partition_maps.end() ||
                    header.partition_id >= map->partition_count) {
                    return Corruption(
                        "segment partition generation is absent from manifest");
                }
            }
            auto& partition_readers =
                grouped[{header.topic_id, generation, header.partition_id}];
            if (!partition_readers.empty() &&
                partition_readers.front()->segment_header().writer_id !=
                    header.writer_id) {
                return Corruption(
                    "partition generation contains multiple writer IDs");
            }
            partition_readers.push_back(std::move(*reader));
        }

        for (auto& [partition, readers] : grouped) {
            std::sort(readers.begin(), readers.end(),
                      [](const std::unique_ptr<SegmentReplayReader>& left,
                         const std::unique_ptr<SegmentReplayReader>& right) {
                          const SegmentHeader& left_header =
                              left->segment_header();
                          const SegmentHeader& right_header =
                              right->segment_header();
                          return std::tie(
                                     left_header.first_ingestion_sequence,
                                     left_header.created_at_ns, left->path()) <
                                 std::tie(
                                     right_header.first_ingestion_sequence,
                                     right_header.created_at_ns, right->path());
                      });

            std::optional<uint64_t> previous_sequence;
            for (const auto& reader : readers) {
                for (const SegmentRecordOffset& record :
                     reader->record_metadata()) {
                    if (previous_sequence.has_value() &&
                        record.ingestion_sequence <= *previous_sequence) {
                        return Corruption(
                            "partition segments overlap or regress ingestion sequence");
                    }
                    previous_sequence = record.ingestion_sequence;
                    if ((record.flags & kRecordFlagGap) != 0) continue;
                    const SchemaKey key =
                        MakeSchemaKey(std::get<0>(partition), record);
                    if (!impl->schemas.contains(key)) {
                        Result<SchemaRefSnapshot> resolved =
                            impl->schema_resolver->Resolve(
                                key.topic_id, key.schema_ref,
                                key.schema_version, key.layout_version);
                        if (!resolved.ok()) return resolved.status();
                        if (!SchemaMatches(*resolved, key)) {
                            return Status::Error(
                                StatusCode::kSchemaMismatch,
                                "schema resolver returned a different layout");
                        }
                        impl->schemas.emplace(key, std::move(*resolved));
                    }
                }
            }

            const TopicTableEntry& topic = *topics.at(std::get<0>(partition));
            impl->streams.push_back(Impl::PartitionStream{
                .topic_id = std::get<0>(partition),
                .partition_generation = std::get<1>(partition),
                .partition_id = std::get<2>(partition),
                .writer_id = readers.front()->segment_header().writer_id,
                .topic_name = topic.topic_name,
                .readers = std::move(readers),
                .reader_index = 0,
            });
        }

        for (size_t index = 0; index < impl->streams.size(); ++index) {
            const Status primed = impl->PrimeStream(index);
            if (!primed.ok()) return primed;
        }
        return std::unique_ptr<ReplayEngine>(
            new ReplayEngine(std::move(impl)));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate replay engine state");
    } catch (const std::length_error&) {
        return Exhausted("replay engine input is too large");
    }
}

Status ReplayEngine::InstallPublisherAdapter(
    ReplayPublisherAdapter* publisher) noexcept {
    if (publisher == nullptr) return Invalid("replay publisher is null");
    if (published_records_ != 0 || impl_->schedule_started) {
        return Invalid("replay publisher cannot change after playback starts");
    }
    impl_->publisher = publisher;
    return Status::Ok();
}

bool ReplayEngine::has_publisher_adapter() const noexcept {
    return impl_->publisher != nullptr;
}

Result<size_t> ReplayEngine::Run() {
    if (impl_->options.playback.mode == ReplayPlaybackMode::kStep) {
        return Invalid("Run is unavailable in step mode; call Step");
    }
    try {
        size_t published_now = 0;
        while (true) {
            Result<bool> replayed = impl_->ReplayOne();
            if (!replayed.ok()) return replayed.status();
            if (!*replayed) break;
            ++published_now;
            ++published_records_;
        }
        return published_now;
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot advance replay queue");
    } catch (const std::length_error&) {
        return Exhausted("replay queue is too large");
    }
}

Result<bool> ReplayEngine::Step() {
    if (impl_->options.playback.mode != ReplayPlaybackMode::kStep) {
        return Invalid("Step is available only in step mode");
    }
    try {
        Result<bool> replayed = impl_->ReplayOne();
        if (replayed.ok() && *replayed) ++published_records_;
        return replayed;
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot advance replay queue");
    } catch (const std::length_error&) {
        return Exhausted("replay queue is too large");
    }
}

}  // namespace mino::storage
