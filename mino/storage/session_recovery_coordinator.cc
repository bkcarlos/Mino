// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/session_recovery_coordinator.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <iomanip>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <utility>

#include "mino/common/status.h"
#include "mino/schema/registry.h"

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status Unavailable(const std::string& message) {
    return Status::Error(StatusCode::kUnavailable, message);
}

Status IoError(std::string_view operation,
               const std::filesystem::path& path) {
    return Unavailable(std::string(operation) + " '" + path.string() +
                       "': " + std::strerror(errno));
}

int OpenFlags(int flags) noexcept {
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    return flags;
}

Status ValidateNoSymlinkAncestors(const std::filesystem::path& path,
                                  bool require_directory) {
    std::error_code error;
    const std::filesystem::path absolute =
        std::filesystem::absolute(path, error).lexically_normal();
    if (error || absolute.empty()) {
        return Invalid("recovery path cannot be made absolute");
    }
    std::filesystem::path current = absolute.root_path();
    for (const std::filesystem::path& component : absolute.relative_path()) {
        current /= component;
        struct stat info {};
        if (::lstat(current.c_str(), &info) != 0) {
            return IoError("cannot inspect recovery path", current);
        }
        if (S_ISLNK(info.st_mode)) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "recovery path traverses a symbolic link");
        }
    }
    struct stat final_info {};
    if (::lstat(absolute.c_str(), &final_info) != 0) {
        return IoError("cannot inspect recovery root", absolute);
    }
    if (require_directory && !S_ISDIR(final_info.st_mode)) {
        return Invalid("recovery root is not a directory");
    }
    return Status::Ok();
}

std::filesystem::path ExpectedPartitionRoot(
    const std::filesystem::path& session_root, uint32_t topic_id,
    uint64_t generation, uint32_t partition_id) {
    std::ostringstream partition_name;
    partition_name << std::setfill('0') << std::setw(4) << partition_id;
    std::filesystem::path topic_root =
        session_root / "topics" / std::to_string(topic_id);
    if (generation == 1) {
        return (topic_root / "partitions" / partition_name.str())
            .lexically_normal();
    }
    std::ostringstream generation_name;
    generation_name << std::setfill('0') << std::setw(20) << generation;
    return (topic_root / "generations" / generation_name.str() /
            "partitions" / partition_name.str())
        .lexically_normal();
}

uint64_t RecoveryNowNs(uint64_t configured) noexcept {
    if (configured != 0) return configured;
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    return elapsed.count() <= 0 ? 1 : static_cast<uint64_t>(elapsed.count());
}

std::optional<uint64_t> ParseSegmentId(const std::filesystem::path& path) {
    const std::string stem = path.stem().string();
    if (stem.empty()) return std::nullopt;
    uint64_t value = 0;
    const auto [end, error] =
        std::from_chars(stem.data(), stem.data() + stem.size(), value);
    if (error != std::errc{} || end != stem.data() + stem.size() || value == 0) {
        return std::nullopt;
    }
    return value;
}

Status ValidateHeader(const SegmentRecoveryReport& report,
                      const PartitionMetadata& partition) {
    const SegmentHeader& header = report.segment_header;
    if (header.recording_id != partition.recording_id ||
        header.topic_id != partition.topic_id ||
        header.partition_id != partition.partition_id ||
        header.writer_id != partition.writer_id) {
        return Corruption("segment identity differs from partition manifest");
    }
    return Status::Ok();
}

Status ValidateTracked(const SegmentRecoveryReport& report,
                       const PartitionMetadata& partition,
                       const SegmentManifestEntry& segment) {
    MINO_RETURN_IF_ERROR(ValidateHeader(report, partition));
    if (report.segment_header.first_ingestion_sequence !=
            segment.first_ingestion_sequence ||
        report.segment_header.created_at_ns != segment.created_at_ns) {
        return Corruption("tracked segment header differs from manifest");
    }
    return Status::Ok();
}

bool SameSchema(const SchemaRefSnapshot& snapshot,
                const SchemaStoreEntry& stored,
                const std::filesystem::path& session_root) {
    std::error_code error;
    const std::filesystem::path relative =
        std::filesystem::relative(stored.descriptor_path, session_root, error);
    return !error && snapshot.schema_ref == stored.ref &&
           snapshot.canonical_digest == stored.identity.canonical_digest() &&
           snapshot.schema_version == stored.identity.schema_version() &&
           snapshot.layout_version == stored.identity.layout_version() &&
           snapshot.descriptor_path == relative;
}

}  // namespace

class SessionRecoveryCoordinator::Impl final {
public:
    Impl(std::filesystem::path root, SessionRecoveryOptions options,
         std::unique_ptr<RecordingManifest> manifest,
         std::unique_ptr<schema::SchemaRegistry> registry,
         std::unique_ptr<SchemaStore> schema_store) noexcept
        : root_(std::move(root)),
          options_(std::move(options)),
          manifest_(std::move(manifest)),
          registry_(std::move(registry)),
          schema_store_(std::move(schema_store)),
          recovery_now_ns_(RecoveryNowNs(options_.recovery_timestamp_ns)) {}

    Result<SessionRecoveryReport> Recover() {
        SessionRecoveryReport report;
        report.recording_id = manifest_->snapshot().session.recording_id;
        report.manifest_generation = manifest_->snapshot().generation;
        MINO_RETURN_IF_ERROR(ValidateSchemas(&report));

        std::vector<std::filesystem::path> partition_roots;
        MINO_ASSIGN_OR_RETURN(partition_roots, DiscoverPartitions());
        report.durable_boundaries.reserve(partition_roots.size());
        for (const std::filesystem::path& root : partition_roots) {
            Result<DurableBoundaryReport> boundary = RecoverPartition(root);
            if (!boundary.ok()) return boundary.status();
            ++report.partitions_recovered;
            report.segments_scanned += boundary->tracked_segments_scanned +
                                       boundary->orphan_candidates_scanned;
            report.repaired_segments += boundary->repaired_segments;
            report.adopted_sealed_orphans +=
                boundary->adopted_sealed_orphans;
            report.quarantined_orphans += boundary->quarantined_orphans;
            report.durable_boundaries.push_back(std::move(*boundary));
        }
        return report;
    }

private:
    Status ValidateSchemas(SessionRecoveryReport* report) {
        for (const TopicTableEntry& topic : manifest_->snapshot().topics) {
            for (const SchemaRefSnapshot& snapshot : topic.schema_snapshot) {
                Result<SchemaStoreEntry> stored =
                    schema_store_->Resolve(snapshot.schema_ref);
                if (!stored.ok()) return stored.status();
                if (!SameSchema(snapshot, *stored, root_)) {
                    return Status::Error(
                        StatusCode::kSchemaMismatch,
                        "recording manifest schema ref differs from SchemaStore");
                }
                ++report->schema_refs_validated;
            }
        }
        return Status::Ok();
    }

    Result<std::vector<std::filesystem::path>> DiscoverPartitions() {
        std::vector<std::filesystem::path> roots;
        auto append_partitions = [&](const std::filesystem::path& partitions,
                                     bool required) -> Status {
            std::error_code error;
            if (!std::filesystem::is_directory(partitions, error) || error) {
                if (!required && !error) return Status::Ok();
                return Corruption(
                    "recording topic partitions directory is missing");
            }
            MINO_RETURN_IF_ERROR(
                ValidateNoSymlinkAncestors(partitions, true));
            for (const auto& entry :
                 std::filesystem::directory_iterator(partitions, error)) {
                if (error) {
                    return Unavailable("cannot enumerate partitions: " +
                                       error.message());
                }
                const std::filesystem::file_status status =
                    entry.symlink_status(error);
                if (error) {
                    return Unavailable("cannot inspect partition: " +
                                       error.message());
                }
                if (std::filesystem::is_symlink(status) ||
                    !std::filesystem::is_directory(status)) {
                    return Corruption(
                        "partition entry is not a real directory");
                }
                MINO_RETURN_IF_ERROR(
                    ValidateNoSymlinkAncestors(entry.path(), true));
                roots.push_back(entry.path());
                if (roots.size() > options_.max_partitions) {
                    return Status::Error(StatusCode::kResourceExhausted,
                                         "session has too many partitions");
                }
            }
            return Status::Ok();
        };

        for (const TopicTableEntry& topic : manifest_->snapshot().topics) {
            const std::filesystem::path topic_root =
                root_ / "topics" / std::to_string(topic.topic_id);
            const bool legacy_required = topic.partition_maps.empty() ||
                std::any_of(topic.partition_maps.begin(),
                            topic.partition_maps.end(),
                            [](const TopicPartitionMap& map) {
                                return map.generation == 1;
                            });
            MINO_RETURN_IF_ERROR(append_partitions(
                topic_root / "partitions", legacy_required));

            const std::filesystem::path generations = topic_root / "generations";
            std::error_code error;
            if (!std::filesystem::exists(generations, error)) {
                if (error) {
                    return Unavailable("cannot inspect partition generations: " +
                                       error.message());
                }
                continue;
            }
            MINO_RETURN_IF_ERROR(
                ValidateNoSymlinkAncestors(generations, true));
            for (const auto& generation :
                 std::filesystem::directory_iterator(generations, error)) {
                if (error) {
                    return Unavailable("cannot enumerate generations: " +
                                       error.message());
                }
                const std::filesystem::file_status status =
                    generation.symlink_status(error);
                if (error || std::filesystem::is_symlink(status) ||
                    !std::filesystem::is_directory(status)) {
                    return Corruption(
                        "partition generation is not a real directory");
                }
                MINO_RETURN_IF_ERROR(append_partitions(
                    generation.path() / "partitions", true));
            }
        }
        std::sort(roots.begin(), roots.end());
        return roots;
    }

    Result<std::unordered_set<uint32_t>> KnownSchemaRefs(uint32_t topic_id) {
        std::unordered_set<uint32_t> refs;
        Result<TopicTableEntry> topic = manifest_->FindTopic(topic_id);
        if (!topic.ok()) return topic.status();
        refs.reserve(topic->schema_snapshot.size());
        for (const SchemaRefSnapshot& schema : topic->schema_snapshot) {
            refs.insert(schema.schema_ref);
        }
        return refs;
    }

    Result<std::vector<std::filesystem::path>> SegmentCandidates(
        const std::filesystem::path& partition_root) {
        const std::filesystem::path directory = partition_root / "segments";
        std::error_code error;
        if (!std::filesystem::is_directory(directory, error) || error) {
            return Corruption("partition segments directory is missing");
        }
        std::vector<std::filesystem::path> result;
        for (const auto& entry : std::filesystem::directory_iterator(directory,
                                                                      error)) {
            if (error) return Unavailable("cannot enumerate segments: " + error.message());
            const std::filesystem::file_status status =
                entry.symlink_status(error);
            if (error) return Unavailable("cannot inspect segment: " + error.message());
            if (std::filesystem::is_symlink(status)) {
                return Corruption("segment candidate is a symbolic link");
            }
            if (!std::filesystem::is_regular_file(status)) continue;
            if (entry.path().extension() == ".mino") {
                result.push_back(entry.path());
                if (result.size() >
                    options_.max_segment_candidates_per_partition) {
                    return Status::Error(StatusCode::kResourceExhausted,
                                         "partition has too many segment candidates");
                }
            }
        }
        std::sort(result.begin(), result.end());
        return result;
    }

    Result<SegmentRecoveryReport> Scan(
        const std::filesystem::path& path,
        const std::unordered_set<uint32_t>& known_schema_refs) {
        SegmentRecoveryOptions options = options_.segment_recovery_options;
        options.known_schema_refs = &known_schema_refs;
        return ScanSegment(path, options);
    }

    Result<SegmentRecoveryReport> Repair(
        const std::filesystem::path& path,
        const std::unordered_set<uint32_t>& known_schema_refs,
        const SegmentRecoveryReport& validated) {
        SegmentRecoveryOptions options = options_.segment_recovery_options;
        options.known_schema_refs = &known_schema_refs;
        SegmentRepairOptions repair = options_.segment_repair_options;
        repair.expected_device = validated.file_device;
        repair.expected_inode = validated.file_inode;
        return RepairSegment(path, options, repair);
    }

    Status ValidateRecordSchemas(const SegmentRecoveryReport& report,
                                 const TopicTableEntry& topic) {
        for (const SegmentRecordOffset& record : report.records) {
            if ((record.flags & kRecordFlagGap) != 0) continue;
            const auto schema = std::find_if(
                topic.schema_snapshot.begin(), topic.schema_snapshot.end(),
                [&record](const SchemaRefSnapshot& candidate) {
                    return candidate.schema_ref == record.schema_ref;
                });
            if (schema == topic.schema_snapshot.end() ||
                schema->schema_version != record.schema_version ||
                schema->layout_version != record.layout_version) {
                return Status::Error(
                    StatusCode::kSchemaMismatch,
                    "segment record schema ref/version/layout differs from manifest");
            }
        }
        return Status::Ok();
    }

    Status SyncDirectory(const std::filesystem::path& path) {
        int flags = O_RDONLY;
#ifdef O_DIRECTORY
        flags |= O_DIRECTORY;
#endif
        const int fd = ::open(path.c_str(), OpenFlags(flags));
        if (fd < 0) return IoError("cannot open recovery directory", path);
        int result = 0;
        do {
            result = options_.directory_sync_hook == nullptr
                         ? ::fsync(fd)
                         : options_.directory_sync_hook(
                               fd, options_.directory_sync_hook_context);
        } while (result != 0 && errno == EINTR);
        const int saved_errno = errno;
        static_cast<void>(::close(fd));
        errno = saved_errno;
        if (result != 0) return IoError("cannot sync recovery directory", path);
        return Status::Ok();
    }

    Status SyncDurableSegment(const std::filesystem::path& path,
                              uint64_t size,
                              const SegmentRecoveryReport& validated) {
        SegmentRepairOptions repair = options_.segment_repair_options;
        repair.expected_device = validated.file_device;
        repair.expected_inode = validated.file_inode;
        MINO_RETURN_IF_ERROR(TruncateSegment(path, size, repair));
        return SyncDirectory(path.parent_path());
    }

    Status AppendHeaderOnlyRecoveryGap(
        const std::filesystem::path& path,
        const PartitionMetadata& partition,
        const SegmentManifestEntry& segment,
        const SegmentRecoveryReport& validated) {
        Record gap;
        gap.header.flags = kRecordFlagGap;
        gap.header.topic_id = partition.topic_id;
        gap.header.partition_id = partition.partition_id;
        gap.header.ingestion_sequence = segment.first_ingestion_sequence;
        gap.header.ingestion_timestamp_ns = recovery_now_ns_;
        gap.payload.clear();
        Result<std::vector<std::byte>> encoded = EncodeRecord(gap);
        if (!encoded.ok()) return encoded.status();

        const int fd = ::open(path.c_str(), OpenFlags(O_WRONLY));
        if (fd < 0) return IoError("cannot open header-only segment", path);
        struct stat info {};
        if (::fstat(fd, &info) != 0 || !S_ISREG(info.st_mode) ||
            info.st_size < 0 ||
            static_cast<uint64_t>(info.st_dev) != validated.file_device ||
            static_cast<uint64_t>(info.st_ino) != validated.file_inode ||
            static_cast<uint64_t>(info.st_size) != kEncodedSegmentHeaderSize) {
            const int saved_errno = errno;
            static_cast<void>(::close(fd));
            errno = saved_errno;
            return Corruption("header-only segment changed during recovery");
        }
        size_t completed = 0;
        while (completed < encoded->size()) {
            const ssize_t count = ::pwrite(
                fd, encoded->data() + completed, encoded->size() - completed,
                static_cast<off_t>(kEncodedSegmentHeaderSize + completed));
            if (count < 0) {
                if (errno == EINTR) continue;
                const Status failure =
                    IoError("cannot append recovery gap", path);
                static_cast<void>(::close(fd));
                return failure;
            }
            if (count == 0) {
                static_cast<void>(::close(fd));
                return Unavailable("zero-byte recovery gap write");
            }
            completed += static_cast<size_t>(count);
        }
        if (::close(fd) != 0) return IoError("cannot close recovered segment", path);
        return Status::Ok();
    }

    Status PublishBoundary(PartitionManifest& manifest,
                           const SegmentManifestEntry& segment,
                           const SegmentRecoveryReport& scanned) {
        if (!scanned.has_last_complete_sequence) {
            return Corruption("durable segment contains no complete record");
        }
        const DurableCheckpoint candidate{
            .segment_id = segment.segment_id,
            .durable_offset = scanned.file_size,
            .durable_sequence = scanned.last_complete_sequence,
        };
        if (manifest.snapshot().checkpoint.has_value()) {
            const DurableCheckpoint& current = *manifest.snapshot().checkpoint;
            if (candidate.segment_id < current.segment_id) {
                return Status::Ok();
            }
            if (candidate.segment_id == current.segment_id &&
                (candidate.durable_offset < current.durable_offset ||
                 candidate.durable_sequence < current.durable_sequence)) {
                return Corruption(
                    "recovered segment is behind the durable checkpoint");
            }
        }
        return manifest.UpdateCheckpoint(candidate);
    }

    Result<DurableBoundaryReport> RecoverPartition(
        const std::filesystem::path& partition_root) {
        Result<std::unique_ptr<PartitionManifest>> opened =
            PartitionManifest::Open(partition_root, options_.manifest_options);
        if (!opened.ok()) return opened.status();
        PartitionManifest& manifest = **opened;
        PartitionMetadata partition = manifest.snapshot().partition;
        const RecordingSessionMetadata& session = manifest_->snapshot().session;
        if (partition.recording_id != session.recording_id ||
            partition.owner_epoch != session.owner_epoch) {
            return Corruption("partition identity differs from session manifest");
        }
        Result<TopicTableEntry> topic = manifest_->FindTopic(partition.topic_id);
        if (!topic.ok()) return Corruption("partition topic is absent from session");
        if (manifest.snapshot().format_version == 1) {
            PartitionMetadata upgraded = partition;
            if (!topic->partition_maps.empty()) {
                const auto map = std::find_if(
                    topic->partition_maps.begin(), topic->partition_maps.end(),
                    [&partition](const TopicPartitionMap& candidate) {
                        return candidate.generation ==
                               partition.partition_generation;
                    });
                if (map == topic->partition_maps.end()) {
                    return Corruption(
                        "legacy partition generation is absent from topic map");
                }
                upgraded.partition_map_version = map->map_version;
                upgraded.partition_generation = map->generation;
                upgraded.partition_count = map->partition_count;
                upgraded.partition_strategy = map->strategy;
                upgraded.partition_map_state = map->state;
                upgraded.hash_algorithm_version =
                    map->hash_algorithm_version;
                upgraded.hash_seed = map->hash_seed;
            } else {
                const std::filesystem::path siblings =
                    partition_root.parent_path();
                std::error_code count_error;
                size_t count = 0;
                for (const auto& entry :
                     std::filesystem::directory_iterator(siblings,
                                                          count_error)) {
                    if (count_error) break;
                    if (entry.is_directory(count_error) && !count_error) ++count;
                }
                if (count_error || count == 0 ||
                    count > kMaximumTopicPartitions) {
                    return Corruption(
                        "cannot infer legacy topic partition count");
                }
                upgraded.partition_count = static_cast<uint32_t>(count);
            }
            MINO_RETURN_IF_ERROR(
                manifest.UpgradeLegacyMetadata(upgraded));
            partition = upgraded;
        }
        if (partition.config_version > topic->config_version) {
            return Corruption("partition config version exceeds topic manifest");
        }
        if (!topic->partition_maps.empty()) {
            const auto partition_map = std::find_if(
                topic->partition_maps.begin(), topic->partition_maps.end(),
                [&partition](const TopicPartitionMap& map) {
                    return map.generation == partition.partition_generation;
                });
            if (partition_map == topic->partition_maps.end() ||
                partition_map->map_version != partition.partition_map_version ||
                partition_map->partition_count != partition.partition_count ||
                partition_map->strategy != partition.partition_strategy ||
                partition_map->hash_algorithm_version !=
                    partition.hash_algorithm_version ||
                partition_map->hash_seed != partition.hash_seed) {
                return Corruption(
                    "partition map identity differs from session manifest");
            }
        }
        std::error_code path_error;
        const std::filesystem::path actual_root =
            std::filesystem::absolute(partition_root, path_error).lexically_normal();
        if (path_error) return Unavailable("cannot resolve partition path");
        const std::filesystem::path expected_root = std::filesystem::absolute(
            ExpectedPartitionRoot(root_, partition.topic_id,
                                  partition.partition_generation,
                                  partition.partition_id),
            path_error).lexically_normal();
        if (path_error || actual_root != expected_root) {
            return Corruption("partition manifest identity differs from its path");
        }
        Result<std::unordered_set<uint32_t>> known =
            KnownSchemaRefs(partition.topic_id);
        if (!known.ok()) return known.status();

        DurableBoundaryReport boundary{
            .topic_id = partition.topic_id,
            .partition_id = partition.partition_id,
            .partition_generation = partition.partition_generation,
            .partition_map_version = partition.partition_map_version,
        };
        std::set<std::filesystem::path> tracked;
        std::vector<SegmentManifestEntry> tracked_snapshot =
            manifest.snapshot().segments;
        for (SegmentManifestEntry segment : tracked_snapshot) {
            if (segment.state == SegmentPersistentState::kDeleted) continue;
            tracked.insert(segment.relative_path.lexically_normal());
            Result<SegmentRecoveryReport> scanned =
                Scan(partition_root / segment.relative_path, *known);
            if (!scanned.ok()) return scanned.status();
            ++boundary.tracked_segments_scanned;
            MINO_RETURN_IF_ERROR(ValidateTracked(*scanned, partition, segment));
            MINO_RETURN_IF_ERROR(ValidateRecordSchemas(*scanned, *topic));

            const bool active =
                segment.state == SegmentPersistentState::kCreating ||
                segment.state == SegmentPersistentState::kOpen;
            if (scanned->repairable() && active) {
                Result<SegmentRecoveryReport> repaired =
                    Repair(partition_root / segment.relative_path, *known,
                           *scanned);
                if (!repaired.ok()) return repaired.status();
                scanned = Scan(partition_root / segment.relative_path, *known);
                if (!scanned.ok()) return scanned.status();
                MINO_RETURN_IF_ERROR(
                    ValidateTracked(*scanned, partition, segment));
                MINO_RETURN_IF_ERROR(ValidateRecordSchemas(*scanned, *topic));
                ++boundary.repaired_segments;
            }
            if (!scanned->clean()) {
                return Corruption("tracked segment cannot be recovered safely");
            }
            if (!scanned->has_last_complete_sequence && active &&
                scanned->file_size == kEncodedSegmentHeaderSize) {
                MINO_RETURN_IF_ERROR(AppendHeaderOnlyRecoveryGap(
                    partition_root / segment.relative_path, partition, segment,
                    *scanned));
                scanned = Scan(partition_root / segment.relative_path, *known);
                if (!scanned.ok()) return scanned.status();
                MINO_RETURN_IF_ERROR(ValidateRecordSchemas(*scanned, *topic));
            }
            if (!scanned->has_last_complete_sequence) {
                return Corruption("tracked segment contains no complete record");
            }
            MINO_RETURN_IF_ERROR(SyncDurableSegment(
                partition_root / segment.relative_path, scanned->file_size,
                *scanned));
            if (active) {
                segment.last_ingestion_sequence =
                    scanned->last_complete_sequence;
                segment.size_bytes = scanned->file_size;
                if (segment.state == SegmentPersistentState::kCreating) {
                    segment.state = SegmentPersistentState::kOpen;
                    MINO_RETURN_IF_ERROR(manifest.UpdateSegment(segment));
                }
                segment.state = SegmentPersistentState::kSealed;
                segment.sealed_at_ns =
                    std::max(recovery_now_ns_, segment.created_at_ns);
                MINO_RETURN_IF_ERROR(manifest.UpdateSegment(segment));
            } else if (segment.size_bytes != scanned->file_size ||
                       segment.last_ingestion_sequence !=
                           scanned->last_complete_sequence) {
                return Corruption("sealed segment progress differs from manifest");
            }
            MINO_RETURN_IF_ERROR(PublishBoundary(manifest, segment, *scanned));
        }

        Result<std::vector<std::filesystem::path>> candidates =
            SegmentCandidates(partition_root);
        if (!candidates.ok()) return candidates.status();
        for (const std::filesystem::path& absolute : *candidates) {
            const std::filesystem::path relative =
                std::filesystem::relative(absolute, partition_root);
            if (tracked.contains(relative.lexically_normal())) continue;
            ++boundary.orphan_candidates_scanned;

            Result<SegmentRecoveryReport> scanned = Scan(absolute, *known);
            if (!scanned.ok()) return scanned.status();
            const std::optional<uint64_t> segment_id = ParseSegmentId(absolute);
            bool adopt = scanned->has_last_complete_sequence &&
                         segment_id.has_value();
            // Orphans are never modified until their immutable header identity,
            // every complete record's schema tuple, and boundary continuity have
            // passed non-destructive validation.
            if (adopt) {
                adopt = ValidateHeader(*scanned, partition).ok() &&
                        ValidateRecordSchemas(*scanned, *topic).ok();
            }
            const uint64_t expected_first =
                manifest.snapshot().segments.empty()
                    ? 1
                    : manifest.snapshot().segments.back()
                                  .last_ingestion_sequence ==
                              std::numeric_limits<uint64_t>::max()
                          ? 0
                          : manifest.snapshot().segments.back()
                                    .last_ingestion_sequence +
                                1;
            if (adopt) {
                adopt = expected_first != 0 &&
                        scanned->segment_header.first_ingestion_sequence ==
                            expected_first;
            }
            if (adopt && !manifest.snapshot().segments.empty()) {
                adopt = *segment_id >
                        manifest.snapshot().segments.back().segment_id;
            }
            if (adopt && scanned->repairable()) {
                Result<SegmentRecoveryReport> repaired =
                    Repair(absolute, *known, *scanned);
                if (!repaired.ok()) return repaired.status();
                scanned = Scan(absolute, *known);
                if (!scanned.ok()) return scanned.status();
                adopt = scanned->clean() &&
                        ValidateHeader(*scanned, partition).ok() &&
                        ValidateRecordSchemas(*scanned, *topic).ok() &&
                        scanned->segment_header.first_ingestion_sequence ==
                            expected_first;
                ++boundary.repaired_segments;
            } else if (adopt) {
                adopt = scanned->clean();
            }

            if (!adopt) {
                Result<std::filesystem::path> quarantined =
                    manifest.QuarantineOrphan(relative, scanned->file_device,
                                              scanned->file_inode);
                if (!quarantined.ok()) return quarantined.status();
                ++boundary.quarantined_orphans;
                continue;
            }

            MINO_RETURN_IF_ERROR(
                SyncDurableSegment(absolute, scanned->file_size, *scanned));
            SegmentManifestEntry orphan{
                .segment_id = *segment_id,
                .state = SegmentPersistentState::kSealed,
                .first_ingestion_sequence =
                    scanned->segment_header.first_ingestion_sequence,
                .last_ingestion_sequence = scanned->last_complete_sequence,
                .created_at_ns = scanned->segment_header.created_at_ns,
                .sealed_at_ns = std::max(
                    recovery_now_ns_, scanned->segment_header.created_at_ns),
                .size_bytes = scanned->file_size,
                .relative_path = relative,
            };
            MINO_RETURN_IF_ERROR(manifest.AdoptSealedOrphan(
                orphan, scanned->file_device, scanned->file_inode));
            MINO_RETURN_IF_ERROR(PublishBoundary(manifest, orphan, *scanned));
            ++boundary.adopted_sealed_orphans;
        }

        boundary.manifest_generation = manifest.snapshot().generation;
        if (manifest.snapshot().checkpoint.has_value()) {
            const DurableCheckpoint& checkpoint = *manifest.snapshot().checkpoint;
            boundary.durable_segment_id = checkpoint.segment_id;
            boundary.durable_offset = checkpoint.durable_offset;
            boundary.durable_sequence = checkpoint.durable_sequence;
        }
        return boundary;
    }

    std::filesystem::path root_;
    SessionRecoveryOptions options_;
    std::unique_ptr<RecordingManifest> manifest_;
    std::unique_ptr<schema::SchemaRegistry> registry_;
    std::unique_ptr<SchemaStore> schema_store_;
    uint64_t recovery_now_ns_ = 0;
};

SessionRecoveryCoordinator::SessionRecoveryCoordinator(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SessionRecoveryCoordinator::~SessionRecoveryCoordinator() = default;

Result<std::unique_ptr<SessionRecoveryCoordinator>>
SessionRecoveryCoordinator::Open(
    const std::filesystem::path& session_root,
    SessionRecoveryOptions options) noexcept {
    try {
        if (session_root.empty() || options.max_partitions == 0 ||
            options.max_segment_candidates_per_partition == 0) {
            return Invalid("session recovery options are invalid");
        }
        MINO_RETURN_IF_ERROR(
            ValidateNoSymlinkAncestors(session_root, true));
        Result<std::unique_ptr<RecordingManifest>> manifest =
            RecordingManifest::Open(session_root, options.manifest_options);
        if (!manifest.ok()) return manifest.status();
        auto registry = std::make_unique<schema::SchemaRegistry>();
        Result<std::unique_ptr<SchemaStore>> schema_store = SchemaStore::Open(
            session_root, registry.get(), options.schema_store_options);
        if (!schema_store.ok()) return schema_store.status();
        auto impl = std::make_unique<Impl>(
            session_root, std::move(options), std::move(*manifest),
            std::move(registry), std::move(*schema_store));
        return std::unique_ptr<SessionRecoveryCoordinator>(
            new SessionRecoveryCoordinator(std::move(impl)));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "cannot allocate session recovery coordinator");
    } catch (const std::filesystem::filesystem_error& error) {
        return Unavailable(error.what());
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal, error.what());
    }
}

Result<SessionRecoveryReport> SessionRecoveryCoordinator::Recover() noexcept {
    try {
        return impl_->Recover();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "cannot allocate session recovery report");
    } catch (const std::filesystem::filesystem_error& error) {
        return Unavailable(error.what());
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal, error.what());
    }
}

}  // namespace mino::storage
