// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recorder.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <sstream>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <utility>

#include "mino/schema/registry.h"
#include "mino/storage/segment_recovery.h"

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Unavailable(std::string_view message) {
    return Status::Error(StatusCode::kUnavailable, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

Status Degraded(std::string_view message) {
    return Status::Error(StatusCode::kDegraded, message);
}

Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status RecoveryIoError(std::string_view operation,
                       const std::filesystem::path& path) {
    const int error = errno;
    StatusCode code = StatusCode::kUnavailable;
    if (error == EACCES || error == EPERM || error == EROFS) {
        code = StatusCode::kPermissionDenied;
    } else if (error == ENOSPC || error == EDQUOT) {
        code = StatusCode::kResourceExhausted;
    } else if (error == ENOENT) {
        code = StatusCode::kNotFound;
    } else if (error == ELOOP) {
        code = StatusCode::kCorruption;
    }
    return Status::Error(code, std::string(operation) + " '" + path.string() +
                                   "': " + std::strerror(error));
}

class ScopedRecoveryFd final {
public:
    explicit ScopedRecoveryFd(int fd) noexcept : fd_(fd) {}
    ~ScopedRecoveryFd() {
        if (fd_ >= 0) static_cast<void>(::close(fd_));
    }

    ScopedRecoveryFd(const ScopedRecoveryFd&) = delete;
    ScopedRecoveryFd& operator=(const ScopedRecoveryFd&) = delete;

    int get() const noexcept { return fd_; }

private:
    int fd_ = -1;
};

int RecoveryOpenFlags(bool create) noexcept {
    int flags = O_WRONLY | O_APPEND;
    if (create) flags |= O_CREAT | O_EXCL;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    return flags;
}

Status WriteAllRecovery(int fd, const std::filesystem::path& path,
                        std::span<const std::byte> bytes) {
    size_t offset = 0;
    while (offset < bytes.size()) {
        const size_t remaining = bytes.size() - offset;
        const size_t request = std::min(
            remaining,
            static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
        const ssize_t written = ::write(fd, bytes.data() + offset, request);
        if (written < 0) {
            if (errno == EINTR) continue;
            return RecoveryIoError("cannot append recovery record", path);
        }
        if (written == 0) {
            return Unavailable("zero-byte recovery segment write");
        }
        offset += static_cast<size_t>(written);
    }
    return Status::Ok();
}

Status SyncRecoveryFile(int fd, const std::filesystem::path& path) {
    while (true) {
#if defined(__APPLE__)
        const int result = ::fsync(fd);
#else
        const int result = ::fdatasync(fd);
#endif
        if (result == 0) return Status::Ok();
        if (errno == EINTR) continue;
        return RecoveryIoError("cannot sync recovered segment", path);
    }
}

Status SyncRecoveryDirectory(const std::filesystem::path& path) {
    int flags = O_RDONLY;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
    const int fd = ::open(path.c_str(), flags);
    if (fd < 0) return RecoveryIoError("cannot open segment directory", path);
    const ScopedRecoveryFd scoped(fd);
    while (::fsync(fd) != 0) {
        if (errno == EINTR) continue;
        return RecoveryIoError("cannot fsync segment directory", path);
    }
    return Status::Ok();
}

Status VerifyRecoveryFile(
    int fd, const std::filesystem::path& path,
    std::optional<uint64_t> expected_size,
    std::optional<uint64_t> expected_device = std::nullopt,
    std::optional<uint64_t> expected_inode = std::nullopt) {
    struct stat info {};
    if (::fstat(fd, &info) != 0) {
        return RecoveryIoError("cannot stat recovery segment", path);
    }
    if (!S_ISREG(info.st_mode) || info.st_size < 0) {
        return Corruption("recovery segment is not a regular file");
    }
    if (expected_size.has_value() &&
        static_cast<uint64_t>(info.st_size) != *expected_size) {
        return Unavailable("recovery segment size changed before append");
    }
    if (expected_device.has_value() != expected_inode.has_value()) {
        return Invalid("recovery identity requires device and inode");
    }
    if (expected_device.has_value() &&
        (static_cast<uint64_t>(info.st_dev) != *expected_device ||
         static_cast<uint64_t>(info.st_ino) != *expected_inode)) {
        return Unavailable(
            "recovery segment inode/device changed after validation");
    }
    return Status::Ok();
}

Status CreateRecoverySegment(const std::filesystem::path& path,
                             std::span<const std::byte> header,
                             std::span<const std::byte> record) {
    const int fd = ::open(path.c_str(), RecoveryOpenFlags(true), 0644);
    if (fd < 0) return RecoveryIoError("cannot create recovery segment", path);
    const ScopedRecoveryFd scoped(fd);
    Status status = VerifyRecoveryFile(fd, path, uint64_t{0});
    if (!status.ok()) return status;
    status = WriteAllRecovery(fd, path, header);
    if (!status.ok()) return status;
    status = WriteAllRecovery(fd, path, record);
    if (!status.ok()) return status;
    status = SyncRecoveryFile(fd, path);
    if (!status.ok()) return status;
    return SyncRecoveryDirectory(path.parent_path());
}

Status AppendRecoveryRecord(const std::filesystem::path& path,
                            uint64_t expected_size,
                            uint64_t expected_device,
                            uint64_t expected_inode,
                            std::span<const std::byte> record) {
    const int fd = ::open(path.c_str(), RecoveryOpenFlags(false));
    if (fd < 0) return RecoveryIoError("cannot open recovery segment", path);
    const ScopedRecoveryFd scoped(fd);
    Status status = VerifyRecoveryFile(fd, path, expected_size,
                                       expected_device, expected_inode);
    if (!status.ok()) return status;
    status = WriteAllRecovery(fd, path, record);
    if (!status.ok()) return status;
    return SyncRecoveryFile(fd, path);
}

uint64_t RecoveryNowNs(uint64_t minimum) noexcept {
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    if (elapsed.count() <= 0) return minimum;
    return std::max(minimum, static_cast<uint64_t>(elapsed.count()));
}

void SaturatingAdd(uint64_t value, uint64_t* target) noexcept {
    const uint64_t maximum = std::numeric_limits<uint64_t>::max();
    *target = value > maximum - *target ? maximum : *target + value;
}

uint32_t PayloadCrc32c(std::span<const std::byte> payload) noexcept {
    uint32_t state = 0xffffffffu;
    for (std::byte byte : payload) {
        state ^= static_cast<uint8_t>(byte);
        for (int bit = 0; bit < 8; ++bit) {
            state = (state >> 1) ^
                    ((state & 1u) != 0 ? 0x82f63b78u : 0u);
        }
    }
    return state ^ 0xffffffffu;
}

RecorderSchemaMetadata RecorderSchemaOf(
    const schema::SchemaIdentity& identity) noexcept {
    return RecorderSchemaMetadata{
        .short_id = identity.short_id(),
        .canonical_digest = identity.canonical_digest(),
        .schema_version = identity.schema_version(),
        .layout_version = identity.layout_version(),
    };
}

bool SameSchema(const RecorderSchemaMetadata& metadata,
                const schema::SchemaIdentity& identity) noexcept {
    return metadata == RecorderSchemaOf(identity);
}

std::filesystem::path PartitionRoot(const std::filesystem::path& session_root,
                                    TopicId topic_id,
                                    uint32_t partition_id) {
    std::ostringstream name;
    name << std::setfill('0') << std::setw(4) << partition_id;
    return session_root / "topics" / std::to_string(topic_id.value) /
           "partitions" / name.str();
}

uint64_t DerivedWriterId(uint64_t recording_id, TopicId topic_id,
                         uint32_t partition_id) noexcept {
    uint64_t value = recording_id;
    value ^= static_cast<uint64_t>(topic_id.value) << 32;
    value ^= static_cast<uint64_t>(partition_id) + 0x9e3779b97f4a7c15ull;
    value ^= value >> 30;
    value *= 0xbf58476d1ce4e5b9ull;
    value ^= value >> 27;
    value *= 0x94d049bb133111ebull;
    value ^= value >> 31;
    return value == 0 ? 1 : value;
}

Result<size_t> ChargedBytes(size_t payload_size,
                            size_t max_large_object_bytes) {
    if (payload_size == 0) return Invalid("recorder payload must not be empty");
    if (payload_size <= kRecorderSmallBufferClassBytes) {
        return kRecorderSmallBufferClassBytes;
    }
    if (payload_size <= kRecorderMediumBufferClassBytes) {
        return kRecorderMediumBufferClassBytes;
    }
    if (payload_size <= kRecorderLargeBufferClassBytes) {
        return kRecorderLargeBufferClassBytes;
    }
    if (payload_size > max_large_object_bytes) {
        return Exhausted("recorder payload exceeds the large-object bound");
    }
    constexpr size_t alignment = 4096;
    if (payload_size > std::numeric_limits<size_t>::max() - (alignment - 1)) {
        return Exhausted("recorder payload charge overflows");
    }
    return (payload_size + alignment - 1) & ~(alignment - 1);
}

Status CreateDirectory(const std::filesystem::path& path) {
    std::error_code error;
    std::filesystem::create_directories(path, error);
    if (error || !std::filesystem::is_directory(path, error) || error) {
        return Unavailable("cannot create recorder directory");
    }
    return Status::Ok();
}

bool ValidFlushLevel(RecordAckLevel level) noexcept {
    switch (level) {
        case RecordAckLevel::kAccepted:
        case RecordAckLevel::kBuffered:
        case RecordAckLevel::kWritten:
        case RecordAckLevel::kDurable:
            return true;
    }
    return false;
}

}  // namespace

Result<capacity::ResourceVector> EstimateRecorderTopicResources(
    const RecorderTopicConfig& config) noexcept {
    if (config.partition_count == 0) {
        return Invalid("recorder capacity estimate requires partitions");
    }
    MINO_ASSIGN_OR_RETURN(auto effective,
                          ValidateRecordingPolicy(config.policy));

    capacity::ResourceVector per_partition;
    if (effective.mode != RecordingMode::kSnapshot) {
        per_partition.recorder_buffer_bytes =
            config.buffer_pool_options.global_byte_limit;
    }
    // Partition manifest plus active segment/snapshot file. This is a
    // conservative descriptor ceiling, not an assertion that all are always open.
    per_partition.file_descriptors = 2;
    MINO_ASSIGN_OR_RETURN(
        auto resources,
        capacity::CheckedScale(per_partition, config.partition_count));

    capacity::ResourceVector topic_resources;
    topic_resources.file_descriptors = 1;  // Topic/session metadata operation.
    for (const RecorderTopicSchema& schema : config.schemas) {
        capacity::ResourceVector artifact;
        artifact.schema_buffer_bytes = schema.descriptor_artifact.size();
        MINO_ASSIGN_OR_RETURN(topic_resources,
                              capacity::CheckedAdd(topic_resources, artifact));
    }
    return capacity::CheckedAdd(resources, topic_resources);
}

class Recorder::Impl final {
public:
    struct TopicRuntime;

    struct PartitionRuntime {
        TopicRuntime* topic = nullptr;
        uint32_t partition_id = 0;
        uint64_t writer_id = 0;
        RecorderBufferPoolOptions pool_options;
        std::unique_ptr<RecorderBufferPool> pool;
        std::unique_ptr<PartitionManifest> manifest;
        std::unique_ptr<RecordingTopologyCoordinator> topology;
        std::unique_ptr<TopicWriter> writer;
        std::unique_ptr<SnapshotStore> snapshot_store;
        TopicWriterState snapshot_writer_state = TopicWriterState::kCreated;
        Status error = Status::Ok();
        uint64_t written_acks = 0;
        uint64_t durable_acks = 0;
    };

    struct TopicRuntime {
        // Declared first so the charge is released after every partition.
        capacity::CapacityLease capacity_lease;
        TopicId topic_id{};
        std::string name;
        uint64_t config_version = 0;
        EffectiveRecordingPolicy policy;
        std::vector<RecorderSchemaMetadata> schemas;
        std::map<uint32_t, std::unique_ptr<PartitionRuntime>> partitions;
    };

    Impl(std::filesystem::path session_root, RecorderSessionOptions options,
         std::unique_ptr<RecordingManifest> manifest,
         std::unique_ptr<schema::SchemaRegistry> registry,
         std::unique_ptr<SchemaStore> schema_store,
         std::shared_ptr<capacity::CapacityController> capacity_controller) noexcept
        : session_root_(std::move(session_root)),
          options_(std::move(options)),
          manifest_(std::move(manifest)),
          registry_(std::move(registry)),
          schema_store_(std::move(schema_store)),
          capacity_controller_(std::move(capacity_controller)) {}

    ~Impl() {
        if (state_ == RecorderState::kCreated ||
            state_ == RecorderState::kRunning ||
            state_ == RecorderState::kDegraded) {
            static_cast<void>(StopLocked(last_now_ns_));
        }
    }

    static void CaptureAck(const TopicWriterAck& ack, void* context) noexcept {
        auto* partition = static_cast<PartitionRuntime*>(context);
        if (ack.level == RecordAckLevel::kWritten) {
            SaturatingAdd(1, &partition->written_acks);
        } else if (ack.level == RecordAckLevel::kDurable) {
            SaturatingAdd(1, &partition->durable_acks);
        }
    }

    Status AddTopic(
        const RecorderTopicConfig& config,
        std::optional<capacity::ResourceVector> capacity_charge) {
        std::lock_guard lock(mutex_);
        try {
            if (state_ != RecorderState::kCreated) {
                return Invalid("topics may only be configured before Start");
            }
            if (config.topic_id.value == 0 || config.topic_name.empty() ||
                config.config_version == 0 || config.partition_count == 0 ||
                config.schemas.empty()) {
                return Invalid("recorder topic configuration is incomplete");
            }
            if (topics_.find(config.topic_id.value) != topics_.end()) {
                return Status::Error(StatusCode::kAlreadyExists,
                                     "recorder topic is already attached");
            }
            if (config.writer_id_base != 0 &&
                config.partition_count - 1 >
                    std::numeric_limits<uint64_t>::max() -
                        config.writer_id_base) {
                return Invalid("recorder writer ID range overflows");
            }

            Result<EffectiveRecordingPolicy> effective =
                ValidateRecordingPolicy(config.policy);
            if (!effective.ok()) return effective.status();

            capacity::CapacityReservation capacity_reservation;
            if (capacity_controller_) {
                capacity::ResourceVector charge;
                if (capacity_charge.has_value()) {
                    charge = *capacity_charge;
                } else {
                    MINO_ASSIGN_OR_RETURN(
                        charge, EstimateRecorderTopicResources(config));
                }
                MINO_ASSIGN_OR_RETURN(
                    capacity_reservation,
                    capacity_controller_->Reserve(
                        capacity::ResourceRequest{
                            .resources = charge,
                            .scope = capacity::ResourceScope::kRecorder,
                            .admission_class =
                                capacity::AdmissionClass::kDataPlane,
                            .name = config.topic_name,
                        }));
            }

            std::vector<SchemaRefSnapshot> schema_snapshot;
            std::vector<RecorderSchemaMetadata> recorder_schemas;
            schema_snapshot.reserve(config.schemas.size());
            recorder_schemas.reserve(config.schemas.size());
            for (const RecorderTopicSchema& configured_schema : config.schemas) {
                Result<SchemaRef> schema_ref = schema_store_->FindRef(
                    configured_schema.identity);
                if (!schema_ref.ok()) {
                    if (schema_ref.status().code() != StatusCode::kNotFound ||
                        configured_schema.descriptor_artifact.empty()) {
                        return schema_ref.status();
                    }
                    schema_ref = schema_store_->Persist(
                        configured_schema.identity,
                        configured_schema.descriptor_artifact);
                    if (!schema_ref.ok()) return schema_ref.status();
                }
                Result<SchemaStoreEntry> entry =
                    schema_store_->Resolve(*schema_ref);
                if (!entry.ok()) return entry.status();
                if (!SameSchema(RecorderSchemaOf(configured_schema.identity),
                                entry->identity)) {
                    return Status::Error(
                        StatusCode::kSchemaMismatch,
                        "stored recorder schema identity does not match topic");
                }
                schema_snapshot.push_back(SchemaRefSnapshot{
                    .schema_ref = *schema_ref,
                    .schema_version = configured_schema.identity.schema_version(),
                    .layout_version = configured_schema.identity.layout_version(),
                    .canonical_digest =
                        configured_schema.identity.canonical_digest(),
                    .descriptor_path = std::filesystem::path("schemas") /
                                       entry->descriptor_path.filename(),
                });
                recorder_schemas.push_back(
                    RecorderSchemaOf(configured_schema.identity));
            }
            std::sort(schema_snapshot.begin(), schema_snapshot.end(),
                      [](const SchemaRefSnapshot& lhs,
                         const SchemaRefSnapshot& rhs) {
                          return lhs.schema_ref < rhs.schema_ref;
                      });
            if (std::adjacent_find(
                    schema_snapshot.begin(), schema_snapshot.end(),
                    [](const SchemaRefSnapshot& lhs,
                       const SchemaRefSnapshot& rhs) {
                        return lhs.schema_ref == rhs.schema_ref;
                    }) != schema_snapshot.end()) {
                return Invalid("recorder topic contains a duplicate schema");
            }

            TopicTableEntry table{
                .topic_id = config.topic_id.value,
                .topic_name = config.topic_name,
                .config_version = config.config_version,
                .schema_snapshot = schema_snapshot,
            };
            Result<TopicTableEntry> existing =
                manifest_->FindTopic(config.topic_id.value);
            Status manifest_status = Status::Ok();
            if (existing.ok()) {
                if (existing->topic_name != table.topic_name) {
                    return Invalid("reopened topic ID/name mapping changed");
                }
                if (table.config_version < existing->config_version) {
                    return Invalid("reopened topic config version moved backward");
                }
                manifest_status = manifest_->UpdateTopic(table);
            } else if (existing.status().code() == StatusCode::kNotFound) {
                manifest_status = manifest_->AddTopic(table);
            } else {
                return existing.status();
            }
            if (!manifest_status.ok()) return PoisonSession(manifest_status);
            if (config.config_version >
                manifest_->snapshot().session.config_version) {
                manifest_status = manifest_->UpdateSessionConfigVersion(
                    config.config_version);
                if (!manifest_status.ok()) return PoisonSession(manifest_status);
            }

            auto topic = std::make_unique<TopicRuntime>();
            topic->topic_id = config.topic_id;
            topic->name = config.topic_name;
            topic->config_version = config.config_version;
            topic->policy = *effective;
            topic->schemas = std::move(recorder_schemas);

            for (uint32_t partition_id = 0;
                 partition_id < config.partition_count; ++partition_id) {
                Result<std::unique_ptr<PartitionRuntime>> partition =
                    BuildPartition(config, topic.get(), partition_id);
                if (!partition.ok()) return partition.status();
                topic->partitions.emplace(partition_id,
                                          std::move(*partition));
            }
            MINO_ASSIGN_OR_RETURN(auto capacity_lease,
                                  capacity_reservation.Commit());
            topic->capacity_lease = std::move(capacity_lease);
            topics_.emplace(config.topic_id.value, std::move(topic));
            return Status::Ok();
        } catch (const std::bad_alloc&) {
            return Exhausted("cannot allocate recorder topic state");
        } catch (const std::length_error&) {
            return Exhausted("recorder topic configuration exceeds a bound");
        } catch (const std::exception& error) {
            return Invalid(error.what());
        }
    }

    Status Start(uint64_t now_ns) {
        std::lock_guard lock(mutex_);
        if (state_ != RecorderState::kCreated) {
            return Invalid("Recorder can only start once");
        }
        if (topics_.empty()) return Invalid("Recorder has no configured topics");
        last_now_ns_ = now_ns;
        state_ = RecorderState::kRunning;
        bool failed = false;
        for (auto& [topic_id, topic] : topics_) {
            static_cast<void>(topic_id);
            for (auto& [partition_id, partition] : topic->partitions) {
                static_cast<void>(partition_id);
                if (partition->snapshot_store != nullptr) {
                    partition->snapshot_writer_state = TopicWriterState::kRunning;
                    continue;
                }
                const Status status = partition->writer->Start(now_ns);
                if (!status.ok()) {
                    MarkPartitionFailed(*partition, status, now_ns);
                    failed = true;
                }
            }
        }
        if (failed) {
            state_ = RecorderState::kDegraded;
            return Degraded("one or more recorder partitions failed to start");
        }
        return Status::Ok();
    }

    Result<RecorderEnqueueResult> Enqueue(
        const RecorderEnqueueRequest& request) {
        std::lock_guard lock(mutex_);
        try {
            SaturatingAdd(1, &metrics_.enqueue_calls);
            if (state_ != RecorderState::kRunning &&
                state_ != RecorderState::kDegraded) {
                return Invalid("Recorder is not running");
            }
            if (request.metadata.topic_id.value == 0 ||
                request.metadata.payload_size != request.payload.size() ||
                request.metadata.payload_crc != PayloadCrc32c(request.payload) ||
                request.timeout < std::chrono::nanoseconds::zero()) {
                return Invalid("recorder enqueue metadata is inconsistent");
            }
            TopicRuntime* topic = FindTopic(request.metadata.topic_id);
            if (topic == nullptr) {
                return Status::Error(StatusCode::kNotFound,
                                     "recorder topic is not configured");
            }
            auto found_partition = topic->partitions.find(request.partition_id);
            if (found_partition == topic->partitions.end()) {
                return Status::Error(StatusCode::kNotFound,
                                     "recorder partition is not configured");
            }
            PartitionRuntime& partition = *found_partition->second;
            if (!partition.error.ok()) {
                return RecorderEnqueueResult{
                    .disposition = RecorderEnqueueDisposition::kFailed,
                    .status = partition.error,
                    .acknowledged = std::nullopt,
                    .discarded = {},
                    .gap_debts = {},
                };
            }
            if (!ContainsSchema(*topic, request.metadata.schema)) {
                return Status::Error(
                    StatusCode::kSchemaMismatch,
                    "record schema is absent from the topic snapshot");
            }

            const uint64_t now_ns =
                std::max(last_now_ns_, request.metadata.ingestion_timestamp_ns);
            last_now_ns_ = now_ns;
            if (partition.snapshot_store != nullptr) {
                Result<SchemaRef> schema_ref = schema_store_->FindRef(
                    request.metadata.schema.canonical_digest);
                if (!schema_ref.ok()) return schema_ref.status();
                Record record;
                record.header.flags =
                    (request.user_tag & kTopicWriterTombstoneUserTagMask) != 0
                        ? kRecordFlagTombstone
                        : 0;
                record.header.schema_ref = *schema_ref;
                record.header.schema_version =
                    request.metadata.schema.schema_version;
                record.header.layout_version =
                    request.metadata.schema.layout_version;
                record.header.topic_id = request.metadata.topic_id.value;
                record.header.partition_id = request.partition_id;
                record.header.ingestion_timestamp_ns =
                    request.metadata.ingestion_timestamp_ns;
                record.header.node_id = request.metadata.source.node_id;
                record.header.publisher_id =
                    request.metadata.source.publisher_id;
                record.header.publisher_epoch =
                    request.metadata.source.publisher_epoch;
                record.header.source_sequence =
                    request.metadata.source.source_sequence;
                record.header.observed_timestamp_ns =
                    request.metadata.source.observed_timestamp_ns;
                record.payload.assign(request.payload.begin(),
                                      request.payload.end());
                Result<uint64_t> stored =
                    partition.snapshot_store->Put(std::move(record));
                if (!stored.ok()) {
                    MarkPartitionFailed(partition, stored.status(), now_ns);
                    return RecorderEnqueueResult{
                        .disposition = RecorderEnqueueDisposition::kFailed,
                        .status = stored.status(),
                        .acknowledged = std::nullopt,
                        .discarded = {},
                        .gap_debts = {},
                    };
                }
                SaturatingAdd(1, &metrics_.accepted_records);
                SaturatingAdd(1, &partition.written_acks);
                SaturatingAdd(1, &partition.durable_acks);
                return RecorderEnqueueResult{
                    .disposition = RecorderEnqueueDisposition::kBuffered,
                    .status = Status::Ok(),
                    .acknowledged = std::nullopt,
                    .discarded = {},
                    .gap_debts = {},
                };
            }

            const uint64_t cursor = request.available_cursor.value_or(
                partition.topology->next_cursor());
            Result<size_t> charged = ChargedBytes(
                request.payload.size(),
                partition.pool_options.max_large_object_bytes);
            if (!charged.ok()) return charged.status();
            const RecorderBufferPoolStats pool_stats = partition.pool->stats();
            const size_t topic_limit =
                partition.pool->TopicByteLimit(request.metadata.topic_id);
            const size_t topic_bytes =
                partition.pool->TopicBytesInUse(request.metadata.topic_id);
            const bool no_slot =
                pool_stats.queued_records + pool_stats.reserved_records >=
                partition.pool_options.queue_capacity;
            const bool no_global_bytes =
                *charged > partition.pool_options.global_byte_limit -
                               std::min(pool_stats.bytes_in_use,
                                        partition.pool_options.global_byte_limit);
            const bool no_topic_bytes =
                *charged > topic_limit - std::min(topic_bytes, topic_limit);
            const RecordingSinkCapacity capacity =
                (no_slot || no_global_bytes || no_topic_bytes ||
                 pool_stats.closed || pool_stats.recording_failed)
                    ? RecordingSinkCapacity::kFull
                    : RecordingSinkCapacity::kAvailable;

            Result<RecordingAdmissionDecision> decision =
                partition.topology->DecideAdmission(
                    RecordingAdmissionRequest{
                        .available_cursor = cursor,
                        .sink_capacity = capacity,
                    },
                    now_ns);
            if (!decision.ok()) return decision.status();

            RecorderEnqueueResult result;
            result.status = decision->status;
            result.gap_debts = decision->gap_debts;
            SaturatingAdd(decision->gap_debts.size(), &metrics_.gap_debts);
            Status debt_status = ReportTopologyDebts(
                partition, request.metadata, cursor, decision->gap_debts);
            if (!debt_status.ok()) {
                MarkPartitionFailed(partition, debt_status, now_ns);
                return RecorderEnqueueResult{
                    .disposition = RecorderEnqueueDisposition::kFailed,
                    .status = debt_status,
                    .acknowledged = std::nullopt,
                    .discarded = {},
                    .gap_debts = std::move(result.gap_debts),
                };
            }
            if (!decision->recorder_admitted) {
                if (decision->reason ==
                        RecordingAdmissionReason::kBufferFull &&
                    topic->policy.full_policy ==
                        BufferFullPolicy::kFailRecording) {
                    const Status failure = Status::Error(
                        StatusCode::kResourceExhausted,
                        "recorder buffer policy failed recording");
                    FailTopic(*topic, failure, now_ns);
                    result.disposition = RecorderEnqueueDisposition::kFailed;
                    result.status = failure;
                } else if (decision->outcome ==
                           RecordingAdmissionOutcome::kBlockPrimary) {
                    result.disposition = RecorderEnqueueDisposition::kBlocked;
                    SaturatingAdd(1, &metrics_.blocked_records);
                } else if (decision->outcome ==
                           RecordingAdmissionOutcome::kFailPrimary) {
                    result.disposition = RecorderEnqueueDisposition::kFailed;
                    MarkPartitionFailed(partition, decision->status, now_ns);
                } else {
                    result.disposition = RecorderEnqueueDisposition::kDropped;
                    SaturatingAdd(1, &metrics_.dropped_records);
                }
                return result;
            }

            Result<BufferReserveResult> reserved = partition.pool->Reserve(
                BufferReservationRequest{
                    .topic_id = request.metadata.topic_id,
                    .payload_size = request.payload.size(),
                    .user_tag = request.user_tag,
                    .full_policy = topic->policy.full_policy,
                    .timeout = request.timeout,
                    .metadata = request.metadata,
                });
            if (!reserved.ok()) {
                RecorderEnqueueResult failed;
                failed.status = reserved.status();
                failed.disposition =
                    reserved.status().code() == StatusCode::kWouldBlock ||
                            reserved.status().code() == StatusCode::kTimeout
                        ? RecorderEnqueueDisposition::kBlocked
                        : RecorderEnqueueDisposition::kFailed;
                if (failed.disposition ==
                    RecorderEnqueueDisposition::kBlocked) {
                    SaturatingAdd(1, &metrics_.blocked_records);
                } else {
                    MarkPartitionFailed(partition, reserved.status(), now_ns);
                }
                return failed;
            }
            result.discarded = std::move(reserved->discarded);
            if (!result.discarded.empty()) {
                const Status reported =
                    partition.writer->ReportDiscards(result.discarded);
                if (!reported.ok()) {
                    MarkPartitionFailed(partition, reported, now_ns);
                    return RecorderEnqueueResult{
                        .disposition = RecorderEnqueueDisposition::kFailed,
                        .status = reported,
                        .acknowledged = std::nullopt,
                        .discarded = std::move(result.discarded),
                        .gap_debts = std::move(result.gap_debts),
                    };
                }
                SaturatingAdd(result.discarded.size(),
                              &metrics_.dropped_records);
            }
            if (!reserved->accepted()) {
                result.disposition =
                    reserved->admission == BufferAdmission::kRecordingFailed
                        ? RecorderEnqueueDisposition::kFailed
                        : RecorderEnqueueDisposition::kDropped;
                if (result.disposition ==
                    RecorderEnqueueDisposition::kFailed) {
                    const Status failure = Unavailable(
                        "recorder buffer policy failed recording");
                    FailTopic(*topic, failure, now_ns);
                    result.status = failure;
                } else if (result.status.ok()) {
                    result.status = Degraded("recorder buffer dropped a record");
                }
                return result;
            }
            std::copy(request.payload.begin(), request.payload.end(),
                      reserved->reservation.bytes().begin());
            const Status committed =
                std::move(reserved->reservation).Commit();
            if (!committed.ok()) {
                MarkPartitionFailed(partition, committed, now_ns);
                return RecorderEnqueueResult{
                    .disposition = RecorderEnqueueDisposition::kFailed,
                    .status = committed,
                    .acknowledged = std::nullopt,
                    .discarded = std::move(result.discarded),
                    .gap_debts = std::move(result.gap_debts),
                };
            }

            SaturatingAdd(1, &metrics_.accepted_records);
            SaturatingAdd(1, &metrics_.buffered_records);
            result.disposition = RecorderEnqueueDisposition::kBuffered;
            result.acknowledged = topic->policy.required_ack;
            if (!topic->policy.required_ack.has_value() ||
                *topic->policy.required_ack == RecordAckLevel::kAccepted ||
                *topic->policy.required_ack == RecordAckLevel::kBuffered) {
                return result;
            }

            Result<TopicWriterPumpResult> pumped =
                partition.writer->Pump(now_ns);
            if (!pumped.ok()) {
                FailForWriterError(*topic, partition, pumped.status(), now_ns);
                result.disposition = RecorderEnqueueDisposition::kFailed;
                result.status = pumped.status();
                result.acknowledged.reset();
                return result;
            }
            AccumulatePump(*pumped, nullptr);
            if (*topic->policy.required_ack == RecordAckLevel::kDurable) {
                const Status durable = partition.writer->Flush(
                    RecordAckLevel::kDurable, now_ns);
                if (!durable.ok()) {
                    FailForWriterError(*topic, partition, durable, now_ns);
                    result.disposition = RecorderEnqueueDisposition::kFailed;
                    result.status = durable;
                    result.acknowledged.reset();
                    return result;
                }
            }
            return result;
        } catch (const std::bad_alloc&) {
            return Exhausted("cannot allocate recorder enqueue state");
        } catch (const std::length_error&) {
            return Exhausted("recorder enqueue exceeds a container bound");
        } catch (const std::exception& error) {
            return Status::Error(StatusCode::kInternal, error.what());
        }
    }

    Result<RecorderPumpResult> Pump(uint64_t now_ns,
                                    size_t max_records_per_partition) {
        std::lock_guard lock(mutex_);
        try {
            if (state_ != RecorderState::kRunning &&
                state_ != RecorderState::kDegraded) {
                return Invalid("Recorder is not running");
            }
            if (now_ns < last_now_ns_) {
                return Invalid("Recorder time must be nondecreasing");
            }
            last_now_ns_ = now_ns;
            SaturatingAdd(1, &metrics_.pump_calls);
            RecorderPumpResult aggregate;
            for (auto& [topic_id, topic] : topics_) {
                static_cast<void>(topic_id);
                for (auto& [partition_id, partition] : topic->partitions) {
                    static_cast<void>(partition_id);
                    ++aggregate.partitions_visited;
                    if (!partition->error.ok() ||
                        partition->snapshot_store != nullptr) {
                        continue;
                    }
                    Result<TopicWriterPumpResult> pumped =
                        partition->writer->Pump(now_ns,
                                                max_records_per_partition);
                    if (!pumped.ok()) {
                        FailForWriterError(*topic, *partition, pumped.status(),
                                           now_ns);
                        aggregate.failures.push_back(RecorderOperationFailure{
                            .topic_id = topic->topic_id,
                            .partition_id = partition->partition_id,
                            .status = pumped.status(),
                        });
                        continue;
                    }
                    AccumulatePump(*pumped, &aggregate);
                }
            }
            return aggregate;
        } catch (const std::bad_alloc&) {
            return Exhausted("cannot allocate recorder pump result");
        } catch (const std::exception& error) {
            return Status::Error(StatusCode::kInternal, error.what());
        }
    }

    Status Flush(RecordAckLevel level, uint64_t now_ns) {
        std::lock_guard lock(mutex_);
        if (!ValidFlushLevel(level)) {
            return Invalid("Recorder flush ACK level is unknown");
        }
        if (state_ == RecorderState::kStopped) return Status::Ok();
        if (state_ != RecorderState::kRunning &&
            state_ != RecorderState::kDegraded) {
            return Invalid("Recorder is not running");
        }
        if (now_ns < last_now_ns_) {
            return Invalid("Recorder time must be nondecreasing");
        }
        last_now_ns_ = now_ns;
        SaturatingAdd(1, &metrics_.flush_calls);
        if (level == RecordAckLevel::kAccepted ||
            level == RecordAckLevel::kBuffered) {
            return Status::Ok();
        }

        bool failed = false;
        for (auto& [topic_id, topic] : topics_) {
            static_cast<void>(topic_id);
            for (auto& [partition_id, partition] : topic->partitions) {
                static_cast<void>(partition_id);
                if (!partition->error.ok()) {
                    failed = true;
                    continue;
                }
                if (partition->snapshot_store != nullptr) continue;
                Result<TopicWriterPumpResult> pumped =
                    partition->writer->Pump(now_ns);
                if (!pumped.ok()) {
                    FailForWriterError(*topic, *partition, pumped.status(),
                                       now_ns);
                    failed = true;
                    continue;
                }
                AccumulatePump(*pumped, nullptr);
                const Status status = partition->writer->Flush(level, now_ns);
                if (!status.ok()) {
                    FailForWriterError(*topic, *partition, status, now_ns);
                    failed = true;
                }
            }
        }
        return failed ? Degraded("one or more recorder partitions failed Flush")
                      : Status::Ok();
    }

    Status Stop(uint64_t now_ns) {
        std::lock_guard lock(mutex_);
        return StopLocked(now_ns);
    }

    RecorderState state() const noexcept {
        std::lock_guard lock(mutex_);
        return state_;
    }

    Status error_status() const noexcept {
        std::lock_guard lock(mutex_);
        if (!session_error_.ok()) return session_error_;
        if (state_ == RecorderState::kDegraded) {
            return Degraded("one or more recorder partitions have failed");
        }
        return Status::Ok();
    }

    RecorderMetrics metrics() const noexcept {
        std::lock_guard lock(mutex_);
        RecorderMetrics snapshot = metrics_;
        uint64_t written = 0;
        uint64_t durable = 0;
        for (const auto& [topic_id, topic] : topics_) {
            static_cast<void>(topic_id);
            for (const auto& [partition_id, partition] : topic->partitions) {
                static_cast<void>(partition_id);
                SaturatingAdd(partition->written_acks, &written);
                SaturatingAdd(partition->durable_acks, &durable);
            }
        }
        snapshot.written_records = written;
        snapshot.durable_records = durable;
        return snapshot;
    }

    Result<RecorderPartitionStatus> GetPartitionStatus(
        TopicId topic_id, uint32_t partition_id, uint64_t now_ns) const {
        std::lock_guard lock(mutex_);
        const TopicRuntime* topic = FindTopic(topic_id);
        if (topic == nullptr) {
            return Status::Error(StatusCode::kNotFound,
                                 "recorder topic is not configured");
        }
        const auto found = topic->partitions.find(partition_id);
        if (found == topic->partitions.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "recorder partition is not configured");
        }
        const PartitionRuntime& partition = *found->second;
        Result<RecordingTopologyMetrics> topology_metrics =
            partition.topology->MetricsSnapshot(now_ns);
        if (!topology_metrics.ok()) return topology_metrics.status();
        const bool snapshot = partition.snapshot_store != nullptr;
        return RecorderPartitionStatus{
            .topic_id = topic_id,
            .partition_id = partition_id,
            .recording_mode = topic->policy.mode,
            .has_snapshot = snapshot && partition.snapshot_store->has_snapshot(),
            .writer_state = snapshot ? partition.snapshot_writer_state
                                     : partition.writer->state(),
            .topology_state = partition.topology->state(),
            .error_status = partition.error,
            .buffer_pool = snapshot ? RecorderBufferPoolStats{}
                                    : partition.pool->stats(),
            .topology_metrics = *topology_metrics,
            .next_ingestion_sequence =
                snapshot
                    ? partition.snapshot_store->next_ingestion_sequence()
                    : partition.writer->next_ingestion_sequence(),
        };
    }

    const RecordingManifestSnapshot& manifest_snapshot() const noexcept {
        return manifest_->snapshot();
    }

private:
    static Status ValidateRecoveredHeader(
        const SegmentRecoveryReport& report,
        const PartitionMetadata& partition,
        const SegmentManifestEntry& segment) {
        const SegmentHeader& header = report.segment_header;
        if (header.recording_id != partition.recording_id ||
            header.topic_id != partition.topic_id ||
            header.partition_id != partition.partition_id ||
            header.writer_id != partition.writer_id ||
            header.first_ingestion_sequence !=
                segment.first_ingestion_sequence ||
            header.created_at_ns != segment.created_at_ns) {
            return Corruption(
                "active segment header does not match partition manifest");
        }
        return Status::Ok();
    }

    static SegmentHeader RecoveryHeader(
        const PartitionMetadata& partition,
        const SegmentManifestEntry& segment) noexcept {
        SegmentHeader header;
        header.recording_id = partition.recording_id;
        header.topic_id = partition.topic_id;
        header.partition_id = partition.partition_id;
        header.writer_id = partition.writer_id;
        header.first_ingestion_sequence =
            segment.first_ingestion_sequence;
        header.created_at_ns = segment.created_at_ns;
        return header;
    }

    static Result<std::vector<std::byte>> RecoveryGapRecord(
        const PartitionMetadata& partition,
        const SegmentManifestEntry& segment, uint64_t now_ns) {
        Result<std::vector<std::byte>> gap_payload = EncodeGapPayload(GapPayload{
            .reason = GapReason::kRecorderRestartRecovery,
            .first_missing_source_sequence =
                segment.first_ingestion_sequence,
            .last_missing_source_sequence =
                segment.first_ingestion_sequence,
        });
        if (!gap_payload.ok()) return gap_payload.status();
        Record gap;
        gap.header.flags = kRecordFlagGap;
        gap.header.schema_ref = 0;
        gap.header.topic_id = partition.topic_id;
        gap.header.partition_id = partition.partition_id;
        gap.header.ingestion_sequence =
            segment.first_ingestion_sequence;
        gap.header.ingestion_timestamp_ns = now_ns;
        gap.payload = std::move(*gap_payload);
        return EncodeRecord(gap);
    }

    Status SealRecoveredActive(
        const std::filesystem::path& root, PartitionManifest& manifest,
        const std::unordered_set<uint32_t>& known_schema_refs) {
        const PartitionManifestSnapshot snapshot = manifest.snapshot();
        if (snapshot.segments.empty()) return Status::Ok();
        SegmentManifestEntry active = snapshot.segments.back();
        if (active.state != SegmentPersistentState::kCreating &&
            active.state != SegmentPersistentState::kOpen) {
            return Status::Ok();
        }

        const std::filesystem::path path = root / active.relative_path;
        SegmentRecoveryOptions recovery_options;
        recovery_options.known_schema_refs = &known_schema_refs;
        Result<SegmentRecoveryReport> recovered =
            ScanSegment(path, recovery_options);
        if (recovered.ok()) {
            Status identity = ValidateRecoveredHeader(
                *recovered, snapshot.partition, active);
            if (!identity.ok()) return identity;
            if (recovered->repairable()) {
                SegmentRepairOptions repair_options;
                repair_options.expected_device = recovered->file_device;
                repair_options.expected_inode = recovered->file_inode;
                recovered =
                    RepairSegment(path, recovery_options, repair_options);
            } else if (!recovered->clean()) {
                return Corruption(recovered->reason_detail);
            }
        }
        const uint64_t now_ns = RecoveryNowNs(active.created_at_ns);
        uint64_t actual_size = 0;
        uint64_t actual_last_sequence = active.first_ingestion_sequence;

        if (!recovered.ok() &&
            recovered.status().code() == StatusCode::kNotFound) {
            if (active.size_bytes != kEncodedSegmentHeaderSize ||
                active.last_ingestion_sequence !=
                    active.first_ingestion_sequence) {
                return Corruption(
                    "missing active segment had persisted record progress");
            }
            Status directory = CreateDirectory(path.parent_path());
            if (!directory.ok()) return directory;
            Result<std::vector<std::byte>> encoded_header =
                EncodeSegmentHeader(RecoveryHeader(snapshot.partition, active));
            if (!encoded_header.ok()) return encoded_header.status();
            Result<std::vector<std::byte>> encoded_gap =
                RecoveryGapRecord(snapshot.partition, active, now_ns);
            if (!encoded_gap.ok()) return encoded_gap.status();
            Status written = CreateRecoverySegment(
                path, *encoded_header, *encoded_gap);
            if (!written.ok()) return written;
            actual_size = static_cast<uint64_t>(encoded_header->size()) +
                          static_cast<uint64_t>(encoded_gap->size());
        } else {
            if (!recovered.ok()) return recovered.status();
            Status identity =
                ValidateRecoveredHeader(*recovered, snapshot.partition, active);
            if (!identity.ok()) return identity;
            actual_size = recovered->repaired
                              ? recovered->last_complete_offset
                              : recovered->file_size;
            if (!recovered->has_last_complete_sequence) {
                if (active.size_bytes != kEncodedSegmentHeaderSize ||
                    active.last_ingestion_sequence !=
                        active.first_ingestion_sequence ||
                    actual_size != kEncodedSegmentHeaderSize) {
                    return Corruption(
                        "empty active segment has inconsistent manifest progress");
                }
                Result<std::vector<std::byte>> encoded_gap =
                    RecoveryGapRecord(snapshot.partition, active, now_ns);
                if (!encoded_gap.ok()) return encoded_gap.status();
                Status appended = AppendRecoveryRecord(
                    path, actual_size, recovered->file_device,
                    recovered->file_inode, *encoded_gap);
                if (!appended.ok()) return appended;
                if (actual_size > std::numeric_limits<uint64_t>::max() -
                                      encoded_gap->size()) {
                    return Exhausted("recovered segment size overflows");
                }
                actual_size += static_cast<uint64_t>(encoded_gap->size());
            } else {
                actual_last_sequence = recovered->last_complete_sequence;
                if (active.last_ingestion_sequence > actual_last_sequence ||
                    active.size_bytes > actual_size) {
                    return Corruption(
                        "active manifest progress exceeds recovered segment");
                }
                SegmentRepairOptions repair_options;
                repair_options.expected_device = recovered->file_device;
                repair_options.expected_inode = recovered->file_inode;
                Status synced =
                    TruncateSegment(path, actual_size, repair_options);
                if (!synced.ok()) return synced;
            }
        }

        SegmentManifestEntry open = active;
        open.state = SegmentPersistentState::kOpen;
        open.last_ingestion_sequence = actual_last_sequence;
        open.size_bytes = actual_size;
        if (active.state == SegmentPersistentState::kCreating ||
            open != active) {
            Status updated = manifest.UpdateSegment(open);
            if (!updated.ok()) return updated;
        }

        SegmentManifestEntry sealed = open;
        sealed.state = SegmentPersistentState::kSealed;
        sealed.sealed_at_ns = now_ns;
        Status sealed_status = manifest.UpdateSegment(sealed);
        if (!sealed_status.ok()) return sealed_status;
        return manifest.UpdateCheckpoint(DurableCheckpoint{
            .segment_id = sealed.segment_id,
            .durable_offset = actual_size,
            .durable_sequence = actual_last_sequence,
        });
    }

    Status ReconcileLastSealedCheckpoint(
        const std::filesystem::path& root, PartitionManifest& manifest,
        const std::unordered_set<uint32_t>& known_schema_refs) {
        const PartitionManifestSnapshot snapshot = manifest.snapshot();
        if (snapshot.segments.empty()) return Status::Ok();
        const SegmentManifestEntry& sealed = snapshot.segments.back();
        if (sealed.state != SegmentPersistentState::kSealed) {
            return Status::Ok();
        }
        SegmentRecoveryOptions recovery_options;
        recovery_options.known_schema_refs = &known_schema_refs;
        Result<SegmentRecoveryReport> report = ScanSegment(
            root / sealed.relative_path, recovery_options);
        if (!report.ok()) return report.status();
        if (!report->clean() || !report->has_last_complete_sequence) {
            return Corruption("sealed segment is incomplete or has no record");
        }
        Status identity =
            ValidateRecoveredHeader(*report, snapshot.partition, sealed);
        if (!identity.ok()) return identity;
        if (report->file_size != sealed.size_bytes ||
            report->last_complete_sequence !=
                sealed.last_ingestion_sequence) {
            return Corruption(
                "sealed manifest does not match recovered segment file");
        }
        SegmentRepairOptions repair_options;
        repair_options.expected_device = report->file_device;
        repair_options.expected_inode = report->file_inode;
        Status synced = TruncateSegment(
            root / sealed.relative_path, report->file_size, repair_options);
        if (!synced.ok()) return synced;
        return manifest.UpdateCheckpoint(DurableCheckpoint{
            .segment_id = sealed.segment_id,
            .durable_offset = report->file_size,
            .durable_sequence = report->last_complete_sequence,
        });
    }

    Status RecoverPartition(const std::filesystem::path& root,
                            PartitionManifest& manifest, TopicId topic_id) {
        Result<TopicTableEntry> topic =
            manifest_->FindTopic(topic_id.value);
        if (!topic.ok()) return topic.status();
        std::unordered_set<uint32_t> known_schema_refs;
        try {
            known_schema_refs.reserve(topic->schema_snapshot.size());
            for (const SchemaRefSnapshot& schema : topic->schema_snapshot) {
                known_schema_refs.insert(schema.schema_ref);
            }
        } catch (const std::bad_alloc&) {
            return Exhausted("cannot allocate partition recovery schema set");
        }

        Status recovered =
            SealRecoveredActive(root, manifest, known_schema_refs);
        if (!recovered.ok()) return recovered;
        return ReconcileLastSealedCheckpoint(root, manifest,
                                             known_schema_refs);
    }

    Result<std::unique_ptr<PartitionRuntime>> BuildPartition(
        const RecorderTopicConfig& config, TopicRuntime* topic,
        uint32_t partition_id) {
        const std::filesystem::path root =
            PartitionRoot(session_root_, config.topic_id, partition_id);
        Status directory = CreateDirectory(root);
        if (!directory.ok()) return directory;

        uint64_t writer_id = config.writer_id_base == 0
                                 ? DerivedWriterId(
                                       manifest_->snapshot().session.recording_id,
                                       config.topic_id, partition_id)
                                 : config.writer_id_base + partition_id;
        if (writer_id == 0) return Invalid("recorder writer ID is zero");

        PartitionMetadata metadata{
            .recording_id = manifest_->snapshot().session.recording_id,
            .topic_id = config.topic_id.value,
            .partition_id = partition_id,
            .writer_id = writer_id,
            .owner_epoch = manifest_->snapshot().session.owner_epoch,
            .config_version = config.config_version,
        };
        Result<std::unique_ptr<RecordingTopologyCoordinator>> topology =
            RecordingTopologyCoordinator::Create(topic->policy);
        if (!topology.ok()) return topology.status();

        Result<std::unique_ptr<PartitionManifest>> partition_manifest =
            PartitionManifest::Open(root, options_.manifest_options);
        if (!partition_manifest.ok()) {
            if (partition_manifest.status().code() != StatusCode::kNotFound) {
                // Open reports missing files as unavailable/corruption on some
                // platforms, so inspect the canonical manifest path explicitly.
                std::error_code error;
                const bool manifest_exists =
                    std::filesystem::exists(root / "manifest", error);
                if (error || manifest_exists) return partition_manifest.status();
            }
            partition_manifest = PartitionManifest::Create(
                root, metadata, options_.manifest_options);
            if (!partition_manifest.ok()) return partition_manifest.status();
        }
        if ((*partition_manifest)->snapshot().partition != metadata) {
            return Invalid("partition manifest identity changed on reopen");
        }

        if (topic->policy.mode == RecordingMode::kSnapshot) {
            const PartitionManifestSnapshot& persisted =
                (*partition_manifest)->snapshot();
            if (!persisted.segments.empty() || persisted.checkpoint.has_value()) {
                return Corruption(
                    "snapshot partition manifest contains segment history");
            }
            SnapshotStoreIdentity identity{
                .recording_id = persisted.partition.recording_id,
                .topic_id = persisted.partition.topic_id,
                .partition_id = persisted.partition.partition_id,
                .writer_id = persisted.partition.writer_id,
                .schema_refs = {},
            };
            identity.schema_refs.reserve(config.schemas.size());
            for (const RecorderTopicSchema& configured_schema : config.schemas) {
                Result<SchemaRef> schema_ref =
                    schema_store_->FindRef(configured_schema.identity);
                if (!schema_ref.ok()) return schema_ref.status();
                identity.schema_refs.push_back(*schema_ref);
            }
            Result<std::unique_ptr<SnapshotStore>> snapshot_store =
                SnapshotStore::Open(root, std::move(identity),
                                    config.snapshot_options);
            if (!snapshot_store.ok()) return snapshot_store.status();

            auto partition = std::make_unique<PartitionRuntime>();
            partition->topic = topic;
            partition->partition_id = partition_id;
            partition->writer_id = writer_id;
            partition->manifest = std::move(*partition_manifest);
            partition->topology = std::move(*topology);
            partition->snapshot_store = std::move(*snapshot_store);
            return partition;
        }

        Status recovery =
            RecoverPartition(root, **partition_manifest, config.topic_id);
        if (!recovery.ok()) return recovery;

        RecorderBufferPoolOptions pool_options = config.buffer_pool_options;
        const auto configured_topic_limit =
            config.buffer_pool_options.topic_byte_limits.find(config.topic_id);
        pool_options.topic_byte_limits[config.topic_id] =
            configured_topic_limit !=
                    config.buffer_pool_options.topic_byte_limits.end()
                ? configured_topic_limit->second
                : config.buffer_pool_options.default_topic_byte_limit;
        Result<std::unique_ptr<RecorderBufferPool>> pool =
            RecorderBufferPool::Create(pool_options);
        if (!pool.ok()) return pool.status();
        auto partition = std::make_unique<PartitionRuntime>();
        partition->topic = topic;
        partition->partition_id = partition_id;
        partition->writer_id = writer_id;
        partition->pool_options = pool_options;
        partition->pool = std::move(*pool);
        partition->manifest = std::move(*partition_manifest);
        partition->topology = std::move(*topology);

        SegmentWriterOptions segment_options = config.segment_options;
        if (topic->policy.sync_policy.has_value()) {
            segment_options.sync_policy = *topic->policy.sync_policy;
        }
        TopicWriterOptions writer_options{
            .partition_root = root,
            .recording_id = manifest_->snapshot().session.recording_id,
            .topic_id = config.topic_id,
            .partition_id = partition_id,
            .writer_id = writer_id,
            .initial_ingestion_sequence = 0,
            .segment_options = segment_options,
            .close_buffer_pool_on_stop = true,
            .schema_resolver = nullptr,
            .schema_resolver_context = nullptr,
            .ack_callback = CaptureAck,
            .ack_context = partition.get(),
        };
        Result<std::unique_ptr<TopicWriter>> writer = TopicWriter::Create(
            std::move(writer_options), partition->pool.get(),
            partition->manifest.get(), schema_store_.get());
        if (!writer.ok()) return writer.status();
        partition->writer = std::move(*writer);
        return partition;
    }

    Status StopLocked(uint64_t now_ns) {
        if (state_ == RecorderState::kStopped) return Status::Ok();
        if (state_ == RecorderState::kStopping) {
            return Invalid("Recorder stop is already in progress");
        }
        if (state_ == RecorderState::kError) return session_error_;
        if (now_ns < last_now_ns_) {
            return Invalid("Recorder time must be nondecreasing");
        }
        last_now_ns_ = now_ns;
        state_ = RecorderState::kStopping;
        bool failed = false;
        for (auto& [topic_id, topic] : topics_) {
            static_cast<void>(topic_id);
            for (auto& [partition_id, partition] : topic->partitions) {
                static_cast<void>(partition_id);
                if (!partition->error.ok()) {
                    if (partition->pool != nullptr) partition->pool->Close();
                    partition->snapshot_writer_state = TopicWriterState::kError;
                    failed = true;
                    continue;
                }
                if (partition->snapshot_store != nullptr) {
                    partition->snapshot_writer_state = TopicWriterState::kStopped;
                    continue;
                }
                const Status status = partition->writer->Stop(now_ns);
                if (!status.ok()) {
                    partition->error = status;
                    failed = true;
                }
            }
        }
        state_ = RecorderState::kStopped;
        return failed ? Degraded("one or more recorder partitions failed Stop")
                      : Status::Ok();
    }

    Status PoisonSession(Status status) {
        session_error_ = std::move(status);
        state_ = RecorderState::kError;
        for (auto& [topic_id, topic] : topics_) {
            static_cast<void>(topic_id);
            for (auto& [partition_id, partition] : topic->partitions) {
                static_cast<void>(partition_id);
                if (partition->pool != nullptr) partition->pool->Close();
                if (partition->snapshot_store != nullptr) {
                    partition->snapshot_writer_state = TopicWriterState::kError;
                }
            }
        }
        return session_error_;
    }

    void MarkPartitionFailed(PartitionRuntime& partition, Status status,
                             uint64_t now_ns) noexcept {
        if (!partition.error.ok()) return;
        partition.error = std::move(status);
        if (partition.pool != nullptr) partition.pool->Close();
        if (partition.snapshot_store != nullptr) {
            partition.snapshot_writer_state = TopicWriterState::kError;
        }
        static_cast<void>(partition.topology->RecorderFailed(now_ns));
        SaturatingAdd(1, &metrics_.writer_failures);
        if (state_ == RecorderState::kRunning) {
            state_ = RecorderState::kDegraded;
        }
    }

    void FailTopic(TopicRuntime& topic, const Status& status,
                   uint64_t now_ns) noexcept {
        for (auto& [partition_id, partition] : topic.partitions) {
            static_cast<void>(partition_id);
            MarkPartitionFailed(*partition, status, now_ns);
        }
    }

    void FailForWriterError(TopicRuntime& topic, PartitionRuntime& partition,
                            const Status& status, uint64_t now_ns) noexcept {
        if (topic.policy.full_policy == BufferFullPolicy::kFailRecording ||
            topic.policy.require_complete_recording) {
            FailTopic(topic, status, now_ns);
        } else {
            MarkPartitionFailed(partition, status, now_ns);
        }
    }

    Status ReportTopologyDebts(
        PartitionRuntime& partition,
        const RecorderRecordMetadata& current_metadata, uint64_t cursor,
        std::span<const RecordingGapDebt> debts) {
        for (const RecordingGapDebt& debt : debts) {
            RecorderRecordMetadata debt_metadata = current_metadata;
            BufferDiscardReason reason = BufferDiscardReason::kReservationCancelled;
            if (debt.reason == RecordingGapReason::kIsolatedFanoutFull ||
                debt.reason == RecordingGapReason::kBestEffortBufferFull) {
                reason = BufferDiscardReason::kDropNewest;
            } else if (!(debt.first_cursor <= cursor &&
                         cursor < debt.end_cursor)) {
                debt_metadata.source.source_sequence = debt.first_cursor;
            }
            const Status status = partition.writer->ReportDiscard(
                DiscardedBuffer{
                    .reason = reason,
                    .topic_id = current_metadata.topic_id,
                    .user_tag = 0,
                    .payload_size = current_metadata.payload_size,
                    .charged_bytes = 0,
                    .metadata = debt_metadata,
                });
            if (!status.ok()) return status;
        }
        return Status::Ok();
    }

    static bool ContainsSchema(const TopicRuntime& topic,
                               const RecorderSchemaMetadata& schema) noexcept {
        return std::find(topic.schemas.begin(), topic.schemas.end(), schema) !=
               topic.schemas.end();
    }

    TopicRuntime* FindTopic(TopicId topic_id) noexcept {
        const auto found = topics_.find(topic_id.value);
        return found == topics_.end() ? nullptr : found->second.get();
    }

    const TopicRuntime* FindTopic(TopicId topic_id) const noexcept {
        const auto found = topics_.find(topic_id.value);
        return found == topics_.end() ? nullptr : found->second.get();
    }

    void AccumulatePump(const TopicWriterPumpResult& value,
                        RecorderPumpResult* aggregate) noexcept {
        SaturatingAdd(value.duplicate_records, &metrics_.duplicate_records);
        if (aggregate == nullptr) return;
        aggregate->dequeued_records += value.dequeued_records;
        aggregate->data_records += value.data_records;
        aggregate->tombstone_records += value.tombstone_records;
        aggregate->gap_records += value.gap_records;
        aggregate->duplicate_records += value.duplicate_records;
        aggregate->rotations += value.rotations;
    }

    mutable std::mutex mutex_;
    std::filesystem::path session_root_;
    RecorderSessionOptions options_;
    std::unique_ptr<RecordingManifest> manifest_;
    std::unique_ptr<schema::SchemaRegistry> registry_;
    std::unique_ptr<SchemaStore> schema_store_;
    std::shared_ptr<capacity::CapacityController> capacity_controller_;
    std::map<uint32_t, std::unique_ptr<TopicRuntime>> topics_;
    RecorderState state_ = RecorderState::kCreated;
    Status session_error_ = Status::Ok();
    uint64_t last_now_ns_ = 0;
    RecorderMetrics metrics_;
};

Result<std::unique_ptr<Recorder>> Recorder::FinishCreate(
    const std::filesystem::path& session_root, RecorderSessionOptions options,
    std::unique_ptr<RecordingManifest> manifest,
    std::shared_ptr<capacity::CapacityController> capacity_controller) {
    try {
        auto registry = std::make_unique<schema::SchemaRegistry>();
        Result<std::unique_ptr<SchemaStore>> schema_store = SchemaStore::Open(
            session_root, registry.get(), options.schema_store_options);
        if (!schema_store.ok()) return schema_store.status();
        auto impl = std::make_unique<Recorder::Impl>(
            session_root, std::move(options), std::move(manifest),
            std::move(registry), std::move(*schema_store),
            std::move(capacity_controller));
        return std::unique_ptr<Recorder>(new Recorder(std::move(impl)));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate Recorder session");
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Result<std::unique_ptr<Recorder>> Recorder::Create(
    const std::filesystem::path& session_root,
    const RecordingSessionMetadata& metadata,
    const RecorderSessionOptions& options,
    std::shared_ptr<capacity::CapacityController> capacity_controller) noexcept {
    try {
        if (session_root.empty()) return Invalid("Recorder session root is empty");
        Status directory = CreateDirectory(session_root);
        if (!directory.ok()) return directory;
        Result<std::unique_ptr<RecordingManifest>> manifest =
            RecordingManifest::Create(session_root, metadata,
                                      options.manifest_options);
        if (!manifest.ok()) return manifest.status();
        return FinishCreate(session_root, options, std::move(*manifest),
                            std::move(capacity_controller));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate Recorder session");
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Result<std::unique_ptr<Recorder>> Recorder::Open(
    const std::filesystem::path& session_root,
    const RecorderSessionOptions& options,
    std::shared_ptr<capacity::CapacityController> capacity_controller) noexcept {
    try {
        if (session_root.empty()) return Invalid("Recorder session root is empty");
        Result<std::unique_ptr<RecordingManifest>> manifest =
            RecordingManifest::Open(session_root, options.manifest_options);
        if (!manifest.ok()) return manifest.status();
        return FinishCreate(session_root, options, std::move(*manifest),
                            std::move(capacity_controller));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate Recorder session");
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Recorder::Recorder(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

Recorder::~Recorder() = default;

Status Recorder::AddTopic(
    const RecorderTopicConfig& config,
    std::optional<capacity::ResourceVector> capacity_charge) noexcept {
    return impl_->AddTopic(config, std::move(capacity_charge));
}

Status Recorder::Start(uint64_t now_ns) noexcept {
    return impl_->Start(now_ns);
}

Result<RecorderEnqueueResult> Recorder::Enqueue(
    const RecorderEnqueueRequest& request) noexcept {
    return impl_->Enqueue(request);
}

Result<RecorderPumpResult> Recorder::Pump(
    uint64_t now_ns, size_t max_records_per_partition) noexcept {
    return impl_->Pump(now_ns, max_records_per_partition);
}

Status Recorder::Flush(RecordAckLevel level, uint64_t now_ns) noexcept {
    return impl_->Flush(level, now_ns);
}

Status Recorder::Stop(uint64_t now_ns) noexcept {
    return impl_->Stop(now_ns);
}

RecorderState Recorder::state() const noexcept { return impl_->state(); }

Status Recorder::error_status() const noexcept {
    return impl_->error_status();
}

RecorderMetrics Recorder::metrics() const noexcept { return impl_->metrics(); }

Result<RecorderPartitionStatus> Recorder::GetPartitionStatus(
    TopicId topic_id, uint32_t partition_id, uint64_t now_ns) const noexcept {
    return impl_->GetPartitionStatus(topic_id, partition_id, now_ns);
}

const RecordingManifestSnapshot& Recorder::manifest_snapshot() const noexcept {
    return impl_->manifest_snapshot();
}

}  // namespace mino::storage
