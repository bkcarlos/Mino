// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "tools/mino/runtime_services.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <exception>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <string_view>
#include <thread>
#include <utility>

#include "toml.hpp"

#include "mino/platform/process_identity.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/storage/recorder.h"
#include "mino/storage/recorder_service.h"
#include "mino/storage/recorder_subscriber.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/recording_policy.h"
#include "mino/storage/segment_writer.h"

namespace mino::tools {
namespace {

constexpr size_t kMaximumConfigBytes = 4u * 1024u * 1024u;
constexpr size_t kMaximumArtifactBytes = 16u * 1024u * 1024u;

Status Invalid(std::string message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

uint64_t MonotonicNowNs() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

bool IsAllowed(std::string_view key,
               std::initializer_list<std::string_view> allowed) {
    return std::find(allowed.begin(), allowed.end(), key) != allowed.end();
}

Status ValidateKeys(const toml::table& table, std::string_view name,
                    std::initializer_list<std::string_view> allowed) {
    for (const auto& [key, value] : table) {
        static_cast<void>(value);
        if (!IsAllowed(key.str(), allowed)) {
            return Invalid("unknown key '" + std::string(name) + "." +
                           std::string(key.str()) + "'");
        }
    }
    return Status::Ok();
}

Result<const toml::table*> RequiredTable(const toml::table& parent,
                                         std::string_view key) {
    const toml::node* node = parent.get(key);
    if (node == nullptr || node->as_table() == nullptr) {
        return Invalid("'" + std::string(key) + "' must be a table");
    }
    return node->as_table();
}

Result<std::string> RequiredString(const toml::table& table,
                                   std::string_view key,
                                   std::string_view full_name) {
    const toml::node* node = table.get(key);
    if (node == nullptr) {
        return Invalid("missing required key '" + std::string(full_name) + "'");
    }
    std::optional<std::string> value = node->value<std::string>();
    if (!value.has_value() || value->empty() || value->find('\0') != std::string::npos) {
        return Invalid("'" + std::string(full_name) +
                       "' must be a non-empty string");
    }
    return std::move(*value);
}

template <typename T>
Result<T> Integer(const toml::table& table, std::string_view key,
                  std::string_view full_name, T default_value, bool required,
                  bool allow_zero = false) {
    const toml::node* node = table.get(key);
    if (node == nullptr) {
        if (required) {
            return Invalid("missing required key '" + std::string(full_name) + "'");
        }
        return default_value;
    }
    const std::optional<int64_t> value = node->value<int64_t>();
    if (!value.has_value() || *value < 0 || (!allow_zero && *value == 0) ||
        static_cast<uint64_t>(*value) >
            static_cast<uint64_t>(std::numeric_limits<T>::max())) {
        return Invalid("'" + std::string(full_name) +
                       "' is outside its allowed integer range");
    }
    return static_cast<T>(*value);
}

Result<std::vector<std::byte>> ReadRegularFile(
    const std::filesystem::path& path, size_t limit) {
    if (path.empty() || path.string().size() > 4096) {
        return Invalid("configured file path is empty or too long");
    }
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        return Status::Error(errno == ELOOP ? StatusCode::kPermissionDenied
                                           : StatusCode::kNotFound,
                             "cannot open configured file '" + path.string() + "'");
    }
    struct stat state {};
    if (::fstat(fd, &state) != 0 || !S_ISREG(state.st_mode) || state.st_nlink != 1 ||
        state.st_size < 0 || static_cast<uint64_t>(state.st_size) > limit) {
        ::close(fd);
        return Status::Error(StatusCode::kPermissionDenied,
                             "configured file is not a bounded single-link regular file");
    }
    try {
        std::vector<std::byte> bytes(static_cast<size_t>(state.st_size));
        size_t offset = 0;
        while (offset < bytes.size()) {
            const ssize_t count =
                ::read(fd, bytes.data() + offset, bytes.size() - offset);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) {
                ::close(fd);
                return Status::Error(StatusCode::kUnavailable,
                                     "configured file changed while reading");
            }
            offset += static_cast<size_t>(count);
        }
        ::close(fd);
        return bytes;
    } catch (const std::bad_alloc&) {
        ::close(fd);
        return Exhausted("cannot allocate configured file buffer");
    }
}

std::filesystem::path ResolvePath(const std::filesystem::path& config_path,
                                  std::string_view value) {
    std::filesystem::path path(value);
    if (path.is_relative()) path = config_path.parent_path() / path;
    return path.lexically_normal();
}

uint64_t SchemaShortId(
    const std::array<std::byte, storage::kSchemaDigestSize>& digest) noexcept {
    uint64_t value = 0;
    for (size_t index = 0; index < sizeof(value); ++index) {
        value |= static_cast<uint64_t>(static_cast<uint8_t>(digest[index]))
                 << (index * 8u);
    }
    return value;
}

storage::RecorderSchemaMetadata RecorderSchema(
    const schema::SchemaIdentity& identity) {
    return storage::RecorderSchemaMetadata{
        .short_id = identity.short_id(),
        .canonical_digest = identity.canonical_digest(),
        .schema_version = identity.schema_version(),
        .layout_version = identity.layout_version(),
    };
}

bool SameSchema(const storage::RecorderSchemaMetadata& left,
                const storage::RecorderSchemaMetadata& right) noexcept {
    return left == right;
}

const RuntimeTopicConfig* FindRuntimeTopic(const RuntimeConfig& config,
                                           std::string_view name) noexcept {
    const auto found = std::find_if(
        config.topics.begin(), config.topics.end(),
        [name](const RuntimeTopicConfig& topic) { return topic.bus.name == name; });
    return found == config.topics.end() ? nullptr : &*found;
}

class BusRecorderSource final : public storage::RecorderServiceSource {
public:
    static Result<std::unique_ptr<BusRecorderSource>> Create(
        BusSubscriber subscriber, storage::Recorder& recorder,
        TopicId recording_topic_id, storage::RecorderSchemaMetadata schema,
        uint32_t schema_ref, size_t max_payload_bytes,
        std::unique_ptr<storage::FileRecorderPendingStore> pending_store) {
        if (schema_ref == 0 || max_payload_bytes == 0 || pending_store == nullptr) {
            return Invalid("Bus recorder source configuration is incomplete");
        }
        Result<std::optional<storage::RecorderPersistedPendingRecord>> loaded =
            pending_store->Load();
        if (!loaded.ok()) return loaded.status();
        if (loaded->has_value()) {
            const storage::RecorderPersistedPendingRecord& record = **loaded;
            if (record.metadata.topic_id != recording_topic_id ||
                record.schema_ref != schema_ref ||
                !SameSchema(record.metadata.schema, schema) ||
                record.payload.size() > max_payload_bytes) {
                return Status::Error(StatusCode::kCorruption,
                                     "pending Bus recorder record does not match runtime config");
            }
        }
        try {
            return std::unique_ptr<BusRecorderSource>(new BusRecorderSource(
                std::move(subscriber), recorder, recording_topic_id,
                std::move(schema), schema_ref, max_payload_bytes,
                std::move(pending_store), std::move(*loaded)));
        } catch (const std::bad_alloc&) {
            return Exhausted("cannot allocate Bus recorder source");
        }
    }

    Result<storage::RecorderRecordResult> PollOne() noexcept override {
        try {
            if (!pending_.has_value()) {
                Result<CanonicalMessage> message = subscriber_.TryPoll();
                if (!message.ok()) return message.status();
                if (message->payload.empty() ||
                    message->payload.size() > max_payload_bytes_ ||
                    !registry::SchemaIdentityEqual(message->schema,
                                                   schema::SchemaIdentity(
                                                       schema_.short_id,
                                                       schema_.canonical_digest,
                                                       schema_.schema_version,
                                                       schema_.layout_version))) {
                    return Status::Error(StatusCode::kSchemaMismatch,
                                         "Bus recorder message violates topic/schema bounds");
                }
                storage::RecorderPersistedPendingRecord record{
                    .metadata = storage::RecorderRecordMetadata{
                        .schema = schema_,
                        .topic_id = recording_topic_id_,
                        .source = storage::MessageSource{
                            .node_id = message->publication.source.node_id,
                            .publisher_id =
                                message->publication.source.publisher_id,
                            .publisher_epoch =
                                message->publication.source.publisher_epoch,
                            .source_sequence =
                                message->publication.sequence_num,
                            .observed_timestamp_ns =
                                message->publication.timestamp_ns,
                        },
                        .ingestion_timestamp_ns = MonotonicNowNs(),
                        .payload_size =
                            static_cast<uint32_t>(message->payload.size()),
                        .payload_crc = storage::RecorderPayloadCrc32c(
                            message->payload),
                    },
                    .schema_ref = schema_ref_,
                    .payload = std::move(message->payload),
                    .user_tag = 0,
                    .ack_attempted = true,
                    .ack_code = StatusCode::kOk,
                };
                // BusSubscriber returns owned canonical bytes after the local
                // channel ACK. Persist immediately, before recorder admission,
                // so every returned message is either journaled or fails closed.
                const Status saved = pending_store_->Save(record);
                if (!saved.ok()) return saved;
                pending_ = std::move(record);
            }
            return AdmitPending();
        } catch (const std::bad_alloc&) {
            return Exhausted("cannot allocate Bus recorder message");
        } catch (const std::exception& error) {
            return Status::Error(StatusCode::kInternal, error.what());
        } catch (...) {
            return Status::Error(StatusCode::kInternal,
                                 "Bus recorder source failed unexpectedly");
        }
    }

    bool has_pending() const noexcept override { return pending_.has_value(); }
    size_t pending_bytes() const noexcept override {
        return pending_.has_value() ? pending_->payload.size() : 0;
    }
    bool pending_persistence_configured() const noexcept override { return true; }
    Status PersistPending() noexcept override {
        return pending_.has_value() ? pending_store_->Save(*pending_)
                                    : Status::Ok();
    }

private:
    BusRecorderSource(
        BusSubscriber subscriber, storage::Recorder& recorder,
        TopicId recording_topic_id, storage::RecorderSchemaMetadata schema,
        uint32_t schema_ref, size_t max_payload_bytes,
        std::unique_ptr<storage::FileRecorderPendingStore> pending_store,
        std::optional<storage::RecorderPersistedPendingRecord> pending) noexcept
        : subscriber_(std::move(subscriber)),
          recorder_(&recorder),
          recording_topic_id_(recording_topic_id),
          schema_(std::move(schema)),
          schema_ref_(schema_ref),
          max_payload_bytes_(max_payload_bytes),
          pending_store_(std::move(pending_store)),
          pending_(std::move(pending)) {}

    Result<storage::RecorderRecordResult> AdmitPending() {
        storage::RecorderPersistedPendingRecord& record = *pending_;
        Result<storage::RecorderEnqueueResult> enqueued = recorder_->Enqueue(
            0, record.metadata, record.payload, record.user_tag, std::nullopt,
            std::chrono::nanoseconds::zero());
        if (!enqueued.ok()) return enqueued.status();
        storage::RecorderRecordResult result{
            .disposition = storage::RecorderRecordDisposition::kFailed,
            .status = enqueued->status,
            .ack_status = Status::Ok(),
            .ack_attempted = true,
            .pending = true,
            .metadata = record.metadata,
            .discarded = std::move(enqueued->discarded),
        };
        switch (enqueued->disposition) {
            case storage::RecorderEnqueueDisposition::kBuffered: {
                const Status cleared = pending_store_->Clear();
                if (!cleared.ok()) return cleared;
                pending_.reset();
                result.disposition = storage::RecorderRecordDisposition::kBuffered;
                result.pending = false;
                return result;
            }
            case storage::RecorderEnqueueDisposition::kBlocked:
                result.disposition = storage::RecorderRecordDisposition::kBlocked;
                return result;
            case storage::RecorderEnqueueDisposition::kDropped:
                result.disposition = storage::RecorderRecordDisposition::kFailed;
                result.status = Status::Error(
                    StatusCode::kUnavailable,
                    "complete Bus recording policy rejected a record");
                return result;
            case storage::RecorderEnqueueDisposition::kFailed:
                result.disposition = storage::RecorderRecordDisposition::kFailed;
                if (result.status.ok()) {
                    result.status = Status::Error(StatusCode::kUnavailable,
                                                  "recorder admission failed");
                }
                return result;
        }
        return Status::Error(StatusCode::kInternal,
                             "recorder returned an unknown admission result");
    }

    BusSubscriber subscriber_;
    storage::Recorder* recorder_ = nullptr;
    TopicId recording_topic_id_{};
    storage::RecorderSchemaMetadata schema_;
    uint32_t schema_ref_ = 0;
    size_t max_payload_bytes_ = 0;
    std::unique_ptr<storage::FileRecorderPendingStore> pending_store_;
    std::optional<storage::RecorderPersistedPendingRecord> pending_;
};

std::string ReplayPublisherKey(
    std::string_view destination,
    const storage::SchemaRefSnapshot& schema_snapshot) {
    constexpr char digits[] = "0123456789abcdef";
    std::string key(destination);
    key.push_back('#');
    key.reserve(key.size() + schema_snapshot.canonical_digest.size() * 2 + 24);
    for (std::byte byte : schema_snapshot.canonical_digest) {
        const uint8_t value = static_cast<uint8_t>(byte);
        key.push_back(digits[value >> 4]);
        key.push_back(digits[value & 0x0f]);
    }
    key.push_back(':');
    key += std::to_string(schema_snapshot.schema_version);
    key.push_back(':');
    key += std::to_string(schema_snapshot.layout_version);
    return key;
}

bool SafeNamespace(std::string_view value) noexcept {
    return !value.empty() && value.size() <= 64 && value != "." && value != ".." &&
           std::all_of(value.begin(), value.end(), [](char character) {
               return (character >= 'a' && character <= 'z') ||
                      (character >= 'A' && character <= 'Z') ||
                      (character >= '0' && character <= '9') ||
                      character == '-' || character == '_';
           });
}

}  // namespace

Result<RuntimeConfig> LoadRuntimeConfig(
    const std::filesystem::path& path) noexcept {
    try {
        Result<std::vector<std::byte>> bytes =
            ReadRegularFile(path, kMaximumConfigBytes);
        if (!bytes.ok()) return bytes.status();
        const std::string text(reinterpret_cast<const char*>(bytes->data()),
                               bytes->size());
        toml::table root = toml::parse(text);
        MINO_RETURN_IF_ERROR(
            ValidateKeys(root, "root", {"runtime", "record", "topics"}));
        Result<const toml::table*> runtime_table =
            RequiredTable(root, "runtime");
        if (!runtime_table.ok()) return runtime_table.status();
        MINO_RETURN_IF_ERROR(ValidateKeys(
            **runtime_table, "runtime",
            {"node_id", "lease_epoch", "lease_duration_ms", "region_id",
             "region_bytes", "topic_id_state"}));

        RuntimeConfig config;
        Result<uint64_t> node_id = Integer<uint64_t>(
            **runtime_table, "node_id", "runtime.node_id", 0, true);
        if (!node_id.ok()) return node_id.status();
        config.bus.node_id = NodeId{*node_id};
        Result<uint64_t> lease_epoch = Integer<uint64_t>(
            **runtime_table, "lease_epoch", "runtime.lease_epoch", 1, false);
        if (!lease_epoch.ok()) return lease_epoch.status();
        config.bus.lease_epoch = *lease_epoch;
        Result<uint64_t> lease_ms = Integer<uint64_t>(
            **runtime_table, "lease_duration_ms", "runtime.lease_duration_ms",
            60'000, false);
        if (!lease_ms.ok() ||
            (lease_ms.ok() && *lease_ms >
                                  std::numeric_limits<uint64_t>::max() /
                                      1'000'000ull)) {
            return lease_ms.ok() ? Invalid("runtime lease duration overflows")
                                 : lease_ms.status();
        }
        config.bus.lease_duration_ns = *lease_ms * 1'000'000ull;
        Result<uint32_t> region_id = Integer<uint32_t>(
            **runtime_table, "region_id", "runtime.region_id", 0, true);
        if (!region_id.ok()) return region_id.status();
        config.bus.region_id = *region_id;
        Result<uint64_t> region_bytes = Integer<uint64_t>(
            **runtime_table, "region_bytes", "runtime.region_bytes", 0, true);
        if (!region_bytes.ok()) return region_bytes.status();
        config.bus.region_bytes = *region_bytes;
        Result<std::string> state_path = RequiredString(
            **runtime_table, "topic_id_state", "runtime.topic_id_state");
        if (!state_path.ok()) return state_path.status();
        config.bus.topic_id_state_path = ResolvePath(path, *state_path);

        if (const toml::node* record_node = root.get("record");
            record_node != nullptr) {
            const toml::table* record = record_node->as_table();
            if (record == nullptr) return Invalid("'record' must be a table");
            MINO_RETURN_IF_ERROR(ValidateKeys(
                *record, "record",
                {"stop_after_records", "max_runtime_ms", "poll_interval_ms",
                 "buffer_bytes", "queue_capacity", "max_segment_bytes"}));
            Result<uint64_t> stop_after = Integer<uint64_t>(
                *record, "stop_after_records", "record.stop_after_records", 0,
                false, true);
            if (!stop_after.ok()) return stop_after.status();
            config.record_stop_after_records = *stop_after;
            Result<uint64_t> max_runtime = Integer<uint64_t>(
                *record, "max_runtime_ms", "record.max_runtime_ms", 0, false,
                true);
            if (!max_runtime.ok()) return max_runtime.status();
            config.record_max_runtime_ms = *max_runtime;
            Result<uint64_t> poll = Integer<uint64_t>(
                *record, "poll_interval_ms", "record.poll_interval_ms", 1,
                false);
            if (!poll.ok() || (poll.ok() && *poll > 1000)) {
                return poll.ok() ? Invalid("record.poll_interval_ms exceeds 1000")
                                 : poll.status();
            }
            config.record_poll_interval_ms = *poll;
            Result<size_t> buffer = Integer<size_t>(
                *record, "buffer_bytes", "record.buffer_bytes",
                config.recorder_buffer_bytes, false);
            if (!buffer.ok()) return buffer.status();
            config.recorder_buffer_bytes = *buffer;
            Result<size_t> queue = Integer<size_t>(
                *record, "queue_capacity", "record.queue_capacity",
                config.recorder_queue_capacity, false);
            if (!queue.ok()) return queue.status();
            config.recorder_queue_capacity = *queue;
            Result<uint64_t> segment = Integer<uint64_t>(
                *record, "max_segment_bytes", "record.max_segment_bytes",
                config.max_segment_bytes, false);
            if (!segment.ok()) return segment.status();
            config.max_segment_bytes = *segment;
        }

        const toml::node* topics_node = root.get("topics");
        const toml::array* topics =
            topics_node == nullptr ? nullptr : topics_node->as_array();
        if (topics == nullptr || topics->empty() ||
            topics->size() > deployment::kMaximumLocalDeploymentTopics) {
            return Invalid("'topics' must be a non-empty bounded array of tables");
        }
        for (size_t index = 0; index < topics->size(); ++index) {
            const toml::table* topic = (*topics)[index].as_table();
            if (topic == nullptr) return Invalid("every topics entry must be a table");
            const std::string prefix = "topics[" + std::to_string(index) + "]";
            MINO_RETURN_IF_ERROR(ValidateKeys(
                *topic, prefix,
                {"name", "schema_artifact", "schema_type", "channel_capacity",
                 "max_subscribers", "max_payload_bytes", "record_partitions"}));
            Result<std::string> name = RequiredString(*topic, "name", prefix + ".name");
            if (!name.ok()) return name.status();
            Result<std::string> artifact_path = RequiredString(
                *topic, "schema_artifact", prefix + ".schema_artifact");
            if (!artifact_path.ok()) return artifact_path.status();
            Result<std::vector<std::byte>> artifact = ReadRegularFile(
                ResolvePath(path, *artifact_path), kMaximumArtifactBytes);
            if (!artifact.ok()) return artifact.status();
            const std::string artifact_text(
                reinterpret_cast<const char*>(artifact->data()), artifact->size());
            Result<schema::codegen::DecodedDescriptorArtifact> decoded =
                schema::codegen::DecodeAndValidate(artifact_text);
            if (!decoded.ok()) return decoded.status();
            const schema::codegen::DecodedTypeArtifact* selected = nullptr;
            const toml::node* schema_type_node = topic->get("schema_type");
            if (schema_type_node != nullptr) {
                const std::optional<std::string_view> schema_type =
                    schema_type_node->value<std::string_view>();
                if (!schema_type.has_value() || schema_type->empty()) {
                    return Invalid("'" + prefix +
                                   ".schema_type' must be a non-empty string");
                }
                const auto found = std::find_if(
                    decoded->types.begin(), decoded->types.end(),
                    [schema_type](const auto& type) {
                        return type.descriptor->aggregate().full_name() ==
                               *schema_type;
                    });
                if (found == decoded->types.end()) {
                    return Status::Error(StatusCode::kNotFound,
                                         "configured schema_type is absent from artifact");
                }
                selected = &*found;
            } else if (decoded->types.size() == 1) {
                selected = &decoded->types.front();
            } else {
                return Invalid("schema_type is required for a multi-type artifact");
            }
            Result<uint32_t> capacity = Integer<uint32_t>(
                *topic, "channel_capacity", prefix + ".channel_capacity", 256,
                false);
            if (!capacity.ok()) return capacity.status();
            Result<uint32_t> subscribers = Integer<uint32_t>(
                *topic, "max_subscribers", prefix + ".max_subscribers", 16,
                false);
            if (!subscribers.ok()) return subscribers.status();
            Result<size_t> max_payload = Integer<size_t>(
                *topic, "max_payload_bytes", prefix + ".max_payload_bytes",
                1024u * 1024u, false);
            if (!max_payload.ok()) return max_payload.status();
            Result<uint32_t> partitions = Integer<uint32_t>(
                *topic, "record_partitions", prefix + ".record_partitions", 1,
                false);
            if (!partitions.ok() || (partitions.ok() && *partitions != 1)) {
                return partitions.ok()
                           ? Invalid("record_partitions must be 1 because Bus messages have no partition key")
                           : partitions.status();
            }
            RuntimeTopicConfig configured{
                .bus = deployment::LocalTopicConfig{
                    .name = std::move(*name),
                    .schema = selected->descriptor->identity(),
                    .channel_capacity = *capacity,
                    .max_subscribers = *subscribers,
                    .max_payload_bytes = *max_payload,
                },
                .descriptor_artifact = std::move(*artifact),
                .record_partitions = *partitions,
            };
            config.bus.topics.push_back(configured.bus);
            config.topics.push_back(std::move(configured));
        }
        size_t maximum_payload_bytes = 0;
        for (const RuntimeTopicConfig& topic : config.topics) {
            maximum_payload_bytes =
                std::max(maximum_payload_bytes, topic.bus.max_payload_bytes);
        }
        constexpr uint64_t kMaximumRecordRuntimeMs = 24ull * 60 * 60 * 1000;
        constexpr uint64_t kMaximumStopAfterRecords = 1ull << 40;
        if (config.record_max_runtime_ms > kMaximumRecordRuntimeMs ||
            config.record_stop_after_records > kMaximumStopAfterRecords ||
            config.recorder_buffer_bytes <
                storage::kRecorderLargeBufferClassBytes ||
            config.recorder_buffer_bytes > deployment::kMaximumLocalRegionBytes ||
            maximum_payload_bytes > config.recorder_buffer_bytes ||
            config.recorder_queue_capacity == 0 ||
            config.recorder_queue_capacity > 1u << 20 ||
            maximum_payload_bytes >
                std::numeric_limits<uint64_t>::max() - 4096u ||
            config.max_segment_bytes < maximum_payload_bytes + 4096u) {
            return Invalid("record runtime/buffer/queue/segment limits are out of bounds");
        }
        return config;
    } catch (const toml::parse_error& error) {
        return Invalid("invalid runtime TOML: " + std::string(error.description()));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate runtime configuration");
    } catch (const std::exception& error) {
        return Invalid(error.what());
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "runtime configuration failed unexpectedly");
    }
}

BusRecorderServiceLauncher::BusRecorderServiceLauncher(
    deployment::LocalBusDeployment& deployment, RuntimeConfig config,
    volatile std::sig_atomic_t* signal_stop)
    : deployment_(&deployment),
      config_(std::move(config)),
      signal_stop_(signal_stop) {}

Status BusRecorderServiceLauncher::Run(
    const std::filesystem::path& session_root) noexcept {
    bool expected = false;
    if (!run_claimed_.compare_exchange_strong(expected, true)) {
        return Invalid("recorder launcher may only run once");
    }
    stop_requested_.store(false, std::memory_order_release);
    try {
        Result<std::unique_ptr<storage::RecorderService>> service =
            storage::RecorderService::OpenRecovered(session_root);
        if (!service.ok()) return service.status();
        const storage::RecordingManifestSnapshot initial_manifest =
            (*service)->recorder().manifest_snapshot();
        if (initial_manifest.topics.empty() ||
            initial_manifest.topics.size() > config_.topics.size()) {
            return Invalid("recording manifest topics do not match runtime config");
        }
        std::error_code directory_error;
        std::filesystem::create_directories(session_root / "pending",
                                            directory_error);
        if (directory_error) {
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot create recorder pending directory");
        }
        for (const storage::TopicTableEntry& manifest_topic :
             initial_manifest.topics) {
            const RuntimeTopicConfig* configured =
                FindRuntimeTopic(config_, manifest_topic.topic_name);
            if (configured == nullptr) {
                return Status::Error(StatusCode::kNotFound,
                                     "recording topic is absent from runtime config");
            }
            Result<std::unique_ptr<storage::PartitionManifest>> partition_manifest =
                storage::PartitionManifest::Open(
                    session_root / "topics" /
                    std::to_string(manifest_topic.topic_id) / "partitions/0000");
            if (!partition_manifest.ok()) return partition_manifest.status();
            const storage::PartitionMetadata partition_identity =
                (*partition_manifest)->snapshot().partition;
            partition_manifest->reset();
            if (partition_identity.topic_id != manifest_topic.topic_id ||
                partition_identity.partition_id != 0 ||
                partition_identity.config_version != manifest_topic.config_version ||
                partition_identity.writer_id == 0) {
                return Status::Error(
                    StatusCode::kCorruption,
                    "recording partition identity differs from the topic manifest");
            }

            storage::RecorderTopicConfig recorder_topic;
            recorder_topic.topic_id = TopicId{manifest_topic.topic_id};
            recorder_topic.topic_name = manifest_topic.topic_name;
            recorder_topic.config_version = manifest_topic.config_version;
            recorder_topic.partition_count = configured->record_partitions;
            recorder_topic.writer_id_base = partition_identity.writer_id;
            recorder_topic.policy.mode = storage::RecordingMode::kDurable;
            recorder_topic.policy.backpressure_topology =
                storage::RecordBackpressureTopology::kIsolated;
            recorder_topic.policy.full_policy = storage::BufferFullPolicy::kBlock;
            recorder_topic.policy.ack_level = storage::RecordAckLevel::kDurable;
            recorder_topic.policy.sync_policy =
                storage::SegmentSyncPolicy::kPerBatch;
            recorder_topic.policy.require_complete_recording = true;
            recorder_topic.buffer_pool_options.global_byte_limit =
                config_.recorder_buffer_bytes;
            recorder_topic.buffer_pool_options.default_topic_byte_limit =
                config_.recorder_buffer_bytes;
            recorder_topic.buffer_pool_options.queue_capacity =
                config_.recorder_queue_capacity;
            recorder_topic.buffer_pool_options.max_large_object_bytes =
                std::max(configured->bus.max_payload_bytes,
                         storage::kRecorderLargeBufferClassBytes);
            recorder_topic.segment_options.sync_policy =
                storage::SegmentSyncPolicy::kPerBatch;
            recorder_topic.segment_options.max_segment_bytes =
                config_.max_segment_bytes;
            recorder_topic.schemas.push_back(storage::RecorderTopicSchema{
                .identity = configured->bus.schema,
                .descriptor_artifact = configured->descriptor_artifact,
            });
            MINO_RETURN_IF_ERROR(
                (*service)->recorder().AddTopic(recorder_topic));

            const storage::TopicTableEntry* updated_topic = nullptr;
            for (const storage::TopicTableEntry& candidate :
                 (*service)->recorder().manifest_snapshot().topics) {
                if (candidate.topic_id == manifest_topic.topic_id) {
                    updated_topic = &candidate;
                    break;
                }
            }
            if (updated_topic == nullptr) {
                return Status::Error(StatusCode::kCorruption,
                                     "recorder dropped a configured manifest topic");
            }
            const auto schema_ref = std::find_if(
                updated_topic->schema_snapshot.begin(),
                updated_topic->schema_snapshot.end(),
                [&configured](const storage::SchemaRefSnapshot& snapshot) {
                    return snapshot.canonical_digest ==
                               configured->bus.schema.canonical_digest() &&
                           snapshot.schema_version ==
                               configured->bus.schema.schema_version() &&
                           snapshot.layout_version ==
                               configured->bus.schema.layout_version();
                });
            if (schema_ref == updated_topic->schema_snapshot.end()) {
                return Status::Error(StatusCode::kSchemaMismatch,
                                     "runtime schema is absent from recorder manifest");
            }
            Result<BusSubscriber> subscriber = deployment_->bus().CreateSubscriber(
                configured->bus.name, configured->bus.schema);
            if (!subscriber.ok()) return subscriber.status();
            Result<std::unique_ptr<storage::FileRecorderPendingStore>> pending =
                storage::FileRecorderPendingStore::Open(
                    session_root / "pending" /
                    (std::to_string(manifest_topic.topic_id) + ".pending"));
            if (!pending.ok()) return pending.status();
            Result<std::unique_ptr<BusRecorderSource>> source =
                BusRecorderSource::Create(
                    std::move(*subscriber), (*service)->recorder(),
                    TopicId{manifest_topic.topic_id},
                    RecorderSchema(configured->bus.schema),
                    schema_ref->schema_ref, configured->bus.max_payload_bytes,
                    std::move(*pending));
            if (!source.ok()) return source.status();
            MINO_RETURN_IF_ERROR((*service)->AddSource(std::move(*source)));
        }
        const Status started = (*service)->Start();
        if (!started.ok()) {
            static_cast<void>((*service)->Stop());
            return started;
        }
        running_.store(true, std::memory_order_release);
        const uint64_t start_ns = MonotonicNowNs();
        Status result = Status::Ok();
        for (;;) {
            const storage::RecorderServiceStatus current = (*service)->status();
            if (current.state == storage::RecorderServiceState::kError) {
                result = current.last_error.ok()
                             ? Status::Error(StatusCode::kUnavailable,
                                             "RecorderService entered error state")
                             : current.last_error;
                break;
            }
            if (config_.record_stop_after_records != 0 &&
                current.metrics.source_buffered_records >=
                    config_.record_stop_after_records) {
                break;
            }
            if (stop_requested_.load(std::memory_order_acquire) ||
                (signal_stop_ != nullptr && *signal_stop_ != 0)) {
                break;
            }
            if (config_.record_max_runtime_ms != 0 &&
                MonotonicNowNs() - start_ns >=
                    config_.record_max_runtime_ms * 1'000'000ull) {
                result = Status::Error(StatusCode::kTimeout,
                                       "recording runtime limit elapsed");
                break;
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(config_.record_poll_interval_ms));
        }
        running_.store(false, std::memory_order_release);
        const Status stopped = (*service)->Stop();
        if (result.ok() && !stopped.ok()) result = stopped;
        return result;
    } catch (const std::bad_alloc&) {
        running_.store(false, std::memory_order_release);
        return Exhausted("cannot allocate recorder runtime");
    } catch (const std::exception& error) {
        running_.store(false, std::memory_order_release);
        return Status::Error(StatusCode::kInternal, error.what());
    } catch (...) {
        running_.store(false, std::memory_order_release);
        return Status::Error(StatusCode::kInternal,
                             "recorder runtime failed unexpectedly");
    }
}

void BusRecorderServiceLauncher::RequestStop() noexcept {
    stop_requested_.store(true, std::memory_order_release);
}

bool BusRecorderServiceLauncher::running() const noexcept {
    return running_.load(std::memory_order_acquire);
}

BusReplayPublisherAdapter::BusReplayPublisherAdapter(
    deployment::LocalBusDeployment& deployment) noexcept
    : deployment_(&deployment) {}

Status BusReplayPublisherAdapter::Publish(
    const storage::ReplayPublishRequest& request) noexcept {
    try {
        if (!SafeNamespace(request.target_namespace) || request.topic_name.empty() ||
            request.topic_name.find('\0') != std::string_view::npos ||
            request.canonical_payload.empty() ||
            request.canonical_payload.size() > kMaxBusCanonicalPayloadBytes) {
            return Invalid("replay publish request violates runtime bounds");
        }
        std::string destination(request.target_namespace);
        destination.push_back('/');
        destination.append(request.topic_name);
        if (destination.size() > registry::kMaxTopicNameBytes) {
            return Invalid("replay destination topic name is too long");
        }
        const schema::SchemaIdentity identity(
            SchemaShortId(request.schema.canonical_digest),
            request.schema.canonical_digest, request.schema.schema_version,
            request.schema.layout_version);
        const std::string key = ReplayPublisherKey(destination, request.schema);
        std::lock_guard lock(mutex_);
        auto found = publishers_.find(key);
        if (found == publishers_.end()) {
            Result<BusPublisher> publisher =
                deployment_->bus().CreatePublisher(destination, identity);
            if (!publisher.ok()) return publisher.status();
            auto owned = std::make_unique<BusPublisher>(std::move(*publisher));
            found = publishers_.emplace(key, std::move(owned)).first;
        }
        Result<BusPublishResult> published =
            found->second->Publish(request.canonical_payload);
        if (!published.ok()) return published.status();
        if (!published->bridge_status.ok()) return published->bridge_status;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate replay publisher state");
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal, error.what());
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "replay publisher failed unexpectedly");
    }
}

Result<std::unique_ptr<RuntimeCommandServices>> RuntimeCommandServices::Create(
    const std::filesystem::path& config_path,
    volatile std::sig_atomic_t* signal_stop) noexcept {
    try {
        Result<RuntimeConfig> config = LoadRuntimeConfig(config_path);
        if (!config.ok()) return config.status();
        Result<std::unique_ptr<deployment::LocalBusDeployment>> deployment =
            deployment::LocalBusDeployment::Create(config->bus);
        if (!deployment.ok()) return deployment.status();
        auto recorder = std::make_unique<BusRecorderServiceLauncher>(
            **deployment, *config, signal_stop);
        auto replay =
            std::make_unique<BusReplayPublisherAdapter>(**deployment);
        return std::unique_ptr<RuntimeCommandServices>(new RuntimeCommandServices(
            std::move(*config), std::move(*deployment), std::move(recorder),
            std::move(replay)));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate command runtime services");
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal, error.what());
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "command runtime assembly failed unexpectedly");
    }
}

RuntimeCommandServices::RuntimeCommandServices(
    RuntimeConfig config,
    std::unique_ptr<deployment::LocalBusDeployment> deployment,
    std::unique_ptr<BusRecorderServiceLauncher> recorder,
    std::unique_ptr<BusReplayPublisherAdapter> replay) noexcept
    : config_(std::move(config)),
      deployment_(std::move(deployment)),
      recorder_(std::move(recorder)),
      replay_(std::move(replay)) {}

RuntimeCommandServices::~RuntimeCommandServices() = default;

StorageCommandServices RuntimeCommandServices::services() noexcept {
    return StorageCommandServices{
        .replay_adapter = replay_.get(),
        .recorder_service_launcher = recorder_.get(),
    };
}

Bus& RuntimeCommandServices::bus() noexcept { return deployment_->bus(); }

BusRecorderServiceLauncher& RuntimeCommandServices::recorder_launcher() noexcept {
    return *recorder_;
}

}  // namespace mino::tools
