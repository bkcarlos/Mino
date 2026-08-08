// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/validations/topic_writer_scaling.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <memory>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "benchmarks/validation/common/payload.h"
#include "benchmarks/validation/common/runtime.h"
#include "mino/storage/recorder_buffer_pool.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/topic_writer.h"

namespace mino::benchmarks::validation {
namespace {

constexpr uint64_t kWriterBatchRecords = 128;

uint32_t Crc32c(std::span<const std::byte> bytes) {
    uint32_t state = 0xffffffffu;
    for (std::byte byte : bytes) {
        state ^= static_cast<uint8_t>(byte);
        for (int bit = 0; bit < 8; ++bit) {
            state = (state >> 1) ^ ((state & 1u) != 0 ? 0x82f63b78u : 0u);
        }
    }
    return state ^ 0xffffffffu;
}

storage::RecorderSchemaMetadata WriterSchema() {
    storage::RecorderSchemaMetadata schema;
    schema.short_id = 1;
    for (size_t index = 0; index < schema.canonical_digest.size(); ++index) {
        schema.canonical_digest[index] = static_cast<std::byte>(index + 1);
    }
    schema.schema_version = 0x00010000u;
    schema.layout_version = 1;
    return schema;
}

Result<storage::SchemaRef> ResolveWriterSchema(
    const storage::RecorderSchemaMetadata&, void*) noexcept {
    return storage::SchemaRef{1};
}

struct TopicWriterInstance {
    TopicId topic_id;
    std::unique_ptr<storage::RecorderBufferPool> pool;
    std::unique_ptr<storage::PartitionManifest> manifest;
    std::unique_ptr<storage::TopicWriter> writer;
};

TopicWriterInstance CreateTopicWriterInstance(const std::filesystem::path& root,
                                               uint32_t topic_value,
                                               uint64_t records_per_writer,
                                               size_t payload_bytes) {
    std::error_code error;
    std::filesystem::create_directories(root, error);
    if (error) throw std::runtime_error(error.message());
    storage::RecorderBufferPoolOptions pool_options;
    pool_options.queue_capacity = static_cast<size_t>(records_per_writer + 1);
    size_t charged_bytes = 0;
    if (payload_bytes <= storage::kRecorderSmallBufferClassBytes) {
        charged_bytes = storage::kRecorderSmallBufferClassBytes;
    } else if (payload_bytes <= storage::kRecorderMediumBufferClassBytes) {
        charged_bytes = storage::kRecorderMediumBufferClassBytes;
    } else if (payload_bytes <= storage::kRecorderLargeBufferClassBytes) {
        charged_bytes = storage::kRecorderLargeBufferClassBytes;
    } else {
        charged_bytes = (payload_bytes + 4095u) & ~size_t{4095u};
    }
    if (charged_bytes > std::numeric_limits<size_t>::max() /
                            pool_options.queue_capacity) {
        throw std::runtime_error("TopicWriter pool byte budget overflows");
    }
    pool_options.global_byte_limit = std::max<size_t>(
        64u * 1024u, pool_options.queue_capacity * charged_bytes);
    pool_options.default_topic_byte_limit = pool_options.global_byte_limit;
    auto pool = Take(storage::RecorderBufferPool::Create(pool_options),
                     "RecorderBufferPool::Create");
    storage::PartitionMetadata partition{
        .recording_id = 0x563136,
        .topic_id = topic_value,
        .partition_id = 0,
        .writer_id = static_cast<uint64_t>(topic_value) + 1000,
        .owner_epoch = 1,
        .config_version = 1,
    };
    auto manifest = Take(storage::PartitionManifest::Create(root, partition),
                         "PartitionManifest::Create");
    storage::TopicWriterOptions writer_options;
    writer_options.partition_root = root;
    writer_options.recording_id = partition.recording_id;
    writer_options.topic_id = TopicId{topic_value};
    writer_options.partition_id = partition.partition_id;
    writer_options.writer_id = partition.writer_id;
    writer_options.segment_options.batch_bytes = 0;
    writer_options.segment_options.batch_records = kWriterBatchRecords;
    writer_options.segment_options.flush_interval_ns = 0;
    writer_options.segment_options.sync_policy = storage::SegmentSyncPolicy::kNone;
    writer_options.schema_resolver = ResolveWriterSchema;
    auto writer = Take(storage::TopicWriter::Create(
                           std::move(writer_options), pool.get(), manifest.get()),
                       "TopicWriter::Create");
    return TopicWriterInstance{TopicId{topic_value}, std::move(pool),
                               std::move(manifest), std::move(writer)};
}

void EnqueueWriterRecord(TopicWriterInstance& instance,
                         std::span<const std::byte> payload,
                         uint64_t sequence) {
    storage::RecorderRecordMetadata metadata{
        .schema = WriterSchema(),
        .topic_id = instance.topic_id,
        .source = storage::MessageSource{
            .node_id = 1,
            .publisher_id = instance.topic_id.value,
            .publisher_epoch = 1,
            .source_sequence = sequence,
            .observed_timestamp_ns = 1000 + sequence,
        },
        .ingestion_timestamp_ns = 2000 + sequence,
        .payload_size = static_cast<uint32_t>(payload.size()),
        .payload_crc = Crc32c(payload),
    };
    storage::BufferReservationRequest request;
    request.topic_id = instance.topic_id;
    request.payload_size = payload.size();
    request.user_tag = sequence;
    request.metadata = metadata;
    auto reserved = Take(instance.pool->Reserve(request),
                         "RecorderBufferPool::Reserve");
    if (!reserved.accepted()) throw std::runtime_error("writer enqueue dropped");
    std::copy(payload.begin(), payload.end(), reserved.reservation.bytes().begin());
    Require(std::move(reserved.reservation).Commit(),
            "RecorderBufferReservation::Commit");
}

}  // namespace

std::string RunTopicWriterScaling(const std::filesystem::path& root,
                   uint64_t records_per_writer, size_t payload_bytes) {
    constexpr std::array<size_t, 4> kWriterCounts = {1, 10, 50, 100};
    const std::vector<std::byte> payload = MakePayload(payload_bytes);
    std::ostringstream output;
    output << "{\"status\":\"MEASURED\",\"model\":\"one TopicWriter per Topic/Partition\",\"sync_policy\":\"none (Seal remains durable)\",\"scenarios\":[";
    bool first = true;
    for (size_t writer_count : kWriterCounts) {
        const auto setup_begin = Clock::now();
        std::vector<TopicWriterInstance> writers;
        writers.reserve(writer_count);
        const auto scenario_root =
            root / ("topic_writer_scaling-writers-" + std::to_string(writer_count));
        for (size_t index = 0; index < writer_count; ++index) {
            writers.push_back(CreateTopicWriterInstance(
                scenario_root / ("topic-" + std::to_string(index + 1)),
                static_cast<uint32_t>(index + 1), records_per_writer,
                payload_bytes));
            Require(writers.back().writer->Start(1), "TopicWriter::Start");
        }
        const uint64_t setup_ns = DurationNs(setup_begin, Clock::now());
        const auto work_begin = Clock::now();
        uint64_t errors = 0;
        for (auto& writer : writers) {
            for (uint64_t sequence = 1; sequence <= records_per_writer;
                 ++sequence) {
                EnqueueWriterRecord(writer, payload, sequence);
            }
            auto pumped = writer.writer->Pump(2);
            if (!pumped.ok() || pumped->dequeued_records != records_per_writer) {
                ++errors;
            }
        }
        const uint64_t work_ns = DurationNs(work_begin, Clock::now());
        const auto stop_begin = Clock::now();
        for (auto& writer : writers) {
            const Status status = writer.writer->Stop(3);
            if (!status.ok()) ++errors;
        }
        const uint64_t stop_ns = DurationNs(stop_begin, Clock::now());
        if (errors != 0) MarkFailed();
        const uint64_t total_records = writer_count * records_per_writer;
        const double records_per_second = work_ns == 0
            ? 0.0
            : static_cast<double>(total_records) * 1'000'000'000.0 /
                  static_cast<double>(work_ns);
        if (!first) output << ',';
        first = false;
        output << "{\"topic_writers\":" << writer_count
               << ",\"records_per_writer\":" << records_per_writer
               << ",\"total_records\":" << total_records
               << ",\"payload_bytes\":" << payload_bytes
               << ",\"setup_start_elapsed_ns\":" << setup_ns
               << ",\"enqueue_pump_elapsed_ns\":" << work_ns
               << ",\"stop_seal_elapsed_ns\":" << stop_ns
               << ",\"enqueue_pump_records_per_second\":"
               << std::setprecision(17) << records_per_second
               << ",\"errors\":" << errors << '}';
    }
    output << "]}";
    return output.str();
}

}  // namespace mino::benchmarks::validation
