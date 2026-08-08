// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/validations/sync_policy.h"

#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "benchmarks/validation/common/payload.h"
#include "benchmarks/validation/common/runtime.h"
#include "benchmarks/validation/common/stats.h"
#include "mino/storage/segment_format.h"
#include "mino/storage/segment_writer.h"

namespace mino::benchmarks::validation {
namespace {

constexpr uint64_t kWriterBatchRecords = 128;

storage::Record MakeStorageRecord(size_t payload_bytes) {
    storage::Record record;
    record.header.schema_ref = 1;
    record.header.schema_version = 0x00010000u;
    record.header.layout_version = 1;
    record.header.topic_id = 17;
    record.header.partition_id = 0;
    record.header.node_id = 1;
    record.header.publisher_id = 1;
    record.header.publisher_epoch = 1;
    record.payload = MakePayload(payload_bytes);
    return record;
}

void SetStorageSequence(storage::Record* record, uint64_t sequence) {
    record->header.ingestion_sequence = sequence;
    record->header.ingestion_timestamp_ns = 1000 + sequence;
    record->header.source_sequence = sequence;
    record->header.observed_timestamp_ns = 2000 + sequence;
}

struct SyncSamples {
    std::vector<uint64_t> latency_ns;
    size_t sample_count = 0;
    uint64_t errors = 0;
};

int TimedDataSync(int fd, void* context) noexcept {
    auto* samples = static_cast<SyncSamples*>(context);
    const auto begin = Clock::now();
#if defined(__APPLE__)
    const int result = ::fsync(fd);
#else
    const int result = ::fdatasync(fd);
#endif
    if (samples->sample_count < samples->latency_ns.size()) {
        samples->latency_ns[samples->sample_count++] =
            DurationNs(begin, Clock::now());
    } else {
        ++samples->errors;
    }
    if (result != 0) ++samples->errors;
    return result;
}

std::string SyncPolicyName(storage::SegmentSyncPolicy policy) {
    switch (policy) {
        case storage::SegmentSyncPolicy::kNone: return "none";
        case storage::SegmentSyncPolicy::kInterval: return "interval";
        case storage::SegmentSyncPolicy::kPerBatch: return "per-batch";
        case storage::SegmentSyncPolicy::kPerRecord: return "per-record";
    }
    return "unknown";
}

}  // namespace

std::string RunSyncPolicy(const std::filesystem::path& root, uint64_t records,
                   size_t payload_bytes) {
    constexpr std::array<storage::SegmentSyncPolicy, 4> kPolicies = {
        storage::SegmentSyncPolicy::kNone,
        storage::SegmentSyncPolicy::kInterval,
        storage::SegmentSyncPolicy::kPerBatch,
        storage::SegmentSyncPolicy::kPerRecord,
    };
    storage::Record record = MakeStorageRecord(payload_bytes);
    SetStorageSequence(&record, 1);
    const size_t encoded_record_bytes =
        Take(storage::EncodeRecord(record), "EncodeRecord").size();
    std::ostringstream output;
    output << "{\"status\":\"MEASURED\",\"records_per_policy\":" << records
           << ",\"payload_bytes\":" << payload_bytes
           << ",\"durability_observation\":\"SegmentWriter durable_records counter before and after Seal\",\"scenarios\":[";
    bool first = true;
    for (storage::SegmentSyncPolicy policy : kPolicies) {
        SyncSamples sync_samples;
        sync_samples.latency_ns.resize(static_cast<size_t>(records) + 1);
        storage::SegmentWriterOptions options;
        options.batch_bytes = 0;
        options.batch_records = kWriterBatchRecords;
        options.flush_interval_ns = 0;
        options.sync_policy = policy;
        options.sync_interval_ns = 0;
        options.sync_interval_bytes = encoded_record_bytes * kWriterBatchRecords;
        options.data_sync_hook = TimedDataSync;
        options.io_hook_context = &sync_samples;
        storage::SegmentHeader header;
        header.recording_id = 0x563137;
        header.topic_id = 17;
        header.partition_id = 0;
        header.writer_id = 1;
        header.first_ingestion_sequence = 1;
        header.created_at_ns = 1;
        auto writer = Take(storage::SegmentWriter::Create(
                               root / ("sync_policy-" + SyncPolicyName(policy) + ".segment"),
                               header, 1, options),
                           "SegmentWriter::Create");
        std::vector<uint64_t> append_samples;
        append_samples.reserve(records);
        uint64_t errors = 0;
        const auto total_begin = Clock::now();
        for (uint64_t sequence = 1; sequence <= records; ++sequence) {
            SetStorageSequence(&record, sequence);
            const auto begin = Clock::now();
            auto appended = writer->Append(record, sequence + 1);
            append_samples.push_back(DurationNs(begin, Clock::now()));
            if (!appended.ok()) {
                ++errors;
                break;
            }
            if (sequence % kWriterBatchRecords == 0) {
                const Status flushed = writer->Flush(sequence + 1);
                if (!flushed.ok()) {
                    ++errors;
                    break;
                }
            }
        }
        if (records % kWriterBatchRecords != 0) {
            const Status flushed = writer->Flush(records + 2);
            if (!flushed.ok()) ++errors;
        }
        const uint64_t durable_before_seal = writer->durable_records();
        const Status sealed = writer->Seal(records + 3);
        if (!sealed.ok()) ++errors;
        const uint64_t elapsed_ns = DurationNs(total_begin, Clock::now());
        const uint64_t durable_after_seal = writer->durable_records();
        sync_samples.latency_ns.resize(sync_samples.sample_count);
        if (errors != 0 || sync_samples.errors != 0) MarkFailed();
        if (!first) output << ',';
        first = false;
        output << "{\"sync_policy\":\"" << SyncPolicyName(policy)
               << "\",\"elapsed_ns\":" << elapsed_ns
               << ",\"records_appended\":" << writer->record_count()
               << ",\"durable_records_before_seal\":" << durable_before_seal
               << ",\"durable_records_after_seal\":" << durable_after_seal
               << ",\"sync_calls\":" << sync_samples.sample_count
               << ",\"sync_errors\":" << sync_samples.errors
               << ",\"errors\":" << errors << ",\"append_latency_ns\":";
        WriteDistribution(output, Summarize(std::move(append_samples)));
        output << ",\"fdatasync_or_fsync_latency_ns\":";
        WriteDistribution(output, Summarize(std::move(sync_samples.latency_ns)));
        output << '}';
    }
    output << "]}";
    return output.str();
}

}  // namespace mino::benchmarks::validation
