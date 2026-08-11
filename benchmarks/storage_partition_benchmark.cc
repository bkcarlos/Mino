// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "mino/storage/recorder_buffer_pool.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/topic_writer.h"

namespace mino::benchmarks {
namespace {

using Clock = std::chrono::steady_clock;
constexpr std::string_view kQualificationTarget =
    "i9-9900KS; Samsung 980 PRO NVMe; ext4; Linux x86-64";

struct Config {
    uint64_t records = 20000;
    size_t payload_bytes = 1024;
    double target_ingress_rps = 0.0;
    std::filesystem::path directory = std::filesystem::temp_directory_path();
    std::optional<std::filesystem::path> output_json;
    std::string qualification_hardware;
};

uint64_t ParseUnsigned(std::string_view value, std::string_view name) {
    if (value.empty()) throw std::runtime_error(std::string(name) + " is empty");
    uint64_t result = 0;
    for (char character : value) {
        if (character < '0' || character > '9' ||
            result > (std::numeric_limits<uint64_t>::max() -
                      static_cast<uint64_t>(character - '0')) /
                         10u) {
            throw std::runtime_error(std::string(name) + " is invalid");
        }
        result = result * 10u + static_cast<uint64_t>(character - '0');
    }
    return result;
}

std::string_view Value(int argc, char** argv, int* index, std::string_view flag) {
    const std::string_view argument(argv[*index]);
    const std::string prefix = std::string(flag) + "=";
    if (argument.starts_with(prefix)) return argument.substr(prefix.size());
    if (argument != flag || *index + 1 >= argc) {
        throw std::runtime_error("invalid benchmark option: " +
                                 std::string(argument));
    }
    return argv[++*index];
}

Config Parse(int argc, char** argv) {
    Config config;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument.starts_with("--records")) {
            config.records = ParseUnsigned(
                Value(argc, argv, &index, "--records"), "records");
        } else if (argument.starts_with("--payload-bytes")) {
            config.payload_bytes = static_cast<size_t>(ParseUnsigned(
                Value(argc, argv, &index, "--payload-bytes"), "payload bytes"));
        } else if (argument.starts_with("--target-ingress-rps")) {
            config.target_ingress_rps = std::stod(std::string(
                Value(argc, argv, &index, "--target-ingress-rps")));
        } else if (argument.starts_with("--directory")) {
            config.directory = Value(argc, argv, &index, "--directory");
        } else if (argument.starts_with("--output-json")) {
            config.output_json = Value(argc, argv, &index, "--output-json");
        } else if (argument.starts_with("--qualification-hardware")) {
            config.qualification_hardware = Value(
                argc, argv, &index, "--qualification-hardware");
        } else {
            throw std::runtime_error("unknown benchmark option: " +
                                     std::string(argument));
        }
    }
    if (config.records < 16 || config.payload_bytes == 0 ||
        config.payload_bytes > 16u * 1024u * 1024u ||
        !std::isfinite(config.target_ingress_rps) ||
        config.target_ingress_rps < 0.0) {
        throw std::runtime_error("benchmark values are outside storage bounds");
    }
    return config;
}

uint32_t Crc32c(std::span<const std::byte> bytes) noexcept {
    uint32_t state = 0xffffffffu;
    for (std::byte byte : bytes) {
        state ^= static_cast<uint8_t>(byte);
        for (int bit = 0; bit < 8; ++bit) {
            state = (state >> 1u) ^
                    ((state & 1u) != 0 ? 0x82f63b78u : 0u);
        }
    }
    return state ^ 0xffffffffu;
}

size_t Charge(size_t bytes) {
    if (bytes <= storage::kRecorderSmallBufferClassBytes) {
        return storage::kRecorderSmallBufferClassBytes;
    }
    if (bytes <= storage::kRecorderMediumBufferClassBytes) {
        return storage::kRecorderMediumBufferClassBytes;
    }
    if (bytes <= storage::kRecorderLargeBufferClassBytes) {
        return storage::kRecorderLargeBufferClassBytes;
    }
    return (bytes + 4095u) & ~size_t{4095u};
}

storage::RecorderSchemaMetadata Schema() {
    storage::RecorderSchemaMetadata schema;
    schema.short_id = 1;
    schema.canonical_digest[0] = std::byte{1};
    schema.schema_version = 1;
    schema.layout_version = 1;
    return schema;
}

Result<storage::SchemaRef> ResolveSchema(
    const storage::RecorderSchemaMetadata&, void*) noexcept {
    return storage::SchemaRef{1};
}

template <typename T>
T Take(Result<T> result, std::string_view operation) {
    if (!result.ok()) {
        throw std::runtime_error(std::string(operation) + ": " +
                                 result.status().ToString());
    }
    return std::move(*result);
}

void Require(Status status, std::string_view operation) {
    if (!status.ok()) {
        throw std::runtime_error(std::string(operation) + ": " +
                                 status.ToString());
    }
}

struct Writer {
    TopicId topic{1};
    std::unique_ptr<storage::RecorderBufferPool> pool;
    std::unique_ptr<storage::PartitionManifest> manifest;
    std::unique_ptr<storage::TopicWriter> writer;
};

Writer CreateWriter(const std::filesystem::path& root, uint32_t partition_id,
                    uint32_t partition_count, uint64_t records,
                    size_t payload_bytes) {
    std::filesystem::create_directories(root);
    storage::RecorderBufferPoolOptions pool_options;
    pool_options.queue_capacity = static_cast<size_t>(records + 1);
    const size_t charge = Charge(payload_bytes);
    if (pool_options.queue_capacity >
        std::numeric_limits<size_t>::max() / charge) {
        throw std::runtime_error("partition benchmark pool budget overflows");
    }
    pool_options.global_byte_limit = pool_options.queue_capacity * charge;
    pool_options.default_topic_byte_limit = pool_options.global_byte_limit;
    auto pool = Take(storage::RecorderBufferPool::Create(pool_options),
                     "create partition pool");
    storage::PartitionMetadata metadata{
        .recording_id = 0x563234,
        .topic_id = 1,
        .partition_id = partition_id,
        .writer_id = 1000u + partition_id,
        .owner_epoch = 1,
        .partition_map_version = 1,
        .partition_generation = 1,
        .partition_count = partition_count,
        .partition_strategy = storage::TopicPartitionStrategy::kHash,
        .partition_map_state = storage::TopicPartitionMapState::kActive,
        .hash_algorithm_version = storage::kStablePartitionHashVersion,
        .hash_seed = storage::kDefaultPartitionHashSeed,
        .config_version = 1,
    };
    auto manifest = Take(storage::PartitionManifest::Create(root, metadata),
                         "create partition manifest");
    storage::TopicWriterOptions options;
    options.partition_root = root;
    options.recording_id = metadata.recording_id;
    options.topic_id = TopicId{1};
    options.partition_id = partition_id;
    options.writer_id = metadata.writer_id;
    options.segment_options.sync_policy = storage::SegmentSyncPolicy::kNone;
    options.segment_options.batch_records = 128;
    options.schema_resolver = ResolveSchema;
    auto writer = Take(storage::TopicWriter::Create(
                           options, pool.get(), manifest.get()),
                       "create partition writer");
    Require(writer->Start(1), "start partition writer");
    return Writer{TopicId{1}, std::move(pool), std::move(manifest),
                  std::move(writer)};
}

struct PartitionResult {
    uint32_t partition_id = 0;
    uint64_t attempted_records = 0;
    uint64_t accepted_records = 0;
    uint64_t dequeued_records = 0;
    uint64_t written_records = 0;
    uint64_t errors = 0;
    uint64_t elapsed_ns = 0;
    double records_per_second = 0;
};

struct Scenario {
    uint32_t partitions = 0;
    uint64_t records = 0;
    uint64_t attempted_records = 0;
    uint64_t accepted_records = 0;
    uint64_t dequeued_records = 0;
    uint64_t written_records = 0;
    uint64_t errors = 0;
    uint64_t elapsed_ns = 0;
    double records_per_second = 0;
    double scaling = 0;
    double efficiency = 0;
    double imbalance = 0;
    uint64_t latency_p50_ns = 0;
    uint64_t latency_p99_ns = 0;
    std::vector<PartitionResult> partition_results;
};

uint64_t Percentile(std::vector<uint64_t> values, double percentile) {
    if (values.empty()) return 0;
    std::sort(values.begin(), values.end());
    const size_t index = std::min(
        values.size() - 1,
        static_cast<size_t>(percentile * static_cast<double>(values.size() - 1)));
    return values[index];
}

Scenario RunScenario(const std::filesystem::path& root, uint32_t partitions,
                     uint64_t total_records,
                     std::span<const std::byte> payload) {
    std::vector<uint64_t> partition_records(partitions,
                                             total_records / partitions);
    for (uint32_t id = 0; id < total_records % partitions; ++id) {
        ++partition_records[id];
    }
    std::vector<Writer> writers;
    writers.reserve(partitions);
    for (uint32_t id = 0; id < partitions; ++id) {
        writers.push_back(CreateWriter(
            root / std::to_string(id), id, partitions,
            partition_records[id], payload.size()));
    }

    std::vector<uint64_t> elapsed(partitions);
    std::vector<uint64_t> dequeued(partitions);
    std::vector<uint64_t> written(partitions);
    std::vector<uint64_t> errors(partitions);
    std::vector<std::vector<uint64_t>> latencies(partitions);
    std::vector<std::thread> workers;
    const uint32_t crc = Crc32c(payload);
    const auto scenario_begin = Clock::now();
    for (uint32_t id = 0; id < partitions; ++id) {
        workers.emplace_back([&, id] {
            latencies[id].reserve(
                static_cast<size_t>(partition_records[id]));
            const auto begin = Clock::now();
            for (uint64_t sequence = 1;
                 sequence <= partition_records[id]; ++sequence) {
                const auto operation_begin = Clock::now();
                storage::RecorderRecordMetadata metadata{
                    .schema = Schema(),
                    .topic_id = TopicId{1},
                    .source = storage::MessageSource{
                        .node_id = id + 1u,
                        .publisher_id = id + 1u,
                        .publisher_epoch = 1,
                        .source_sequence = sequence,
                        .observed_timestamp_ns = sequence,
                    },
                    .ingestion_timestamp_ns = sequence,
                    .payload_size = static_cast<uint32_t>(payload.size()),
                    .payload_crc = crc,
                };
                storage::BufferReservationRequest request;
                request.topic_id = TopicId{1};
                request.payload_size = payload.size();
                request.metadata = metadata;
                auto reserved = Take(writers[id].pool->Reserve(request),
                                     "reserve benchmark record");
                if (!reserved.accepted()) {
                    throw std::runtime_error("partition benchmark dropped a record");
                }
                std::copy(payload.begin(), payload.end(),
                          reserved.reservation.bytes().begin());
                Require(std::move(reserved.reservation).Commit(),
                        "commit benchmark record");
                auto pumped = Take(writers[id].writer->Pump(sequence + 1, 1),
                                   "pump benchmark record");
                dequeued[id] += pumped.dequeued_records;
                written[id] += pumped.data_records;
                errors[id] += pumped.tombstone_records + pumped.gap_records +
                              pumped.duplicate_records;
                if (pumped.dequeued_records != 1 || pumped.data_records != 1 ||
                    pumped.tombstone_records != 0 || pumped.gap_records != 0 ||
                    pumped.duplicate_records != 0) {
                    throw std::runtime_error(
                        "partition writer record conservation failed");
                }
                latencies[id].push_back(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        Clock::now() - operation_begin)
                        .count()));
            }
            elapsed[id] = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    Clock::now() - begin)
                    .count());
        });
    }
    for (std::thread& worker : workers) worker.join();
    const uint64_t scenario_elapsed = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now() - scenario_begin)
            .count());
    for (Writer& writer : writers) Require(writer.writer->Stop(total_records + 2),
                                           "stop partition writer");

    std::vector<uint64_t> all_latencies;
    std::vector<double> partition_rates;
    std::vector<PartitionResult> partition_results;
    uint64_t accepted_records = 0;
    uint64_t dequeued_records = 0;
    uint64_t written_records = 0;
    uint64_t total_errors = 0;
    for (uint32_t id = 0; id < partitions; ++id) {
        all_latencies.insert(all_latencies.end(), latencies[id].begin(),
                             latencies[id].end());
        const double rate =
            elapsed[id] == 0
                ? 0.0
                : static_cast<double>(partition_records[id]) * 1e9 /
                      static_cast<double>(elapsed[id]);
        partition_rates.push_back(rate);
        const storage::RecorderBufferPoolStats stats = writers[id].pool->stats();
        const uint64_t partition_errors =
            errors[id] + stats.dropped_newest_records +
            stats.dropped_oldest_records + stats.cancelled_reservations +
            stats.block_timeouts + stats.recording_failures;
        accepted_records += stats.accepted_records;
        dequeued_records += dequeued[id];
        written_records += written[id];
        total_errors += partition_errors;
        partition_results.push_back(PartitionResult{
            .partition_id = id,
            .attempted_records = partition_records[id],
            .accepted_records = stats.accepted_records,
            .dequeued_records = dequeued[id],
            .written_records = written[id],
            .errors = partition_errors,
            .elapsed_ns = elapsed[id],
            .records_per_second = rate,
        });
    }
    const auto [minimum, maximum] = std::minmax_element(
        partition_rates.begin(), partition_rates.end());
    return Scenario{
        .partitions = partitions,
        .records = total_records,
        .attempted_records = total_records,
        .accepted_records = accepted_records,
        .dequeued_records = dequeued_records,
        .written_records = written_records,
        .errors = total_errors,
        .elapsed_ns = scenario_elapsed,
        .records_per_second =
            scenario_elapsed == 0
                ? 0.0
                : static_cast<double>(total_records) * 1e9 /
                      static_cast<double>(scenario_elapsed),
        .scaling = 0,
        .efficiency = 0,
        .imbalance = *minimum > 0.0 ? *maximum / *minimum : 0.0,
        .latency_p50_ns = Percentile(all_latencies, 0.50),
        .latency_p99_ns = Percentile(all_latencies, 0.99),
        .partition_results = std::move(partition_results),
    };
}

std::string Escape(std::string_view value) {
    std::string result;
    for (char character : value) {
        if (character == '"' || character == '\\') result.push_back('\\');
        result.push_back(character);
    }
    return result;
}

}  // namespace
}  // namespace mino::benchmarks

int main(int argc, char** argv) {
    using namespace mino::benchmarks;
    try {
        const Config config = Parse(argc, argv);
        const std::filesystem::path root =
            config.directory /
            ("mino-storage-partition-benchmark-" +
             std::to_string(static_cast<uint64_t>(::getpid())));
        if (std::filesystem::exists(root)) {
            throw std::runtime_error("benchmark directory already exists");
        }
        std::filesystem::create_directories(root);
        std::vector<std::byte> payload(config.payload_bytes);
        for (size_t index = 0; index < payload.size(); ++index) {
            payload[index] = static_cast<std::byte>((index * 131u + 17u) & 0xffu);
        }
        constexpr std::array<uint32_t, 5> counts = {1, 2, 4, 8, 16};
        std::vector<Scenario> scenarios;
        for (uint32_t count : counts) {
            scenarios.push_back(RunScenario(root / std::to_string(count), count,
                                            config.records, payload));
        }
        const double single_writer_threshold = scenarios.front().records_per_second;
        for (Scenario& scenario : scenarios) {
            scenario.scaling = single_writer_threshold > 0.0
                                   ? scenario.records_per_second /
                                         single_writer_threshold
                                   : 0.0;
            scenario.efficiency = scenario.scaling / scenario.partitions;
        }
        uint64_t total_errors = 0;
        for (const Scenario& scenario : scenarios) {
            total_errors += scenario.errors;
        }
        std::ostringstream json;
        json << std::setprecision(17)
             << "{\"schema\":\"mino.storage_partition_benchmark.v1\""
             << ",\"validation\":\"V-24\""
             << ",\"configuration\":{\"records\":" << config.records
             << ",\"payload_bytes\":" << config.payload_bytes
             << ",\"partition_counts\":[1,2,4,8,16]}"
             << ",\"errors\":" << total_errors
             << ",\"qualification\":{\"target\":\""
             << Escape(config.qualification_hardware)
             << "\",\"required_target\":\""
             << kQualificationTarget
             << "\",\"eligible\":"
             << (config.qualification_hardware == kQualificationTarget
                     ? "true"
                     : "false")
             << "}"
             << ",\"single_writer_threshold_records_per_second\":"
             << single_writer_threshold
             << ",\"target_ingress_records_per_second\":"
             << config.target_ingress_rps
             << ",\"partitioning_required_for_target\":"
             << (config.target_ingress_rps > single_writer_threshold ? "true"
                                                                    : "false")
             << ",\"scenarios\":[";
        for (size_t index = 0; index < scenarios.size(); ++index) {
            if (index != 0) json << ',';
            const Scenario& scenario = scenarios[index];
            json << "{\"partitions\":" << scenario.partitions
                 << ",\"records\":" << scenario.records
                 << ",\"attempted_records\":" << scenario.attempted_records
                 << ",\"accepted_records\":" << scenario.accepted_records
                 << ",\"dequeued_records\":" << scenario.dequeued_records
                 << ",\"written_records\":" << scenario.written_records
                 << ",\"errors\":" << scenario.errors
                 << ",\"stable_partition_map\":{\"map_version\":1"
                 << ",\"generation\":1"
                 << ",\"partition_count\":" << scenario.partitions
                 << ",\"strategy\":\"hash\""
                 << ",\"state\":\"active\""
                 << ",\"hash_algorithm_version\":"
                 << mino::storage::kStablePartitionHashVersion
                 << ",\"hash_seed\":"
                 << mino::storage::kDefaultPartitionHashSeed
                 << "}"
                 << ",\"elapsed_ns\":" << scenario.elapsed_ns
                 << ",\"records_per_second\":"
                 << scenario.records_per_second
                 << ",\"scaling\":" << scenario.scaling
                 << ",\"scaling_efficiency\":" << scenario.efficiency
                 << ",\"partition_throughput_imbalance_ratio\":"
                 << scenario.imbalance
                 << ",\"record_latency_p50_ns\":"
                 << scenario.latency_p50_ns
                 << ",\"record_latency_p99_ns\":"
                 << scenario.latency_p99_ns
                 << ",\"partition_results\":[";
            for (size_t partition_index = 0;
                 partition_index < scenario.partition_results.size();
                 ++partition_index) {
                if (partition_index != 0) json << ',';
                const PartitionResult& partition =
                    scenario.partition_results[partition_index];
                json << "{\"partition_id\":" << partition.partition_id
                     << ",\"attempted_records\":"
                     << partition.attempted_records
                     << ",\"accepted_records\":"
                     << partition.accepted_records
                     << ",\"dequeued_records\":"
                     << partition.dequeued_records
                     << ",\"written_records\":"
                     << partition.written_records
                     << ",\"errors\":" << partition.errors
                     << ",\"elapsed_ns\":" << partition.elapsed_ns
                     << ",\"records_per_second\":"
                     << partition.records_per_second << '}';
            }
            json << "]}";
        }
        json << "]}";
        std::cout << json.str() << '\n';
        if (config.output_json.has_value()) {
            std::ofstream output(*config.output_json,
                                 std::ios::binary | std::ios::trunc);
            if (!output) throw std::runtime_error("cannot open output JSON");
            output << json.str() << '\n';
            if (!output) throw std::runtime_error("cannot write output JSON");
        }
        std::error_code cleanup_error;
        std::filesystem::remove_all(root, cleanup_error);
        if (cleanup_error) {
            std::cerr << "warning: benchmark cleanup failed: "
                      << cleanup_error.message() << '\n';
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "storage partition benchmark: " << error.what() << '\n';
        return 1;
    }
}
