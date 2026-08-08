// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <sys/utsname.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "mino/registry/registry.h"
#include "mino/runtime/allocation_journal.h"
#include "mino/schema/codegen/testdata/golden.generated.h"
#include "mino/schema/compiler.h"
#include "mino/schema/dynamic_object.h"
#include "mino/schema/layout.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/channel/broadcast_channel.h"
#include "mino/storage/recorder_buffer_pool.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/segment_format.h"
#include "mino/storage/segment_writer.h"
#include "mino/storage/topic_writer.h"

namespace mino::benchmarks {
namespace {

using Clock = std::chrono::steady_clock;

constexpr uint64_t kDefaultIterations = 10'000;
constexpr uint64_t kDefaultStorageRecords = 1'000;
constexpr uint64_t kDefaultRecordsPerWriter = 100;
constexpr size_t kDefaultPayloadBytes = 64;
constexpr size_t kDefaultPinCount = 1'000;
constexpr uint64_t kBroadcastCapacity = 256;
constexpr uint64_t kWriterBatchRecords = 128;
constexpr uint64_t kPayloadSeed = 0x4d494e4f563134ULL;

std::atomic<uint64_t> g_sink{0};
std::atomic<bool> g_failed{false};

struct Config {
    uint64_t iterations = kDefaultIterations;
    uint64_t storage_records = kDefaultStorageRecords;
    uint64_t records_per_writer = kDefaultRecordsPerWriter;
    size_t payload_bytes = kDefaultPayloadBytes;
    size_t pin_count = kDefaultPinCount;
    std::string suite = "all";
    std::optional<std::filesystem::path> output_json;
    std::optional<std::filesystem::path> directory;
    std::string commit;
    std::string build_config;
    std::string cpu_model;
    std::string memory_description;
    std::string storage_device;
    std::string filesystem;
    std::string command;
};

struct Distribution {
    size_t samples = 0;
    uint64_t p50 = 0;
    uint64_t p95 = 0;
    uint64_t p99 = 0;
    uint64_t maximum = 0;
};

uint64_t DurationNs(Clock::time_point begin, Clock::time_point end) {
    const auto value =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
    return value <= 0 ? 0 : static_cast<uint64_t>(value);
}

Distribution Summarize(std::vector<uint64_t> samples) {
    Distribution result;
    result.samples = samples.size();
    if (samples.empty()) return result;
    std::sort(samples.begin(), samples.end());
    const auto nearest_rank = [&](double percentile) {
        size_t rank = static_cast<size_t>(
            std::ceil(percentile * static_cast<double>(samples.size())));
        rank = std::max<size_t>(1, std::min(rank, samples.size()));
        return samples[rank - 1];
    };
    result.p50 = nearest_rank(0.50);
    result.p95 = nearest_rank(0.95);
    result.p99 = nearest_rank(0.99);
    result.maximum = samples.back();
    return result;
}

std::string JsonEscape(std::string_view input) {
    std::ostringstream output;
    for (const unsigned char character : input) {
        switch (character) {
            case '"': output << "\\\""; break;
            case '\\': output << "\\\\"; break;
            case '\b': output << "\\b"; break;
            case '\f': output << "\\f"; break;
            case '\n': output << "\\n"; break;
            case '\r': output << "\\r"; break;
            case '\t': output << "\\t"; break;
            default:
                if (character < 0x20u) {
                    output << "\\u" << std::hex << std::setw(4)
                           << std::setfill('0')
                           << static_cast<unsigned int>(character) << std::dec
                           << std::setfill(' ');
                } else {
                    output << static_cast<char>(character);
                }
        }
    }
    return output.str();
}

void WriteDistribution(std::ostream& output, const Distribution& value) {
    output << "{\"samples\":" << value.samples << ",\"p50\":" << value.p50
           << ",\"p95\":" << value.p95 << ",\"p99\":" << value.p99
           << ",\"max\":" << value.maximum << "}";
}

void Require(const Status& status, std::string_view operation) {
    if (!status.ok()) {
        throw std::runtime_error(std::string(operation) + ": " +
                                 status.ToString());
    }
}

template <typename T>
T Take(Result<T>&& result, std::string_view operation) {
    if (!result.ok()) {
        throw std::runtime_error(std::string(operation) + ": " +
                                 result.status().ToString());
    }
    return std::move(*result);
}

uint64_t ParseUnsigned(std::string_view text, std::string_view option) {
    uint64_t value = 0;
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size()) {
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

std::string EnvironmentOr(std::string_view name, std::string fallback) {
    const char* value = std::getenv(std::string(name).c_str());
    return value == nullptr || *value == '\0' ? std::move(fallback)
                                               : std::string(value);
}

std::string ShellQuote(std::string_view value) {
    if (value.find_first_of(" \t\n'\"\\$`") == std::string_view::npos) {
        return std::string(value);
    }
    std::string quoted = "'";
    for (char character : value) {
        if (character == '\'') quoted += "'\\''";
        else quoted += character;
    }
    quoted += '\'';
    return quoted;
}

void PrintUsage(std::ostream& output, std::string_view program) {
    output
        << "Usage: " << program << " [options]\n"
        << "  --suite all|memory|storage\n"
        << "  --iterations N              V-14/V-15 measured operations\n"
        << "  --storage-records N         V-17 records per sync policy\n"
        << "  --records-per-writer N      V-16 records per TopicWriter\n"
        << "  --payload-bytes N           V-16/V-17 payload bytes\n"
        << "  --pin-count N               V-27 lease-cleanup pins\n"
        << "  --directory PATH            temporary storage base\n"
        << "  --output-json PATH          additionally write JSON to PATH\n"
        << "  --commit SHA                overrides MINO_BENCHMARK_COMMIT\n"
        << "  --build-config TEXT         overrides MINO_BENCHMARK_BUILD_CONFIG\n"
        << "  --cpu-model TEXT            overrides MINO_BENCHMARK_CPU_MODEL\n"
        << "  --memory TEXT               overrides MINO_BENCHMARK_MEMORY\n"
        << "  --storage-device TEXT       overrides MINO_BENCHMARK_STORAGE_DEVICE\n"
        << "  --filesystem TEXT           overrides MINO_BENCHMARK_FILESYSTEM\n";
}

Config ParseArguments(int argc, char** argv) {
    Config config;
    std::ostringstream command;
    for (int index = 0; index < argc; ++index) {
        if (index != 0) command << ' ';
        command << ShellQuote(argv[index]);
    }
    config.command = command.str();

    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--help") {
            PrintUsage(std::cout, argv[0]);
            std::exit(0);
        }
        const auto parse_string = [&](std::string_view option,
                                      std::string* destination) -> bool {
            const auto value = InlineOption(argument, option);
            if (!value.has_value()) return false;
            *destination = OptionValue(&index, argc, argv, option, *value);
            return true;
        };
        const auto parse_number = [&](std::string_view option,
                                      uint64_t* destination) -> bool {
            const auto value = InlineOption(argument, option);
            if (!value.has_value()) return false;
            *destination = ParseUnsigned(
                OptionValue(&index, argc, argv, option, *value), option);
            return true;
        };
        uint64_t parsed = 0;
        if (parse_string("--suite", &config.suite) ||
            parse_string("--commit", &config.commit) ||
            parse_string("--build-config", &config.build_config) ||
            parse_string("--cpu-model", &config.cpu_model) ||
            parse_string("--memory", &config.memory_description) ||
            parse_string("--storage-device", &config.storage_device) ||
            parse_string("--filesystem", &config.filesystem)) {
            continue;
        }
        if (parse_number("--iterations", &config.iterations) ||
            parse_number("--storage-records", &config.storage_records) ||
            parse_number("--records-per-writer", &config.records_per_writer)) {
            continue;
        }
        if (parse_number("--payload-bytes", &parsed)) {
            if (parsed > std::numeric_limits<size_t>::max()) {
                throw std::runtime_error("--payload-bytes exceeds size_t");
            }
            config.payload_bytes = static_cast<size_t>(parsed);
            continue;
        }
        if (parse_number("--pin-count", &parsed)) {
            if (parsed > std::numeric_limits<size_t>::max()) {
                throw std::runtime_error("--pin-count exceeds size_t");
            }
            config.pin_count = static_cast<size_t>(parsed);
            continue;
        }
        if (const auto value = InlineOption(argument, "--directory")) {
            config.directory = std::filesystem::path(
                OptionValue(&index, argc, argv, "--directory", *value));
            continue;
        }
        if (const auto value = InlineOption(argument, "--output-json")) {
            config.output_json = std::filesystem::path(
                OptionValue(&index, argc, argv, "--output-json", *value));
            continue;
        }
        throw std::runtime_error("unknown option: " + std::string(argument));
    }

    if (config.suite != "all" && config.suite != "memory" &&
        config.suite != "storage") {
        throw std::runtime_error("--suite must be all, memory, or storage");
    }
    if (config.iterations == 0 || config.storage_records == 0 ||
        config.records_per_writer == 0 || config.pin_count == 0) {
        throw std::runtime_error("operation counts must be greater than zero");
    }
    if (config.payload_bytes > 16u * 1024u * 1024u) {
        throw std::runtime_error("--payload-bytes exceeds the 16 MiB pool limit");
    }
    if (config.iterations > 1'000'000 ||
        config.storage_records > 1'000'000 ||
        config.records_per_writer > 100'000) {
        throw std::runtime_error("operation count exceeds benchmark safety limit");
    }
    if (config.pin_count > 50'000) {
        throw std::runtime_error("--pin-count exceeds benchmark safety limit 50000");
    }

    if (config.commit.empty()) {
        config.commit = EnvironmentOr("MINO_BENCHMARK_COMMIT", "PENDING");
    }
    if (config.build_config.empty()) {
        config.build_config =
            EnvironmentOr("MINO_BENCHMARK_BUILD_CONFIG", "PENDING");
    }
    if (config.cpu_model.empty()) {
        config.cpu_model = EnvironmentOr("MINO_BENCHMARK_CPU_MODEL", "PENDING");
    }
    if (config.memory_description.empty()) {
        config.memory_description =
            EnvironmentOr("MINO_BENCHMARK_MEMORY", "PENDING");
    }
    if (config.storage_device.empty()) {
        config.storage_device =
            EnvironmentOr("MINO_BENCHMARK_STORAGE_DEVICE", "PENDING");
    }
    if (config.filesystem.empty()) {
        config.filesystem =
            EnvironmentOr("MINO_BENCHMARK_FILESYSTEM", "PENDING");
    }
    return config;
}

class OwnedTemporaryDirectory final {
public:
    explicit OwnedTemporaryDirectory(
        const std::optional<std::filesystem::path>& requested_base) {
        std::error_code error;
        std::filesystem::path base = requested_base.has_value()
            ? *requested_base
            : std::filesystem::temp_directory_path(error);
        if (error) throw std::runtime_error(error.message());
        std::filesystem::create_directories(base, error);
        if (error) throw std::runtime_error(error.message());
        base_ = std::filesystem::canonical(base, error);
        if (error) throw std::runtime_error(error.message());
        const uint64_t nonce = static_cast<uint64_t>(
            Clock::now().time_since_epoch().count());
        path_ = base_ / ("mino-d0-d5-benchmark-" +
                         std::to_string(static_cast<uint64_t>(::getpid())) + "-" +
                         std::to_string(nonce));
        if (!std::filesystem::create_directory(path_, error) || error) {
            throw std::runtime_error("cannot create benchmark temporary directory");
        }
        std::ofstream marker(path_ / ".mino-d0-d5-benchmark-owned");
        marker << "Mino D0-D5 validation benchmark\n";
        marker.close();
        if (!marker) throw std::runtime_error("cannot create ownership marker");
    }

    ~OwnedTemporaryDirectory() {
        std::error_code error;
        if (path_.parent_path() != base_ ||
            !path_.filename().string().starts_with("mino-d0-d5-benchmark-")) {
            return;
        }
        std::ifstream marker(path_ / ".mino-d0-d5-benchmark-owned");
        std::string value;
        std::getline(marker, value);
        if (value != "Mino D0-D5 validation benchmark") return;
        std::filesystem::remove_all(path_, error);
    }

    const std::filesystem::path& path() const noexcept { return path_; }
    const std::filesystem::path& base() const noexcept { return base_; }

private:
    std::filesystem::path base_;
    std::filesystem::path path_;
};

struct AlignedDeleter {
    void operator()(std::byte* pointer) const {
        ::operator delete[](pointer, std::align_val_t(64));
    }
};
using AlignedBytes = std::unique_ptr<std::byte[], AlignedDeleter>;

AlignedBytes AllocateAligned(size_t bytes) {
    AlignedBytes memory(new (std::align_val_t(64)) std::byte[bytes]);
    std::memset(memory.get(), 0, bytes);
    return memory;
}

void FillBroadcastSlot(BroadcastChannel::Reservation& reservation,
                       uint64_t sequence) {
    reservation->msg_type = 0x563134u;
    reservation->schema_version = 0x00010000u;
    reservation->schema_short_id = 0x563134563134ULL;
    reservation->schema_layout_version = 1;
    reservation->timestamp_ns = sequence + 1;
    reservation->payload = ShmHandle{
        .offset = 4096 + sequence * 64,
        .generation = 1,
        .region_id = 1,
    };
    reservation->payload_len = 64;
    reservation->flags = 0;
}

void BroadcastRound(BroadcastChannel& channel,
                    std::span<const BroadcastChannel::SubscriberHandle> subscribers,
                    uint64_t tag, std::vector<uint64_t>* publish_samples,
                    std::vector<uint64_t>* ack_samples,
                    std::vector<uint64_t>* round_samples) {
    const auto round_begin = Clock::now();
    const auto publish_begin = Clock::now();
    auto reservation = Take(channel.Reserve(), "BroadcastChannel::Reserve");
    FillBroadcastSlot(reservation, tag);
    Require(std::move(reservation).Commit(), "BroadcastChannel::Commit");
    const auto publish_end = Clock::now();
    if (publish_samples != nullptr) {
        publish_samples->push_back(DurationNs(publish_begin, publish_end));
    }
    uint64_t sink = 0;
    for (const auto& subscriber : subscribers) {
        const auto ack_begin = Clock::now();
        auto borrow = Take(channel.Poll(subscriber), "BroadcastChannel::Poll");
        sink ^= borrow->sequence_num;
        Require(std::move(borrow).Ack(), "BroadcastChannel::Borrow::Ack");
        if (ack_samples != nullptr) {
            ack_samples->push_back(DurationNs(ack_begin, Clock::now()));
        }
    }
    const auto round_end = Clock::now();
    if (round_samples != nullptr) {
        round_samples->push_back(DurationNs(round_begin, round_end));
    }
    g_sink.fetch_xor(sink, std::memory_order_relaxed);
}

std::string RunV14(uint64_t iterations) {
    constexpr std::array<uint32_t, 5> kSubscriberCounts = {
        1, 2, 8, 16, BroadcastChannel::kMaxSubscribers};
    std::ostringstream output;
    output << "{\"status\":\"MEASURED\",\"subscriber_limit\":"
           << BroadcastChannel::kMaxSubscribers
           << ",\"channel_capacity\":" << kBroadcastCapacity
           << ",\"fixed_layout\":true,\"scenarios\":[";
    bool first = true;
    for (uint32_t subscriber_count : kSubscriberCounts) {
        const size_t bytes = static_cast<size_t>(
            BroadcastChannel::RequiredSize(kBroadcastCapacity));
        auto memory = AllocateAligned(bytes);
        auto channel = Take(BroadcastChannel::Init(memory.get(), kBroadcastCapacity),
                            "BroadcastChannel::Init");
        std::vector<BroadcastChannel::SubscriberHandle> subscribers;
        subscribers.reserve(subscriber_count);
        for (uint32_t id = 0; id < subscriber_count; ++id) {
            subscribers.push_back(Take(
                channel.RegisterSubscriber(SubscriberId{id}),
                "BroadcastChannel::RegisterSubscriber"));
        }
        const uint64_t warmup = std::max<uint64_t>(1, iterations / 10);
        for (uint64_t index = 0; index < warmup; ++index) {
            BroadcastRound(channel, subscribers, index, nullptr, nullptr, nullptr);
        }
        std::vector<uint64_t> publish_samples;
        std::vector<uint64_t> ack_samples;
        std::vector<uint64_t> round_samples;
        publish_samples.reserve(iterations);
        ack_samples.reserve(iterations * subscriber_count);
        round_samples.reserve(iterations);
        for (uint64_t index = 0; index < iterations; ++index) {
            BroadcastRound(channel, subscribers, warmup + index,
                           &publish_samples, &ack_samples, &round_samples);
        }
        if (!first) output << ',';
        first = false;
        output << "{\"subscribers\":" << subscriber_count
               << ",\"iterations\":" << iterations
               << ",\"warmup_iterations\":" << warmup
               << ",\"memory\":{\"required_bytes\":" << bytes
               << ",\"incremental_bytes_for_active_count\":0"
               << ",\"subscriber_slots_bytes\":"
               << BroadcastChannel::kMaxSubscribers *
                      sizeof(BroadcastChannel::SubscriberSlot)
               << ",\"era_sidecar_bytes_per_channel_slot\":"
               << sizeof(BroadcastChannel::BroadcastEraMeta)
               << ",\"legacy_ack_meta_bytes_per_channel_slot\":"
               << sizeof(BroadcastSlotMeta) << "},\"publish_commit_latency_ns\":";
        WriteDistribution(output, Summarize(std::move(publish_samples)));
        output << ",\"poll_ack_latency_ns\":";
        WriteDistribution(output, Summarize(std::move(ack_samples)));
        output << ",\"fanout_roundtrip_latency_ns\":";
        WriteDistribution(output, Summarize(std::move(round_samples)));
        output << '}';
    }
    output << "]}";
    return output.str();
}

ClassTableConfig DynamicAllocatorConfig() {
    ClassTableConfig config;
    config.classes = {
        {.slot_size = 64, .slot_count = 32},
        {.slot_size = 128, .slot_count = 32},
        {.slot_size = 256, .slot_count = 32},
        {.slot_size = 512, .slot_count = 32},
        {.slot_size = 1024, .slot_count = 16},
        {.slot_size = 2048, .slot_count = 16},
    };
    return config;
}

std::string RunV15(uint64_t iterations) {
    auto compiled = Take(schema::SchemaCompiler::Compile(R"idl(
syntax = "v1";
package golden;
option schema_version = "2.1";
message Telemetry {
  required uint32 sequence = 1;
  optional string label = 2
      [max_bytes = 16, snapshot_key, default = "re\x00ady"];
  bytes payload = 3 [max_bytes = 32, default = "\xff\x00"];
  vector<uint64> samples = 4 [max_capacity = 8];
  optional bool active = 5;
  reserved 6 to 7;
}
)idl"), "SchemaCompiler::Compile");
    schema::DynamicSchemaHandle descriptor;
    for (const auto& candidate : compiled.types()) {
        if (candidate->aggregate().full_name() == "golden.Telemetry") {
            descriptor = candidate;
        }
    }
    if (descriptor == nullptr) throw std::runtime_error("Telemetry descriptor absent");
    auto layout = Take(schema::LayoutPlanner::Plan(*descriptor, compiled.types()),
                       "LayoutPlanner::Plan");
    auto sequence_field = Take(
        schema::FieldHandle::ById(*descriptor, layout, 1),
        "FieldHandle::ById(sequence)");
    auto payload_field = Take(
        schema::FieldHandle::ById(*descriptor, layout, 3),
        "FieldHandle::ById(payload)");
    auto samples_field = Take(
        schema::FieldHandle::ById(*descriptor, layout, 4),
        "FieldHandle::ById(samples)");

    constexpr size_t kAllocatorBytes = 4u << 20;
    auto allocator_memory = AllocateAligned(kAllocatorBytes);
    auto allocator = Take(CentralSlabAllocator::Create(
                              allocator_memory.get(), kAllocatorBytes,
                              DynamicAllocatorConfig()),
                          "CentralSlabAllocator::Create");
    const size_t journal_bytes = AllocationJournal::RequiredSize(8, 128);
    auto journal_memory = AllocateAligned(journal_bytes);
    auto journal = Take(AllocationJournal::Init(
                            journal_memory.get(), journal_bytes, 8, 128, allocator),
                        "AllocationJournal::Init");
    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = Take(ShmPinTable::Init(pin_memory.get(),
                                      ShmPinTable::RequiredSize(), allocator),
                     "ShmPinTable::Init");

    auto builder = Take(schema::DynamicBuilder::Create(
                            descriptor, layout, allocator, journal, TypeId{42},
                            compiled.types()),
                        "DynamicBuilder::Create");
    Require(builder.SetUnsigned(sequence_field, 0x12345678u),
            "DynamicBuilder::SetUnsigned");
    const std::array<std::byte, 2> payload = {std::byte{0xff}, std::byte{0}};
    Require(builder.SetBytes(payload_field, payload),
            "DynamicBuilder::SetBytes");
    const schema::DynamicVector samples;
    Require(builder.SetVector(samples_field, samples),
            "DynamicBuilder::SetVector");
    auto object = Take(builder.Commit(pins), "DynamicBuilder::Commit");

    golden::Telemetry static_object;
    golden::TelemetryBuilder static_builder(static_object);
    static_builder.set_sequence(0x12345678u);
    if (!static_builder.set_payload({.element_size = 1u}) ||
        !static_builder.set_samples({.element_size = 8u})) {
        throw std::runtime_error("generated static builder initialization failed");
    }
    const golden::TelemetryAccessor static_view(static_object);

    std::vector<uint64_t> static_samples;
    std::vector<uint64_t> dynamic_samples;
    static_samples.reserve(iterations);
    dynamic_samples.reserve(iterations);
    uint64_t dynamic_errors = 0;
    std::string json;
    {
        auto pin = Take(object.Pin(), "DynamicObject::Pin");
        auto dynamic_view = Take(schema::DynamicView::Create(
                                     descriptor, layout, object.root_handle(),
                                     allocator, std::move(pin), compiled.types()),
                                 "DynamicView::Create");
        const uint64_t warmup = std::max<uint64_t>(1, iterations / 10);
        for (uint64_t index = 0; index < warmup; ++index) {
            g_sink.fetch_xor(static_view.sequence(), std::memory_order_relaxed);
            auto value = dynamic_view.GetUnsigned(sequence_field);
            if (!value.ok()) throw std::runtime_error(value.status().ToString());
            g_sink.fetch_xor(*value, std::memory_order_relaxed);
        }
        for (uint64_t index = 0; index < iterations; ++index) {
            auto begin = Clock::now();
            const uint32_t static_value = static_view.sequence();
            std::atomic_signal_fence(std::memory_order_seq_cst);
            static_samples.push_back(DurationNs(begin, Clock::now()));
            g_sink.fetch_xor(static_value, std::memory_order_relaxed);

            begin = Clock::now();
            auto dynamic_value = dynamic_view.GetUnsigned(sequence_field);
            std::atomic_signal_fence(std::memory_order_seq_cst);
            dynamic_samples.push_back(DurationNs(begin, Clock::now()));
            if (!dynamic_value.ok()) {
                ++dynamic_errors;
            } else {
                g_sink.fetch_xor(*dynamic_value, std::memory_order_relaxed);
            }
        }
        if (dynamic_errors != 0) {
            g_failed.store(true, std::memory_order_relaxed);
        }
        std::ostringstream output;
        output << "{\"status\":\""
               << (dynamic_errors == 0 ? "MEASURED" : "FAILED")
               << "\",\"iterations\":" << iterations
               << ",\"warmup_iterations\":" << warmup
               << ",\"logical_field\":\"golden.Telemetry.sequence(uint32)\""
               << ",\"static_view\":{\"implementation\":\"generated TelemetryAccessor\",\"errors\":0,\"latency_ns\":";
        WriteDistribution(output, Summarize(std::move(static_samples)));
        output << "},\"dynamic_view\":{\"implementation\":\"schema::DynamicView::GetUnsigned(FieldHandle)\",\"errors\":"
               << dynamic_errors << ",\"latency_ns\":";
        WriteDistribution(output, Summarize(std::move(dynamic_samples)));
        output << "}}";
        json = output.str();
    }
    Require(object.Reclaim(), "DynamicObject::Reclaim");
    return json;
}

std::string RunV18() {
    constexpr std::array<size_t, 7> kPayloads = {
        64, 4096, 4097, 65536, 65537, 1024 * 1024, 1024 * 1024 + 1};
    storage::RecorderBufferPoolOptions options;
    options.global_byte_limit = 32u * 1024u * 1024u;
    options.default_topic_byte_limit = options.global_byte_limit;
    options.queue_capacity = 32;
    auto pool = Take(storage::RecorderBufferPool::Create(options),
                     "RecorderBufferPool::Create");
    std::ostringstream output;
    output << "{\"status\":\"MEASURED_AND_MODELED\",\"capacity_charge_samples\":[";
    bool first = true;
    for (size_t payload : kPayloads) {
        storage::BufferReservationRequest request;
        request.topic_id = TopicId{1};
        request.payload_size = payload;
        request.full_policy = storage::BufferFullPolicy::kDropNewest;
        auto reserved = Take(pool->Reserve(request), "RecorderBufferPool::Reserve");
        if (!reserved.accepted()) {
            throw std::runtime_error("capacity charge sample was unexpectedly dropped");
        }
        const size_t charged = reserved.reservation.capacity();
        static_cast<void>(reserved.reservation.Cancel());
        if (!first) output << ',';
        first = false;
        output << "{\"payload_bytes\":" << payload
               << ",\"charged_bytes\":" << charged << '}';
    }
    constexpr std::array<uint64_t, 3> kIngressMiB = {10, 100, 1000};
    constexpr std::array<uint64_t, 3> kPauseMs = {10, 100, 1000};
    output << "],\"disk_pause_capacity_model\":{\"kind\":\"deterministic_formula\",\"formula\":\"required_bytes=ingress_bytes_per_second*pause_seconds\",\"scenarios\":[";
    first = true;
    for (uint64_t ingress_mib : kIngressMiB) {
        for (uint64_t pause_ms : kPauseMs) {
            const uint64_t required = ingress_mib * 1024u * 1024u * pause_ms / 1000u;
            if (!first) output << ',';
            first = false;
            output << "{\"ingress_mib_per_second\":" << ingress_mib
                   << ",\"disk_pause_ms\":" << pause_ms
                   << ",\"minimum_payload_bytes\":" << required << '}';
        }
    }
    output << "],\"excludes\":[\"queue metadata\",\"fixed-class internal fragmentation beyond separately measured charged_bytes\",\"safety margin\"]}}";
    return output.str();
}

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

std::vector<std::byte> MakePayload(size_t payload_bytes) {
    std::vector<std::byte> payload(payload_bytes);
    uint64_t state = kPayloadSeed;
    for (std::byte& byte : payload) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        byte = static_cast<std::byte>(state & 0xffu);
    }
    return payload;
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
    writer_options.segment_options.sync_policy =
        storage::SegmentSyncPolicy::kNone;
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

std::string RunV16(const std::filesystem::path& root,
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
        const auto scenario_root = root / ("v16-writers-" +
                                           std::to_string(writer_count));
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
            for (uint64_t sequence = 1; sequence <= records_per_writer; ++sequence) {
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
        if (errors != 0) {
            g_failed.store(true, std::memory_order_relaxed);
        }
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

std::string RunV17(const std::filesystem::path& root, uint64_t records,
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
                               root / ("v17-" + SyncPolicyName(policy) + ".segment"),
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
        if (errors != 0 || sync_samples.errors != 0) {
            g_failed.store(true, std::memory_order_relaxed);
        }
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

class BenchmarkLivenessProbe final : public registry::LivenessProbe {
public:
    ProcessIdentityLiveness Probe(
        const ProcessIdentity&) const noexcept override {
        return dead_.load(std::memory_order_acquire)
            ? ProcessIdentityLiveness::kDead
            : ProcessIdentityLiveness::kAlive;
    }
    void MarkDead() noexcept { dead_.store(true, std::memory_order_release); }
private:
    std::atomic<bool> dead_{false};
};

registry::NodeRegistration BenchmarkNode() {
    const NodeId node_id{1};
    const ProcessIdentity identity{
        .node_id = 1,
        .process_id = 1,
        .process_epoch = 1,
        .start_time_ns = 1,
    };
    const std::array<std::byte, 4> address = {
        std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}};
    auto endpoint = Take(transport::EndpointDescriptor::Ipv4Tcp(address, 43141),
                         "EndpointDescriptor::Ipv4Tcp");
    return registry::NodeRegistration{
        .node_id = node_id,
        .process_identity = identity,
        .endpoints = {endpoint},
        .trust_domain = "benchmark",
        .health = registry::NodeHealth::kHealthy,
        .lease_epoch = 1,
        .lease_duration_ns = 100,
        .config_version = 1,
    };
}

registry::TopicMetadata BenchmarkTopic() {
    schema::CanonicalDigest digest{};
    digest[0] = std::byte{1};
    digest[31] = std::byte{0x5a};
    return registry::TopicMetadata{
        .topic_id = {},
        .name = "benchmark/pins",
        .channel_kind = registry::ChannelKind::kBroadcast,
        .delivery = {.reliability = registry::Reliability::kBestEffort,
                     .allow_drop = false},
        .queue_full_policy = QueueFullPolicy::kBlock,
        .schema = schema::SchemaIdentity(1, digest, 1, 1),
        .accepted_schemas = {},
        .route_policy = registry::RoutePolicy::kDiscovery,
        .static_routes = {},
        .route_set_version = 0,
        .capacity = 256,
        .max_publishers = 1,
        .max_subscribers = 1,
        .partition_count = 1,
        .record_topology = registry::RecordBackpressureTopology::kIsolated,
        .region_version = 1,
        .channel_version = 1,
        .acl_version = 1,
        .config_version = 0,
        .state = registry::TopicState::kCreating,
    };
}

std::string RunV27(size_t pin_count) {
    auto probe = std::make_shared<BenchmarkLivenessProbe>();
    registry::CoordinatorLimits limits;
    limits.max_topic_pins = std::max<size_t>(pin_count + 16, 1024);
    auto coordinator = Take(registry::Coordinator::CreateForTesting(
                                limits,
                                std::make_shared<registry::InMemoryMonotonicIdAllocator>(),
                                probe),
                            "Coordinator::CreateForTesting");
    const auto node = BenchmarkNode();
    Take(coordinator->RegisterNode(node, 1), "Coordinator::RegisterNode");
    auto created = Take(coordinator->CreateTopic(BenchmarkTopic()),
                        "Coordinator::CreateTopic");
    const TopicId topic_id = created->metadata.topic_id;
    const registry::ActivationReadinessProof proof{
        .topic_id = topic_id,
        .config_version = created->metadata.config_version,
        .schema = created->metadata.schema,
        .region_version = created->metadata.region_version,
        .channel_version = created->metadata.channel_version,
        .acl_version = created->metadata.acl_version,
        .schema_ready = true,
        .region_ready = true,
        .channel_ready = true,
        .acl_ready = true,
    };
    Require(coordinator->ActivateTopic(topic_id, proof),
            "Coordinator::ActivateTopic");
    const registry::NodeLeaseOwner owner{
        .node_id = node.node_id,
        .process_identity = node.process_identity,
        .lease_epoch = node.lease_epoch,
    };

    std::vector<uint64_t> acquire_samples;
    std::vector<uint64_t> release_samples;
    acquire_samples.reserve(pin_count);
    release_samples.reserve(pin_count);
    for (size_t index = 0; index < pin_count; ++index) {
        const registry::TopicPinRegistration pin{
            .topic_id = topic_id,
            .pin_id = registry::TopicPinId{index + 1},
            .kind = registry::TopicPinKind::kRecorder,
            .generation = index + 1,
            .owner = owner,
        };
        auto begin = Clock::now();
        Require(coordinator->AcquireTopicPin(pin, 1),
                "Coordinator::AcquireTopicPin");
        acquire_samples.push_back(DurationNs(begin, Clock::now()));
        begin = Clock::now();
        Require(coordinator->ReleaseTopicPin(pin),
                "Coordinator::ReleaseTopicPin");
        release_samples.push_back(DurationNs(begin, Clock::now()));
    }

    for (size_t index = 0; index < pin_count; ++index) {
        const registry::TopicPinRegistration pin{
            .topic_id = topic_id,
            .pin_id = registry::TopicPinId{1'000'000 + index},
            .kind = registry::TopicPinKind::kRecorder,
            .generation = index + 1,
            .owner = owner,
        };
        Require(coordinator->AcquireTopicPin(pin, 1),
                "Coordinator::AcquireTopicPin(cleanup set)");
    }
    probe->MarkDead();
    const auto cleanup_begin = Clock::now();
    auto swept = Take(coordinator->SweepExpiredNodes(101),
                      "Coordinator::SweepExpiredNodes");
    const uint64_t cleanup_ns = DurationNs(cleanup_begin, Clock::now());
    if (swept.pins_removed != pin_count) {
        throw std::runtime_error("lease cleanup did not remove every benchmark pin");
    }
    std::ostringstream output;
    output << "{\"status\":\"MEASURED\",\"pin_operations\":" << pin_count
           << ",\"acquire_latency_ns\":";
    WriteDistribution(output, Summarize(std::move(acquire_samples)));
    output << ",\"release_latency_ns\":";
    WriteDistribution(output, Summarize(std::move(release_samples)));
    output << ",\"lease_cleanup\":{\"expired_owner_pins\":" << pin_count
           << ",\"pins_removed\":" << swept.pins_removed
           << ",\"elapsed_ns\":" << cleanup_ns
           << ",\"liveness\":\"explicitly marked dead after lease deadline\"}}";
    return output.str();
}

std::string RunTimestampUtc() {
    const std::time_t now = std::time(nullptr);
    std::tm utc{};
    if (now == static_cast<std::time_t>(-1) ||
        ::gmtime_r(&now, &utc) == nullptr) {
        return "PENDING";
    }
    std::array<char, 32> buffer{};
    if (std::strftime(buffer.data(), buffer.size(), "%Y-%m-%dT%H:%M:%SZ",
                      &utc) == 0) {
        return "PENDING";
    }
    return buffer.data();
}

std::string OperatingSystem() {
    struct utsname value {};
    if (::uname(&value) != 0) return "PENDING";
    return std::string(value.sysname) + " " + value.release + " " + value.machine;
}

uint64_t PhysicalMemoryBytes() {
    const long pages = ::sysconf(_SC_PHYS_PAGES);
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if (pages <= 0 || page_size <= 0) return 0;
    const auto unsigned_pages = static_cast<uint64_t>(pages);
    const auto unsigned_page_size = static_cast<uint64_t>(page_size);
    if (unsigned_pages > std::numeric_limits<uint64_t>::max() /
                             unsigned_page_size) {
        return 0;
    }
    return unsigned_pages * unsigned_page_size;
}

std::string PendingResult(std::string_view reason) {
    return "{\"status\":\"PENDING\",\"reason\":\"" +
           JsonEscape(reason) + "\",\"metrics\":null}";
}

std::string BuildFailureJson(int argc, char** argv, std::string_view reason) {
    std::ostringstream command;
    for (int index = 0; index < argc; ++index) {
        if (index != 0) command << ' ';
        command << ShellQuote(argv[index]);
    }
    const std::string pending = PendingResult(
        "benchmark aborted: " + std::string(reason));
    std::ostringstream output;
    output << "{\n  \"schema\": \"mino.d0_d5_validation_benchmark.v1\",\n"
           << "  \"artifact_status\": \"FAILED\",\n"
           << "  \"clock\": \"std::chrono::steady_clock\",\n"
           << "  \"provenance\": {\n"
           << "    \"run_timestamp_utc\": \""
           << JsonEscape(RunTimestampUtc()) << "\",\n"
           << "    \"commit\": \"PENDING\",\n"
           << "    \"command\": \"" << JsonEscape(command.str()) << "\",\n"
           << "    \"build_config\": \"PENDING\",\n"
           << "    \"compiler\": \"" << JsonEscape(__VERSION__) << "\",\n"
           << "    \"cplusplus\": " << __cplusplus << ",\n"
           << "    \"os\": \"" << JsonEscape(OperatingSystem()) << "\",\n"
           << "    \"hardware\": {\"logical_cpu_count\":"
           << std::thread::hardware_concurrency()
           << ",\"physical_memory_bytes_detected\":"
           << PhysicalMemoryBytes()
           << ",\"cpu_model\":\"PENDING\",\"memory\":\"PENDING\","
              "\"storage_device\":\"PENDING\",\"filesystem\":\"PENDING\"}\n"
           << "  },\n"
           << "  \"configuration\": {\"suite\":\"PENDING\","
              "\"iterations\":null,\"storage_records\":null,"
              "\"records_per_writer\":null,\"payload_bytes\":null,"
              "\"pin_count\":null,\"temporary_directory_base\":null},\n"
           << "  \"methodology\": {\"percentile_method\":\"nearest-rank\","
              "\"latency_unit\":\"ns\"},\n"
           << "  \"results\": {\"V-14\":" << pending
           << ",\"V-15\":" << pending << ",\"V-16\":" << pending
           << ",\"V-17\":" << pending << ",\"V-18\":" << pending
           << ",\"V-27\":" << pending << "},\n"
           << "  \"sink\": " << g_sink.load(std::memory_order_relaxed)
           << "\n}\n";
    return output.str();
}

std::string BuildJson(const Config& config, const std::string& v14,
                      const std::string& v15, const std::string& v16,
                      const std::string& v17, const std::string& v18,
                      const std::string& v27,
                      const std::optional<OwnedTemporaryDirectory>& temporary) {
    std::ostringstream output;
    output << std::setprecision(17)
           << "{\n  \"schema\": \"mino.d0_d5_validation_benchmark.v1\",\n"
           << "  \"artifact_status\": \""
           << (g_failed.load(std::memory_order_relaxed) ? "FAILED" : "MEASURED")
           << "\",\n"
           << "  \"clock\": \"std::chrono::steady_clock\",\n"
           << "  \"provenance\": {\n"
           << "    \"run_timestamp_utc\": \""
           << JsonEscape(RunTimestampUtc()) << "\",\n"
           << "    \"commit\": \"" << JsonEscape(config.commit) << "\",\n"
           << "    \"command\": \"" << JsonEscape(config.command) << "\",\n"
           << "    \"build_config\": \"" << JsonEscape(config.build_config)
           << "\",\n"
           << "    \"compiler\": \"" << JsonEscape(__VERSION__) << "\",\n"
           << "    \"cplusplus\": " << __cplusplus << ",\n"
           << "    \"os\": \"" << JsonEscape(OperatingSystem()) << "\",\n"
           << "    \"hardware\": {\n"
           << "      \"logical_cpu_count\": "
           << std::thread::hardware_concurrency() << ",\n"
           << "      \"physical_memory_bytes_detected\": "
           << PhysicalMemoryBytes() << ",\n"
           << "      \"cpu_model\": \"" << JsonEscape(config.cpu_model)
           << "\",\n"
           << "      \"memory\": \""
           << JsonEscape(config.memory_description) << "\",\n"
           << "      \"storage_device\": \""
           << JsonEscape(config.storage_device) << "\",\n"
           << "      \"filesystem\": \"" << JsonEscape(config.filesystem)
           << "\"\n    }\n  },\n"
           << "  \"configuration\": {\n"
           << "    \"suite\": \"" << config.suite << "\",\n"
           << "    \"iterations\": " << config.iterations << ",\n"
           << "    \"storage_records\": " << config.storage_records << ",\n"
           << "    \"records_per_writer\": " << config.records_per_writer
           << ",\n"
           << "    \"payload_bytes\": " << config.payload_bytes << ",\n"
           << "    \"pin_count\": " << config.pin_count << ",\n"
           << "    \"temporary_directory_base\": \""
           << JsonEscape(temporary.has_value() ? temporary->base().string()
                                               : "not-used")
           << "\"\n  },\n"
           << "  \"methodology\": {\n"
           << "    \"percentile_method\": \"nearest-rank\",\n"
           << "    \"latency_unit\": \"ns\",\n"
           << "    \"measured_results_are_generated_only_by_execution\": true,\n"
           << "    \"capacity_model_is_labeled_separately\": true\n  },\n"
           << "  \"results\": {\n"
           << "    \"V-14\": " << v14 << ",\n"
           << "    \"V-15\": " << v15 << ",\n"
           << "    \"V-16\": " << v16 << ",\n"
           << "    \"V-17\": " << v17 << ",\n"
           << "    \"V-18\": " << v18 << ",\n"
           << "    \"V-27\": " << v27 << "\n  },\n"
           << "  \"sink\": " << g_sink.load(std::memory_order_relaxed)
           << "\n}\n";
    return output.str();
}

}  // namespace
}  // namespace mino::benchmarks

int main(int argc, char** argv) {
    try {
        const mino::benchmarks::Config config =
            mino::benchmarks::ParseArguments(argc, argv);
        const bool run_memory = config.suite == "all" || config.suite == "memory";
        const bool run_storage = config.suite == "all" || config.suite == "storage";
        std::optional<mino::benchmarks::OwnedTemporaryDirectory> temporary;
        if (run_storage) temporary.emplace(config.directory);

        const std::string v14 = run_memory
            ? mino::benchmarks::RunV14(config.iterations)
            : mino::benchmarks::PendingResult("suite excludes memory benchmarks");
        const std::string v15 = run_memory
            ? mino::benchmarks::RunV15(config.iterations)
            : mino::benchmarks::PendingResult("suite excludes memory benchmarks");
        const std::string v18 = run_memory
            ? mino::benchmarks::RunV18()
            : mino::benchmarks::PendingResult("suite excludes memory benchmarks");
        const std::string v27 = run_memory
            ? mino::benchmarks::RunV27(config.pin_count)
            : mino::benchmarks::PendingResult("suite excludes memory benchmarks");
        const std::string v16 = run_storage
            ? mino::benchmarks::RunV16(temporary->path(),
                                      config.records_per_writer,
                                      config.payload_bytes)
            : mino::benchmarks::PendingResult("suite excludes storage benchmarks");
        const std::string v17 = run_storage
            ? mino::benchmarks::RunV17(temporary->path(),
                                      config.storage_records,
                                      config.payload_bytes)
            : mino::benchmarks::PendingResult("suite excludes storage benchmarks");
        const std::string json = mino::benchmarks::BuildJson(
            config, v14, v15, v16, v17, v18, v27, temporary);
        std::cout << json;
        if (config.output_json.has_value()) {
            const std::filesystem::path parent =
                config.output_json->parent_path();
            if (!parent.empty()) {
                std::error_code error;
                std::filesystem::create_directories(parent, error);
                if (error) {
                    throw std::runtime_error(
                        "cannot create --output-json directory: " +
                        error.message());
                }
            }
            std::ofstream output(*config.output_json,
                                 std::ios::binary | std::ios::trunc);
            output << json;
            output.close();
            if (!output) throw std::runtime_error("cannot write --output-json");
        }
        return mino::benchmarks::g_failed.load(std::memory_order_relaxed) ? 1 : 0;
    } catch (const std::exception& error) {
        mino::benchmarks::g_failed.store(true, std::memory_order_relaxed);
        std::cout << mino::benchmarks::BuildFailureJson(argc, argv, error.what());
        std::cerr << "d0_d5_validation_benchmark: " << error.what() << '\n';
        return 1;
    }
}
