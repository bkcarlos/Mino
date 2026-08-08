// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "mino/bridge/wire_frame.h"
#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/observability/metrics.h"
#include "mino/observability/telemetry.h"
#include "mino/observability/tracing.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/channel/broadcast_channel.h"
#include "mino/storage/recorder_buffer_pool.h"

namespace mino::benchmarks::soak_probe {
namespace {

using Clock = std::chrono::steady_clock;

constexpr size_t kAllocatorBytes = 16u * 1024u * 1024u;
constexpr size_t kLiveSlabs = 64;
constexpr uint64_t kChannelCapacity = 256;
constexpr size_t kChannelBacklog = 16;
constexpr size_t kStorageBacklog = 16;
constexpr size_t kPayloadBytes = 256;

volatile std::sig_atomic_t g_stop = 0;

extern "C" void HandleSignal(int signal_number) { g_stop = signal_number; }

struct AlignedDeleter {
    void operator()(std::byte* pointer) const noexcept {
        ::operator delete[](pointer, std::align_val_t(64));
    }
};
using AlignedBytes = std::unique_ptr<std::byte[], AlignedDeleter>;

AlignedBytes AllocateAligned(size_t bytes) {
    AlignedBytes result(new (std::align_val_t(64)) std::byte[bytes]);
    std::memset(result.get(), 0, bytes);
    return result;
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

struct Config {
    uint64_t seed = 1;
    uint64_t report_interval_ms = 1000;
};

Config ParseArguments(int argc, char** argv) {
    Config config;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        const auto value = [&](std::string_view option) -> std::string_view {
            const std::string prefix = std::string(option) + "=";
            if (argument.starts_with(prefix)) return argument.substr(prefix.size());
            if (argument == option && index + 1 < argc) return argv[++index];
            return {};
        };
        if (argument == "--help") {
            std::cout << "Usage: soak_probe [--seed=N] [--report-interval-ms=N]\n";
            std::exit(0);
        }
        if (argument == "--seed" || argument.starts_with("--seed=")) {
            config.seed = ParseUnsigned(value("--seed"), "--seed");
        } else if (argument == "--report-interval-ms" ||
                   argument.starts_with("--report-interval-ms=")) {
            config.report_interval_ms =
                ParseUnsigned(value("--report-interval-ms"),
                              "--report-interval-ms");
        } else {
            throw std::runtime_error("unknown argument: " + std::string(argument));
        }
    }
    if (config.report_interval_ms == 0 || config.report_interval_ms > 60'000) {
        throw std::runtime_error("--report-interval-ms must be in [1, 60000]");
    }
    if (config.seed == 0) config.seed = 1;
    return config;
}

ClassTableConfig AllocatorConfig() {
    ClassTableConfig config;
    config.classes = {
        {.slot_size = 64, .slot_count = 128},
        {.slot_size = 128, .slot_count = 128},
        {.slot_size = 256, .slot_count = 128},
        {.slot_size = 512, .slot_count = 128},
        {.slot_size = 1024, .slot_count = 128},
        {.slot_size = 2048, .slot_count = 64},
    };
    return config;
}

uint64_t NextRandom(uint64_t* state) {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    return *state;
}

AllocationRequest MakeAllocation() {
    return AllocationRequest{
        .object_size = 256,
        .type_id = TypeId{7},
        .schema = SchemaIdentity{.short_id = 0x534f414b, .layout_version = 1},
        .alignment = 8,
    };
}

void FillChannelSlot(BroadcastChannel::Reservation& reservation,
                     uint64_t sequence) {
    reservation->msg_type = 0x534f414b;
    reservation->schema_version = 0x00010000u;
    reservation->schema_short_id = 0x534f414b;
    reservation->schema_layout_version = 1;
    reservation->timestamp_ns = sequence + 1;
    reservation->payload = ShmHandle{};
    reservation->payload_len = 0;
    reservation->flags = 0;
}

void Publish(BroadcastChannel* channel, uint64_t sequence) {
    auto reservation = Take(channel->Reserve(), "BroadcastChannel::Reserve");
    FillChannelSlot(reservation, sequence);
    Require(std::move(reservation).Commit(), "BroadcastChannel::Commit");
}

void EnqueueStorage(storage::RecorderBufferPool* pool, uint64_t sequence,
                    uint64_t random) {
    storage::BufferReservationRequest request;
    request.topic_id = TopicId{1};
    request.payload_size = kPayloadBytes;
    request.user_tag = sequence;
    request.full_policy = storage::BufferFullPolicy::kDropNewest;
    auto reserved = Take(pool->Reserve(request), "RecorderBufferPool::Reserve");
    if (!reserved.accepted()) {
        throw std::runtime_error("RecorderBufferPool unexpectedly rejected record");
    }
    auto bytes = reserved.reservation.bytes();
    for (size_t index = 0; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>((random + index) & 0xffu);
    }
    Require(std::move(reserved.reservation).Commit(),
            "RecorderBufferReservation::Commit");
}

bridge::WireFrame MakeFrame(uint64_t sequence, uint64_t random) {
    bridge::WireFrame frame;
    frame.header.frame_type = bridge::FrameType::kData;
    frame.header.flags = bridge::FlagValue(bridge::FrameFlag::kPayloadCrcPresent);
    frame.header.topic_id = 1;
    frame.header.msg_type = 0x534f414b;
    frame.header.connection_schema_ref = 1;
    frame.header.schema_version = 0x00010000u;
    frame.header.layout_version = 1;
    frame.header.source_node_id = 1;
    frame.header.source_publisher_id = 1;
    frame.header.source_publisher_epoch = 1;
    frame.header.sequence_num = sequence;
    frame.header.timestamp_ns = sequence;
    frame.payload.resize(kPayloadBytes);
    for (size_t index = 0; index < frame.payload.size(); ++index) {
        frame.payload[index] = static_cast<std::byte>((random + index) & 0xffu);
    }
    return frame;
}

struct SlabUsage {
    uint64_t occupied_slots = 0;
    uint64_t occupied_bytes = 0;
};

SlabUsage ReadSlabUsage(const CentralSlabAllocator& allocator) {
    SlabUsage usage;
    for (uint32_t index = 0; index < allocator.total_slot_count(); ++index) {
        if (!allocator.IsSlotOccupiedForRecovery(index)) continue;
        SlabHeader header{};
        if (!allocator.ReadSlotByIndex(index, &header, nullptr)) {
            throw std::runtime_error("occupied allocator slot was unreadable");
        }
        ++usage.occupied_slots;
        usage.occupied_bytes += sizeof(SlabHeader) + header.capacity;
    }
    return usage;
}

class Workloads {
public:
    explicit Workloads(const Config& config)
        : allocator_memory_(AllocateAligned(kAllocatorBytes)),
          channel_memory_(AllocateAligned(
              BroadcastChannel::RequiredSize(kChannelCapacity))),
          allocator_(Take(CentralSlabAllocator::Create(
                              allocator_memory_.get(), kAllocatorBytes,
                              AllocatorConfig()),
                          "CentralSlabAllocator::Create")),
          channel_(Take(BroadcastChannel::Init(channel_memory_.get(),
                                               kChannelCapacity),
                        "BroadcastChannel::Init")),
          subscriber_(Take(channel_.RegisterSubscriber(SubscriberId{0}),
                           "BroadcastChannel::RegisterSubscriber")),
          telemetry_control_(observability::PerfTelemetryPolicy{
              .mode = observability::PerfTelemetryMode::kFullDebug,
              .sample_rate_ppm = 1'000'000,
              .slow_threshold_ns = 0,
              .max_events_per_second = std::numeric_limits<uint32_t>::max(),
          }),
          tracer_(telemetry_control_),
          random_(config.seed) {
        storage::RecorderBufferPoolOptions options;
        options.global_byte_limit = 8u * 1024u * 1024u;
        options.default_topic_byte_limit = options.global_byte_limit;
        options.queue_capacity = 256;
        storage_pool_ = Take(storage::RecorderBufferPool::Create(options),
                             "RecorderBufferPool::Create");
        Require(metrics_.RegisterCounter("soak_operations", &operations_metric_),
                "MetricRegistry::RegisterCounter");
        Require(metrics_.RegisterGauge("soak_queue_depth", &queue_metric_),
                "MetricRegistry::RegisterGauge");
        Require(metrics_.RegisterHistogram("soak_wire_bytes", &wire_metric_),
                "MetricRegistry::RegisterHistogram");

        live_slabs_.reserve(kLiveSlabs);
        for (size_t index = 0; index < kLiveSlabs; ++index) {
            live_slabs_.push_back(Take(
                allocator_.Allocate(MakeAllocation()),
                "CentralSlabAllocator::Allocate"));
        }
        for (size_t index = 0; index < kChannelBacklog; ++index) {
            Publish(&channel_, sequence_++);
        }
        for (size_t index = 0; index < kStorageBacklog; ++index) {
            EnqueueStorage(storage_pool_.get(), sequence_++, NextRandom(&random_));
        }
    }

    ~Workloads() {
        storage_pool_->Close();
        for (const ShmHandle handle : live_slabs_) {
            (void)allocator_.Abort(handle);
        }
    }

    void Step() {
        const uint64_t random = NextRandom(&random_);

        Require(allocator_.Abort(live_slabs_[slab_cursor_]),
                "CentralSlabAllocator::Abort");
        live_slabs_[slab_cursor_] = Take(
            allocator_.Allocate(MakeAllocation()),
            "CentralSlabAllocator::Allocate");
        slab_cursor_ = (slab_cursor_ + 1) % live_slabs_.size();

        Publish(&channel_, sequence_);
        auto borrowed = Take(channel_.Poll(subscriber_),
                             "BroadcastChannel::Poll");
        Require(std::move(borrowed).Ack(), "BroadcastChannel::Borrow::Ack");

        const bridge::WireFrame frame = MakeFrame(sequence_, random);
        auto encoded = Take(bridge::WireFrameCodec::Encode(frame),
                            "WireFrameCodec::Encode");
        auto decoded = Take(bridge::WireFrameCodec::Decode(encoded),
                            "WireFrameCodec::Decode");
        if (decoded != frame) throw std::runtime_error("bridge wire round trip drifted");

        auto dequeued = Take(storage_pool_->TryDequeue(),
                             "RecorderBufferPool::TryDequeue");
        if (!dequeued.valid()) throw std::runtime_error("storage dequeue was empty");
        dequeued.Reset();
        EnqueueStorage(storage_pool_.get(), sequence_, random);

        const observability::SampleKey key{
            .topic_id = 1, .source_identity = 1, .sequence = sequence_};
        const uint64_t now_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                Clock::now().time_since_epoch())
                .count());
        const observability::TraceEvent event{
            .trace_id_high = observability::StableSampleHash(key),
            .trace_id_low = sequence_,
            .topic_id = 1,
            .monotonic_time_ns = now_ns,
            .duration_ns = random & 0xffffu,
            .payload_bytes = kPayloadBytes,
            .wire_bytes = static_cast<uint32_t>(encoded.size()),
            .flags = observability::kPerfTraceSampled,
            .stage = observability::TraceStage::kEncodeEnd,
        };
        if (!tracer_.TryRecordEvent(key, event, sequence_, now_ns)) {
            throw std::runtime_error("TelemetryTracer rejected full-debug event");
        }
        observability::TraceEvent observed;
        if (!tracer_.TryPop(&observed)) {
            throw std::runtime_error("TelemetryTracer lost accepted event");
        }

        ++sequence_;
        ++operations_;
        operations_metric_->counter().Increment(0);
        wire_metric_->histogram().Record(encoded.size(), 0);
    }

    void Report(uint64_t elapsed_ms) {
        const SlabUsage slab = ReadSlabUsage(allocator_);
        const storage::RecorderBufferPoolStats storage = storage_pool_->stats();
        const uint64_t channel_depth = kChannelBacklog;
        const uint64_t queue_depth = channel_depth + storage.queued_records;
        queue_metric_->gauge().Set(queue_depth, 0);
        observability::TelemetrySnapshot snapshot;
        metrics_.TakeSnapshot(elapsed_ms * 1'000'000u, &snapshot);
        std::cout
            << "{\"type\":\"sample\",\"elapsed_ms\":" << elapsed_ms
            << ",\"operations\":" << operations_
            << ",\"allocator_occupied_slots\":" << slab.occupied_slots
            << ",\"allocator_total_slots\":" << allocator_.total_slot_count()
            << ",\"slab_bytes_in_use\":" << slab.occupied_bytes
            << ",\"channel_queue_depth\":" << channel_depth
            << ",\"storage_queue_depth\":" << storage.queued_records
            << ",\"queue_depth\":" << queue_depth
            << ",\"storage_bytes_in_use\":" << storage.bytes_in_use
            << ",\"storage_allocated_bytes\":" << storage.allocated_bytes
            << ",\"telemetry_accepted\":" << tracer_.accepted()
            << ",\"telemetry_dropped\":" << tracer_.dropped()
            << ",\"metric_counters\":" << snapshot.counter_count
            << "}\n";
        std::cout.flush();
    }

private:
    AlignedBytes allocator_memory_;
    AlignedBytes channel_memory_;
    CentralSlabAllocator allocator_;
    BroadcastChannel channel_;
    BroadcastChannel::SubscriberHandle subscriber_;
    std::unique_ptr<storage::RecorderBufferPool> storage_pool_;
    observability::MetricRegistry metrics_;
    observability::CounterMetric* operations_metric_ = nullptr;
    observability::GaugeMetric* queue_metric_ = nullptr;
    observability::HistogramMetric* wire_metric_ = nullptr;
    observability::TelemetryControl telemetry_control_;
    observability::TelemetryTracer<1024> tracer_;
    std::vector<ShmHandle> live_slabs_;
    size_t slab_cursor_ = 0;
    uint64_t sequence_ = 1;
    uint64_t operations_ = 0;
    uint64_t random_;
};

}  // namespace
}  // namespace mino::benchmarks::soak_probe

int main(int argc, char** argv) {
    using namespace mino::benchmarks::soak_probe;
    try {
        const Config config = ParseArguments(argc, argv);
        std::signal(SIGINT, HandleSignal);
        std::signal(SIGTERM, HandleSignal);

        Workloads workloads(config);
        const auto start = Clock::now();
        auto next_report = start;
        uint64_t yield_counter = 0;
        std::cout << "{\"type\":\"ready\",\"workloads\":[\"allocator\","
                     "\"channel\",\"bridge\",\"storage\","
                     "\"observability\"]}\n";
        workloads.Report(0);
        next_report += std::chrono::milliseconds(config.report_interval_ms);

        while (g_stop == 0) {
            workloads.Step();
            const auto now = Clock::now();
            if (now >= next_report) {
                const auto elapsed = std::chrono::duration_cast<
                    std::chrono::milliseconds>(now - start);
                workloads.Report(static_cast<uint64_t>(elapsed.count()));
                do {
                    next_report +=
                        std::chrono::milliseconds(config.report_interval_ms);
                } while (next_report <= now);
            }
            if ((yield_counter++ & 0x3ffu) == 0) {
                std::this_thread::yield();
            }
        }
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::now() - start);
        workloads.Report(static_cast<uint64_t>(elapsed.count()));
        std::cout << "{\"type\":\"stopped\",\"signal\":" << g_stop << "}\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "soak_probe: " << error.what() << '\n';
        return 2;
    }
}
