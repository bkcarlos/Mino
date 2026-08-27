// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/pipeline_comparison/mino_generated/autonomy_pipeline.generated.h"
#include "benchmarks/pipeline_comparison/pipeline_common.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <unistd.h>

#include "mino/common/ids.h"
#include "mino/common/status.h"
#include "mino/platform/shared_memory.h"
#include "mino/runtime/allocation_journal.h"
#include "mino/runtime/deadline.h"
#include "mino/runtime/message_traits.h"
#include "mino/runtime/publisher.h"
#include "mino/runtime/shm_shared_ptr.h"
#include "mino/runtime/subscriber.h"
#include "mino/shm/allocator/bitmap.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/allocator/class_table.h"
#include "mino/shm/allocator/slab_header.h"
#include "mino/shm/channel/spsc_channel.h"

namespace mino::benchmarks::pipeline {

using GeneratedAutonomyFrame = AutonomyPipelineFrame;
using GeneratedAutonomyFrameAccessor = AutonomyPipelineFrameAccessor;
using GeneratedAutonomyFrameBuilder = AutonomyPipelineFrameBuilder;
using GeneratedVariableMetadata = AutonomyPipelineFrameVariableMetadata;

static_assert(std::is_standard_layout_v<GeneratedAutonomyFrame>);
static_assert(std::is_trivially_copyable_v<GeneratedAutonomyFrame>);
static_assert(std::is_trivially_default_constructible_v<GeneratedAutonomyFrame>);
static_assert(std::is_trivially_destructible_v<GeneratedAutonomyFrame>);
static_assert(sizeof(GeneratedAutonomyFrame) ==
              GeneratedAutonomyFrame::kObjectSize);
static_assert(StaticMessageTraits<GeneratedAutonomyFrame>::index_flags ==
              kIndexSlotFlagHasChildSlabs);
namespace {

constexpr std::string_view kBackend = "mino-shm";
constexpr uint64_t kManifestMagic = 0x4D494E4F50495045ull;  // "MINOPIPE"
constexpr uint32_t kManifestVersion = 2;
constexpr uint64_t kCacheLine = 64;
constexpr uint64_t kChannelCount = 5;
constexpr uint64_t kDefaultChannelCapacity = 64;
constexpr uint64_t kMinimumChannelCapacity = 2;
constexpr uint64_t kMaximumChannelCapacity = 4096;
constexpr uint64_t kMaximumSegmentBytes = uint64_t{8} << 30;
constexpr uint64_t kNanosecondsPerSecond = 1'000'000'000ull;
constexpr uint64_t kMaximumInitialLatencyReserve = 1'000'000;

static_assert(ATOMIC_LLONG_LOCK_FREE == 2,
              "pipeline manifest requires lock-free 64-bit atomics");

enum class Operation : uint8_t { kSetup, kWorker, kCleanup };

struct BackendOptions {
    Operation operation = Operation::kWorker;
    std::string shm_name;
    uint64_t channel_capacity = kDefaultChannelCapacity;
};

struct ChannelExtent {
    uint64_t offset = 0;
    uint64_t extent = 0;
    uint64_t capacity = 0;
};

static_assert(sizeof(ChannelExtent) == 24);
static_assert(std::is_standard_layout_v<ChannelExtent>);
static_assert(std::is_trivially_copyable_v<ChannelExtent>);

struct alignas(kCacheLine) ManifestHeader {
    // Cache line 0. magic is the only publication point for the whole segment.
    std::atomic<uint64_t> magic{0};
    uint32_t version = 0;
    uint32_t header_size = 0;
    uint64_t total_size = 0;
    uint32_t profile = 0;
    uint32_t reserved0 = 0;
    uint64_t payload_bytes = 0;
    uint64_t allocator_offset = 0;
    uint64_t allocator_extent = 0;
    uint64_t frame_size = 0;

    // Cache line 1.
    uint64_t slot_count = 0;
    uint32_t channel_count = 0;
    uint32_t channel_entry_size = 0;
    uint64_t root_slot_count = 0;
    uint64_t child_slot_count = 0;
    uint64_t journal_offset = 0;
    uint64_t journal_extent = 0;
    uint64_t pin_offset = 0;
    uint64_t pin_extent = 0;

    // Cache lines 2 and 3.
    std::array<ChannelExtent, kChannelCount> channels{};
    uint64_t reserved2 = 0;
};

static_assert(sizeof(ManifestHeader) == 256);
static_assert(alignof(ManifestHeader) == kCacheLine);
static_assert(std::is_standard_layout_v<ManifestHeader>);
static_assert(offsetof(ManifestHeader, magic) == 0);
static_assert(offsetof(ManifestHeader, slot_count) == 64);
static_assert(offsetof(ManifestHeader, channels) == 128);

struct SegmentLayout {
    uint64_t total_size = 0;
    uint64_t allocator_offset = 0;
    uint64_t allocator_extent = 0;
    uint64_t slot_count = 0;
    uint64_t root_slot_count = 0;
    uint64_t child_slot_count = 0;
    uint64_t journal_offset = 0;
    uint64_t journal_extent = 0;
    uint64_t pin_offset = 0;
    uint64_t pin_extent = 0;
    std::array<ChannelExtent, kChannelCount> channels{};
};

struct RunStatistics {
    uint64_t measured_completed = 0;
    uint64_t duplicate = 0;
    uint64_t out_of_order = 0;
    uint64_t corrupt = 0;
    uint64_t first_measured_origin_ns = 0;
    uint64_t first_measured_completion_ns = 0;
    uint64_t last_measured_completion_ns = 0;
    std::vector<uint64_t> latencies_ns;
};

bool CheckedAdd(uint64_t left, uint64_t right, uint64_t* result) {
    if (result == nullptr || right > std::numeric_limits<uint64_t>::max() - left) {
        return false;
    }
    *result = left + right;
    return true;
}

bool CheckedMultiply(uint64_t left, uint64_t right, uint64_t* result) {
    if (result == nullptr ||
        (left != 0 && right > std::numeric_limits<uint64_t>::max() / left)) {
        return false;
    }
    *result = left * right;
    return true;
}

bool CheckedAlignUp(uint64_t value, uint64_t alignment, uint64_t* result) {
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) return false;
    uint64_t with_padding = 0;
    if (!CheckedAdd(value, alignment - 1, &with_padding)) return false;
    *result = with_padding & ~(alignment - 1);
    return true;
}

uint64_t AddOrThrow(uint64_t left, uint64_t right, std::string_view field) {
    uint64_t result = 0;
    if (!CheckedAdd(left, right, &result)) {
        throw std::runtime_error(std::string(field) + " overflows uint64_t");
    }
    return result;
}

uint64_t MultiplyOrThrow(uint64_t left, uint64_t right,
                         std::string_view field) {
    uint64_t result = 0;
    if (!CheckedMultiply(left, right, &result)) {
        throw std::runtime_error(std::string(field) + " overflows uint64_t");
    }
    return result;
}

uint64_t AlignOrThrow(uint64_t value, uint64_t alignment,
                      std::string_view field) {
    uint64_t result = 0;
    if (!CheckedAlignUp(value, alignment, &result)) {
        throw std::runtime_error(std::string(field) + " alignment overflows");
    }
    return result;
}

uint64_t HostPageSize() {
    const long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        throw std::runtime_error("cannot determine host page size");
    }
    const uint64_t value = static_cast<uint64_t>(page_size);
    if (value < kCacheLine || (value & (value - 1)) != 0) {
        throw std::runtime_error("host page size is not a supported power of two");
    }
    return value;
}

template <typename Frame>
SegmentLayout ComputeSegmentLayout(Profile profile,
                                   uint64_t channel_capacity) {
    if (channel_capacity < kMinimumChannelCapacity ||
        channel_capacity > kMaximumChannelCapacity ||
        (channel_capacity & (channel_capacity - 1)) != 0) {
        throw std::invalid_argument(
            "--channel-capacity must be a power of two in [2, 4096]");
    }
    if (sizeof(Frame) > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("SHM frame size exceeds allocator ABI");
    }

    const uint64_t minimum_slots = AddOrThrow(
        MultiplyOrThrow(kChannelCount, channel_capacity,
                        "allocator slots per class"),
        16, "allocator slots per class");
    const uint64_t slots_per_class = AlignOrThrow(
        minimum_slots, kBitmapShardBits, "allocator slots per class");
    const uint64_t slot_count = MultiplyOrThrow(
        slots_per_class, 2, "allocator total slot count");
    if (slot_count > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("allocator slot count exceeds uint32_t");
    }

    constexpr uint64_t kClassCount = 2;
    uint64_t metadata_size = CentralSlabAllocator::kMetadataHeaderSize;
    metadata_size = AddOrThrow(
        metadata_size,
        MultiplyOrThrow(sizeof(ClassDescriptor), kClassCount,
                        "allocator class descriptors"),
        "allocator class descriptors end");
    metadata_size = AlignOrThrow(metadata_size, alignof(std::atomic<uint64_t>),
                                 "allocator bitmap");
    const uint64_t bitmap_words =
        AddOrThrow(slot_count, kBitmapShardBits - 1, "allocator bitmap words") /
        kBitmapShardBits;
    metadata_size = AddOrThrow(
        metadata_size,
        MultiplyOrThrow(sizeof(std::atomic<uint64_t>), bitmap_words,
                        "allocator bitmap bytes"),
        "allocator bitmap end");
    metadata_size = AlignOrThrow(metadata_size, alignof(std::atomic<uint32_t>),
                                 "allocator generations");
    metadata_size = AddOrThrow(
        metadata_size,
        MultiplyOrThrow(sizeof(std::atomic<uint32_t>), slot_count,
                        "allocator generation bytes"),
        "allocator generations end");
    metadata_size = AddOrThrow(
        metadata_size,
        MultiplyOrThrow(sizeof(std::atomic<uint32_t>), kClassCount,
                        "allocator draining flags"),
        "allocator draining flags end");
    metadata_size =
        AlignOrThrow(metadata_size, alignof(SlabHeader), "allocator slot area");

    const uint64_t maximum_object_size = std::max<uint64_t>(
        sizeof(Frame), ProfilePayloadBytes(profile));
    const uint64_t slot_stride = AlignOrThrow(
        AddOrThrow(sizeof(SlabHeader), maximum_object_size,
                   "allocator slot stride"),
        alignof(SlabHeader), "allocator slot stride");
    const uint64_t allocator_extent = AlignOrThrow(
        AddOrThrow(
            metadata_size,
            MultiplyOrThrow(slot_stride, slot_count, "allocator slot bytes"),
            "allocator extent"),
        kCacheLine, "allocator extent");

    SegmentLayout layout;
    layout.allocator_offset = sizeof(ManifestHeader);
    layout.allocator_extent = allocator_extent;
    layout.slot_count = slot_count;
    layout.root_slot_count = slots_per_class;
    layout.child_slot_count = slots_per_class;

    uint64_t next = AddOrThrow(layout.allocator_offset,
                               layout.allocator_extent, "journal area offset");
    layout.journal_offset = AlignOrThrow(next, kCacheLine, "journal offset");
    layout.journal_extent = AlignOrThrow(
        AllocationJournal::RequiredSize(
            static_cast<uint32_t>(slots_per_class), 2),
        kCacheLine, "journal extent");
    next = AddOrThrow(layout.journal_offset, layout.journal_extent,
                      "pin area offset");
    layout.pin_offset = AlignOrThrow(next, kCacheLine, "pin offset");
    layout.pin_extent = AlignOrThrow(ShmPinTable::RequiredSize(), kCacheLine,
                                    "pin extent");
    next = AddOrThrow(layout.pin_offset, layout.pin_extent,
                      "channel area offset");

    const uint64_t channel_extent = SpscChannel::RequiredSize(channel_capacity);
    if (channel_extent % kCacheLine != 0) {
        throw std::runtime_error("SPSC extent is not cache-line aligned");
    }
    for (size_t edge = 0; edge < layout.channels.size(); ++edge) {
        layout.channels[edge] = ChannelExtent{
            .offset = next,
            .extent = channel_extent,
            .capacity = channel_capacity,
        };
        next = AddOrThrow(next, channel_extent, "channel extent end");
    }
    layout.total_size =
        AlignOrThrow(next, HostPageSize(), "shared-memory segment size");

    if (layout.allocator_offset % kCacheLine != 0 ||
        layout.allocator_extent % kCacheLine != 0 ||
        layout.journal_offset % kCacheLine != 0 ||
        layout.journal_extent % kCacheLine != 0 ||
        layout.pin_offset % kCacheLine != 0 ||
        layout.pin_extent % kCacheLine != 0 ||
        layout.total_size % kCacheLine != 0) {
        throw std::runtime_error(
            "computed segment extents are not 64-byte aligned");
    }
    if (layout.total_size > kMaximumSegmentBytes) {
        throw std::runtime_error(
            "computed SHM segment exceeds the 8 GiB benchmark safety limit");
    }
    if (layout.total_size > std::numeric_limits<size_t>::max()) {
        throw std::runtime_error("computed SHM segment exceeds size_t");
    }
    return layout;
}

std::optional<size_t> InputEdge(Role role) {
    switch (role) {
        case Role::kPerception: return std::nullopt;
        case Role::kPrediction: return 0;
        case Role::kPlanning: return 1;
        case Role::kControl: return 2;
        case Role::kGuardian: return 3;
        case Role::kCanbus: return 4;
    }
    throw std::invalid_argument("invalid pipeline role");
}

std::optional<size_t> OutputEdge(Role role) {
    switch (role) {
        case Role::kPerception: return 0;
        case Role::kPrediction: return 1;
        case Role::kPlanning: return 2;
        case Role::kControl: return 3;
        case Role::kGuardian: return 4;
        case Role::kCanbus: return std::nullopt;
    }
    throw std::invalid_argument("invalid pipeline role");
}

void ValidateShmName(std::string_view name) {
    if (name.size() < 2 || name.size() > 200 || name.front() != '/') {
        throw std::invalid_argument(
            "--shm-name must begin with '/' and contain 1..199 token bytes");
    }
    const unsigned char first = static_cast<unsigned char>(name[1]);
    if ((first < 'A' || first > 'Z') && (first < 'a' || first > 'z') &&
        (first < '0' || first > '9')) {
        throw std::invalid_argument(
            "--shm-name token must begin with an ASCII alphanumeric byte");
    }
    for (size_t index = 1; index < name.size(); ++index) {
        const unsigned char value = static_cast<unsigned char>(name[index]);
        const bool alphanumeric =
            (value >= 'A' && value <= 'Z') ||
            (value >= 'a' && value <= 'z') ||
            (value >= '0' && value <= '9');
        if (!alphanumeric && value != '_' && value != '-' && value != '.') {
            throw std::invalid_argument(
                "--shm-name may contain only ASCII alphanumeric, '_', '-', and '.'");
        }
    }
}

std::string_view OptionValue(int argc, char** argv, int* index,
                             std::string_view option) {
    const std::string_view argument(argv[*index]);
    if (argument == option) {
        if (*index + 1 >= argc || argv[*index + 1] == nullptr) {
            throw std::invalid_argument(std::string(option) + " requires a value");
        }
        ++*index;
        return argv[*index];
    }
    return argument.substr(option.size() + 1);
}

BackendOptions ParseBackendOptions(int argc, char** argv) {
    BackendOptions options;
    bool operation_seen = false;
    bool name_seen = false;
    bool capacity_seen = false;
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            throw std::invalid_argument("argv contains a null argument");
        }
        const std::string_view argument(argv[index]);
        if (argument == "--operation" || argument.starts_with("--operation=")) {
            if (operation_seen) {
                throw std::invalid_argument("--operation may be specified only once");
            }
            operation_seen = true;
            const std::string_view value =
                OptionValue(argc, argv, &index, "--operation");
            if (value == "setup") {
                options.operation = Operation::kSetup;
            } else if (value == "worker") {
                options.operation = Operation::kWorker;
            } else if (value == "cleanup") {
                options.operation = Operation::kCleanup;
            } else {
                throw std::invalid_argument(
                    "--operation must be setup, worker, or cleanup");
            }
        } else if (argument == "--shm-name" ||
                   argument.starts_with("--shm-name=")) {
            if (name_seen) {
                throw std::invalid_argument("--shm-name may be specified only once");
            }
            name_seen = true;
            options.shm_name =
                OptionValue(argc, argv, &index, "--shm-name");
        } else if (argument == "--channel-capacity" ||
                   argument.starts_with("--channel-capacity=")) {
            if (capacity_seen) {
                throw std::invalid_argument(
                    "--channel-capacity may be specified only once");
            }
            capacity_seen = true;
            const std::string_view value =
                OptionValue(argc, argv, &index, "--channel-capacity");
            uint64_t parsed = 0;
            const auto conversion = std::from_chars(
                value.data(), value.data() + value.size(), parsed);
            if (value.empty() || conversion.ec != std::errc{} ||
                conversion.ptr != value.data() + value.size() ||
                parsed < kMinimumChannelCapacity ||
                parsed > kMaximumChannelCapacity ||
                (parsed & (parsed - 1)) != 0) {
                throw std::invalid_argument(
                    "--channel-capacity must be a power of two in [2, 4096]");
            }
            options.channel_capacity = parsed;
        }
    }
    if (!name_seen) throw std::invalid_argument("--shm-name is required");
    ValidateShmName(options.shm_name);
    return options;
}

uint64_t AbsoluteDeadline(const CommonOptions& options) {
    const uint64_t now = NowNs();
    const uint64_t duration =
        MultiplyOrThrow(options.deadline_seconds, kNanosecondsPerSecond,
                        "deadline duration");
    return AddOrThrow(now, duration, "absolute deadline");
}

Deadline ProcessingDeadline(const CommonOptions& options) {
    return Deadline::FromNow(std::chrono::seconds(options.deadline_seconds));
}

void ThrowStatus(std::string_view operation, const Status& status) {
    throw std::runtime_error(std::string(operation) + ": " + status.ToString());
}

template <typename T>
T TakeOrThrow(std::string_view operation, Result<T>&& result) {
    if (!result.ok()) ThrowStatus(operation, result.status());
    return std::move(result).value();
}

bool RangeWithin(uint64_t offset, uint64_t extent, uint64_t total) {
    uint64_t end = 0;
    return offset % kCacheLine == 0 && extent % kCacheLine == 0 &&
           CheckedAdd(offset, extent, &end) && end <= total;
}

template <typename Frame>
const ManifestHeader& ValidateManifest(const SharedMemorySegment& segment,
                                       Profile profile,
                                       uint64_t channel_capacity) {
    if (segment.base() == nullptr || segment.size() < sizeof(ManifestHeader) ||
        reinterpret_cast<uintptr_t>(segment.base()) % kCacheLine != 0) {
        throw std::runtime_error("shared-memory mapping cannot contain manifest");
    }
    const auto* header = static_cast<const ManifestHeader*>(segment.base());
    if (header->magic.load(std::memory_order_acquire) != kManifestMagic) {
        throw std::runtime_error("SHM pipeline manifest magic mismatch");
    }
    if (header->version != kManifestVersion ||
        header->header_size != sizeof(ManifestHeader)) {
        throw std::runtime_error("SHM pipeline manifest version/size mismatch");
    }
    if (header->reserved0 != 0 || header->reserved2 != 0) {
        throw std::runtime_error(
            "SHM pipeline manifest reserved fields are nonzero");
    }
    if (header->total_size != segment.size()) {
        throw std::runtime_error("SHM pipeline manifest total-size mismatch");
    }
    if (header->profile != static_cast<uint32_t>(profile)) {
        throw std::runtime_error("SHM pipeline manifest profile mismatch");
    }
    if (header->payload_bytes != ProfilePayloadBytes(profile)) {
        throw std::runtime_error("SHM pipeline manifest payload-size mismatch");
    }
    if (header->frame_size != sizeof(Frame)) {
        throw std::runtime_error("SHM pipeline manifest frame-size mismatch");
    }
    if (header->channel_count != kChannelCount ||
        header->channel_entry_size != sizeof(ChannelExtent)) {
        throw std::runtime_error("SHM pipeline manifest channel-shape mismatch");
    }
    if (!RangeWithin(header->allocator_offset, header->allocator_extent,
                     header->total_size)) {
        throw std::runtime_error("SHM pipeline allocator extent is invalid");
    }
    if (!RangeWithin(header->journal_offset, header->journal_extent,
                     header->total_size)) {
        throw std::runtime_error("SHM pipeline journal extent is invalid");
    }
    if (!RangeWithin(header->pin_offset, header->pin_extent,
                     header->total_size)) {
        throw std::runtime_error("SHM pipeline pin-table extent is invalid");
    }
    for (const ChannelExtent& channel : header->channels) {
        if (!RangeWithin(channel.offset, channel.extent, header->total_size) ||
            channel.capacity != channel_capacity) {
            throw std::runtime_error("SHM pipeline channel extent is invalid");
        }
    }

    const SegmentLayout expected =
        ComputeSegmentLayout<Frame>(profile, channel_capacity);
    if (header->total_size != expected.total_size ||
        header->allocator_offset != expected.allocator_offset ||
        header->allocator_extent != expected.allocator_extent ||
        header->slot_count != expected.slot_count ||
        header->root_slot_count != expected.root_slot_count ||
        header->child_slot_count != expected.child_slot_count ||
        header->journal_offset != expected.journal_offset ||
        header->journal_extent != expected.journal_extent ||
        header->pin_offset != expected.pin_offset ||
        header->pin_extent != expected.pin_extent) {
        throw std::runtime_error("SHM pipeline allocator manifest is not canonical");
    }
    for (size_t edge = 0; edge < expected.channels.size(); ++edge) {
        const ChannelExtent& actual = header->channels[edge];
        const ChannelExtent& wanted = expected.channels[edge];
        if (actual.offset != wanted.offset || actual.extent != wanted.extent ||
            actual.capacity != wanted.capacity) {
            throw std::runtime_error("SHM pipeline channel manifest is not canonical");
        }
    }
    return *header;
}

template <typename Frame>
void PopulateManifest(ManifestHeader* header, Profile profile,
                      const SegmentLayout& layout) {
    header->version = kManifestVersion;
    header->header_size = sizeof(ManifestHeader);
    header->total_size = layout.total_size;
    header->profile = static_cast<uint32_t>(profile);
    header->payload_bytes = ProfilePayloadBytes(profile);
    header->allocator_offset = layout.allocator_offset;
    header->allocator_extent = layout.allocator_extent;
    header->frame_size = sizeof(Frame);
    header->slot_count = layout.slot_count;
    header->channel_count = kChannelCount;
    header->channel_entry_size = sizeof(ChannelExtent);
    header->root_slot_count = layout.root_slot_count;
    header->child_slot_count = layout.child_slot_count;
    header->journal_offset = layout.journal_offset;
    header->journal_extent = layout.journal_extent;
    header->pin_offset = layout.pin_offset;
    header->pin_extent = layout.pin_extent;
    header->channels = layout.channels;
}

template <typename Frame>
void Setup(const CommonOptions& common, const BackendOptions& backend) {
    const SegmentLayout layout =
        ComputeSegmentLayout<Frame>(common.profile, backend.channel_capacity);
    const Status stale = SharedMemorySegment::Unlink(backend.shm_name);
    if (!stale.ok() && stale.code() != StatusCode::kNotFound) {
        ThrowStatus("unlink stale shared-memory segment", stale);
    }

    SharedMemorySegment segment = TakeOrThrow(
        "create shared-memory segment",
        SharedMemorySegment::Create(backend.shm_name, layout.total_size));
    std::memset(segment.base(), 0, static_cast<size_t>(segment.size()));

    auto* header = std::construct_at(
        static_cast<ManifestHeader*>(segment.base()));
    PopulateManifest<Frame>(header, common.profile, layout);

    auto* bytes = static_cast<std::byte*>(segment.base());
    ClassTableConfig allocator_config;
    allocator_config.classes = {
        {
            .slot_size = static_cast<uint32_t>(sizeof(Frame)),
            .slot_count = static_cast<uint32_t>(layout.root_slot_count),
        },
        {
            .slot_size = static_cast<uint32_t>(
                ProfilePayloadBytes(common.profile)),
            .slot_count = static_cast<uint32_t>(layout.child_slot_count),
        },
    };
    Result<CentralSlabAllocator> allocator = CentralSlabAllocator::Create(
        bytes + layout.allocator_offset, layout.allocator_extent,
        allocator_config);
    if (!allocator.ok()) {
        ThrowStatus("initialize CentralSlabAllocator", allocator.status());
    }
    Result<AllocationJournal> journal = AllocationJournal::Init(
        bytes + layout.journal_offset,
        static_cast<size_t>(layout.journal_extent),
        static_cast<uint32_t>(layout.root_slot_count), 2, *allocator);
    if (!journal.ok()) {
        ThrowStatus("initialize AllocationJournal", journal.status());
    }
    Result<ShmPinTable> pins = ShmPinTable::Init(
        bytes + layout.pin_offset, static_cast<size_t>(layout.pin_extent),
        *allocator);
    if (!pins.ok()) {
        ThrowStatus("initialize ShmPinTable", pins.status());
    }

    for (size_t edge = 0; edge < layout.channels.size(); ++edge) {
        Result<SpscChannel> channel = SpscChannel::Init(
            bytes + layout.channels[edge].offset,
            layout.channels[edge].capacity);
        if (!channel.ok()) {
            ThrowStatus("initialize SpscChannel edge " + std::to_string(edge),
                        channel.status());
        }
    }

    header->magic.store(kManifestMagic, std::memory_order_release);
    const Status close = segment.Close();
    if (!close.ok()) ThrowStatus("close initialized shared-memory segment", close);
}

void Cleanup(const BackendOptions& backend) {
    const Status status = SharedMemorySegment::Unlink(backend.shm_name);
    if (!status.ok()) ThrowStatus("unlink shared-memory segment", status);
}

template <typename Frame>
void PopulateGeneratedFrame(const SemanticFrame& source,
                            MessageBuilder<Frame>* destination) {
    if (destination == nullptr || !destination->active()) {
        throw std::invalid_argument(
            "generated SHM message builder must be active");
    }
    if (source.payload.empty() ||
        source.payload.size() > kLargePayloadBytes ||
        source.payload.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error(
            "semantic payload does not fit the generated Mino schema");
    }

    GeneratedAutonomyFrameBuilder builder(**destination);
    builder.set_sample_id(source.sample_id);
    builder.set_origin_timestamp_ns(source.origin_timestamp_ns);
    builder.set_perception_timestamp_ns(source.perception_timestamp_ns);
    builder.set_prediction_timestamp_ns(source.prediction_timestamp_ns);
    builder.set_planning_timestamp_ns(source.planning_timestamp_ns);
    builder.set_control_timestamp_ns(source.control_timestamp_ns);
    builder.set_guardian_timestamp_ns(source.guardian_timestamp_ns);
    builder.set_completed_stage_mask(source.completed_stage_mask);
    builder.set_profile(source.profile);
    builder.set_object_count(source.object_count);
    builder.set_trajectory_point_count(source.trajectory_point_count);
    builder.set_ego_speed_mps(source.ego_speed_mps);
    builder.set_steering_angle_rad(source.steering_angle_rad);
    builder.set_acceleration_mps2(source.acceleration_mps2);
    builder.set_brake_percentage(source.brake_percentage);
    builder.set_emergency_stop(source.emergency_stop);
    builder.set_payload_checksum(source.payload_checksum);

    AllocationRequest child_request;
    child_request.object_size = static_cast<uint32_t>(source.payload.size());
    child_request.type_id = StaticMessageTraits<Frame>::type_id;
    child_request.schema = {
        .short_id = StaticMessageTraits<Frame>::schema_short_id,
        .layout_version = StaticMessageTraits<Frame>::layout_version,
    };
    child_request.alignment = 1;
    Result<MutableBuildView> child =
        destination->AllocateChild(child_request);
    if (!child.ok()) ThrowStatus("allocate generated payload child", child.status());
    if (child->data == nullptr ||
        child->object_size != source.payload.size() ||
        child->capacity < source.payload.size()) {
        throw std::runtime_error(
            "allocator returned an invalid generated payload child");
    }
    std::memcpy(child->data, source.payload.data(), source.payload.size());
    const bool metadata_set = builder.set_payload(GeneratedVariableMetadata{
        .offset = child->handle.offset,
        .generation = child->handle.generation,
        .region_id = child->handle.region_id,
        .length = source.payload.size(),
        .capacity = source.payload.size(),
        .element_size = 1,
    });
    if (!metadata_set) {
        throw std::runtime_error(
            "generated Mino builder rejected payload child metadata");
    }
}

template <typename Frame>
SemanticFrame GeneratedScalarsToSemantic(const Frame& source) {
    const GeneratedAutonomyFrameAccessor accessor(source);
    if (!accessor.valid()) {
        throw std::runtime_error("generated Mino root object is invalid");
    }
    SemanticFrame destination;
    destination.sample_id = accessor.sample_id();
    destination.origin_timestamp_ns = accessor.origin_timestamp_ns();
    destination.perception_timestamp_ns = accessor.perception_timestamp_ns();
    destination.prediction_timestamp_ns = accessor.prediction_timestamp_ns();
    destination.planning_timestamp_ns = accessor.planning_timestamp_ns();
    destination.control_timestamp_ns = accessor.control_timestamp_ns();
    destination.guardian_timestamp_ns = accessor.guardian_timestamp_ns();
    destination.completed_stage_mask = accessor.completed_stage_mask();
    destination.profile = accessor.profile();
    destination.object_count = accessor.object_count();
    destination.trajectory_point_count = accessor.trajectory_point_count();
    destination.ego_speed_mps = accessor.ego_speed_mps();
    destination.steering_angle_rad = accessor.steering_angle_rad();
    destination.acceleration_mps2 = accessor.acceleration_mps2();
    destination.brake_percentage = accessor.brake_percentage();
    destination.emergency_stop = accessor.emergency_stop();
    destination.payload_checksum = accessor.payload_checksum();
    return destination;
}

template <typename Frame>
std::span<const uint8_t> InspectGeneratedPayload(
    const Frame& source, ShmHandle root_handle,
    const CentralSlabAllocator& allocator, Profile expected_profile) {
    const GeneratedAutonomyFrameAccessor accessor(source);
    if (!accessor.valid()) {
        throw std::runtime_error("generated Mino root object is invalid");
    }
    const GeneratedVariableMetadata payload = accessor.payload();
    const size_t expected_payload_bytes =
        ProfilePayloadBytes(expected_profile);
    if (payload.length != expected_payload_bytes ||
        payload.capacity != expected_payload_bytes ||
        payload.element_size != 1 || payload.offset == 0) {
        throw std::runtime_error(
            "generated Mino payload metadata does not match profile");
    }
    const ShmHandle child_handle{
        .offset = payload.offset,
        .generation = payload.generation,
        .region_id = payload.region_id,
    };
    Result<SlabView> root = allocator.Inspect(root_handle);
    if (!root.ok()) ThrowStatus("inspect generated root", root.status());
    Result<SlabView> child = allocator.Inspect(child_handle);
    if (!child.ok()) ThrowStatus("inspect generated payload child", child.status());
    if (root->state != ObjectState::kPublished ||
        (root->allocation_flags & kAllocationFlagTransactionRoot) == 0 ||
        child->state != ObjectState::kPublished ||
        (child->allocation_flags & kAllocationFlagTransactionChild) == 0 ||
        child->type_id != StaticMessageTraits<Frame>::type_id ||
        child->schema_short_id != StaticMessageTraits<Frame>::schema_short_id ||
        child->layout_version != StaticMessageTraits<Frame>::layout_version ||
        child->owner_epoch != root->owner_epoch ||
        child->allocation_transaction_id != root->allocation_transaction_id ||
        child->object_size != payload.capacity ||
        child->capacity < child->object_size || child->data == nullptr) {
        throw std::runtime_error(
            "generated Mino payload child does not belong to the root graph");
    }
    const auto* payload_bytes = static_cast<const uint8_t*>(child->data);
    return std::span<const uint8_t>(payload_bytes, payload.length);
}

template <typename Frame>
void WriteGeneratedScalars(const SemanticFrame& source, Frame* destination,
                           const GeneratedVariableMetadata& payload_meta) {
    if (destination == nullptr) {
        throw std::invalid_argument("generated SHM frame must not be null");
    }
    // Codegen Builder zeros the whole root. Restore child-handle metadata so
    // the hop keeps the published payload slab without copying its bytes.
    GeneratedAutonomyFrameBuilder builder(*destination);
    builder.set_sample_id(source.sample_id);
    builder.set_origin_timestamp_ns(source.origin_timestamp_ns);
    builder.set_perception_timestamp_ns(source.perception_timestamp_ns);
    builder.set_prediction_timestamp_ns(source.prediction_timestamp_ns);
    builder.set_planning_timestamp_ns(source.planning_timestamp_ns);
    builder.set_control_timestamp_ns(source.control_timestamp_ns);
    builder.set_guardian_timestamp_ns(source.guardian_timestamp_ns);
    builder.set_completed_stage_mask(source.completed_stage_mask);
    builder.set_profile(source.profile);
    builder.set_object_count(source.object_count);
    builder.set_trajectory_point_count(source.trajectory_point_count);
    builder.set_ego_speed_mps(source.ego_speed_mps);
    builder.set_steering_angle_rad(source.steering_angle_rad);
    builder.set_acceleration_mps2(source.acceleration_mps2);
    builder.set_brake_percentage(source.brake_percentage);
    builder.set_emergency_stop(source.emergency_stop);
    builder.set_payload_checksum(source.payload_checksum);
    if (!builder.set_payload(payload_meta)) {
        throw std::runtime_error(
            "generated Mino builder rejected restored payload child metadata");
    }
}

template <typename Frame>
SemanticFrame GeneratedToSemantic(const Frame& source, ShmHandle root_handle,
                                  const CentralSlabAllocator& allocator,
                                  Profile expected_profile) {
    SemanticFrame destination = GeneratedScalarsToSemantic(source);
    const std::span<const uint8_t> payload = InspectGeneratedPayload(
        source, root_handle, allocator, expected_profile);
    destination.payload.assign(payload.begin(), payload.end());
    return destination;
}

uint64_t TotalFrames(const CommonOptions& options) {
    return AddOrThrow(options.warmup_messages, options.messages,
                      "total pipeline frames");
}

void ValidateSequenceAndPhase(const CommonOptions& options,
                              const SemanticFrame& frame, uint64_t expected_id,
                              RunStatistics* statistics) {
    if (frame.sample_id != expected_id) {
        if (frame.sample_id < expected_id) {
            ++statistics->duplicate;
            throw std::runtime_error(
                "duplicate sample_id: expected " + std::to_string(expected_id) +
                ", got " + std::to_string(frame.sample_id));
        }
        ++statistics->out_of_order;
        throw std::runtime_error(
            "out-of-order sample_id: expected " +
            std::to_string(expected_id) + ", got " +
            std::to_string(frame.sample_id));
    }
    const bool measured = expected_id >= options.warmup_messages;
    if ((measured && frame.origin_timestamp_ns == 0) ||
        (!measured && frame.origin_timestamp_ns != 0)) {
        ++statistics->corrupt;
        throw std::runtime_error(
            measured ? "measured frame has a zero origin timestamp"
                     : "warmup frame has a non-zero origin timestamp");
    }
    if (frame.profile != static_cast<uint32_t>(options.profile)) {
        ++statistics->corrupt;
        throw std::runtime_error("frame profile does not match --profile");
    }
}

template <typename Frame>
void PublishExclusiveBounded(Publisher<Frame>* publisher,
                             ExclusiveMessage<Frame> exclusive,
                             Deadline deadline) {
    if (deadline.expired()) {
        throw std::runtime_error("deadline expired before exclusive SHM publish");
    }
    const Status published =
        publisher->PublishLocal(std::move(exclusive), deadline);
    if (!published.ok()) {
        ThrowStatus("Publisher::PublishLocal exclusive", published);
    }
}

template <typename Frame>
void PublishBounded(Publisher<Frame>* publisher,
                    const SemanticFrame& semantic, Deadline deadline) {
    if (deadline.expired()) {
        throw std::runtime_error("deadline expired before SHM publish");
    }
    Result<MessageBuilder<Frame>> allocated = publisher->Allocate(deadline);
    if (!allocated.ok()) ThrowStatus("Publisher::Allocate", allocated.status());
    PopulateGeneratedFrame(semantic, &*allocated);
    const Status published =
        publisher->PublishLocal(std::move(*allocated), deadline);
    if (!published.ok()) {
        ThrowStatus("Publisher::PublishLocal", published);
    }
}

template <typename Frame>
BorrowedMessage<Frame> PollBounded(Subscriber<Frame>* subscriber,
                                   Deadline deadline,
                                   RunStatistics* statistics) {
    Result<BorrowedMessage<Frame>> result = subscriber->Poll(deadline);
    if (!result.ok()) {
        if (result.status().code() == StatusCode::kCorruption ||
            result.status().code() == StatusCode::kSchemaMismatch) {
            ++statistics->corrupt;
        }
        ThrowStatus("Subscriber::Poll", result.status());
    }
    return std::move(*result);
}

template <typename Frame>
void RunSource(const CommonOptions& options, CentralSlabAllocator* allocator,
               AllocationJournal* journal, SpscChannel* output,
               Deadline deadline, uint64_t absolute_deadline_ns,
               RunStatistics* statistics) {
    Publisher<Frame> publisher(
        *allocator, *output, 1, *journal, ProcessIdentity::Current(),
        PublisherOptions{.queue_full_policy = QueueFullPolicy::kBlock});
    const uint64_t total = TotalFrames(options);
    const uint64_t schedule_start_ns = NowNs();
    for (uint64_t sample_id = 0; sample_id < total; ++sample_id) {
        PaceSource(schedule_start_ns, sample_id, options.publish_interval_us,
                   absolute_deadline_ns);
        const bool measured = sample_id >= options.warmup_messages;
        SemanticFrame semantic =
            InitializeSourceFrame(sample_id, options.profile, measured);
        std::string error;
        if (!ApplyStageForClockMode(Role::kPerception, &semantic,
                                    options.clock_mode, &error)) {
            ++statistics->corrupt;
            throw std::runtime_error(
                "perception stage rejected source frame: " + error);
        }
        PublishBounded(&publisher, semantic, deadline);
        if (measured) ++statistics->measured_completed;
    }
}

template <typename Frame>
void RunForwarder(const CommonOptions& options,
                  CentralSlabAllocator* allocator, AllocationJournal* journal,
                  SpscChannel* input, SpscChannel* output, size_t output_edge,
                  Deadline deadline, RunStatistics* statistics) {
    // The SPSC borrow remains active through graph resolution and publication;
    // no SHM pointer escapes this scope. Supplying the optional pin table here
    // would add a full PinCount scan to every Ack without extending lifetime.
    Subscriber<Frame> subscriber(*allocator, *input);
    Publisher<Frame> publisher(
        *allocator, *output, output_edge + 1, *journal,
        ProcessIdentity::Current(),
        PublisherOptions{.queue_full_policy = QueueFullPolicy::kBlock});
    const uint64_t total = TotalFrames(options);
    for (uint64_t expected_id = 0; expected_id < total; ++expected_id) {
        BorrowedMessage<Frame> borrowed =
            PollBounded(&subscriber, deadline, statistics);
        // Exclusive SPSC transfer keeps the published graph intact. TakeExclusive
        // ACKs the input slot without reclaiming; PublishLocal republishes the same
        // root and child handles. Only root scalars are rewritten.
        SemanticFrame semantic = GeneratedScalarsToSemantic(*borrowed);
        const std::span<const uint8_t> payload = InspectGeneratedPayload(
            *borrowed, borrowed.metadata().payload, *allocator, options.profile);
        ValidateSequenceAndPhase(options, semantic, expected_id, statistics);
        std::string error;
        if (!ApplyStageForClockMode(options.role, &semantic, payload,
                                    options.clock_mode, &error)) {
            ++statistics->corrupt;
            throw std::runtime_error(std::string(RoleName(options.role)) +
                                     " stage rejected frame: " + error);
        }
        const GeneratedVariableMetadata payload_meta =
            GeneratedAutonomyFrameAccessor(*borrowed).payload();
        Result<ExclusiveMessage<Frame>> exclusive =
            std::move(borrowed).TakeExclusive();
        if (!exclusive.ok()) {
            ThrowStatus("BorrowedMessage::TakeExclusive", exclusive.status());
        }
        WriteGeneratedScalars(semantic, exclusive->get(), payload_meta);
        PublishExclusiveBounded(&publisher, std::move(*exclusive), deadline);
        if (expected_id >= options.warmup_messages) {
            ++statistics->measured_completed;
        }
    }
}

template <typename Frame>
void RunSink(const CommonOptions& options, CentralSlabAllocator* allocator,
             SpscChannel* input, Deadline deadline,
             RunStatistics* statistics) {
    // Synchronous SPSC consumption is already protected by the active borrow;
    // an additional runtime Pin would only add per-message pin-table scans.
    Subscriber<Frame> subscriber(*allocator, *input);
    statistics->latencies_ns.reserve(static_cast<size_t>(
        std::min(options.messages, kMaximumInitialLatencyReserve)));
    const uint64_t total = TotalFrames(options);
    for (uint64_t expected_id = 0; expected_id < total; ++expected_id) {
        BorrowedMessage<Frame> borrowed =
            PollBounded(&subscriber, deadline, statistics);
        SemanticFrame semantic = GeneratedToSemantic(
            *borrowed, borrowed.metadata().payload, *allocator, options.profile);
        ValidateSequenceAndPhase(options, semantic, expected_id, statistics);
        std::string error;
        if (!ApplyStageForClockMode(Role::kCanbus, &semantic,
                                    options.clock_mode, &error)) {
            ++statistics->corrupt;
            throw std::runtime_error("canbus stage rejected frame: " + error);
        }

        const bool measured = expected_id >= options.warmup_messages;
        const Status ack = std::move(borrowed).Ack();
        if (!ack.ok()) ThrowStatus("Subscriber sink Ack", ack);
        if (!measured) continue;

        const uint64_t completion_ns = NowNs();
        if (statistics->measured_completed == 0) {
            statistics->first_measured_origin_ns = semantic.origin_timestamp_ns;
            statistics->first_measured_completion_ns = completion_ns;
        }
        statistics->last_measured_completion_ns = completion_ns;
        if (options.clock_mode == ClockMode::kSameHost) {
            if (completion_ns < semantic.origin_timestamp_ns) {
                ++statistics->corrupt;
                throw std::runtime_error(
                    "sink completion timestamp precedes frame origin");
            }
            statistics->latencies_ns.push_back(
                completion_ns - semantic.origin_timestamp_ns);
        }
        ++statistics->measured_completed;
    }
}

std::string BackendDetails(uint64_t capacity, uint64_t segment_bytes,
                           uint64_t frame_bytes, uint64_t slot_count,
                           ClockMode clock_mode) {
    return "{\"manifest\":\"benchmark_static\","
           "\"allocator\":\"CentralSlabAllocator\","
           "\"channel\":\"SpscChannel\","
           "\"typed_runtime\":\"Publisher<T>/Subscriber<T>\","
           "\"queue_full_policy\":\"deadline_bounded_block\","
           "\"capacity\":" + std::to_string(capacity) +
           ",\"channel_count\":5,\"segment_bytes\":" +
           std::to_string(segment_bytes) + ",\"frame_bytes\":" +
           std::to_string(frame_bytes) + ",\"allocator_slot_count\":" +
           std::to_string(slot_count) +
           ",\"transport\":\"zero-copy-handle-transfer\","
           "\"compilation_mode\":\"" + std::string(CompilationMode()) +
           "\",\"synchronous_spsc_borrow_pin\":false,"
           "\"clock_mode\":\"" + std::string(ClockModeName(clock_mode)) +
           "\",\"one_way_latency_valid\":" +
           (clock_mode == ClockMode::kSameHost ? "true" : "false") +
           ",\"stage_processing\":\"exclusive-spsc-graph-transfer\"}";
}

template <typename Frame>
void RunWorker(const CommonOptions& options, const BackendOptions& backend,
               RunStatistics* statistics) {
    const uint64_t absolute_deadline_ns = AbsoluteDeadline(options);
    const Deadline deadline = ProcessingDeadline(options);
    SharedMemorySegment segment = TakeOrThrow(
        "open shared-memory segment",
        SharedMemorySegment::Open(backend.shm_name, /*read_only=*/false));
    const ManifestHeader& header = ValidateManifest<Frame>(
        segment, options.profile, backend.channel_capacity);
    auto* bytes = static_cast<std::byte*>(segment.base());
    CentralSlabAllocator allocator = TakeOrThrow(
        "attach CentralSlabAllocator",
        CentralSlabAllocator::Attach(bytes + header.allocator_offset,
                                     header.allocator_extent));
    AllocationJournal journal = TakeOrThrow(
        "attach AllocationJournal",
        AllocationJournal::Attach(
            bytes + header.journal_offset,
            static_cast<size_t>(header.journal_extent), allocator));


    std::optional<SpscChannel> input;
    std::optional<SpscChannel> output;
    const std::optional<size_t> input_edge = InputEdge(options.role);
    if (input_edge.has_value()) {
        input.emplace(TakeOrThrow(
            "attach input SpscChannel",
            SpscChannel::Attach(bytes + header.channels[*input_edge].offset)));
        if (input->capacity() != backend.channel_capacity) {
            throw std::runtime_error("attached input SPSC capacity mismatch");
        }
    }
    const std::optional<size_t> output_edge = OutputEdge(options.role);
    if (output_edge.has_value()) {
        output.emplace(TakeOrThrow(
            "attach output SpscChannel",
            SpscChannel::Attach(bytes + header.channels[*output_edge].offset)));
        if (output->capacity() != backend.channel_capacity) {
            throw std::runtime_error("attached output SPSC capacity mismatch");
        }
    }

    WriteReadyFile(options.runtime_dir, kBackend, options.role, options.run_id);
    if (!WaitForStartFile(options.runtime_dir, options.run_id,
                          absolute_deadline_ns)) {
        throw std::runtime_error("deadline expired waiting for start file");
    }

    switch (options.role) {
        case Role::kPerception:
            RunSource<Frame>(options, &allocator, &journal, &*output, deadline,
                             absolute_deadline_ns, statistics);
            break;
        case Role::kPrediction:
        case Role::kPlanning:
        case Role::kControl:
        case Role::kGuardian:
            RunForwarder<Frame>(options, &allocator, &journal, &*input, &*output,
                                *output_edge, deadline, statistics);
            break;
        case Role::kCanbus:
            RunSink<Frame>(options, &allocator, &*input, deadline, statistics);
            break;
    }
}

void PopulateResult(const CommonOptions& options,
                    const RunStatistics& statistics, uint64_t frame_size,
                    bool success, SinkResult* result) {
    result->counts.offered = options.messages;
    result->counts.received = statistics.measured_completed;
    result->counts.duplicate = statistics.duplicate;
    result->counts.out_of_order = statistics.out_of_order;
    result->counts.corrupt = statistics.corrupt;
    result->counts.lost =
        statistics.measured_completed < options.messages
            ? options.messages - statistics.measured_completed
            : 0;
    result->encoded_bytes = frame_size;
    if (success) {
        result->counts.received = options.messages;
        result->counts.lost = 0;
    }
    if (options.role != Role::kCanbus) return;

    result->latency_ns = Summarize(statistics.latencies_ns);
    if (options.clock_mode == ClockMode::kIndependentHosts) {
        if (statistics.measured_completed > 1 &&
            statistics.first_measured_completion_ns != 0 &&
            statistics.last_measured_completion_ns >
                statistics.first_measured_completion_ns) {
            result->elapsed_ns = statistics.last_measured_completion_ns -
                                 statistics.first_measured_completion_ns;
            result->throughput_messages_per_second =
                static_cast<double>(statistics.measured_completed - 1) *
                static_cast<double>(kNanosecondsPerSecond) /
                static_cast<double>(result->elapsed_ns);
        }
        return;
    }
    if (statistics.first_measured_origin_ns != 0 &&
        statistics.last_measured_completion_ns >=
            statistics.first_measured_origin_ns) {
        result->elapsed_ns = statistics.last_measured_completion_ns -
                             statistics.first_measured_origin_ns;
        if (result->elapsed_ns != 0) {
            result->throughput_messages_per_second =
                static_cast<double>(statistics.measured_completed) *
                static_cast<double>(kNanosecondsPerSecond) /
                static_cast<double>(result->elapsed_ns);
        }
    }
}

void WriteResultBestEffort(const SinkResult& result) noexcept {
    try {
        WriteSinkResult(result);
    } catch (const std::exception& exception) {
        std::cerr << "failed to write result artifact " << result.options.output
                  << ": " << exception.what() << '\n';
    } catch (...) {
        std::cerr << "failed to write result artifact " << result.options.output
                  << ": unknown exception\n";
    }
}

template <typename Frame>
int RunProfile(const CommonOptions& common, const BackendOptions& backend,
               RunStatistics* statistics, SinkResult* result) {
    if (backend.operation == Operation::kCleanup) {
        Cleanup(backend);
        return 0;
    }

    const SegmentLayout layout =
        ComputeSegmentLayout<Frame>(common.profile, backend.channel_capacity);
    const uint64_t allocated_bytes =
        sizeof(Frame) + ProfilePayloadBytes(common.profile);
    result->encoded_bytes = allocated_bytes;
    result->backend_details =
        BackendDetails(backend.channel_capacity, layout.total_size,
                       sizeof(Frame), layout.slot_count, common.clock_mode);
    switch (backend.operation) {
        case Operation::kSetup:
            Setup<Frame>(common, backend);
            return 0;
        case Operation::kCleanup:
            throw std::logic_error("cleanup dispatch was not handled");
        case Operation::kWorker:
            RunWorker<Frame>(common, backend, statistics);
            if (statistics->measured_completed != common.messages) {
                throw std::runtime_error(
                    "completed measured-frame count mismatch");
            }
            PopulateResult(common, *statistics, allocated_bytes, true, result);
            WriteSinkResult(*result);
            return 0;
    }
    throw std::logic_error("invalid operation");
}

int PipelineMain(int argc, char** argv) {
    std::optional<CommonOptions> parsed_common;
    std::optional<BackendOptions> parsed_backend;
    RunStatistics statistics;
    SinkResult result;
    uint64_t frame_size = 0;
    try {
        parsed_common = ParseCommonOptions(argc, argv);
        parsed_backend = ParseBackendOptions(argc, argv);
        const CommonOptions& common = *parsed_common;
        const BackendOptions& backend = *parsed_backend;
        result.backend = std::string(kBackend);
        result.options = common;
        result.payload_bytes = ProfilePayloadBytes(common.profile);

        frame_size =
            sizeof(GeneratedAutonomyFrame) + ProfilePayloadBytes(common.profile);
        return RunProfile<GeneratedAutonomyFrame>(
            common, backend, &statistics, &result);
    } catch (const std::exception& exception) {
        std::cerr << kBackend << " pipeline failed: " << exception.what() << '\n';
        if (parsed_common.has_value()) {
            result.backend = std::string(kBackend);
            result.options = *parsed_common;
            result.payload_bytes = ProfilePayloadBytes(result.options.profile);
            PopulateResult(result.options, statistics, frame_size, false, &result);
            result.outcome = "failure";
            result.error = exception.what();
            WriteResultBestEffort(result);
        }
        return 1;
    } catch (...) {
        std::cerr << kBackend << " pipeline failed: unknown exception\n";
        if (parsed_common.has_value()) {
            result.backend = std::string(kBackend);
            result.options = *parsed_common;
            result.payload_bytes = ProfilePayloadBytes(result.options.profile);
            PopulateResult(result.options, statistics, frame_size, false, &result);
            result.outcome = "failure";
            result.error = "unknown exception";
            WriteResultBestEffort(result);
        }
        return 1;
    }
}

}  // namespace
}  // namespace mino::benchmarks::pipeline

int main(int argc, char** argv) {
    return mino::benchmarks::pipeline::PipelineMain(argc, argv);
}
