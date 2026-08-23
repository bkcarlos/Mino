// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/pipeline_comparison/mino_generated/autonomy_pipeline.generated.h"
#include "benchmarks/pipeline_comparison/pipeline_common.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "mino/bridge/wire_frame.h"
#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/shared_memory.h"
#include "mino/runtime/allocation_journal.h"
#include "mino/runtime/deadline.h"
#include "mino/runtime/message_traits.h"
#include "mino/runtime/publisher.h"
#include "mino/runtime/shm_shared_ptr.h"
#include "mino/runtime/subscriber.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/dynamic_value.h"
#include "mino/schema/wire.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/allocator/slab_header.h"
#include "mino/shm/channel/spsc_channel.h"
#include "mino/transport/tcp_driver.h"
#include "mino/transport/transport_driver.h"

namespace mino::benchmarks::pipeline {
namespace {

using Frame = AutonomyPipelineFrame;
using FrameAccessor = AutonomyPipelineFrameAccessor;
using FrameBuilder = AutonomyPipelineFrameBuilder;
using VariableMetadata = AutonomyPipelineFrameVariableMetadata;
using bridge::FlagValue;
using bridge::FrameFlag;
using bridge::FrameType;
using bridge::WireFrame;
using bridge::WireFrameCodec;
using bridge::WireFrameLimits;
using schema::DynamicMessage;
using schema::DynamicValue;
using schema::PreparedCanonicalWireCodec;
using schema::SchemaDescriptor;
using transport::ConnectionId;
using transport::EndpointDescriptor;
using transport::TcpDriver;
using transport::TcpDriverOptions;

static_assert(std::is_standard_layout_v<Frame>);
static_assert(std::is_trivially_copyable_v<Frame>);
static_assert(std::is_trivially_default_constructible_v<Frame>);
static_assert(std::is_trivially_destructible_v<Frame>);
static_assert(sizeof(Frame) == Frame::kObjectSize);
static_assert(StaticMessageTraits<Frame>::index_flags ==
              kIndexSlotFlagHasChildSlabs);

constexpr std::string_view kBridgeToken = "mino-shm-tcp-bridge";
constexpr std::string_view kSchemaName =
    "mino.benchmarks.pipeline.AutonomyPipelineFrame";
constexpr uint64_t kManifestMagic = 0x4D494E4F50495045ull;  // "MINOPIPE"
constexpr uint32_t kManifestVersion = 2;
constexpr uint64_t kCacheLine = 64;
constexpr size_t kChannelCount = 5;
constexpr uint64_t kMinimumChannelCapacity = 2;
constexpr uint64_t kMaximumChannelCapacity = 4096;
constexpr uint64_t kNanosecondsPerSecond = 1'000'000'000ull;
constexpr uint32_t kMaximumFrameBodyBytes = 2u * 1024u * 1024u;
constexpr size_t kMaximumArtifactBytes = 16u * 1024u * 1024u;
constexpr uint64_t kCompletionMagic = 0x4D494E4F42524944ull;  // "MINOBRID"

static_assert(ATOMIC_LLONG_LOCK_FREE == 2,
              "pipeline bridge manifest requires lock-free 64-bit atomics");

enum class Mode : uint8_t { kSource, kSink };

struct Options {
    Mode mode = Mode::kSource;
    Profile profile = Profile::kSmall;
    uint64_t messages = 0;
    uint64_t warmup_messages = 0;
    uint64_t deadline_seconds = 0;
    std::string run_id;
    std::filesystem::path runtime_dir;
    std::string shm_name;
    size_t edge = 0;
    std::string listen_address;
    std::string peer_address;
    uint16_t port = 0;
    std::filesystem::path descriptor;
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

    uint64_t slot_count = 0;
    uint32_t channel_count = 0;
    uint32_t channel_entry_size = 0;
    uint64_t root_slot_count = 0;
    uint64_t child_slot_count = 0;
    uint64_t journal_offset = 0;
    uint64_t journal_extent = 0;
    uint64_t pin_offset = 0;
    uint64_t pin_extent = 0;

    std::array<ChannelExtent, kChannelCount> channels{};
    uint64_t reserved2 = 0;
};

static_assert(sizeof(ManifestHeader) == 256);
static_assert(alignof(ManifestHeader) == kCacheLine);
static_assert(std::is_standard_layout_v<ManifestHeader>);
static_assert(offsetof(ManifestHeader, magic) == 0);
static_assert(offsetof(ManifestHeader, slot_count) == 64);
static_assert(offsetof(ManifestHeader, channels) == 128);

std::string_view ModeName(Mode mode) {
    return mode == Mode::kSource ? "source" : "sink";
}

std::string StatusToken(Mode mode) {
    return std::string(kBridgeToken) + "-" + std::string(ModeName(mode));
}

[[noreturn]] void UsageError(std::string_view message) {
    throw std::invalid_argument(
        std::string(message) +
        "\nusage: mino_shm_tcp_bridge --mode=source|sink --profile=small|medium|large "
        "--messages=N --warmup-messages=N --run-id=ID --runtime-dir=DIR "
        "--shm-name=/NAME --edge=0..4 --port=PORT "
        "--schema-descriptor=PATH --deadline-seconds=N "
        "[--peer-address=IPv4] [--listen-address=IPv4]");
}

uint64_t ParseUnsigned(std::string_view value, std::string_view option) {
    uint64_t parsed = 0;
    const auto result =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (value.empty() || result.ec != std::errc{} ||
        result.ptr != value.data() + value.size()) {
        UsageError(std::string(option) + " requires an unsigned integer");
    }
    return parsed;
}

std::optional<std::string_view> TakeOptionValue(int* index, int argc,
                                                char** argv,
                                                std::string_view option) {
    const std::string_view argument(argv[*index]);
    if (argument == option) {
        if (*index + 1 >= argc || argv[*index + 1] == nullptr ||
            std::string_view(argv[*index + 1]).empty()) {
            UsageError(std::string(option) + " requires a non-empty value");
        }
        return std::string_view(argv[++(*index)]);
    }
    if (argument.size() > option.size() && argument.starts_with(option) &&
        argument[option.size()] == '=') {
        const std::string_view value = argument.substr(option.size() + 1);
        if (value.empty()) {
            UsageError(std::string(option) + " requires a non-empty value");
        }
        return value;
    }
    return std::nullopt;
}

void ValidateIpv4(std::string_view value, std::string_view option) {
    in_addr address{};
    const std::string text(value);
    if (inet_pton(AF_INET, text.c_str(), &address) != 1) {
        UsageError(std::string(option) + " requires a numeric IPv4 address");
    }
}

void ValidateShmName(std::string_view name) {
    if (name.size() < 2 || name.size() > 200 || name.front() != '/') {
        UsageError(
            "--shm-name must begin with '/' and contain 1..199 token bytes");
    }
    for (size_t index = 1; index < name.size(); ++index) {
        const unsigned char value = static_cast<unsigned char>(name[index]);
        const bool alphanumeric =
            (value >= 'A' && value <= 'Z') ||
            (value >= 'a' && value <= 'z') ||
            (value >= '0' && value <= '9');
        if (!alphanumeric && value != '_' && value != '-' && value != '.') {
            UsageError(
                "--shm-name may contain only ASCII alphanumeric, '_', '-', and '.'");
        }
    }
}

Options ParseOptions(int argc, char** argv) {
    if (argc < 0 || (argc > 0 && argv == nullptr)) {
        UsageError("invalid argc/argv");
    }
    Options options;
    bool mode_seen = false;
    bool profile_seen = false;
    bool messages_seen = false;
    bool warmup_seen = false;
    bool deadline_seen = false;
    bool run_seen = false;
    bool runtime_seen = false;
    bool shm_seen = false;
    bool edge_seen = false;
    bool listen_seen = false;
    bool peer_seen = false;
    bool port_seen = false;
    bool descriptor_seen = false;

    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) UsageError("argv contains a null argument");
        const std::string_view argument(argv[index]);
        if (argument == "--help" || argument == "-h") {
            UsageError("help requested");
        }
        const auto unique = [](bool* seen, std::string_view option) {
            if (*seen) UsageError(std::string(option) + " may be specified only once");
            *seen = true;
        };

        if (const auto value =
                TakeOptionValue(&index, argc, argv, "--mode")) {
            unique(&mode_seen, "--mode");
            if (*value == "source") {
                options.mode = Mode::kSource;
            } else if (*value == "sink") {
                options.mode = Mode::kSink;
            } else {
                UsageError("--mode must be source or sink");
            }
            continue;
        }
        if (const auto value =
                TakeOptionValue(&index, argc, argv, "--profile")) {
            unique(&profile_seen, "--profile");
            const std::optional<Profile> profile = ParseProfile(*value);
            if (!profile.has_value()) {
                UsageError("--profile must be small, medium, or large");
            }
            options.profile = *profile;
            continue;
        }
        if (const auto value =
                TakeOptionValue(&index, argc, argv, "--messages")) {
            unique(&messages_seen, "--messages");
            options.messages = ParseUnsigned(*value, "--messages");
            continue;
        }
        if (const auto value = TakeOptionValue(
                &index, argc, argv, "--warmup-messages")) {
            unique(&warmup_seen, "--warmup-messages");
            options.warmup_messages =
                ParseUnsigned(*value, "--warmup-messages");
            continue;
        }
        if (const auto value = TakeOptionValue(
                &index, argc, argv, "--deadline-seconds")) {
            unique(&deadline_seen, "--deadline-seconds");
            options.deadline_seconds =
                ParseUnsigned(*value, "--deadline-seconds");
            continue;
        }
        if (const auto value =
                TakeOptionValue(&index, argc, argv, "--run-id")) {
            unique(&run_seen, "--run-id");
            options.run_id = std::string(*value);
            continue;
        }
        if (const auto value =
                TakeOptionValue(&index, argc, argv, "--runtime-dir")) {
            unique(&runtime_seen, "--runtime-dir");
            options.runtime_dir = std::filesystem::path(*value);
            continue;
        }
        if (const auto value =
                TakeOptionValue(&index, argc, argv, "--shm-name")) {
            unique(&shm_seen, "--shm-name");
            options.shm_name = std::string(*value);
            continue;
        }
        if (const auto value =
                TakeOptionValue(&index, argc, argv, "--edge")) {
            unique(&edge_seen, "--edge");
            const uint64_t parsed = ParseUnsigned(*value, "--edge");
            if (parsed >= kChannelCount) UsageError("--edge must be in [0, 4]");
            options.edge = static_cast<size_t>(parsed);
            continue;
        }
        if (const auto value = TakeOptionValue(
                &index, argc, argv, "--listen-address")) {
            unique(&listen_seen, "--listen-address");
            ValidateIpv4(*value, "--listen-address");
            options.listen_address = std::string(*value);
            continue;
        }
        if (const auto value = TakeOptionValue(
                &index, argc, argv, "--peer-address")) {
            unique(&peer_seen, "--peer-address");
            ValidateIpv4(*value, "--peer-address");
            options.peer_address = std::string(*value);
            continue;
        }
        if (const auto value =
                TakeOptionValue(&index, argc, argv, "--port")) {
            unique(&port_seen, "--port");
            const uint64_t parsed = ParseUnsigned(*value, "--port");
            if (parsed == 0 || parsed > std::numeric_limits<uint16_t>::max()) {
                UsageError("--port must be in [1, 65535]");
            }
            options.port = static_cast<uint16_t>(parsed);
            continue;
        }
        if (const auto value = TakeOptionValue(
                &index, argc, argv, "--schema-descriptor")) {
            unique(&descriptor_seen, "--schema-descriptor");
            options.descriptor = std::filesystem::path(*value);
            continue;
        }
        UsageError("unknown argument: " + std::string(argument));
    }

    if (!mode_seen || !profile_seen || !messages_seen || !warmup_seen ||
        !deadline_seen || !run_seen || !runtime_seen || !shm_seen ||
        !edge_seen || !port_seen || !descriptor_seen) {
        UsageError("all non-address options are required");
    }
    if (options.mode == Mode::kSource && !peer_seen) {
        UsageError("source mode requires --peer-address");
    }
    if (options.mode == Mode::kSink && !listen_seen) {
        UsageError("sink mode requires --listen-address");
    }
    if (options.messages == 0 || options.messages > kMaxMessages) {
        UsageError("--messages must be in [1, 1000000000]");
    }
    if (options.warmup_messages > kMaxWarmupMessages) {
        UsageError("--warmup-messages must be in [0, 1000000000]");
    }
    if (options.deadline_seconds == 0 ||
        options.deadline_seconds > kMaxDeadlineSeconds) {
        UsageError("--deadline-seconds must be in [1, 86400]");
    }
    if (options.run_id.find('\0') != std::string::npos) {
        UsageError("--run-id must not contain NUL");
    }
    ValidateShmName(options.shm_name);
    std::error_code error;
    if (!std::filesystem::is_directory(options.runtime_dir, error) || error) {
        UsageError("--runtime-dir must name an existing directory");
    }
    return options;
}

uint64_t TotalFrames(const Options& options) {
    if (options.messages > std::numeric_limits<uint64_t>::max() -
                               options.warmup_messages) {
        throw std::runtime_error("total frame count overflows uint64_t");
    }
    return options.messages + options.warmup_messages;
}

uint64_t AbsoluteDeadline(const Options& options) {
    const uint64_t now = NowNs();
    if (options.deadline_seconds >
        (std::numeric_limits<uint64_t>::max() - now) /
            kNanosecondsPerSecond) {
        throw std::runtime_error("absolute deadline overflows uint64_t");
    }
    return now + options.deadline_seconds * kNanosecondsPerSecond;
}

Deadline RuntimeDeadline(uint64_t absolute_deadline_ns) {
    const uint64_t now = NowNs();
    if (now >= absolute_deadline_ns) {
        throw std::runtime_error("bridge deadline has already expired");
    }
    return Deadline::FromNow(
        std::chrono::nanoseconds(absolute_deadline_ns - now));
}

uint32_t RemainingMs(uint64_t deadline_ns, uint32_t maximum = 60'000) {
    const uint64_t now = NowNs();
    if (now >= deadline_ns) return 0;
    const uint64_t remaining_ns = deadline_ns - now;
    const uint64_t rounded = (remaining_ns + 999'999u) / 1'000'000u;
    return static_cast<uint32_t>(
        std::min<uint64_t>(std::max<uint64_t>(rounded, 1), maximum));
}

uint64_t StableRunHash(std::string_view value) {
    constexpr uint64_t kOffset = 14'695'981'039'346'656'037ull;
    constexpr uint64_t kPrime = 1'099'511'628'211ull;
    uint64_t hash = kOffset;
    for (char byte : value) {
        hash ^= static_cast<uint8_t>(byte);
        hash *= kPrime;
    }
    return hash == 0 ? 1 : hash;
}

void ThrowStatus(std::string_view operation, const Status& status) {
    throw std::runtime_error(std::string(operation) + ": " + status.ToString());
}

template <typename T>
T TakeOrThrow(std::string_view operation, Result<T>&& result) {
    if (!result.ok()) ThrowStatus(operation, result.status());
    return std::move(result).value();
}

bool CheckedAdd(uint64_t left, uint64_t right, uint64_t* result) {
    if (result == nullptr || right > std::numeric_limits<uint64_t>::max() - left) {
        return false;
    }
    *result = left + right;
    return true;
}

uint64_t AlignUp(uint64_t value, uint64_t alignment) {
    if (alignment == 0 || (alignment & (alignment - 1)) != 0 ||
        value > std::numeric_limits<uint64_t>::max() - (alignment - 1)) {
        throw std::runtime_error("manifest alignment overflow");
    }
    return (value + alignment - 1) & ~(alignment - 1);
}

bool RangeWithin(uint64_t offset, uint64_t extent, uint64_t total) {
    uint64_t end = 0;
    return offset % kCacheLine == 0 && extent != 0 &&
           extent % kCacheLine == 0 && CheckedAdd(offset, extent, &end) &&
           end <= total;
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

const ManifestHeader& ValidateManifest(const SharedMemorySegment& segment,
                                       Profile profile) {
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
        throw std::runtime_error("SHM pipeline manifest reserved fields are nonzero");
    }
    if (header->total_size != segment.size() ||
        header->profile != static_cast<uint32_t>(profile) ||
        header->payload_bytes != ProfilePayloadBytes(profile) ||
        header->frame_size != sizeof(Frame)) {
        throw std::runtime_error("SHM pipeline manifest profile/type mismatch");
    }
    if (header->channel_count != kChannelCount ||
        header->channel_entry_size != sizeof(ChannelExtent) ||
        header->allocator_offset != sizeof(ManifestHeader) ||
        header->root_slot_count == 0 ||
        header->root_slot_count != header->child_slot_count ||
        header->slot_count !=
            header->root_slot_count + header->child_slot_count ||
        header->slot_count > std::numeric_limits<uint32_t>::max() ||
        header->root_slot_count > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("SHM pipeline manifest shape is invalid");
    }
    if (!RangeWithin(header->allocator_offset, header->allocator_extent,
                     header->total_size) ||
        !RangeWithin(header->journal_offset, header->journal_extent,
                     header->total_size) ||
        !RangeWithin(header->pin_offset, header->pin_extent,
                     header->total_size)) {
        throw std::runtime_error("SHM pipeline manifest extent is invalid");
    }

    uint64_t cursor = header->allocator_offset + header->allocator_extent;
    if (header->journal_offset != AlignUp(cursor, kCacheLine) ||
        header->journal_extent !=
            AlignUp(AllocationJournal::RequiredSize(
                        static_cast<uint32_t>(header->root_slot_count), 2),
                    kCacheLine)) {
        throw std::runtime_error("SHM pipeline journal manifest is not canonical");
    }
    cursor = header->journal_offset + header->journal_extent;
    if (header->pin_offset != AlignUp(cursor, kCacheLine) ||
        header->pin_extent !=
            AlignUp(ShmPinTable::RequiredSize(), kCacheLine)) {
        throw std::runtime_error("SHM pipeline pin manifest is not canonical");
    }
    cursor = header->pin_offset + header->pin_extent;
    uint64_t common_capacity = 0;
    for (size_t edge = 0; edge < kChannelCount; ++edge) {
        const ChannelExtent& channel = header->channels[edge];
        if (!RangeWithin(channel.offset, channel.extent, header->total_size) ||
            channel.offset != cursor ||
            channel.capacity < kMinimumChannelCapacity ||
            channel.capacity > kMaximumChannelCapacity ||
            (channel.capacity & (channel.capacity - 1)) != 0 ||
            channel.extent != SpscChannel::RequiredSize(channel.capacity)) {
            throw std::runtime_error("SHM pipeline channel manifest is invalid");
        }
        if (edge == 0) {
            common_capacity = channel.capacity;
        } else if (channel.capacity != common_capacity) {
            throw std::runtime_error("SHM pipeline channel capacities differ");
        }
        cursor = channel.offset + channel.extent;
    }
    if (header->total_size != AlignUp(cursor, HostPageSize())) {
        throw std::runtime_error("SHM pipeline total extent is not canonical");
    }
    return *header;
}

void WriteAll(int descriptor, std::string_view content) {
    size_t written = 0;
    while (written < content.size()) {
        const ssize_t result =
            write(descriptor, content.data() + written, content.size() - written);
        if (result < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "write status");
        }
        if (result == 0) throw std::runtime_error("status write returned zero");
        written += static_cast<size_t>(result);
    }
}

void AtomicWrite(const std::filesystem::path& destination,
                 std::string_view content) {
    static std::atomic<uint64_t> sequence{0};
    const std::filesystem::path temporary =
        destination.string() + ".tmp." + std::to_string(getpid()) + "." +
        std::to_string(sequence.fetch_add(1, std::memory_order_relaxed));
    const int descriptor =
        open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (descriptor < 0) {
        throw std::system_error(errno, std::generic_category(),
                                "open temporary bridge status");
    }
    bool open_descriptor = true;
    try {
        WriteAll(descriptor, content);
        if (fsync(descriptor) != 0) {
            throw std::system_error(errno, std::generic_category(),
                                    "fsync bridge status");
        }
        if (close(descriptor) != 0) {
            open_descriptor = false;
            throw std::system_error(errno, std::generic_category(),
                                    "close bridge status");
        }
        open_descriptor = false;
    } catch (...) {
        const int saved_errno = errno;
        if (open_descriptor) (void)close(descriptor);
        (void)unlink(temporary.c_str());
        errno = saved_errno;
        throw;
    }
    if (rename(temporary.c_str(), destination.c_str()) != 0) {
        const int saved_errno = errno;
        (void)unlink(temporary.c_str());
        throw std::system_error(saved_errno, std::generic_category(),
                                "rename bridge status");
    }
}

std::string OneLine(std::string_view value) {
    std::string result(value);
    for (char& byte : result) {
        if (byte == '\n' || byte == '\r' || byte == '\0') byte = ' ';
    }
    return result;
}

void WriteBridgeStatus(const Options& options, std::string_view suffix,
                       std::string_view detail = {}) {
    std::string content = options.run_id + "\n";
    if (!detail.empty()) content += OneLine(detail) + "\n";
    AtomicWrite(options.runtime_dir /
                    (StatusToken(options.mode) + "." + std::string(suffix)),
                content);
}

void WriteErrorBestEffort(const Options& options,
                          std::string_view message) noexcept {
    try {
        WriteBridgeStatus(options, "error", "error=" + OneLine(message));
    } catch (const std::exception& exception) {
        std::cerr << "failed to write bridge error status: " << exception.what()
                  << '\n';
    } catch (...) {
        std::cerr << "failed to write bridge error status: unknown exception\n";
    }
}

std::string ReadArtifact(const std::filesystem::path& path) {
    std::ifstream stream(path, std::ios::binary);
    if (!stream.good()) {
        throw std::runtime_error("cannot open schema descriptor: " +
                                 path.string());
    }
    stream.seekg(0, std::ios::end);
    const std::streamoff length = stream.tellg();
    if (length <= 0 || static_cast<uint64_t>(length) > kMaximumArtifactBytes) {
        throw std::runtime_error("schema descriptor size is invalid");
    }
    stream.seekg(0, std::ios::beg);
    std::string bytes(static_cast<size_t>(length), '\0');
    stream.read(bytes.data(), length);
    if (!stream || stream.gcount() != length) {
        throw std::runtime_error("cannot read complete schema descriptor");
    }
    return bytes;
}

class PipelineSchema final {
  public:
    explicit PipelineSchema(const std::filesystem::path& artifact_path) {
        const std::string bytes = ReadArtifact(artifact_path);
        auto artifact = schema::codegen::DecodeAndValidate(bytes);
        if (!artifact.ok()) {
            ThrowStatus("decode schema descriptor", artifact.status());
        }
        if (artifact->version != schema::codegen::kDescriptorArtifactVersion ||
            artifact->types.size() != 1 ||
            artifact->types.front().descriptor == nullptr) {
            throw std::runtime_error(
                "schema descriptor must contain exactly one current-version type");
        }
        const auto& type = artifact->types.front();
        descriptor_ = type.descriptor;
        const auto& identity = descriptor_->identity();
        if (descriptor_->aggregate().full_name() != kSchemaName ||
            descriptor_->aggregate().fields().size() != 18 ||
            identity.short_id() != StaticMessageTraits<Frame>::schema_short_id ||
            identity.schema_version() !=
                StaticMessageTraits<Frame>::schema_version ||
            identity.layout_version() !=
                StaticMessageTraits<Frame>::layout_version ||
            type.layout.layout_version != Frame::kLayoutVersion ||
            type.layout.object_size != sizeof(Frame) ||
            type.layout.object_alignment != alignof(Frame) ||
            type.layout.fields.size() != 18) {
            throw std::runtime_error(
                "schema descriptor does not exactly match generated AutonomyPipelineFrame");
        }
        const auto& digest = identity.canonical_digest();
        for (size_t index = 0; index < digest.size(); ++index) {
            if (static_cast<uint8_t>(digest[index]) != Frame::kSchemaDigest[index]) {
                throw std::runtime_error(
                    "schema descriptor digest does not match generated type");
            }
        }
        auto prepared = PreparedCanonicalWireCodec::Create(descriptor_);
        if (!prepared.ok()) {
            ThrowStatus("prepare canonical wire codec", prepared.status());
        }
        prepared_codec_.emplace(std::move(*prepared));
    }

    const SchemaDescriptor& descriptor() const noexcept { return *descriptor_; }

    std::vector<std::byte> Encode(const SemanticFrame& frame) const {
        DynamicMessage message;
        Set(message, 1, DynamicValue::Unsigned(frame.sample_id));
        Set(message, 2, DynamicValue::Unsigned(frame.origin_timestamp_ns));
        Set(message, 3,
            DynamicValue::Unsigned(frame.perception_timestamp_ns));
        Set(message, 4, DynamicValue::Unsigned(frame.prediction_timestamp_ns));
        Set(message, 5, DynamicValue::Unsigned(frame.planning_timestamp_ns));
        Set(message, 6, DynamicValue::Unsigned(frame.control_timestamp_ns));
        Set(message, 7, DynamicValue::Unsigned(frame.guardian_timestamp_ns));
        Set(message, 8, DynamicValue::Unsigned(frame.completed_stage_mask));
        Set(message, 9, DynamicValue::Unsigned(frame.profile));
        Set(message, 10, DynamicValue::Unsigned(frame.object_count));
        Set(message, 11,
            DynamicValue::Unsigned(frame.trajectory_point_count));
        Set(message, 12, DynamicValue::Float64Bits(
                             std::bit_cast<uint64_t>(frame.ego_speed_mps)));
        Set(message, 13, DynamicValue::Float64Bits(
                             std::bit_cast<uint64_t>(frame.steering_angle_rad)));
        Set(message, 14, DynamicValue::Float64Bits(
                             std::bit_cast<uint64_t>(frame.acceleration_mps2)));
        Set(message, 15, DynamicValue::Float64Bits(
                             std::bit_cast<uint64_t>(frame.brake_percentage)));
        Set(message, 16, DynamicValue::Boolean(frame.emergency_stop));
        Set(message, 17, DynamicValue::Unsigned(frame.payload_checksum));
        const auto payload = std::as_bytes(
            std::span(frame.payload.data(), frame.payload.size()));
        auto bytes = DynamicValue::Bytes(payload);
        if (!bytes.ok()) ThrowStatus("create dynamic payload", bytes.status());
        Set(message, 18, std::move(*bytes));
        return TakeOrThrow("CanonicalWireCodec::Encode",
                           prepared_codec_->Encode(message));
    }

    void Decode(std::span<const std::byte> bytes, SemanticFrame* frame) const {
        if (frame == nullptr) {
            throw std::invalid_argument("semantic decode destination is null");
        }
        DynamicMessage message = TakeOrThrow("CanonicalWireCodec::Decode",
                                             prepared_codec_->Decode(bytes));
        if (!message.unknown_fields().fields().empty() ||
            message.fields().size() != 18) {
            throw std::runtime_error(
                "canonical pipeline message has unknown or missing fields");
        }
        frame->sample_id = Unsigned(message, 1);
        frame->origin_timestamp_ns = Unsigned(message, 2);
        frame->perception_timestamp_ns = Unsigned(message, 3);
        frame->prediction_timestamp_ns = Unsigned(message, 4);
        frame->planning_timestamp_ns = Unsigned(message, 5);
        frame->control_timestamp_ns = Unsigned(message, 6);
        frame->guardian_timestamp_ns = Unsigned(message, 7);
        frame->completed_stage_mask = NarrowU32(Unsigned(message, 8), 8);
        frame->profile = NarrowU32(Unsigned(message, 9), 9);
        frame->object_count = NarrowU32(Unsigned(message, 10), 10);
        frame->trajectory_point_count =
            NarrowU32(Unsigned(message, 11), 11);
        frame->ego_speed_mps = Float64(message, 12);
        frame->steering_angle_rad = Float64(message, 13);
        frame->acceleration_mps2 = Float64(message, 14);
        frame->brake_percentage = Float64(message, 15);
        frame->emergency_stop = Boolean(message, 16);
        frame->payload_checksum = Unsigned(message, 17);
        const DynamicValue& payload = Field(message, 18);
        if (payload.bytes() == nullptr ||
            payload.bytes()->value.size() > kLargePayloadBytes) {
            throw std::runtime_error(
                "canonical payload has the wrong dynamic type or size");
        }
        const auto& payload_bytes = payload.bytes()->value;
        frame->payload.resize(payload_bytes.size());
        if (!payload_bytes.empty()) {
            std::memcpy(frame->payload.data(), payload_bytes.data(),
                        payload_bytes.size());
        }
    }

  private:
    static void Set(DynamicMessage& message, uint32_t id,
                    DynamicValue value) {
        const Status status = message.SetField(id, std::move(value));
        if (!status.ok()) ThrowStatus("set dynamic field", status);
    }

    static const DynamicValue& Field(const DynamicMessage& message,
                                     uint32_t id) {
        const DynamicValue* value = message.FindField(id);
        if (value == nullptr) {
            throw std::runtime_error("canonical message is missing field " +
                                     std::to_string(id));
        }
        return *value;
    }

    static uint64_t Unsigned(const DynamicMessage& message, uint32_t id) {
        const auto* value = Field(message, id).unsigned_integer();
        if (value == nullptr) {
            throw std::runtime_error("canonical unsigned field has wrong type");
        }
        return value->value;
    }

    static uint32_t NarrowU32(uint64_t value, uint32_t id) {
        if (value > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error("canonical uint32 field overflows: " +
                                     std::to_string(id));
        }
        return static_cast<uint32_t>(value);
    }

    static double Float64(const DynamicMessage& message, uint32_t id) {
        const auto* value = Field(message, id).float64();
        if (value == nullptr) {
            throw std::runtime_error("canonical double field has wrong type");
        }
        return std::bit_cast<double>(value->bits);
    }

    static bool Boolean(const DynamicMessage& message, uint32_t id) {
        const auto* value = Field(message, id).boolean();
        if (value == nullptr) {
            throw std::runtime_error("canonical bool field has wrong type");
        }
        return value->value;
    }

    std::shared_ptr<const SchemaDescriptor> descriptor_;
    std::optional<PreparedCanonicalWireCodec> prepared_codec_;
};

EndpointDescriptor MakeEndpoint(std::string_view address, uint16_t port) {
    in_addr parsed{};
    const std::string text(address);
    if (inet_pton(AF_INET, text.c_str(), &parsed) != 1) {
        throw std::runtime_error("invalid numeric IPv4 endpoint");
    }
    const auto bytes = std::as_bytes(std::span(&parsed, size_t{1}));
    return TakeOrThrow("EndpointDescriptor::Ipv4Tcp",
                       EndpointDescriptor::Ipv4Tcp(bytes, port));
}

void StoreU64(std::vector<std::byte>* bytes, size_t offset, uint64_t value) {
    for (size_t index = 0; index < 8; ++index) {
        (*bytes)[offset + index] =
            static_cast<std::byte>(value >> ((7 - index) * 8));
    }
}

uint64_t LoadU64(std::span<const std::byte> bytes, size_t offset) {
    uint64_t value = 0;
    for (size_t index = 0; index < 8; ++index) {
        value = (value << 8) | static_cast<uint8_t>(bytes[offset + index]);
    }
    return value;
}

class BridgeTransport final {
  public:
    BridgeTransport(const Options& options, const PipelineSchema& schema,
                    uint64_t deadline_ns)
        : options_(options),
          schema_(schema),
          run_hash_(StableRunHash(options.run_id)) {
        try {
            Initialize(deadline_ns);
        } catch (...) {
            CloseBestEffort();
            throw;
        }
    }

    BridgeTransport(const BridgeTransport&) = delete;
    BridgeTransport& operator=(const BridgeTransport&) = delete;
    ~BridgeTransport() { CloseBestEffort(); }

    void SendData(const SemanticFrame& semantic, uint64_t deadline_ns) {
        WireFrame frame;
        PopulateIdentityHeader(&frame);
        frame.header.frame_type = FrameType::kData;
        frame.header.flags = FlagValue(FrameFlag::kPayloadCrcPresent);
        frame.header.sequence_num = semantic.sample_id + 1;
        frame.header.timestamp_ns = semantic.origin_timestamp_ns;
        frame.payload = schema_.Encode(semantic);
        std::vector<std::byte> body = TakeOrThrow(
            "WireFrameCodec::Encode", WireFrameCodec::Encode(frame, limits_));
        Send(body, deadline_ns, transport::UntrackedTrafficClass::kData);
    }

    void ReceiveData(uint64_t expected_id, uint64_t deadline_ns,
                     SemanticFrame* semantic) {
        std::vector<std::byte> body = Receive(deadline_ns);
        WireFrame frame = TakeOrThrow(
            "WireFrameCodec::Decode", WireFrameCodec::Decode(body, limits_));
        ValidateIdentityHeader(frame);
        if (frame.header.frame_type != FrameType::kData ||
            frame.header.flags != FlagValue(FrameFlag::kPayloadCrcPresent) ||
            frame.header.perf_trace.has_value() ||
            frame.header.sequence_num != expected_id + 1) {
            throw std::runtime_error(
                "bridge data WireFrame header does not match expected sample");
        }
        schema_.Decode(frame.payload, semantic);
        if (frame.header.timestamp_ns != semantic->origin_timestamp_ns) {
            throw std::runtime_error(
                "WireFrame timestamp does not match canonical message origin");
        }
    }

    void SendCompletion(uint64_t total_frames, uint64_t deadline_ns) {
        WireFrame completion;
        PopulateIdentityHeader(&completion);
        completion.header.frame_type = FrameType::kAck;
        completion.header.flags = FlagValue(FrameFlag::kControlFrame) |
                                  FlagValue(FrameFlag::kPayloadCrcPresent);
        completion.header.sequence_num = total_frames;
        completion.payload.resize(40);
        StoreU64(&completion.payload, 0, kCompletionMagic);
        StoreU64(&completion.payload, 8, run_hash_);
        StoreU64(&completion.payload, 16, total_frames);
        StoreU64(&completion.payload, 24, options_.edge);
        StoreU64(&completion.payload, 32,
                 static_cast<uint32_t>(options_.profile));
        std::vector<std::byte> body = TakeOrThrow(
            "encode bridge completion",
            WireFrameCodec::Encode(completion, limits_));
        Send(body, deadline_ns,
             transport::UntrackedTrafficClass::kProtocolControl);
        Flush(deadline_ns);
    }

    void ReceiveCompletion(uint64_t total_frames, uint64_t deadline_ns) {
        std::vector<std::byte> body = Receive(deadline_ns);
        WireFrame completion = TakeOrThrow(
            "decode bridge completion", WireFrameCodec::Decode(body, limits_));
        ValidateIdentityHeader(completion);
        if (completion.header.frame_type != FrameType::kAck ||
            completion.header.flags !=
                (FlagValue(FrameFlag::kControlFrame) |
                 FlagValue(FrameFlag::kPayloadCrcPresent)) ||
            completion.header.perf_trace.has_value() ||
            completion.header.sequence_num != total_frames ||
            completion.header.timestamp_ns != 0 ||
            completion.payload.size() != 40 ||
            LoadU64(completion.payload, 0) != kCompletionMagic ||
            LoadU64(completion.payload, 8) != run_hash_ ||
            LoadU64(completion.payload, 16) != total_frames ||
            LoadU64(completion.payload, 24) != options_.edge ||
            LoadU64(completion.payload, 32) !=
                static_cast<uint32_t>(options_.profile)) {
            throw std::runtime_error("bridge completion acknowledgment is invalid");
        }
    }

    void CloseOrThrow() {
        if (driver_ == nullptr) return;
        const Status status = driver_->Shutdown();
        driver_.reset();
        if (!status.ok()) ThrowStatus("TcpDriver::Shutdown", status);
    }

  private:
    void PopulateIdentityHeader(WireFrame* frame) const {
        const auto& identity = schema_.descriptor().identity();
        frame->header.topic_id = static_cast<uint32_t>(options_.edge + 1);
        frame->header.msg_type = static_cast<uint32_t>(identity.short_id());
        frame->header.connection_schema_ref = 1;
        frame->header.schema_version = identity.schema_version();
        frame->header.layout_version = identity.layout_version();
        frame->header.source_node_id = run_hash_;
        frame->header.source_publisher_id = 1;
        frame->header.source_publisher_epoch = run_hash_;
    }

    void ValidateIdentityHeader(const WireFrame& frame) const {
        const auto& identity = schema_.descriptor().identity();
        if (frame.header.topic_id != options_.edge + 1 ||
            frame.header.msg_type !=
                static_cast<uint32_t>(identity.short_id()) ||
            frame.header.connection_schema_ref != 1 ||
            frame.header.schema_version != identity.schema_version() ||
            frame.header.layout_version != identity.layout_version() ||
            frame.header.source_node_id != run_hash_ ||
            frame.header.source_publisher_id != 1 ||
            frame.header.source_publisher_epoch != run_hash_) {
            throw std::runtime_error(
                "bridge WireFrame identity does not match this schema/run/edge");
        }
    }

    void Initialize(uint64_t deadline_ns) {
        limits_.max_payload_length = kMaximumFrameBodyBytes;
        limits_.max_buffered_bytes =
            bridge::kLengthPrefixSize + bridge::kWireMaximumHeaderLength +
            kMaximumFrameBodyBytes;

        TcpDriverOptions driver_options;
        driver_options.max_frame_body_bytes = kMaximumFrameBodyBytes;
        driver_options.max_total_send_buffer_bytes = 32u * 1024u * 1024u;
        driver_options.max_connection_send_buffer_bytes = 16u * 1024u * 1024u;
        driver_options.max_ready_receive_bytes = 32u * 1024u * 1024u;
        driver_options.max_ready_receive_messages = 65'536;
        driver_options.max_pending_accepts = 16;
        driver_options.heartbeat_interval_ms = 1000;
        driver_options.idle_timeout_ms = 60'000;
        driver_options.partial_frame_timeout_ms = 10'000;
        driver_options.io_poll_max_ms = 1;
        driver_options.max_control_send_buffer_bytes = 4u * 1024u * 1024u;
        driver_options.max_control_send_messages = 1024;
        driver_ = TakeOrThrow("TcpDriver::Create",
                              TcpDriver::Create(driver_options));
        const Status started = driver_->Start({
            .max_connections = 2,
            .max_listeners = 1,
            .max_queued_sends = 65'536,
        });
        if (!started.ok()) ThrowStatus("TcpDriver::Start", started);

        if (options_.mode == Mode::kSink) {
            const EndpointDescriptor endpoint =
                MakeEndpoint(options_.listen_address, options_.port);
            auto listener = driver_->Listen({
                .local_endpoint = endpoint,
                .backlog = 4,
            });
            if (!listener.ok()) ThrowStatus("TcpDriver::Listen", listener.status());
            listener_ = listener->id;
            const uint32_t timeout = RemainingMs(deadline_ns);
            if (timeout == 0) {
                throw std::runtime_error(
                    "deadline expired accepting source bridge connection");
            }
            auto accepted = driver_->Accept({
                .listener_id = *listener_,
                .timeout_ms = timeout,
            });
            if (!accepted.ok()) ThrowStatus("TcpDriver::Accept", accepted.status());
            connection_ = accepted->id;
            return;
        }

        const EndpointDescriptor endpoint =
            MakeEndpoint(options_.peer_address, options_.port);
        for (;;) {
            const uint32_t timeout = RemainingMs(deadline_ns, 1000);
            if (timeout == 0) {
                throw std::runtime_error(
                    "deadline expired connecting to sink bridge");
            }
            auto connected = driver_->Connect({
                .remote_endpoint = endpoint,
                .local_bind = std::nullopt,
                .timeout_ms = timeout,
            });
            if (connected.ok()) {
                connection_ = connected->id;
                return;
            }
            if (connected.status().code() == StatusCode::kInvalidArgument ||
                connected.status().code() == StatusCode::kPermissionDenied ||
                connected.status().code() == StatusCode::kUnsupported) {
                ThrowStatus("TcpDriver::Connect", connected.status());
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    void Send(std::span<const std::byte> body, uint64_t deadline_ns,
              transport::UntrackedTrafficClass traffic_class) {
        if (!connection_.has_value()) {
            throw std::logic_error("bridge TCP connection is not initialized");
        }
        while (NowNs() < deadline_ns) {
            auto sent = driver_->SendUntracked({
                .connection_id = *connection_,
                .payload = body,
                .traffic_class = traffic_class,
            });
            if (sent.ok()) return;
            if (sent.status().code() != StatusCode::kWouldBlock &&
                sent.status().code() != StatusCode::kResourceExhausted) {
                ThrowStatus("TcpDriver::SendUntracked", sent.status());
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        throw std::runtime_error("deadline expired sending through bridge TCP");
    }

    std::vector<std::byte> Receive(uint64_t deadline_ns) {
        if (!connection_.has_value()) {
            throw std::logic_error("bridge TCP connection is not initialized");
        }
        while (NowNs() < deadline_ns) {
            const uint32_t timeout = RemainingMs(deadline_ns);
            if (timeout == 0) break;
            auto received = driver_->Poll({
                .max_messages = 1,
                .max_bytes = kMaximumFrameBodyBytes,
                .timeout_ms = timeout,
                .connection_id = *connection_,
            });
            if (!received.ok()) {
                if (received.status().code() == StatusCode::kTimeout ||
                    received.status().code() == StatusCode::kWouldBlock) {
                    continue;
                }
                ThrowStatus("TcpDriver::Poll", received.status());
            }
            if (received->messages.size() != 1 ||
                received->messages.front().connection_id != *connection_) {
                throw std::runtime_error(
                    "TcpDriver returned an invalid bridge receive batch");
            }
            return std::move(received->messages.front().payload);
        }
        throw std::runtime_error("deadline expired receiving through bridge TCP");
    }

    void Flush(uint64_t deadline_ns) {
        while (driver_->stats().queued_send_bytes != 0) {
            if (NowNs() >= deadline_ns) {
                throw std::runtime_error(
                    "deadline expired flushing bridge TCP completion");
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }

    void CloseBestEffort() noexcept {
        if (driver_ == nullptr) return;
        const Status status = driver_->Shutdown();
        if (!status.ok()) {
            std::cerr << "TcpDriver::Shutdown failed: " << status.ToString()
                      << '\n';
        }
        driver_.reset();
    }

    const Options& options_;
    const PipelineSchema& schema_;
    uint64_t run_hash_ = 0;
    WireFrameLimits limits_;
    std::unique_ptr<TcpDriver> driver_;
    std::optional<ConnectionId> listener_;
    std::optional<ConnectionId> connection_;
};

Role RoleAfterEdge(size_t edge) {
    constexpr std::array<Role, kChannelCount> kRoles = {
        Role::kPrediction, Role::kPlanning, Role::kControl, Role::kGuardian,
        Role::kCanbus};
    if (edge >= kRoles.size()) throw std::invalid_argument("invalid edge");
    return kRoles[edge];
}

void ValidateSampleAndPhase(const Options& options,
                            const SemanticFrame& frame,
                            uint64_t expected_id) {
    if (frame.sample_id != expected_id) {
        throw std::runtime_error(
            frame.sample_id < expected_id
                ? "duplicate sample_id: expected " + std::to_string(expected_id) +
                      ", got " + std::to_string(frame.sample_id)
                : "out-of-order sample_id: expected " +
                      std::to_string(expected_id) + ", got " +
                      std::to_string(frame.sample_id));
    }
    const bool measured = expected_id >= options.warmup_messages;
    if ((measured && frame.origin_timestamp_ns == 0) ||
        (!measured && frame.origin_timestamp_ns != 0)) {
        throw std::runtime_error(
            measured ? "measured frame has a zero origin timestamp"
                     : "warmup frame has a non-zero origin timestamp");
    }
    if (frame.profile != static_cast<uint32_t>(options.profile)) {
        throw std::runtime_error("frame profile does not match --profile");
    }
    std::string error;
    if (!ValidateFrameForStage(RoleAfterEdge(options.edge), frame, &error)) {
        throw std::runtime_error("frame sample/phase validation failed: " + error);
    }
}

void GeneratedToSemantic(const Frame& source, ShmHandle root_handle,
                         const CentralSlabAllocator& allocator,
                         Profile expected_profile, SemanticFrame* frame) {
    if (frame == nullptr) {
        throw std::invalid_argument("semantic destination is null");
    }
    const FrameAccessor accessor(source);
    if (!accessor.valid()) {
        throw std::runtime_error("generated Mino root object is invalid");
    }
    const VariableMetadata payload = accessor.payload();
    const size_t expected_payload_bytes = ProfilePayloadBytes(expected_profile);
    if (payload.length != expected_payload_bytes ||
        payload.capacity != expected_payload_bytes || payload.element_size != 1 ||
        payload.offset == 0) {
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
        child->schema_short_id !=
            StaticMessageTraits<Frame>::schema_short_id ||
        child->layout_version != StaticMessageTraits<Frame>::layout_version ||
        child->owner_epoch != root->owner_epoch ||
        child->allocation_transaction_id != root->allocation_transaction_id ||
        child->object_size != payload.capacity ||
        child->capacity < child->object_size || child->data == nullptr) {
        throw std::runtime_error(
            "generated Mino payload child does not belong to root graph");
    }

    frame->sample_id = accessor.sample_id();
    frame->origin_timestamp_ns = accessor.origin_timestamp_ns();
    frame->perception_timestamp_ns = accessor.perception_timestamp_ns();
    frame->prediction_timestamp_ns = accessor.prediction_timestamp_ns();
    frame->planning_timestamp_ns = accessor.planning_timestamp_ns();
    frame->control_timestamp_ns = accessor.control_timestamp_ns();
    frame->guardian_timestamp_ns = accessor.guardian_timestamp_ns();
    frame->completed_stage_mask = accessor.completed_stage_mask();
    frame->profile = accessor.profile();
    frame->object_count = accessor.object_count();
    frame->trajectory_point_count = accessor.trajectory_point_count();
    frame->ego_speed_mps = accessor.ego_speed_mps();
    frame->steering_angle_rad = accessor.steering_angle_rad();
    frame->acceleration_mps2 = accessor.acceleration_mps2();
    frame->brake_percentage = accessor.brake_percentage();
    frame->emergency_stop = accessor.emergency_stop();
    frame->payload_checksum = accessor.payload_checksum();
    frame->payload.resize(payload.length);
    std::memcpy(frame->payload.data(), child->data, payload.length);
}

void PopulateGeneratedFrame(const SemanticFrame& source,
                            MessageBuilder<Frame>* destination) {
    if (destination == nullptr || !destination->active()) {
        throw std::invalid_argument("generated SHM builder must be active");
    }
    if (source.payload.empty() || source.payload.size() > kLargePayloadBytes ||
        source.payload.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error(
            "semantic payload does not fit generated Mino schema");
    }

    FrameBuilder builder(**destination);
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

    AllocationRequest request;
    request.object_size = static_cast<uint32_t>(source.payload.size());
    request.type_id = StaticMessageTraits<Frame>::type_id;
    request.schema = {
        .short_id = StaticMessageTraits<Frame>::schema_short_id,
        .layout_version = StaticMessageTraits<Frame>::layout_version,
    };
    request.alignment = 1;
    Result<MutableBuildView> child = destination->AllocateChild(request);
    if (!child.ok()) ThrowStatus("allocate generated payload child", child.status());
    if (child->data == nullptr || child->object_size != source.payload.size() ||
        child->capacity < source.payload.size()) {
        throw std::runtime_error(
            "allocator returned invalid generated payload child");
    }
    std::memcpy(child->data, source.payload.data(), source.payload.size());
    if (!builder.set_payload(VariableMetadata{
            .offset = child->handle.offset,
            .generation = child->handle.generation,
            .region_id = child->handle.region_id,
            .length = source.payload.size(),
            .capacity = source.payload.size(),
            .element_size = 1,
        })) {
        throw std::runtime_error(
            "generated builder rejected payload child metadata");
    }
}

bool IsRetryable(const Status& status) {
    return status.code() == StatusCode::kResourceExhausted ||
           status.code() == StatusCode::kWouldBlock;
}

void PublishBounded(Publisher<Frame>* publisher,
                    const SemanticFrame& semantic, Deadline deadline) {
    for (;;) {
        if (deadline.expired()) {
            throw std::runtime_error("deadline expired before bridge SHM publish");
        }
        Result<MessageBuilder<Frame>> allocated = publisher->Allocate(deadline);
        if (!allocated.ok()) {
            if (IsRetryable(allocated.status())) {
                std::this_thread::yield();
                continue;
            }
            ThrowStatus("Publisher::Allocate", allocated.status());
        }
        PopulateGeneratedFrame(semantic, &*allocated);
        const Status published =
            publisher->PublishLocal(std::move(*allocated), deadline);
        if (published.ok()) return;
        if (IsRetryable(published)) {
            std::this_thread::yield();
            continue;
        }
        ThrowStatus("Publisher::PublishLocal", published);
    }
}

void RunSourceBridge(const Options& options, BridgeTransport* transport,
                     uint64_t deadline_ns) {
    SharedMemorySegment segment = TakeOrThrow(
        "open source shared-memory segment",
        SharedMemorySegment::Open(options.shm_name, /*read_only=*/false));
    const ManifestHeader& header = ValidateManifest(segment, options.profile);
    auto* bytes = static_cast<std::byte*>(segment.base());
    CentralSlabAllocator allocator = TakeOrThrow(
        "attach source CentralSlabAllocator",
        CentralSlabAllocator::Attach(bytes + header.allocator_offset,
                                     header.allocator_extent));
    if (allocator.class_count() != 2 ||
        allocator.total_slot_count() != header.slot_count) {
        throw std::runtime_error("source allocator shape disagrees with manifest");
    }
    SpscChannel channel = TakeOrThrow(
        "attach source SpscChannel",
        SpscChannel::Attach(bytes + header.channels[options.edge].offset));
    if (channel.capacity() != header.channels[options.edge].capacity) {
        throw std::runtime_error("source SPSC capacity disagrees with manifest");
    }
    // Encoding and ACK complete before the borrow leaves this iteration, so the
    // SPSC borrow itself protects the graph and no optional Pin is required.
    Subscriber<Frame> subscriber(allocator, channel);

    WriteBridgeStatus(options, "ready");
    if (!WaitForStartFile(options.runtime_dir, options.run_id, deadline_ns)) {
        throw std::runtime_error("deadline expired waiting for start file");
    }
    const Deadline runtime_deadline = RuntimeDeadline(deadline_ns);
    const uint64_t total = TotalFrames(options);
    SemanticFrame semantic;
    semantic.payload.reserve(ProfilePayloadBytes(options.profile));
    for (uint64_t expected_id = 0; expected_id < total; ++expected_id) {
        Result<BorrowedMessage<Frame>> polled =
            subscriber.Poll(runtime_deadline);
        if (!polled.ok()) ThrowStatus("Subscriber::Poll", polled.status());
        BorrowedMessage<Frame> borrowed = std::move(*polled);
        GeneratedToSemantic(*borrowed, borrowed.metadata().payload, allocator,
                            options.profile, &semantic);
        ValidateSampleAndPhase(options, semantic, expected_id);
        transport->SendData(semantic, deadline_ns);
        const Status ack = std::move(borrowed).Ack();
        if (!ack.ok()) ThrowStatus("Subscriber source Ack", ack);
    }
    transport->ReceiveCompletion(total, deadline_ns);
}

void RunSinkBridge(const Options& options, BridgeTransport* transport,
                   uint64_t deadline_ns) {
    SharedMemorySegment segment = TakeOrThrow(
        "open sink shared-memory segment",
        SharedMemorySegment::Open(options.shm_name, /*read_only=*/false));
    const ManifestHeader& header = ValidateManifest(segment, options.profile);
    auto* bytes = static_cast<std::byte*>(segment.base());
    CentralSlabAllocator allocator = TakeOrThrow(
        "attach sink CentralSlabAllocator",
        CentralSlabAllocator::Attach(bytes + header.allocator_offset,
                                     header.allocator_extent));
    if (allocator.class_count() != 2 ||
        allocator.total_slot_count() != header.slot_count) {
        throw std::runtime_error("sink allocator shape disagrees with manifest");
    }
    AllocationJournal journal = TakeOrThrow(
        "attach sink AllocationJournal",
        AllocationJournal::Attach(bytes + header.journal_offset,
                                  static_cast<size_t>(header.journal_extent),
                                  allocator));
    SpscChannel channel = TakeOrThrow(
        "attach sink SpscChannel",
        SpscChannel::Attach(bytes + header.channels[options.edge].offset));
    if (channel.capacity() != header.channels[options.edge].capacity) {
        throw std::runtime_error("sink SPSC capacity disagrees with manifest");
    }
    Publisher<Frame> publisher(
        allocator, channel, options.edge + 1, journal,
        ProcessIdentity::Current(),
        PublisherOptions{.queue_full_policy = QueueFullPolicy::kFail});

    WriteBridgeStatus(options, "ready");
    if (!WaitForStartFile(options.runtime_dir, options.run_id, deadline_ns)) {
        throw std::runtime_error("deadline expired waiting for start file");
    }
    const Deadline runtime_deadline = RuntimeDeadline(deadline_ns);
    const uint64_t total = TotalFrames(options);
    SemanticFrame semantic;
    semantic.payload.reserve(ProfilePayloadBytes(options.profile));
    for (uint64_t expected_id = 0; expected_id < total; ++expected_id) {
        transport->ReceiveData(expected_id, deadline_ns, &semantic);
        ValidateSampleAndPhase(options, semantic, expected_id);
        PublishBounded(&publisher, semantic, runtime_deadline);
    }
    transport->SendCompletion(total, deadline_ns);
}

int BridgeMain(int argc, char** argv) {
    std::optional<Options> parsed_options;
    try {
        parsed_options = ParseOptions(argc, argv);
        const Options& options = *parsed_options;
        const uint64_t deadline_ns = AbsoluteDeadline(options);
        PipelineSchema schema(options.descriptor);
        BridgeTransport transport(options, schema, deadline_ns);
        if (options.mode == Mode::kSource) {
            RunSourceBridge(options, &transport, deadline_ns);
        } else {
            RunSinkBridge(options, &transport, deadline_ns);
        }
        transport.CloseOrThrow();
        WriteBridgeStatus(options, "done",
                          "completed=" + std::to_string(TotalFrames(options)));
        std::cout << StatusToken(options.mode) << " completed "
                  << TotalFrames(options) << " frames compilation_mode="
                  << CompilationMode() << '\n';
        return 0;
    } catch (const std::exception& exception) {
        std::cerr << kBridgeToken << " failed: " << exception.what() << '\n';
        if (parsed_options.has_value()) {
            WriteErrorBestEffort(*parsed_options, exception.what());
        }
        return 1;
    } catch (...) {
        std::cerr << kBridgeToken << " failed: unknown exception\n";
        if (parsed_options.has_value()) {
            WriteErrorBestEffort(*parsed_options, "unknown exception");
        }
        return 1;
    }
}

}  // namespace
}  // namespace mino::benchmarks::pipeline

int main(int argc, char** argv) {
    return mino::benchmarks::pipeline::BridgeMain(argc, argv);
}
