// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_WIRE_FRAME_H_
#define MINO_BRIDGE_WIRE_FRAME_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "mino/common/result.h"

namespace mino::bridge {

// All integer fields, including the stream prefix and CRC values, use network
// byte order (big-endian). No C++ object representation is sent on the wire.
inline constexpr uint32_t kWireFrameMagic = 0x4d494e4fu;  // "MINO"
inline constexpr uint16_t kWireProtocolVersion = 1;
inline constexpr uint32_t kLengthPrefixSize = 4;

// Wire v1 is exactly the 80-byte base header specified by detailed design
// section 16.2. No extra field, padding, process pointer, ShmHandle, or region
// offset exists between these documented fields.
inline constexpr uint32_t kWireBaseHeaderLength = 80;
inline constexpr uint32_t kWirePayloadCrcLength = 4;
inline constexpr uint32_t kWireControlOpcodeLength = 4;
inline constexpr uint32_t kWirePerfTraceContextLength = 40;
inline constexpr uint32_t kWireMaximumHeaderLength =
    kWireBaseHeaderLength + kWirePayloadCrcLength +
    kWirePerfTraceContextLength;

// Logical frame type. Data is represented solely by CONTROL_FRAME=false.
// Non-data values are stable canonical control opcodes encoded as an explicit
// 4-byte big-endian prefix of the wire payload, never as a header field.
enum class FrameType : uint32_t {
    kData = 0,
    kSchemaAnnounce = 1,
    kSchemaRequest = 2,
    kAck = 3,
    kHeartbeat = 4,
    kSessionHello = 5,
    // Consumed only by the connection owner before BridgePipeline is bound.
    // Older codecs reject this unknown opcode, preserving fail-closed rollout.
    kSessionDiscovery = 6,
};

enum class FrameFlag : uint16_t {
    kPayloadCrcPresent = 1u << 0,
    kAeadPresent = 1u << 1,
    kCompressed = 1u << 2,
    kControlFrame = 1u << 3,
    kPerfTraceSampled = 1u << 4,
};

inline constexpr uint16_t kKnownFrameFlags = 0x001fu;

constexpr uint16_t FlagValue(FrameFlag flag) noexcept {
    return static_cast<uint16_t>(flag);
}

constexpr bool HasFrameFlag(uint16_t flags, FrameFlag flag) noexcept {
    return (flags & FlagValue(flag)) != 0;
}

struct PerfTraceContext {
    uint64_t trace_id_high = 0;
    uint64_t trace_id_low = 0;
    uint32_t sample_flags = 0;
    uint32_t clock_domain_id = 0;
    uint64_t origin_wall_time_ns = 0;
    uint64_t origin_monotonic_ns = 0;

    bool operator==(const PerfTraceContext&) const = default;
};

// Logical header fields only. header_length, payload_length, header_crc and the
// optional payload CRC are canonical derived wire fields and therefore cannot
// be supplied inconsistently by callers.
struct WireFrameHeader {
    FrameType frame_type = FrameType::kData;
    uint16_t flags = 0;
    uint32_t topic_id = 0;
    uint32_t msg_type = 0;
    uint32_t connection_schema_ref = 0;
    uint32_t schema_version = 0;
    uint32_t layout_version = 0;
    uint64_t source_node_id = 0;
    uint64_t source_publisher_id = 0;
    uint64_t source_publisher_epoch = 0;
    uint64_t sequence_num = 0;
    uint64_t timestamp_ns = 0;
    std::optional<PerfTraceContext> perf_trace;

    bool operator==(const WireFrameHeader&) const = default;
};

struct WireFrame {
    WireFrameHeader header;
    std::vector<std::byte> payload;

    bool operator==(const WireFrame&) const = default;
};

struct WireFrameLimits {
    // Maximum value of the header payload_length field. For control frames it
    // includes the 4-byte canonical control opcode prefix. Payload CRC, when
    // present, covers exactly these payload_length wire bytes.
    uint32_t max_payload_length = 16u * 1024u * 1024u;

    // Maximum retained bytes for one partial stream frame, including its
    // 4-byte length prefix. The decoder grows storage only as bytes arrive and
    // never buffers a subsequent frame while the current frame is incomplete.
    size_t max_buffered_bytes = static_cast<size_t>(kLengthPrefixSize) +
                                kWireMaximumHeaderLength +
                                16u * 1024u * 1024u;

    // Per-Push limits. Decoded payload excludes the control opcode. Work bytes
    // count bytes consumed from the Push input plus complete frame bodies
    // revisited for validation, CRC, and decoded-payload copying.
    size_t max_frames_per_push = 1024;
    size_t max_decoded_payload_bytes_per_push = 64u * 1024u * 1024u;
    size_t max_work_bytes_per_push = 64u * 1024u * 1024u;
};

class WireFrameCodec {
public:
    // Validates the same inputs as Encode and returns the exact frame-body size.
    // Successful validation performs no allocation, payload copying, or CRC
    // calculation. The header-only overload is useful when a caller knows a
    // canonical payload's size without materializing its bytes.
    static Result<size_t> EncodedSize(
        const WireFrame& frame, const WireFrameLimits& limits = {}) noexcept;
    static Result<size_t> EncodedSize(
        const WireFrameHeader& header, size_t payload_size,
        const WireFrameLimits& limits = {}) noexcept;

    // Encodes/decodes exactly one frame body. Decode rejects extra trailing
    // bytes. The length-prefixed form is for byte-stream transports.
    static Result<std::vector<std::byte>> Encode(
        const WireFrame& frame, const WireFrameLimits& limits = {}) noexcept;
    static Result<std::vector<std::byte>> EncodeLengthPrefixed(
        const WireFrame& frame, const WireFrameLimits& limits = {}) noexcept;
    static Result<WireFrame> Decode(
        std::span<const std::byte> frame_body,
        const WireFrameLimits& limits = {}) noexcept;
};

// Incremental decoder for a sequence of 4-byte-big-endian-length-prefixed
// frames. Push accepts arbitrary partial reads and may return zero or many
// frames. A malformed input makes the decoder failed until Reset(), preventing
// accidental resynchronization inside attacker-controlled bytes.
class LengthPrefixedFrameDecoder {
public:
    explicit LengthPrefixedFrameDecoder(WireFrameLimits limits = {}) noexcept
        : limits_(limits) {}

    Result<std::vector<WireFrame>> Push(
        std::span<const std::byte> bytes) noexcept;

    // Signals end-of-stream. An incomplete prefix or frame is corruption.
    Status Finish() noexcept;
    void Reset() noexcept;
    bool failed() const noexcept { return failed_; }
    size_t buffered_bytes() const noexcept {
        return prefix_size_ + frame_buffer_.size();
    }
    // Test/telemetry observation for slowloris and retention behavior. This is
    // capacity only; it does not expose or permit mutation of buffered bytes.
    size_t retained_capacity() const noexcept {
        return frame_buffer_.capacity();
    }

private:
    Result<std::vector<WireFrame>> Fail(Status status) noexcept;
    void ReleaseFrameBuffer() noexcept;

    WireFrameLimits limits_;
    std::array<std::byte, kLengthPrefixSize> prefix_{};
    size_t prefix_size_ = 0;
    std::vector<std::byte> frame_buffer_;
    size_t expected_frame_length_ = 0;
    bool failed_ = false;
};

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_WIRE_FRAME_H_
