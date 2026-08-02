// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/wire_frame.h"

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <string_view>
#include <utility>

#include "mino/bridge/crc32c.h"
#include "mino/common/status.h"

namespace mino::bridge {
namespace {

constexpr size_t kMagicOffset = 0;
constexpr size_t kVersionOffset = 4;
constexpr size_t kFlagsOffset = 6;
constexpr size_t kHeaderLengthOffset = 8;
constexpr size_t kTopicIdOffset = 12;
constexpr size_t kMsgTypeOffset = 16;
constexpr size_t kConnectionSchemaRefOffset = 20;
constexpr size_t kSchemaVersionOffset = 24;
constexpr size_t kLayoutVersionOffset = 28;
constexpr size_t kSourceNodeIdOffset = 32;
constexpr size_t kSourcePublisherIdOffset = 40;
constexpr size_t kSourcePublisherEpochOffset = 48;
constexpr size_t kSequenceNumOffset = 56;
constexpr size_t kTimestampNsOffset = 64;
constexpr size_t kPayloadLengthOffset = 72;
constexpr size_t kHeaderCrcOffset = 76;
constexpr size_t kOptionalHeaderOffset = 80;

Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Resource(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

Status Unsupported(std::string_view message) {
    return Status::Error(StatusCode::kUnsupported, message);
}

void WriteBe16(std::span<std::byte> bytes, size_t offset,
               uint16_t value) noexcept {
    bytes[offset] = static_cast<std::byte>((value >> 8) & 0xffu);
    bytes[offset + 1] = static_cast<std::byte>(value & 0xffu);
}

void WriteBe32(std::span<std::byte> bytes, size_t offset,
               uint32_t value) noexcept {
    for (size_t i = 0; i < 4; ++i) {
        bytes[offset + i] =
            static_cast<std::byte>((value >> (24 - 8 * i)) & 0xffu);
    }
}

void WriteBe64(std::span<std::byte> bytes, size_t offset,
               uint64_t value) noexcept {
    for (size_t i = 0; i < 8; ++i) {
        bytes[offset + i] =
            static_cast<std::byte>((value >> (56 - 8 * i)) & 0xffu);
    }
}

uint16_t ReadBe16(std::span<const std::byte> bytes, size_t offset) noexcept {
    return static_cast<uint16_t>(static_cast<uint16_t>(bytes[offset]) << 8) |
           static_cast<uint16_t>(bytes[offset + 1]);
}

uint32_t ReadBe32(std::span<const std::byte> bytes, size_t offset) noexcept {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
        value = (value << 8) | static_cast<uint8_t>(bytes[offset + i]);
    }
    return value;
}

uint64_t ReadBe64(std::span<const std::byte> bytes, size_t offset) noexcept {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) {
        value = (value << 8) | static_cast<uint8_t>(bytes[offset + i]);
    }
    return value;
}

bool IsValidFrameType(FrameType type) noexcept {
    switch (type) {
        case FrameType::kData:
        case FrameType::kSchemaAnnounce:
        case FrameType::kSchemaRequest:
        case FrameType::kAck:
        case FrameType::kHeartbeat:
        case FrameType::kSessionHello:
            return true;
    }
    return false;
}

uint32_t CanonicalHeaderLength(uint16_t flags) noexcept {
    uint32_t length = kWireBaseHeaderLength;
    if (HasFrameFlag(flags, FrameFlag::kPayloadCrcPresent) ||
        HasFrameFlag(flags, FrameFlag::kAeadPresent)) {
        length += kWirePayloadCrcLength;
    }
    if (HasFrameFlag(flags, FrameFlag::kPerfTraceSampled)) {
        length += kWirePerfTraceContextLength;
    }
    return length;
}

Status ValidateFlags(uint16_t flags, bool has_perf_trace, bool encoding) {
    if ((flags & ~kKnownFrameFlags) != 0) {
        return encoding ? Invalid("frame contains unknown flag bits")
                        : Corruption("frame contains unknown flag bits");
    }
    if (HasFrameFlag(flags, FrameFlag::kAeadPresent)) {
        return Unsupported("AEAD framing is not implemented");
    }
    if (HasFrameFlag(flags, FrameFlag::kPerfTraceSampled) != has_perf_trace) {
        return encoding
                   ? Invalid("PERF_TRACE_SAMPLED flag does not match trace data")
                   : Corruption("noncanonical PERF_TRACE_SAMPLED flag");
    }
    return Status::Ok();
}

Status ValidateFrameType(uint16_t flags, FrameType type, bool encoding) {
    if (!IsValidFrameType(type)) {
        return encoding ? Invalid("frame type is unknown")
                        : Corruption("control opcode is unknown");
    }
    const bool is_control = type != FrameType::kData;
    if (HasFrameFlag(flags, FrameFlag::kControlFrame) != is_control) {
        return encoding
                   ? Invalid("CONTROL_FRAME flag does not match frame type")
                   : Corruption("noncanonical control opcode");
    }
    return Status::Ok();
}

bool ExceedsBudget(size_t used, size_t amount, size_t limit) noexcept {
    return used > limit || amount > limit - used;
}

uint32_t HeaderCrc(std::span<const std::byte> header) noexcept {
    // CRC32C covers every byte of the canonical header, including optional
    // fields. The four header_crc bytes are fed as zero; the stream length
    // prefix and payload are not covered.
    Crc32cAccumulator accumulator;
    accumulator.Update(header.first(kHeaderCrcOffset));
    constexpr std::array<std::byte, 4> zeros{};
    accumulator.Update(zeros);
    accumulator.Update(header.subspan(kHeaderCrcOffset + 4));
    return accumulator.Finish();
}

void EncodeTrace(const PerfTraceContext& trace, std::span<std::byte> output,
                 size_t offset) noexcept {
    WriteBe64(output, offset, trace.trace_id_high);
    WriteBe64(output, offset + 8, trace.trace_id_low);
    WriteBe32(output, offset + 16, trace.sample_flags);
    WriteBe32(output, offset + 20, trace.clock_domain_id);
    WriteBe64(output, offset + 24, trace.origin_wall_time_ns);
    WriteBe64(output, offset + 32, trace.origin_monotonic_ns);
}

PerfTraceContext DecodeTrace(std::span<const std::byte> input,
                             size_t offset) noexcept {
    return PerfTraceContext{
        .trace_id_high = ReadBe64(input, offset),
        .trace_id_low = ReadBe64(input, offset + 8),
        .sample_flags = ReadBe32(input, offset + 16),
        .clock_domain_id = ReadBe32(input, offset + 20),
        .origin_wall_time_ns = ReadBe64(input, offset + 24),
        .origin_monotonic_ns = ReadBe64(input, offset + 32),
    };
}

}  // namespace

Result<std::vector<std::byte>> WireFrameCodec::Encode(
    const WireFrame& frame, const WireFrameLimits& limits) noexcept {
    try {
        Status validation = ValidateFlags(
            frame.header.flags, frame.header.perf_trace.has_value(), true);
        if (!validation.ok()) return validation;
        validation = ValidateFrameType(frame.header.flags,
                                       frame.header.frame_type, true);
        if (!validation.ok()) return validation;

        const bool is_control =
            HasFrameFlag(frame.header.flags, FrameFlag::kControlFrame);
        const uint64_t wire_payload_length =
            static_cast<uint64_t>(frame.payload.size()) +
            (is_control ? kWireControlOpcodeLength : 0u);
        if (wire_payload_length > limits.max_payload_length ||
            wire_payload_length > std::numeric_limits<uint32_t>::max()) {
            return Resource("wire payload exceeds max_payload_length");
        }

        const uint32_t header_length =
            CanonicalHeaderLength(frame.header.flags);
        const uint64_t body_length = header_length + wire_payload_length;
        if (body_length > std::numeric_limits<uint32_t>::max()) {
            return Resource("encoded frame exceeds uint32 length prefix");
        }

        std::vector<std::byte> output(static_cast<size_t>(body_length));
        std::span<std::byte> bytes(output);
        WriteBe32(bytes, kMagicOffset, kWireFrameMagic);
        WriteBe16(bytes, kVersionOffset, kWireProtocolVersion);
        WriteBe16(bytes, kFlagsOffset, frame.header.flags);
        WriteBe32(bytes, kHeaderLengthOffset, header_length);
        WriteBe32(bytes, kTopicIdOffset, frame.header.topic_id);
        WriteBe32(bytes, kMsgTypeOffset, frame.header.msg_type);
        WriteBe32(bytes, kConnectionSchemaRefOffset,
                  frame.header.connection_schema_ref);
        WriteBe32(bytes, kSchemaVersionOffset, frame.header.schema_version);
        WriteBe32(bytes, kLayoutVersionOffset, frame.header.layout_version);
        WriteBe64(bytes, kSourceNodeIdOffset, frame.header.source_node_id);
        WriteBe64(bytes, kSourcePublisherIdOffset,
                  frame.header.source_publisher_id);
        WriteBe64(bytes, kSourcePublisherEpochOffset,
                  frame.header.source_publisher_epoch);
        WriteBe64(bytes, kSequenceNumOffset, frame.header.sequence_num);
        WriteBe64(bytes, kTimestampNsOffset, frame.header.timestamp_ns);
        WriteBe32(bytes, kPayloadLengthOffset,
                  static_cast<uint32_t>(wire_payload_length));
        WriteBe32(bytes, kHeaderCrcOffset, 0);

        size_t payload_offset = header_length;
        if (is_control) {
            WriteBe32(bytes, payload_offset,
                      static_cast<uint32_t>(frame.header.frame_type));
            payload_offset += kWireControlOpcodeLength;
        }
        std::copy(frame.payload.begin(), frame.payload.end(),
                  output.begin() + payload_offset);
        const auto wire_payload =
            std::span<const std::byte>(bytes).subspan(
                header_length, static_cast<size_t>(wire_payload_length));

        size_t optional_offset = kOptionalHeaderOffset;
        if (HasFrameFlag(frame.header.flags,
                         FrameFlag::kPayloadCrcPresent)) {
            WriteBe32(bytes, optional_offset, Crc32c(wire_payload));
            optional_offset += kWirePayloadCrcLength;
        }
        if (frame.header.perf_trace.has_value()) {
            EncodeTrace(*frame.header.perf_trace, bytes, optional_offset);
            optional_offset += kWirePerfTraceContextLength;
        }
        if (optional_offset != header_length) {
            return Status::Error(StatusCode::kInternal,
                                 "canonical header construction mismatch");
        }

        WriteBe32(bytes, kHeaderCrcOffset,
                  HeaderCrc(std::span<const std::byte>(bytes).first(
                      header_length)));
        return output;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<std::vector<std::byte>> WireFrameCodec::EncodeLengthPrefixed(
    const WireFrame& frame, const WireFrameLimits& limits) noexcept {
    try {
        auto body = Encode(frame, limits);
        if (!body.ok()) return body.status();
        const size_t output_size = kLengthPrefixSize + body->size();
        std::vector<std::byte> output(output_size);
        WriteBe32(output, 0, static_cast<uint32_t>(body->size()));
        std::copy(body->begin(), body->end(),
                  output.begin() + kLengthPrefixSize);
        return output;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<WireFrame> WireFrameCodec::Decode(
    std::span<const std::byte> frame_body,
    const WireFrameLimits& limits) noexcept {
    try {
        const uint64_t maximum_body_length =
            static_cast<uint64_t>(kWireMaximumHeaderLength) +
            limits.max_payload_length;
        if (frame_body.size() > maximum_body_length) {
            return Resource("frame body exceeds configured maximum");
        }
        if (frame_body.size() < kWireBaseHeaderLength) {
            return Corruption("frame is shorter than the base header");
        }
        if (ReadBe32(frame_body, kMagicOffset) != kWireFrameMagic) {
            return Corruption("frame magic mismatch");
        }
        if (ReadBe16(frame_body, kVersionOffset) != kWireProtocolVersion) {
            return Unsupported("unsupported wire protocol version");
        }

        const uint16_t flags = ReadBe16(frame_body, kFlagsOffset);
        const bool has_trace =
            HasFrameFlag(flags, FrameFlag::kPerfTraceSampled);
        Status validation = ValidateFlags(flags, has_trace, false);
        if (!validation.ok()) return validation;

        const uint32_t header_length =
            ReadBe32(frame_body, kHeaderLengthOffset);
        if (header_length != CanonicalHeaderLength(flags)) {
            return Corruption("noncanonical header_length");
        }
        if (header_length > frame_body.size()) {
            return Corruption("truncated optional header");
        }

        const uint32_t stored_header_crc =
            ReadBe32(frame_body, kHeaderCrcOffset);
        if (stored_header_crc != HeaderCrc(frame_body.first(header_length))) {
            return Corruption("header CRC32C mismatch");
        }

        const uint32_t payload_length =
            ReadBe32(frame_body, kPayloadLengthOffset);
        if (payload_length > limits.max_payload_length) {
            return Resource("wire payload exceeds max_payload_length");
        }
        const uint64_t expected_body_length =
            static_cast<uint64_t>(header_length) + payload_length;
        if (expected_body_length != frame_body.size()) {
            return Corruption(expected_body_length < frame_body.size()
                                  ? "trailing bytes after frame payload"
                                  : "truncated frame payload");
        }

        size_t optional_offset = kOptionalHeaderOffset;
        std::optional<uint32_t> stored_payload_crc;
        if (HasFrameFlag(flags, FrameFlag::kPayloadCrcPresent)) {
            stored_payload_crc = ReadBe32(frame_body, optional_offset);
            optional_offset += kWirePayloadCrcLength;
        }

        std::optional<PerfTraceContext> trace;
        if (has_trace) {
            trace = DecodeTrace(frame_body, optional_offset);
            optional_offset += kWirePerfTraceContextLength;
        }
        if (optional_offset != header_length) {
            return Corruption("noncanonical optional header fields");
        }

        const auto wire_payload =
            frame_body.subspan(header_length, payload_length);
        if (stored_payload_crc.has_value() &&
            *stored_payload_crc != Crc32c(wire_payload)) {
            return Corruption("payload CRC32C mismatch");
        }

        FrameType frame_type = FrameType::kData;
        auto decoded_payload = wire_payload;
        if (HasFrameFlag(flags, FrameFlag::kControlFrame)) {
            if (wire_payload.size() < kWireControlOpcodeLength) {
                return Corruption("control payload is missing its opcode");
            }
            frame_type =
                static_cast<FrameType>(ReadBe32(wire_payload, 0));
            validation = ValidateFrameType(flags, frame_type, false);
            if (!validation.ok()) return validation;
            decoded_payload = wire_payload.subspan(kWireControlOpcodeLength);
        }

        WireFrame frame;
        frame.header = WireFrameHeader{
            .frame_type = frame_type,
            .flags = flags,
            .topic_id = ReadBe32(frame_body, kTopicIdOffset),
            .msg_type = ReadBe32(frame_body, kMsgTypeOffset),
            .connection_schema_ref =
                ReadBe32(frame_body, kConnectionSchemaRefOffset),
            .schema_version = ReadBe32(frame_body, kSchemaVersionOffset),
            .layout_version = ReadBe32(frame_body, kLayoutVersionOffset),
            .source_node_id = ReadBe64(frame_body, kSourceNodeIdOffset),
            .source_publisher_id =
                ReadBe64(frame_body, kSourcePublisherIdOffset),
            .source_publisher_epoch =
                ReadBe64(frame_body, kSourcePublisherEpochOffset),
            .sequence_num = ReadBe64(frame_body, kSequenceNumOffset),
            .timestamp_ns = ReadBe64(frame_body, kTimestampNsOffset),
            .perf_trace = trace,
        };
        frame.payload.assign(decoded_payload.begin(), decoded_payload.end());
        return frame;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

void LengthPrefixedFrameDecoder::ReleaseFrameBuffer() noexcept {
    std::vector<std::byte>().swap(frame_buffer_);
    expected_frame_length_ = 0;
}

Result<std::vector<WireFrame>> LengthPrefixedFrameDecoder::Fail(
    Status status) noexcept {
    failed_ = true;
    prefix_size_ = 0;
    ReleaseFrameBuffer();
    return status;
}

Result<std::vector<WireFrame>> LengthPrefixedFrameDecoder::Push(
    std::span<const std::byte> bytes) noexcept {
    try {
        if (failed_) {
            return Corruption("stream decoder is failed; call Reset");
        }

        std::vector<WireFrame> frames;
        size_t decoded_payload_bytes = 0;
        size_t work_bytes = 0;
        size_t input_offset = 0;
        while (input_offset < bytes.size()) {
            if (prefix_size_ < kLengthPrefixSize) {
                const size_t count = std::min(
                    static_cast<size_t>(kLengthPrefixSize) - prefix_size_,
                    bytes.size() - input_offset);
                if (ExceedsBudget(work_bytes, count,
                                  limits_.max_work_bytes_per_push)) {
                    return Fail(Resource("Push work budget exceeded"));
                }
                std::copy_n(bytes.begin() + input_offset, count,
                            prefix_.begin() + prefix_size_);
                prefix_size_ += count;
                input_offset += count;
                work_bytes += count;
                if (prefix_size_ < kLengthPrefixSize) break;

                const uint32_t frame_length = ReadBe32(prefix_, 0);
                const uint64_t maximum_body_length =
                    static_cast<uint64_t>(kWireMaximumHeaderLength) +
                    limits_.max_payload_length;
                if (frame_length < kWireBaseHeaderLength) {
                    return Fail(Corruption(
                        "length prefix is shorter than the base header"));
                }
                if (frame_length > maximum_body_length) {
                    return Fail(Resource(
                        "length prefix exceeds configured frame maximum"));
                }
                if (limits_.max_buffered_bytes < kLengthPrefixSize ||
                    frame_length >
                        limits_.max_buffered_bytes - kLengthPrefixSize) {
                    return Fail(Resource(
                        "length prefix exceeds bounded stream buffer"));
                }
                expected_frame_length_ = frame_length;
            }

            const size_t count = std::min(
                expected_frame_length_ - frame_buffer_.size(),
                bytes.size() - input_offset);
            if (ExceedsBudget(work_bytes, count,
                              limits_.max_work_bytes_per_push)) {
                return Fail(Resource("Push work budget exceeded"));
            }
            frame_buffer_.insert(frame_buffer_.end(),
                                 bytes.begin() + input_offset,
                                 bytes.begin() + input_offset + count);
            input_offset += count;
            work_bytes += count;
            if (frame_buffer_.size() < expected_frame_length_) break;

            if (frames.size() >= limits_.max_frames_per_push) {
                return Fail(Resource("Push completed-frame budget exceeded"));
            }
            if (ExceedsBudget(work_bytes, frame_buffer_.size(),
                              limits_.max_work_bytes_per_push)) {
                return Fail(Resource("Push work budget exceeded"));
            }
            work_bytes += frame_buffer_.size();

            auto decoded = WireFrameCodec::Decode(frame_buffer_, limits_);
            if (!decoded.ok()) return Fail(decoded.status());
            if (ExceedsBudget(decoded_payload_bytes, decoded->payload.size(),
                              limits_.max_decoded_payload_bytes_per_push)) {
                return Fail(
                    Resource("Push decoded-payload budget exceeded"));
            }
            decoded_payload_bytes += decoded->payload.size();
            frames.push_back(std::move(*decoded));
            prefix_size_ = 0;
            frame_buffer_.clear();
            expected_frame_length_ = 0;
        }
        return frames;
    } catch (const std::bad_alloc&) {
        return Fail(Status::Error(StatusCode::kResourceExhausted));
    } catch (const std::length_error&) {
        return Fail(Status::Error(StatusCode::kResourceExhausted));
    }
}

Status LengthPrefixedFrameDecoder::Finish() noexcept {
    if (failed_) {
        return Corruption("stream decoder is failed; call Reset");
    }
    if (prefix_size_ != 0 || !frame_buffer_.empty()) {
        failed_ = true;
        prefix_size_ = 0;
        ReleaseFrameBuffer();
        return Corruption("truncated length-prefixed stream");
    }
    return Status::Ok();
}

void LengthPrefixedFrameDecoder::Reset() noexcept {
    failed_ = false;
    prefix_size_ = 0;
    ReleaseFrameBuffer();
}

}  // namespace mino::bridge
