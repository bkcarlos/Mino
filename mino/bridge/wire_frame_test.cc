// Copyright 2026 The Mino Authors

#include "mino/bridge/wire_frame.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/bridge/crc32c.h"
#include "mino/common/status.h"

namespace mino::bridge {
namespace {

template <typename T>
concept HasOffsetMember = requires(T value) { value.offset; };

template <typename T>
concept HasShmHandleMember = requires(T value) { value.shm_handle; };

static_assert(!HasOffsetMember<WireFrameHeader>);
static_assert(!HasShmHandleMember<WireFrameHeader>);

std::vector<std::byte> Bytes(std::initializer_list<uint8_t> values) {
    std::vector<std::byte> result;
    result.reserve(values.size());
    for (uint8_t value : values) {
        result.push_back(static_cast<std::byte>(value));
    }
    return result;
}

std::string Hex(std::span<const std::byte> bytes) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (std::byte byte : bytes) {
        const uint8_t value = static_cast<uint8_t>(byte);
        result.push_back(kHex[value >> 4]);
        result.push_back(kHex[value & 0x0fu]);
    }
    return result;
}

void ExpectBytesAt(std::span<const std::byte> actual, size_t offset,
                   std::initializer_list<uint8_t> expected,
                   std::string_view field) {
    SCOPED_TRACE(field);
    ASSERT_LE(offset + expected.size(), actual.size());
    size_t index = 0;
    for (uint8_t byte : expected) {
        EXPECT_EQ(static_cast<uint8_t>(actual[offset + index]), byte)
            << "byte " << index << " at absolute offset " << offset + index;
        ++index;
    }
}

WireFrame GoldenFrame() {
    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    frame.header.flags = 0;
    frame.header.topic_id = 0x01020304u;
    frame.header.msg_type = 0x11121314u;
    frame.header.connection_schema_ref = 0x21222324u;
    frame.header.schema_version = 0x00020003u;
    frame.header.layout_version = 0x31323334u;
    frame.header.source_node_id = 0x0102030405060708ull;
    frame.header.source_publisher_id = 0x1112131415161718ull;
    frame.header.source_publisher_epoch = 0x2122232425262728ull;
    frame.header.sequence_num = 0x3132333435363738ull;
    frame.header.timestamp_ns = 0x4142434445464748ull;
    frame.payload = Bytes({0xde, 0xad, 0xbe, 0xef});
    return frame;
}

void ExpectDecodeFailure(std::span<const std::byte> body,
                         StatusCode expected = StatusCode::kCorruption) {
    auto decoded = WireFrameCodec::Decode(body);
    ASSERT_FALSE(decoded.ok());
    EXPECT_EQ(decoded.status().code(), expected)
        << decoded.status().ToString();
}

TEST(Crc32cTest, MatchesStandardCheckVector) {
    const std::array<std::byte, 9> input = {
        std::byte{'1'}, std::byte{'2'}, std::byte{'3'},
        std::byte{'4'}, std::byte{'5'}, std::byte{'6'},
        std::byte{'7'}, std::byte{'8'}, std::byte{'9'},
    };
    EXPECT_EQ(Crc32c(input), 0xe3069283u);
}

TEST(WireFrameCodecTest, GoldenVectorIsExact80ByteV1Header) {
    const WireFrame frame = GoldenFrame();
    auto encoded = WireFrameCodec::EncodeLengthPrefixed(frame);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    // Independently generated from detailed design 16.2 and CRC-32C. No
    // frame_type exists between header_length and topic_id.
    EXPECT_EQ(
        Hex(*encoded),
        "00000054"
        "4d494e4f0001000000000050"
        "0102030411121314212223240002000331323334"
        "010203040506070811121314151617182122232425262728"
        "31323334353637384142434445464748"
        "00000004ed380e74deadbeef");

    ASSERT_EQ(encoded->size(), kLengthPrefixSize + 80u + 4u);
    const auto body =
        std::span<const std::byte>(*encoded).subspan(kLengthPrefixSize);
    ExpectBytesAt(*encoded, 0, {0x00, 0x00, 0x00, 0x54}, "length prefix");
    ExpectBytesAt(body, 0, {0x4d, 0x49, 0x4e, 0x4f}, "magic");
    ExpectBytesAt(body, 4, {0x00, 0x01}, "protocol_version");
    ExpectBytesAt(body, 6, {0x00, 0x00}, "flags");
    ExpectBytesAt(body, 8, {0x00, 0x00, 0x00, 0x50}, "header_length");
    ExpectBytesAt(body, 12, {0x01, 0x02, 0x03, 0x04}, "topic_id");
    ExpectBytesAt(body, 16, {0x11, 0x12, 0x13, 0x14}, "msg_type");
    ExpectBytesAt(body, 20, {0x21, 0x22, 0x23, 0x24},
                  "connection_schema_ref");
    ExpectBytesAt(body, 24, {0x00, 0x02, 0x00, 0x03}, "schema_version");
    ExpectBytesAt(body, 28, {0x31, 0x32, 0x33, 0x34}, "layout_version");
    ExpectBytesAt(body, 32,
                  {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
                  "source_node_id");
    ExpectBytesAt(body, 40,
                  {0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
                  "source_publisher_id");
    ExpectBytesAt(body, 48,
                  {0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28},
                  "source_publisher_epoch");
    ExpectBytesAt(body, 56,
                  {0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38},
                  "sequence_num");
    ExpectBytesAt(body, 64,
                  {0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48},
                  "timestamp_ns");
    ExpectBytesAt(body, 72, {0x00, 0x00, 0x00, 0x04}, "payload_length");
    ExpectBytesAt(body, 76, {0xed, 0x38, 0x0e, 0x74}, "header_crc");
    ExpectBytesAt(body, 80, {0xde, 0xad, 0xbe, 0xef}, "payload");

    auto decoded = WireFrameCodec::Decode(body);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(*decoded, frame);
}

TEST(WireFrameCodecTest, RoundTripsTraceContextAndEveryFrameType) {
    const std::array<FrameType, 5> types = {
        FrameType::kData, FrameType::kSchemaAnnounce,
        FrameType::kSchemaRequest, FrameType::kAck,
        FrameType::kHeartbeat,
    };
    for (FrameType type : types) {
        WireFrame frame = GoldenFrame();
        frame.header.frame_type = type;
        frame.header.flags =
            FlagValue(FrameFlag::kPayloadCrcPresent) |
            FlagValue(FrameFlag::kPerfTraceSampled);
        if (type != FrameType::kData) {
            frame.header.flags |= FlagValue(FrameFlag::kControlFrame);
        }
        frame.header.perf_trace = PerfTraceContext{
            .trace_id_high = 1,
            .trace_id_low = 2,
            .sample_flags = 3,
            .clock_domain_id = 4,
            .origin_wall_time_ns = 5,
            .origin_monotonic_ns = 6,
        };

        auto encoded = WireFrameCodec::Encode(frame);
        ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
        const size_t control_prefix =
            type == FrameType::kData ? 0u : kWireControlOpcodeLength;
        EXPECT_EQ(encoded->size(), kWireMaximumHeaderLength +
                                       frame.payload.size() + control_prefix);
        auto decoded = WireFrameCodec::Decode(*encoded);
        ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
        EXPECT_EQ(*decoded, frame);
    }
}

TEST(WireFrameCodecTest, RoundTripsEveryOptionalCrcAndPerfCombination) {
    for (uint32_t combination = 0; combination < 4; ++combination) {
        WireFrame frame = GoldenFrame();
        const bool has_crc = (combination & 1u) != 0;
        const bool has_trace = (combination & 2u) != 0;
        if (has_crc) {
            frame.header.flags |= FlagValue(FrameFlag::kPayloadCrcPresent);
        }
        if (has_trace) {
            frame.header.flags |= FlagValue(FrameFlag::kPerfTraceSampled);
            frame.header.perf_trace = PerfTraceContext{
                .trace_id_high = 1,
                .trace_id_low = 2,
                .sample_flags = 3,
                .clock_domain_id = 4,
                .origin_wall_time_ns = 5,
                .origin_monotonic_ns = 6,
            };
        }

        auto encoded = WireFrameCodec::Encode(frame);
        ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
        const size_t expected_header_length =
            kWireBaseHeaderLength +
            (has_crc ? kWirePayloadCrcLength : 0u) +
            (has_trace ? kWirePerfTraceContextLength : 0u);
        EXPECT_EQ(encoded->size(),
                  expected_header_length + frame.payload.size());
        ExpectBytesAt(*encoded, 8,
                      {0x00, 0x00, 0x00,
                       static_cast<uint8_t>(expected_header_length)},
                      "optional header_length");

        auto decoded = WireFrameCodec::Decode(*encoded);
        ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
        EXPECT_EQ(*decoded, frame);
    }
}

TEST(WireFrameCodecTest, ControlOpcodePrefixesWirePayloadAndIsCrcCovered) {
    WireFrame frame = GoldenFrame();
    frame.header.frame_type = FrameType::kHeartbeat;
    frame.header.flags = FlagValue(FrameFlag::kControlFrame) |
                         FlagValue(FrameFlag::kPayloadCrcPresent);

    auto encoded = WireFrameCodec::Encode(frame);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    ExpectBytesAt(*encoded, 8, {0x00, 0x00, 0x00, 0x54}, "header_length");
    ExpectBytesAt(*encoded, 72, {0x00, 0x00, 0x00, 0x08},
                  "wire payload_length includes opcode");
    ExpectBytesAt(*encoded, 80, {0xd5, 0xd6, 0x30, 0x73},
                  "payload CRC covers opcode and control data");
    ExpectBytesAt(*encoded, 84, {0x00, 0x00, 0x00, 0x04},
                  "big-endian control opcode");
    ExpectBytesAt(*encoded, 88, {0xde, 0xad, 0xbe, 0xef},
                  "control data after opcode");

    auto decoded = WireFrameCodec::Decode(*encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(*decoded, frame);
}

TEST(WireFrameCodecTest, HeaderAndPayloadCrcDetectTampering) {
    WireFrame frame = GoldenFrame();
    frame.header.flags = FlagValue(FrameFlag::kPayloadCrcPresent);
    auto body = WireFrameCodec::Encode(frame);
    ASSERT_TRUE(body.ok());

    std::vector<std::byte> corrupt_header = *body;
    corrupt_header[12] ^= std::byte{0x01};
    ExpectDecodeFailure(corrupt_header);

    std::vector<std::byte> corrupt_stored_header_crc = *body;
    corrupt_stored_header_crc[76] ^= std::byte{0x01};
    ExpectDecodeFailure(corrupt_stored_header_crc);

    std::vector<std::byte> corrupt_payload = *body;
    ASSERT_FALSE(corrupt_payload.empty());
    corrupt_payload.at(corrupt_payload.size() - 1) ^= std::byte{0x01};
    ExpectDecodeFailure(corrupt_payload);
}

TEST(WireFrameCodecTest, RejectsTruncationAndTrailingBytes) {
    auto body = WireFrameCodec::Encode(GoldenFrame());
    ASSERT_TRUE(body.ok());
    for (size_t size = 0; size < body->size(); ++size) {
        ExpectDecodeFailure(std::span<const std::byte>(*body).first(size));
    }

    std::vector<std::byte> trailing = *body;
    trailing.push_back(std::byte{0});
    ExpectDecodeFailure(trailing);
}

TEST(WireFrameCodecTest, EnforcesPayloadLimitBeforePayloadAllocation) {
    WireFrameLimits limits;
    limits.max_payload_length = 3;

    auto rejected_encode = WireFrameCodec::Encode(GoldenFrame(), limits);
    ASSERT_FALSE(rejected_encode.ok());
    EXPECT_EQ(rejected_encode.status().code(),
              StatusCode::kResourceExhausted);

    auto valid_body = WireFrameCodec::Encode(GoldenFrame());
    ASSERT_TRUE(valid_body.ok());
    auto rejected_decode = WireFrameCodec::Decode(*valid_body, limits);
    ASSERT_FALSE(rejected_decode.ok());
    EXPECT_EQ(rejected_decode.status().code(),
              StatusCode::kResourceExhausted);
}

TEST(WireFrameCodecTest, PayloadLimitCountsCanonicalControlOpcode) {
    WireFrame frame = GoldenFrame();
    frame.header.frame_type = FrameType::kHeartbeat;
    frame.header.flags = FlagValue(FrameFlag::kControlFrame);
    frame.payload.clear();

    WireFrameLimits limits;
    limits.max_payload_length = kWireControlOpcodeLength;
    auto accepted = WireFrameCodec::Encode(frame, limits);
    ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();

    limits.max_payload_length = kWireControlOpcodeLength - 1;
    auto rejected = WireFrameCodec::Encode(frame, limits);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kResourceExhausted);
}

TEST(WireFrameCodecTest, RejectsUnknownAndNoncanonicalHeaderValues) {
    auto body = WireFrameCodec::Encode(GoldenFrame());
    ASSERT_TRUE(body.ok());

    std::vector<std::byte> unknown_flags = *body;
    unknown_flags[6] = std::byte{0x80};
    ExpectDecodeFailure(unknown_flags);

    std::vector<std::byte> unknown_version = *body;
    unknown_version[5] = std::byte{0x02};
    ExpectDecodeFailure(unknown_version, StatusCode::kUnsupported);

    WireFrame control = GoldenFrame();
    control.header.frame_type = FrameType::kHeartbeat;
    control.header.flags = FlagValue(FrameFlag::kControlFrame);
    auto control_body = WireFrameCodec::Encode(control);
    ASSERT_TRUE(control_body.ok());
    std::vector<std::byte> unknown_opcode = *control_body;
    unknown_opcode[kWireBaseHeaderLength + 3] = std::byte{0x7f};
    ExpectDecodeFailure(unknown_opcode);

    std::vector<std::byte> noncanonical_length = *body;
    noncanonical_length[11] = std::byte{0x54};
    ExpectDecodeFailure(noncanonical_length);

    std::vector<std::byte> noncanonical_control = *body;
    noncanonical_control[7] |= std::byte{0x08};
    ExpectDecodeFailure(noncanonical_control);
}

TEST(WireFrameCodecTest, EncoderRejectsInconsistentOrUnsupportedFlags) {
    WireFrame frame = GoldenFrame();
    frame.header.flags |= 0x8000u;
    auto result = WireFrameCodec::Encode(frame);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);

    frame = GoldenFrame();
    frame.header.frame_type = FrameType::kHeartbeat;
    result = WireFrameCodec::Encode(frame);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);

    frame = GoldenFrame();
    frame.header.flags |= FlagValue(FrameFlag::kPerfTraceSampled);
    result = WireFrameCodec::Encode(frame);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);

    frame = GoldenFrame();
    frame.header.flags |= FlagValue(FrameFlag::kAeadPresent);
    result = WireFrameCodec::Encode(frame);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kUnsupported);
}

TEST(LengthPrefixedFrameDecoderTest, HandlesEveryByteAsAPartialRead) {
    const WireFrame expected = GoldenFrame();
    auto encoded = WireFrameCodec::EncodeLengthPrefixed(expected);
    ASSERT_TRUE(encoded.ok());

    LengthPrefixedFrameDecoder decoder;
    for (size_t i = 0; i < encoded->size(); ++i) {
        auto frames = decoder.Push(
            std::span<const std::byte>(*encoded).subspan(i, 1));
        ASSERT_TRUE(frames.ok()) << frames.status().ToString();
        if (i + 1 == encoded->size()) {
            ASSERT_EQ(frames->size(), 1u);
            EXPECT_EQ((*frames)[0], expected);
        } else {
            EXPECT_TRUE(frames->empty());
            EXPECT_LE(decoder.buffered_bytes(), encoded->size());
        }
    }
    EXPECT_TRUE(decoder.Finish().ok());
}

TEST(LengthPrefixedFrameDecoderTest, ReturnsMultipleConsecutiveFrames) {
    WireFrame first = GoldenFrame();
    WireFrame second = GoldenFrame();
    second.header.sequence_num++;
    second.payload = Bytes({1, 2, 3});
    auto encoded_first = WireFrameCodec::EncodeLengthPrefixed(first);
    auto encoded_second = WireFrameCodec::EncodeLengthPrefixed(second);
    ASSERT_TRUE(encoded_first.ok() && encoded_second.ok());

    std::vector<std::byte> stream = *encoded_first;
    stream.insert(stream.end(), encoded_second->begin(), encoded_second->end());
    LengthPrefixedFrameDecoder decoder;
    auto frames = decoder.Push(stream);
    ASSERT_TRUE(frames.ok()) << frames.status().ToString();
    ASSERT_EQ(frames->size(), 2u);
    EXPECT_EQ((*frames)[0], first);
    EXPECT_EQ((*frames)[1], second);
    EXPECT_TRUE(decoder.Finish().ok());
}

TEST(LengthPrefixedFrameDecoderTest, RejectsTruncatedStreamAtFinish) {
    auto encoded = WireFrameCodec::EncodeLengthPrefixed(GoldenFrame());
    ASSERT_TRUE(encoded.ok());

    LengthPrefixedFrameDecoder decoder;
    auto frames = decoder.Push(
        std::span<const std::byte>(*encoded).first(encoded->size() - 1));
    ASSERT_TRUE(frames.ok());
    EXPECT_TRUE(frames->empty());
    const Status status = decoder.Finish();
    EXPECT_EQ(status.code(), StatusCode::kCorruption);
    EXPECT_TRUE(decoder.failed());
}

TEST(LengthPrefixedFrameDecoderTest, RejectsOverflowWithoutLargeAllocation) {
    WireFrameLimits limits;
    limits.max_payload_length = 32;
    limits.max_buffered_bytes = 128;
    LengthPrefixedFrameDecoder decoder(limits);
    const auto malicious_prefix = Bytes({0xff, 0xff, 0xff, 0xff});
    auto frames = decoder.Push(malicious_prefix);
    ASSERT_FALSE(frames.ok());
    EXPECT_EQ(frames.status().code(), StatusCode::kResourceExhausted);
    EXPECT_TRUE(decoder.failed());
    EXPECT_EQ(decoder.buffered_bytes(), 0u);
    EXPECT_EQ(decoder.retained_capacity(), 0u);
}

TEST(LengthPrefixedFrameDecoderTest,
     MaximumLegalSlowlorisPrefixDoesNotAllocateFrameMaximum) {
    LengthPrefixedFrameDecoder decoder;
    // 0x0100007c = 16 MiB payload + 124-byte maximum optional header.
    const auto maximum_legal_prefix = Bytes({0x01, 0x00, 0x00, 0x7c});
    auto frames = decoder.Push(maximum_legal_prefix);
    ASSERT_TRUE(frames.ok()) << frames.status().ToString();
    EXPECT_TRUE(frames->empty());
    EXPECT_EQ(decoder.buffered_bytes(), kLengthPrefixSize);
    EXPECT_EQ(decoder.retained_capacity(), 0u);

    const auto one_byte = Bytes({0x4d});
    frames = decoder.Push(one_byte);
    ASSERT_TRUE(frames.ok()) << frames.status().ToString();
    EXPECT_EQ(decoder.buffered_bytes(), kLengthPrefixSize + 1u);
    EXPECT_LT(decoder.retained_capacity(), 1024u);
}

TEST(LengthPrefixedFrameDecoderTest, ResetAndFailReleaseLargeCapacity) {
    const auto one_mib_prefix = Bytes({0x00, 0x10, 0x00, 0x00});
    const std::vector<std::byte> partial(256u * 1024u, std::byte{0});

    LengthPrefixedFrameDecoder reset_decoder;
    ASSERT_TRUE(reset_decoder.Push(one_mib_prefix).ok());
    ASSERT_TRUE(reset_decoder.Push(partial).ok());
    ASSERT_GE(reset_decoder.retained_capacity(), partial.size());
    reset_decoder.Reset();
    EXPECT_EQ(reset_decoder.buffered_bytes(), 0u);
    EXPECT_EQ(reset_decoder.retained_capacity(), 0u);

    WireFrameLimits limits;
    limits.max_work_bytes_per_push = 1024;
    LengthPrefixedFrameDecoder fail_decoder(limits);
    ASSERT_TRUE(fail_decoder.Push(one_mib_prefix).ok());
    const std::vector<std::byte> retained(512, std::byte{0});
    ASSERT_TRUE(fail_decoder.Push(retained).ok());
    ASSERT_GE(fail_decoder.retained_capacity(), retained.size());
    const std::vector<std::byte> over_work_budget(1025, std::byte{0});
    auto failed = fail_decoder.Push(over_work_budget);
    ASSERT_FALSE(failed.ok());
    EXPECT_EQ(failed.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(fail_decoder.buffered_bytes(), 0u);
    EXPECT_EQ(fail_decoder.retained_capacity(), 0u);
}

TEST(LengthPrefixedFrameDecoderTest, EnforcesCompletedFrameBudgetPerPush) {
    auto encoded = WireFrameCodec::EncodeLengthPrefixed(GoldenFrame());
    ASSERT_TRUE(encoded.ok());
    std::vector<std::byte> stream = *encoded;
    stream.insert(stream.end(), encoded->begin(), encoded->end());

    WireFrameLimits limits;
    limits.max_frames_per_push = 1;
    LengthPrefixedFrameDecoder decoder(limits);
    auto frames = decoder.Push(stream);
    ASSERT_FALSE(frames.ok());
    EXPECT_EQ(frames.status().code(), StatusCode::kResourceExhausted);
    EXPECT_TRUE(decoder.failed());
    EXPECT_EQ(decoder.buffered_bytes(), 0u);
    EXPECT_EQ(decoder.retained_capacity(), 0u);
}

TEST(LengthPrefixedFrameDecoderTest, EnforcesDecodedPayloadBudgetPerPush) {
    auto encoded = WireFrameCodec::EncodeLengthPrefixed(GoldenFrame());
    ASSERT_TRUE(encoded.ok());
    std::vector<std::byte> stream = *encoded;
    stream.insert(stream.end(), encoded->begin(), encoded->end());

    WireFrameLimits limits;
    limits.max_decoded_payload_bytes_per_push = 7;
    LengthPrefixedFrameDecoder decoder(limits);
    auto frames = decoder.Push(stream);
    ASSERT_FALSE(frames.ok());
    EXPECT_EQ(frames.status().code(), StatusCode::kResourceExhausted);
    EXPECT_TRUE(decoder.failed());
    EXPECT_EQ(decoder.buffered_bytes(), 0u);
    EXPECT_EQ(decoder.retained_capacity(), 0u);
}

TEST(LengthPrefixedFrameDecoderTest, EnforcesWorkBudgetPerPush) {
    auto encoded = WireFrameCodec::EncodeLengthPrefixed(GoldenFrame());
    ASSERT_TRUE(encoded.ok());

    WireFrameLimits limits;
    // Input bytes are consumed once; the complete body is then revisited by
    // validation, CRC, and payload copying.
    limits.max_work_bytes_per_push =
        encoded->size() + (encoded->size() - kLengthPrefixSize) - 1;
    LengthPrefixedFrameDecoder decoder(limits);
    auto frames = decoder.Push(*encoded);
    ASSERT_FALSE(frames.ok());
    EXPECT_EQ(frames.status().code(), StatusCode::kResourceExhausted);
    EXPECT_TRUE(decoder.failed());
    EXPECT_EQ(decoder.buffered_bytes(), 0u);
    EXPECT_EQ(decoder.retained_capacity(), 0u);
}

TEST(WireFrameCodecTest, FuzzLikeMalformedInputsDoNotCrash) {
    uint32_t state = 0x9e3779b9u;
    auto next = [&state]() {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        return state;
    };

    WireFrameLimits limits;
    limits.max_payload_length = 256;
    limits.max_buffered_bytes = kLengthPrefixSize +
                                kWireMaximumHeaderLength + 256;
    auto valid = WireFrameCodec::Encode(GoldenFrame());
    ASSERT_TRUE(valid.ok());

    for (size_t iteration = 0; iteration < 2000; ++iteration) {
        std::vector<std::byte> mutated = *valid;
        const size_t mutation_count = 1 + next() % 6;
        for (size_t mutation = 0; mutation < mutation_count; ++mutation) {
            const size_t offset = next() % mutated.size();
            mutated[offset] ^= static_cast<std::byte>(next() & 0xffu);
        }
        (void)WireFrameCodec::Decode(mutated, limits);

        const size_t garbage_size = next() % 257;
        std::vector<std::byte> garbage(garbage_size);
        for (std::byte& byte : garbage) {
            byte = static_cast<std::byte>(next() & 0xffu);
        }
        (void)WireFrameCodec::Decode(garbage, limits);

        LengthPrefixedFrameDecoder decoder(limits);
        size_t offset = 0;
        while (offset < garbage.size() && !decoder.failed()) {
            const size_t chunk =
                std::min<size_t>(1 + next() % 17, garbage.size() - offset);
            auto frames = decoder.Push(
                std::span<const std::byte>(garbage).subspan(offset, chunk));
            if (!frames.ok()) break;
            offset += chunk;
        }
        if (!decoder.failed()) (void)decoder.Finish();
    }
}

}  // namespace
}  // namespace mino::bridge
