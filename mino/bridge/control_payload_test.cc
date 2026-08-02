// Copyright 2026 The Mino Authors

#include "mino/bridge/control_payload.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "mino/common/status.h"

namespace mino::bridge {
namespace {

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

AckPayload GoldenAck() {
    return AckPayload{
        .sender_session_epoch = 0x0102030405060708ull,
        .receiver_session_epoch = 0x1112131415161718ull,
        .source = SourceIdentity{0x2122232425262728ull,
                                 0x3132333435363738ull,
                                 0x4142434445464748ull},
        .observed_sequence = 0x5152535455565758ull,
        .highest_contiguous_sequence = 0x6162636465666768ull,
        .disposition = AckDisposition::kNackWithHighest,
    };
}

TEST(ControlPayloadCodecTest, AckGoldenVectorIsFixedBigEndian) {
    const AckPayload ack = GoldenAck();
    auto encoded = ControlPayloadCodec::EncodeAck(ack);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    EXPECT_EQ(encoded->size(), kAckPayloadWireSize);
    EXPECT_EQ(Hex(*encoded),
              "0001020100000000"
              "0102030405060708"
              "1112131415161718"
              "2122232425262728"
              "3132333435363738"
              "4142434445464748"
              "5152535455565758"
              "6162636465666768");

    auto decoded = ControlPayloadCodec::DecodeAck(*encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(*decoded, ack);
}

TEST(ControlPayloadCodecTest, AckRejectsEveryTruncationAndNoncanonicalFields) {
    auto encoded = ControlPayloadCodec::EncodeAck(GoldenAck());
    ASSERT_TRUE(encoded.ok());
    for (size_t length = 0; length < encoded->size(); ++length) {
        auto decoded = ControlPayloadCodec::DecodeAck(
            std::span<const std::byte>(*encoded).first(length));
        ASSERT_FALSE(decoded.ok()) << "length=" << length;
        EXPECT_EQ(decoded.status().code(), StatusCode::kCorruption);
    }
    encoded->push_back(std::byte{0});
    EXPECT_FALSE(ControlPayloadCodec::DecodeAck(*encoded).ok());

    encoded->resize(kAckPayloadWireSize);
    (*encoded)[3] = std::byte{0};
    auto missing_highest = ControlPayloadCodec::DecodeAck(*encoded);
    ASSERT_FALSE(missing_highest.ok());
    EXPECT_EQ(missing_highest.status().code(), StatusCode::kCorruption);
}

TEST(ControlPayloadCodecTest, AcceptedAckCanonicalizesAbsentHighest) {
    AckPayload ack = GoldenAck();
    ack.disposition = AckDisposition::kAccepted;
    ack.highest_contiguous_sequence.reset();
    auto encoded = ControlPayloadCodec::EncodeAck(ack);
    ASSERT_TRUE(encoded.ok());
    EXPECT_EQ(static_cast<uint8_t>((*encoded)[3]), 0u);
    for (size_t i = 56; i < 64; ++i) EXPECT_EQ((*encoded)[i], std::byte{0});
    auto decoded = ControlPayloadCodec::DecodeAck(*encoded);
    ASSERT_TRUE(decoded.ok());
    EXPECT_EQ(*decoded, ack);
}

TEST(ControlPayloadCodecTest, SessionHelloGoldenRoundTripAndTruncation) {
    SessionHello hello;
    hello.sender_session_epoch = 0x0102030405060708ull;
    hello.receiver_session_epoch = 0x1112131415161718ull;
    hello.dedup_state_lost = true;
    hello.sources.push_back(SessionHelloSource{
        .source = SourceIdentity{1, 2, 3},
        .last_accepted_sequence = 4,
    });
    auto encoded = ControlPayloadCodec::EncodeSessionHello(hello);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    EXPECT_EQ(Hex(*encoded),
              "0001000100000001"
              "0102030405060708"
              "1112131415161718"
              "0000000000000001"
              "0000000000000002"
              "0000000000000003"
              "0000000000000004");
    auto decoded = ControlPayloadCodec::DecodeSessionHello(*encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(*decoded, hello);

    for (size_t length = 0; length < encoded->size(); ++length) {
        auto truncated = ControlPayloadCodec::DecodeSessionHello(
            std::span<const std::byte>(*encoded).first(length));
        EXPECT_FALSE(truncated.ok()) << "length=" << length;
    }
}

TEST(ControlPayloadCodecTest, SessionHelloChecksLimitsBeforeEntryAllocation) {
    std::vector<std::byte> declared_two(kSessionHelloHeaderWireSize);
    declared_two[0] = std::byte{0};
    declared_two[1] = std::byte{1};
    declared_two[7] = std::byte{2};
    ControlPayloadLimits limits{.max_hello_sources = 1,
                                .max_hello_payload_bytes = 1024};
    auto decoded = ControlPayloadCodec::DecodeSessionHello(declared_two, limits);
    ASSERT_FALSE(decoded.ok());
    EXPECT_EQ(decoded.status().code(), StatusCode::kResourceExhausted);

    SessionHello duplicate;
    duplicate.sender_session_epoch = 1;
    duplicate.receiver_session_epoch = 2;
    duplicate.sources = {
        SessionHelloSource{SourceIdentity{1, 2, 3}, 4},
        SessionHelloSource{SourceIdentity{1, 2, 3}, 5},
    };
    auto encoded = ControlPayloadCodec::EncodeSessionHello(duplicate);
    ASSERT_FALSE(encoded.ok());
    EXPECT_EQ(encoded.status().code(), StatusCode::kInvalidArgument);
}

TEST(ControlPayloadCodecTest,
     SessionDiscoveryGoldenVectorAndRejectsIdentityOrVersionMismatch) {
    const SessionDiscovery discovery{
        .session_epoch = 0x0102030405060708ull,
        .node_id = NodeId{0x1112131415161718ull},
        .process_identity = ProcessIdentity{
            .node_id = 0x1112131415161718ull,
            .process_id = 0x2122232425262728ull,
            .process_epoch = 0x3132333435363738ull,
            .start_time_ns = 0x4142434445464748ull,
        },
        .lease_epoch = 0x5152535455565758ull,
        .node_config_version = 0x6162636465666768ull,
    };
    auto encoded = ControlPayloadCodec::EncodeSessionDiscovery(discovery);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    EXPECT_EQ(encoded->size(), kSessionDiscoveryPayloadWireSize);
    EXPECT_EQ(Hex(*encoded),
              "0001000000000000"
              "0102030405060708"
              "1112131415161718"
              "1112131415161718"
              "2122232425262728"
              "3132333435363738"
              "4142434445464748"
              "5152535455565758"
              "6162636465666768");
    auto decoded = ControlPayloadCodec::DecodeSessionDiscovery(*encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(*decoded, discovery);

    for (size_t length = 0; length < encoded->size(); ++length) {
        auto truncated = ControlPayloadCodec::DecodeSessionDiscovery(
            std::span<const std::byte>(*encoded).first(length));
        EXPECT_FALSE(truncated.ok()) << "length=" << length;
    }
    (*encoded)[1] = std::byte{2};
    EXPECT_EQ(ControlPayloadCodec::DecodeSessionDiscovery(*encoded)
                  .status()
                  .code(),
              StatusCode::kCorruption);

    SessionDiscovery mismatched = discovery;
    mismatched.process_identity.node_id = 99;
    auto rejected =
        ControlPayloadCodec::EncodeSessionDiscovery(mismatched);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kInvalidArgument);
}

TEST(ControlPayloadCodecTest, SessionHelloRejectsZeroEpochDiscoveryOverload) {
    SessionHello hello;
    hello.sender_session_epoch = 1;
    auto encoded = ControlPayloadCodec::EncodeSessionHello(hello);
    ASSERT_FALSE(encoded.ok());
    EXPECT_EQ(encoded.status().code(), StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino::bridge
