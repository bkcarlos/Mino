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

TEST(SourceIdentityTest, StableBridgeLaneHashHasFixedGoldenVectors) {
    EXPECT_EQ(StableBridgeLaneHash(SourceIdentity{0, 0, 0}),
              0x6d688654e9834407ull);
    EXPECT_EQ(StableBridgeLaneHash(SourceIdentity{1, 2, 3}),
              0xd49e55b95607803eull);
    EXPECT_EQ(StableBridgeLaneHash(SourceIdentity{
                  0x0102030405060708ull,
                  0x1112131415161718ull,
                  0x2122232425262728ull,
              }),
              0x1ec8bfdc3a8ab631ull);
}

TEST(SourceIdentityTest, BridgeLaneSelectionIsStableAndBounded) {
    const SourceIdentity source{
        0x0102030405060708ull,
        0x1112131415161718ull,
        0x2122232425262728ull,
    };
    EXPECT_EQ(BridgeLaneFor(source, 0), 0);
    EXPECT_EQ(BridgeLaneFor(source, 1), 0);
    EXPECT_EQ(BridgeLaneFor(source, kMaxBridgeLaneCount + 1), 0);

    constexpr uint16_t kExpectedLaneByCount[] = {0, 0, 1, 0, 1, 3, 3, 5, 1};
    for (uint16_t lane_count = 2; lane_count <= kMaxBridgeLaneCount;
         ++lane_count) {
        EXPECT_EQ(BridgeLaneFor(source, lane_count),
                  kExpectedLaneByCount[lane_count])
            << "lane_count=" << lane_count;
    }
}

TEST(SourceIdentityTest, StructuredIdentitiesUseEveryFourLaneBucket) {
    std::array<uint32_t, 4> lane_counts{};
    for (uint64_t topic = 1; topic <= 32; ++topic) {
        const SourceIdentity source{
            .node_id = 101,
            .publisher_id = 1'050'500 + topic,
            .publisher_epoch = 3'050'500 + topic,
        };
        ++lane_counts[BridgeLaneFor(source, 4)];
    }
    for (uint32_t count : lane_counts) EXPECT_GT(count, 0u);
}

TEST(SourceIdentityTest, EveryIdentityFieldAffectsStableLaneHash) {
    const SourceIdentity source{1, 2, 3};
    EXPECT_NE(StableBridgeLaneHash(source),
              StableBridgeLaneHash(SourceIdentity{9, 2, 3}));
    EXPECT_NE(StableBridgeLaneHash(source),
              StableBridgeLaneHash(SourceIdentity{1, 9, 3}));
    EXPECT_NE(StableBridgeLaneHash(source),
              StableBridgeLaneHash(SourceIdentity{1, 2, 9}));
    // Sequence and topic are absent by design: only SourceIdentity participates.
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

SessionDiscovery GoldenDiscovery() {
    return SessionDiscovery{
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
}

TEST(ControlPayloadCodecTest,
     SessionDiscoveryGoldenVectorAndRejectsIdentityMismatch) {
    const SessionDiscovery discovery = GoldenDiscovery();
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
    SessionDiscovery mismatched = discovery;
    mismatched.process_identity.node_id = 99;
    auto rejected =
        ControlPayloadCodec::EncodeSessionDiscovery(mismatched);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kInvalidArgument);
}

TEST(ControlPayloadCodecTest, SessionDiscoveryV2GoldenVectorIsFixedBigEndian) {
    SessionDiscovery discovery = GoldenDiscovery();
    discovery.lane_index = 3;
    discovery.lane_count = 8;

    auto encoded = ControlPayloadCodec::EncodeSessionDiscovery(discovery);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    EXPECT_EQ(encoded->size(), kSessionDiscoveryPayloadWireSize);
    EXPECT_EQ(Hex(*encoded),
              "0002000000030008"
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
}

TEST(ControlPayloadCodecTest, SessionDiscoveryRejectsInvalidLaneCombinations) {
    const auto expect_encode_rejected = [](uint16_t lane_index,
                                           uint16_t lane_count) {
        SessionDiscovery discovery = GoldenDiscovery();
        discovery.lane_index = lane_index;
        discovery.lane_count = lane_count;
        auto encoded = ControlPayloadCodec::EncodeSessionDiscovery(discovery);
        ASSERT_FALSE(encoded.ok());
        EXPECT_EQ(encoded.status().code(), StatusCode::kInvalidArgument);
    };
    expect_encode_rejected(0, 0);
    expect_encode_rejected(1, 1);
    expect_encode_rejected(2, 2);
    expect_encode_rejected(0, kMaxBridgeLaneCount + 1);

    SessionDiscovery v2_discovery = GoldenDiscovery();
    v2_discovery.lane_index = 1;
    v2_discovery.lane_count = 2;
    auto v2 = ControlPayloadCodec::EncodeSessionDiscovery(v2_discovery);
    ASSERT_TRUE(v2.ok());

    const auto expect_decode_rejected = [](std::vector<std::byte> bytes) {
        auto decoded = ControlPayloadCodec::DecodeSessionDiscovery(bytes);
        ASSERT_FALSE(decoded.ok());
        EXPECT_EQ(decoded.status().code(), StatusCode::kCorruption);
    };

    auto malformed = *v2;
    malformed[2] = std::byte{1};
    expect_decode_rejected(malformed);

    malformed = *v2;
    malformed[4] = std::byte{0};
    malformed[5] = std::byte{0};
    malformed[6] = std::byte{0};
    malformed[7] = std::byte{1};
    expect_decode_rejected(malformed);

    malformed = *v2;
    malformed[4] = std::byte{0};
    malformed[5] = std::byte{2};
    expect_decode_rejected(malformed);

    malformed = *v2;
    malformed[6] = std::byte{0};
    malformed[7] = std::byte{9};
    expect_decode_rejected(malformed);

    malformed = *v2;
    malformed[1] = std::byte{3};
    expect_decode_rejected(malformed);

    auto legacy = ControlPayloadCodec::EncodeSessionDiscovery(GoldenDiscovery());
    ASSERT_TRUE(legacy.ok());
    for (size_t offset = 2; offset < 8; ++offset) {
        malformed = *legacy;
        malformed[offset] = std::byte{1};
        expect_decode_rejected(malformed);
    }
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
