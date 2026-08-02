// Copyright 2026 The Mino Authors

#include "mino/bridge/schema_negotiator.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <new>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"

namespace mino::bridge {
namespace {

struct TestSchema {
    schema::SchemaHandle handle;
    std::string artifact;
};

Result<TestSchema> BuildSchema(std::string_view type_name) {
    const std::string idl =
        "option schema_version = \"1.0\"; package negotiation; message " +
        std::string(type_name) + " { uint32 value = 1; }";
    auto compiled = schema::SchemaCompiler::Compile(idl);
    if (!compiled.ok()) return compiled.status();
    if (compiled->types().size() != 1) {
        return Status::Error(StatusCode::kInternal,
                             "test schema did not compile to one type");
    }
    auto layout = schema::LayoutPlanner::Plan(*compiled->types()[0]);
    if (!layout.ok()) return layout.status();
    const std::array<schema::LayoutPlan, 1> layouts = {std::move(*layout)};
    auto artifact =
        schema::codegen::EncodeDescriptorArtifact(*compiled, layouts);
    if (!artifact.ok()) return artifact.status();
    return TestSchema{compiled->types()[0], std::move(*artifact)};
}

std::span<const std::byte> ArtifactBytes(const std::string& artifact) {
    return std::as_bytes(
        std::span<const char>(artifact.data(), artifact.size()));
}

std::vector<std::byte> ArtifactVector(const std::string& artifact) {
    const auto bytes = ArtifactBytes(artifact);
    return std::vector<std::byte>(bytes.begin(), bytes.end());
}

schema::CanonicalDigest Digest(uint8_t seed) {
    schema::CanonicalDigest digest{};
    for (size_t i = 0; i < digest.size(); ++i) {
        digest[i] = static_cast<std::byte>(seed + static_cast<uint8_t>(i));
    }
    return digest;
}

schema::SchemaIdentity Identity(uint8_t seed, uint32_t schema_version = 0x10000,
                                uint32_t layout_version = 1) {
    auto digest = Digest(seed);
    return schema::SchemaIdentity(schema::DigestShortId(digest), digest,
                                  schema_version, layout_version);
}

WireFrame AnnouncementFrame(uint32_t schema_ref,
                            const schema::SchemaIdentity& identity,
                            std::vector<std::byte> artifact = {}) {
    auto payload = SchemaControlCodec::EncodeAnnouncement(
        SchemaAnnouncement(schema_ref, identity, std::move(artifact)));
    EXPECT_TRUE(payload.ok()) << payload.status().ToString();
    WireFrame frame;
    frame.header.frame_type = FrameType::kSchemaAnnounce;
    frame.header.flags = FlagValue(FrameFlag::kControlFrame);
    if (payload.ok()) frame.payload = std::move(*payload);
    return frame;
}

WireFrame RequestFrame(const SchemaRequest& request) {
    auto payload = SchemaControlCodec::EncodeRequest(request);
    EXPECT_TRUE(payload.ok()) << payload.status().ToString();
    WireFrame frame;
    frame.header.frame_type = FrameType::kSchemaRequest;
    frame.header.flags = FlagValue(FrameFlag::kControlFrame);
    if (payload.ok()) frame.payload = std::move(*payload);
    return frame;
}

WireFrame DataFrame(uint32_t schema_ref,
                    const schema::SchemaIdentity& identity,
                    size_t payload_size = 1) {
    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    frame.header.connection_schema_ref = schema_ref;
    frame.header.msg_type = static_cast<uint32_t>(identity.short_id());
    frame.header.schema_version = identity.schema_version();
    frame.header.layout_version = identity.layout_version();
    frame.payload.assign(payload_size, std::byte{0x5a});
    return frame;
}

uint32_t AnnouncementRef(const WireFrame& frame) {
    auto announcement =
        SchemaControlCodec::DecodeAnnouncement(frame.payload);
    EXPECT_TRUE(announcement.ok()) << announcement.status().ToString();
    return announcement.ok() ? announcement->connection_schema_ref : 0;
}

class TestAuth final : public DescriptorAuth {
public:
    Status Authenticate(const schema::SchemaIdentity&,
                        std::span<const std::byte>) override {
        ++calls;
        if (throw_bad_alloc) throw std::bad_alloc();
        return status;
    }

    Status status = Status::Ok();
    size_t calls = 0;
    bool throw_bad_alloc = false;
};

class TestPersistence final : public DescriptorPersistence {
public:
    Status Persist(const schema::SchemaIdentity&,
                   std::span<const std::byte>) override {
        ++calls;
        if (throw_bad_alloc) throw std::bad_alloc();
        return status;
    }

    Status status = Status::Ok();
    size_t calls = 0;
    bool throw_bad_alloc = false;
};

TEST(SchemaControlCodecTest, UsesExplicitBigEndianAndRoundTrips) {
    const auto identity = Identity(0x10, 0x00020003u, 0x11223344u);
    const SchemaAnnouncement announcement(
        0x01020304u, identity,
        {std::byte{0xaa}, std::byte{0xbb}, std::byte{0xcc}});
    auto encoded = SchemaControlCodec::EncodeAnnouncement(announcement);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    ASSERT_EQ(encoded->size(), 63u);
    EXPECT_EQ((*encoded)[0], std::byte{0x00});
    EXPECT_EQ((*encoded)[3], std::byte{0x01});
    EXPECT_EQ((*encoded)[4], std::byte{0x01});
    EXPECT_EQ((*encoded)[5], std::byte{0x02});
    EXPECT_EQ((*encoded)[6], std::byte{0x03});
    EXPECT_EQ((*encoded)[7], std::byte{0x04});
    EXPECT_EQ((*encoded)[48], std::byte{0x00});
    EXPECT_EQ((*encoded)[49], std::byte{0x02});
    EXPECT_EQ((*encoded)[50], std::byte{0x00});
    EXPECT_EQ((*encoded)[51], std::byte{0x03});
    EXPECT_EQ((*encoded)[52], std::byte{0x11});
    EXPECT_EQ((*encoded)[55], std::byte{0x44});
    EXPECT_EQ((*encoded)[59], std::byte{0x03});

    auto decoded = SchemaControlCodec::DecodeAnnouncement(*encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(decoded->connection_schema_ref, 0x01020304u);
    EXPECT_EQ(decoded->identity.canonical_digest(),
              identity.canonical_digest());
    EXPECT_EQ(decoded->identity.short_id(), identity.short_id());
    EXPECT_EQ(decoded->descriptor_artifact,
              announcement.descriptor_artifact);

    auto request = SchemaControlCodec::EncodeRequest(
        SchemaRequest::ByRef(0x11223344u));
    ASSERT_TRUE(request.ok()) << request.status().ToString();
    ASSERT_EQ(request->size(), 12u);
    EXPECT_EQ((*request)[8], std::byte{0x11});
    EXPECT_EQ((*request)[11], std::byte{0x44});
    auto decoded_request = SchemaControlCodec::DecodeRequest(*request);
    ASSERT_TRUE(decoded_request.ok());
    EXPECT_EQ(decoded_request->kind, SchemaRequestKind::kByRef);
    EXPECT_EQ(decoded_request->connection_schema_ref, 0x11223344u);
}

TEST(SchemaControlCodecTest, RejectsTruncationTrailingBytesAndUnknownVersion) {
    auto encoded = SchemaControlCodec::EncodeAnnouncement(
        SchemaAnnouncement(1, Identity(1)));
    ASSERT_TRUE(encoded.ok());

    for (size_t size : {0u, 3u, 59u}) {
        auto decoded = SchemaControlCodec::DecodeAnnouncement(
            std::span<const std::byte>(*encoded).first(size));
        ASSERT_FALSE(decoded.ok());
        EXPECT_EQ(decoded.status().code(), StatusCode::kCorruption);
    }
    encoded->push_back(std::byte{0});
    auto trailing = SchemaControlCodec::DecodeAnnouncement(*encoded);
    ASSERT_FALSE(trailing.ok());
    EXPECT_EQ(trailing.status().code(), StatusCode::kCorruption);

    auto request = SchemaControlCodec::EncodeRequest(SchemaRequest::ByRef(7));
    ASSERT_TRUE(request.ok());
    (*request)[3] = std::byte{2};
    auto unknown = SchemaControlCodec::DecodeRequest(*request);
    ASSERT_FALSE(unknown.ok());
    EXPECT_EQ(unknown.status().code(), StatusCode::kUnsupported);
    (*request)[3] = std::byte{1};
    request->push_back(std::byte{0});
    auto request_trailing = SchemaControlCodec::DecodeRequest(*request);
    ASSERT_FALSE(request_trailing.ok());
    EXPECT_EQ(request_trailing.status().code(), StatusCode::kCorruption);
}

TEST(SchemaNegotiatorTest, MapsFullIdentityAndRejectsRefReuseOrRegression) {
    auto first = BuildSchema("First");
    auto second = BuildSchema("Second");
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    schema::SchemaRegistry registry;
    ASSERT_TRUE(registry.RegisterDescriptor(first->handle).ok());
    ASSERT_TRUE(registry.RegisterDescriptor(second->handle).ok());
    TestAuth auth;
    TestPersistence persistence;
    SchemaNegotiator negotiator(&registry, &auth, &persistence);

    auto announced = negotiator.HandleControlFrame(
        AnnouncementFrame(2, first->handle->identity()), 0);
    ASSERT_TRUE(announced.ok()) << announced.status().ToString();
    auto mapped = negotiator.IdentityForRemoteRef(2);
    ASSERT_TRUE(mapped.ok());
    EXPECT_EQ(mapped->canonical_digest(),
              first->handle->identity().canonical_digest());
    EXPECT_EQ(negotiator.remote_ref_high_watermark(), 2u);

    auto duplicate = negotiator.HandleControlFrame(
        AnnouncementFrame(2, first->handle->identity()), 0);
    EXPECT_TRUE(duplicate.ok()) << duplicate.status().ToString();
    auto rebound = negotiator.HandleControlFrame(
        AnnouncementFrame(2, second->handle->identity()), 0);
    ASSERT_FALSE(rebound.ok());
    EXPECT_EQ(rebound.status().code(), StatusCode::kSchemaMismatch);
    auto regressed = negotiator.HandleControlFrame(
        AnnouncementFrame(1, second->handle->identity()), 0);
    ASSERT_FALSE(regressed.ok());
    EXPECT_EQ(regressed.status().code(), StatusCode::kSchemaMismatch);
}

TEST(SchemaNegotiatorTest, BuffersUnknownRefDeduplicatesAndReleasesAfterArtifact) {
    auto test_schema = BuildSchema("Buffered");
    ASSERT_TRUE(test_schema.ok()) << test_schema.status().ToString();
    schema::SchemaRegistry registry;
    TestAuth auth;
    TestPersistence persistence;
    SchemaNegotiator negotiator(&registry, &auth, &persistence);

    auto first = negotiator.HandleDataFrame(
        DataFrame(7, test_schema->handle->identity()), 100);
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_EQ(first->outbound_control_frames.size(), 1u);
    auto request = SchemaControlCodec::DecodeRequest(
        first->outbound_control_frames[0].payload);
    ASSERT_TRUE(request.ok());
    EXPECT_EQ(request->kind, SchemaRequestKind::kByRef);
    EXPECT_EQ(request->connection_schema_ref, 7u);
    ASSERT_TRUE(negotiator
                    .ConfirmControlQueued(first->outbound_control_frames[0])
                    .ok());

    auto second = negotiator.HandleDataFrame(
        DataFrame(7, test_schema->handle->identity()), 100);
    ASSERT_TRUE(second.ok());
    EXPECT_TRUE(second->outbound_control_frames.empty());
    EXPECT_EQ(negotiator.pending_request_count(), 1u);
    EXPECT_EQ(negotiator.buffered_frames(), 2u);

    auto resolved = negotiator.HandleControlFrame(
        AnnouncementFrame(7, test_schema->handle->identity(),
                          ArtifactVector(test_schema->artifact)),
        100);
    ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
    EXPECT_EQ(resolved->ready_frames.size(), 2u);
    EXPECT_EQ(negotiator.buffered_frames(), 0u);
    EXPECT_EQ(negotiator.buffered_bytes(), 0u);
    EXPECT_EQ(negotiator.pending_request_count(), 0u);
    EXPECT_EQ(auth.calls, 1u);
    EXPECT_EQ(persistence.calls, 1u);
    EXPECT_TRUE(registry.Find(test_schema->handle->identity()).ok());
}

TEST(SchemaNegotiatorTest, DeduplicatesConcurrentDigestRequestsAcrossRefs) {
    auto test_schema = BuildSchema("DigestDedup");
    ASSERT_TRUE(test_schema.ok());
    schema::SchemaRegistry registry;
    TestAuth auth;
    TestPersistence persistence;
    SchemaNegotiator negotiator(&registry, &auth, &persistence);

    auto first = negotiator.HandleControlFrame(
        AnnouncementFrame(1, test_schema->handle->identity()), 10);
    ASSERT_TRUE(first.ok());
    ASSERT_EQ(first->outbound_control_frames.size(), 1u);
    auto request = SchemaControlCodec::DecodeRequest(
        first->outbound_control_frames[0].payload);
    ASSERT_TRUE(request.ok());
    EXPECT_EQ(request->kind, SchemaRequestKind::kByDigest);
    ASSERT_TRUE(negotiator
                    .ConfirmControlQueued(first->outbound_control_frames[0])
                    .ok());

    auto second = negotiator.HandleControlFrame(
        AnnouncementFrame(2, test_schema->handle->identity()), 10);
    ASSERT_TRUE(second.ok());
    EXPECT_TRUE(second->outbound_control_frames.empty());
    EXPECT_EQ(negotiator.pending_request_count(), 1u);
}

TEST(SchemaNegotiatorTest,
     RequestStateCommitsOnlyAfterQueueConfirmationAndRetriesAdmission) {
    schema::SchemaRegistry registry;
    TestAuth auth;
    TestPersistence persistence;
    SchemaNegotiatorLimits limits;
    limits.max_distinct_requests_per_window = 1;
    SchemaNegotiator negotiator(&registry, &auth, &persistence, limits);
    const auto identity = Identity(4);

    auto first = negotiator.HandleDataFrame(DataFrame(9, identity), 50);
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_EQ(first->outbound_control_frames.size(), 1u);
    EXPECT_EQ(negotiator.pending_request_count(), 1u);

    // Simulate queue admission failure by withholding confirmation. The exact
    // same pending control is returned and no additional budget is consumed.
    auto retry = negotiator.HandleDataFrame(DataFrame(9, identity), 50);
    ASSERT_TRUE(retry.ok()) << retry.status().ToString();
    ASSERT_EQ(retry->outbound_control_frames.size(), 1u);
    EXPECT_EQ(retry->outbound_control_frames[0],
              first->outbound_control_frames[0]);

    WireFrame wrong = RequestFrame(SchemaRequest::ByRef(10));
    auto wrong_confirmation = negotiator.ConfirmControlQueued(wrong);
    ASSERT_FALSE(wrong_confirmation.ok());
    EXPECT_EQ(wrong_confirmation.code(), StatusCode::kInvalidArgument);

    ASSERT_TRUE(negotiator
                    .ConfirmControlQueued(retry->outbound_control_frames[0])
                    .ok());
    auto deduplicated =
        negotiator.HandleDataFrame(DataFrame(9, identity), 50);
    ASSERT_TRUE(deduplicated.ok());
    EXPECT_TRUE(deduplicated->outbound_control_frames.empty());

    auto limited = negotiator.HandleDataFrame(DataFrame(10, identity), 50);
    ASSERT_FALSE(limited.ok());
    EXPECT_EQ(limited.status().code(), StatusCode::kResourceExhausted);

    auto next_window = negotiator.HandleDataFrame(
        DataFrame(10, identity), 50 + limits.request_window_ns);
    ASSERT_TRUE(next_window.ok()) << next_window.status().ToString();
    EXPECT_EQ(next_window->outbound_control_frames.size(), 1u);
}

TEST(SchemaNegotiatorTest,
     ProvisionalRequestsTrackMultipleRefsAndConfirmIndependently) {
    schema::SchemaRegistry registry;
    TestAuth auth;
    TestPersistence persistence;
    SchemaNegotiatorLimits limits;
    limits.max_distinct_requests_per_window = 2;
    SchemaNegotiator negotiator(&registry, &auth, &persistence, limits);
    const auto identity = Identity(5);

    auto ref1 = negotiator.HandleDataFrame(DataFrame(1, identity), 50);
    auto ref2 = negotiator.HandleDataFrame(DataFrame(2, identity), 50);
    auto ref1_retry = negotiator.HandleDataFrame(DataFrame(1, identity), 50);
    ASSERT_TRUE(ref1.ok()) << ref1.status().ToString();
    ASSERT_TRUE(ref2.ok()) << ref2.status().ToString();
    ASSERT_TRUE(ref1_retry.ok()) << ref1_retry.status().ToString();
    ASSERT_EQ(ref1->outbound_control_frames.size(), 1u);
    ASSERT_EQ(ref2->outbound_control_frames.size(), 1u);
    ASSERT_EQ(ref1_retry->outbound_control_frames.size(), 1u);
    EXPECT_NE(ref1->outbound_control_frames[0],
              ref2->outbound_control_frames[0]);
    EXPECT_EQ(ref1_retry->outbound_control_frames[0],
              ref1->outbound_control_frames[0]);
    EXPECT_EQ(negotiator.pending_request_count(), 2u);
    auto provisional_limited =
        negotiator.HandleDataFrame(DataFrame(3, identity), 50);
    ASSERT_FALSE(provisional_limited.ok());
    EXPECT_EQ(provisional_limited.status().code(),
              StatusCode::kResourceExhausted);

    auto first_request = SchemaControlCodec::DecodeRequest(
        ref1->outbound_control_frames[0].payload);
    auto second_request = SchemaControlCodec::DecodeRequest(
        ref2->outbound_control_frames[0].payload);
    ASSERT_TRUE(first_request.ok());
    ASSERT_TRUE(second_request.ok());
    EXPECT_EQ(first_request->connection_schema_ref, 1u);
    EXPECT_EQ(second_request->connection_schema_ref, 2u);

    // Confirm ref2 first. Ref1 remains provisional and retryable while ref2 is
    // now committed and deduplicated.
    ASSERT_TRUE(negotiator
                    .ConfirmControlQueued(ref2->outbound_control_frames[0])
                    .ok());
    auto ref2_deduplicated =
        negotiator.HandleDataFrame(DataFrame(2, identity), 50);
    auto ref1_still_pending =
        negotiator.HandleDataFrame(DataFrame(1, identity), 50);
    ASSERT_TRUE(ref2_deduplicated.ok());
    ASSERT_TRUE(ref1_still_pending.ok());
    EXPECT_TRUE(ref2_deduplicated->outbound_control_frames.empty());
    ASSERT_EQ(ref1_still_pending->outbound_control_frames.size(), 1u);
    EXPECT_EQ(ref1_still_pending->outbound_control_frames[0],
              ref1->outbound_control_frames[0]);
    EXPECT_EQ(negotiator.pending_request_count(), 2u);

    ASSERT_TRUE(negotiator
                    .ConfirmControlQueued(
                        ref1_still_pending->outbound_control_frames[0])
                    .ok());
    auto ref1_deduplicated =
        negotiator.HandleDataFrame(DataFrame(1, identity), 50);
    ASSERT_TRUE(ref1_deduplicated.ok());
    EXPECT_TRUE(ref1_deduplicated->outbound_control_frames.empty());
    EXPECT_EQ(negotiator.pending_request_count(), 2u);
}

TEST(SchemaNegotiatorTest, LimitsDistinctRequestsInFixedWindow) {
    schema::SchemaRegistry registry;
    TestAuth auth;
    TestPersistence persistence;
    SchemaNegotiatorLimits limits;
    limits.max_distinct_requests_per_window = 2;
    SchemaNegotiator negotiator(&registry, &auth, &persistence, limits);
    const auto identity = Identity(5);

    auto first = negotiator.HandleDataFrame(DataFrame(1, identity), 50);
    ASSERT_TRUE(first.ok());
    ASSERT_EQ(first->outbound_control_frames.size(), 1u);
    ASSERT_TRUE(negotiator
                    .ConfirmControlQueued(first->outbound_control_frames[0])
                    .ok());
    auto second = negotiator.HandleDataFrame(DataFrame(2, identity), 50);
    ASSERT_TRUE(second.ok());
    ASSERT_EQ(second->outbound_control_frames.size(), 1u);
    ASSERT_TRUE(negotiator
                    .ConfirmControlQueued(second->outbound_control_frames[0])
                    .ok());
    auto limited = negotiator.HandleDataFrame(DataFrame(3, identity), 50);
    ASSERT_FALSE(limited.ok());
    EXPECT_EQ(limited.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(negotiator.buffered_frames(), 2u);

    auto next_window = negotiator.HandleDataFrame(
        DataFrame(3, identity), 50 + limits.request_window_ns);
    ASSERT_TRUE(next_window.ok()) << next_window.status().ToString();
    EXPECT_EQ(next_window->outbound_control_frames.size(), 1u);
    EXPECT_EQ(negotiator.buffered_frames(), 3u);
}

TEST(SchemaNegotiatorTest, BufferOverflowFailsConnectionUntilReset) {
    schema::SchemaRegistry registry;
    TestAuth auth;
    TestPersistence persistence;
    SchemaNegotiatorLimits limits;
    limits.max_buffered_frames = 1;
    SchemaNegotiator negotiator(&registry, &auth, &persistence, limits);
    const auto identity = Identity(6);

    ASSERT_TRUE(negotiator.HandleDataFrame(DataFrame(1, identity), 0).ok());
    auto overflow = negotiator.HandleDataFrame(DataFrame(1, identity), 0);
    ASSERT_FALSE(overflow.ok());
    EXPECT_EQ(overflow.status().code(), StatusCode::kResourceExhausted);
    EXPECT_TRUE(negotiator.failed());
    EXPECT_EQ(negotiator.buffered_frames(), 0u);
    auto failed = negotiator.HandleDataFrame(DataFrame(1, identity), 0);
    ASSERT_FALSE(failed.ok());
    EXPECT_EQ(failed.status().code(), StatusCode::kUnavailable);

    negotiator.Reset();
    EXPECT_FALSE(negotiator.failed());
    EXPECT_TRUE(negotiator.HandleDataFrame(DataFrame(1, identity), 0).ok());
}

TEST(SchemaNegotiatorTest, AuthenticationAndPersistenceAreFailClosed) {
    auto test_schema = BuildSchema("FailClosed");
    ASSERT_TRUE(test_schema.ok());
    const WireFrame announcement = AnnouncementFrame(
        1, test_schema->handle->identity(),
        ArtifactVector(test_schema->artifact));

    {
        schema::SchemaRegistry registry;
        TestPersistence persistence;
        SchemaNegotiator negotiator(&registry, nullptr, &persistence);
        auto missing = negotiator.HandleControlFrame(announcement, 0);
        ASSERT_FALSE(missing.ok());
        EXPECT_EQ(missing.status().code(), StatusCode::kPermissionDenied);
        EXPECT_EQ(registry.size(), 0u);
    }
    {
        schema::SchemaRegistry registry;
        TestAuth auth;
        TestPersistence persistence;
        auth.status = Status::Error(StatusCode::kPermissionDenied, "rejected");
        SchemaNegotiator negotiator(&registry, &auth, &persistence);
        auto rejected = negotiator.HandleControlFrame(announcement, 0);
        ASSERT_FALSE(rejected.ok());
        EXPECT_EQ(rejected.status().code(), StatusCode::kPermissionDenied);
        EXPECT_EQ(persistence.calls, 0u);
        EXPECT_EQ(registry.size(), 0u);
    }
    {
        schema::SchemaRegistry registry;
        TestAuth auth;
        TestPersistence persistence;
        persistence.status =
            Status::Error(StatusCode::kUnavailable, "disk unavailable");
        SchemaNegotiator negotiator(&registry, &auth, &persistence);
        ASSERT_TRUE(negotiator
                        .HandleDataFrame(
                            DataFrame(1, test_schema->handle->identity()), 0)
                        .ok());
        auto rejected = negotiator.HandleControlFrame(announcement, 0);
        ASSERT_FALSE(rejected.ok());
        EXPECT_EQ(rejected.status().code(), StatusCode::kUnavailable);
        EXPECT_EQ(negotiator.buffered_frames(), 1u);
        EXPECT_EQ(registry.size(), 0u);

        auto bypass = negotiator.HandleControlFrame(
            AnnouncementFrame(1, test_schema->handle->identity()), 0);
        ASSERT_TRUE(bypass.ok()) << bypass.status().ToString();
        EXPECT_TRUE(bypass->ready_frames.empty());
        ASSERT_EQ(bypass->outbound_control_frames.size(), 1u);
        auto digest_request = SchemaControlCodec::DecodeRequest(
            bypass->outbound_control_frames[0].payload);
        ASSERT_TRUE(digest_request.ok());
        EXPECT_EQ(digest_request->kind, SchemaRequestKind::kByDigest);
        EXPECT_EQ(negotiator.buffered_frames(), 1u);

        negotiator.Reset();
        auto after_reset = negotiator.HandleControlFrame(
            AnnouncementFrame(1, test_schema->handle->identity()), 1);
        ASSERT_TRUE(after_reset.ok()) << after_reset.status().ToString();
        EXPECT_TRUE(after_reset->ready_frames.empty());
        EXPECT_EQ(after_reset->outbound_control_frames.size(), 1u);

        SchemaNegotiator second_negotiator(&registry, &auth, &persistence);
        auto cross_connection = second_negotiator.HandleControlFrame(
            AnnouncementFrame(1, test_schema->handle->identity()), 1);
        ASSERT_TRUE(cross_connection.ok())
            << cross_connection.status().ToString();
        EXPECT_TRUE(cross_connection->ready_frames.empty());
        EXPECT_EQ(cross_connection->outbound_control_frames.size(), 1u);
        EXPECT_EQ(registry.size(), 0u);
    }
}

TEST(SchemaNegotiatorTest, CrossValidatesArtifactAndTranslatesBadAlloc) {
    auto artifact_schema = BuildSchema("ArtifactType");
    auto announced_schema = BuildSchema("AnnouncedType");
    ASSERT_TRUE(artifact_schema.ok());
    ASSERT_TRUE(announced_schema.ok());
    schema::SchemaRegistry registry;
    TestAuth auth;
    TestPersistence persistence;
    SchemaNegotiator negotiator(&registry, &auth, &persistence);

    auto mismatch = negotiator.HandleControlFrame(
        AnnouncementFrame(1, announced_schema->handle->identity(),
                          ArtifactVector(artifact_schema->artifact)),
        0);
    ASSERT_FALSE(mismatch.ok());
    EXPECT_EQ(mismatch.status().code(), StatusCode::kSchemaMismatch);
    EXPECT_EQ(persistence.calls, 0u);

    schema::SchemaRegistry second_registry;
    TestAuth throwing_auth;
    throwing_auth.throw_bad_alloc = true;
    SchemaNegotiator throwing(&second_registry, &throwing_auth, &persistence);
    auto exhausted = throwing.HandleControlFrame(
        AnnouncementFrame(1, artifact_schema->handle->identity(),
                          ArtifactVector(artifact_schema->artifact)),
        0);
    ASSERT_FALSE(exhausted.ok());
    EXPECT_EQ(exhausted.status().code(), StatusCode::kResourceExhausted);
}

TEST(SchemaNegotiatorTest, DescriptorFailsFastAtControlFrameAdmissionLimit) {
    auto test_schema = BuildSchema("ControlFrameLimit");
    ASSERT_TRUE(test_schema.ok()) << test_schema.status().ToString();
    schema::SchemaRegistry registry;
    ASSERT_TRUE(registry.RegisterDescriptor(test_schema->handle).ok());
    TestAuth auth;
    TestPersistence persistence;

    const size_t exact_frame_bytes =
        kWireBaseHeaderLength + kWireControlOpcodeLength +
        kSchemaAnnouncementFixedPayloadBytes + test_schema->artifact.size();
    SchemaNegotiatorLimits too_small;
    too_small.max_control_frame_bytes = exact_frame_bytes - 1;
    SchemaNegotiator rejected(&registry, &auth, &persistence, too_small);
    auto local = rejected.BindLocalSchema(test_schema->handle->identity(),
                                          ArtifactBytes(test_schema->artifact));
    ASSERT_FALSE(local.ok());
    EXPECT_EQ(local.status().code(), StatusCode::kResourceExhausted);

    auto remote = rejected.HandleControlFrame(
        AnnouncementFrame(1, test_schema->handle->identity(),
                          ArtifactVector(test_schema->artifact)),
        0);
    ASSERT_FALSE(remote.ok());
    EXPECT_EQ(remote.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(auth.calls, 0u);
    EXPECT_EQ(persistence.calls, 0u);

    SchemaNegotiatorLimits exact;
    exact.max_control_frame_bytes = exact_frame_bytes;
    SchemaNegotiator accepted(&registry, &auth, &persistence, exact);
    EXPECT_TRUE(accepted
                    .BindLocalSchema(test_schema->handle->identity(),
                                     ArtifactBytes(test_schema->artifact))
                    .ok());
}

TEST(SchemaNegotiatorTest, LocalRefsAreMonotonicNotReusedAndResetIsNewEpoch) {
    auto first = BuildSchema("LocalFirst");
    auto second = BuildSchema("LocalSecond");
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(second.ok());
    schema::SchemaRegistry registry;
    ASSERT_TRUE(registry.RegisterDescriptor(first->handle).ok());
    ASSERT_TRUE(registry.RegisterDescriptor(second->handle).ok());
    TestAuth auth;
    TestPersistence persistence;
    SchemaNegotiator negotiator(&registry, &auth, &persistence);

    auto ref1 = negotiator.BindLocalSchema(first->handle->identity(),
                                           ArtifactBytes(first->artifact));
    auto ref2 = negotiator.BindLocalSchema(second->handle->identity(),
                                           ArtifactBytes(second->artifact));
    auto same = negotiator.BindLocalSchema(first->handle->identity());
    ASSERT_TRUE(ref1.ok());
    ASSERT_TRUE(ref2.ok());
    ASSERT_TRUE(same.ok());
    EXPECT_EQ(AnnouncementRef(*ref1), 1u);
    EXPECT_EQ(AnnouncementRef(*ref2), 2u);
    EXPECT_EQ(AnnouncementRef(*same), 1u);
    EXPECT_EQ(negotiator.local_ref_high_watermark(), 2u);

    auto response = negotiator.HandleControlFrame(
        RequestFrame(SchemaRequest::ByDigest(
            first->handle->identity().canonical_digest())),
        0);
    ASSERT_TRUE(response.ok()) << response.status().ToString();
    ASSERT_EQ(response->outbound_control_frames.size(), 1u);
    auto with_artifact = SchemaControlCodec::DecodeAnnouncement(
        response->outbound_control_frames[0].payload);
    ASSERT_TRUE(with_artifact.ok());
    EXPECT_FALSE(with_artifact->descriptor_artifact.empty());

    negotiator.Reset();
    EXPECT_EQ(negotiator.local_ref_high_watermark(), 0u);
    EXPECT_EQ(negotiator.remote_ref_high_watermark(), 0u);
    EXPECT_EQ(negotiator.pending_request_count(), 0u);
    auto new_epoch = negotiator.BindLocalSchema(second->handle->identity());
    ASSERT_TRUE(new_epoch.ok());
    EXPECT_EQ(AnnouncementRef(*new_epoch), 1u);
}

}  // namespace
}  // namespace mino::bridge
