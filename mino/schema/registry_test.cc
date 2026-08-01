// Copyright 2026 The Mino Authors

#include "mino/schema/registry.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/layout.h"

namespace mino::schema {
namespace {

uint64_t ConstantShortIdIndex(const CanonicalDigest&) noexcept { return 7; }

SchemaHandle OnlyType(Result<CompiledSchema>& compiled) {
    EXPECT_TRUE(compiled.ok()) << compiled.status().ToString();
    if (!compiled.ok() || compiled->types().size() != 1) return nullptr;
    return compiled->types()[0];
}

Result<std::vector<SchemaHandle>> ExactClosure(
    const SchemaDescriptor& descriptor,
    std::span<const SchemaHandle> candidates) {
    std::vector<SchemaHandle> exact;
    exact.reserve(descriptor.dependencies().size());
    for (const DependencyDescriptor& dependency : descriptor.dependencies()) {
        SchemaHandle resolved;
        for (const SchemaHandle& candidate : candidates) {
            if (candidate != nullptr &&
                candidate->aggregate().full_name() == dependency.full_name() &&
                candidate->identity().canonical_digest() == dependency.digest()) {
                resolved = candidate;
                break;
            }
        }
        if (resolved == nullptr) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "descriptor dependency closure is unavailable");
        }
        exact.push_back(std::move(resolved));
    }
    return exact;
}

Result<std::string> EncodeArtifact(
    const CompiledSchema& schema,
    std::span<const SchemaHandle> descriptor_closure = {}) {
    // schema.types() are the artifact's local types and form a candidate pool;
    // they are not all members of every local descriptor's exact closure.
    const std::span<const SchemaHandle> candidates =
        descriptor_closure.empty() ? schema.types() : descriptor_closure;
    std::vector<LayoutPlan> layouts;
    layouts.reserve(schema.types().size());
    for (const SchemaHandle& descriptor : schema.types()) {
        auto exact = ExactClosure(*descriptor, candidates);
        if (!exact.ok()) return exact.status();
        auto layout = LayoutPlanner::Plan(*descriptor, *exact);
        if (!layout.ok()) return layout.status();
        layouts.push_back(std::move(*layout));
    }
    return codegen::EncodeDescriptorArtifact(schema, layouts);
}

std::span<const std::byte> ArtifactBytes(const std::string& artifact) {
    return std::as_bytes(
        std::span<const char>(artifact.data(), artifact.size()));
}

uint32_t ReadU32(const std::string& bytes, size_t offset) {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
        value |= static_cast<uint32_t>(
                     static_cast<uint8_t>(bytes[offset + i]))
                 << (i * 8);
    }
    return value;
}

uint64_t ReadU64(const std::string& bytes, size_t offset) {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) {
        value |= static_cast<uint64_t>(
                     static_cast<uint8_t>(bytes[offset + i]))
                 << (i * 8);
    }
    return value;
}

void WriteU64(std::string& bytes, size_t offset, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) {
        bytes[offset + i] = static_cast<char>(value >> (i * 8));
    }
}

void ResignArtifact(std::string& artifact) {
    const size_t payload_size = artifact.size() - CanonicalDigest{}.size();
    const CanonicalDigest checksum =
        Sha256(std::string_view(artifact.data(), payload_size));
    for (size_t i = 0; i < checksum.size(); ++i) {
        artifact[payload_size + i] = static_cast<char>(checksum[i]);
    }
}

SchemaHandle RebuildWithIdentity(
    const SchemaDescriptor& descriptor, const SchemaIdentity& identity,
    std::string canonical_text) {
    return std::make_shared<const SchemaDescriptor>(
        descriptor.aggregate(), identity, std::move(canonical_text),
        std::vector<DependencyDescriptor>(descriptor.dependencies().begin(),
                                          descriptor.dependencies().end()));
}

TEST(SchemaRegistryTest, RegistersDescriptorArtifactAndDependencyClosure) {
    auto compiled = SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
package p;
struct Child { uint32 id = 1; }
message Root { Child child = 1; }
)idl");
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    auto artifact = EncodeArtifact(*compiled);
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();

    SchemaRegistry registry;
    auto registered = registry.RegisterDescriptor(ArtifactBytes(*artifact));
    ASSERT_TRUE(registered.ok()) << registered.status().ToString();
    ASSERT_EQ(compiled->types().size(), 2u);
    EXPECT_EQ((*registered)->identity().canonical_digest(),
              compiled->types()[0]->identity().canonical_digest());
    EXPECT_EQ(registry.size(), 2u);
    for (const SchemaHandle& descriptor : compiled->types()) {
        auto found = registry.Find(descriptor->identity().canonical_digest());
        ASSERT_TRUE(found.ok()) << found.status().ToString();
        EXPECT_EQ((*found)->canonical_schema(), descriptor->canonical_schema());
    }
}

TEST(SchemaRegistryTest, RejectsMalformedDescriptorArtifact) {
    SchemaRegistry registry;
    const std::array<std::byte, 4> bytes = {
        std::byte{'M'}, std::byte{'D'}, std::byte{'S'}, std::byte{1}};
    auto result = registry.RegisterDescriptor(bytes);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(registry.size(), 0u);
}

TEST(SchemaRegistryTest, RejectsResignedDescriptorIdentityTampering) {
    auto compiled = SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package p; message M {}");
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    auto encoded = EncodeArtifact(*compiled);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    std::string tampered = *encoded;
    constexpr size_t kHeaderBytes = 8 + 4 + 4;
    const size_t short_id_offset =
        kHeaderBytes + 4 + compiled->types()[0]->aggregate().full_name().size() + 1;
    ASSERT_LT(short_id_offset, tampered.size() - CanonicalDigest{}.size());
    tampered[short_id_offset] ^= 1;
    ResignArtifact(tampered);

    SchemaRegistry registry;
    auto result = registry.RegisterDescriptor(ArtifactBytes(tampered));
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kSchemaMismatch);
    EXPECT_EQ(registry.size(), 0u);
}

TEST(SchemaRegistryTest, RejectsResignedNonCanonicalLayoutTampering) {
    auto compiled = SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
package p;
message Layout {
  uint32 first = 1;
  uint32 second = 2;
  string text = 3 [max_bytes = 16];
}
)idl");
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    auto encoded = EncodeArtifact(*compiled);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();

    const size_t name_size =
        compiled->types()[0]->aggregate().full_name().size();
    constexpr size_t kArtifactHeaderBytes = 8 + 4 + 4;
    const size_t layout_offset =
        kArtifactHeaderBytes + 4 + name_size + 1 + 8 + 32 + 4 + 4;
    const size_t unknown_offset = layout_offset + 4 + 5 * 8;
    const size_t object_size_offset = layout_offset + 4 + 6 * 8;
    const size_t child_budget_offset = layout_offset + 4 + 8 * 8;

    size_t cursor = layout_offset + 4 + 10 * 8;
    ASSERT_EQ(ReadU32(*encoded, cursor), 0u);
    cursor += 4;
    ASSERT_EQ(ReadU32(*encoded, cursor), 0u);
    cursor += 4;
    const uint32_t field_count = ReadU32(*encoded, cursor);
    cursor += 4;
    ASSERT_EQ(field_count, 3u);
    std::vector<size_t> field_offsets;
    for (uint32_t i = 0; i < field_count; ++i) {
        cursor += 4;
        const uint32_t field_name_size = ReadU32(*encoded, cursor);
        cursor += 4 + field_name_size;
        cursor += 1;
        ASSERT_EQ(static_cast<uint8_t>((*encoded)[cursor]), 1u);
        cursor += 2;
        const uint8_t constraint_flags =
            static_cast<uint8_t>((*encoded)[cursor++]);
        if ((constraint_flags & 1) != 0) cursor += 8;
        if ((constraint_flags & 2) != 0) cursor += 8;
        ASSERT_EQ(static_cast<uint8_t>((*encoded)[cursor]), 0u);
        cursor += 1;
        field_offsets.push_back(cursor);
        cursor += 8 + 8 + 8 + 1 + 8 + 8 + 8;
    }

    struct Mutation {
        const char* name;
        size_t offset;
        uint64_t value;
    };
    const std::array mutations = {
        Mutation{"overlapping field offset", field_offsets[1],
                 ReadU64(*encoded, field_offsets[0])},
        Mutation{"object size", object_size_offset,
                 ReadU64(*encoded, object_size_offset) + 8},
        Mutation{"unknown-field offset", unknown_offset,
                 ReadU64(*encoded, field_offsets[0])},
        Mutation{"child budget", child_budget_offset,
                 ReadU64(*encoded, child_budget_offset) + 1},
    };
    for (const Mutation& mutation : mutations) {
        SCOPED_TRACE(mutation.name);
        std::string tampered = *encoded;
        WriteU64(tampered, mutation.offset, mutation.value);
        ResignArtifact(tampered);

        auto decoded = codegen::DecodeAndValidate(tampered);
        ASSERT_FALSE(decoded.ok());
        EXPECT_EQ(decoded.status().code(), StatusCode::kSchemaMismatch);
        SchemaRegistry registry;
        auto registered = registry.RegisterDescriptor(ArtifactBytes(tampered));
        ASSERT_FALSE(registered.ok());
        EXPECT_EQ(registered.status().code(), StatusCode::kSchemaMismatch);
        EXPECT_EQ(registry.size(), 0u);
    }
}

TEST(SchemaRegistryTest, RequiresResolvableArtifactDependencyClosure) {
    auto dependency = SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package dep; "
        "struct Child { uint32 id = 1; }");
    ASSERT_TRUE(dependency.ok()) << dependency.status().ToString();
    CompileOptions options;
    options.dependencies.assign(dependency->types().begin(),
                                dependency->types().end());
    auto root = SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package app; "
        "message Root { dep.Child child = 1; }",
        options);
    ASSERT_TRUE(root.ok()) << root.status().ToString();

    std::vector<SchemaHandle> closure(dependency->types().begin(),
                                      dependency->types().end());
    closure.insert(closure.end(), root->types().begin(), root->types().end());
    auto root_artifact = EncodeArtifact(*root, closure);
    auto dependency_artifact = EncodeArtifact(*dependency);
    ASSERT_TRUE(root_artifact.ok()) << root_artifact.status().ToString();
    ASSERT_TRUE(dependency_artifact.ok())
        << dependency_artifact.status().ToString();

    SchemaRegistry registry;
    auto missing = registry.RegisterDescriptor(ArtifactBytes(*root_artifact));
    ASSERT_FALSE(missing.ok());
    EXPECT_EQ(missing.status().code(), StatusCode::kSchemaMismatch);
    EXPECT_EQ(registry.size(), 0u);

    ASSERT_TRUE(registry
                    .RegisterDescriptor(ArtifactBytes(*dependency_artifact))
                    .ok());
    auto registered = registry.RegisterDescriptor(ArtifactBytes(*root_artifact));
    ASSERT_TRUE(registered.ok()) << registered.status().ToString();
    EXPECT_EQ((*registered)->aggregate().full_name(), "app.Root");
    EXPECT_EQ(registry.size(), 2u);
}

TEST(SchemaRegistryTest, DuplicateDescriptorArtifactRegistrationIsIdempotent) {
    auto compiled = SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package p; message M {}");
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    auto artifact = EncodeArtifact(*compiled);
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();

    SchemaRegistry registry;
    auto first = registry.RegisterDescriptor(ArtifactBytes(*artifact));
    auto second = registry.RegisterDescriptor(ArtifactBytes(*artifact));
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    EXPECT_EQ(first->get(), second->get());
    EXPECT_EQ(registry.size(), 1u);
}

TEST(SchemaRegistryTest, RegisterIdlRequiresExplicitSchemaVersion) {
    SchemaRegistry registry;
    auto result = registry.RegisterIdl("message M {}");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(result.status().message().find("schema_version"),
              std::string_view::npos);
}

TEST(SchemaRegistryTest, DeduplicatesFullDigestAcrossVersionLabels) {
    SchemaRegistry registry;
    auto first = registry.RegisterIdl(
        "package p; option schema_version = \"1.0\"; "
        "message M { optional uint32 id = 1; }");
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    auto second = registry.RegisterIdl(
        "package p; option schema_version = \"9.7\"; "
        "message M { optional uint32 renamed = 1; }");
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    ASSERT_EQ(first->size(), 1u);
    ASSERT_EQ(second->size(), 1u);
    EXPECT_EQ((*first)[0].get(), (*second)[0].get());
    EXPECT_EQ(registry.size(), 1u);

    auto found = registry.Find((*first)[0]->identity().canonical_digest());
    ASSERT_TRUE(found.ok());
    EXPECT_EQ(found->get(), (*first)[0].get());
}

TEST(SchemaRegistryTest, FindIdentityRequiresRegisteredSchemaVersionAlias) {
    SchemaRegistry registry;
    auto first = registry.RegisterIdl(
        "package p; option schema_version = \"1.0\"; "
        "message M { optional uint32 id = 1; }");
    auto alias = registry.RegisterIdl(
        "package p; option schema_version = \"9.7\"; "
        "message M { optional uint32 renamed = 1; }");
    ASSERT_TRUE(first.ok() && alias.ok());
    const SchemaHandle& descriptor = (*first)[0];
    const SchemaIdentity valid_alias(
        descriptor->identity().short_id(),
        descriptor->identity().canonical_digest(), (9u << 16) | 7u,
        descriptor->identity().layout_version());
    EXPECT_TRUE(registry.Find(valid_alias).ok());

    const SchemaIdentity unregistered_alias(
        descriptor->identity().short_id(),
        descriptor->identity().canonical_digest(), (8u << 16),
        descriptor->identity().layout_version());
    auto rejected = registry.Find(unregistered_alias);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kSchemaMismatch);
}

TEST(SchemaRegistryTest, RejectsDifferentDigestForSameTypeAndVersion) {
    SchemaRegistry registry;
    auto first = registry.RegisterIdl(
        "package p; option schema_version = \"1.0\"; "
        "message M { optional uint32 id = 1; }");
    ASSERT_TRUE(first.ok());
    auto conflict = registry.RegisterIdl(
        "package p; option schema_version = \"1.0\"; "
        "message M { optional uint64 id = 1; }");
    ASSERT_FALSE(conflict.ok());
    EXPECT_EQ(conflict.status().code(), StatusCode::kAlreadyExists);
    EXPECT_EQ(registry.size(), 1u);
}

TEST(SchemaRegistryTest, EnforcesMonotonicAndMajorEvolutionRules) {
    SchemaRegistry compatible_registry;
    ASSERT_TRUE(compatible_registry
                    .RegisterIdl(
                        "package p; option schema_version = \"1.2\"; "
                        "message M { optional uint32 id = 1; }")
                    .ok());
    auto rollback = compatible_registry.RegisterIdl(
        "package p; option schema_version = \"1.1\"; "
        "message M { optional uint32 id = 1; optional uint32 x = 2; }");
    ASSERT_FALSE(rollback.ok());
    EXPECT_EQ(rollback.status().code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(compatible_registry.size(), 1u);

    SchemaRegistry breaking_registry;
    ASSERT_TRUE(breaking_registry
                    .RegisterIdl(
                        "package p; option schema_version = \"1.0\"; "
                        "message M { optional uint32 id = 1; }")
                    .ok());
    auto same_major = breaking_registry.RegisterIdl(
        "package p; option schema_version = \"1.1\"; "
        "message M { optional uint64 id = 1; }");
    ASSERT_FALSE(same_major.ok());
    EXPECT_NE(same_major.status().message().find("major"),
              std::string_view::npos);
    auto next_major = breaking_registry.RegisterIdl(
        "package p; option schema_version = \"2.0\"; "
        "message M { optional uint64 id = 1; }");
    ASSERT_TRUE(next_major.ok()) << next_major.status().ToString();
}

TEST(SchemaRegistryTest, RecanonicalizesAndRejectsCallerMetadataTampering) {
    auto compiled = SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package p; message M {}");
    SchemaHandle valid = OnlyType(compiled);
    ASSERT_NE(valid, nullptr);

    SchemaRegistry canonical_registry;
    SchemaIdentity valid_identity(
        valid->identity().short_id(), valid->identity().canonical_digest(),
        valid->identity().schema_version(), valid->identity().layout_version());
    auto bad_text = canonical_registry.RegisterDescriptor(RebuildWithIdentity(
        *valid, valid_identity, std::string(valid->canonical_schema()) + "x"));
    ASSERT_FALSE(bad_text.ok());
    EXPECT_EQ(bad_text.status().code(), StatusCode::kSchemaMismatch);

    CanonicalDigest bad_digest = valid->identity().canonical_digest();
    bad_digest[31] ^= std::byte{1};
    SchemaIdentity digest_identity(
        DigestShortId(bad_digest), bad_digest,
        valid->identity().schema_version(), valid->identity().layout_version());
    SchemaRegistry digest_registry;
    auto bad_hash = digest_registry.RegisterDescriptor(RebuildWithIdentity(
        *valid, digest_identity, std::string(valid->canonical_schema())));
    ASSERT_FALSE(bad_hash.ok());
    EXPECT_EQ(bad_hash.status().code(), StatusCode::kSchemaMismatch);

    SchemaIdentity short_identity(
        valid->identity().short_id() + 1,
        valid->identity().canonical_digest(),
        valid->identity().schema_version(), valid->identity().layout_version());
    SchemaRegistry short_registry;
    auto bad_short = short_registry.RegisterDescriptor(RebuildWithIdentity(
        *valid, short_identity, std::string(valid->canonical_schema())));
    ASSERT_FALSE(bad_short.ok());
    EXPECT_EQ(bad_short.status().code(), StatusCode::kSchemaMismatch);
}

TEST(SchemaRegistryTest, ValidatesExactDependencyClosure) {
    auto compiled = SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
package p;
message Child { uint32 id = 1; }
message Root { Child child = 1; }
)idl");
    ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
    const SchemaDescriptor* child = compiled->FindType("p.Child");
    const SchemaDescriptor* root = compiled->FindType("p.Root");
    ASSERT_NE(child, nullptr);
    ASSERT_NE(root, nullptr);

    SchemaRegistry registry;
    SchemaHandle child_handle;
    for (const SchemaHandle& descriptor : compiled->types()) {
        if (descriptor.get() == child) child_handle = descriptor;
    }
    ASSERT_TRUE(registry.RegisterDescriptor(child_handle).ok());

    auto canonical = Canonicalizer::Canonicalize(root->aggregate(), {});
    ASSERT_TRUE(canonical.ok());
    SchemaIdentity identity(canonical->short_id(), canonical->digest(),
                            root->identity().schema_version(),
                            root->identity().layout_version());
    SchemaHandle incomplete = std::make_shared<const SchemaDescriptor>(
        root->aggregate(), std::move(identity), std::string(canonical->text()),
        std::vector<DependencyDescriptor>{});
    auto result = registry.RegisterDescriptor(std::move(incomplete));
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kSchemaMismatch);
    EXPECT_EQ(registry.size(), 1u);
}

TEST(SchemaRegistryTest, CollisionInjectionDoesNotRelaxIdentityValidation) {
    SchemaRegistry registry(&ConstantShortIdIndex);
    auto first = registry.RegisterIdl(
        "option schema_version = \"1.0\"; package p; message A {}");
    auto second = registry.RegisterIdl(
        "option schema_version = \"1.0\"; package p; message B {}");
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    EXPECT_EQ(registry.size(), 2u);

    auto ambiguous = registry.FindByShortId(7);
    ASSERT_FALSE(ambiguous.ok());
    EXPECT_EQ(ambiguous.status().code(), StatusCode::kSchemaMismatch);
    EXPECT_TRUE(
        registry.Find((*first)[0]->identity().canonical_digest()).ok());
    EXPECT_TRUE(
        registry.Find((*second)[0]->identity().canonical_digest()).ok());
}

TEST(SchemaRegistryTest, RequiresUniqueRegisteredDependencyCandidate) {
    SchemaRegistry registry;
    auto first = registry.RegisterIdl(
        "option schema_version = \"1.0\"; package common; "
        "struct Point { uint32 x = 1; }");
    auto second = registry.RegisterIdl(
        "option schema_version = \"1.1\"; package common; "
        "struct Point { uint32 x = 1; optional uint32 y = 2; }");
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();

    constexpr std::string_view kRoot =
        "option schema_version = \"1.0\"; package app; "
        "message Root { common.Point point = 1; }";
    auto ambiguous = registry.RegisterIdl(kRoot);
    ASSERT_FALSE(ambiguous.ok());
    EXPECT_EQ(ambiguous.status().code(), StatusCode::kSchemaMismatch);
    EXPECT_NE(ambiguous.status().message().find("ambiguous"),
              std::string_view::npos);

    CompileOptions options;
    options.dependencies.push_back((*second)[0]);
    auto selected = registry.RegisterIdl(kRoot, options);
    ASSERT_TRUE(selected.ok()) << selected.status().ToString();
}

TEST(SchemaRegistryTest, ResolvesDependenciesWithoutImportingOldLocalVersion) {
    SchemaRegistry registry;
    auto point = registry.RegisterIdl(
        "package common; option schema_version = \"1.0\"; "
        "struct Point { fixed32 x = 1; }");
    ASSERT_TRUE(point.ok()) << point.status().ToString();
    auto first = registry.RegisterIdl(
        "package p; option schema_version = \"1.0\"; "
        "message M { common.Point point = 1; }");
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    auto evolved = registry.RegisterIdl(
        "package p; option schema_version = \"1.1\"; "
        "message M { common.Point point = 1; optional uint32 extra = 2; }");
    ASSERT_TRUE(evolved.ok()) << evolved.status().ToString();
    EXPECT_EQ(registry.size(), 3u);
}

TEST(SchemaRegistryTest, RecursesCompatibilityThroughNestedDependencies) {
    auto old_schema = SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
package p;
message Child { string value = 1 [max_bytes = 8]; }
message Root { Child child = 1; }
)idl");
    auto new_schema = SchemaCompiler::Compile(R"idl(
option schema_version = "1.1";
package p;
message Child { string value = 1 [max_bytes = 16]; }
message Root { Child child = 1; }
)idl");
    ASSERT_TRUE(old_schema.ok()) << old_schema.status().ToString();
    ASSERT_TRUE(new_schema.ok()) << new_schema.status().ToString();

    const CanonicalDigest old_root =
        old_schema->FindType("p.Root")->identity().canonical_digest();
    const CanonicalDigest new_root =
        new_schema->FindType("p.Root")->identity().canonical_digest();
    SchemaRegistry registry;
    ASSERT_TRUE(registry.RegisterCompiled(*old_schema).ok());
    auto registered = registry.RegisterCompiled(*new_schema);
    ASSERT_TRUE(registered.ok()) << registered.status().ToString();
    auto compatibility = registry.CheckCompatibility(old_root, new_root);
    ASSERT_TRUE(compatibility.ok()) << compatibility.status().ToString();
    EXPECT_EQ(*compatibility, Compatibility::kReadCompatible);
}

TEST(SchemaRegistryTest, RegisterCompiledConflictIsAtomic) {
    SchemaRegistry registry;
    auto existing = registry.RegisterIdl(
        "option schema_version = \"1.0\"; package p; "
        "message B { uint32 id = 1; }");
    ASSERT_TRUE(existing.ok());

    auto batch = SchemaCompiler::Compile(R"idl(
option schema_version = "1.0";
package p;
message A { uint32 id = 1; }
message B { uint64 id = 1; }
)idl");
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    const CanonicalDigest a_digest =
        batch->FindType("p.A")->identity().canonical_digest();
    auto result = registry.RegisterCompiled(*batch);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kAlreadyExists);
    EXPECT_EQ(registry.size(), 1u);
    auto absent = registry.Find(a_digest);
    ASSERT_FALSE(absent.ok());
    EXPECT_EQ(absent.status().code(), StatusCode::kNotFound);
}

TEST(SchemaRegistryTest, ChecksCompatibilityByFullDigest) {
    SchemaRegistry registry;
    auto first = registry.RegisterIdl(
        "package p; option schema_version = \"1.0\"; "
        "message M { optional uint32 id = 1; }");
    auto second = registry.RegisterIdl(
        "package p; option schema_version = \"1.1\"; "
        "message M { optional uint32 id = 1; optional uint32 extra = 2; }");
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    auto compatibility = registry.CheckCompatibility(
        (*first)[0]->identity().canonical_digest(),
        (*second)[0]->identity().canonical_digest());
    ASSERT_TRUE(compatibility.ok());
    EXPECT_EQ(*compatibility, Compatibility::kWireCompatible);
}

TEST(SchemaRegistryTest, ConcurrentRegistrationPublishesOneImmutableEntry) {
    SchemaRegistry registry;
    constexpr size_t kThreads = 16;
    std::atomic<size_t> failures{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (size_t i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            auto result = registry.RegisterIdl(
                "option schema_version = \"1.0\"; package p; "
                "message Concurrent { optional uint32 id = 1; }");
            if (!result.ok() || result->size() != 1) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(registry.size(), 1u);
}

}  // namespace
}  // namespace mino::schema
