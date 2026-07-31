// Copyright 2026 The Mino Authors

#include "mino/schema/registry.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"

namespace mino::schema {
namespace {

uint64_t ConstantShortIdIndex(const CanonicalDigest&) noexcept { return 7; }

SchemaHandle OnlyType(Result<CompiledSchema>& compiled) {
    EXPECT_TRUE(compiled.ok()) << compiled.status().ToString();
    if (!compiled.ok() || compiled->types().size() != 1) return nullptr;
    return compiled->types()[0];
}

SchemaHandle RebuildWithIdentity(
    const SchemaDescriptor& descriptor, const SchemaIdentity& identity,
    std::string canonical_text) {
    return std::make_shared<const SchemaDescriptor>(
        descriptor.aggregate(), identity, std::move(canonical_text),
        std::vector<DependencyDescriptor>(descriptor.dependencies().begin(),
                                          descriptor.dependencies().end()));
}

TEST(SchemaRegistryTest, ByteDescriptorBoundaryIsExplicitlyVersionGated) {
    SchemaRegistry registry;
    const std::array<std::byte, 4> bytes = {
        std::byte{'M'}, std::byte{'D'}, std::byte{'S'}, std::byte{1}};
    auto result = registry.RegisterDescriptor(bytes);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kUnsupported);
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
