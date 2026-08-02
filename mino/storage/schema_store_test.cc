// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/schema_store.h"

#include <gtest/gtest.h>

#include <unistd.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"
#include "mino/schema/registry.h"

namespace mino::storage {
namespace {

struct Artifact {
    std::string bytes;
    std::vector<schema::SchemaIdentity> identities;
};

std::span<const std::byte> Bytes(const std::string& bytes) {
    return std::as_bytes(std::span<const char>(bytes.data(), bytes.size()));
}

Result<Artifact> CompileArtifact(std::string_view idl) {
    auto compiled = schema::SchemaCompiler::Compile(idl);
    if (!compiled.ok()) return compiled.status();
    std::vector<schema::LayoutPlan> layouts;
    layouts.reserve(compiled->types().size());
    for (const schema::SchemaHandle& descriptor : compiled->types()) {
        auto layout = schema::LayoutPlanner::Plan(*descriptor, {});
        if (!layout.ok()) return layout.status();
        layouts.push_back(std::move(*layout));
    }
    auto encoded =
        schema::codegen::EncodeDescriptorArtifact(*compiled, layouts);
    if (!encoded.ok()) return encoded.status();
    Artifact artifact;
    artifact.bytes = std::move(*encoded);
    artifact.identities.reserve(compiled->types().size());
    for (const schema::SchemaHandle& descriptor : compiled->types()) {
        artifact.identities.push_back(descriptor->identity());
    }
    return artifact;
}

std::filesystem::path TestDirectory(std::string_view name) {
    static std::atomic<uint64_t> sequence{0};
    const char* temporary = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        temporary == nullptr ? std::filesystem::temp_directory_path()
                             : std::filesystem::path(temporary);
    const std::filesystem::path path =
        base / ("mino_schema_store_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    std::filesystem::create_directories(path);
    return path;
}

std::vector<std::byte> ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    const std::string characters{std::istreambuf_iterator<char>(input),
                                 std::istreambuf_iterator<char>()};
    std::vector<std::byte> bytes;
    bytes.reserve(characters.size());
    for (char character : characters) {
        bytes.push_back(static_cast<std::byte>(
            static_cast<uint8_t>(character)));
    }
    return bytes;
}

void WriteFile(const std::filesystem::path& path,
               std::span<const std::byte> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.good());
    output.write(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<std::streamsize>(bytes.size()));
    ASSERT_TRUE(output.good());
}

uint32_t ReadLe32(std::span<const std::byte> bytes, size_t offset) {
    uint32_t value = 0;
    for (size_t index = 0; index < 4; ++index) {
        value |= static_cast<uint32_t>(
                     static_cast<uint8_t>(bytes[offset + index]))
                 << (index * 8);
    }
    return value;
}

void WriteLe16(std::vector<std::byte>* bytes, size_t offset, uint16_t value) {
    ASSERT_LE(offset + 2, bytes->size());
    for (size_t index = 0; index < 2; ++index) {
        (*bytes)[offset + index] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
}

void WriteLe32(std::vector<std::byte>* bytes, size_t offset, uint32_t value) {
    ASSERT_LE(offset + 4, bytes->size());
    for (size_t index = 0; index < 4; ++index) {
        (*bytes)[offset + index] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
}

class TestCrc32c {
public:
    void Update(std::span<const std::byte> bytes) {
        for (std::byte byte : bytes) {
            state_ ^= static_cast<uint8_t>(byte);
            for (int bit = 0; bit < 8; ++bit) {
                state_ = (state_ >> 1) ^
                         ((state_ & 1u) != 0 ? 0x82f63b78u : 0u);
            }
        }
    }

    uint32_t Finish() const { return state_ ^ 0xffffffffu; }

private:
    uint32_t state_ = 0xffffffffu;
};

void RefreshManifestCrc(std::vector<std::byte>* manifest) {
    ASSERT_GE(manifest->size(), kSchemaManifestHeaderSize);
    TestCrc32c crc;
    crc.Update(std::span<const std::byte>(*manifest).first(28));
    constexpr std::array<std::byte, 4> zeros{};
    crc.Update(zeros);
    crc.Update(std::span<const std::byte>(*manifest).subspan(32));
    WriteLe32(manifest, 28, crc.Finish());
}

bool SameIdentity(const schema::SchemaIdentity& lhs,
                  const schema::SchemaIdentity& rhs) {
    return lhs.short_id() == rhs.short_id() &&
           lhs.canonical_digest() == rhs.canonical_digest() &&
           lhs.schema_version() == rhs.schema_version() &&
           lhs.layout_version() == rhs.layout_version();
}

struct HookState {
    std::array<SchemaStoreFaultPoint, 16> points{};
    size_t count = 0;
    std::optional<SchemaStoreFaultPoint> fail_at;
    bool failed = false;
};

Status RecordingFaultHook(SchemaStoreFaultPoint point, void* context) noexcept {
    auto* state = static_cast<HookState*>(context);
    if (state->count < state->points.size()) {
        state->points[state->count++] = point;
    }
    if (!state->failed && state->fail_at == point) {
        state->failed = true;
        return Status::Error(StatusCode::kUnavailable, "injected failure");
    }
    return Status::Ok();
}

Artifact TwoTypeArtifactWithSameFirstIdentity() {
    auto artifact = CompileArtifact(R"idl(
option schema_version = "1.0";
package store;
message Alpha {}
message Extra { string note = 1 [max_bytes = 32]; }
)idl");
    EXPECT_TRUE(artifact.ok()) << artifact.status().ToString();
    return artifact.ok() ? std::move(*artifact) : Artifact{};
}

TEST(SchemaStoreTest, PersistsResolvesAndRecoversMonotonicRefs) {
    const std::filesystem::path root = TestDirectory("round_trip");
    auto alpha = CompileArtifact(
        "option schema_version = \"1.0\"; package store; "
        "message Alpha { uint32 id = 1; }");
    auto beta = CompileArtifact(
        "option schema_version = \"1.0\"; package store; "
        "message Beta { uint64 id = 1; }");
    auto gamma = CompileArtifact(
        "option schema_version = \"1.0\"; package store; "
        "message Gamma { bool ready = 1; }");
    ASSERT_TRUE(alpha.ok()) << alpha.status().ToString();
    ASSERT_TRUE(beta.ok()) << beta.status().ToString();
    ASSERT_TRUE(gamma.ok()) << gamma.status().ToString();

    schema::SchemaRegistry registry;
    auto store = SchemaStore::Open(root, &registry);
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    ASSERT_TRUE(std::filesystem::is_directory(root / "schemas"));

    auto alpha_ref = (*store)->Persist(alpha->identities[0], Bytes(alpha->bytes));
    ASSERT_TRUE(alpha_ref.ok()) << alpha_ref.status().ToString();
    EXPECT_EQ(*alpha_ref, 1u);
    EXPECT_EQ((*store)->size(), 1u);
    EXPECT_EQ((*store)->high_watermark(), 1u);

    auto duplicate =
        (*store)->Persist(alpha->identities[0], Bytes(alpha->bytes));
    ASSERT_TRUE(duplicate.ok()) << duplicate.status().ToString();
    EXPECT_EQ(*duplicate, *alpha_ref);
    EXPECT_EQ((*store)->size(), 1u);

    auto beta_ref = (*store)->Persist(beta->identities[0], Bytes(beta->bytes));
    ASSERT_TRUE(beta_ref.ok()) << beta_ref.status().ToString();
    EXPECT_EQ(*beta_ref, 2u);
    auto resolved = (*store)->Resolve(*alpha_ref);
    ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
    EXPECT_TRUE(SameIdentity(resolved->identity, alpha->identities[0]));
    EXPECT_EQ(resolved->descriptor_path.filename().string(),
              schema::DigestHex(alpha->identities[0].canonical_digest()) +
                  ".schema");
    EXPECT_EQ(resolved->descriptor_path.filename().string().size(), 71u);
    EXPECT_EQ(*(*store)->FindRef(alpha->identities[0]), *alpha_ref);
    EXPECT_EQ(*(*store)->FindRef(beta->identities[0].canonical_digest()),
              *beta_ref);
    EXPECT_EQ((*store)->Resolve(kInvalidSchemaRef).status().code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ((*store)->Resolve(99).status().code(), StatusCode::kNotFound);

    store->reset();
    auto reopened = SchemaStore::Open(root, &registry);
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    EXPECT_EQ((*reopened)->size(), 2u);
    EXPECT_EQ((*reopened)->high_watermark(), 2u);
    auto gamma_ref =
        (*reopened)->Persist(gamma->identities[0], Bytes(gamma->bytes));
    ASSERT_TRUE(gamma_ref.ok()) << gamma_ref.status().ToString();
    EXPECT_EQ(*gamma_ref, 3u);
}

TEST(SchemaStoreTest, EnforcesSingleOwnerAndBoundedOptions) {
    const std::filesystem::path root = TestDirectory("owner");
    schema::SchemaRegistry registry;
    auto owner = SchemaStore::Open(root, &registry);
    ASSERT_TRUE(owner.ok()) << owner.status().ToString();
    auto second = SchemaStore::Open(root, &registry);
    ASSERT_FALSE(second.ok());
    EXPECT_EQ(second.status().code(), StatusCode::kUnavailable);
    owner->reset();
    EXPECT_TRUE(SchemaStore::Open(root, &registry).ok());

    SchemaStoreOptions invalid;
    invalid.max_entries = 0;
    auto zero_entries =
        SchemaStore::Open(TestDirectory("zero_entries"), &registry, invalid);
    ASSERT_FALSE(zero_entries.ok());
    EXPECT_EQ(zero_entries.status().code(), StatusCode::kInvalidArgument);

    invalid = {};
    invalid.max_descriptor_bytes = (16u << 20) + 1;
    auto huge_descriptor =
        SchemaStore::Open(TestDirectory("huge_descriptor"), &registry, invalid);
    ASSERT_FALSE(huge_descriptor.ok());
    EXPECT_EQ(huge_descriptor.status().code(), StatusCode::kInvalidArgument);

    auto null_registry =
        SchemaStore::Open(TestDirectory("null_registry"), nullptr);
    ASSERT_FALSE(null_registry.ok());
    EXPECT_EQ(null_registry.status().code(), StatusCode::kInvalidArgument);
}

TEST(SchemaStoreTest, RunsTheRequiredDurabilitySequence) {
    const std::filesystem::path root = TestDirectory("order");
    auto artifact = CompileArtifact(
        "option schema_version = \"1.0\"; package store; message Ordered {} ");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    HookState hooks;
    SchemaStoreOptions options;
    options.fault_hook = RecordingFaultHook;
    options.fault_hook_context = &hooks;
    schema::SchemaRegistry registry;
    auto store = SchemaStore::Open(root, &registry, options);
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    auto persisted =
        (*store)->Persist(artifact->identities[0], Bytes(artifact->bytes));
    ASSERT_TRUE(persisted.ok()) << persisted.status().ToString();

    const std::array expected = {
        SchemaStoreFaultPoint::kAfterDescriptorTempWrite,
        SchemaStoreFaultPoint::kAfterDescriptorSync,
        SchemaStoreFaultPoint::kAfterDescriptorRename,
        SchemaStoreFaultPoint::kAfterDescriptorDirectorySync,
        SchemaStoreFaultPoint::kAfterManifestTempWrite,
        SchemaStoreFaultPoint::kAfterManifestSync,
        SchemaStoreFaultPoint::kAfterManifestRename,
        SchemaStoreFaultPoint::kAfterManifestDirectorySync,
    };
    ASSERT_EQ(hooks.count, expected.size());
    for (size_t index = 0; index < expected.size(); ++index) {
        EXPECT_EQ(hooks.points[index], expected[index]) << index;
    }
}

TEST(SchemaStoreTest, OrphanDescriptorIsInvisibleAndReusedAfterReopen) {
    const std::filesystem::path root = TestDirectory("orphan");
    auto artifact = CompileArtifact(
        "option schema_version = \"1.0\"; package store; message Orphan {} ");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    HookState hooks;
    hooks.fail_at = SchemaStoreFaultPoint::kAfterDescriptorRename;
    SchemaStoreOptions options;
    options.fault_hook = RecordingFaultHook;
    options.fault_hook_context = &hooks;
    schema::SchemaRegistry registry;
    auto store = SchemaStore::Open(root, &registry, options);
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    auto failed =
        (*store)->Persist(artifact->identities[0], Bytes(artifact->bytes));
    ASSERT_FALSE(failed.ok());
    EXPECT_EQ(failed.status().code(), StatusCode::kUnavailable);
    EXPECT_EQ((*store)->size(), 0u);
    EXPECT_EQ((*store)->high_watermark(), 0u);
    EXPECT_TRUE(std::filesystem::is_regular_file(root / "schemas" / "manifest"));
    const std::filesystem::path descriptor_path =
        root / "schemas" /
        (schema::DigestHex(artifact->identities[0].canonical_digest()) +
         ".schema");
    EXPECT_TRUE(std::filesystem::is_regular_file(descriptor_path));

    store->reset();
    hooks.count = 0;
    hooks.fail_at.reset();
    auto reopened = SchemaStore::Open(root, &registry, options);
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    EXPECT_EQ((*reopened)->Resolve(1).status().code(), StatusCode::kNotFound);
    auto retried =
        (*reopened)->Persist(artifact->identities[0], Bytes(artifact->bytes));
    ASSERT_TRUE(retried.ok()) << retried.status().ToString();
    EXPECT_EQ(*retried, 1u);
    ASSERT_EQ(hooks.count, 5u);
    EXPECT_EQ(hooks.points[0],
              SchemaStoreFaultPoint::kAfterDescriptorDirectorySync);
    EXPECT_EQ(hooks.points[1], SchemaStoreFaultPoint::kAfterManifestTempWrite);
    EXPECT_EQ(hooks.points[2], SchemaStoreFaultPoint::kAfterManifestSync);
    EXPECT_EQ(hooks.points[3], SchemaStoreFaultPoint::kAfterManifestRename);
    EXPECT_EQ(hooks.points[4],
              SchemaStoreFaultPoint::kAfterManifestDirectorySync);
}

TEST(SchemaStoreTest, RejectsMalformedMissingAndMismatchedIdentities) {
    const std::filesystem::path root = TestDirectory("identity");
    auto alpha = CompileArtifact(
        "option schema_version = \"1.0\"; package store; message Alpha {} ");
    auto beta = CompileArtifact(
        "option schema_version = \"1.0\"; package store; message Beta {} ");
    ASSERT_TRUE(alpha.ok()) << alpha.status().ToString();
    ASSERT_TRUE(beta.ok()) << beta.status().ToString();
    schema::SchemaRegistry registry;
    auto store = SchemaStore::Open(root, &registry);
    ASSERT_TRUE(store.ok()) << store.status().ToString();

    const std::array<std::byte, 4> malformed = {
        std::byte{'M'}, std::byte{'I'}, std::byte{'N'}, std::byte{'O'}};
    auto malformed_result =
        (*store)->Persist(alpha->identities[0], malformed);
    ASSERT_FALSE(malformed_result.ok());
    EXPECT_EQ((*store)->size(), 0u);

    auto missing_identity =
        (*store)->Persist(beta->identities[0], Bytes(alpha->bytes));
    ASSERT_FALSE(missing_identity.ok());
    EXPECT_EQ(missing_identity.status().code(), StatusCode::kSchemaMismatch);
    EXPECT_EQ((*store)->size(), 0u);

    const schema::SchemaIdentity altered(
        alpha->identities[0].short_id(),
        alpha->identities[0].canonical_digest(),
        alpha->identities[0].schema_version() + 1,
        alpha->identities[0].layout_version());
    auto altered_result = (*store)->Persist(altered, Bytes(alpha->bytes));
    ASSERT_FALSE(altered_result.ok());
    EXPECT_EQ(altered_result.status().code(), StatusCode::kSchemaMismatch);

    ASSERT_TRUE(
        (*store)->Persist(alpha->identities[0], Bytes(alpha->bytes)).ok());
    const Artifact expanded = TwoTypeArtifactWithSameFirstIdentity();
    ASSERT_GE(expanded.identities.size(), 2u);
    ASSERT_EQ(expanded.identities[0].canonical_digest(),
              alpha->identities[0].canonical_digest());
    auto different_bytes = (*store)->Persist(
        alpha->identities[0], Bytes(expanded.bytes));
    ASSERT_FALSE(different_bytes.ok());
    EXPECT_EQ(different_bytes.status().code(), StatusCode::kSchemaMismatch);
}

TEST(SchemaStoreTest, RejectsManifestCrcTruncationTrailingAndVersion) {
    auto artifact = CompileArtifact(
        "option schema_version = \"1.0\"; package store; message Disk {} ");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();

    enum class Mutation { kCrc, kTruncate, kTrailing, kVersion };
    for (Mutation mutation : {Mutation::kCrc, Mutation::kTruncate,
                              Mutation::kTrailing, Mutation::kVersion}) {
        const std::filesystem::path root = TestDirectory("manifest_damage");
        schema::SchemaRegistry registry;
        auto store = SchemaStore::Open(root, &registry);
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        ASSERT_TRUE((*store)->Persist(artifact->identities[0],
                                     Bytes(artifact->bytes))
                        .ok());
        store->reset();

        const std::filesystem::path manifest_path = root / "schemas" / "manifest";
        std::vector<std::byte> manifest = ReadFile(manifest_path);
        ASSERT_GT(manifest.size(), kSchemaManifestHeaderSize);
        switch (mutation) {
            case Mutation::kCrc:
                manifest.back() ^= std::byte{1};
                break;
            case Mutation::kTruncate:
                manifest.pop_back();
                break;
            case Mutation::kTrailing:
                manifest.push_back(std::byte{0});
                break;
            case Mutation::kVersion:
                WriteLe16(&manifest, 8, kSchemaManifestVersion + 1);
                RefreshManifestCrc(&manifest);
                break;
        }
        WriteFile(manifest_path, manifest);
        auto reopened = SchemaStore::Open(root, &registry);
        ASSERT_FALSE(reopened.ok()) << static_cast<int>(mutation);
        EXPECT_EQ(reopened.status().code(), StatusCode::kCorruption)
            << static_cast<int>(mutation) << " "
            << reopened.status().ToString();
    }
}

TEST(SchemaStoreTest, RejectsDuplicateRefsDigestsAndRegressedWatermark) {
    auto first = CompileArtifact(
        "option schema_version = \"1.0\"; package store; message First {} ");
    auto second = CompileArtifact(
        "option schema_version = \"1.0\"; package store; message Second {} ");
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();

    enum class Mutation { kDuplicateRef, kDuplicateDigest, kWatermark };
    for (Mutation mutation : {Mutation::kDuplicateRef,
                              Mutation::kDuplicateDigest,
                              Mutation::kWatermark}) {
        const std::filesystem::path root = TestDirectory("manifest_invariant");
        schema::SchemaRegistry registry;
        auto store = SchemaStore::Open(root, &registry);
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        ASSERT_TRUE((*store)->Persist(first->identities[0], Bytes(first->bytes)).ok());
        ASSERT_TRUE((*store)->Persist(second->identities[0], Bytes(second->bytes)).ok());
        store->reset();

        const std::filesystem::path manifest_path = root / "schemas" / "manifest";
        std::vector<std::byte> manifest = ReadFile(manifest_path);
        const size_t first_offset = kSchemaManifestHeaderSize;
        const size_t second_offset =
            first_offset + ReadLe32(manifest, first_offset);
        ASSERT_LT(second_offset + 131, manifest.size() + 1);
        switch (mutation) {
            case Mutation::kDuplicateRef:
                WriteLe32(&manifest, second_offset + 4, 1);
                break;
            case Mutation::kDuplicateDigest:
                for (size_t index = 0; index < 32; ++index) {
                    manifest[second_offset + 16 + index] =
                        manifest[first_offset + 16 + index];
                }
                for (size_t index = 0; index < 71; ++index) {
                    manifest[second_offset + 60 + index] =
                        manifest[first_offset + 60 + index];
                }
                break;
            case Mutation::kWatermark:
                WriteLe32(&manifest, 16, 1);
                break;
        }
        RefreshManifestCrc(&manifest);
        WriteFile(manifest_path, manifest);
        auto reopened = SchemaStore::Open(root, &registry);
        ASSERT_FALSE(reopened.ok()) << static_cast<int>(mutation);
        EXPECT_EQ(reopened.status().code(), StatusCode::kCorruption)
            << reopened.status().ToString();
    }
}

TEST(SchemaStoreTest, RejectsMissingManifestWhenDescriptorsExist) {
    const std::filesystem::path root = TestDirectory("missing_manifest");
    auto artifact = CompileArtifact(
        "option schema_version = \"1.0\"; package store; message Lost {} ");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    schema::SchemaRegistry registry;
    auto store = SchemaStore::Open(root, &registry);
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    ASSERT_TRUE((*store)->Persist(artifact->identities[0], Bytes(artifact->bytes))
                    .ok());
    store->reset();

    ASSERT_TRUE(std::filesystem::remove(root / "schemas" / "manifest"));
    auto reopened = SchemaStore::Open(root, &registry);
    ASSERT_FALSE(reopened.ok());
    EXPECT_EQ(reopened.status().code(), StatusCode::kCorruption);
}

TEST(SchemaStoreTest, RejectsMissingSymlinkAndNonRegularDescriptors) {
    auto artifact = CompileArtifact(
        "option schema_version = \"1.0\"; package store; message File {} ");
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    const std::string filename =
        schema::DigestHex(artifact->identities[0].canonical_digest()) +
        ".schema";

    {
        const std::filesystem::path root = TestDirectory("missing_file");
        schema::SchemaRegistry registry;
        auto store = SchemaStore::Open(root, &registry);
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        ASSERT_TRUE((*store)->Persist(artifact->identities[0],
                                     Bytes(artifact->bytes))
                        .ok());
        store->reset();
        std::filesystem::remove(root / "schemas" / filename);
        auto reopened = SchemaStore::Open(root, &registry);
        ASSERT_FALSE(reopened.ok());
        EXPECT_EQ(reopened.status().code(), StatusCode::kCorruption);
    }
    {
        const std::filesystem::path root = TestDirectory("symlink_file");
        schema::SchemaRegistry registry;
        auto store = SchemaStore::Open(root, &registry);
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        const std::filesystem::path target = root / "target";
        WriteFile(target, Bytes(artifact->bytes));
        std::filesystem::create_symlink(target, root / "schemas" / filename);
        auto persisted =
            (*store)->Persist(artifact->identities[0], Bytes(artifact->bytes));
        ASSERT_FALSE(persisted.ok());
        EXPECT_EQ(persisted.status().code(), StatusCode::kCorruption);
    }
    {
        const std::filesystem::path root = TestDirectory("directory_file");
        schema::SchemaRegistry registry;
        auto store = SchemaStore::Open(root, &registry);
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        std::filesystem::create_directory(root / "schemas" / filename);
        auto persisted =
            (*store)->Persist(artifact->identities[0], Bytes(artifact->bytes));
        ASSERT_FALSE(persisted.ok());
        EXPECT_EQ(persisted.status().code(), StatusCode::kCorruption);
    }
}

TEST(SchemaStoreTest, LimitsDoNotPublishOrReuseCommittedRefs) {
    auto first = CompileArtifact(
        "option schema_version = \"1.0\"; package store; message One {} ");
    auto second = CompileArtifact(
        "option schema_version = \"1.0\"; package store; message Two {} ");
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE(second.ok()) << second.status().ToString();

    SchemaStoreOptions options;
    options.max_entries = 1;
    const std::filesystem::path root = TestDirectory("entry_limit");
    schema::SchemaRegistry registry;
    auto store = SchemaStore::Open(root, &registry, options);
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    auto first_ref =
        (*store)->Persist(first->identities[0], Bytes(first->bytes));
    ASSERT_TRUE(first_ref.ok()) << first_ref.status().ToString();
    auto exhausted =
        (*store)->Persist(second->identities[0], Bytes(second->bytes));
    ASSERT_FALSE(exhausted.ok());
    EXPECT_EQ(exhausted.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ((*store)->high_watermark(), 1u);
    EXPECT_EQ((*store)->size(), 1u);
    EXPECT_FALSE((*store)->FindRef(second->identities[0]).ok());

    SchemaStoreOptions manifest_limit;
    manifest_limit.max_manifest_bytes = kSchemaManifestHeaderSize + 130;
    auto small_manifest = SchemaStore::Open(
        TestDirectory("manifest_limit"), &registry, manifest_limit);
    ASSERT_TRUE(small_manifest.ok()) << small_manifest.status().ToString();
    auto cannot_fit = (*small_manifest)
                          ->Persist(first->identities[0], Bytes(first->bytes));
    ASSERT_FALSE(cannot_fit.ok());
    EXPECT_EQ(cannot_fit.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ((*small_manifest)->high_watermark(), 0u);

    SchemaStoreOptions artifact_limit;
    artifact_limit.max_descriptor_bytes = first->bytes.size() - 1;
    auto small_artifact = SchemaStore::Open(
        TestDirectory("artifact_limit"), &registry, artifact_limit);
    ASSERT_TRUE(small_artifact.ok()) << small_artifact.status().ToString();
    auto too_large = (*small_artifact)
                         ->Persist(first->identities[0], Bytes(first->bytes));
    ASSERT_FALSE(too_large.ok());
    EXPECT_EQ(too_large.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ((*small_artifact)->high_watermark(), 0u);
}

}  // namespace
}  // namespace mino::storage
