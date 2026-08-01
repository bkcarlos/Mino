// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_CODEGEN_ARTIFACT_CODEC_H_
#define MINO_SCHEMA_CODEGEN_ARTIFACT_CODEC_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/result.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/layout.h"

namespace mino::schema::codegen {

inline constexpr uint32_t kDescriptorArtifactVersion = 2;

struct DecodedFieldLayoutArtifact {
    uint32_t field_id = 0;
    size_t offset = 0;
    size_t size = 0;
    size_t alignment = 1;
    FieldStorageKind storage_kind = FieldStorageKind::kScalar;
    std::optional<size_t> presence_bit;
    uint64_t max_child_bytes = 0;
    uint64_t max_dynamic_children = 0;
};

struct DecodedLayoutArtifact {
    uint32_t layout_version = 0;
    size_t header_size = 0;
    size_t presence_bitmap_offset = 0;
    size_t presence_bitmap_words = 0;
    size_t fixed_area_offset = 0;
    size_t fixed_area_size = 0;
    std::optional<size_t> unknown_fields_offset;
    size_t object_size = 0;
    size_t object_alignment = 1;
    uint64_t max_child_bytes = 0;
    uint64_t max_dynamic_children = 0;
    std::vector<DecodedFieldLayoutArtifact> fields;
};

struct DecodedTypeArtifact {
    std::shared_ptr<const SchemaDescriptor> descriptor;
    DecodedLayoutArtifact layout;
};

struct DecodedDescriptorArtifact {
    uint32_t version = 0;
    // Types physically encoded by this artifact. These are local publication
    // units, not a shared dependency closure: each descriptor's dependencies()
    // remains its own exact transitive name+digest closure.
    std::vector<DecodedTypeArtifact> types;
};

// Encodes schema.types() as the artifact's local types. Logical descriptor
// fields are explicit in a stable little-endian format; no sizeof/offsetof of a
// descriptor C++ object enters the artifact. A local type may satisfy another
// local type's dependency, but unrelated local types are never closure members.
Result<std::string> EncodeDescriptorArtifact(
    const CompiledSchema& schema,
    std::span<const LayoutPlan> layouts) noexcept;

// Decodes a MINODSC2 artifact, reconstructs every descriptor, reruns
// Canonicalizer and LayoutPlanner, and rejects inconsistent checksums,
// identities, semantics, canonical layouts, unresolved or mismatched declared
// dependencies, and trailing bytes. The checksum provides corruption detection,
// not authenticity.
//
// external_descriptors supplies dependency candidates that are referenced by
// the artifact but not embedded in it. Artifact types are always considered as
// candidates. The overload without a closure therefore accepts only artifacts
// whose exact descriptor closures are self-contained.
Result<DecodedDescriptorArtifact> DecodeAndValidate(
    std::string_view bytes,
    std::span<const std::shared_ptr<const SchemaDescriptor>>
        external_descriptors) noexcept;
Result<DecodedDescriptorArtifact> DecodeAndValidate(
    std::string_view bytes) noexcept;

}  // namespace mino::schema::codegen

#endif  // MINO_SCHEMA_CODEGEN_ARTIFACT_CODEC_H_
