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
    std::vector<DecodedTypeArtifact> types;
};

// Encodes logical descriptor fields explicitly in a stable little-endian
// format. No sizeof/offsetof of a descriptor C++ object enters the artifact.
Result<std::string> EncodeDescriptorArtifact(
    const CompiledSchema& schema,
    std::span<const LayoutPlan> layouts) noexcept;

// Fully decodes the artifact, reconstructs every AggregateDescriptor, reruns
// Canonicalizer, and rejects digest, identity, semantic, layout, or trailing
// byte tampering. The decoded descriptors/layouts are suitable for a future
// Registry bytes ingestion path.
Result<DecodedDescriptorArtifact> DecodeAndValidate(
    std::string_view bytes) noexcept;

}  // namespace mino::schema::codegen

#endif  // MINO_SCHEMA_CODEGEN_ARTIFACT_CODEC_H_
