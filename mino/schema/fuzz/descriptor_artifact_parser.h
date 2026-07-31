// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_FUZZ_DESCRIPTOR_ARTIFACT_PARSER_H_
#define MINO_SCHEMA_FUZZ_DESCRIPTOR_ARTIFACT_PARSER_H_

#include <cstddef>
#include <span>

#include "mino/common/status.h"

namespace mino::schema::fuzz::internal {

struct DescriptorArtifactLimits {
    size_t max_input_bytes = 32u << 10;
    size_t max_line_bytes = 1024;
    size_t max_types = 32;
    size_t max_fields_per_type = 128;
    size_t max_dependencies_per_type = 64;
    size_t max_name_bytes = 256;
    size_t max_type_depth = 8;
    size_t max_object_bytes = 1u << 20;
    size_t max_child_bytes = 4u << 20;
    size_t max_dynamic_children = 4096;
};

// Validates the versioned text artifact emitted by CodeGenerator. This is a
// fuzz-local safety parser, not the missing registry descriptor byte codec.
Status ValidateCodegenDescriptorArtifact(
    std::span<const std::byte> input,
    const DescriptorArtifactLimits& limits = {}) noexcept;

}  // namespace mino::schema::fuzz::internal

#endif  // MINO_SCHEMA_FUZZ_DESCRIPTOR_ARTIFACT_PARSER_H_
