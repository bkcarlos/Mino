// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#ifndef MINO_SCHEMA_COMPILER_H_
#define MINO_SCHEMA_COMPILER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "mino/common/result.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/unknown_field_set.h"

namespace mino::schema {

struct CompileOptions {
    size_t max_input_bytes = 1u << 20;
    size_t max_tokens = 1u << 18;
    size_t max_token_bytes = 1u << 16;
    size_t max_types = 1024;
    size_t max_fields = 1024;
    size_t max_reserved_ranges = 1024;
    size_t max_annotations = 4096;
    size_t max_nesting_depth = 32;
    size_t max_name_bytes = 1024;
    uint64_t max_total_capacity = 64u << 20;
    UnknownFieldLimits unknown_fields;
    size_t max_canonical_bytes = 4u << 20;
    uint32_t layout_version = 1;
    // Production schemas must declare option schema_version = "major.minor".
    // Tests with legacy fixtures may opt in explicitly to version 0.
    bool allow_implicit_schema_version = false;
    std::vector<std::shared_ptr<const SchemaDescriptor>> dependencies;
};

class SchemaCompiler {
public:
    // Parses, validates, canonicalizes, hashes, and publishes immutable
    // descriptors. No exception crosses this runtime compiler boundary.
    static Result<CompiledSchema> Compile(
        std::string_view idl,
        const CompileOptions& options = {}) noexcept;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_COMPILER_H_
