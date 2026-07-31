// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_CODEGEN_CODE_GENERATOR_H_
#define MINO_SCHEMA_CODEGEN_CODE_GENERATOR_H_

#include <cstddef>
#include <memory>
#include <span>
#include <string>

#include "mino/common/result.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/layout.h"

namespace mino::schema::codegen {

struct CodeGeneratorOptions {
    // Include spelling used by the generated source. This is intentionally
    // independent of the host path used to write the header.
    std::string header_include;
    size_t max_output_bytes = 16u << 20;
    // Complete local+import descriptor closure. Required whenever a generated
    // type references an imported user-defined type; generation rejects an
    // incomplete or identity-conflicting closure.
    std::span<const std::shared_ptr<const SchemaDescriptor>> descriptor_closure;
};

struct GeneratedArtifacts {
    std::string header;
    std::string source;
    // Stable, versioned logical descriptor. It is not a dump of a C++ object.
    std::string descriptor;
};

class CodeGenerator {
public:
    // layouts[i] must describe schema.types()[i]. The compiler already sorts
    // types and fields by logical identity, so generation is deterministic.
    static Result<GeneratedArtifacts> Generate(
        const CompiledSchema& schema, std::span<const LayoutPlan> layouts,
        const CodeGeneratorOptions& options) noexcept;
};

}  // namespace mino::schema::codegen

#endif  // MINO_SCHEMA_CODEGEN_CODE_GENERATOR_H_
