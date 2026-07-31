// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#ifndef MINO_SCHEMA_PARSER_H_
#define MINO_SCHEMA_PARSER_H_

#include <cstddef>
#include <string_view>

#include "mino/common/result.h"
#include "mino/schema/ast.h"
#include "mino/schema/lexer.h"

namespace mino::schema {

struct ParserOptions {
    LexerOptions lexer;
    size_t max_declarations = 1024;
    size_t max_fields = 1024;
    size_t max_reserved_ranges = 1024;
    size_t max_annotations = 4096;
    size_t max_nesting_depth = 32;
};

class Parser {
public:
    // Parses one complete IDL v1 source file. Semantic validation (duplicate
    // identities, recursion, annotation names/constraints) is intentionally left
    // to D3-02. No exception crosses this API.
    static Result<SchemaFile> Parse(
        std::string_view input,
        const ParserOptions& options = {}) noexcept;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_PARSER_H_
