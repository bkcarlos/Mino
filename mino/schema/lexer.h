// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#ifndef MINO_SCHEMA_LEXER_H_
#define MINO_SCHEMA_LEXER_H_

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/result.h"
#include "mino/schema/ast.h"

namespace mino::schema {

enum class TokenKind {
    kIdentifier,
    kIntegerLiteral,
    kFloatingPointLiteral,
    kStringLiteral,
    kEquals,
    kSemicolon,
    kDot,
    kComma,
    kLeftBracket,
    kRightBracket,
    kLeftAngle,
    kRightAngle,
    kLeftBrace,
    kRightBrace,
    kPlus,
    kMinus,
    kEndOfFile,
};

struct Token {
    TokenKind kind = TokenKind::kEndOfFile;
    // String tokens contain their decoded value; all other tokens retain source
    // spelling. Comments and whitespace do not produce tokens.
    std::string text;
    SourceRange source;
};

struct LexerOptions {
    size_t max_input_bytes = 1u << 20;
    // Includes the final end-of-file token.
    size_t max_tokens = 1u << 18;
    size_t max_token_bytes = 1u << 16;
};

class Tokenizer {
public:
    // Converts the complete input to owned tokens. No exception crosses this API.
    static Result<std::vector<Token>> Tokenize(
        std::string_view input,
        const LexerOptions& options = {}) noexcept;
};

// Strict UTF-8 validation: rejects truncation, overlong encodings, surrogates,
// invalid continuation bytes, and code points above U+10FFFF.
bool IsValidUtf8(std::string_view bytes) noexcept;

std::string_view TokenKindName(TokenKind kind) noexcept;

}  // namespace mino::schema

#endif  // MINO_SCHEMA_LEXER_H_
