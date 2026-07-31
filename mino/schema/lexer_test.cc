// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/schema/lexer.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mino/common/status.h"

namespace mino::schema {
namespace {

static_assert(noexcept(Tokenizer::Tokenize(std::string_view{})));

TEST(TokenizerTest, TokenizesCommentsEscapesAndLocations) {
    constexpr std::string_view kInput =
        "// leading comment\n"
        "package demo; /* block\ncomment */\n"
        "import \"dir\\n\\\"file\\x2f\\u0041.mino\";";

    auto result = Tokenizer::Tokenize(kInput);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    const auto& tokens = *result;
    ASSERT_EQ(tokens.size(), 7u);
    EXPECT_EQ(tokens[0].kind, TokenKind::kIdentifier);
    EXPECT_EQ(tokens[0].text, "package");
    EXPECT_EQ(tokens[0].source.begin.line, 2u);
    EXPECT_EQ(tokens[0].source.begin.column, 1u);
    EXPECT_EQ(tokens[1].text, "demo");
    EXPECT_EQ(tokens[4].kind, TokenKind::kStringLiteral);
    EXPECT_EQ(tokens[4].text, "dir\n\"file/A.mino");
    EXPECT_EQ(tokens[4].source.begin.line, 4u);
    EXPECT_EQ(tokens.back().kind, TokenKind::kEndOfFile);
}

TEST(TokenizerTest, DistinguishesIntegerAndFloatingPointLiterals) {
    auto result = Tokenizer::Tokenize("1 1.25 2e3 4E-2");
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    ASSERT_EQ(result->size(), 5u);
    EXPECT_EQ((*result)[0].kind, TokenKind::kIntegerLiteral);
    EXPECT_EQ((*result)[1].kind, TokenKind::kFloatingPointLiteral);
    EXPECT_EQ((*result)[2].kind, TokenKind::kFloatingPointLiteral);
    EXPECT_EQ((*result)[3].kind, TokenKind::kFloatingPointLiteral);
}

TEST(TokenizerTest, ReportsUnexpectedCharacterWithLocation) {
    auto result = Tokenizer::Tokenize("package ok;\n@");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(result.status().message().find("line 2, column 1"),
              std::string_view::npos);
    EXPECT_NE(result.status().message().find("unexpected character"),
              std::string_view::npos);
}

TEST(TokenizerTest, RejectsInvalidAndUnterminatedEscapes) {
    auto invalid = Tokenizer::Tokenize("\"bad\\q\"");
    ASSERT_FALSE(invalid.ok());
    EXPECT_EQ(invalid.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(invalid.status().message().find("unknown escape"),
              std::string_view::npos);

    auto unterminated = Tokenizer::Tokenize("/* no end");
    ASSERT_FALSE(unterminated.ok());
    EXPECT_EQ(unterminated.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(unterminated.status().message().find("unterminated block comment"),
              std::string_view::npos);
}

TEST(TokenizerTest, RejectsMalformedUtf8BeforeTokenization) {
    const std::string overlong("\xc0\x80", 2);
    auto result = Tokenizer::Tokenize(overlong);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(result.status().message().find("UTF-8"),
              std::string_view::npos);

    const std::string truncated("\xe2\x82", 2);
    EXPECT_FALSE(Tokenizer::Tokenize(truncated).ok());
}

TEST(TokenizerTest, IdentifiersAreAsciiOnly) {
    const std::string non_ascii_identifier("\xc3\xa9", 2);
    auto result = Tokenizer::Tokenize(non_ascii_identifier);
    ASSERT_FALSE(result.ok());
    EXPECT_NE(result.status().message().find("unexpected character"),
              std::string_view::npos);

    auto utf8_string = Tokenizer::Tokenize("\"\xc3\xa9\"");
    ASSERT_TRUE(utf8_string.ok()) << utf8_string.status().ToString();
}

TEST(TokenizerTest, EnforcesInputTokenCountAndTokenSizeLimits) {
    LexerOptions options;
    options.max_input_bytes = 3;
    auto input_limit = Tokenizer::Tokenize("four", options);
    ASSERT_FALSE(input_limit.ok());
    EXPECT_EQ(input_limit.status().code(), StatusCode::kResourceExhausted);

    options = LexerOptions{};
    options.max_tokens = 2;
    auto token_limit = Tokenizer::Tokenize("a b", options);
    ASSERT_FALSE(token_limit.ok());
    EXPECT_EQ(token_limit.status().code(), StatusCode::kResourceExhausted);

    options = LexerOptions{};
    options.max_token_bytes = 3;
    auto size_limit = Tokenizer::Tokenize("four", options);
    ASSERT_FALSE(size_limit.ok());
    EXPECT_EQ(size_limit.status().code(), StatusCode::kResourceExhausted);
}

}  // namespace
}  // namespace mino::schema
