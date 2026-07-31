// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/schema/lexer.h"

#include <cstdint>
#include <limits>
#include <new>
#include <string>
#include <utility>

#include "mino/common/status.h"

namespace mino::schema {
namespace {

bool IsContinuation(uint8_t byte) noexcept {
    return byte >= 0x80u && byte <= 0xbfu;
}

bool IsValidUtf8Impl(std::string_view bytes) noexcept {
    size_t index = 0;
    while (index < bytes.size()) {
        const uint8_t first = static_cast<uint8_t>(bytes[index]);
        if (first <= 0x7fu) {
            ++index;
            continue;
        }
        if (first >= 0xc2u && first <= 0xdfu) {
            if (index + 1 >= bytes.size() ||
                !IsContinuation(static_cast<uint8_t>(bytes[index + 1]))) {
                return false;
            }
            index += 2;
            continue;
        }
        if (first >= 0xe0u && first <= 0xefu) {
            if (index + 2 >= bytes.size()) return false;
            const uint8_t second = static_cast<uint8_t>(bytes[index + 1]);
            const uint8_t third = static_cast<uint8_t>(bytes[index + 2]);
            const bool valid_second =
                first == 0xe0u ? second >= 0xa0u && second <= 0xbfu
                : first == 0xedu ? second >= 0x80u && second <= 0x9fu
                                 : IsContinuation(second);
            if (!valid_second || !IsContinuation(third)) return false;
            index += 3;
            continue;
        }
        if (first >= 0xf0u && first <= 0xf4u) {
            if (index + 3 >= bytes.size()) return false;
            const uint8_t second = static_cast<uint8_t>(bytes[index + 1]);
            const bool valid_second =
                first == 0xf0u ? second >= 0x90u && second <= 0xbfu
                : first == 0xf4u ? second >= 0x80u && second <= 0x8fu
                                 : IsContinuation(second);
            if (!valid_second ||
                !IsContinuation(static_cast<uint8_t>(bytes[index + 2])) ||
                !IsContinuation(static_cast<uint8_t>(bytes[index + 3]))) {
                return false;
            }
            index += 4;
            continue;
        }
        return false;
    }
    return true;
}

class LexerImpl {
public:
    LexerImpl(std::string_view input, const LexerOptions& options)
        : input_(input), options_(options) {}

    Result<std::vector<Token>> Run() {
        if (input_.size() > options_.max_input_bytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "IDL input exceeds max_input_bytes");
        }
        if (!IsValidUtf8Impl(input_)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "IDL input is not valid UTF-8");
        }

        while (true) {
            auto skip = SkipTrivia();
            if (!skip.ok()) {
                return skip.status();
            }
            if (AtEnd()) {
                auto emitted = Emit(TokenKind::kEndOfFile, "", location_);
                if (!emitted.ok()) {
                    return emitted.status();
                }
                return std::move(tokens_);
            }

            const SourceLocation begin = location_;
            const char c = Peek();
            if (IsIdentifierStart(c)) {
                auto result = LexIdentifier(begin);
                if (!result.ok()) {
                    return result.status();
                }
                continue;
            }
            if (IsDigit(c)) {
                auto result = LexNumber(begin);
                if (!result.ok()) {
                    return result.status();
                }
                continue;
            }
            if (c == '"') {
                auto result = LexString(begin);
                if (!result.ok()) {
                    return result.status();
                }
                continue;
            }

            TokenKind kind;
            switch (c) {
                case '=': kind = TokenKind::kEquals; break;
                case ';': kind = TokenKind::kSemicolon; break;
                case '.': kind = TokenKind::kDot; break;
                case ',': kind = TokenKind::kComma; break;
                case '[': kind = TokenKind::kLeftBracket; break;
                case ']': kind = TokenKind::kRightBracket; break;
                case '<': kind = TokenKind::kLeftAngle; break;
                case '>': kind = TokenKind::kRightAngle; break;
                case '{': kind = TokenKind::kLeftBrace; break;
                case '}': kind = TokenKind::kRightBrace; break;
                case '+': kind = TokenKind::kPlus; break;
                case '-': kind = TokenKind::kMinus; break;
                default:
                    return Error(begin, "unexpected character");
            }
            Advance();
            auto emitted = Emit(kind, input_.substr(begin.offset, 1), begin);
            if (!emitted.ok()) {
                return emitted.status();
            }
        }
    }

private:
    bool AtEnd() const { return index_ >= input_.size(); }

    char Peek(size_t lookahead = 0) const {
        const size_t position = index_ + lookahead;
        return position < input_.size() ? input_[position] : '\0';
    }

    void Advance() {
        if (AtEnd()) {
            return;
        }
        const char c = input_[index_++];
        ++location_.offset;
        if (c == '\n') {
            ++location_.line;
            location_.column = 1;
        } else {
            ++location_.column;
        }
    }

    static bool IsIdentifierStart(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    }

    static bool IsIdentifierContinue(char c) {
        return IsIdentifierStart(c) || IsDigit(c);
    }

    static bool IsDigit(char c) {
        return c >= '0' && c <= '9';
    }

    Result<void> SkipTrivia() {
        while (!AtEnd()) {
            const char c = Peek();
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                Advance();
                continue;
            }
            if (c == '/' && Peek(1) == '/') {
                Advance();
                Advance();
                while (!AtEnd() && Peek() != '\n') {
                    Advance();
                }
                continue;
            }
            if (c == '/' && Peek(1) == '*') {
                const SourceLocation begin = location_;
                Advance();
                Advance();
                bool closed = false;
                while (!AtEnd()) {
                    if (Peek() == '*' && Peek(1) == '/') {
                        Advance();
                        Advance();
                        closed = true;
                        break;
                    }
                    Advance();
                }
                if (!closed) {
                    return Error(begin, "unterminated block comment");
                }
                continue;
            }
            break;
        }
        return Result<void>();
    }

    Result<void> LexIdentifier(SourceLocation begin) {
        while (!AtEnd() && IsIdentifierContinue(Peek())) {
            Advance();
        }
        return EmitSlice(TokenKind::kIdentifier, begin);
    }

    Result<void> LexNumber(SourceLocation begin) {
        while (IsDigit(Peek())) {
            Advance();
        }

        bool floating_point = false;
        if (Peek() == '.') {
            floating_point = true;
            Advance();
            while (IsDigit(Peek())) {
                Advance();
            }
        }
        if (Peek() == 'e' || Peek() == 'E') {
            floating_point = true;
            Advance();
            if (Peek() == '+' || Peek() == '-') {
                Advance();
            }
            if (!IsDigit(Peek())) {
                return Error(begin, "exponent requires at least one digit");
            }
            while (IsDigit(Peek())) {
                Advance();
            }
        }
        return EmitSlice(floating_point ? TokenKind::kFloatingPointLiteral
                                        : TokenKind::kIntegerLiteral,
                         begin);
    }

    static int HexValue(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        return -1;
    }

    Result<uint32_t> ReadHexDigits(SourceLocation begin, size_t count) {
        uint32_t value = 0;
        for (size_t i = 0; i < count; ++i) {
            if (AtEnd()) {
                return Error(begin, "incomplete hexadecimal escape");
            }
            const int digit = HexValue(Peek());
            if (digit < 0) {
                return Error(location_, "invalid hexadecimal escape");
            }
            value = (value << 4) | static_cast<uint32_t>(digit);
            Advance();
        }
        return value;
    }

    static bool AppendUtf8(uint32_t code_point, std::string& output) {
        if (code_point <= 0x7f) {
            output.push_back(static_cast<char>(code_point));
        } else if (code_point <= 0x7ff) {
            output.push_back(static_cast<char>(0xc0 | (code_point >> 6)));
            output.push_back(static_cast<char>(0x80 | (code_point & 0x3f)));
        } else if (code_point <= 0xffff) {
            if (code_point >= 0xd800 && code_point <= 0xdfff) {
                return false;
            }
            output.push_back(static_cast<char>(0xe0 | (code_point >> 12)));
            output.push_back(
                static_cast<char>(0x80 | ((code_point >> 6) & 0x3f)));
            output.push_back(static_cast<char>(0x80 | (code_point & 0x3f)));
        } else if (code_point <= 0x10ffff) {
            output.push_back(static_cast<char>(0xf0 | (code_point >> 18)));
            output.push_back(
                static_cast<char>(0x80 | ((code_point >> 12) & 0x3f)));
            output.push_back(
                static_cast<char>(0x80 | ((code_point >> 6) & 0x3f)));
            output.push_back(static_cast<char>(0x80 | (code_point & 0x3f)));
        } else {
            return false;
        }
        return true;
    }

    Result<void> LexString(SourceLocation begin) {
        Advance();
        std::string decoded;
        while (!AtEnd()) {
            const char c = Peek();
            if (c == '"') {
                Advance();
                return Emit(TokenKind::kStringLiteral, decoded, begin);
            }
            if (c == '\n' || c == '\r' ||
                static_cast<unsigned char>(c) < 0x20) {
                return Error(location_, "unescaped control character in string");
            }
            if (c != '\\') {
                decoded.push_back(c);
                Advance();
            } else {
                const SourceLocation escape_begin = location_;
                Advance();
                if (AtEnd()) {
                    return Error(escape_begin, "unterminated escape sequence");
                }
                const char escaped = Peek();
                Advance();
                switch (escaped) {
                    case '"': decoded.push_back('"'); break;
                    case '\\': decoded.push_back('\\'); break;
                    case '/': decoded.push_back('/'); break;
                    case 'b': decoded.push_back('\b'); break;
                    case 'f': decoded.push_back('\f'); break;
                    case 'n': decoded.push_back('\n'); break;
                    case 'r': decoded.push_back('\r'); break;
                    case 't': decoded.push_back('\t'); break;
                    case 'x': {
                        auto value = ReadHexDigits(escape_begin, 2);
                        if (!value.ok()) {
                            return value.status();
                        }
                        decoded.push_back(static_cast<char>(*value));
                        break;
                    }
                    case 'u': {
                        auto value = ReadHexDigits(escape_begin, 4);
                        if (!value.ok()) {
                            return value.status();
                        }
                        if (!AppendUtf8(*value, decoded)) {
                            return Error(escape_begin,
                                         "invalid Unicode code point");
                        }
                        break;
                    }
                    default:
                        return Error(escape_begin, "unknown escape sequence");
                }
            }
            if (decoded.size() > options_.max_token_bytes ||
                location_.offset - begin.offset > options_.max_token_bytes) {
                return Status::Error(StatusCode::kResourceExhausted,
                                     LocationPrefix(begin) +
                                         "string token exceeds max_token_bytes");
            }
        }
        return Error(begin, "unterminated string literal");
    }

    Result<void> EmitSlice(TokenKind kind, SourceLocation begin) {
        const size_t length = static_cast<size_t>(location_.offset - begin.offset);
        if (length > options_.max_token_bytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 LocationPrefix(begin) +
                                     "token exceeds max_token_bytes");
        }
        return Emit(kind, input_.substr(static_cast<size_t>(begin.offset), length),
                    begin);
    }

    Result<void> Emit(TokenKind kind, std::string_view text,
                      SourceLocation begin) {
        const uint64_t source_bytes = location_.offset - begin.offset;
        if (text.size() > options_.max_token_bytes ||
            source_bytes > options_.max_token_bytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 LocationPrefix(begin) +
                                     "token exceeds max_token_bytes");
        }
        if (tokens_.size() >= options_.max_tokens) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 LocationPrefix(begin) +
                                     "token count exceeds max_tokens");
        }
        tokens_.push_back(Token{kind, std::string(text), {begin, location_}});
        return Result<void>();
    }

    static std::string LocationPrefix(SourceLocation location) {
        return "line " + std::to_string(location.line) + ", column " +
               std::to_string(location.column) + ": ";
    }

    static Status Error(SourceLocation location, std::string_view message) {
        return Status::Error(StatusCode::kInvalidArgument,
                             LocationPrefix(location) + std::string(message));
    }

    std::string_view input_;
    const LexerOptions& options_;
    size_t index_ = 0;
    SourceLocation location_;
    std::vector<Token> tokens_;
};

}  // namespace

bool IsValidUtf8(std::string_view bytes) noexcept {
    return IsValidUtf8Impl(bytes);
}

Result<std::vector<Token>> Tokenizer::Tokenize(
    std::string_view input, const LexerOptions& options) noexcept {
    try {
        return LexerImpl(input, options).Run();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

std::string_view TokenKindName(TokenKind kind) noexcept {
    switch (kind) {
        case TokenKind::kIdentifier: return "identifier";
        case TokenKind::kIntegerLiteral: return "integer literal";
        case TokenKind::kFloatingPointLiteral: return "floating-point literal";
        case TokenKind::kStringLiteral: return "string literal";
        case TokenKind::kEquals: return "'='";
        case TokenKind::kSemicolon: return "';'";
        case TokenKind::kDot: return "'.'";
        case TokenKind::kComma: return "','";
        case TokenKind::kLeftBracket: return "'['";
        case TokenKind::kRightBracket: return "']'";
        case TokenKind::kLeftAngle: return "'<'";
        case TokenKind::kRightAngle: return "'>'";
        case TokenKind::kLeftBrace: return "'{'";
        case TokenKind::kRightBrace: return "'}'";
        case TokenKind::kPlus: return "'+'";
        case TokenKind::kMinus: return "'-'";
        case TokenKind::kEndOfFile: return "end of file";
    }
    return "unknown token";
}

}  // namespace mino::schema
