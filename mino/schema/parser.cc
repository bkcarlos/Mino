// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/schema/parser.h"

#include <charconv>
#include <cstdint>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "mino/common/status.h"

namespace mino::schema {
namespace {

class ParserImpl {
public:
    ParserImpl(std::vector<Token> tokens, const ParserOptions& options)
        : tokens_(std::move(tokens)), options_(options) {}

    Result<SchemaFile> Run() {
        SchemaFile file;
        file.source.begin = Current().source.begin;

        while (!AtEnd()) {
            if (Current().kind != TokenKind::kIdentifier) {
                return Error(Current(), "expected a top-level declaration");
            }
            if (declaration_count_ >= options_.max_declarations) {
                return ResourceError(Current(),
                                     "declaration count exceeds max_declarations");
            }
            ++declaration_count_;

            if (Current().text == "syntax") {
                if (file.syntax.has_value()) {
                    return Error(Current(), "duplicate syntax declaration");
                }
                auto declaration = ParseSyntax();
                if (!declaration.ok()) {
                    return declaration.status();
                }
                file.syntax = std::move(*declaration);
            } else if (Current().text == "package") {
                if (file.package.has_value()) {
                    return Error(Current(), "duplicate package declaration");
                }
                auto declaration = ParsePackage();
                if (!declaration.ok()) {
                    return declaration.status();
                }
                file.package = std::move(*declaration);
            } else if (Current().text == "import") {
                auto declaration = ParseImport();
                if (!declaration.ok()) {
                    return declaration.status();
                }
                file.imports.push_back(std::move(*declaration));
            } else if (Current().text == "option") {
                auto declaration = ParseOption();
                if (!declaration.ok()) {
                    return declaration.status();
                }
                file.options.push_back(std::move(*declaration));
            } else if (Current().text == "message" ||
                       Current().text == "struct") {
                auto declaration = ParseAggregate();
                if (!declaration.ok()) {
                    return declaration.status();
                }
                file.aggregates.push_back(std::move(*declaration));
            } else {
                return Error(Current(), "unknown top-level declaration '" +
                                            Current().text + "'");
            }
        }

        file.source.end = Current().source.end;
        return file;
    }

private:
    const Token& Current(size_t lookahead = 0) const {
        const size_t position = index_ + lookahead;
        return tokens_[position < tokens_.size() ? position : tokens_.size() - 1];
    }

    bool AtEnd() const { return Current().kind == TokenKind::kEndOfFile; }

    bool IsIdentifier(std::string_view text) const {
        return Current().kind == TokenKind::kIdentifier && Current().text == text;
    }

    Result<const Token*> Expect(TokenKind kind) {
        if (Current().kind != kind) {
            return Error(Current(), "expected " +
                                        std::string(TokenKindName(kind)) +
                                        ", found " +
                                        std::string(TokenKindName(Current().kind)));
        }
        return &tokens_[index_++];
    }

    Result<const Token*> ExpectIdentifier() {
        return Expect(TokenKind::kIdentifier);
    }

    Result<std::string> ParseDottedName() {
        auto first = ExpectIdentifier();
        if (!first.ok()) {
            return first.status();
        }
        std::string name = (*first)->text;
        while (Current().kind == TokenKind::kDot) {
            ++index_;
            auto part = ExpectIdentifier();
            if (!part.ok()) {
                return part.status();
            }
            name.push_back('.');
            name.append((*part)->text);
        }
        return name;
    }

    Result<uint64_t> ParseUnsignedInteger() {
        auto token = Expect(TokenKind::kIntegerLiteral);
        if (!token.ok()) {
            return token.status();
        }
        uint64_t value = 0;
        const std::string& text = (*token)->text;
        const auto converted =
            std::from_chars(text.data(), text.data() + text.size(), value);
        if (converted.ec != std::errc() ||
            converted.ptr != text.data() + text.size()) {
            return Error(**token, "integer literal is outside uint64 range");
        }
        return value;
    }

    Result<SyntaxDeclaration> ParseSyntax() {
        const SourceLocation begin = Current().source.begin;
        ++index_;
        auto equals = Expect(TokenKind::kEquals);
        if (!equals.ok()) {
            return equals.status();
        }
        auto version = Expect(TokenKind::kStringLiteral);
        if (!version.ok()) {
            return version.status();
        }
        auto semicolon = Expect(TokenKind::kSemicolon);
        if (!semicolon.ok()) {
            return semicolon.status();
        }
        return SyntaxDeclaration{(*version)->text,
                                 {begin, (*semicolon)->source.end}};
    }

    Result<PackageDeclaration> ParsePackage() {
        const SourceLocation begin = Current().source.begin;
        ++index_;
        auto name = ParseDottedName();
        if (!name.ok()) {
            return name.status();
        }
        auto semicolon = Expect(TokenKind::kSemicolon);
        if (!semicolon.ok()) {
            return semicolon.status();
        }
        return PackageDeclaration{std::move(*name),
                                  {begin, (*semicolon)->source.end}};
    }

    Result<ImportDeclaration> ParseImport() {
        const SourceLocation begin = Current().source.begin;
        ++index_;
        auto path = Expect(TokenKind::kStringLiteral);
        if (!path.ok()) {
            return path.status();
        }
        auto semicolon = Expect(TokenKind::kSemicolon);
        if (!semicolon.ok()) {
            return semicolon.status();
        }
        return ImportDeclaration{(*path)->text,
                                 {begin, (*semicolon)->source.end}};
    }

    Result<OptionDeclaration> ParseOption() {
        const SourceLocation begin = Current().source.begin;
        ++index_;
        auto name = ParseDottedName();
        if (!name.ok()) {
            return name.status();
        }
        auto equals = Expect(TokenKind::kEquals);
        if (!equals.ok()) {
            return equals.status();
        }
        auto value = ParseLiteral();
        if (!value.ok()) {
            return value.status();
        }
        auto semicolon = Expect(TokenKind::kSemicolon);
        if (!semicolon.ok()) {
            return semicolon.status();
        }
        return OptionDeclaration{std::move(*name), std::move(*value),
                                 {begin, (*semicolon)->source.end}};
    }

    static std::optional<ScalarType> ScalarFromName(std::string_view name) {
        if (name == "int32") return ScalarType::kInt32;
        if (name == "int64") return ScalarType::kInt64;
        if (name == "uint32") return ScalarType::kUint32;
        if (name == "uint64") return ScalarType::kUint64;
        if (name == "fixed32") return ScalarType::kFixed32;
        if (name == "fixed64") return ScalarType::kFixed64;
        if (name == "float") return ScalarType::kFloat;
        if (name == "double") return ScalarType::kDouble;
        if (name == "bool") return ScalarType::kBool;
        if (name == "string") return ScalarType::kString;
        if (name == "bytes") return ScalarType::kBytes;
        return std::nullopt;
    }

    Result<TypeReference> ParseType(size_t depth) {
        if (depth > options_.max_nesting_depth) {
            return ResourceError(Current(),
                                 "type nesting exceeds max_nesting_depth");
        }
        const SourceLocation begin = Current().source.begin;
        if (IsIdentifier("vector")) {
            ++index_;
            auto left_angle = Expect(TokenKind::kLeftAngle);
            if (!left_angle.ok()) {
                return left_angle.status();
            }
            auto element = ParseType(depth + 1);
            if (!element.ok()) {
                return element.status();
            }
            auto right_angle = Expect(TokenKind::kRightAngle);
            if (!right_angle.ok()) {
                return right_angle.status();
            }
            TypeReference type;
            type.kind = TypeKind::kVector;
            type.name = "vector";
            type.element_type =
                std::make_unique<TypeReference>(std::move(*element));
            type.source = {begin, (*right_angle)->source.end};
            return type;
        }

        auto first = ExpectIdentifier();
        if (!first.ok()) {
            return first.status();
        }
        TypeReference type;
        type.source.begin = begin;
        const auto scalar = ScalarFromName((*first)->text);
        if (scalar.has_value()) {
            type.kind = TypeKind::kScalar;
            type.scalar = scalar;
            type.name = (*first)->text;
            type.source.end = (*first)->source.end;
            return type;
        }

        type.kind = TypeKind::kUserDefined;
        type.name = (*first)->text;
        type.source.end = (*first)->source.end;
        while (Current().kind == TokenKind::kDot) {
            ++index_;
            auto part = ExpectIdentifier();
            if (!part.ok()) {
                return part.status();
            }
            type.name.push_back('.');
            type.name.append((*part)->text);
            type.source.end = (*part)->source.end;
        }
        return type;
    }

    Result<Literal> ParseLiteral() {
        SourceLocation begin = Current().source.begin;
        std::string sign;
        if (Current().kind == TokenKind::kPlus ||
            Current().kind == TokenKind::kMinus) {
            sign = Current().text;
            ++index_;
        }

        const Token& token = Current();
        Literal literal;
        literal.source.begin = begin;
        if (token.kind == TokenKind::kIntegerLiteral) {
            literal.kind = LiteralKind::kInteger;
        } else if (token.kind == TokenKind::kFloatingPointLiteral) {
            literal.kind = LiteralKind::kFloatingPoint;
        } else if (token.kind == TokenKind::kStringLiteral && sign.empty()) {
            literal.kind = LiteralKind::kString;
        } else if (token.kind == TokenKind::kIdentifier && sign.empty()) {
            literal.kind = token.text == "true" || token.text == "false"
                               ? LiteralKind::kBoolean
                               : LiteralKind::kIdentifier;
        } else {
            return Error(token, "expected a literal value");
        }
        literal.value = sign + token.text;
        literal.source.end = token.source.end;
        ++index_;
        return literal;
    }

    Result<std::vector<Annotation>> ParseAnnotations() {
        std::vector<Annotation> annotations;
        while (Current().kind == TokenKind::kLeftBracket) {
            ++index_;
            if (Current().kind == TokenKind::kRightBracket) {
                return Error(Current(), "annotation list cannot be empty");
            }
            while (true) {
                if (annotation_count_ >= options_.max_annotations) {
                    return ResourceError(
                        Current(), "annotation count exceeds max_annotations");
                }
                ++annotation_count_;
                const SourceLocation begin = Current().source.begin;
                auto name = ParseDottedName();
                if (!name.ok()) {
                    return name.status();
                }
                Annotation annotation;
                annotation.name = std::move(*name);
                annotation.source.begin = begin;
                annotation.source.end = tokens_[index_ - 1].source.end;
                if (Current().kind == TokenKind::kEquals) {
                    ++index_;
                    auto value = ParseLiteral();
                    if (!value.ok()) {
                        return value.status();
                    }
                    annotation.source.end = value->source.end;
                    annotation.value = std::move(*value);
                }
                annotations.push_back(std::move(annotation));

                if (Current().kind != TokenKind::kComma) {
                    break;
                }
                ++index_;
                if (Current().kind == TokenKind::kRightBracket) {
                    return Error(Current(),
                                 "expected annotation after ','");
                }
            }
            auto right_bracket = Expect(TokenKind::kRightBracket);
            if (!right_bracket.ok()) {
                return right_bracket.status();
            }
        }
        return annotations;
    }

    Result<FieldDeclaration> ParseField() {
        if (field_count_ >= options_.max_fields) {
            return ResourceError(Current(), "field count exceeds max_fields");
        }
        ++field_count_;

        const SourceLocation begin = Current().source.begin;
        FieldCardinality cardinality = FieldCardinality::kUnspecified;
        if (IsIdentifier("optional")) {
            cardinality = FieldCardinality::kOptional;
            ++index_;
        } else if (IsIdentifier("required")) {
            cardinality = FieldCardinality::kRequired;
            ++index_;
        }

        auto type = ParseType(1);
        if (!type.ok()) {
            return type.status();
        }
        auto name = ExpectIdentifier();
        if (!name.ok()) {
            return name.status();
        }
        auto equals = Expect(TokenKind::kEquals);
        if (!equals.ok()) {
            return equals.status();
        }
        auto id = ParseUnsignedInteger();
        if (!id.ok()) {
            return id.status();
        }
        auto annotations = ParseAnnotations();
        if (!annotations.ok()) {
            return annotations.status();
        }
        auto semicolon = Expect(TokenKind::kSemicolon);
        if (!semicolon.ok()) {
            return semicolon.status();
        }

        FieldDeclaration field;
        field.cardinality = cardinality;
        field.type = std::move(*type);
        field.name = (*name)->text;
        field.id = *id;
        field.annotations = std::move(*annotations);
        field.source = {begin, (*semicolon)->source.end};
        return field;
    }

    Result<ReservedDeclaration> ParseReserved() {
        const SourceLocation begin = Current().source.begin;
        ++index_;
        ReservedDeclaration declaration;
        while (true) {
            if (reserved_range_count_ >= options_.max_reserved_ranges) {
                return ResourceError(
                    Current(),
                    "reserved range count exceeds max_reserved_ranges");
            }
            ++reserved_range_count_;
            const SourceLocation range_begin = Current().source.begin;
            auto first = ParseUnsignedInteger();
            if (!first.ok()) {
                return first.status();
            }
            uint64_t last = *first;
            SourceLocation range_end = tokens_[index_ - 1].source.end;
            if (IsIdentifier("to")) {
                ++index_;
                auto parsed_last = ParseUnsignedInteger();
                if (!parsed_last.ok()) {
                    return parsed_last.status();
                }
                last = *parsed_last;
                range_end = tokens_[index_ - 1].source.end;
            }
            declaration.ranges.push_back(
                ReservedRange{*first, last, {range_begin, range_end}});
            if (Current().kind != TokenKind::kComma) {
                break;
            }
            ++index_;
        }
        auto semicolon = Expect(TokenKind::kSemicolon);
        if (!semicolon.ok()) {
            return semicolon.status();
        }
        declaration.source = {begin, (*semicolon)->source.end};
        return declaration;
    }

    Result<AggregateDeclaration> ParseAggregate() {
        const SourceLocation begin = Current().source.begin;
        const AggregateKind kind = Current().text == "message"
                                       ? AggregateKind::kMessage
                                       : AggregateKind::kStruct;
        ++index_;
        auto name = ExpectIdentifier();
        if (!name.ok()) {
            return name.status();
        }
        auto left_brace = Expect(TokenKind::kLeftBrace);
        if (!left_brace.ok()) {
            return left_brace.status();
        }

        AggregateDeclaration declaration;
        declaration.kind = kind;
        declaration.name = (*name)->text;
        while (Current().kind != TokenKind::kRightBrace) {
            if (AtEnd()) {
                return Error(Current(), "expected '}' before end of file");
            }
            if (IsIdentifier("reserved")) {
                auto reserved = ParseReserved();
                if (!reserved.ok()) {
                    return reserved.status();
                }
                declaration.reserved.push_back(std::move(*reserved));
            } else {
                auto field = ParseField();
                if (!field.ok()) {
                    return field.status();
                }
                declaration.fields.push_back(std::move(*field));
            }
        }
        auto right_brace = Expect(TokenKind::kRightBrace);
        if (!right_brace.ok()) {
            return right_brace.status();
        }
        declaration.source = {begin, (*right_brace)->source.end};
        return declaration;
    }

    static std::string LocationPrefix(const Token& token) {
        return "line " + std::to_string(token.source.begin.line) +
               ", column " + std::to_string(token.source.begin.column) + ": ";
    }

    static Status Error(const Token& token, std::string message) {
        return Status::Error(StatusCode::kInvalidArgument,
                             LocationPrefix(token) + std::move(message));
    }

    static Status ResourceError(const Token& token, std::string message) {
        return Status::Error(StatusCode::kResourceExhausted,
                             LocationPrefix(token) + std::move(message));
    }

    std::vector<Token> tokens_;
    const ParserOptions& options_;
    size_t index_ = 0;
    size_t declaration_count_ = 0;
    size_t field_count_ = 0;
    size_t reserved_range_count_ = 0;
    size_t annotation_count_ = 0;
};

}  // namespace

Result<SchemaFile> Parser::Parse(std::string_view input,
                                 const ParserOptions& options) noexcept {
    try {
        auto tokens = Tokenizer::Tokenize(input, options.lexer);
        if (!tokens.ok()) {
            return tokens.status();
        }
        return ParserImpl(std::move(*tokens), options).Run();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::schema
