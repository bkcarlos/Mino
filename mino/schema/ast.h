// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#ifndef MINO_SCHEMA_AST_H_
#define MINO_SCHEMA_AST_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace mino::schema {

// Lines and columns are one-based; byte offsets are zero-based.
struct SourceLocation {
    uint64_t offset = 0;
    uint32_t line = 1;
    uint32_t column = 1;

    friend bool operator==(const SourceLocation&, const SourceLocation&) =
        default;
};

// A half-open source range [begin, end).
struct SourceRange {
    SourceLocation begin;
    SourceLocation end;

    friend bool operator==(const SourceRange&, const SourceRange&) = default;
};

enum class LiteralKind {
    kInteger,
    kFloatingPoint,
    kString,
    kBoolean,
    kIdentifier,
};

struct Literal {
    LiteralKind kind = LiteralKind::kIdentifier;
    // Strings contain their decoded value. Other literals retain source spelling.
    std::string value;
    SourceRange source;
};

struct SyntaxDeclaration {
    std::string version;
    SourceRange source;
};

struct PackageDeclaration {
    std::string name;
    SourceRange source;
};

struct ImportDeclaration {
    std::string path;
    SourceRange source;
};

struct OptionDeclaration {
    std::string name;
    Literal value;
    SourceRange source;
};

enum class ScalarType {
    kInt32,
    kInt64,
    kUint32,
    kUint64,
    kFixed32,
    kFixed64,
    kFloat,
    kDouble,
    kBool,
    kString,
    kBytes,
};

enum class TypeKind {
    kScalar,
    kUserDefined,
    kVector,
};

struct TypeReference {
    TypeKind kind = TypeKind::kUserDefined;
    std::optional<ScalarType> scalar;
    std::string name;
    std::unique_ptr<TypeReference> element_type;
    SourceRange source;

    TypeReference() = default;
    TypeReference(TypeReference&&) noexcept = default;
    TypeReference& operator=(TypeReference&&) noexcept = default;
    TypeReference(const TypeReference&) = delete;
    TypeReference& operator=(const TypeReference&) = delete;
};

enum class FieldCardinality {
    kUnspecified,
    kOptional,
    kRequired,
};

struct Annotation {
    std::string name;
    std::optional<Literal> value;
    SourceRange source;
};

struct FieldDeclaration {
    FieldCardinality cardinality = FieldCardinality::kUnspecified;
    TypeReference type;
    std::string name;
    // The parser preserves the syntactic integer. D3-02 validates the v1 range.
    uint64_t id = 0;
    std::vector<Annotation> annotations;
    SourceRange source;
};

struct ReservedRange {
    uint64_t first = 0;
    uint64_t last = 0;
    SourceRange source;
};

struct ReservedDeclaration {
    std::vector<ReservedRange> ranges;
    SourceRange source;
};

enum class AggregateKind {
    kMessage,
    kStruct,
};

struct AggregateDeclaration {
    AggregateKind kind = AggregateKind::kMessage;
    std::string name;
    std::vector<FieldDeclaration> fields;
    std::vector<ReservedDeclaration> reserved;
    SourceRange source;
};

struct SchemaFile {
    std::optional<SyntaxDeclaration> syntax;
    std::optional<PackageDeclaration> package;
    std::vector<ImportDeclaration> imports;
    std::vector<OptionDeclaration> options;
    std::vector<AggregateDeclaration> aggregates;
    SourceRange source;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_AST_H_
