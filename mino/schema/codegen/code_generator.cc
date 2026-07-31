// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/codegen/code_generator.h"

#include "mino/schema/codegen/artifact_codec.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"

namespace mino::schema::codegen {
namespace {

constexpr size_t kVariableMetadataSize = VariableMetadataLayout::kSize;

void Line(std::string& output, std::string_view text = {}) {
    output.append(text);
    output.push_back('\n');
}

std::vector<std::string> SplitName(std::string_view full_name) {
    std::vector<std::string> result;
    size_t begin = 0;
    while (begin <= full_name.size()) {
        const size_t end = full_name.find('.', begin);
        result.emplace_back(full_name.substr(
            begin, end == std::string_view::npos ? full_name.size() - begin
                                                 : end - begin));
        if (end == std::string_view::npos) break;
        begin = end + 1;
    }
    return result;
}

bool IsCppKeyword(std::string_view name) {
    static const std::set<std::string_view> kKeywords = {
        "alignas",      "alignof",       "and",          "and_eq",
        "asm",          "auto",          "bitand",       "bitor",
        "bool",         "break",         "case",         "catch",
        "char",         "char8_t",       "char16_t",     "char32_t",
        "class",        "compl",         "concept",      "const",
        "consteval",    "constexpr",     "constinit",    "const_cast",
        "continue",     "co_await",      "co_return",    "co_yield",
        "decltype",     "default",       "delete",       "do",
        "double",       "dynamic_cast",  "else",         "enum",
        "explicit",     "export",        "extern",       "false",
        "float",        "for",           "friend",       "goto",
        "if",           "inline",        "int",          "long",
        "mutable",      "namespace",     "new",          "noexcept",
        "not",          "not_eq",        "nullptr",      "operator",
        "or",           "or_eq",         "private",      "protected",
        "public",       "register",      "reinterpret_cast",
        "requires",     "return",        "short",        "signed",
        "sizeof",       "static",        "static_assert", "static_cast",
        "struct",       "switch",        "template",     "this",
        "thread_local", "throw",         "true",         "try",
        "typedef",      "typeid",        "typename",     "union",
        "unsigned",     "using",         "virtual",      "void",
        "volatile",     "wchar_t",       "while",        "xor",
        "xor_eq",
    };
    return kKeywords.contains(name);
}

std::string CppIdentifier(std::string_view name) {
    std::string result;
    result.reserve(name.size() + 8);
    for (const unsigned char ch : name) {
        if (std::isalnum(ch) != 0 || ch == '_') {
            result.push_back(static_cast<char>(ch));
        } else {
            result.push_back('_');
        }
    }
    if (result.empty() || std::isdigit(static_cast<unsigned char>(result[0]))) {
        result.insert(0, "mino_");
    }
    while (result.find("__") != std::string::npos) {
        result.replace(result.find("__"), 2, "_u_");
    }
    if (!result.empty() && result.front() == '_') result.insert(0, "mino");
    if (IsCppKeyword(result)) result.append("_mino");
    return result;
}



std::string NamespaceName(const std::vector<std::string>& parts) {
    std::string result;
    for (size_t i = 0; i + 1 < parts.size(); ++i) {
        if (!result.empty()) result.append("::");
        result.append(CppIdentifier(parts[i]));
    }
    return result;
}

std::string Hex64(uint64_t value) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string result(16, '0');
    for (size_t i = 0; i < result.size(); ++i) {
        result[result.size() - i - 1] = kHex[value & 0xfu];
        value >>= 4;
    }
    return result;
}

std::string DigestHex(const CanonicalDigest& digest) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string result;
    result.reserve(64);
    for (const std::byte byte : digest) {
        const uint8_t value = static_cast<uint8_t>(byte);
        result.push_back(kHex[value >> 4]);
        result.push_back(kHex[value & 0xf]);
    }
    return result;
}

std::string DigestInitializer(const CanonicalDigest& digest) {
    std::string result;
    for (size_t i = 0; i < digest.size(); ++i) {
        if (i != 0) result.append(", ");
        result.append("0x");
        const uint8_t value = static_cast<uint8_t>(digest[i]);
        static constexpr char kHex[] = "0123456789abcdef";
        result.push_back(kHex[value >> 4]);
        result.push_back(kHex[value & 0xf]);
    }
    return result;
}

std::string CppScalar(ScalarType scalar) {
    switch (scalar) {
        case ScalarType::kInt32:
            return "std::int32_t";
        case ScalarType::kInt64:
            return "std::int64_t";
        case ScalarType::kUint32:
        case ScalarType::kFixed32:
            return "std::uint32_t";
        case ScalarType::kUint64:
        case ScalarType::kFixed64:
            return "std::uint64_t";
        case ScalarType::kFloat:
            return "float";
        case ScalarType::kDouble:
            return "double";
        case ScalarType::kBool:
            return "bool";
        case ScalarType::kString:
        case ScalarType::kBytes:
            return {};
    }
    return {};
}

size_t ScalarSize(ScalarType scalar) {
    switch (scalar) {
        case ScalarType::kBool:
            return 1;
        case ScalarType::kInt32:
        case ScalarType::kUint32:
        case ScalarType::kFixed32:
        case ScalarType::kFloat:
            return 4;
        case ScalarType::kInt64:
        case ScalarType::kUint64:
        case ScalarType::kFixed64:
        case ScalarType::kDouble:
            return 8;
        case ScalarType::kString:
        case ScalarType::kBytes:
            return kVariableMetadataSize;
    }
    return 0;
}



Result<std::map<uint32_t, std::string>> FieldNames(
    const AggregateDescriptor& aggregate,
    std::span<const FieldLayout> layouts) {
    std::set<std::string, std::less<>> accessor_symbols = {
        "layout_version", "header_size", "schema_short_id", "object_size",
        "field_count", "presence_bitmap_words", "header", "valid"};
    std::set<std::string, std::less<>> builder_symbols = {"object"};
    std::map<uint32_t, std::string> result;
    for (size_t i = 0; i < aggregate.fields().size(); ++i) {
        const FieldDescriptor& field = aggregate.fields()[i];
        const FieldLayout& layout = layouts[i];
        std::string name = CppIdentifier(field.name());
        for (size_t attempt = 0; attempt < 2; ++attempt) {
            const std::string getter =
                layout.storage_kind() == FieldStorageKind::kInlineStruct
                    ? name + "_bytes"
                    : name;
            const std::string setter =
                layout.storage_kind() == FieldStorageKind::kInlineStruct
                    ? "set_" + name + "_bytes"
                    : "set_" + name;
            const std::string has = "has_" + name;
            const std::string clear = "clear_" + name;
            const bool accessor_collision = accessor_symbols.contains(getter) ||
                (layout.presence_bit().has_value() && accessor_symbols.contains(has));
            const bool builder_collision = builder_symbols.contains(setter) ||
                (layout.presence_bit().has_value() && builder_symbols.contains(clear));
            if (!accessor_collision && !builder_collision) {
                accessor_symbols.insert(getter);
                builder_symbols.insert(setter);
                if (layout.presence_bit().has_value()) {
                    accessor_symbols.insert(has);
                    builder_symbols.insert(clear);
                }
                result.emplace(field.id(), std::move(name));
                break;
            }
            name = CppIdentifier(field.name()) + "_field_" +
                   std::to_string(field.id());
        }
        if (!result.contains(field.id())) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "C++ field name cannot be mangled without collision");
        }
    }
    return result;
}

size_t ElementSize(
    const TypeDescriptor& type,
    const std::map<std::string, const LayoutPlan*, std::less<>>& plans,
    const std::map<std::string, const SchemaDescriptor*, std::less<>>& descriptors) {
    if (type.kind() == TypeDescriptor::Kind::kScalar) {
        return ScalarSize(*type.scalar());
    }
    if (type.kind() == TypeDescriptor::Kind::kVector) {
        return kVariableMetadataSize;
    }
    const auto descriptor = descriptors.find(type.name());
    const auto plan = plans.find(type.name());
    if (descriptor != descriptors.end() && plan != plans.end()) {
        if (descriptor->second->aggregate().kind() == AggregateKind::kStruct) {
            return plan->second->object_size();
        }
        return kVariableMetadataSize;
    }
    return 0;
}

size_t NestedObjectSize(
    const TypeDescriptor& type,
    const std::map<std::string, const LayoutPlan*, std::less<>>& plans) {
    const auto plan = plans.find(type.name());
    return plan == plans.end() ? 0 : plan->second->object_size();
}

std::string HeaderGuard(std::string_view include,
                        const CompiledSchema& schema) {
    std::string result("MINO_GENERATED_");
    bool previous_underscore = false;
    for (const unsigned char ch : include) {
        const char output = std::isalnum(ch) != 0
                                ? static_cast<char>(std::toupper(ch))
                                : '_';
        if (output == '_' && previous_underscore) continue;
        result.push_back(output);
        previous_underscore = output == '_';
    }
    std::string digest_material;
    for (const auto& descriptor : schema.types()) {
        for (std::byte byte : descriptor->identity().canonical_digest()) {
            digest_material.push_back(static_cast<char>(byte));
        }
    }
    result.push_back('_');
    result.append(DigestHex(Sha256(digest_material)));
    result.push_back('_');
    return result;
}

Result<std::vector<std::shared_ptr<const SchemaDescriptor>>> ExactClosure(
    const SchemaDescriptor& root,
    std::span<const std::shared_ptr<const SchemaDescriptor>> available) {
    std::map<std::string, CanonicalDigest, std::less<>> expected;
    expected.emplace(std::string(root.aggregate().full_name()),
                     root.identity().canonical_digest());
    for (const DependencyDescriptor& dependency : root.dependencies()) {
        const auto [it, inserted] = expected.emplace(
            std::string(dependency.full_name()), dependency.digest());
        if (!inserted && it->second != dependency.digest()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "descriptor dependency closure is inconsistent");
        }
    }
    std::map<std::string, std::shared_ptr<const SchemaDescriptor>, std::less<>> found;
    for (const auto& descriptor : available) {
        if (descriptor == nullptr) continue;
        const std::string name(descriptor->aggregate().full_name());
        const auto wanted = expected.find(name);
        if (wanted == expected.end()) continue;
        if (wanted->second != descriptor->identity().canonical_digest()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "descriptor closure has a mismatched dependency");
        }
        found.insert_or_assign(name, descriptor);
    }
    if (!found.contains(root.aggregate().full_name())) {
        found.emplace(std::string(root.aggregate().full_name()),
                      std::shared_ptr<const SchemaDescriptor>(
                          std::shared_ptr<const SchemaDescriptor>{}, &root));
    }
    if (found.size() != expected.size()) {
        return Status::Error(StatusCode::kNotFound,
                             "descriptor closure is missing a dependency");
    }
    std::vector<std::shared_ptr<const SchemaDescriptor>> result;
    result.reserve(found.size());
    for (auto& [name, descriptor] : found) {
        static_cast<void>(name);
        result.push_back(std::move(descriptor));
    }
    return result;
}

bool LayoutMatches(const LayoutPlan& lhs, const LayoutPlan& rhs) {
    if (lhs.layout_version() != rhs.layout_version() ||
        lhs.header_size() != rhs.header_size() ||
        lhs.presence_bitmap_offset() != rhs.presence_bitmap_offset() ||
        lhs.presence_bitmap_words() != rhs.presence_bitmap_words() ||
        lhs.fixed_area_offset() != rhs.fixed_area_offset() ||
        lhs.fixed_area_size() != rhs.fixed_area_size() ||
        lhs.unknown_fields_offset() != rhs.unknown_fields_offset() ||
        lhs.object_size() != rhs.object_size() ||
        lhs.object_alignment() != rhs.object_alignment() ||
        lhs.max_child_bytes() != rhs.max_child_bytes() ||
        lhs.max_dynamic_children() != rhs.max_dynamic_children() ||
        lhs.fields().size() != rhs.fields().size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.fields().size(); ++i) {
        const FieldLayout& a = lhs.fields()[i];
        const FieldLayout& b = rhs.fields()[i];
        if (a.field_id() != b.field_id() || a.offset() != b.offset() ||
            a.size() != b.size() || a.alignment() != b.alignment() ||
            a.storage_kind() != b.storage_kind() ||
            a.presence_bit() != b.presence_bit() ||
            a.max_child_bytes() != b.max_child_bytes() ||
            a.max_dynamic_children() != b.max_dynamic_children()) {
            return false;
        }
    }
    return true;
}

Status ValidateInputs(
    const CompiledSchema& schema, std::span<const LayoutPlan> layouts,
    std::span<const std::shared_ptr<const SchemaDescriptor>> closure,
    const CodeGeneratorOptions& options) {
    if (schema.types().empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "code generation requires at least one type");
    }
    if (schema.types().size() != layouts.size()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "layout count does not match compiled type count");
    }
    if (options.header_include.empty() ||
        options.header_include.find_first_of("\r\n\"") != std::string::npos) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "header_include is empty or unsafe");
    }
    std::map<std::string, CanonicalDigest, std::less<>> names;
    std::map<uint64_t, CanonicalDigest> short_ids;
    for (const auto& descriptor : closure) {
        if (descriptor == nullptr ||
            DigestShortId(descriptor->identity().canonical_digest()) !=
                descriptor->identity().short_id()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "descriptor closure has an invalid identity");
        }
        const std::string name(descriptor->aggregate().full_name());
        const auto existing = names.find(name);
        if (existing != names.end() &&
            existing->second != descriptor->identity().canonical_digest()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "descriptor closure FQN maps to multiple digests");
        }
        const auto short_existing = short_ids.find(descriptor->identity().short_id());
        if (short_existing != short_ids.end() &&
            short_existing->second != descriptor->identity().canonical_digest()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "descriptor closure has a short-ID collision");
        }
        names.insert_or_assign(name, descriptor->identity().canonical_digest());
        short_ids.insert_or_assign(descriptor->identity().short_id(),
                                   descriptor->identity().canonical_digest());
    }
    for (size_t i = 0; i < schema.types().size(); ++i) {
        if (schema.types()[i] == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "compiled schema contains a null descriptor");
        }
        auto exact = ExactClosure(*schema.types()[i], closure);
        if (!exact.ok()) return exact.status();
        auto authoritative = LayoutPlanner::Plan(*schema.types()[i], *exact);
        if (!authoritative.ok()) {
            return Status::Error(authoritative.status().code(),
                                 "cannot resolve complete codegen layout closure");
        }
        if (!LayoutMatches(layouts[i], *authoritative)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "provided LayoutPlan is not authoritative");
        }
    }
    return Status::Ok();
}



Result<std::map<std::string, std::string, std::less<>>> BuildTypeNames(
    const CompiledSchema& schema) {
    std::map<std::string, std::set<std::string, std::less<>>, std::less<>> symbols;
    std::map<std::string, std::string, std::less<>> result;
    for (const auto& descriptor : schema.types()) {
        const auto parts = SplitName(descriptor->aggregate().full_name());
        const std::string cpp_namespace = NamespaceName(parts);
        std::string candidate = CppIdentifier(parts.back());
        for (size_t attempt = 0; attempt < 2; ++attempt) {
            const std::array<std::string, 5> generated = {
                candidate, candidate + "VariableMetadata",
                candidate + "ObjectHeader", candidate + "Accessor",
                candidate + "Builder"};
            bool collision = false;
            for (const std::string& symbol : generated) {
                collision = collision || symbols[cpp_namespace].contains(symbol);
            }
            if (!collision) {
                for (const std::string& symbol : generated) {
                    symbols[cpp_namespace].insert(symbol);
                }
                result.emplace(std::string(descriptor->aggregate().full_name()),
                               std::move(candidate));
                break;
            }
            candidate = CppIdentifier(parts.back()) + "_type_" +
                        DigestHex(descriptor->identity().canonical_digest()).substr(0, 8);
        }
        if (!result.contains(descriptor->aggregate().full_name())) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "C++ type/helper names collide after mangling");
        }
    }
    return result;
}

Status EmitType(
    const SchemaDescriptor& descriptor, const LayoutPlan& layout,
    const std::map<std::string, const LayoutPlan*, std::less<>>& plans,
    const std::map<std::string, const SchemaDescriptor*, std::less<>>& descriptors,
    const std::map<std::string, std::string, std::less<>>& type_names,
    std::string& header, std::string& source) {
    const auto parts = SplitName(descriptor.aggregate().full_name());
    const std::string cpp_namespace = NamespaceName(parts);
    const std::string& type_name =
        type_names.at(std::string(descriptor.aggregate().full_name()));
    const std::string qualified_name = cpp_namespace.empty()
                                           ? "::" + type_name
                                           : "::" + cpp_namespace + "::" + type_name;
    const std::string metadata_name = type_name + "VariableMetadata";
    const std::string header_name = type_name + "ObjectHeader";
    const std::string accessor_name = type_name + "Accessor";
    const std::string builder_name = type_name + "Builder";
    auto field_names_result = FieldNames(descriptor.aggregate(), layout.fields());
    if (!field_names_result.ok()) return field_names_result.status();
    const auto& field_names = *field_names_result;

    if (!cpp_namespace.empty()) Line(header, "namespace " + cpp_namespace + " {");
    Line(header);
    Line(header, "// Logical value returned by variable-field accessors. Its members are");
    Line(header, "// serialized individually; sizeof(" + metadata_name + ") is not a format.");
    Line(header, "struct " + metadata_name + " {");
    Line(header, "    std::uint64_t offset = 0;");
    Line(header, "    std::uint32_t generation = 0;");
    Line(header, "    std::uint32_t region_id = 0;");
    Line(header, "    std::uint64_t length = 0;");
    Line(header, "    std::uint64_t capacity = 0;");
    Line(header, "    std::uint64_t element_size = 0;");
    Line(header, "};");
    Line(header);
    Line(header, "// Logical object-header value. The façade reads each member at its stable");
    Line(header, "// byte offset; this host value is never copied into the SHM format.");
    Line(header, "struct " + header_name + " {");
    Line(header, "    std::uint32_t layout_version = 0;");
    Line(header, "    std::uint32_t header_size = 0;");
    Line(header, "    std::uint64_t schema_short_id = 0;");
    Line(header, "    std::uint64_t object_size = 0;");
    Line(header, "    std::uint32_t field_count = 0;");
    Line(header, "    std::uint32_t presence_bitmap_words = 0;");
    Line(header, "};");
    Line(header);
    Line(header, "struct alignas(" + std::to_string(layout.object_alignment()) + ") " +
                     type_name + " final {");
    Line(header, "    static constexpr std::size_t kObjectSize = " +
                     std::to_string(layout.object_size()) + ";");
    Line(header, "    static constexpr std::size_t kFixedAreaSize = " +
                     std::to_string(layout.fixed_area_size()) + ";");
    Line(header, "    static constexpr std::size_t kObjectAlignment = " +
                     std::to_string(layout.object_alignment()) + ";");
    Line(header, "    static constexpr std::uint64_t kSchemaShortId = 0x" +
                     Hex64(descriptor.identity().short_id()) + "ULL;");
    Line(header, "    static constexpr std::uint32_t kSchemaVersion = " +
                     std::to_string(descriptor.identity().schema_version()) + "u;");
    Line(header, "    static constexpr std::uint32_t kLayoutVersion = " +
                     std::to_string(descriptor.identity().layout_version()) + "u;");
    Line(header, "    static constexpr std::array<std::uint8_t, 32> kSchemaDigest = {");
    Line(header, "        " + DigestInitializer(descriptor.identity().canonical_digest()));
    Line(header, "    };");
    Line(header, "    std::array<std::byte, kObjectSize> storage{};");
    Line(header, "};");
    Line(header);

    Line(header, "class " + accessor_name + " {");
    Line(header, "public:");
    Line(header, "    explicit " + accessor_name + "(const " + type_name +
                     "& object) noexcept : data_(object.storage.data()), size_(object.storage.size()) {}");
    Line(header, "    explicit " + accessor_name +
                     "(std::span<const std::byte> bytes) noexcept : data_(bytes.data()), size_(bytes.size()) {}");
    Line(header, "    " + accessor_name +
                     "(const std::byte* data, std::size_t size) noexcept : data_(data), size_(size) {}");
    Line(header);
    Line(header, "    std::uint32_t layout_version() const noexcept { return Load<std::uint32_t>(0); }");
    Line(header, "    std::uint32_t header_size() const noexcept { return Load<std::uint32_t>(4); }");
    Line(header, "    std::uint64_t schema_short_id() const noexcept { return Load<std::uint64_t>(8); }");
    Line(header, "    std::uint64_t object_size() const noexcept { return Load<std::uint64_t>(16); }");
    Line(header, "    std::uint32_t field_count() const noexcept { return Load<std::uint32_t>(24); }");
    Line(header, "    std::uint32_t presence_bitmap_words() const noexcept { return Load<std::uint32_t>(28); }");
    Line(header, "    " + header_name + " header() const noexcept {");
    Line(header, "        return {layout_version(), header_size(), schema_short_id(), object_size(),");
    Line(header, "                field_count(), presence_bitmap_words()};");
    Line(header, "    }");
    Line(header);

    for (size_t i = 0; i < descriptor.aggregate().fields().size(); ++i) {
        const FieldDescriptor& field = descriptor.aggregate().fields()[i];
        const FieldLayout& field_layout = layout.fields()[i];
        const std::string& name = field_names.at(field.id());
        if (field_layout.presence_bit().has_value()) {
            const size_t bit = *field_layout.presence_bit();
            Line(header, "    bool has_" + name + "() const noexcept {");
            Line(header, "        return (Load<std::uint64_t>(" +
                             std::to_string(layout.presence_bitmap_offset() +
                                            (bit / 64) * 8) +
                             ") & (std::uint64_t{1} << " +
                             std::to_string(bit % 64) + ")) != 0;");
            Line(header, "    }");
        }
        if (field_layout.storage_kind() == FieldStorageKind::kScalar) {
            const std::string cpp_type = CppScalar(*field.type().scalar());
            if (field.type().scalar() == ScalarType::kBool) {
                Line(header, "    bool " + name +
                                 "() const noexcept { return LoadByte(" +
                                 std::to_string(field_layout.offset()) + ") == 1; }");
            } else {
                Line(header, "    " + cpp_type + " " + name +
                                 "() const noexcept { return Load<" + cpp_type + ">(" +
                                 std::to_string(field_layout.offset()) + "); }");
            }
        } else if (field_layout.storage_kind() == FieldStorageKind::kVariable) {
            Line(header, "    " + metadata_name + " " + name +
                             "() const noexcept { return ReadVariable(" +
                             std::to_string(field_layout.offset()) + "); }");
        } else {
            Line(header, "    std::span<const std::byte> " + name +
                             "_bytes() const noexcept {");
            Line(header, "        return CanRead(" +
                             std::to_string(field_layout.offset()) + ", " +
                             std::to_string(field_layout.size()) + ")");
            Line(header, "            ? std::span<const std::byte>(data_ + " +
                             std::to_string(field_layout.offset()) + ", " +
                             std::to_string(field_layout.size()) + ")");
            Line(header, "            : std::span<const std::byte>{};");
            Line(header, "    }");
        }
        Line(header);
    }

    Line(header, "    bool valid() const noexcept {");
    Line(header, "        if (data_ == nullptr || size_ < " + type_name +
                     "::kObjectSize || layout_version() != " + type_name +
                     "::kLayoutVersion || header_size() != " +
                     std::to_string(layout.header_size()) + "u ||");
    Line(header, "            schema_short_id() != " + type_name +
                     "::kSchemaShortId || object_size() != " + type_name +
                     "::kFixedAreaSize ||");
    Line(header, "            field_count() != " +
                     std::to_string(descriptor.aggregate().fields().size()) +
                     "u || presence_bitmap_words() != " +
                     std::to_string(layout.presence_bitmap_words()) + "u ||");
    size_t used_presence_bits = 0;
    for (const FieldLayout& field_layout : layout.fields()) {
        if (field_layout.presence_bit().has_value()) {
            used_presence_bits = std::max(used_presence_bits,
                                          *field_layout.presence_bit() + 1);
        }
    }
    Line(header, "            !UnusedPresenceBitsZero(" +
                     std::to_string(used_presence_bits) + "u, " +
                     std::to_string(layout.presence_bitmap_words()) +
                     "u)) return false;");
    for (size_t i = 0; i < descriptor.aggregate().fields().size(); ++i) {
        const FieldDescriptor& field = descriptor.aggregate().fields()[i];
        const FieldLayout& field_layout = layout.fields()[i];
        const std::string& name = field_names.at(field.id());
        const std::string present = field_layout.presence_bit().has_value()
                                        ? "has_" + name + "()"
                                        : "true";
        if (field_layout.presence_bit().has_value()) {
            Line(header, "        if (!" + present + " && !IsZero(" +
                             std::to_string(field_layout.offset()) + "u, " +
                             std::to_string(field_layout.size()) +
                             "u)) return false;");
        }
        if (field_layout.storage_kind() == FieldStorageKind::kScalar &&
            field.type().scalar() == ScalarType::kBool) {
            Line(header, "        if (" + present + " && LoadByte(" +
                             std::to_string(field_layout.offset()) +
                             "u) > 1u) return false;");
        } else if (field_layout.storage_kind() == FieldStorageKind::kVariable) {
            std::string validator;
            if (field.type().kind() == TypeDescriptor::Kind::kScalar) {
                validator = "ValidateBytes(" + name + "(), " +
                            std::to_string(field.constraints().max_bytes().value_or(0)) +
                            "u)";
            } else if (field.type().kind() == TypeDescriptor::Kind::kVector) {
                validator = "ValidateVector(" + name + "(), " +
                            std::to_string(ElementSize(*field.type().element_type(),
                                                       plans, descriptors)) +
                            "u, " +
                            std::to_string(field.constraints().max_capacity().value_or(0)) +
                            "u)";
            } else {
                validator = "ValidateNested(" + name + "(), " +
                            std::to_string(NestedObjectSize(field.type(), plans)) +
                            "u)";
            }
            Line(header, "        if (" + present + " && !" + validator +
                             ") return false;");
        }
    }
    if (layout.unknown_fields_offset().has_value()) {
        Line(header, "        if (!ValidateUnknown(ReadVariable(" +
                         std::to_string(*layout.unknown_fields_offset()) +
                         "u))) return false;");
    }
    Line(header, "        return true;");
    Line(header, "    }");
    Line(header);
    Line(header, "private:");
    Line(header, "    bool CanRead(std::size_t offset, std::size_t bytes) const noexcept {");
    Line(header, "        return data_ != nullptr && offset <= size_ && bytes <= size_ - offset;");
    Line(header, "    }");
    Line(header, "    std::uint8_t LoadByte(std::size_t offset) const noexcept {");
    Line(header, "        return CanRead(offset, 1) ? static_cast<std::uint8_t>(data_[offset]) : 0;");
    Line(header, "    }");
    Line(header, "    template <typename T> T Load(std::size_t offset) const noexcept {");
    Line(header, "        if (!CanRead(offset, sizeof(T))) return T{};");
    Line(header, "        std::array<std::byte, sizeof(T)> native{};");
    Line(header, "        for (std::size_t i = 0; i < sizeof(T); ++i) {");
    Line(header, "            const std::size_t index = std::endian::native == std::endian::little ? i : sizeof(T) - 1u - i;");
    Line(header, "            native[index] = data_[offset + i];");
    Line(header, "        }");
    Line(header, "        return std::bit_cast<T>(native);");
    Line(header, "    }");
    Line(header, "    bool IsZero(std::size_t offset, std::size_t bytes) const noexcept {");
    Line(header, "        if (!CanRead(offset, bytes)) return false;");
    Line(header, "        for (std::size_t i = 0; i < bytes; ++i) if (data_[offset + i] != std::byte{0}) return false;");
    Line(header, "        return true;");
    Line(header, "    }");
    Line(header, "    bool UnusedPresenceBitsZero(std::size_t used, std::size_t words) const noexcept {");
    Line(header, "        for (std::size_t bit = used; bit < words * 64u; ++bit) {");
    Line(header, "            if ((Load<std::uint64_t>(" +
                     std::to_string(layout.presence_bitmap_offset()) +
                     "u + (bit / 64u) * 8u) & (std::uint64_t{1} << (bit % 64u))) != 0) return false;");
    Line(header, "        }");
    Line(header, "        return true;");
    Line(header, "    }");
    Line(header, "    " + metadata_name + " ReadVariable(std::size_t offset) const noexcept {");
    Line(header, "        if (!CanRead(offset, 40)) return {};");
    Line(header, "        return {Load<std::uint64_t>(offset), Load<std::uint32_t>(offset + 8),");
    Line(header, "                Load<std::uint32_t>(offset + 12), Load<std::uint64_t>(offset + 16),");
    Line(header, "                Load<std::uint64_t>(offset + 24), Load<std::uint64_t>(offset + 32)};");
    Line(header, "    }");
    Line(header, "    static bool ValidateBasic(const " + metadata_name + "& value) noexcept {");
    Line(header, "        if (value.length > value.capacity) return false;");
    Line(header, "        return value.offset == 0 ? value.length == 0 && value.capacity == 0 : value.capacity != 0;");
    Line(header, "    }");
    Line(header, "    static bool ValidateBytes(const " + metadata_name +
                     "& value, std::uint64_t maximum) noexcept {");
    Line(header, "        return ValidateBasic(value) && value.element_size == 1 && value.capacity <= maximum;");
    Line(header, "    }");
    Line(header, "    static bool ValidateVector(const " + metadata_name +
                     "& value, std::uint64_t element_size, std::uint64_t maximum) noexcept {");
    Line(header, "        return ValidateBasic(value) && element_size != 0 && value.element_size == element_size && value.capacity <= maximum;");
    Line(header, "    }");
    Line(header, "    static bool ValidateNested(const " + metadata_name +
                     "& value, std::uint64_t object_size) noexcept {");
    Line(header, "        return value.offset != 0 && object_size != 0 && value.length == object_size &&");
    Line(header, "               value.capacity == object_size && value.element_size == 1;");
    Line(header, "    }");
    Line(header, "    static bool ValidateUnknown(const " + metadata_name + "& value) noexcept {");
    Line(header, "        if (value.element_size != 0 || value.length > " +
                     std::to_string(kDynamicUnknownFieldMaxCount) + "u || value.capacity > " +
                     std::to_string(kDynamicUnknownFieldMaxBytes) + "u) return false;");
    Line(header, "        return value.length == 0 ? value.offset == 0 && value.capacity == 0");
    Line(header, "                                 : value.offset != 0 && value.capacity != 0;");
    Line(header, "    }");
    Line(header, "    const std::byte* data_ = nullptr;");
    Line(header, "    std::size_t size_ = 0;");
    Line(header, "};");
    Line(header);

    Line(header, "class " + builder_name + " {");
    Line(header, "public:");
    Line(header, "    explicit " + builder_name + "(" + type_name +
                     "& object) noexcept : object_(&object), data_(object.storage.data()) {");
    Line(header, "        std::fill(object.storage.begin(), object.storage.end(), std::byte{0});");
    Line(header, "        Store<std::uint32_t>(0, " + type_name + "::kLayoutVersion);");
    Line(header, "        Store<std::uint32_t>(4, " + std::to_string(layout.header_size()) + "u);");
    Line(header, "        Store<std::uint64_t>(8, " + type_name + "::kSchemaShortId);");
    Line(header, "        Store<std::uint64_t>(16, " + type_name + "::kFixedAreaSize);");
    Line(header, "        Store<std::uint32_t>(24, " +
                     std::to_string(descriptor.aggregate().fields().size()) + "u);");
    Line(header, "        Store<std::uint32_t>(28, " +
                     std::to_string(layout.presence_bitmap_words()) + "u);");
    Line(header, "    }");
    Line(header, "    " + type_name + "& object() noexcept { return *object_; }");
    Line(header);

    for (size_t i = 0; i < descriptor.aggregate().fields().size(); ++i) {
        const FieldDescriptor& field = descriptor.aggregate().fields()[i];
        const FieldLayout& field_layout = layout.fields()[i];
        const std::string& name = field_names.at(field.id());
        if (field_layout.storage_kind() == FieldStorageKind::kScalar) {
            const std::string cpp_type = CppScalar(*field.type().scalar());
            Line(header, "    void set_" + name + "(" + cpp_type + " value) noexcept {");
            if (field.type().scalar() == ScalarType::kBool) {
                Line(header, "        data_[" +
                                 std::to_string(field_layout.offset()) +
                                 "] = value ? std::byte{1} : std::byte{0};");
            } else {
                Line(header, "        Store<" + cpp_type + ">(" +
                                 std::to_string(field_layout.offset()) + ", value);");
            }
            if (field_layout.presence_bit().has_value()) {
                Line(header, "        SetPresence(" +
                                 std::to_string(*field_layout.presence_bit()) + ", true);");
            }
            Line(header, "    }");
        } else if (field_layout.storage_kind() == FieldStorageKind::kVariable) {
            std::string condition;
            if (field.type().kind() == TypeDescriptor::Kind::kScalar) {
                condition = "value.length > value.capacity || value.capacity > " +
                            std::to_string(field.constraints().max_bytes().value_or(0)) +
                            "u || value.element_size != 1u || "
                            "(value.offset == 0 ? (value.length != 0 || value.capacity != 0) : value.capacity == 0)";
            } else if (field.type().kind() == TypeDescriptor::Kind::kVector) {
                const size_t element_size = ElementSize(
                    *field.type().element_type(), plans, descriptors);
                condition = "value.length > value.capacity || value.capacity > " +
                            std::to_string(field.constraints().max_capacity().value_or(0)) +
                            "u || value.element_size != " +
                            std::to_string(element_size) +
                            "u || (value.offset == 0 ? (value.length != 0 || value.capacity != 0) : value.capacity == 0)";
            } else {
                const size_t nested_size = NestedObjectSize(field.type(), plans);
                condition = "value.offset == 0 || value.length != " +
                            std::to_string(nested_size) +
                            "u || value.capacity != " +
                            std::to_string(nested_size) +
                            "u || value.element_size != 1u";
            }
            Line(header, "    bool set_" + name + "(const " + metadata_name +
                             "& value) noexcept {");
            Line(header, "        if (" + condition + ") return false;");
            Line(header, "        WriteVariable(" +
                             std::to_string(field_layout.offset()) + ", value);");
            if (field_layout.presence_bit().has_value()) {
                Line(header, "        SetPresence(" +
                                 std::to_string(*field_layout.presence_bit()) + ", true);");
            }
            Line(header, "        return true;");
            Line(header, "    }");
        } else {
            Line(header, "    bool set_" + name +
                             "_bytes(std::span<const std::byte> value) noexcept {");
            Line(header, "        if (value.size() != " +
                             std::to_string(field_layout.size()) + "u) return false;");
            Line(header, "        std::memcpy(data_ + " +
                             std::to_string(field_layout.offset()) +
                             ", value.data(), value.size());");
            if (field_layout.presence_bit().has_value()) {
                Line(header, "        SetPresence(" +
                                 std::to_string(*field_layout.presence_bit()) + ", true);");
            }
            Line(header, "        return true;");
            Line(header, "    }");
        }
        if (field_layout.presence_bit().has_value()) {
            Line(header, "    void clear_" + name + "() noexcept {");
            Line(header, "        std::fill_n(data_ + " +
                             std::to_string(field_layout.offset()) + ", " +
                             std::to_string(field_layout.size()) + ", std::byte{0});");
            Line(header, "        SetPresence(" +
                             std::to_string(*field_layout.presence_bit()) + ", false);");
            Line(header, "    }");
        }
        Line(header);
    }

    Line(header, "private:");
    Line(header, "    template <typename T> void Store(std::size_t offset, T value) noexcept {");
    Line(header, "        const auto native = std::bit_cast<std::array<std::byte, sizeof(T)>>(value);");
    Line(header, "        for (std::size_t i = 0; i < sizeof(T); ++i) {");
    Line(header, "            const std::size_t index = std::endian::native == std::endian::little ? i : sizeof(T) - 1u - i;");
    Line(header, "            data_[offset + i] = native[index];");
    Line(header, "        }");
    Line(header, "    }");
    Line(header, "    void WriteVariable(std::size_t offset, const " + metadata_name +
                     "& value) noexcept {");
    Line(header, "        Store<std::uint64_t>(offset, value.offset);");
    Line(header, "        Store<std::uint32_t>(offset + 8, value.generation);");
    Line(header, "        Store<std::uint32_t>(offset + 12, value.region_id);");
    Line(header, "        Store<std::uint64_t>(offset + 16, value.length);");
    Line(header, "        Store<std::uint64_t>(offset + 24, value.capacity);");
    Line(header, "        Store<std::uint64_t>(offset + 32, value.element_size);");
    Line(header, "    }");
    Line(header, "    void SetPresence(std::size_t bit, bool present) noexcept {");
    Line(header, "        const std::size_t offset = " +
                     std::to_string(layout.presence_bitmap_offset()) + "u + (bit / 64u) * 8u;");
    Line(header, "        std::uint64_t word = 0;");
    Line(header, "        for (std::size_t i = 0; i < 8; ++i) word |= std::uint64_t{static_cast<std::uint8_t>(data_[offset + i])} << (i * 8u);");
    Line(header, "        const std::uint64_t mask = std::uint64_t{1} << (bit % 64u);");
    Line(header, "        word = present ? (word | mask) : (word & ~mask);");
    Line(header, "        Store<std::uint64_t>(offset, word);");
    Line(header, "    }");
    Line(header, "    " + type_name + "* object_;");
    Line(header, "    std::byte* data_;");
    Line(header, "};");
    Line(header);
    if (!cpp_namespace.empty()) Line(header, "}  // namespace " + cpp_namespace);
    Line(header);

    Line(header, "namespace mino {");
    Line(header, "template <> struct StaticMessageTraits<" + qualified_name + "> {");
    Line(header, "    static constexpr bool kIsSpecialized = true;");
    Line(header, "    static constexpr TypeId type_id{static_cast<std::uint32_t>(" +
                     qualified_name + "::kSchemaShortId)};");
    Line(header, "    static constexpr std::uint32_t message_type = type_id.value;");
    Line(header, "    static constexpr std::uint32_t schema_version = " +
                     qualified_name + "::kSchemaVersion;");
    Line(header, "    static constexpr std::uint64_t schema_short_id = " +
                     qualified_name + "::kSchemaShortId;");
    Line(header, "    static constexpr std::uint32_t layout_version = " +
                     qualified_name + "::kLayoutVersion;");
    Line(header, "    static constexpr std::uint32_t index_flags = 0;");
    Line(header, "    static Status Validate(const " + qualified_name +
                     "& value) noexcept {");
    Line(header, "        return " + qualified_name +
                     "Accessor(value).valid() ? Status::Ok() :");
    Line(header, "            Status::Error(StatusCode::kSchemaMismatch, \"invalid generated SHM object\");");
    Line(header, "    }");
    Line(header, "};");
    Line(header, "}  // namespace mino");
    Line(header);

    Line(source, "static_assert(sizeof(" + qualified_name + ") == " +
                     qualified_name + "::kObjectSize);");
    Line(source, "static_assert(alignof(" + qualified_name + ") == " +
                     qualified_name + "::kObjectAlignment);");
    Line(source, "static_assert(std::is_standard_layout_v<" + qualified_name + ">);");
    Line(source, "static_assert(std::is_trivially_copyable_v<" + qualified_name + ">);");
    const std::string qualified_header =
        cpp_namespace.empty() ? "::" + header_name
                              : "::" + cpp_namespace + "::" + header_name;
    Line(source, "static_assert(std::is_standard_layout_v<" + qualified_header + ">);");
    Line(source);
    return Status::Ok();
}

}  // namespace

Result<GeneratedArtifacts> CodeGenerator::Generate(
    const CompiledSchema& schema, std::span<const LayoutPlan> layouts,
    const CodeGeneratorOptions& options) noexcept {
    try {
        std::vector<std::shared_ptr<const SchemaDescriptor>> closure(
            options.descriptor_closure.begin(),
            options.descriptor_closure.end());
        for (const auto& descriptor : schema.types()) closure.push_back(descriptor);
        const Status validation = ValidateInputs(schema, layouts, closure, options);
        if (!validation.ok()) return validation;
        auto type_names = BuildTypeNames(schema);
        if (!type_names.ok()) return type_names.status();

        GeneratedArtifacts artifacts;
        const std::string guard = HeaderGuard(options.header_include, schema);
        Line(artifacts.header, "// Generated by minoc. DO NOT EDIT.");
        Line(artifacts.header, "// Stable SHM layout: all fields are accessed by compiler-planned byte offsets.");
        Line(artifacts.header);
        Line(artifacts.header, "#ifndef " + guard);
        Line(artifacts.header, "#define " + guard);
        Line(artifacts.header);
        Line(artifacts.header, "#include <algorithm>");
        Line(artifacts.header, "#include <array>");
        Line(artifacts.header, "#include <bit>");
        Line(artifacts.header, "#include <cstddef>");
        Line(artifacts.header, "#include <cstdint>");
        Line(artifacts.header, "#include <cstring>");
        Line(artifacts.header, "#include <span>");
        Line(artifacts.header, "#include <type_traits>");
        Line(artifacts.header);
        Line(artifacts.header, "#include \"mino/runtime/message_traits.h\"");
        Line(artifacts.header);

        Line(artifacts.source, "// Generated by minoc. DO NOT EDIT.");
        Line(artifacts.source, "#include \"" + options.header_include + "\"");
        Line(artifacts.source);

        std::map<std::string, LayoutPlan, std::less<>> owned_plans;
        std::map<std::string, const LayoutPlan*, std::less<>> plan_by_name;
        std::map<std::string, const SchemaDescriptor*, std::less<>> descriptor_by_name;
        for (const auto& descriptor : closure) {
            const std::string name(descriptor->aggregate().full_name());
            if (descriptor_by_name.contains(name)) continue;
            auto exact = ExactClosure(*descriptor, closure);
            if (!exact.ok()) return exact.status();
            auto plan = LayoutPlanner::Plan(*descriptor, *exact);
            if (!plan.ok()) return plan.status();
            auto [it, inserted] = owned_plans.emplace(name, std::move(*plan));
            static_cast<void>(inserted);
            plan_by_name.emplace(name, &it->second);
            descriptor_by_name.emplace(name, descriptor.get());
        }
        for (size_t i = 0; i < schema.types().size(); ++i) {
            const Status emitted = EmitType(
                *schema.types()[i], layouts[i], plan_by_name,
                descriptor_by_name, *type_names, artifacts.header,
                artifacts.source);
            if (!emitted.ok()) return emitted;
        }
        while (artifacts.source.size() >= 2 &&
               artifacts.source.ends_with("\n\n")) {
            artifacts.source.pop_back();
        }
        Line(artifacts.header, "#endif  // " + guard);
        auto encoded_descriptor = EncodeDescriptorArtifact(schema, layouts);
        if (!encoded_descriptor.ok()) return encoded_descriptor.status();
        artifacts.descriptor = std::move(*encoded_descriptor);

        if (artifacts.header.size() > options.max_output_bytes ||
            artifacts.source.size() > options.max_output_bytes ||
            artifacts.descriptor.size() > options.max_output_bytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "generated artifact exceeds max_output_bytes");
        }
        return artifacts;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::schema::codegen
