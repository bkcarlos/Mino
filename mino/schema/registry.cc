// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/registry.h"

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
#include <cmath>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <set>
#include <string>
#include <utility>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"
#include "mino/schema/lexer.h"
#include "mino/schema/parser.h"

namespace mino::schema {
namespace {

constexpr uint32_t kMaxFieldId = 536870911;

Status Invalid(std::string message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Mismatch(std::string message) {
    return Status::Error(StatusCode::kSchemaMismatch, message);
}

bool IsIdentifierStart(char c) noexcept {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
}

bool IsIdentifierContinue(char c) noexcept {
    return IsIdentifierStart(c) || (c >= '0' && c <= '9');
}

bool IsIdentifier(std::string_view text) noexcept {
    if (text.empty() || !IsIdentifierStart(text.front())) return false;
    return std::all_of(text.begin() + 1, text.end(), IsIdentifierContinue);
}

bool IsFullName(std::string_view text) noexcept {
    if (text.empty()) return false;
    size_t begin = 0;
    while (begin < text.size()) {
        const size_t dot = text.find('.', begin);
        const size_t end = dot == std::string_view::npos ? text.size() : dot;
        if (!IsIdentifier(text.substr(begin, end - begin))) return false;
        if (dot == std::string_view::npos) return true;
        begin = dot + 1;
    }
    return false;
}

std::string_view ScalarName(ScalarType scalar) noexcept {
    switch (scalar) {
        case ScalarType::kInt32: return "int32";
        case ScalarType::kInt64: return "int64";
        case ScalarType::kUint32: return "uint32";
        case ScalarType::kUint64: return "uint64";
        case ScalarType::kFixed32: return "fixed32";
        case ScalarType::kFixed64: return "fixed64";
        case ScalarType::kFloat: return "float";
        case ScalarType::kDouble: return "double";
        case ScalarType::kBool: return "bool";
        case ScalarType::kString: return "string";
        case ScalarType::kBytes: return "bytes";
    }
    return {};
}

std::optional<uint64_t> ParseHexBits(std::string_view text,
                                     size_t digits) noexcept {
    if (text.size() != digits + 2 || text[0] != '0' || text[1] != 'x') {
        return std::nullopt;
    }
    uint64_t result = 0;
    for (char c : text.substr(2)) {
        uint8_t digit = 0;
        if (c >= '0' && c <= '9') {
            digit = static_cast<uint8_t>(c - '0');
        } else if (c >= 'a' && c <= 'f') {
            digit = static_cast<uint8_t>(c - 'a' + 10);
        } else {
            return std::nullopt;
        }
        result = (result << 4) | digit;
    }
    return result;
}

bool IsCanonicalSigned(std::string_view text, int64_t minimum,
                       int64_t maximum) noexcept {
    int64_t value = 0;
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), value);
    return !text.empty() && parsed.ec == std::errc() &&
           parsed.ptr == text.data() + text.size() && value >= minimum &&
           value <= maximum && std::to_string(value) == text;
}

bool IsCanonicalUnsigned(std::string_view text, uint64_t maximum) noexcept {
    uint64_t value = 0;
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), value);
    return !text.empty() && parsed.ec == std::errc() &&
           parsed.ptr == text.data() + text.size() && value <= maximum &&
           std::to_string(value) == text;
}

bool IsStringLike(const TypeDescriptor& type) noexcept {
    if (type.kind() == TypeDescriptor::Kind::kScalar) {
        return type.scalar() == ScalarType::kString ||
               type.scalar() == ScalarType::kBytes;
    }
    return type.kind() == TypeDescriptor::Kind::kVector &&
           type.element_type() != nullptr && IsStringLike(*type.element_type());
}

Result<void> ValidateType(const TypeDescriptor& type, size_t depth) {
    if (depth > 64) return Invalid("descriptor type nesting is too deep");
    if (type.kind() == TypeDescriptor::Kind::kScalar) {
        if (!type.scalar().has_value() || type.element_type() != nullptr ||
            type.name() != ScalarName(*type.scalar())) {
            return Invalid("descriptor contains an invalid scalar type");
        }
        return Result<void>();
    }
    if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
        if (type.scalar().has_value() || type.element_type() != nullptr ||
            !IsFullName(type.name())) {
            return Invalid("descriptor contains an invalid user type");
        }
        return Result<void>();
    }
    if (type.scalar().has_value() || type.name() != "vector" ||
        type.element_type() == nullptr) {
        return Invalid("descriptor contains an invalid vector type");
    }
    return ValidateType(*type.element_type(), depth + 1);
}

bool DefaultMatchesScalar(const DefaultValue& value, ScalarType scalar) {
    const std::string_view text = value.canonical_value();
    switch (scalar) {
        case ScalarType::kInt32:
            return value.kind() == DefaultValue::Kind::kInteger &&
                   IsCanonicalSigned(text, std::numeric_limits<int32_t>::min(),
                                     std::numeric_limits<int32_t>::max());
        case ScalarType::kInt64:
            return value.kind() == DefaultValue::Kind::kInteger &&
                   IsCanonicalSigned(text, std::numeric_limits<int64_t>::min(),
                                     std::numeric_limits<int64_t>::max());
        case ScalarType::kUint32:
        case ScalarType::kFixed32:
            return value.kind() == DefaultValue::Kind::kInteger &&
                   IsCanonicalUnsigned(text,
                                       std::numeric_limits<uint32_t>::max());
        case ScalarType::kUint64:
        case ScalarType::kFixed64:
            return value.kind() == DefaultValue::Kind::kInteger &&
                   IsCanonicalUnsigned(text,
                                       std::numeric_limits<uint64_t>::max());
        case ScalarType::kFloat: {
            const auto bits = ParseHexBits(text, 8);
            return value.kind() == DefaultValue::Kind::kFloat32 &&
                   bits.has_value() &&
                   std::isfinite(std::bit_cast<float>(
                       static_cast<uint32_t>(*bits)));
        }
        case ScalarType::kDouble: {
            const auto bits = ParseHexBits(text, 16);
            return value.kind() == DefaultValue::Kind::kFloat64 &&
                   bits.has_value() &&
                   std::isfinite(std::bit_cast<double>(*bits));
        }
        case ScalarType::kBool:
            return value.kind() == DefaultValue::Kind::kBoolean &&
                   (text == "true" || text == "false");
        case ScalarType::kString:
            return value.kind() == DefaultValue::Kind::kString &&
                   IsValidUtf8(text);
        case ScalarType::kBytes:
            return value.kind() == DefaultValue::Kind::kBytes;
    }
    return false;
}

Result<void> ValidateAggregate(const AggregateDescriptor& aggregate) {
    if (!IsFullName(aggregate.full_name())) {
        return Invalid("descriptor aggregate has an invalid full name");
    }
    uint32_t previous_field_id = 0;
    std::set<std::string, std::less<>> field_names;
    for (const FieldDescriptor& field : aggregate.fields()) {
        if (field.id() < 1 || field.id() > kMaxFieldId ||
            field.id() <= previous_field_id) {
            return Invalid("descriptor field IDs are invalid or duplicated");
        }
        previous_field_id = field.id();
        if (!IsIdentifier(field.name()) ||
            !field_names.insert(std::string(field.name())).second) {
            return Invalid("descriptor field names are invalid or duplicated");
        }
        auto type = ValidateType(field.type(), 1);
        if (!type.ok()) return type.status();
        const bool string_like = IsStringLike(field.type());
        const bool vector = field.type().kind() == TypeDescriptor::Kind::kVector;
        if (field.constraints().max_bytes().has_value() != string_like ||
            field.constraints().max_capacity().has_value() != vector ||
            (field.constraints().max_bytes().has_value() &&
             *field.constraints().max_bytes() == 0) ||
            (field.constraints().max_capacity().has_value() &&
             *field.constraints().max_capacity() == 0)) {
            return Invalid("descriptor field constraints do not match its type");
        }
        if (field.default_value().has_value()) {
            if (field.type().kind() != TypeDescriptor::Kind::kScalar ||
                !field.type().scalar().has_value() ||
                !DefaultMatchesScalar(*field.default_value(),
                                      *field.type().scalar())) {
                return Invalid("descriptor field default is invalid");
            }
            if ((field.default_value()->kind() == DefaultValue::Kind::kString ||
                 field.default_value()->kind() == DefaultValue::Kind::kBytes) &&
                field.default_value()->canonical_value().size() >
                    *field.constraints().max_bytes()) {
                return Invalid("descriptor field default exceeds max_bytes");
            }
        }
    }

    uint32_t previous_last = 0;
    for (const ReservedRangeDescriptor& range :
         aggregate.reserved_ranges()) {
        if (range.first() < 1 || range.last() > kMaxFieldId ||
            range.first() > range.last() || range.first() <= previous_last) {
            return Invalid("descriptor reserved ranges are invalid or overlap");
        }
        previous_last = range.last();
    }
    for (const FieldDescriptor& field : aggregate.fields()) {
        if (aggregate.IsReserved(field.id())) {
            return Invalid("descriptor field conflicts with a reserved range");
        }
    }
    return Result<void>();
}

void CollectUserTypes(const TypeDescriptor& type,
                      std::set<std::string, std::less<>>& names) {
    if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
        names.emplace(type.name());
    } else if (type.kind() == TypeDescriptor::Kind::kVector &&
               type.element_type() != nullptr) {
        CollectUserTypes(*type.element_type(), names);
    }
}

void CollectUserTypeReferences(const TypeReference& type,
                               std::set<std::string, std::less<>>& names) {
    if (type.kind == TypeKind::kUserDefined) {
        names.emplace(type.name);
    } else if (type.kind == TypeKind::kVector && type.element_type != nullptr) {
        CollectUserTypeReferences(*type.element_type, names);
    }
}

using DependencyMap =
    std::map<std::string, CanonicalDigest, std::less<>>;

Result<SchemaHandle> RebuildTrustedDescriptor(const SchemaHandle& descriptor) {
    if (descriptor == nullptr) return Invalid("descriptor is null");
    auto aggregate = ValidateAggregate(descriptor->aggregate());
    if (!aggregate.ok()) return aggregate.status();

    std::vector<DependencyDescriptor> dependencies(
        descriptor->dependencies().begin(), descriptor->dependencies().end());
    std::sort(dependencies.begin(), dependencies.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.full_name() < rhs.full_name();
              });
    for (size_t index = 0; index < dependencies.size(); ++index) {
        if (!IsFullName(dependencies[index].full_name()) ||
            dependencies[index].full_name() ==
                descriptor->aggregate().full_name()) {
            return Invalid("descriptor dependency has an invalid full name");
        }
        if (index != 0 && dependencies[index - 1].full_name() ==
                              dependencies[index].full_name()) {
            return Invalid("descriptor dependency names are duplicated");
        }
    }

    auto canonical = Canonicalizer::Canonicalize(descriptor->aggregate(),
                                                  dependencies);
    if (!canonical.ok()) return canonical.status();
    if (canonical->text() != descriptor->canonical_schema()) {
        return Mismatch("descriptor canonical text does not match its aggregate");
    }
    if (canonical->digest() != descriptor->identity().canonical_digest()) {
        return Mismatch("descriptor digest does not match canonical SHA-256");
    }
    if (DigestShortId(canonical->digest()) !=
            descriptor->identity().short_id() ||
        canonical->short_id() != descriptor->identity().short_id()) {
        return Mismatch("descriptor short ID does not match canonical digest");
    }

    SchemaIdentity identity(
        canonical->short_id(), canonical->digest(),
        descriptor->identity().schema_version(),
        descriptor->identity().layout_version());
    return std::make_shared<const SchemaDescriptor>(
        descriptor->aggregate(), std::move(identity),
        std::string(canonical->text()), std::move(dependencies));
}

uint16_t VersionMajor(uint32_t version) noexcept {
    return static_cast<uint16_t>(version >> 16);
}

bool IsBreaking(Compatibility compatibility) noexcept {
    return compatibility == Compatibility::kIncompatible ||
           compatibility == Compatibility::kRequiresTranslation;
}

Status AmbiguousDependency(std::string_view name) {
    return Status::Error(StatusCode::kSchemaMismatch,
                         "ambiguous registered dependency '" +
                             std::string(name) +
                             "'; provide exactly one descriptor in CompileOptions");
}

}  // namespace

uint64_t SchemaRegistry::ShortIdIndex(
    const CanonicalDigest& digest) const noexcept {
    return short_id_index_provider_for_testing_ == nullptr
               ? DigestShortId(digest)
               : short_id_index_provider_for_testing_(digest);
}

Result<std::vector<SchemaHandle>> SchemaRegistry::RegisterIdl(
    std::string_view idl, const CompileOptions& options) noexcept {
    try {
        auto compiled = SchemaCompiler::Compile(idl, options);
        if (compiled.ok()) return RegisterCompiled(*compiled);

        ParserOptions parser_options;
        parser_options.lexer.max_input_bytes = options.max_input_bytes;
        parser_options.lexer.max_tokens = options.max_tokens;
        parser_options.lexer.max_token_bytes = options.max_token_bytes;
        const size_t dependency_count = options.dependencies.size();
        if (dependency_count > std::numeric_limits<size_t>::max() - 16 ||
            options.max_types > std::numeric_limits<size_t>::max() -
                                    dependency_count - 16) {
            parser_options.max_declarations =
                std::numeric_limits<size_t>::max();
        } else {
            parser_options.max_declarations =
                options.max_types + dependency_count + 16;
        }
        parser_options.max_fields = options.max_fields;
        parser_options.max_reserved_ranges = options.max_reserved_ranges;
        parser_options.max_annotations = options.max_annotations;
        parser_options.max_nesting_depth = options.max_nesting_depth;
        auto ast = Parser::Parse(idl, parser_options);
        if (!ast.ok()) return compiled.status();

        const std::string package =
            ast->package.has_value() ? ast->package->name : std::string();
        std::set<std::string, std::less<>> local_names;
        std::set<std::string, std::less<>> references;
        for (const AggregateDeclaration& aggregate : ast->aggregates) {
            local_names.emplace(package.empty()
                                    ? aggregate.name
                                    : package + "." + aggregate.name);
            for (const FieldDeclaration& field : aggregate.fields) {
                CollectUserTypeReferences(field.type, references);
            }
        }

        std::map<CanonicalDigest, SchemaHandle> registered_by_digest;
        std::map<std::string, std::vector<SchemaHandle>, std::less<>>
            registered_by_name;
        {
            std::shared_lock lock(mutex_);
            registered_by_digest = by_digest_;
            for (const auto& [digest, descriptor] : by_digest_) {
                static_cast<void>(digest);
                registered_by_name[std::string(
                    descriptor->aggregate().full_name())]
                    .push_back(descriptor);
            }
        }
        if (registered_by_digest.empty()) return compiled.status();

        std::map<std::string, SchemaHandle, std::less<>> explicit_by_name;
        for (const SchemaHandle& dependency : options.dependencies) {
            if (dependency != nullptr) {
                const std::string name(dependency->aggregate().full_name());
                const auto [it, inserted] =
                    explicit_by_name.emplace(name, dependency);
                if (!inserted &&
                    it->second->identity().canonical_digest() !=
                        dependency->identity().canonical_digest()) {
                    return AmbiguousDependency(name);
                }
            }
        }

        std::map<std::string, SchemaHandle, std::less<>> selected;
        auto add_selected = [&](const SchemaHandle& descriptor) -> Result<void> {
            const std::string name(descriptor->aggregate().full_name());
            const auto explicit_dependency = explicit_by_name.find(name);
            if (explicit_dependency != explicit_by_name.end()) {
                if (explicit_dependency->second->identity().canonical_digest() !=
                    descriptor->identity().canonical_digest()) {
                    return AmbiguousDependency(name);
                }
                return Result<void>();
            }
            const auto [it, inserted] = selected.emplace(name, descriptor);
            if (!inserted &&
                it->second->identity().canonical_digest() !=
                    descriptor->identity().canonical_digest()) {
                return AmbiguousDependency(name);
            }
            return Result<void>();
        };

        for (const std::string& reference : references) {
            const std::string package_relative =
                package.empty() ? reference : package + "." + reference;
            if (local_names.contains(package_relative) ||
                local_names.contains(reference)) {
                continue;
            }

            std::optional<std::string> resolved_name;
            for (const std::string& candidate :
                 std::array<std::string, 2>{package_relative, reference}) {
                if (resolved_name.has_value() && *resolved_name == candidate) {
                    continue;
                }
                if (explicit_by_name.contains(candidate)) {
                    resolved_name = candidate;
                    break;
                }
                const auto registered = registered_by_name.find(candidate);
                if (registered == registered_by_name.end()) continue;
                if (registered->second.size() != 1) {
                    return AmbiguousDependency(candidate);
                }
                resolved_name = candidate;
                auto added = add_selected(registered->second.front());
                if (!added.ok()) return added.status();
                break;
            }
            static_cast<void>(resolved_name);
        }

        std::vector<SchemaHandle> pending;
        for (const auto& [name, descriptor] : explicit_by_name) {
            static_cast<void>(name);
            pending.push_back(descriptor);
        }
        for (const auto& [name, descriptor] : selected) {
            static_cast<void>(name);
            if (std::find(pending.begin(), pending.end(), descriptor) ==
                pending.end()) {
                pending.push_back(descriptor);
            }
        }
        for (size_t index = 0; index < pending.size(); ++index) {
            for (const DependencyDescriptor& dependency :
                 pending[index]->dependencies()) {
                const auto resolved =
                    registered_by_digest.find(dependency.digest());
                if (resolved == registered_by_digest.end() ||
                    resolved->second->aggregate().full_name() !=
                        dependency.full_name()) {
                    return Status::Error(
                        StatusCode::kNotFound,
                        "registered dependency closure is unavailable");
                }
                auto added = add_selected(resolved->second);
                if (!added.ok()) return added.status();
                if (!explicit_by_name.contains(dependency.full_name())) {
                    const auto selected_dependency =
                        selected.find(dependency.full_name());
                    if (selected_dependency != selected.end() &&
                        std::find(pending.begin(), pending.end(),
                                  selected_dependency->second) == pending.end()) {
                        pending.push_back(selected_dependency->second);
                    }
                }
            }
        }
        if (selected.empty()) return compiled.status();

        CompileOptions compile_options = options;
        for (const auto& [name, dependency] : selected) {
            static_cast<void>(name);
            compile_options.dependencies.push_back(dependency);
        }
        compiled = SchemaCompiler::Compile(idl, compile_options);
        if (!compiled.ok()) return compiled.status();
        return RegisterCompiled(*compiled);
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaHandle> SchemaRegistry::RegisterDescriptor(
    SchemaHandle descriptor) noexcept {
    const std::array<SchemaHandle, 1> descriptors = {std::move(descriptor)};
    auto registered = RegisterDescriptors(descriptors);
    if (!registered.ok()) return registered.status();
    return (*registered)[0];
}

Result<SchemaHandle> SchemaRegistry::RegisterDescriptor(
    std::span<const std::byte> descriptor_bytes) noexcept {
    try {
        static_cast<void>(descriptor_bytes);
        return Status::Error(
            StatusCode::kUnsupported,
            "byte descriptor codec is not yet versioned; use the structured API");
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<std::vector<SchemaHandle>> SchemaRegistry::RegisterCompiled(
    const CompiledSchema& schema) noexcept {
    return RegisterDescriptors(schema.types());
}

Result<std::vector<SchemaHandle>> SchemaRegistry::RegisterDescriptors(
    std::span<const SchemaHandle> descriptors) noexcept {
    try {
        std::vector<SchemaHandle> trusted;
        trusted.reserve(descriptors.size());
        for (const SchemaHandle& descriptor : descriptors) {
            auto rebuilt = RebuildTrustedDescriptor(descriptor);
            if (!rebuilt.ok()) return rebuilt.status();
            trusted.push_back(std::move(*rebuilt));
        }

        std::unique_lock lock(mutex_);
        std::map<CanonicalDigest, SchemaHandle> available = by_digest_;
        std::map<std::string, CanonicalDigest, std::less<>> batch_names;
        for (const SchemaHandle& descriptor : trusted) {
            const std::string name(descriptor->aggregate().full_name());
            if (!batch_names.emplace(name,
                                     descriptor->identity().canonical_digest())
                     .second) {
                return Invalid("compiled batch contains duplicate full type names");
            }
            const auto [existing, inserted] = available.emplace(
                descriptor->identity().canonical_digest(), descriptor);
            if (!inserted &&
                existing->second->canonical_schema() !=
                    descriptor->canonical_schema()) {
                return Mismatch("canonical digest collision has different text");
            }
        }

        for (const SchemaHandle& descriptor : trusted) {
            DependencyMap declared;
            for (const DependencyDescriptor& dependency :
                 descriptor->dependencies()) {
                if (!declared.emplace(std::string(dependency.full_name()),
                                      dependency.digest())
                         .second) {
                    return Invalid("descriptor dependency closure is duplicated");
                }
                const auto resolved = available.find(dependency.digest());
                if (resolved == available.end() ||
                    resolved->second->aggregate().full_name() !=
                        dependency.full_name()) {
                    return Mismatch(
                        "descriptor dependency digest/name cannot be resolved");
                }
            }

            std::set<std::string, std::less<>> direct_names;
            for (const FieldDescriptor& field :
                 descriptor->aggregate().fields()) {
                CollectUserTypes(field.type(), direct_names);
            }
            DependencyMap expected;
            for (const std::string& direct_name : direct_names) {
                const auto direct = declared.find(direct_name);
                if (direct == declared.end()) {
                    return Mismatch(
                        "descriptor dependency closure omits a direct type");
                }
                const auto resolved = available.find(direct->second);
                if (resolved == available.end() ||
                    resolved->second->aggregate().full_name() != direct_name) {
                    return Mismatch("descriptor direct dependency is invalid");
                }
                expected.emplace(direct_name, direct->second);
                for (const DependencyDescriptor& transitive :
                     resolved->second->dependencies()) {
                    const auto [it, inserted] = expected.emplace(
                        std::string(transitive.full_name()),
                        transitive.digest());
                    if (!inserted && it->second != transitive.digest()) {
                        return Mismatch(
                            "descriptor dependency closure has conflicting digests");
                    }
                }
            }
            if (expected != declared) {
                return Mismatch(
                    "descriptor dependency list is not the exact transitive closure");
            }
        }

        std::vector<SchemaHandle> compatibility_closure;
        compatibility_closure.reserve(available.size());
        for (const auto& [digest, descriptor] : available) {
            static_cast<void>(digest);
            compatibility_closure.push_back(descriptor);
        }

        for (const SchemaHandle& descriptor : trusted) {
            const CanonicalDigest digest =
                descriptor->identity().canonical_digest();
            const TypeVersionKey key{
                std::string(descriptor->aggregate().full_name()),
                descriptor->identity().schema_version()};
            const auto same_version = by_type_version_.find(key);
            if (same_version != by_type_version_.end() &&
                same_version->second != digest) {
                return Status::Error(
                    StatusCode::kAlreadyExists,
                    "type and schema_version already map to a different digest");
            }

            std::optional<std::pair<uint32_t, CanonicalDigest>> latest;
            auto version = by_type_version_.lower_bound(
                TypeVersionKey{key.full_name, 0});
            while (version != by_type_version_.end() &&
                   version->first.full_name == key.full_name) {
                latest = std::pair{version->first.schema_version,
                                   version->second};
                ++version;
            }
            if (!latest.has_value() || latest->second == digest) continue;
            if (key.schema_version <= latest->first) {
                return Invalid(
                    "compatible schema content changes require a strictly "
                    "increasing schema_version");
            }
            const auto previous = available.find(latest->second);
            if (previous == available.end()) {
                return Status::Error(StatusCode::kInternal,
                                     "registry version index is inconsistent");
            }
            auto compatibility = CompatibilityChecker::Check(
                *previous->second, *descriptor, compatibility_closure);
            if (!compatibility.ok()) return compatibility.status();
            if (IsBreaking(*compatibility) &&
                VersionMajor(key.schema_version) <=
                    VersionMajor(latest->first)) {
                return Invalid(
                    "breaking schema evolution requires a strictly increasing "
                    "major version");
            }
        }

        auto next_by_digest = by_digest_;
        auto next_by_type_version = by_type_version_;
        auto next_by_short_id = by_short_id_;
        std::vector<SchemaHandle> handles;
        handles.reserve(trusted.size());
        for (const SchemaHandle& descriptor : trusted) {
            const CanonicalDigest digest =
                descriptor->identity().canonical_digest();
            const TypeVersionKey key{
                std::string(descriptor->aggregate().full_name()),
                descriptor->identity().schema_version()};
            const auto existing = next_by_digest.find(digest);
            SchemaHandle published;
            if (existing != next_by_digest.end()) {
                published = existing->second;
            } else {
                next_by_digest.emplace(digest, descriptor);
                next_by_short_id[ShortIdIndex(digest)].push_back(digest);
                published = descriptor;
            }
            next_by_type_version.emplace(key, digest);
            handles.push_back(std::move(published));
        }

        by_digest_.swap(next_by_digest);
        by_type_version_.swap(next_by_type_version);
        by_short_id_.swap(next_by_short_id);
        return handles;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaHandle> SchemaRegistry::Find(
    const CanonicalDigest& digest) const noexcept {
    try {
        std::shared_lock lock(mutex_);
        const auto it = by_digest_.find(digest);
        if (it == by_digest_.end()) {
            return Status::Error(StatusCode::kNotFound, "schema digest not found");
        }
        return it->second;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaHandle> SchemaRegistry::Find(
    const SchemaIdentity& identity) const noexcept {
    try {
        if (identity.short_id() != DigestShortId(identity.canonical_digest())) {
            return Status::Error(
                StatusCode::kSchemaMismatch,
                "schema identity short ID does not match digest");
        }
        auto found = Find(identity.canonical_digest());
        if (!found.ok()) return found.status();
        {
            std::shared_lock lock(mutex_);
            const TypeVersionKey alias{
                std::string((*found)->aggregate().full_name()),
                identity.schema_version()};
            const auto version = by_type_version_.find(alias);
            if (version == by_type_version_.end() ||
                version->second != identity.canonical_digest()) {
                return Status::Error(StatusCode::kSchemaMismatch,
                                     "schema version alias does not match digest");
            }
        }
        if ((*found)->identity().layout_version() != identity.layout_version()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "schema layout version mismatch");
        }
        return found;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaHandle> SchemaRegistry::FindByShortId(
    uint64_t short_id) const noexcept {
    try {
        std::shared_lock lock(mutex_);
        const auto bucket = by_short_id_.find(short_id);
        if (bucket == by_short_id_.end() || bucket->second.empty()) {
            return Status::Error(StatusCode::kNotFound,
                                 "schema short ID not found");
        }
        if (bucket->second.size() != 1) {
            return Status::Error(
                StatusCode::kSchemaMismatch,
                "schema short ID collision requires a full digest");
        }
        const auto descriptor = by_digest_.find(bucket->second.front());
        if (descriptor == by_digest_.end()) {
            return Status::Error(StatusCode::kInternal,
                                 "registry short ID index is inconsistent");
        }
        return descriptor->second;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<Compatibility> SchemaRegistry::CheckCompatibility(
    const CanonicalDigest& from_digest,
    const CanonicalDigest& to_digest) const noexcept {
    try {
        SchemaHandle from;
        SchemaHandle to;
        std::vector<SchemaHandle> closure;
        {
            std::shared_lock lock(mutex_);
            const auto from_it = by_digest_.find(from_digest);
            const auto to_it = by_digest_.find(to_digest);
            if (from_it == by_digest_.end() || to_it == by_digest_.end()) {
                return Status::Error(StatusCode::kNotFound,
                                     "compatibility schema digest not found");
            }
            from = from_it->second;
            to = to_it->second;
            closure.reserve(by_digest_.size());
            for (const auto& [digest, descriptor] : by_digest_) {
                static_cast<void>(digest);
                closure.push_back(descriptor);
            }
        }
        return CompatibilityChecker::Check(*from, *to, closure);
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

size_t SchemaRegistry::size() const noexcept {
    try {
        std::shared_lock lock(mutex_);
        return by_digest_.size();
    } catch (...) {
        return 0;
    }
}

}  // namespace mino::schema
