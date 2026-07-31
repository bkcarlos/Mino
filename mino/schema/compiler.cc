// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/schema/compiler.h"

#include <limits>
#include <map>
#include <new>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"
#include "mino/schema/parser.h"
#include "mino/schema/validator.h"

namespace mino::schema {
namespace {

void CollectUserTypes(const TypeDescriptor& type,
                      std::set<std::string, std::less<>>& names) {
    if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
        names.emplace(type.name());
    } else if (type.kind() == TypeDescriptor::Kind::kVector) {
        CollectUserTypes(*type.element_type(), names);
    }
}

class DescriptorBuilder {
public:
    DescriptorBuilder(
        const ValidatedSchema& schema,
        const std::vector<std::shared_ptr<const SchemaDescriptor>>& imported,
        uint32_t layout_version, size_t max_canonical_bytes)
        : schema_(schema),
          imported_(imported),
          layout_version_(layout_version),
          max_canonical_bytes_(max_canonical_bytes) {
        for (const AggregateDescriptor& aggregate : schema_.aggregates()) {
            local_.emplace(std::string(aggregate.full_name()), &aggregate);
        }
        for (const auto& descriptor : imported_) {
            external_.emplace(std::string(descriptor->aggregate().full_name()),
                              descriptor.get());
        }
    }

    Result<CompiledSchema> Build() {
        for (const auto& [name, aggregate] : local_) {
            static_cast<void>(aggregate);
            auto descriptor = BuildType(name);
            if (!descriptor.ok()) return descriptor.status();
        }
        std::vector<std::shared_ptr<const SchemaDescriptor>> types;
        types.reserve(built_.size());
        for (auto& [name, descriptor] : built_) {
            static_cast<void>(name);
            types.push_back(std::move(descriptor));
        }
        return CompiledSchema(std::move(types));
    }

private:
    Result<std::shared_ptr<const SchemaDescriptor>> BuildType(
        const std::string& name) {
        const auto existing = built_.find(name);
        if (existing != built_.end()) return existing->second;
        if (!building_.insert(name).second) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "recursive type dependency involving '" + name +
                                     "'");
        }

        const AggregateDescriptor& aggregate = *local_.at(name);
        std::set<std::string, std::less<>> direct_dependencies;
        for (const FieldDescriptor& field : aggregate.fields()) {
            CollectUserTypes(field.type(), direct_dependencies);
        }

        std::map<std::string, CanonicalDigest, std::less<>> closure;
        for (const std::string& dependency_name : direct_dependencies) {
            const auto local_dependency = local_.find(dependency_name);
            if (local_dependency != local_.end()) {
                auto descriptor = BuildType(dependency_name);
                if (!descriptor.ok()) return descriptor.status();
                AddDescriptorToClosure(**descriptor, closure);
                continue;
            }
            const auto external_dependency = external_.find(dependency_name);
            if (external_dependency == external_.end()) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "unavailable dependency '" +
                                         dependency_name + "'");
            }
            AddDescriptorToClosure(*external_dependency->second, closure);
        }

        std::vector<DependencyDescriptor> dependencies;
        dependencies.reserve(closure.size());
        for (const auto& [dependency_name, digest] : closure) {
            dependencies.emplace_back(dependency_name, digest);
        }
        CanonicalOptions canonical_options;
        canonical_options.max_output_bytes = max_canonical_bytes_;
        auto canonical = Canonicalizer::Canonicalize(
            aggregate, dependencies, canonical_options);
        if (!canonical.ok()) return canonical.status();

        SchemaIdentity identity(canonical->short_id(), canonical->digest(),
                                schema_.schema_version(), layout_version_);
        auto descriptor = std::make_shared<const SchemaDescriptor>(
            aggregate, std::move(identity), std::string(canonical->text()),
            std::move(dependencies));
        built_.emplace(name, descriptor);
        building_.erase(name);
        return descriptor;
    }

    static void AddDescriptorToClosure(
        const SchemaDescriptor& descriptor,
        std::map<std::string, CanonicalDigest, std::less<>>& closure) {
        closure.insert_or_assign(std::string(descriptor.aggregate().full_name()),
                                 descriptor.identity().canonical_digest());
        for (const DependencyDescriptor& dependency :
             descriptor.dependencies()) {
            closure.insert_or_assign(std::string(dependency.full_name()),
                                     dependency.digest());
        }
    }

    const ValidatedSchema& schema_;
    const std::vector<std::shared_ptr<const SchemaDescriptor>>& imported_;
    uint32_t layout_version_;
    size_t max_canonical_bytes_;
    std::map<std::string, const AggregateDescriptor*, std::less<>> local_;
    std::map<std::string, const SchemaDescriptor*, std::less<>> external_;
    std::map<std::string, std::shared_ptr<const SchemaDescriptor>, std::less<>>
        built_;
    std::set<std::string, std::less<>> building_;
};

}  // namespace

Result<CompiledSchema> SchemaCompiler::Compile(
    std::string_view idl, const CompileOptions& options) noexcept {
    try {
        ParserOptions parser_options;
        parser_options.lexer.max_input_bytes = options.max_input_bytes;
        parser_options.lexer.max_tokens = options.max_tokens;
        parser_options.lexer.max_token_bytes = options.max_token_bytes;
        if (options.dependencies.size() >
                std::numeric_limits<size_t>::max() - 16 ||
            options.max_types > std::numeric_limits<size_t>::max() -
                                    options.dependencies.size() - 16) {
            parser_options.max_declarations =
                std::numeric_limits<size_t>::max();
        } else {
            parser_options.max_declarations =
                options.max_types + options.dependencies.size() + 16;
        }
        parser_options.max_fields = options.max_fields;
        parser_options.max_reserved_ranges = options.max_reserved_ranges;
        parser_options.max_annotations = options.max_annotations;
        parser_options.max_nesting_depth = options.max_nesting_depth;
        auto ast = Parser::Parse(idl, parser_options);
        if (!ast.ok()) return ast.status();

        ValidatorOptions validator_options;
        validator_options.max_types = options.max_types;
        validator_options.max_fields = options.max_fields;
        validator_options.max_reserved_ranges = options.max_reserved_ranges;
        validator_options.max_type_nesting_depth = options.max_nesting_depth;
        validator_options.max_name_bytes = options.max_name_bytes;
        validator_options.max_total_capacity = options.max_total_capacity;
        validator_options.unknown_fields = options.unknown_fields;
        validator_options.allow_implicit_schema_version =
            options.allow_implicit_schema_version;
        auto validated = SemanticValidator::Validate(
            *ast, options.dependencies, validator_options);
        if (!validated.ok()) return validated.status();

        return DescriptorBuilder(*validated, options.dependencies,
                                 options.layout_version,
                                 options.max_canonical_bytes)
            .Build();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::schema
