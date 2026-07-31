// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#ifndef MINO_SCHEMA_VALIDATOR_H_
#define MINO_SCHEMA_VALIDATOR_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "mino/common/result.h"
#include "mino/schema/ast.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/unknown_field_set.h"

namespace mino::schema {

struct ValidatorOptions {
    size_t max_types = 1024;
    size_t max_fields = 1024;
    size_t max_reserved_ranges = 1024;
    size_t max_type_nesting_depth = 32;
    size_t max_name_bytes = 1024;
    uint64_t max_total_capacity = 64u << 20;
    UnknownFieldLimits unknown_fields;
    bool allow_implicit_schema_version = false;
};

class ValidatedSchema {
public:
    ValidatedSchema(std::string package_name, uint32_t schema_version,
                    std::vector<AggregateDescriptor> aggregates);

    std::string_view package_name() const noexcept { return package_name_; }
    uint32_t schema_version() const noexcept { return schema_version_; }
    std::span<const AggregateDescriptor> aggregates() const noexcept {
        return aggregates_;
    }

private:
    std::string package_name_;
    uint32_t schema_version_ = 0;
    std::vector<AggregateDescriptor> aggregates_;
};

class SemanticValidator {
public:
    // imported_types are already-compiled roots available for user-type
    // resolution. Import path strings are intentionally not identities and do
    // not participate in semantic lookup or canonicalization.
    static Result<ValidatedSchema> Validate(
        const SchemaFile& file,
        std::span<const std::shared_ptr<const SchemaDescriptor>> imported_types =
            {},
        const ValidatorOptions& options = {}) noexcept;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_VALIDATOR_H_
