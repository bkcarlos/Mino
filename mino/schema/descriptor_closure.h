// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_DESCRIPTOR_CLOSURE_H_
#define MINO_SCHEMA_DESCRIPTOR_CLOSURE_H_

#include <memory>
#include <span>

#include "mino/common/status.h"
#include "mino/schema/descriptor.h"

namespace mino::schema {

// Validates the exact transitive descriptor closure declared by root. `root` is
// implicit and may also appear in descriptors. Missing, extra, duplicate-name,
// short-id collision, and name/digest dependency mismatches are rejected.
Status ValidateDescriptorClosure(
    const SchemaDescriptor& root,
    std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors = {})
    noexcept;

}  // namespace mino::schema

#endif  // MINO_SCHEMA_DESCRIPTOR_CLOSURE_H_
