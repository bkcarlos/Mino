// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#ifndef MINO_SCHEMA_COMPATIBILITY_H_
#define MINO_SCHEMA_COMPATIBILITY_H_

#include <memory>
#include <span>

#include "mino/common/result.h"
#include "mino/schema/descriptor.h"

namespace mino::schema {

enum class Compatibility {
    kIdentical,
    kWireCompatible,
    kReadCompatible,
    kWriteCompatible,
    kRequiresTranslation,
    kIncompatible,
};

class CompatibilityChecker {
public:
    // Checks evolution from -> to. Constraint tightening is write-compatible;
    // widening is read-compatible, matching detailed design section 13.3.2.
    static Result<Compatibility> Check(
        const SchemaDescriptor& from,
        const SchemaDescriptor& to) noexcept;

    // descriptor_closure supplies descriptors addressable by canonical digest.
    // It is required to classify changed nested dependency digests recursively.
    static Result<Compatibility> Check(
        const SchemaDescriptor& from, const SchemaDescriptor& to,
        std::span<const std::shared_ptr<const SchemaDescriptor>>
            descriptor_closure) noexcept;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_COMPATIBILITY_H_
