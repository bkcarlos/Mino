// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_OBJECT_GRAPH_WALKER_H_
#define MINO_SCHEMA_OBJECT_GRAPH_WALKER_H_

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

#include "mino/abi/shm_handle.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/layout.h"
#include "mino/schema/unknown_field_set.h"
#include "mino/shm/allocator/central_slab.h"

namespace mino::schema {

struct ObjectGraphLimits {
    size_t max_depth = 32;
    size_t max_allocations = 1u << 20;
};

struct ObjectGraphWalkOptions {
    ObjectGraphLimits limits;
    UnknownFieldLimits unknown_fields;
    // Builders use this only before publication. Reader-facing validation keeps
    // the default and accepts published slabs exclusively.
    bool allow_building = false;
};

// Descriptor-driven traversal of a dynamic shared-memory object graph. Handles
// are returned in deterministic parent-before-child, field-id, vector-index
// order. Every allocation is visited once; cycles and shared ownership are
// rejected rather than silently deduplicated.
class ObjectGraphWalker {
public:
    static Status Validate(
        const SchemaDescriptor& descriptor, const LayoutPlan& layout,
        ShmHandle root, const CentralSlabAllocator& allocator,
        std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors = {},
        const ObjectGraphWalkOptions& options = {}) noexcept;

    static Result<std::vector<ShmHandle>> Collect(
        const SchemaDescriptor& descriptor, const LayoutPlan& layout,
        ShmHandle root, const CentralSlabAllocator& allocator,
        std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors = {},
        const ObjectGraphWalkOptions& options = {}) noexcept;

    // Restricted recovery helper for unpublished BUILDING graphs. Published
    // graphs are rejected so callers cannot bypass the shared Pin table.
    static Status Reclaim(
        const SchemaDescriptor& descriptor, const LayoutPlan& layout,
        ShmHandle root, CentralSlabAllocator& allocator,
        std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors = {},
        const ObjectGraphLimits& limits = {}) noexcept;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_OBJECT_GRAPH_WALKER_H_
