// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#ifndef MINO_SHM_REGION_REGION_NAME_REGISTRY_H_
#define MINO_SHM_REGION_REGION_NAME_REGISTRY_H_

#include <cstdint>
#include <string>
#include <string_view>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino::region_internal {

// Durable host-local map from never-reused Region ID -> POSIX shm name.
// Lives beside the Region ID HWM under the deployment state directory so
// Attach can resolve an ID without the caller supplying the name (design
// doc 6.2). Entries are never silently overwritten: IDs do not reuse, and a
// colliding registration fails closed.
struct RegionNameRegistryOptions {
    // Empty selects MINO_REGION_NAME_REGISTRY_DIR, then the sibling of the
    // Region ID HWM path (same TEST_TMPDIR / XDG_STATE_HOME / HOME /var/tmp
    // resolution), named "region_names". Tests may inject an explicit
    // directory.
    std::string registry_dir;
};

// Publishes region_id -> name. The mapping is fsynced (file + directory)
// before returning OK. region_id must be nonzero; name must be a POSIX shm
// object name ("/token" with no additional '/').
Status RegisterRegionName(uint32_t region_id, std::string_view name,
                          const RegionNameRegistryOptions& options = {});

// Resolves a previously registered POSIX shm name. Returns kNotFound when no
// durable entry exists.
Result<std::string> LookupRegionName(
    uint32_t region_id, const RegionNameRegistryOptions& options = {});

// Best-effort removal used by Create failure rollback. Missing entries are OK.
Status UnregisterRegionName(uint32_t region_id,
                            const RegionNameRegistryOptions& options = {});

}  // namespace mino::region_internal

#endif  // MINO_SHM_REGION_REGION_NAME_REGISTRY_H_
