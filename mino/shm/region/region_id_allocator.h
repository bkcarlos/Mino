// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#ifndef MINO_SHM_REGION_REGION_ID_ALLOCATOR_H_
#define MINO_SHM_REGION_REGION_ID_ALLOCATOR_H_

#include <cstdint>
#include <string>

#include "mino/common/result.h"

namespace mino::region_internal {

// Options for the deployment-local durable Region ID high-water mark.
struct RegionIdAllocatorOptions {
    // Empty selects MINO_REGION_ID_HWM_PATH, then
    // $XDG_STATE_HOME/mino/region_id_hwm, then
    // $HOME/.local/state/mino/region_id_hwm, then a UID-isolated
    // /var/tmp/mino-<uid>/region_id_hwm fallback. Tests may inject an explicit path.
    std::string hwm_path;

    // When a new durable file is initialized, migrate this legacy POSIX SHM HWM
    // while holding its advisory lock. Empty disables legacy migration.
    std::string legacy_shm_name = "/mino_region_id_hwm";

    // Used only when neither a durable value nor a legacy value exists.
    uint64_t first_id = 1;
};

// Allocates a non-zero Region ID. The following high-water mark is written and
// fdatasync/fsync-completed before the ID is returned. Creation additionally
// fsyncs the containing state directory, so process or host restart cannot
// reuse an ID that was returned successfully. Explicit deletion or loss of the
// state file is a deployment recovery event and must not silently reinitialize
// an identity domain.
Result<uint32_t> AllocateRegionId(
    const RegionIdAllocatorOptions& options = {});

}  // namespace mino::region_internal

#endif  // MINO_SHM_REGION_REGION_ID_ALLOCATOR_H_
