// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef TOOLS_MINO_STORAGE_COMMANDS_H_
#define TOOLS_MINO_STORAGE_COMMANDS_H_

#include <iosfwd>
#include <string>
#include <vector>

#include "mino/storage/replay_engine.h"

namespace mino::tools {

// Stable process exit codes used by the storage command family.
inline constexpr int kStorageExitSuccess = 0;
inline constexpr int kStorageExitFailure = 1;
inline constexpr int kStorageExitUsage = 2;
inline constexpr int kStorageExitInvalidData = 3;
inline constexpr int kStorageExitPermissionDenied = 4;

// Runs a storage command without depending on global stdio or argv. The first
// argument is one of inspect, verify, repair, replay, or record. replay_adapter
// is the integration seam for a real Bus-backed publisher; the CLI never
// fabricates one. A null adapter is valid only for replay --validate-only.
int RunStorageCommand(
    const std::vector<std::string>& args, std::ostream& out, std::ostream& err,
    storage::ReplayPublisherAdapter* replay_adapter = nullptr);

}  // namespace mino::tools

#endif  // TOOLS_MINO_STORAGE_COMMANDS_H_
