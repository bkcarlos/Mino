// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef TOOLS_MINO_STORAGE_COMMANDS_H_
#define TOOLS_MINO_STORAGE_COMMANDS_H_

#include <filesystem>
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

// D4 installs a Bus-backed implementation at process assembly time. The CLI
// owns neither the launcher nor the resulting long-running service.
class RecorderServiceLauncher {
public:
    virtual ~RecorderServiceLauncher() = default;
    virtual Status Run(const std::filesystem::path& session_root) noexcept = 0;
};

struct StorageCommandServices {
    storage::ReplayPublisherAdapter* replay_adapter = nullptr;
    RecorderServiceLauncher* recorder_service_launcher = nullptr;
};

// Runs a storage command without depending on global stdio or argv. The first
// argument is one of inspect, verify, repair, replay, or record. Production
// adapters are explicitly installed through services; the CLI fabricates none.
int RunStorageCommand(
    const std::vector<std::string>& args, std::ostream& out, std::ostream& err,
    const StorageCommandServices& services = {});

// Compatibility overload for existing embedders; new assembly code should use
// the named StorageCommandServices object.
int RunStorageCommand(
    const std::vector<std::string>& args, std::ostream& out, std::ostream& err,
    storage::ReplayPublisherAdapter* replay_adapter);

}  // namespace mino::tools

#endif  // TOOLS_MINO_STORAGE_COMMANDS_H_
