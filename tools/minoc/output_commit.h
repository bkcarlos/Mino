// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef TOOLS_MINOC_OUTPUT_COMMIT_H_
#define TOOLS_MINOC_OUTPUT_COMMIT_H_

#include <cstddef>
#include <filesystem>
#include <optional>
#include <span>
#include <string_view>

#include "mino/common/status.h"

namespace mino::tools::minoc {

struct OutputFile {
    std::filesystem::path path;
    std::string_view bytes;
};

struct CommitOptions {
    // One-based rename index. Production leaves this unset; tests use it to
    // verify rollback after the second/third final rename.
    std::optional<size_t> fail_before_rename;
};

// Resolves parent symlinks, rejects a symlink at the output leaf, and rejects
// physical aliases among outputs or with protected input/import files.
Status ValidateOutputPaths(
    std::span<const std::filesystem::path> outputs,
    std::span<const std::filesystem::path> protected_paths = {}) noexcept;

// Transactionally replaces all outputs as one best-effort filesystem
// transaction. Temporary and backup files are unique and remain in the output
// directory, so each rename is atomic. A failed final rename rolls every output
// back to its pre-call state.
Status CommitOutputFiles(
    std::span<const OutputFile> files,
    const CommitOptions& options = {}) noexcept;

}  // namespace mino::tools::minoc

#endif  // TOOLS_MINOC_OUTPUT_COMMIT_H_
