// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_VALIDATION_COMMON_TEMPORARY_DIRECTORY_H_
#define BENCHMARKS_VALIDATION_COMMON_TEMPORARY_DIRECTORY_H_

#include <filesystem>
#include <optional>

namespace mino::benchmarks::validation {

class OwnedTemporaryDirectory final {
public:
    explicit OwnedTemporaryDirectory(
        const std::optional<std::filesystem::path>& requested_base);
    ~OwnedTemporaryDirectory();

    OwnedTemporaryDirectory(const OwnedTemporaryDirectory&) = delete;
    OwnedTemporaryDirectory& operator=(const OwnedTemporaryDirectory&) = delete;
    OwnedTemporaryDirectory(OwnedTemporaryDirectory&&) = delete;
    OwnedTemporaryDirectory& operator=(OwnedTemporaryDirectory&&) = delete;

    const std::filesystem::path& path() const noexcept { return path_; }
    const std::filesystem::path& base() const noexcept { return base_; }

private:
    std::filesystem::path base_;
    std::filesystem::path path_;
};

}  // namespace mino::benchmarks::validation

#endif  // BENCHMARKS_VALIDATION_COMMON_TEMPORARY_DIRECTORY_H_
