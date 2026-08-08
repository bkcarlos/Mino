// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/common/temporary_directory.h"

#include <unistd.h>

#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>

#include "benchmarks/validation/common/runtime.h"

namespace mino::benchmarks::validation {

OwnedTemporaryDirectory::OwnedTemporaryDirectory(
    const std::optional<std::filesystem::path>& requested_base) {
    std::error_code error;
    std::filesystem::path base = requested_base.has_value()
        ? *requested_base
        : std::filesystem::temp_directory_path(error);
    if (error) throw std::runtime_error(error.message());
    std::filesystem::create_directories(base, error);
    if (error) throw std::runtime_error(error.message());
    base_ = std::filesystem::canonical(base, error);
    if (error) throw std::runtime_error(error.message());
    const uint64_t nonce =
        static_cast<uint64_t>(Clock::now().time_since_epoch().count());
    path_ = base_ / ("mino-validation-benchmark-" +
                     std::to_string(static_cast<uint64_t>(::getpid())) + "-" +
                     std::to_string(nonce));
    if (!std::filesystem::create_directory(path_, error) || error) {
        throw std::runtime_error("cannot create benchmark temporary directory");
    }
    std::ofstream marker(path_ / ".mino-validation-benchmark-owned");
    marker << "Mino validation benchmark\n";
    marker.close();
    if (!marker) throw std::runtime_error("cannot create ownership marker");
}

OwnedTemporaryDirectory::~OwnedTemporaryDirectory() {
    std::error_code error;
    if (path_.parent_path() != base_ ||
        !path_.filename().string().starts_with("mino-validation-benchmark-")) {
        return;
    }
    std::ifstream marker(path_ / ".mino-validation-benchmark-owned");
    std::string value;
    std::getline(marker, value);
    if (value != "Mino validation benchmark") return;
    std::filesystem::remove_all(path_, error);
}

}  // namespace mino::benchmarks::validation
