// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#ifndef MINO_COMMON_LOGGING_SPDLOG_BACKEND_H_
#define MINO_COMMON_LOGGING_SPDLOG_BACKEND_H_

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>

#include "mino/common/logging/logger.h"
#include "mino/common/result.h"

namespace mino::logging::internal {

struct LoggingOptions {
    LogLevel level = LogLevel::kInfo;
    std::string pattern = "[%Y-%m-%d %H:%M:%S.%e] [%l] [%s:%#] %v";
    bool console_enabled = true;
    std::optional<std::filesystem::path> file_path;
    std::size_t rotation_max_size = 10 * 1024 * 1024;
    std::size_t rotation_max_files = 3;
    bool async = false;
    std::size_t queue_size = 8192;
    std::size_t thread_count = 1;
    LogLevel flush_on = LogLevel::kError;
    std::chrono::milliseconds flush_interval{1000};
};

Result<std::shared_ptr<Logger>> CreateSpdlogLogger(
    const LoggingOptions& options);

}  // namespace mino::logging::internal

#endif  // MINO_COMMON_LOGGING_SPDLOG_BACKEND_H_
