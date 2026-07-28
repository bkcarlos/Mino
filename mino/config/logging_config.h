// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_CONFIG_LOGGING_CONFIG_H_
#define MINO_CONFIG_LOGGING_CONFIG_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "mino/common/logging/logger.h"
#include "mino/common/status.h"

namespace mino {

// Keep configuration independent of the concrete backend while sharing the
// stable logging facade's severity type.
using LogLevel = logging::LogLevel;

struct LoggingConfig {
    struct ConsoleConfig {
        bool enabled = true;
    };

    struct FileConfig {
        bool enabled = false;
        std::string path;
    };

    struct RotationConfig {
        uint64_t max_size_bytes = 10 * 1024 * 1024;
        uint32_t max_files = 5;
    };

    struct AsyncConfig {
        bool enabled = false;
        std::size_t queue_size = 8192;
        std::size_t thread_count = 1;
    };

    LogLevel level = LogLevel::kInfo;
    std::string pattern =
        "[%Y-%m-%d %H:%M:%S.%e] [%l] [thread %t] [%s:%#] %v";
    ConsoleConfig console;
    FileConfig file;
    RotationConfig rotation;
    AsyncConfig async;
    LogLevel flush_on = LogLevel::kError;
    uint64_t flush_interval_ms = 1000;
};

// Creates and installs the configured default backend. Application code keeps
// using the stable logging facade and never observes spdlog types.
Status InitializeLogging(const LoggingConfig& config);

}  // namespace mino

#endif  // MINO_CONFIG_LOGGING_CONFIG_H_
