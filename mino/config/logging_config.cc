// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/config/logging_config.h"

#include <chrono>
#include <filesystem>
#include <optional>
#include <utility>

#include "mino/common/logging/logging.h"
#include "mino/common/logging/spdlog_backend.h"

namespace mino {

Status InitializeLogging(const LoggingConfig& config) {
    logging::internal::LoggingOptions options;
    options.level = config.level;
    options.pattern = config.pattern;
    options.console_enabled = config.console.enabled;
    if (config.file.enabled) {
        options.file_path = std::filesystem::path(config.file.path);
    } else {
        options.file_path = std::nullopt;
    }
    options.rotation_max_size = config.rotation.max_size_bytes;
    options.rotation_max_files = config.rotation.max_files;
    options.async = config.async.enabled;
    options.queue_size = config.async.queue_size;
    options.thread_count = config.async.thread_count;
    options.flush_on = config.flush_on;
    options.flush_interval =
        std::chrono::milliseconds(config.flush_interval_ms);

    auto logger = logging::internal::CreateSpdlogLogger(options);
    if (!logger.ok()) {
        return logger.status();
    }
    logging::SetLogger(std::move(*logger));
    return Status::Ok();
}

}  // namespace mino
