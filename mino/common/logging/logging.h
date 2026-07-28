// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#ifndef MINO_COMMON_LOGGING_LOGGING_H_
#define MINO_COMMON_LOGGING_LOGGING_H_

#include <memory>
#include <source_location>
#include <string_view>

#include "mino/common/logging/logger.h"

namespace mino::logging {

// Installs logger globally. Passing nullptr restores the stderr fallback.
void SetLogger(std::shared_ptr<Logger> logger) noexcept;

// Always returns a non-null logger. Before initialization this is a stderr
// fallback, so startup and error-path messages are never silently discarded.
std::shared_ptr<Logger> GetLogger() noexcept;

bool ShouldLog(LogLevel level) noexcept;

void Log(LogLevel level, const std::source_location& location,
         std::string_view message) noexcept;

void Flush() noexcept;

}  // namespace mino::logging

#define MINO_INTERNAL_LOG_(level, message)                                  \
    do {                                                                    \
        if (::mino::logging::ShouldLog(level)) {                            \
            ::mino::logging::Log(level, std::source_location::current(),    \
                                 (message));                                \
        }                                                                   \
    } while (false)

#define MINO_LOG_TRACE(message) \
    MINO_INTERNAL_LOG_(::mino::logging::LogLevel::kTrace, message)
#define MINO_LOG_DEBUG(message) \
    MINO_INTERNAL_LOG_(::mino::logging::LogLevel::kDebug, message)
#define MINO_LOG_INFO(message) \
    MINO_INTERNAL_LOG_(::mino::logging::LogLevel::kInfo, message)
#define MINO_LOG_WARN(message) \
    MINO_INTERNAL_LOG_(::mino::logging::LogLevel::kWarn, message)
#define MINO_LOG_ERROR(message) \
    MINO_INTERNAL_LOG_(::mino::logging::LogLevel::kError, message)
#define MINO_LOG_CRITICAL(message) \
    MINO_INTERNAL_LOG_(::mino::logging::LogLevel::kCritical, message)

#endif  // MINO_COMMON_LOGGING_LOGGING_H_
