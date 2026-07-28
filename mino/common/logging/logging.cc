// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/common/logging/logging.h"

#include <atomic>
#include <cstdio>
#include <memory>

namespace mino::logging {
namespace {

const char* LevelName(LogLevel level) noexcept {
    switch (level) {
        case LogLevel::kTrace: return "trace";
        case LogLevel::kDebug: return "debug";
        case LogLevel::kInfo: return "info";
        case LogLevel::kWarn: return "warn";
        case LogLevel::kError: return "error";
        case LogLevel::kCritical: return "critical";
        case LogLevel::kOff: return "off";
    }
    return "unknown";
}

class StderrLogger final : public Logger {
public:
    bool ShouldLog(LogLevel level) const noexcept override {
        return level != LogLevel::kOff;
    }

    void Log(const LogRecord& record) noexcept override {
        if (!ShouldLog(record.level)) return;
        const int file_size = static_cast<int>(record.source_location.file_name.size());
        const int message_size = static_cast<int>(record.message.size());
        std::fprintf(stderr, "[%s] %.*s:%u %.*s\n", LevelName(record.level),
                     file_size, record.source_location.file_name.data(),
                     record.source_location.line, message_size,
                     record.message.data());
    }

    void Flush() noexcept override { std::fflush(stderr); }
};

std::shared_ptr<Logger> FallbackLogger() noexcept {
    static const std::shared_ptr<Logger> logger = std::make_shared<StderrLogger>();
    return logger;
}

std::shared_ptr<Logger>& GlobalLogger() noexcept {
    static std::shared_ptr<Logger> logger = FallbackLogger();
    return logger;
}

}  // namespace

void SetLogger(std::shared_ptr<Logger> logger) noexcept {
    std::atomic_store_explicit(
        &GlobalLogger(), logger ? std::move(logger) : FallbackLogger(),
        std::memory_order_release);
}

std::shared_ptr<Logger> GetLogger() noexcept {
    return std::atomic_load_explicit(&GlobalLogger(), std::memory_order_acquire);
}

bool ShouldLog(LogLevel level) noexcept {
    return GetLogger()->ShouldLog(level);
}

void Log(LogLevel level, const std::source_location& location,
         std::string_view message) noexcept {
    const auto logger = GetLogger();
    if (!logger->ShouldLog(level)) return;
    logger->Log(LogRecord{
        .level = level,
        .source_location = SourceLocation{
            .file_name = location.file_name(),
            .function_name = location.function_name(),
            .line = location.line(),
            .column = location.column(),
        },
        .message = message,
    });
}

void Flush() noexcept { GetLogger()->Flush(); }

}  // namespace mino::logging
