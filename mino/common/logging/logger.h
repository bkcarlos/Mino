// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#ifndef MINO_COMMON_LOGGING_LOGGER_H_
#define MINO_COMMON_LOGGING_LOGGER_H_

#include <cstdint>
#include <string_view>

namespace mino::logging {

enum class LogLevel : uint8_t {
    kTrace = 0,
    kDebug = 1,
    kInfo = 2,
    kWarn = 3,
    kError = 4,
    kCritical = 5,
    kOff = 6,
};

struct SourceLocation {
    std::string_view file_name;
    std::string_view function_name;
    uint_least32_t line = 0;
    uint_least32_t column = 0;
};

struct LogRecord {
    LogLevel level = LogLevel::kInfo;
    SourceLocation source_location;
    std::string_view message;
};

class Logger {
public:
    virtual ~Logger() = default;

    virtual bool ShouldLog(LogLevel level) const noexcept = 0;
    virtual void Log(const LogRecord& record) noexcept = 0;
    virtual void Flush() noexcept = 0;
};

}  // namespace mino::logging

#endif  // MINO_COMMON_LOGGING_LOGGER_H_
