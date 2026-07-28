// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/common/logging/spdlog_backend.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include "gtest/gtest.h"
#include "mino/common/status.h"

namespace mino::logging::internal {
namespace {

std::filesystem::path UniqueLogPath() {
    const auto value = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           ("mino_logging_" + std::to_string(value) + ".log");
}

TEST(SpdlogBackendTest, WritesEnabledLevelToFileAndFlushes) {
    const std::filesystem::path path = UniqueLogPath();
    LoggingOptions options;
    options.console_enabled = false;
    options.file_path = path;
    options.level = LogLevel::kInfo;
    options.pattern = "%v";

    auto result = CreateSpdlogLogger(options);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    auto logger = result.value();
    EXPECT_FALSE(logger->ShouldLog(LogLevel::kDebug));
    EXPECT_TRUE(logger->ShouldLog(LogLevel::kInfo));

    logger->Log(LogRecord{.level = LogLevel::kDebug,
                          .source_location = {},
                          .message = "filtered"});
    logger->Log(LogRecord{.level = LogLevel::kInfo,
                          .source_location = {},
                          .message = "written"});
    logger->Flush();

    std::ifstream input(path);
    const std::string contents((std::istreambuf_iterator<char>(input)),
                               std::istreambuf_iterator<char>());
    EXPECT_EQ(contents.find("filtered"), std::string::npos);
    EXPECT_NE(contents.find("written"), std::string::npos);
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
}

TEST(SpdlogBackendTest, RejectsConfigurationWithoutSink) {
    LoggingOptions options;
    options.console_enabled = false;

    auto result = CreateSpdlogLogger(options);

    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino::logging::internal
