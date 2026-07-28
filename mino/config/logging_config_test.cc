// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/config/toml_config.h"

#include <cstdlib>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

#include "mino/common/logging/logging.h"
#include "mino/common/status.h"
#include "mino/config/logging_config.h"

namespace mino {
namespace {

TEST(LoggingConfigTest, UsesDefaults) {
    auto result = ParseLoggingConfigToml("");
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    const LoggingConfig& config = *result;
    EXPECT_EQ(config.level, LogLevel::kInfo);
    EXPECT_TRUE(config.console.enabled);
    EXPECT_FALSE(config.file.enabled);
    EXPECT_FALSE(config.pattern.empty());
    EXPECT_NE(config.pattern.find("%Y"), std::string::npos);
    EXPECT_NE(config.pattern.find("%l"), std::string::npos);
    EXPECT_NE(config.pattern.find("%t"), std::string::npos);
    EXPECT_NE(config.pattern.find("%s"), std::string::npos);
    EXPECT_NE(config.pattern.find("%v"), std::string::npos);
    EXPECT_EQ(config.rotation.max_size_bytes, 10u * 1024u * 1024u);
    EXPECT_EQ(config.rotation.max_files, 5u);
    EXPECT_FALSE(config.async.enabled);
    EXPECT_EQ(config.async.queue_size, 8192u);
    EXPECT_EQ(config.async.thread_count, 1u);
    EXPECT_EQ(config.flush_on, LogLevel::kError);
    EXPECT_EQ(config.flush_interval_ms, 1000u);
}

TEST(LoggingConfigTest, ParsesCompleteConfiguration) {
    constexpr std::string_view kToml = R"(
[logging]
level = "debug"
pattern = "[%l] %v"
flush_on = "critical"
flush_interval_ms = 250

[logging.console]
enabled = false

[logging.file]
enabled = true
path = "/tmp/mino.log"

[logging.rotation]
max_size_bytes = 4096
max_files = 3

[logging.async]
enabled = true
queue_size = 16384
thread_count = 2
)";
    auto result = ParseLoggingConfigToml(kToml);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_EQ(result->level, LogLevel::kDebug);
    EXPECT_EQ(result->pattern, "[%l] %v");
    EXPECT_FALSE(result->console.enabled);
    EXPECT_TRUE(result->file.enabled);
    EXPECT_EQ(result->file.path, "/tmp/mino.log");
    EXPECT_EQ(result->rotation.max_size_bytes, 4096u);
    EXPECT_EQ(result->rotation.max_files, 3u);
    EXPECT_TRUE(result->async.enabled);
    EXPECT_EQ(result->async.queue_size, 16384u);
    EXPECT_EQ(result->async.thread_count, 2u);
    EXPECT_EQ(result->flush_on, LogLevel::kCritical);
    EXPECT_EQ(result->flush_interval_ms, 250u);
}

TEST(LoggingConfigTest, RejectsInvalidLevel) {
    auto result = ParseLoggingConfigToml("[logging]\nlevel = \"verbose\"\n");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST(LoggingConfigTest, RejectsUnknownKey) {
    auto result = ParseLoggingConfigToml(
        "[logging.async]\nqueue_sise = 1024\n");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
    EXPECT_NE(result.status().message().find("queue_sise"),
              std::string_view::npos);
}

TEST(LoggingConfigTest, RejectsConfigurationWithNoSink) {
    auto result = ParseLoggingConfigToml(R"(
[logging.console]
enabled = false
[logging.file]
enabled = false
)");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST(LoggingConfigTest, RejectsInvalidPositiveValues) {
    const char* invalid_configs[] = {
        "[logging.rotation]\nmax_size_bytes = 0\n",
        "[logging.rotation]\nmax_files = -1\n",
        "[logging.async]\nqueue_size = 0\n",
        "[logging.async]\nthread_count = 0\n",
        "[logging]\nflush_interval_ms = 0\n",
    };
    for (const char* text : invalid_configs) {
        auto result = ParseLoggingConfigToml(text);
        EXPECT_FALSE(result.ok()) << text;
        if (!result.ok()) {
            EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
        }
    }
}

TEST(LoggingConfigTest, RejectsEmptyEnabledFilePath) {
    auto result = ParseLoggingConfigToml(
        "[logging.file]\nenabled = true\npath = \"\"\n");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST(LoggingConfigTest, InitializesDefaultBackend) {
    LoggingConfig config;
    config.flush_interval_ms = 10;
    Status status = InitializeLogging(config);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(logging::ShouldLog(LogLevel::kInfo));
    MINO_LOG_INFO("configured logger is active");
    logging::Flush();
    logging::SetLogger(nullptr);
}

TEST(LoggingConfigTest, LoadsConfigurationFromFile) {
    const char* test_tmpdir = std::getenv("TEST_TMPDIR");
    ASSERT_NE(test_tmpdir, nullptr);
    const std::string path = std::string(test_tmpdir) + "/logging.toml";
    {
        std::ofstream output(path);
        ASSERT_TRUE(output.is_open());
        output << "[logging]\nlevel = \"warn\"\n";
    }

    auto result = LoadLoggingConfigFromTomlFile(path);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    EXPECT_EQ(result->level, LogLevel::kWarn);
}

}  // namespace
}  // namespace mino
