// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/common/logging/logging.h"

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace mino::logging {
namespace {

class FakeLogger final : public Logger {
public:
    explicit FakeLogger(LogLevel minimum) : minimum_(minimum) {}

    bool ShouldLog(LogLevel level) const noexcept override {
        return level >= minimum_ && level != LogLevel::kOff;
    }

    void Log(const LogRecord& record) noexcept override {
        std::lock_guard lock(mutex_);
        records_.push_back(OwnedRecord{record.level,
                                       std::string(record.source_location.file_name),
                                       std::string(record.source_location.function_name),
                                       record.source_location.line,
                                       std::string(record.message)});
    }

    void Flush() noexcept override { flushed_ = true; }

    struct OwnedRecord {
        LogLevel level;
        std::string file;
        std::string function;
        uint_least32_t line;
        std::string message;
    };

    std::vector<OwnedRecord> records() const {
        std::lock_guard lock(mutex_);
        return records_;
    }

    bool flushed() const noexcept { return flushed_; }

private:
    LogLevel minimum_;
    mutable std::mutex mutex_;
    std::vector<OwnedRecord> records_;
    bool flushed_ = false;
};

class LoggingTest : public testing::Test {
protected:
    void TearDown() override { SetLogger(nullptr); }
};

TEST_F(LoggingTest, ReplacesGlobalLoggerAndFlushes) {
    auto fake = std::make_shared<FakeLogger>(LogLevel::kTrace);
    SetLogger(fake);

    EXPECT_EQ(GetLogger(), fake);
    MINO_LOG_INFO("installed");
    Flush();

    ASSERT_EQ(fake->records().size(), 1);
    EXPECT_EQ(fake->records()[0].message, "installed");
    EXPECT_TRUE(fake->flushed());
}

TEST_F(LoggingTest, FiltersLevelsAndDoesNotEvaluateFilteredMessage) {
    auto fake = std::make_shared<FakeLogger>(LogLevel::kWarn);
    SetLogger(fake);
    int evaluations = 0;

    MINO_LOG_DEBUG((++evaluations, std::string("expensive")));
    MINO_LOG_ERROR("kept");

    EXPECT_EQ(evaluations, 0);
    ASSERT_EQ(fake->records().size(), 1);
    EXPECT_EQ(fake->records()[0].level, LogLevel::kError);
}

TEST_F(LoggingTest, CapturesCallSiteSourceLocation) {
    auto fake = std::make_shared<FakeLogger>(LogLevel::kTrace);
    SetLogger(fake);
    const uint_least32_t expected_line = __LINE__ + 1;
    MINO_LOG_INFO("located");

    ASSERT_EQ(fake->records().size(), 1);
    EXPECT_NE(fake->records()[0].file.find("logging_test.cc"), std::string::npos);
    EXPECT_FALSE(fake->records()[0].function.empty());
    EXPECT_EQ(fake->records()[0].line, expected_line);
}

TEST_F(LoggingTest, DefaultFallbackDoesNotCrash) {
    SetLogger(nullptr);
    EXPECT_NE(GetLogger(), nullptr);
    MINO_LOG_INFO("fallback smoke test");
    Flush();
}

}  // namespace
}  // namespace mino::logging
