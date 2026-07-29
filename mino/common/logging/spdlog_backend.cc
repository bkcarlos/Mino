// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/common/logging/spdlog_backend.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "spdlog/async.h"
#include "spdlog/async_logger.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

namespace mino::logging::internal {
namespace {

spdlog::level::level_enum ToSpdlogLevel(LogLevel level) noexcept {
    switch (level) {
        case LogLevel::kTrace: return spdlog::level::trace;
        case LogLevel::kDebug: return spdlog::level::debug;
        case LogLevel::kInfo: return spdlog::level::info;
        case LogLevel::kWarn: return spdlog::level::warn;
        case LogLevel::kError: return spdlog::level::err;
        case LogLevel::kCritical: return spdlog::level::critical;
        case LogLevel::kOff: return spdlog::level::off;
    }
    return spdlog::level::off;
}

class SpdlogLogger final : public Logger {
public:
    SpdlogLogger(std::shared_ptr<spdlog::logger> logger,
                 std::chrono::milliseconds flush_interval)
        : logger_(std::move(logger)), flush_interval_(flush_interval) {
        if (flush_interval_ > std::chrono::milliseconds::zero()) {
            flush_thread_ = std::thread([this] { FlushLoop(); });
        }
    }

    ~SpdlogLogger() override {
        // Signal stop with a plain atomic store: taking flush_mutex_ here
        // would race the flush thread's condition_variable relock of the same
        // mutex (TSAN reports it as a double lock + data race), and the
        // predicate only needs atomicity, not the mutex.
        stop_flush_.store(true, std::memory_order_release);
        flush_cv_.notify_all();
        if (flush_thread_.joinable()) {
            flush_thread_.join();
        }
        Flush();
    }

    bool ShouldLog(LogLevel level) const noexcept override {
        try {
            return logger_->should_log(ToSpdlogLevel(level));
        } catch (...) {
            return false;
        }
    }

    void Log(const LogRecord& record) noexcept override {
        try {
            const auto [file, function] = InternSourceLocation(
                record.source_location.file_name,
                record.source_location.function_name);
            const spdlog::source_loc location(
                file, static_cast<int>(record.source_location.line), function);
            logger_->log(location, ToSpdlogLevel(record.level), "{}",
                         record.message);
        } catch (...) {
            // Logging must never throw into application code.
        }
    }

    void Flush() noexcept override {
        try {
            logger_->flush();
        } catch (...) {
            // Logging must never throw into application code.
        }
    }

private:
    void FlushLoop() noexcept {
        std::unique_lock lock(flush_mutex_);
        while (!flush_cv_.wait_for(lock, flush_interval_, [this] {
            return stop_flush_.load(std::memory_order_acquire);
        })) {
            lock.unlock();
            Flush();
            lock.lock();
        }
    }

    std::pair<const char*, const char*> InternSourceLocation(
        std::string_view file, std::string_view function) {
        std::lock_guard lock(source_mutex_);
        const auto file_it = source_strings_.emplace(file).first;
        const auto function_it = source_strings_.emplace(function).first;
        return {file_it->c_str(), function_it->c_str()};
    }

    // spdlog async messages retain source_loc pointers until the worker formats
    // them, so intern source strings for the lifetime of this backend.
    std::mutex source_mutex_;
    std::unordered_set<std::string> source_strings_;
    std::shared_ptr<spdlog::logger> logger_;
    std::chrono::milliseconds flush_interval_;
    std::mutex flush_mutex_;
    std::condition_variable flush_cv_;
    std::atomic<bool> stop_flush_{false};
    std::thread flush_thread_;
};

}  // namespace

Result<std::shared_ptr<Logger>> CreateSpdlogLogger(
    const LoggingOptions& options) {
    if (!options.console_enabled && !options.file_path.has_value()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "logging requires at least one enabled sink");
    }
    if (options.file_path.has_value() &&
        (options.rotation_max_size == 0 || options.rotation_max_files == 0)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "file rotation size and file count must be positive");
    }
    if (options.async &&
        (options.queue_size == 0 || options.thread_count == 0)) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "async logging queue size and thread count must be positive");
    }

    try {
        std::vector<spdlog::sink_ptr> sinks;
        if (options.console_enabled) {
            sinks.push_back(std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
        }
        if (options.file_path.has_value()) {
            sinks.push_back(std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                options.file_path->string(), options.rotation_max_size,
                options.rotation_max_files));
        }

        std::shared_ptr<spdlog::logger> logger;
        if (options.async) {
            spdlog::init_thread_pool(options.queue_size, options.thread_count);
            logger = std::make_shared<spdlog::async_logger>(
                "mino", sinks.begin(), sinks.end(), spdlog::thread_pool(),
                spdlog::async_overflow_policy::block);
        } else {
            logger = std::make_shared<spdlog::logger>("mino", sinks.begin(),
                                                      sinks.end());
        }
        logger->set_pattern(options.pattern);
        logger->set_level(ToSpdlogLevel(options.level));
        logger->flush_on(ToSpdlogLevel(options.flush_on));
        return std::shared_ptr<Logger>(std::make_shared<SpdlogLogger>(
            std::move(logger), options.flush_interval));
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal,
                             std::string("spdlog initialization failed: ") +
                                 error.what());
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "spdlog initialization failed: unknown exception");
    }
}

}  // namespace mino::logging::internal
