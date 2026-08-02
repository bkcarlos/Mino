// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_RECORDER_SERVICE_H_
#define MINO_STORAGE_RECORDER_SERVICE_H_

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/storage/recorder.h"
#include "mino/storage/recorder_subscriber.h"
#include "mino/storage/session_recovery_coordinator.h"

namespace mino::storage {

// Bridges RecorderSubscriber's canonical copy boundary to Recorder's owned,
// policy-controlled partition buffer. The Recorder must outlive this sink.
class RecorderEnqueueSink final : public RecorderBufferSink {
public:
    RecorderEnqueueSink(Recorder& recorder, uint32_t partition_id) noexcept
        : recorder_(&recorder), partition_id_(partition_id) {}

    Result<RecorderCopyResult> ReserveCopyCommit(
        const RecorderCopyRequest& request) noexcept override;

private:
    Recorder* recorder_ = nullptr;
    uint32_t partition_id_ = 0;
};

// Type-erased production poll seam. Implementations normally wrap a typed
// RecorderSubscriber; tests may inject deterministic sources without affecting
// the production API.
class RecorderServiceSource {
public:
    virtual ~RecorderServiceSource() = default;
    virtual Result<RecorderRecordResult> PollOne() noexcept = 0;
    virtual bool has_pending() const noexcept = 0;
    virtual size_t pending_bytes() const noexcept = 0;
    virtual bool pending_persistence_configured() const noexcept = 0;
    virtual Status PersistPending() noexcept = 0;
};

template <typename T>
class RecorderSubscriberSourceAdapter final : public RecorderServiceSource {
public:
    explicit RecorderSubscriberSourceAdapter(
        std::unique_ptr<RecorderSubscriber<T>> subscriber) noexcept
        : subscriber_(std::move(subscriber)) {}

    Result<RecorderRecordResult> PollOne() noexcept override {
        if (subscriber_ == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "recorder subscriber adapter is empty");
        }
        return subscriber_->TryRecord();
    }

    bool has_pending() const noexcept override {
        return subscriber_ != nullptr && subscriber_->has_pending();
    }
    size_t pending_bytes() const noexcept override {
        return subscriber_ == nullptr ? 0 : subscriber_->pending_bytes();
    }
    bool pending_persistence_configured() const noexcept override {
        return subscriber_ != nullptr &&
               subscriber_->pending_persistence_configured();
    }
    Status PersistPending() noexcept override {
        if (subscriber_ == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "recorder subscriber adapter is empty");
        }
        return subscriber_->PersistPending();
    }

private:
    std::unique_ptr<RecorderSubscriber<T>> subscriber_;
};

enum class RecorderServiceState : uint8_t {
    kCreated,
    kStarting,
    kRunning,
    kStopping,
    kStopped,
    kError,
};

struct RecorderServiceOptions {
    std::chrono::nanoseconds idle_poll_interval = std::chrono::milliseconds(1);
    size_t max_records_per_partition_per_cycle = 256;
    size_t pending_flush_attempts = 100;
    RecordAckLevel stop_flush_level = RecordAckLevel::kDurable;
};

struct RecorderServiceMetrics {
    uint64_t poll_cycles = 0;
    uint64_t source_poll_calls = 0;
    uint64_t source_idle_polls = 0;
    uint64_t source_buffered_records = 0;
    uint64_t source_dropped_records = 0;
    uint64_t source_blocked_records = 0;
    uint64_t source_failures = 0;
    uint64_t pump_calls = 0;
    uint64_t pumped_records = 0;
};

struct RecorderServiceStatus {
    RecorderServiceState state = RecorderServiceState::kCreated;
    RecorderState recorder_state = RecorderState::kCreated;
    Status last_error = Status::Ok();
    size_t source_count = 0;
    RecorderServiceMetrics metrics{};
    RecorderMetrics recorder_metrics{};
    std::optional<SessionRecoveryReport> recovery_report;
};

// Product lifecycle owner around Recorder. Start launches one worker that polls
// every installed source and pumps Recorder partitions. Recorder remains the
// sole owner of persistence buffers/writers and serializes concurrent Flush and
// status calls.
class RecorderService final {
public:
    static Result<std::unique_ptr<RecorderService>> Create(
        std::unique_ptr<Recorder> recorder,
        RecorderServiceOptions options = {},
        RecorderClock* clock = nullptr) noexcept;
    // Product open path: recover manifests/segments/schema refs under owner locks,
    // release recovery ownership, then open Recorder for topic/source assembly.
    static Result<std::unique_ptr<RecorderService>> OpenRecovered(
        const std::filesystem::path& session_root,
        const RecorderSessionOptions& recorder_options = {},
        const SessionRecoveryOptions& recovery_options = {},
        RecorderServiceOptions service_options = {},
        RecorderClock* clock = nullptr) noexcept;

    ~RecorderService();
    RecorderService(const RecorderService&) = delete;
    RecorderService& operator=(const RecorderService&) = delete;
    RecorderService(RecorderService&&) = delete;
    RecorderService& operator=(RecorderService&&) = delete;

    // Sources must be completely assembled before Start. The service takes
    // ownership, while dependencies referenced by a typed subscriber must
    // outlive the service.
    Status AddSource(std::unique_ptr<RecorderServiceSource> source) noexcept;
    Status Start() noexcept;
    Status Flush(RecordAckLevel level = RecordAckLevel::kDurable) noexcept;
    Status Stop() noexcept;

    RecorderServiceStatus status() const noexcept;
    Recorder& recorder() noexcept { return *recorder_; }
    const Recorder& recorder() const noexcept { return *recorder_; }

private:
    RecorderService(std::unique_ptr<Recorder> recorder,
                    RecorderServiceOptions options,
                    RecorderClock* clock,
                    std::optional<SessionRecoveryReport> recovery_report =
                        std::nullopt) noexcept;

    void WorkerMain() noexcept;
    Status PollSource(RecorderServiceSource& source, bool* made_progress) noexcept;
    Status FlushPendingSources() noexcept;
    Status PersistPendingSources() noexcept;
    bool HasPendingSources() const noexcept;
    Status DrainAndFlush(RecordAckLevel level) noexcept;
    void RecordError(Status status) noexcept;
    uint64_t NowNs() noexcept;

    std::unique_ptr<Recorder> recorder_;
    RecorderServiceOptions options_;
    SystemRecorderClock system_clock_;
    RecorderClock* clock_ = nullptr;
    std::vector<std::unique_ptr<RecorderServiceSource>> sources_;

    mutable std::mutex mutex_;
    std::mutex lifecycle_mutex_;
    std::mutex cycle_mutex_;
    RecorderServiceState state_ = RecorderServiceState::kCreated;
    Status last_error_ = Status::Ok();
    RecorderServiceMetrics metrics_{};
    std::optional<SessionRecoveryReport> recovery_report_;
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> intake_paused_{false};
    std::thread worker_;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_RECORDER_SERVICE_H_
