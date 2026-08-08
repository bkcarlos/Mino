// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recorder_service.h"

#include <algorithm>
#include <new>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

bool IsIdleStatus(const Status& status) noexcept {
    return status.code() == StatusCode::kWouldBlock ||
           status.code() == StatusCode::kTimeout;
}

bool ValidOptions(const RecorderServiceOptions& options) noexcept {
    return options.idle_poll_interval >= std::chrono::nanoseconds::zero() &&
           options.max_records_per_partition_per_cycle != 0 &&
           options.pending_flush_attempts != 0;
}

Result<capacity::CapacityReservation> ReserveServiceThread(
    const std::shared_ptr<capacity::CapacityController>& controller) {
    if (!controller) return capacity::CapacityReservation{};
    capacity::ResourceVector resources;
    resources.threads = 1;
    return controller->Reserve(capacity::ResourceRequest{
        .resources = resources,
        .scope = capacity::ResourceScope::kRecorder,
        .admission_class = capacity::AdmissionClass::kDataPlane,
        .name = "recorder service worker",
    });
}

}  // namespace

Result<RecorderCopyResult> RecorderEnqueueSink::ReserveCopyCommit(
    const RecorderCopyRequest& request) noexcept {
    if (recorder_ == nullptr || request.metadata == nullptr) {
        return Invalid("recorder enqueue sink request is incomplete");
    }
    Result<RecorderEnqueueResult> enqueued = recorder_->Enqueue(
        partition_id_, *request.metadata, request.payload, request.user_tag,
        std::nullopt, request.timeout);
    if (!enqueued.ok()) return enqueued.status();

    RecorderCopyResult result{.admission = BufferAdmission::kAccepted,
                              .discarded = std::move(enqueued->discarded)};
    switch (enqueued->disposition) {
        case RecorderEnqueueDisposition::kBuffered:
            return result;
        case RecorderEnqueueDisposition::kDropped:
            result.admission = BufferAdmission::kDroppedNewest;
            return result;
        case RecorderEnqueueDisposition::kFailed:
            if (!enqueued->status.ok()) return enqueued->status;
            result.admission = BufferAdmission::kRecordingFailed;
            return result;
        case RecorderEnqueueDisposition::kBlocked:
            if (IsIdleStatus(enqueued->status)) return enqueued->status;
            return Status::Error(StatusCode::kWouldBlock,
                                 "recorder partition buffer is full");
    }
    return Status::Error(StatusCode::kInternal,
                         "recorder returned an unknown enqueue disposition");
}

Result<std::unique_ptr<RecorderService>> RecorderService::Create(
    std::unique_ptr<Recorder> recorder, RecorderServiceOptions options,
    RecorderClock* clock,
    std::shared_ptr<capacity::CapacityController> capacity_controller) noexcept {
    if (recorder == nullptr) return Invalid("recorder service recorder is null");
    if (!ValidOptions(options)) {
        return Invalid("recorder service options are invalid");
    }
    MINO_ASSIGN_OR_RETURN(auto capacity_reservation,
                          ReserveServiceThread(capacity_controller));
    try {
        MINO_ASSIGN_OR_RETURN(auto capacity_lease,
                              capacity_reservation.Commit());
        return std::unique_ptr<RecorderService>(new RecorderService(
            std::move(capacity_lease), std::move(recorder), options, clock,
            std::nullopt));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "cannot allocate recorder service");
    }
}

Result<std::unique_ptr<RecorderService>> RecorderService::OpenRecovered(
    const std::filesystem::path& session_root,
    const RecorderSessionOptions& recorder_options,
    const SessionRecoveryOptions& recovery_options,
    RecorderServiceOptions service_options,
    RecorderClock* clock,
    std::shared_ptr<capacity::CapacityController> capacity_controller) noexcept {
    if (!ValidOptions(service_options)) {
        return Invalid("recorder service options are invalid");
    }
    MINO_ASSIGN_OR_RETURN(auto capacity_reservation,
                          ReserveServiceThread(capacity_controller));
    Result<std::unique_ptr<SessionRecoveryCoordinator>> coordinator =
        SessionRecoveryCoordinator::Open(session_root, recovery_options);
    if (!coordinator.ok()) return coordinator.status();
    Result<SessionRecoveryReport> recovery = (*coordinator)->Recover();
    if (!recovery.ok()) return recovery.status();
    coordinator->reset();
    Result<std::unique_ptr<Recorder>> recorder =
        Recorder::Open(session_root, recorder_options);
    if (!recorder.ok()) return recorder.status();
    try {
        MINO_ASSIGN_OR_RETURN(auto capacity_lease,
                              capacity_reservation.Commit());
        return std::unique_ptr<RecorderService>(new RecorderService(
            std::move(capacity_lease), std::move(*recorder), service_options,
            clock, std::move(*recovery)));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "cannot allocate recovered recorder service");
    }
}

RecorderService::RecorderService(
    capacity::CapacityLease capacity_lease,
    std::unique_ptr<Recorder> recorder, RecorderServiceOptions options,
    RecorderClock* clock,
    std::optional<SessionRecoveryReport> recovery_report) noexcept
    : capacity_lease_(std::move(capacity_lease)),
      recorder_(std::move(recorder)),
      options_(options),
      clock_(clock == nullptr ? &system_clock_ : clock),
      recovery_report_(std::move(recovery_report)) {}

RecorderService::~RecorderService() {
    static_cast<void>(Stop());
}

Status RecorderService::AddSource(
    std::unique_ptr<RecorderServiceSource> source) noexcept {
    if (source == nullptr) return Invalid("recorder service source is null");
    if (!source->pending_persistence_configured()) {
        return Invalid(
            "recorder service source requires durable pending persistence");
    }
    std::lock_guard lock(mutex_);
    if (state_ != RecorderServiceState::kCreated) {
        return Invalid("recorder service sources may only be added before Start");
    }
    try {
        sources_.push_back(std::move(source));
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "cannot allocate recorder service source");
    }
}

uint64_t RecorderService::NowNs() noexcept {
    return clock_->NowNs();
}

Status RecorderService::Start() noexcept {
    std::lock_guard lifecycle_lock(lifecycle_mutex_);
    {
        std::lock_guard lock(mutex_);
        if (state_ != RecorderServiceState::kCreated) {
            return Invalid("RecorderService can only start once");
        }
        if (sources_.empty()) {
            return Invalid("RecorderService has no installed sources");
        }
        state_ = RecorderServiceState::kStarting;
    }

    stop_requested_.store(false, std::memory_order_release);
    intake_paused_.store(false, std::memory_order_release);
    const Status started = recorder_->Start(NowNs());
    if (!started.ok() && started.code() != StatusCode::kDegraded) {
        RecordError(started);
        capacity_lease_.Reset();
        return started;
    }

    try {
        worker_ = std::thread(&RecorderService::WorkerMain, this);
    } catch (const std::system_error& error) {
        const Status failure = Status::Error(
            StatusCode::kUnavailable,
            std::string("cannot start recorder worker: ") + error.what());
        RecordError(failure);
        static_cast<void>(recorder_->Stop(NowNs()));
        capacity_lease_.Reset();
        return failure;
    } catch (const std::bad_alloc&) {
        const Status failure = Status::Error(
            StatusCode::kResourceExhausted,
            "cannot allocate recorder worker state");
        RecordError(failure);
        static_cast<void>(recorder_->Stop(NowNs()));
        capacity_lease_.Reset();
        return failure;
    }
    {
        std::lock_guard lock(mutex_);
        state_ = RecorderServiceState::kRunning;
        if (!started.ok()) last_error_ = started;
    }
    return started;
}

Status RecorderService::PollSource(RecorderServiceSource& source,
                                   bool* made_progress) noexcept {
    {
        std::lock_guard lock(mutex_);
        ++metrics_.source_poll_calls;
    }
    Result<RecorderRecordResult> polled = source.PollOne();
    if (!polled.ok()) {
        if (IsIdleStatus(polled.status())) {
            std::lock_guard lock(mutex_);
            ++metrics_.source_idle_polls;
            return Status::Ok();
        }
        std::lock_guard lock(mutex_);
        ++metrics_.source_failures;
        return polled.status();
    }

    switch (polled->disposition) {
        case RecorderRecordDisposition::kBuffered:
            *made_progress = true;
            {
                std::lock_guard lock(mutex_);
                ++metrics_.source_buffered_records;
            }
            break;
        case RecorderRecordDisposition::kDropped:
            *made_progress = true;
            {
                std::lock_guard lock(mutex_);
                ++metrics_.source_dropped_records;
            }
            break;
        case RecorderRecordDisposition::kBlocked:
            {
                std::lock_guard lock(mutex_);
                ++metrics_.source_blocked_records;
            }
            break;
        case RecorderRecordDisposition::kFailed:
            {
                std::lock_guard lock(mutex_);
                ++metrics_.source_failures;
            }
            if (!polled->ack_status.ok()) return polled->ack_status;
            return polled->status.ok()
                       ? Status::Error(StatusCode::kUnavailable,
                                       "recorder source failed")
                       : polled->status;
    }
    return Status::Ok();
}

void RecorderService::WorkerMain() noexcept {
    while (!stop_requested_.load(std::memory_order_acquire)) {
        if (intake_paused_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
            continue;
        }
        std::unique_lock cycle_lock(cycle_mutex_);
        if (stop_requested_.load(std::memory_order_acquire)) return;
        if (intake_paused_.load(std::memory_order_acquire)) continue;

        bool made_progress = false;
        {
            std::lock_guard lock(mutex_);
            ++metrics_.poll_cycles;
        }
        Status failure = Status::Ok();
        for (const auto& source : sources_) {
            const Status polled = PollSource(*source, &made_progress);
            if (!polled.ok()) {
                failure = polled;
                break;
            }
        }

        if (failure.ok()) {
            Result<RecorderPumpResult> pumped = recorder_->Pump(
                NowNs(), options_.max_records_per_partition_per_cycle);
            if (!pumped.ok()) {
                failure = pumped.status();
            } else {
                {
                    std::lock_guard lock(mutex_);
                    ++metrics_.pump_calls;
                    metrics_.pumped_records += pumped->dequeued_records;
                }
                made_progress = made_progress || pumped->dequeued_records != 0;
                if (!pumped->failures.empty()) {
                    failure = pumped->failures.front().status;
                }
            }
        }
        if (!failure.ok()) {
            const Status drained = DrainAndFlush(RecordAckLevel::kDurable);
            if (!drained.ok()) {
                const Status persisted = PersistPendingSources();
                if (!persisted.ok()) failure = persisted;
            }
            RecordError(failure);
            stop_requested_.store(true, std::memory_order_release);
            return;
        }
        cycle_lock.unlock();
        if (!made_progress && options_.idle_poll_interval.count() != 0) {
            std::this_thread::sleep_for(options_.idle_poll_interval);
        }
    }
}

Status RecorderService::Flush(RecordAckLevel level) noexcept {
    std::lock_guard lifecycle_lock(lifecycle_mutex_);
    {
        std::lock_guard lock(mutex_);
        if (state_ != RecorderServiceState::kRunning) {
            return Invalid("RecorderService is not running");
        }
    }
    // Pause future intake, then wait for the in-flight cycle. Everything polled
    // before this call is now either Recorder-owned or subscriber-pending.
    intake_paused_.store(true, std::memory_order_release);
    std::lock_guard cycle_lock(cycle_mutex_);
    const Status flushed = DrainAndFlush(level);
    intake_paused_.store(false, std::memory_order_release);
    if (!flushed.ok()) RecordError(flushed);
    return flushed;
}

Status RecorderService::FlushPendingSources() noexcept {
    for (size_t attempt = 0; attempt < options_.pending_flush_attempts; ++attempt) {
        bool any_pending = false;
        bool made_progress = false;
        for (const auto& source : sources_) {
            if (!source->has_pending()) continue;
            any_pending = true;
            const Status polled = PollSource(*source, &made_progress);
            if (!polled.ok()) return polled;
        }
        if (!any_pending) return Status::Ok();
        Result<RecorderPumpResult> pumped = recorder_->Pump(
            NowNs(), std::numeric_limits<size_t>::max());
        if (!pumped.ok()) return pumped.status();
        {
            std::lock_guard lock(mutex_);
            ++metrics_.pump_calls;
            metrics_.pumped_records += pumped->dequeued_records;
        }
        if (!pumped->failures.empty()) return pumped->failures.front().status;
        if (!made_progress && options_.idle_poll_interval.count() != 0) {
            std::this_thread::sleep_for(options_.idle_poll_interval);
        }
    }
    return Status::Error(StatusCode::kTimeout,
                         "recorder pending source bytes did not drain");
}

bool RecorderService::HasPendingSources() const noexcept {
    return std::any_of(sources_.begin(), sources_.end(),
                       [](const auto& source) { return source->has_pending(); });
}

Status RecorderService::PersistPendingSources() noexcept {
    Status first_error = Status::Ok();
    for (const auto& source : sources_) {
        if (!source->has_pending()) continue;
        const Status persisted = source->PersistPending();
        if (!persisted.ok() && first_error.ok()) first_error = persisted;
    }
    return first_error;
}

Status RecorderService::DrainAndFlush(RecordAckLevel level) noexcept {
    Status drained = FlushPendingSources();
    if (!drained.ok()) return drained;
    Result<RecorderPumpResult> pumped = recorder_->Pump(
        NowNs(), std::numeric_limits<size_t>::max());
    if (!pumped.ok()) return pumped.status();
    {
        std::lock_guard lock(mutex_);
        ++metrics_.pump_calls;
        metrics_.pumped_records += pumped->dequeued_records;
    }
    if (!pumped->failures.empty()) return pumped->failures.front().status;
    return recorder_->Flush(level, NowNs());
}

Status RecorderService::Stop() noexcept {
    std::lock_guard lifecycle_lock(lifecycle_mutex_);
    {
        std::lock_guard lock(mutex_);
        if (state_ == RecorderServiceState::kStopped) return Status::Ok();
        if (state_ == RecorderServiceState::kStopping) {
            return Invalid("RecorderService stop is already in progress");
        }
        state_ = RecorderServiceState::kStopping;
    }

    intake_paused_.store(true, std::memory_order_release);
    stop_requested_.store(true, std::memory_order_release);
    if (worker_.joinable()) worker_.join();
    capacity_lease_.Reset();
    std::lock_guard cycle_lock(cycle_mutex_);

    Status result = Status::Ok();
    const RecorderState recorder_state = recorder_->state();
    if (recorder_state == RecorderState::kRunning ||
        recorder_state == RecorderState::kDegraded) {
        result = DrainAndFlush(options_.stop_flush_level);
        if (!result.ok() && HasPendingSources()) {
            const Status persisted = PersistPendingSources();
            if (!persisted.ok()) {
                RecordError(persisted);
                return persisted;
            }
        }
    }
    const Status stopped = recorder_->Stop(NowNs());
    if (result.ok() && !stopped.ok()) result = stopped;

    {
        std::lock_guard lock(mutex_);
        if (!result.ok()) last_error_ = result;
        state_ = stopped.ok() ? RecorderServiceState::kStopped
                              : RecorderServiceState::kError;
    }
    return result;
}

void RecorderService::RecordError(Status status) noexcept {
    std::lock_guard lock(mutex_);
    last_error_ = std::move(status);
    state_ = RecorderServiceState::kError;
}

RecorderServiceStatus RecorderService::status() const noexcept {
    RecorderServiceStatus snapshot;
    {
        std::lock_guard lock(mutex_);
        snapshot.state = state_;
        snapshot.last_error = last_error_;
        snapshot.source_count = sources_.size();
        snapshot.metrics = metrics_;
        snapshot.recovery_report = recovery_report_;
    }
    // Never hold the service mutex while entering Recorder: the worker updates
    // service metrics after Pump and therefore uses the opposite lock order.
    snapshot.recorder_state = recorder_->state();
    snapshot.recorder_metrics = recorder_->metrics();
    return snapshot;
}

}  // namespace mino::storage
