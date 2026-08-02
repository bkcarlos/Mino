// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/retention.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <new>
#include <set>
#include <string>
#include <string_view>
#include <utility>

#include "mino/common/status.h"

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

Status IoError(std::string_view operation, const std::filesystem::path& path,
               int error) {
    StatusCode code = StatusCode::kUnavailable;
    if (error == EACCES || error == EPERM) code = StatusCode::kPermissionDenied;
    if (error == ENOENT) code = StatusCode::kNotFound;
    return Status::Error(code, std::string(operation) + " '" + path.string() +
                                   "': " + std::strerror(error));
}

bool IsKnownState(SegmentPersistentState state) noexcept {
    switch (state) {
        case SegmentPersistentState::kCreating:
        case SegmentPersistentState::kOpen:
        case SegmentPersistentState::kSealed:
        case SegmentPersistentState::kIndexed:
        case SegmentPersistentState::kRetained:
        case SegmentPersistentState::kDeleted:
            return true;
    }
    return false;
}

bool IsRetentionEligible(SegmentPersistentState state) noexcept {
    return state == SegmentPersistentState::kSealed ||
           state == SegmentPersistentState::kIndexed ||
           state == SegmentPersistentState::kRetained;
}

bool IsPinable(SegmentPersistentState state) noexcept {
    return state == SegmentPersistentState::kOpen ||
           IsRetentionEligible(state);
}

uint64_t SteadyNowNs(void*) noexcept {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    const auto nanoseconds =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
    if (nanoseconds <= 0) return 0;
    return static_cast<uint64_t>(nanoseconds);
}

uint64_t Expiration(uint64_t now_ns, uint64_t ttl_ns) noexcept {
    if (ttl_ns > std::numeric_limits<uint64_t>::max() - now_ns) {
        return std::numeric_limits<uint64_t>::max();
    }
    return now_ns + ttl_ns;
}

int DirectoryOpenFlags() noexcept {
    int flags = O_RDONLY;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    return flags;
}

Status VerifyDirectoryDescriptor(int fd, std::string_view description) {
    struct stat info {};
    if (::fstat(fd, &info) != 0) {
        return Status::Error(StatusCode::kUnavailable,
                             std::string("cannot inspect ") +
                                 std::string(description) + ": " +
                                 std::strerror(errno));
    }
    if (!S_ISDIR(info.st_mode)) {
        return Invalid(std::string(description) + " is not a directory");
    }
    return Status::Ok();
}

Status SyncDirectory(int fd, const std::filesystem::path& path) {
    while (::fsync(fd) != 0) {
        if (errno == EINTR) continue;
        return IoError("cannot fsync retention parent directory", path, errno);
    }
    return Status::Ok();
}

Result<std::string> SafeSegmentFilename(
    const std::filesystem::path& relative_path) {
    try {
        const std::string text = relative_path.generic_string();
        if (text.empty() || relative_path.is_absolute() ||
            relative_path.has_root_name() ||
            text.find('\0') != std::string::npos ||
            relative_path.lexically_normal().generic_string() != text) {
            return Invalid("retention segment path is not canonical and relative");
        }
        auto iterator = relative_path.begin();
        if (iterator == relative_path.end() || *iterator != "segments") {
            return Invalid("retention segment path must be below segments");
        }
        ++iterator;
        if (iterator == relative_path.end() || iterator->empty() ||
            *iterator == "." || *iterator == "..") {
            return Invalid("retention segment filename is invalid");
        }
        const std::filesystem::path filename = *iterator;
        ++iterator;
        if (iterator != relative_path.end() || filename.extension() != ".mino") {
            return Invalid("retention path must be segments/<name>.mino");
        }
        const std::string result = filename.string();
        if (result.find('/') != std::string::npos || result.find('\0') != std::string::npos) {
            return Invalid("retention segment filename contains a separator");
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate retention path validation state");
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

}  // namespace

Result<RetentionPlan> RetentionPlanner::Plan(
    std::span<const RetentionSegment> segments,
    const RetentionPolicy& policy, uint64_t now_ns) noexcept {
    try {
        struct Candidate {
            const RetentionSegment* segment = nullptr;
            RetentionReason reasons = RetentionReason::kNone;
            bool selected = false;
        };

        RetentionPlan plan;
        std::set<uint64_t> ids;
        std::vector<Candidate> candidates;
        candidates.reserve(segments.size());

        uint64_t total_bytes = 0;
        size_t total_count = 0;
        for (const RetentionSegment& segment : segments) {
            if (segment.manifest.segment_id == 0 ||
                !IsKnownState(segment.manifest.state)) {
                return Invalid("retention input contains invalid segment metadata");
            }
            if (!ids.insert(segment.manifest.segment_id).second) {
                return Invalid("retention input contains duplicate segment IDs");
            }
            if (segment.manifest.state != SegmentPersistentState::kDeleted) {
                if (segment.manifest.size_bytes >
                    std::numeric_limits<uint64_t>::max() - total_bytes) {
                    return Exhausted("retention byte total overflows uint64_t");
                }
                total_bytes += segment.manifest.size_bytes;
                ++total_count;
            }
            if (IsRetentionEligible(segment.manifest.state)) {
                candidates.push_back(Candidate{.segment = &segment});
            }
        }

        std::sort(candidates.begin(), candidates.end(),
                  [](const Candidate& left, const Candidate& right) {
                      if (left.segment->latest_ingestion_timestamp_ns !=
                          right.segment->latest_ingestion_timestamp_ns) {
                          return left.segment->latest_ingestion_timestamp_ns <
                                 right.segment->latest_ingestion_timestamp_ns;
                      }
                      return left.segment->manifest.segment_id <
                             right.segment->manifest.segment_id;
                  });

        for (Candidate& candidate : candidates) {
            const RetentionSegment& segment = *candidate.segment;
            if (policy.max_age_ns.has_value() &&
                segment.latest_ingestion_timestamp_ns <= now_ns &&
                now_ns - segment.latest_ingestion_timestamp_ns >=
                    *policy.max_age_ns) {
                candidate.reasons |= RetentionReason::kAge;
            }
            if (policy.delete_archived_segments && segment.archive_complete) {
                candidate.reasons |= RetentionReason::kArchiveComplete;
            }
            candidate.selected = candidate.reasons != RetentionReason::kNone;
            if (candidate.selected) {
                total_bytes -= segment.manifest.size_bytes;
                --total_count;
            }
        }

        for (Candidate& candidate : candidates) {
            const bool bytes_excess = policy.max_total_bytes.has_value() &&
                                      total_bytes > *policy.max_total_bytes;
            const bool count_excess = policy.max_segment_count.has_value() &&
                                      total_count > *policy.max_segment_count;
            if (!bytes_excess && !count_excess) break;
            if (candidate.selected) continue;
            if (bytes_excess) candidate.reasons |= RetentionReason::kTotalBytes;
            if (count_excess) {
                candidate.reasons |= RetentionReason::kSegmentCount;
            }
            candidate.selected = true;
            total_bytes -= candidate.segment->manifest.size_bytes;
            --total_count;
        }

        plan.deletions.reserve(candidates.size());
        for (const Candidate& candidate : candidates) {
            if (!candidate.selected) continue;
            plan.deletions.push_back(RetentionDecision{
                .segment_id = candidate.segment->manifest.segment_id,
                .reasons = candidate.reasons,
            });
        }
        plan.remaining_total_bytes = total_bytes;
        plan.remaining_segment_count = total_count;
        plan.byte_limit_satisfied =
            !policy.max_total_bytes.has_value() ||
            total_bytes <= *policy.max_total_bytes;
        plan.segment_limit_satisfied =
            !policy.max_segment_count.has_value() ||
            total_count <= *policy.max_segment_count;
        return plan;
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate retention plan");
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

SegmentPinManager::SegmentPinManager(
    PartitionManifest& manifest, SegmentPinManagerOptions options) noexcept
    : manifest_(&manifest), options_(options) {
    if (options_.now == nullptr) options_.now = &SteadyNowNs;
}

uint64_t SegmentPinManager::NowNs() const noexcept {
    return options_.now(options_.now_context);
}

size_t SegmentPinManager::PurgeExpiredLocked(uint64_t now_ns) noexcept {
    size_t purged = 0;
    for (auto iterator = leases_.begin(); iterator != leases_.end();) {
        if (iterator->second.expires_at_ns <= now_ns) {
            iterator = leases_.erase(iterator);
            ++purged;
        } else {
            ++iterator;
        }
    }
    return purged;
}

Result<SegmentPinLease> SegmentPinManager::AcquireLocked(
    uint64_t segment_id, uint64_t ttl_ns) {
    if (segment_id == 0) return Invalid("segment pin requires a non-zero ID");
    if (ttl_ns == 0 || options_.max_ttl_ns == 0 ||
        ttl_ns > options_.max_ttl_ns) {
        return Invalid("segment pin TTL is zero or exceeds its configured limit");
    }
    const uint64_t now_ns = NowNs();
    static_cast<void>(PurgeExpiredLocked(now_ns));

    auto segment = manifest_->FindSegment(segment_id);
    if (!segment.ok()) return segment.status();
    if (!IsPinable(segment->state)) {
        return Status::Error(StatusCode::kNotFound,
                             "segment is not in the current pinnable manifest set");
    }
    if (next_pin_id_ == 0) return Exhausted("segment pin ID space is exhausted");

    const uint64_t pin_id = next_pin_id_++;
    SegmentPinLease lease{
        .pin_id = pin_id,
        .segment_id = segment_id,
        .expires_at_ns = Expiration(now_ns, ttl_ns),
    };
    leases_.emplace(pin_id, lease);
    return lease;
}

Result<SegmentPinLease> SegmentPinManager::Acquire(
    uint64_t segment_id) noexcept {
    return Acquire(segment_id, options_.default_ttl_ns);
}

Result<SegmentPinLease> SegmentPinManager::Acquire(
    uint64_t segment_id, uint64_t ttl_ns) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        return AcquireLocked(segment_id, ttl_ns);
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate segment pin");
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal, error.what());
    }
}

Result<SegmentPinLease> SegmentPinManager::RenewLocked(
    uint64_t pin_id, uint64_t ttl_ns) {
    if (pin_id == 0) return Invalid("segment pin requires a non-zero ID");
    if (ttl_ns == 0 || options_.max_ttl_ns == 0 ||
        ttl_ns > options_.max_ttl_ns) {
        return Invalid("segment pin TTL is zero or exceeds its configured limit");
    }
    const uint64_t now_ns = NowNs();
    static_cast<void>(PurgeExpiredLocked(now_ns));
    auto found = leases_.find(pin_id);
    if (found == leases_.end()) {
        return Status::Error(StatusCode::kNotFound,
                             "segment pin is unknown or expired");
    }
    found->second.expires_at_ns = Expiration(now_ns, ttl_ns);
    return found->second;
}

Result<SegmentPinLease> SegmentPinManager::Renew(uint64_t pin_id) noexcept {
    return Renew(pin_id, options_.default_ttl_ns);
}

Result<SegmentPinLease> SegmentPinManager::Renew(
    uint64_t pin_id, uint64_t ttl_ns) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        return RenewLocked(pin_id, ttl_ns);
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal, error.what());
    }
}

Status SegmentPinManager::Release(uint64_t pin_id) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        static_cast<void>(PurgeExpiredLocked(NowNs()));
        leases_.erase(pin_id);
        return Status::Ok();
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal, error.what());
    }
}

size_t SegmentPinManager::PurgeExpired() noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        return PurgeExpiredLocked(NowNs());
    } catch (const std::exception&) {
        return 0;
    }
}

size_t SegmentPinManager::ActivePinCount(uint64_t segment_id) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        static_cast<void>(PurgeExpiredLocked(NowNs()));
        return static_cast<size_t>(std::count_if(
            leases_.begin(), leases_.end(),
            [segment_id](const auto& item) {
                return item.second.segment_id == segment_id;
            }));
    } catch (const std::exception&) {
        // Fail closed: an executor must never unlink merely because pin state
        // could not be inspected.
        return std::numeric_limits<size_t>::max();
    }
}

Result<SegmentPinManager::BeginDeletionResult>
SegmentPinManager::BeginDeletion(uint64_t segment_id) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        auto current = manifest_->FindSegment(segment_id);
        if (!current.ok()) return current.status();
        if (current->state == SegmentPersistentState::kDeleted) {
            return BeginDeletionResult{.segment = *current,
                                       .transitioned = false};
        }
        if (!IsRetentionEligible(current->state)) {
            return Invalid("retention cannot delete an OPEN or CREATING segment");
        }

        // PartitionManifest's existing state machine routes SEALED through
        // INDEXED before DELETED. Both commits are durable; a crash between them
        // leaves an INDEXED segment that a later plan can safely select again.
        if (current->state == SegmentPersistentState::kSealed) {
            current->state = SegmentPersistentState::kIndexed;
            const Status indexed = manifest_->UpdateSegment(*current);
            if (!indexed.ok()) return indexed;
        }
        current->state = SegmentPersistentState::kDeleted;
        const Status deleted = manifest_->UpdateSegment(*current);
        if (!deleted.ok()) return deleted;
        return BeginDeletionResult{.segment = *current, .transitioned = true};
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate manifest deletion transition");
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal, error.what());
    }
}

RetentionExecutor::RetentionExecutor(
    std::filesystem::path partition_root, PartitionManifest& manifest,
    SegmentPinManager& pins, RetentionExecutorOptions options, int root_fd,
    int segments_fd) noexcept
    : partition_root_(std::move(partition_root)),
      manifest_(&manifest),
      pins_(&pins),
      options_(options),
      root_fd_(root_fd),
      segments_fd_(segments_fd) {}

RetentionExecutor::~RetentionExecutor() {
    if (segments_fd_ >= 0) static_cast<void>(::close(segments_fd_));
    if (root_fd_ >= 0) static_cast<void>(::close(root_fd_));
}

Result<std::unique_ptr<RetentionExecutor>> RetentionExecutor::Create(
    const std::filesystem::path& partition_root,
    PartitionManifest& manifest, SegmentPinManager& pins,
    RetentionExecutorOptions options) noexcept {
    int root_fd = -1;
    int segments_fd = -1;
    try {
        if (pins.manifest_ != &manifest) {
            return Invalid("retention executor and pin manager use different manifests");
        }
        struct stat link_info {};
        if (::lstat(partition_root.c_str(), &link_info) != 0) {
            return IoError("cannot inspect retention partition root",
                           partition_root, errno);
        }
        if (S_ISLNK(link_info.st_mode) || !S_ISDIR(link_info.st_mode)) {
            return Invalid("retention partition root is not a real directory");
        }

        root_fd = ::open(partition_root.c_str(), DirectoryOpenFlags());
        if (root_fd < 0) {
            return IoError("cannot open retention partition root",
                           partition_root, errno);
        }
        Status status = VerifyDirectoryDescriptor(root_fd, "partition root");
        if (!status.ok()) {
            static_cast<void>(::close(root_fd));
            return status;
        }
        segments_fd = ::openat(root_fd, "segments", DirectoryOpenFlags());
        if (segments_fd < 0) {
            const Status error = IoError("cannot open retention segments directory",
                                         partition_root / "segments", errno);
            static_cast<void>(::close(root_fd));
            return error;
        }
        status = VerifyDirectoryDescriptor(segments_fd, "segments path");
        if (!status.ok()) {
            static_cast<void>(::close(segments_fd));
            static_cast<void>(::close(root_fd));
            return status;
        }
        return std::unique_ptr<RetentionExecutor>(new RetentionExecutor(
            partition_root, manifest, pins, options, root_fd, segments_fd));
    } catch (const std::bad_alloc&) {
        if (segments_fd >= 0) static_cast<void>(::close(segments_fd));
        if (root_fd >= 0) static_cast<void>(::close(root_fd));
        return Exhausted("cannot allocate retention executor");
    } catch (const std::exception& error) {
        if (segments_fd >= 0) static_cast<void>(::close(segments_fd));
        if (root_fd >= 0) static_cast<void>(::close(root_fd));
        return Invalid(error.what());
    }
}

Status RetentionExecutor::RunFaultHook(RetentionFaultPoint point,
                                       uint64_t segment_id) const noexcept {
    if (options_.fault_hook == nullptr) return Status::Ok();
    return options_.fault_hook(point, segment_id,
                               options_.fault_hook_context);
}

Result<bool> RetentionExecutor::UnlinkSegment(
    const SegmentManifestEntry& segment) noexcept {
    try {
        auto filename = SafeSegmentFilename(segment.relative_path);
        if (!filename.ok()) return filename.status();
        const std::filesystem::path display_path =
            partition_root_ / segment.relative_path;

        struct stat info {};
        int stat_result;
        do {
            stat_result = ::fstatat(segments_fd_, filename->c_str(), &info,
                                    AT_SYMLINK_NOFOLLOW);
        } while (stat_result != 0 && errno == EINTR);
        if (stat_result != 0) {
            const int error = errno;
            if (error != ENOENT) {
                return IoError("cannot inspect segment before unlink",
                               display_path, error);
            }
            MINO_RETURN_IF_ERROR(SyncDirectory(
                segments_fd_, partition_root_ / "segments"));
            MINO_RETURN_IF_ERROR(RunFaultHook(
                RetentionFaultPoint::kAfterParentDirectorySync,
                segment.segment_id));
            return true;
        }
        if (S_ISLNK(info.st_mode)) {
            return Invalid("refusing to unlink a symlink segment path");
        }
        if (!S_ISREG(info.st_mode)) {
            return Invalid("refusing to unlink a non-regular segment path");
        }

        MINO_RETURN_IF_ERROR(RunFaultHook(RetentionFaultPoint::kBeforeUnlink,
                                          segment.segment_id));
        int unlink_result;
        do {
            unlink_result = ::unlinkat(segments_fd_, filename->c_str(), 0);
        } while (unlink_result != 0 && errno == EINTR);
        if (unlink_result != 0) {
            const int error = errno;
            if (error != ENOENT) {
                return IoError("cannot unlink retained segment", display_path,
                               error);
            }
        } else {
            MINO_RETURN_IF_ERROR(RunFaultHook(
                RetentionFaultPoint::kAfterUnlink, segment.segment_id));
        }
        MINO_RETURN_IF_ERROR(SyncDirectory(
            segments_fd_, partition_root_ / "segments"));
        MINO_RETURN_IF_ERROR(RunFaultHook(
            RetentionFaultPoint::kAfterParentDirectorySync,
            segment.segment_id));
        return unlink_result != 0;
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate retention unlink state");
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal, error.what());
    }
}

Result<RetentionExecutionReport> RetentionExecutor::ExecuteIds(
    std::span<const uint64_t> segment_ids) noexcept {
    try {
        std::set<uint64_t> unique_ids;
        for (uint64_t segment_id : segment_ids) {
            if (segment_id == 0 || !unique_ids.insert(segment_id).second) {
                return Invalid("retention execution contains a zero or duplicate ID");
            }
        }

        RetentionExecutionReport report;
        for (uint64_t segment_id : segment_ids) {
            auto deletion = pins_->BeginDeletion(segment_id);
            if (!deletion.ok()) return deletion.status();
            if (deletion->transitioned) {
                report.marked_deleted.push_back(segment_id);
            }
            MINO_RETURN_IF_ERROR(RunFaultHook(
                RetentionFaultPoint::kAfterManifestDeleted, segment_id));
            if (pins_->ActivePinCount(segment_id) != 0) {
                report.pending_pins.push_back(segment_id);
                continue;
            }
            auto missing = UnlinkSegment(deletion->segment);
            if (!missing.ok()) return missing.status();
            if (*missing) {
                report.already_missing.push_back(segment_id);
            } else {
                report.unlinked.push_back(segment_id);
            }
        }
        return report;
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate retention execution report");
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal, error.what());
    }
}

Result<RetentionExecutionReport> RetentionExecutor::Execute(
    const RetentionPlan& plan) noexcept {
    try {
        std::vector<uint64_t> ids;
        ids.reserve(plan.deletions.size());
        for (const RetentionDecision& decision : plan.deletions) {
            ids.push_back(decision.segment_id);
        }
        return ExecuteIds(ids);
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate retention execution IDs");
    }
}

Result<RetentionExecutionReport>
RetentionExecutor::RecoverPendingDeletions() noexcept {
    try {
        std::vector<uint64_t> ids;
        for (const SegmentManifestEntry& segment : manifest_->snapshot().segments) {
            if (segment.state == SegmentPersistentState::kDeleted) {
                ids.push_back(segment.segment_id);
            }
        }
        return ExecuteIds(ids);
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate pending deletion recovery IDs");
    } catch (const std::exception& error) {
        return Status::Error(StatusCode::kInternal, error.what());
    }
}

}  // namespace mino::storage
