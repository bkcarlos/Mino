// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/platform/shared_memory.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#if defined(__unix__) || defined(__APPLE__)
#define MINO_HAS_POSIX 1
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#else
#define MINO_HAS_POSIX 0
#endif

#if defined(__linux__)
#define MINO_HAS_HUGETLB 1
#include <linux/magic.h>
#include <sys/vfs.h>
#else
#define MINO_HAS_HUGETLB 0
#endif

#include "mino/platform/process_identity.h"
#include "mino/platform/shared_memory_marker.h"

namespace mino {
namespace {

using shared_memory_internal::MarkerBackingKind;
using shared_memory_internal::MarkerPayloadCrc32;
using shared_memory_internal::MarkerState;
using shared_memory_internal::SharedMemoryMarkerPayload;
using shared_memory_internal::SharedMemoryMarkerRecord;
using shared_memory_internal::SharedMemoryTestPoint;

constexpr uint64_t kFallbackHugePageSize = 2ull * 1024 * 1024;
std::atomic<shared_memory_internal::SharedMemoryTestHook> g_test_hook{nullptr};

void RunTestHook(SharedMemoryTestPoint point) {
    auto hook = g_test_hook.load(std::memory_order_acquire);
    if (hook != nullptr) hook(point);
}

Status ErrnoStatus(std::string_view what, int error_number = errno) {
    StatusCode code = StatusCode::kInternal;
    if (error_number == ENOENT) code = StatusCode::kNotFound;
    if (error_number == EACCES || error_number == EPERM) {
        code = StatusCode::kPermissionDenied;
    }
    if (error_number == EEXIST) code = StatusCode::kAlreadyExists;
    if (error_number == EAGAIN || error_number == EWOULDBLOCK) {
        code = StatusCode::kWouldBlock;
    }
    return Status::Error(code, std::string(what) + ": " +
                                   std::strerror(error_number));
}

Status ValidateName(const std::string& name) {
    if (name.empty() || name[0] != '/') {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm name must start with '/'");
    }
    if (name.size() == 1) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm name is empty after '/'");
    }
    if (name.find('/', 1) != std::string::npos) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm name must not contain additional '/'");
    }
    return Status::Ok();
}

uint64_t HostPageSize() {
#if MINO_HAS_POSIX
    const long page = ::sysconf(_SC_PAGESIZE);
    return page > 0 ? static_cast<uint64_t>(page) : 4096;
#else
    return 4096;
#endif
}

Result<uint64_t> CheckedRoundUp(uint64_t size, uint64_t alignment) {
    if (alignment == 0) {
        return Status::Error(StatusCode::kInternal, "page size is zero");
    }
    const uint64_t remainder = size % alignment;
    if (remainder == 0) return size;
    const uint64_t increment = alignment - remainder;
    if (size > std::numeric_limits<uint64_t>::max() - increment) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm size rounding overflow");
    }
    return size + increment;
}

uint64_t DefaultHugePageSize() {
#if MINO_HAS_HUGETLB
    std::ifstream meminfo("/proc/meminfo");
    std::string line;
    while (std::getline(meminfo, line)) {
        if (line.rfind("Hugepagesize:", 0) != 0) continue;
        std::istringstream fields(line.substr(line.find(':') + 1));
        uint64_t value = 0;
        std::string unit;
        if (fields >> value >> unit && unit == "kB" &&
            value <= std::numeric_limits<uint64_t>::max() / 1024) {
            return value * 1024;
        }
    }
#endif
    return kFallbackHugePageSize;
}

#if MINO_HAS_HUGETLB
std::string ConfiguredHugePageDirectory(const std::string& explicit_path) {
    if (!explicit_path.empty()) return explicit_path;
#if MINO_HAS_HUGETLB
    const char* configured = std::getenv("MINO_HUGETLBFS_PATH");
    if (configured != nullptr && configured[0] != '\0') return configured;
#endif
    return "/dev/hugepages";
}

int StatusFallbackErrno(const Status& status) {
    switch (status.code()) {
        case StatusCode::kNotFound:
            return ENOENT;
        case StatusCode::kPermissionDenied:
            return EACCES;
        case StatusCode::kUnsupported:
            return ENODEV;
        case StatusCode::kResourceExhausted:
            return ENOMEM;
        default:
            return errno != 0 ? errno : EIO;
    }
}

HugePageFallbackReason ClassifyHugePageFailure(int error_number) {
    switch (error_number) {
        case ENOENT:
        case ENOTDIR:
            return HugePageFallbackReason::kHugetlbfsUnavailable;
        case ENOMEM:
        case ENOSPC:
            return HugePageFallbackReason::kInsufficientHugePages;
        case EACCES:
        case EPERM:
            return HugePageFallbackReason::kPermissionDenied;
        case EINVAL:
        case ENODEV:
#if defined(EOPNOTSUPP) && EOPNOTSUPP != ENOTSUP
        case EOPNOTSUPP:
#endif
        case ENOTSUP:
            return HugePageFallbackReason::kUnsupportedBacking;
        default:
            return HugePageFallbackReason::kSystemError;
    }
}
#endif  // MINO_HAS_HUGETLB

bool CopyMarkerString(std::string_view value, char* destination,
                      size_t capacity) {
    if (value.size() >= capacity) return false;
    std::memset(destination, 0, capacity);
    std::memcpy(destination, value.data(), value.size());
    return true;
}

Result<std::string> ReadMarkerString(const char* value, size_t capacity,
                                     std::string_view field) {
    const void* terminator = std::memchr(value, '\0', capacity);
    if (terminator == nullptr) {
        return Status::Error(StatusCode::kCorruption,
                             std::string(field) + " is not terminated");
    }
    return std::string(value,
                       static_cast<const char*>(terminator) - value);
}

bool IsKnownState(uint32_t state) {
    return state >= static_cast<uint32_t>(MarkerState::kCreating) &&
           state <= static_cast<uint32_t>(MarkerState::kUnlinking);
}

bool IsKnownBacking(uint32_t kind) {
    return kind <= static_cast<uint32_t>(MarkerBackingKind::kPosixData);
}

Status ValidatePayload(const SharedMemoryMarkerPayload& payload) {
    if (payload.magic != shared_memory_internal::kMarkerMagic ||
        payload.version != shared_memory_internal::kMarkerVersion ||
        payload.payload_size != sizeof(SharedMemoryMarkerPayload)) {
        return Status::Error(StatusCode::kCorruption,
                             "invalid shared-memory marker version");
    }
    if (!IsKnownState(payload.state) ||
        !IsKnownBacking(payload.backing_kind) || payload.data_size == 0 ||
        payload.page_size == 0) {
        return Status::Error(StatusCode::kCorruption,
                             "invalid shared-memory marker fields");
    }
    if (!ReadMarkerString(payload.mount_path, sizeof(payload.mount_path),
                          "mount_path")
             .ok() ||
        !ReadMarkerString(payload.backing_name, sizeof(payload.backing_name),
                          "backing_name")
             .ok()) {
        return Status::Error(StatusCode::kCorruption,
                             "invalid shared-memory marker string");
    }
    return Status::Ok();
}

#if MINO_HAS_POSIX

struct MarkerMapping {
    int fd = -1;
    SharedMemoryMarkerRecord* record = nullptr;

    MarkerMapping() = default;
    MarkerMapping(const MarkerMapping&) = delete;
    MarkerMapping& operator=(const MarkerMapping&) = delete;
    MarkerMapping(MarkerMapping&& other) noexcept
        : fd(other.fd), record(other.record) {
        other.fd = -1;
        other.record = nullptr;
    }
    ~MarkerMapping() {
        if (record != nullptr) {
            (void)::munmap(record, sizeof(SharedMemoryMarkerRecord));
        }
        if (fd >= 0) (void)::close(fd);
    }
};

Result<MarkerMapping> MapMarkerFd(int fd) {
    void* address = ::mmap(nullptr, sizeof(SharedMemoryMarkerRecord),
                           PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (address == MAP_FAILED) return ErrnoStatus("mmap(marker) failed");
    MarkerMapping mapping;
    mapping.fd = fd;
    mapping.record = static_cast<SharedMemoryMarkerRecord*>(address);
    return mapping;
}

Status LockMarkerExclusive(int fd, bool already_exists) {
#if defined(__APPLE__)
    (void)fd;
    (void)already_exists;
    return Status::Error(
        StatusCode::kUnsupported,
        "persistent shared-memory marker locking is supported only on Linux");
#else
    if (::flock(fd, LOCK_EX | LOCK_NB) == 0) return Status::Ok();
    if (errno == EWOULDBLOCK || errno == EAGAIN) {
        return Status::Error(already_exists ? StatusCode::kAlreadyExists
                                           : StatusCode::kWouldBlock,
                             "shared-memory marker is busy");
    }
    return ErrnoStatus("flock(marker) failed");
#endif
}

Status WriteInitialMarker(int fd,
                          const SharedMemoryMarkerPayload& payload) {
    SharedMemoryMarkerRecord record;
    record.published_word = 2;  // generation 1, slot 0
    record.slots[0].payload = payload;
    record.slots[0].payload_crc32 = MarkerPayloadCrc32(payload);
    const ssize_t written = ::pwrite(fd, &record, sizeof(record), 0);
    if (written != static_cast<ssize_t>(sizeof(record))) {
        return written < 0 ? ErrnoStatus("pwrite(marker) failed")
                           : Status::Error(StatusCode::kInternal,
                                           "short marker write");
    }
    if (::fsync(fd) != 0) return ErrnoStatus("fsync(marker) failed");
    return Status::Ok();
}

Status PublishMarker(SharedMemoryMarkerRecord* record,
                     const SharedMemoryMarkerPayload& payload) {
    std::atomic_ref<uint64_t> published(record->published_word);
    const uint64_t current = published.load(std::memory_order_acquire);
    const size_t inactive_slot = (current & 1u) ^ 1u;
    record->slots[inactive_slot].payload = payload;
    record->slots[inactive_slot].payload_crc32 = MarkerPayloadCrc32(payload);
    if (::msync(record, sizeof(*record), MS_SYNC) != 0) {
        return ErrnoStatus("msync(marker slot) failed");
    }
    RunTestHook(SharedMemoryTestPoint::kBeforeMarkerPublication);
    const uint64_t next = (((current >> 1) + 1) << 1) | inactive_slot;
    published.store(next, std::memory_order_release);
    if (::msync(record, sizeof(*record), MS_SYNC) != 0) {
        return ErrnoStatus("msync(marker publication) failed");
    }
    return Status::Ok();
}

bool ValidMarkerSlot(const shared_memory_internal::SharedMemoryMarkerSlot& slot) {
    return slot.payload_crc32 == MarkerPayloadCrc32(slot.payload) &&
           ValidatePayload(slot.payload).ok();
}

Result<SharedMemoryMarkerPayload> ReadStableMarker(int fd) {
    for (int attempt = 0; attempt < 8; ++attempt) {
        SharedMemoryMarkerRecord snapshot;
        const ssize_t count = ::pread(fd, &snapshot, sizeof(snapshot), 0);
        if (count != static_cast<ssize_t>(sizeof(snapshot))) {
            if (count < 0) return ErrnoStatus("pread(marker) failed");
            return Status::Error(StatusCode::kWouldBlock,
                                 "marker initialization is incomplete");
        }
        uint64_t published_after = 0;
        if (::pread(fd, &published_after, sizeof(published_after), 0) !=
            static_cast<ssize_t>(sizeof(published_after))) {
            return ErrnoStatus("pread(marker publication) failed");
        }
        if (snapshot.published_word != published_after) {
            std::this_thread::yield();
            continue;
        }
        const auto& slot = snapshot.slots[snapshot.published_word & 1u];
        if (!ValidMarkerSlot(slot)) {
            return Status::Error(StatusCode::kCorruption,
                                 "published marker slot is invalid");
        }
        return slot.payload;
    }
    return Status::Error(StatusCode::kWouldBlock,
                         "shared-memory marker is being published");
}

Result<SharedMemoryMarkerPayload> ReadMarkerUnderLock(
    const MarkerMapping& marker) {
    const SharedMemoryMarkerRecord& record = *marker.record;
    const size_t active = record.published_word & 1u;
    if (ValidMarkerSlot(record.slots[active])) {
        return record.slots[active].payload;
    }
    if (ValidMarkerSlot(record.slots[active ^ 1u])) {
        return record.slots[active ^ 1u].payload;
    }
    // A zero/short initial marker cannot have created a backing because Create
    // writes and fsyncs a valid CREATING record before any backing operation.
    struct stat st;
    if (::fstat(marker.fd, &st) == 0 &&
        (st.st_size < static_cast<off_t>(sizeof(SharedMemoryMarkerRecord)) ||
         (record.slots[0].payload.magic == 0 &&
          record.slots[1].payload.magic == 0))) {
        return Status::Error(StatusCode::kNotFound,
                             "incomplete initial marker");
    }
    return Status::Error(StatusCode::kCorruption,
                         "unrecoverable shared-memory marker");
}

Result<void*> MapDataFd(int fd, uint64_t size, bool read_only,
                        int extra_flags) {
    const int prot = read_only ? PROT_READ : (PROT_READ | PROT_WRITE);
    void* mapping = ::mmap(nullptr, size, prot, MAP_SHARED | extra_flags, fd, 0);
    if (mapping == MAP_FAILED) return ErrnoStatus("mmap(data) failed");
    return mapping;
}

uint64_t RandomToken() {
    static std::atomic<uint64_t> sequence{0};
    std::random_device random;
    return (static_cast<uint64_t>(random()) << 32) ^ random() ^
           (static_cast<uint64_t>(::getpid()) << 16) ^
           sequence.fetch_add(1, std::memory_order_relaxed);
}

std::string UniquePosixDataName() {
    char name[32];
    std::snprintf(name, sizeof(name), "/mino-d-%016llx",
                  static_cast<unsigned long long>(RandomToken()));
    return name;
}

#if MINO_HAS_HUGETLB
std::string UniqueHugeBasename() {
    char name[32];
    std::snprintf(name, sizeof(name), ".mino-h-%016llx",
                  static_cast<unsigned long long>(RandomToken()));
    return name;
}
#endif  // MINO_HAS_HUGETLB

Status VerifyDataIdentity(int fd, const SharedMemoryMarkerPayload& payload,
                          std::string_view kind, bool require_size = true) {
    struct stat st;
    if (::fstat(fd, &st) != 0) return ErrnoStatus("fstat(backing) failed");
    if ((require_size &&
         st.st_size != static_cast<off_t>(payload.data_size)) ||
        static_cast<uint64_t>(st.st_dev) != payload.backing_device ||
        static_cast<uint64_t>(st.st_ino) != payload.backing_inode) {
        return Status::Error(StatusCode::kCorruption,
                             std::string(kind) + " backing identity mismatch");
    }
    return Status::Ok();
}

#if MINO_HAS_HUGETLB
struct HugeMountIdentity {
    std::string path;
    uint64_t device = 0;
    uint64_t page_size = 0;
};

Result<HugeMountIdentity> ResolveHugeMount(const std::string& configured) {
    char* resolved = ::realpath(configured.c_str(), nullptr);
    if (resolved == nullptr) return ErrnoStatus("realpath(hugetlbfs) failed");
    HugeMountIdentity identity;
    identity.path = resolved;
    std::free(resolved);
    struct stat st;
    struct statfs fs;
    if (::stat(identity.path.c_str(), &st) != 0) {
        return ErrnoStatus("stat(hugetlbfs) failed");
    }
    if (::statfs(identity.path.c_str(), &fs) != 0) {
        return ErrnoStatus("statfs(hugetlbfs) failed");
    }
    if (static_cast<unsigned long>(fs.f_type) !=
        static_cast<unsigned long>(HUGETLBFS_MAGIC)) {
        return Status::Error(StatusCode::kUnsupported,
                             "configured path is not hugetlbfs");
    }
    identity.device = static_cast<uint64_t>(st.st_dev);
    identity.page_size = fs.f_bsize > 0 ? static_cast<uint64_t>(fs.f_bsize)
                                        : DefaultHugePageSize();
    return identity;
}

Result<std::pair<int, std::string>> OpenRecordedHugeDirectory(
    const SharedMemoryMarkerPayload& payload) {
    MINO_ASSIGN_OR_RETURN(std::string mount,
                          ReadMarkerString(payload.mount_path,
                                           sizeof(payload.mount_path),
                                           "mount_path"));
    MINO_ASSIGN_OR_RETURN(std::string backing,
                          ReadMarkerString(payload.backing_name,
                                           sizeof(payload.backing_name),
                                           "backing_name"));
    const std::string prefix = mount + "/";
    if (backing.rfind(prefix, 0) != 0) {
        return Status::Error(StatusCode::kCorruption,
                             "huge backing is outside recorded mount");
    }
    std::string basename = backing.substr(prefix.size());
    if (basename.empty() || basename.find('/') != std::string::npos) {
        return Status::Error(StatusCode::kCorruption,
                             "invalid huge backing basename");
    }
    int directory_fd = ::open(mount.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory_fd < 0) return ErrnoStatus("open(recorded hugetlbfs) failed");
    struct stat st;
    struct statfs fs;
    if (::fstat(directory_fd, &st) != 0 || ::fstatfs(directory_fd, &fs) != 0) {
        const int saved_errno = errno;
        ::close(directory_fd);
        return ErrnoStatus("validate recorded hugetlbfs failed", saved_errno);
    }
    if (static_cast<uint64_t>(st.st_dev) != payload.mount_device ||
        static_cast<unsigned long>(fs.f_type) !=
            static_cast<unsigned long>(HUGETLBFS_MAGIC) ||
        (fs.f_bsize > 0 &&
         static_cast<uint64_t>(fs.f_bsize) != payload.page_size)) {
        ::close(directory_fd);
        return Status::Error(StatusCode::kCorruption,
                             "recorded hugetlbfs identity mismatch");
    }
    return std::make_pair(directory_fd, std::move(basename));
}
#endif

Status AdoptMissingBackingIdentity(SharedMemoryMarkerPayload* payload) {
    if (payload->backing_inode != 0) return Status::Ok();
    const auto kind = static_cast<MarkerBackingKind>(payload->backing_kind);
    if (kind == MarkerBackingKind::kNone) return Status::Ok();
    if (kind == MarkerBackingKind::kPosixData) {
        MINO_ASSIGN_OR_RETURN(std::string data_name,
                              ReadMarkerString(payload->backing_name,
                                               sizeof(payload->backing_name),
                                               "backing_name"));
        if (data_name.rfind("/mino-d-", 0) != 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "unsafe fallback candidate name");
        }
        int fd = ::shm_open(data_name.c_str(), O_RDWR, 0);
        if (fd < 0) {
            if (errno == ENOENT) return Status::Ok();
            return ErrnoStatus("open fallback candidate failed");
        }
        struct stat st;
        const int stat_result = ::fstat(fd, &st);
        const int saved_errno = errno;
        ::close(fd);
        if (stat_result != 0) return ErrnoStatus("fstat fallback candidate failed", saved_errno);
        payload->backing_device = static_cast<uint64_t>(st.st_dev);
        payload->backing_inode = static_cast<uint64_t>(st.st_ino);
        if (st.st_size == static_cast<off_t>(payload->data_size)) {
            payload->flags |=
                shared_memory_internal::kMarkerFlagBackingSizeCommitted;
        }
        return Status::Ok();
    }
#if MINO_HAS_HUGETLB
    if (kind == MarkerBackingKind::kHugeFile) {
        MINO_ASSIGN_OR_RETURN(auto directory,
                              OpenRecordedHugeDirectory(*payload));
        int fd = ::openat(directory.first, directory.second.c_str(),
                          O_RDWR | O_CLOEXEC | O_NOFOLLOW);
        if (fd < 0) {
            const int saved_errno = errno;
            ::close(directory.first);
            if (saved_errno == ENOENT) return Status::Ok();
            return ErrnoStatus("open huge candidate failed", saved_errno);
        }
        struct stat st;
        const int stat_result = ::fstat(fd, &st);
        const int saved_errno = errno;
        ::close(fd);
        ::close(directory.first);
        if (stat_result != 0) return ErrnoStatus("fstat huge candidate failed", saved_errno);
        if (static_cast<uint64_t>(st.st_dev) != payload->mount_device) {
            return Status::Error(StatusCode::kCorruption,
                                 "huge candidate device mismatch");
        }
        payload->backing_device = static_cast<uint64_t>(st.st_dev);
        payload->backing_inode = static_cast<uint64_t>(st.st_ino);
        if (st.st_size == static_cast<off_t>(payload->data_size)) {
            payload->flags |=
                shared_memory_internal::kMarkerFlagBackingSizeCommitted;
        }
        return Status::Ok();
    }
#endif
    return Status::Error(StatusCode::kUnsupported,
                         "huge backing unsupported on this platform");
}

Status RemoveRecordedBacking(const SharedMemoryMarkerPayload& payload) {
    const auto kind = static_cast<MarkerBackingKind>(payload.backing_kind);
    if (kind == MarkerBackingKind::kNone || payload.backing_inode == 0) {
        return Status::Ok();
    }
    if (kind == MarkerBackingKind::kPosixData) {
        MINO_ASSIGN_OR_RETURN(std::string data_name,
                              ReadMarkerString(payload.backing_name,
                                               sizeof(payload.backing_name),
                                               "backing_name"));
        int fd = ::shm_open(data_name.c_str(), O_RDWR, 0);
        if (fd < 0) {
            if (errno == ENOENT) return Status::Ok();
            return ErrnoStatus("open fallback for unlink failed");
        }
        Status identity = VerifyDataIdentity(
            fd, payload, "fallback",
            (payload.flags &
             shared_memory_internal::kMarkerFlagBackingSizeCommitted) != 0);
        ::close(fd);
        if (!identity.ok()) return identity;
        if (::shm_unlink(data_name.c_str()) != 0 && errno != ENOENT) {
            return ErrnoStatus("shm_unlink(fallback data) failed");
        }
        return Status::Ok();
    }
#if MINO_HAS_HUGETLB
    if (kind == MarkerBackingKind::kHugeFile) {
        MINO_ASSIGN_OR_RETURN(auto directory,
                              OpenRecordedHugeDirectory(payload));
        int fd = ::openat(directory.first, directory.second.c_str(),
                          O_RDWR | O_CLOEXEC | O_NOFOLLOW);
        if (fd < 0) {
            const int saved_errno = errno;
            ::close(directory.first);
            if (saved_errno == ENOENT) return Status::Ok();
            return ErrnoStatus("open huge backing for unlink failed", saved_errno);
        }
        Status identity = VerifyDataIdentity(
            fd, payload, "huge",
            (payload.flags &
             shared_memory_internal::kMarkerFlagBackingSizeCommitted) != 0);
        ::close(fd);
        if (!identity.ok()) {
            ::close(directory.first);
            return identity;
        }
        if (::unlinkat(directory.first, directory.second.c_str(), 0) != 0 &&
            errno != ENOENT) {
            const int saved_errno = errno;
            ::close(directory.first);
            return ErrnoStatus("unlinkat(huge backing) failed", saved_errno);
        }
        ::close(directory.first);
        return Status::Ok();
    }
#endif
    return Status::Error(StatusCode::kUnsupported,
                         "recorded huge backing unsupported");
}

Status PublishUnlinking(MarkerMapping& marker,
                         SharedMemoryMarkerPayload* payload) {
    MINO_RETURN_IF_ERROR(AdoptMissingBackingIdentity(payload));
    payload->state = static_cast<uint32_t>(MarkerState::kUnlinking);
    payload->creator = ProcessIdentity::Current();
    MINO_RETURN_IF_ERROR(PublishMarker(marker.record, *payload));
    RunTestHook(SharedMemoryTestPoint::kAfterUnlinkingPublished);
    return Status::Ok();
}

Status FinishUnlinkLocked(const std::string& marker_name,
                          MarkerMapping& marker,
                          SharedMemoryMarkerPayload payload,
                          bool publish_unlinking) {
    if (publish_unlinking) {
        MINO_RETURN_IF_ERROR(PublishUnlinking(marker, &payload));
    } else {
        MINO_RETURN_IF_ERROR(AdoptMissingBackingIdentity(&payload));
        MINO_RETURN_IF_ERROR(PublishMarker(marker.record, payload));
    }
    MINO_RETURN_IF_ERROR(RemoveRecordedBacking(payload));
    if (::shm_unlink(marker_name.c_str()) != 0 && errno != ENOENT) {
        return ErrnoStatus("shm_unlink(marker) failed");
    }
    return Status::Ok();
}

class CreationCleanupGuard {
public:
    CreationCleanupGuard(std::string marker_name, MarkerMapping* marker)
        : marker_name_(std::move(marker_name)), marker_(marker) {}
    CreationCleanupGuard(const CreationCleanupGuard&) = delete;
    CreationCleanupGuard& operator=(const CreationCleanupGuard&) = delete;
    ~CreationCleanupGuard() {
        if (committed_ || marker_ == nullptr) return;
        auto snapshot = ReadMarkerUnderLock(*marker_);
        if (!snapshot.ok()) return;
        const auto state = static_cast<MarkerState>(snapshot->state);
        (void)FinishUnlinkLocked(marker_name_, *marker_, *snapshot,
                                 /*publish_unlinking=*/
                                     state != MarkerState::kUnlinking);
    }
    void Commit() noexcept { committed_ = true; }

private:
    std::string marker_name_;
    MarkerMapping* marker_;
    bool committed_ = false;
};

Status RecoverExistingMarkerForCreate(const std::string& name) {
    int fd = ::shm_open(name.c_str(), O_RDWR, 0);
    if (fd < 0) return ErrnoStatus("open existing marker failed");
    Status lock = LockMarkerExclusive(fd, /*already_exists=*/true);
    if (!lock.ok()) {
        ::close(fd);
        return lock;
    }
    struct stat st;
    if (::fstat(fd, &st) != 0) {
        Status status = ErrnoStatus("fstat existing marker failed");
        ::close(fd);
        return status;
    }
    if (st.st_size < static_cast<off_t>(sizeof(SharedMemoryMarkerRecord))) {
        ::close(fd);
        if (::shm_unlink(name.c_str()) != 0 && errno != ENOENT) {
            return ErrnoStatus("remove incomplete marker failed");
        }
        return Status::Ok();
    }
    auto mapped = MapMarkerFd(fd);
    if (!mapped.ok()) {
        ::close(fd);
        return mapped.status();
    }
    MarkerMapping marker = std::move(mapped).value();
    auto snapshot = ReadMarkerUnderLock(marker);
    if (!snapshot.ok()) {
        if (snapshot.status().code() == StatusCode::kNotFound) {
            if (::shm_unlink(name.c_str()) != 0 && errno != ENOENT) {
                return ErrnoStatus("remove incomplete marker failed");
            }
            return Status::Ok();
        }
        return snapshot.status();
    }
    SharedMemoryMarkerPayload payload = *snapshot;
    const auto state = static_cast<MarkerState>(payload.state);
    if (state == MarkerState::kHugeReady ||
        state == MarkerState::kFallbackReady) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "shared-memory object already exists");
    }
    if (state == MarkerState::kCreating) {
        const ProcessIdentityLiveness liveness =
            ProbeProcessIdentity(payload.creator);
        if (liveness == ProcessIdentityLiveness::kAlive) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "shared-memory creation is active");
        }
        if (liveness == ProcessIdentityLiveness::kUnknown) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "cannot prove crashed creator is dead");
        }
        return FinishUnlinkLocked(name, marker, payload,
                                  /*publish_unlinking=*/true);
    }
    return FinishUnlinkLocked(name, marker, payload,
                              /*publish_unlinking=*/false);
}

struct CreatedFallbackMapping {
    void* base = nullptr;
    uint64_t size = 0;
    uint64_t page_size = 0;
};

Result<CreatedFallbackMapping> CreateFallback(
    MarkerMapping& marker, SharedMemoryMarkerPayload payload,
    HugePageFallbackReason fallback_reason, int fallback_errno) {
    payload.backing_kind =
        static_cast<uint32_t>(MarkerBackingKind::kPosixData);
    payload.page_size = HostPageSize();
    payload.fallback_reason = static_cast<uint32_t>(fallback_reason);
    payload.fallback_errno = fallback_errno;
    payload.mount_device = 0;
    payload.backing_device = 0;
    payload.backing_inode = 0;
    payload.flags &=
        ~shared_memory_internal::kMarkerFlagBackingSizeCommitted;
    std::memset(payload.mount_path, 0, sizeof(payload.mount_path));

    int data_fd = -1;
    std::string data_name;
    for (int attempt = 0; attempt < 8; ++attempt) {
        data_name = UniquePosixDataName();
        if (!CopyMarkerString(data_name, payload.backing_name,
                              sizeof(payload.backing_name))) {
            return Status::Error(StatusCode::kInternal,
                                 "fallback data name is too long");
        }
        MINO_RETURN_IF_ERROR(PublishMarker(marker.record, payload));
        data_fd = ::shm_open(data_name.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
        if (data_fd >= 0 || errno != EEXIST) break;
    }
    if (data_fd < 0) return ErrnoStatus("shm_open(fallback data) failed");
    (void)::fcntl(data_fd, F_SETFD, FD_CLOEXEC);
    if (::ftruncate(data_fd, static_cast<off_t>(payload.data_size)) != 0) {
        const int saved_errno = errno;
        ::close(data_fd);
        return ErrnoStatus("ftruncate(fallback data) failed", saved_errno);
    }
    struct stat st;
    if (::fstat(data_fd, &st) != 0) {
        const int saved_errno = errno;
        ::close(data_fd);
        return ErrnoStatus("fstat(fallback data) failed", saved_errno);
    }
    payload.backing_device = static_cast<uint64_t>(st.st_dev);
    payload.backing_inode = static_cast<uint64_t>(st.st_ino);
    payload.flags |=
        shared_memory_internal::kMarkerFlagBackingSizeCommitted;
    MINO_RETURN_IF_ERROR(PublishMarker(marker.record, payload));
    RunTestHook(SharedMemoryTestPoint::kAfterBackingIdentityRecorded);

    auto mapping = MapDataFd(data_fd, payload.data_size,
                             /*read_only=*/false, /*extra_flags=*/0);
    ::close(data_fd);
    if (!mapping.ok()) return mapping.status();
    payload.state = static_cast<uint32_t>(MarkerState::kFallbackReady);
    Status published = PublishMarker(marker.record, payload);
    if (!published.ok()) {
        (void)::munmap(mapping.value(), payload.data_size);
        return published;
    }

    return CreatedFallbackMapping{
        .base = mapping.value(),
        .size = payload.data_size,
        .page_size = payload.page_size,
    };
}

#endif  // MINO_HAS_POSIX

}  // namespace

namespace shared_memory_internal {
void SetSharedMemoryTestHook(SharedMemoryTestHook hook) noexcept {
    g_test_hook.store(hook, std::memory_order_release);
}
}  // namespace shared_memory_internal

const char* HugePageFallbackReasonName(
    HugePageFallbackReason reason) noexcept {
    switch (reason) {
        case HugePageFallbackReason::kNone:
            return "none";
        case HugePageFallbackReason::kUnsupportedPlatform:
            return "unsupported-platform";
        case HugePageFallbackReason::kHugetlbfsUnavailable:
            return "hugetlbfs-unavailable";
        case HugePageFallbackReason::kInsufficientHugePages:
            return "insufficient-hugepages";
        case HugePageFallbackReason::kPermissionDenied:
            return "permission-denied";
        case HugePageFallbackReason::kUnsupportedBacking:
            return "unsupported-backing";
        case HugePageFallbackReason::kSystemError:
            return "system-error";
    }
    return "unknown";
}

SharedMemorySegment::SharedMemorySegment(SharedMemorySegment&& other) noexcept {
    *this = std::move(other);
}

SharedMemorySegment& SharedMemorySegment::operator=(
    SharedMemorySegment&& other) noexcept {
    if (this != &other) {
        (void)Close();
        base_ = other.base_;
        size_ = other.size_;
        read_only_ = other.read_only_;
        huge_pages_requested_ = other.huge_pages_requested_;
        huge_pages_actual_ = other.huge_pages_actual_;
        actual_page_size_ = other.actual_page_size_;
        huge_page_fallback_reason_ = other.huge_page_fallback_reason_;
        huge_page_fallback_errno_ = other.huge_page_fallback_errno_;
        name_ = std::move(other.name_);
        other.base_ = nullptr;
        other.size_ = 0;
        other.huge_pages_requested_ = false;
        other.huge_pages_actual_ = false;
        other.actual_page_size_ = 0;
        other.huge_page_fallback_reason_ = HugePageFallbackReason::kNone;
        other.huge_page_fallback_errno_ = 0;
    }
    return *this;
}

SharedMemorySegment::~SharedMemorySegment() { (void)Close(); }

Result<SharedMemorySegment> SharedMemorySegment::Create(
    const SharedMemoryCreateOptions& options) {
    MINO_RETURN_IF_ERROR(ValidateName(options.name));
    if (options.size == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm size must be > 0");
    }
#if !MINO_HAS_POSIX
    return Status::Error(StatusCode::kUnsupported,
                         "POSIX shared memory is unsupported");
#else
    uint64_t huge_page_size = DefaultHugePageSize();
    HugePageFallbackReason fallback_reason = HugePageFallbackReason::kNone;
    int fallback_errno = 0;
#if MINO_HAS_HUGETLB
    Result<HugeMountIdentity> huge_mount = Status::Error(
        StatusCode::kUnsupported, "huge pages were not requested");
    if (options.use_huge_pages) {
        huge_mount = ResolveHugeMount(
            ConfiguredHugePageDirectory(options.hugetlbfs_path));
        if (huge_mount.ok()) {
            huge_page_size = huge_mount->page_size;
        } else {
            fallback_errno = StatusFallbackErrno(huge_mount.status());
            fallback_reason = ClassifyHugePageFailure(fallback_errno);
        }
    }
#else
    if (options.use_huge_pages) {
        fallback_reason = HugePageFallbackReason::kUnsupportedPlatform;
    }
#endif
    MINO_ASSIGN_OR_RETURN(uint64_t data_size,
                          CheckedRoundUp(options.size,
                                         options.use_huge_pages
                                             ? huge_page_size
                                             : HostPageSize()));
    if (data_size > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shared-memory size exceeds off_t");
    }

    for (int creation_attempt = 0; creation_attempt < 2; ++creation_attempt) {
        int marker_fd = ::shm_open(options.name.c_str(),
                                   O_RDWR | O_CREAT | O_EXCL, 0600);
        if (marker_fd < 0) {
            if (errno != EEXIST) return ErrnoStatus("create marker failed");
            Status recovered = RecoverExistingMarkerForCreate(options.name);
            if (!recovered.ok()) return recovered;
            continue;
        }
        (void)::fcntl(marker_fd, F_SETFD, FD_CLOEXEC);
        if (::ftruncate(marker_fd,
                        static_cast<off_t>(sizeof(SharedMemoryMarkerRecord))) != 0) {
            Status status = ErrnoStatus("ftruncate(marker) failed");
            ::close(marker_fd);
            (void)::shm_unlink(options.name.c_str());
            return status;
        }
        Status lock = LockMarkerExclusive(marker_fd, /*already_exists=*/false);
        if (!lock.ok()) {
            ::close(marker_fd);
            (void)::shm_unlink(options.name.c_str());
            return lock;
        }
        SharedMemoryMarkerPayload payload;
        payload.state = static_cast<uint32_t>(MarkerState::kCreating);
        payload.flags = options.use_huge_pages
                            ? shared_memory_internal::kMarkerFlagHugeRequested
                            : 0;
        payload.data_size = data_size;
        payload.page_size = options.use_huge_pages ? huge_page_size
                                                   : HostPageSize();
        payload.creator = ProcessIdentity::Current();
        Status initial = WriteInitialMarker(marker_fd, payload);
        if (!initial.ok()) {
            ::close(marker_fd);
            (void)::shm_unlink(options.name.c_str());
            return initial;
        }
        auto mapped = MapMarkerFd(marker_fd);
        if (!mapped.ok()) {
            ::close(marker_fd);
            return mapped.status();
        }
        MarkerMapping marker = std::move(mapped).value();
        CreationCleanupGuard cleanup(options.name, &marker);
        RunTestHook(SharedMemoryTestPoint::kAfterCreatingMarker);

#if MINO_HAS_HUGETLB
        if (options.use_huge_pages && huge_mount.ok()) {
            const std::string basename = UniqueHugeBasename();
            const std::string backing_path = huge_mount->path + "/" + basename;
            payload.backing_kind =
                static_cast<uint32_t>(MarkerBackingKind::kHugeFile);
            payload.mount_device = huge_mount->device;
            payload.page_size = huge_mount->page_size;
            if (!CopyMarkerString(huge_mount->path, payload.mount_path,
                                  sizeof(payload.mount_path)) ||
                !CopyMarkerString(backing_path, payload.backing_name,
                                  sizeof(payload.backing_name))) {
                fallback_reason = HugePageFallbackReason::kUnsupportedBacking;
                fallback_errno = ENAMETOOLONG;
            } else {
                MINO_RETURN_IF_ERROR(PublishMarker(marker.record, payload));
                int huge_fd = ::open(backing_path.c_str(),
                                     O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC |
                                         O_NOFOLLOW,
                                     0600);
                if (huge_fd >= 0 &&
                    ::ftruncate(huge_fd, static_cast<off_t>(data_size)) == 0) {
                    struct stat st;
                    if (::fstat(huge_fd, &st) == 0) {
                        payload.backing_device =
                            static_cast<uint64_t>(st.st_dev);
                        payload.backing_inode =
                            static_cast<uint64_t>(st.st_ino);
                        payload.flags |= shared_memory_internal::
                            kMarkerFlagBackingSizeCommitted;
                        MINO_RETURN_IF_ERROR(
                            PublishMarker(marker.record, payload));
                        RunTestHook(
                            SharedMemoryTestPoint::kAfterBackingIdentityRecorded);
                        auto mapping = MapDataFd(huge_fd, data_size,
                                                 /*read_only=*/false,
                                                 MAP_HUGETLB);
                        if (mapping.ok()) {
                            payload.state = static_cast<uint32_t>(
                                MarkerState::kHugeReady);
                            payload.fallback_reason = 0;
                            payload.fallback_errno = 0;
                            Status ready = PublishMarker(marker.record, payload);
                            ::close(huge_fd);
                            if (!ready.ok()) {
                                (void)::munmap(mapping.value(), data_size);
                                return ready;
                            }
                            cleanup.Commit();
                            SharedMemorySegment segment;
                            segment.base_ = mapping.value();
                            segment.size_ = data_size;
                            segment.read_only_ = false;
                            segment.huge_pages_requested_ = true;
                            segment.huge_pages_actual_ = true;
                            segment.actual_page_size_ = payload.page_size;
                            segment.name_ = options.name;
                            return segment;
                        }
                        fallback_errno = errno;
                    } else {
                        fallback_errno = errno;
                    }
                } else {
                    fallback_errno = errno;
                }
                if (huge_fd >= 0) ::close(huge_fd);
                fallback_reason = ClassifyHugePageFailure(fallback_errno);
                MINO_RETURN_IF_ERROR(AdoptMissingBackingIdentity(&payload));
                MINO_RETURN_IF_ERROR(RemoveRecordedBacking(payload));
            }
        }
#endif
        MINO_ASSIGN_OR_RETURN(
            CreatedFallbackMapping fallback,
            CreateFallback(marker, payload, fallback_reason, fallback_errno));
        cleanup.Commit();
        SharedMemorySegment segment;
        segment.base_ = fallback.base;
        segment.size_ = fallback.size;
        segment.read_only_ = false;
        segment.huge_pages_requested_ = options.use_huge_pages;
        segment.huge_pages_actual_ = false;
        segment.actual_page_size_ = fallback.page_size;
        segment.huge_page_fallback_reason_ = fallback_reason;
        segment.huge_page_fallback_errno_ = fallback_errno;
        segment.name_ = options.name;
        return segment;
    }
    return Status::Error(StatusCode::kAlreadyExists,
                         "shared-memory marker recovery did not converge");
#endif
}

Result<SharedMemorySegment> SharedMemorySegment::Create(
    const std::string& name, uint64_t size) {
    SharedMemoryCreateOptions options;
    options.name = name;
    options.size = size;
    return Create(options);
}

Result<SharedMemorySegment> SharedMemorySegment::Open(
    const std::string& name, bool read_only) {
    SharedMemoryOpenOptions options;
    options.name = name;
    options.read_only = read_only;
    return Open(options);
}

Result<SharedMemorySegment> SharedMemorySegment::Open(
    const SharedMemoryOpenOptions& options) {
    MINO_RETURN_IF_ERROR(ValidateName(options.name));
#if !MINO_HAS_POSIX
    return Status::Error(StatusCode::kUnsupported,
                         "POSIX shared memory is unsupported");
#else
    int marker_fd = ::shm_open(options.name.c_str(), O_RDONLY, 0);
    if (marker_fd < 0) return ErrnoStatus("open marker failed");
    (void)::fcntl(marker_fd, F_SETFD, FD_CLOEXEC);
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(
                              options.creating_wait_timeout_ms);
    SharedMemoryMarkerPayload payload;
    for (;;) {
        auto snapshot = ReadStableMarker(marker_fd);
        if (snapshot.ok()) {
            payload = *snapshot;
            const auto state = static_cast<MarkerState>(payload.state);
            if (state == MarkerState::kHugeReady ||
                state == MarkerState::kFallbackReady) {
                break;
            }
            if (state == MarkerState::kUnlinking) {
                ::close(marker_fd);
                return Status::Error(StatusCode::kWouldBlock,
                                     "shared-memory object is unlinking");
            }
        } else if (snapshot.status().code() != StatusCode::kWouldBlock) {
            ::close(marker_fd);
            return snapshot.status();
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            ::close(marker_fd);
            return Status::Error(StatusCode::kWouldBlock,
                                 "shared-memory creation is incomplete");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ::close(marker_fd);

    const bool read_only = options.read_only;
    const int open_flags = read_only ? O_RDONLY : O_RDWR;
    int data_fd = -1;
    int map_flags = 0;
    const auto kind = static_cast<MarkerBackingKind>(payload.backing_kind);
    if (payload.backing_inode == 0 ||
        (payload.flags &
         shared_memory_internal::kMarkerFlagBackingSizeCommitted) == 0) {
        return Status::Error(StatusCode::kCorruption,
                             "ready marker has incomplete backing identity");
    }
    if (kind == MarkerBackingKind::kPosixData) {
        MINO_ASSIGN_OR_RETURN(std::string data_name,
                              ReadMarkerString(payload.backing_name,
                                               sizeof(payload.backing_name),
                                               "backing_name"));
        data_fd = ::shm_open(data_name.c_str(), open_flags, 0);
        if (data_fd < 0) return ErrnoStatus("open fallback data failed");
    } else if (kind == MarkerBackingKind::kHugeFile) {
#if MINO_HAS_HUGETLB
        MINO_ASSIGN_OR_RETURN(auto directory,
                              OpenRecordedHugeDirectory(payload));
        data_fd = ::openat(directory.first, directory.second.c_str(),
                           open_flags | O_CLOEXEC | O_NOFOLLOW);
        const int saved_errno = errno;
        ::close(directory.first);
        if (data_fd < 0) return ErrnoStatus("open huge data failed", saved_errno);
        map_flags = MAP_HUGETLB;
#else
        return Status::Error(StatusCode::kUnsupported,
                             "recorded huge backing is unsupported");
#endif
    } else {
        return Status::Error(StatusCode::kCorruption,
                             "ready marker has no data backing");
    }
    (void)::fcntl(data_fd, F_SETFD, FD_CLOEXEC);
    Status identity = VerifyDataIdentity(
        data_fd, payload,
        kind == MarkerBackingKind::kHugeFile ? "huge" : "fallback");
    if (!identity.ok()) {
        ::close(data_fd);
        return identity;
    }
    auto mapping = MapDataFd(data_fd, payload.data_size, read_only, map_flags);
    ::close(data_fd);
    if (!mapping.ok()) return mapping.status();

    SharedMemorySegment segment;
    segment.base_ = mapping.value();
    segment.size_ = payload.data_size;
    segment.read_only_ = read_only;
    segment.huge_pages_requested_ =
        (payload.flags &
         shared_memory_internal::kMarkerFlagHugeRequested) != 0;
    segment.huge_pages_actual_ = kind == MarkerBackingKind::kHugeFile;
    segment.actual_page_size_ = payload.page_size;
    if (payload.fallback_reason >
        static_cast<uint32_t>(HugePageFallbackReason::kSystemError)) {
        (void)::munmap(segment.base_, segment.size_);
        return Status::Error(StatusCode::kCorruption,
                             "invalid huge-page fallback reason");
    }
    segment.huge_page_fallback_reason_ =
        static_cast<HugePageFallbackReason>(payload.fallback_reason);
    segment.huge_page_fallback_errno_ = payload.fallback_errno;
    segment.name_ = options.name;
    return segment;
#endif
}

Status SharedMemorySegment::Close() {
    if (base_ == nullptr) return Status::Ok();
#if MINO_HAS_POSIX
    if (::munmap(base_, size_) != 0) {
        Status status = ErrnoStatus("munmap(data) failed");
        base_ = nullptr;
        size_ = 0;
        return status;
    }
#endif
    base_ = nullptr;
    size_ = 0;
    huge_pages_requested_ = false;
    huge_pages_actual_ = false;
    actual_page_size_ = 0;
    huge_page_fallback_reason_ = HugePageFallbackReason::kNone;
    huge_page_fallback_errno_ = 0;
    return Status::Ok();
}

Status SharedMemorySegment::Unlink(const std::string& name) {
    MINO_RETURN_IF_ERROR(ValidateName(name));
#if !MINO_HAS_POSIX
    return Status::Error(StatusCode::kUnsupported,
                         "POSIX shared memory is unsupported");
#else
    int fd = ::shm_open(name.c_str(), O_RDWR, 0);
    if (fd < 0) return ErrnoStatus("open marker for unlink failed");
    Status lock = LockMarkerExclusive(fd, /*already_exists=*/false);
    if (!lock.ok()) {
        ::close(fd);
        return lock;
    }
    struct stat st;
    if (::fstat(fd, &st) != 0 ||
        st.st_size < static_cast<off_t>(sizeof(SharedMemoryMarkerRecord))) {
        const int saved_errno = errno;
        ::close(fd);
        return saved_errno != 0
                   ? ErrnoStatus("validate marker for unlink failed", saved_errno)
                   : Status::Error(StatusCode::kCorruption,
                                   "marker is too small");
    }
    auto mapped = MapMarkerFd(fd);
    if (!mapped.ok()) {
        ::close(fd);
        return mapped.status();
    }
    MarkerMapping marker = std::move(mapped).value();
    auto snapshot = ReadMarkerUnderLock(marker);
    if (!snapshot.ok()) return snapshot.status();
    SharedMemoryMarkerPayload payload = *snapshot;
    const auto state = static_cast<MarkerState>(payload.state);
    if (state == MarkerState::kCreating) {
        const ProcessIdentityLiveness liveness =
            ProbeProcessIdentity(payload.creator);
        if (liveness != ProcessIdentityLiveness::kDead) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "shared-memory creation is active or unverifiable");
        }
    }
    return FinishUnlinkLocked(
        name, marker, payload,
        /*publish_unlinking=*/state != MarkerState::kUnlinking);
#endif
}

}  // namespace mino
