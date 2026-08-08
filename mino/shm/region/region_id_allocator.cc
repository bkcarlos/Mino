// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/region_id_allocator.h"

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "mino/common/status.h"

namespace mino::region_internal {
namespace {

constexpr uint64_t kExhaustedRegionIdHwm =
    static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;

Status ErrnoStatus(std::string_view operation, const std::string& path,
                   int error_number = errno) {
    StatusCode code = StatusCode::kInternal;
    if (error_number == EACCES || error_number == EPERM ||
        error_number == EROFS) {
        code = StatusCode::kPermissionDenied;
    } else if (error_number == ENOSPC || error_number == EDQUOT) {
        code = StatusCode::kResourceExhausted;
    } else if (error_number == ENOENT) {
        code = StatusCode::kNotFound;
    }
    return Status::Error(code, std::string(operation) + " for Region ID HWM '" +
                                   path + "': " + std::strerror(error_number));
}

#if defined(__unix__) || defined(__APPLE__)
class ScopedFd {
public:
    explicit ScopedFd(int fd = -1) : fd_(fd) {}
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) (void)::close(fd_);
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }
    ~ScopedFd() {
        if (fd_ >= 0) (void)::close(fd_);
    }

    int get() const { return fd_; }

private:
    int fd_;
};

int CloseOnExecFlag() noexcept {
#ifdef O_CLOEXEC
    return O_CLOEXEC;
#else
    return 0;
#endif
}

Status SetCloseOnExec(int fd, const std::string& path) {
#ifdef O_CLOEXEC
    (void)fd;
    (void)path;
    return Status::Ok();
#else
    const int flags = ::fcntl(fd, F_GETFD);
    if (flags < 0 || ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0) {
        return ErrnoStatus("fcntl(FD_CLOEXEC) failed", path);
    }
    return Status::Ok();
#endif
}

Status LockExclusive(int fd, const std::string& path) {
    while (::flock(fd, LOCK_EX) != 0) {
        if (errno == EINTR) continue;
        return ErrnoStatus("flock(LOCK_EX) failed", path);
    }
    return Status::Ok();
}

Status ReadExact(int fd, void* value, size_t size, const std::string& path) {
    auto* bytes = static_cast<unsigned char*>(value);
    size_t consumed = 0;
    while (consumed < size) {
        const ssize_t count =
            ::pread(fd, bytes + consumed, size - consumed,
                    static_cast<off_t>(consumed));
        if (count < 0) {
            if (errno == EINTR) continue;
            return ErrnoStatus("pread failed", path);
        }
        if (count == 0) {
            return Status::Error(StatusCode::kCorruption,
                                 "short read from Region ID HWM '" + path + "'");
        }
        consumed += static_cast<size_t>(count);
    }
    return Status::Ok();
}

Status WriteExact(int fd, const void* value, size_t size,
                  const std::string& path) {
    const auto* bytes = static_cast<const unsigned char*>(value);
    size_t consumed = 0;
    while (consumed < size) {
        const ssize_t count =
            ::pwrite(fd, bytes + consumed, size - consumed,
                     static_cast<off_t>(consumed));
        if (count < 0) {
            if (errno == EINTR) continue;
            return ErrnoStatus("pwrite failed", path);
        }
        if (count == 0) {
            return Status::Error(StatusCode::kInternal,
                                 "short write to Region ID HWM '" + path + "'");
        }
        consumed += static_cast<size_t>(count);
    }
    return Status::Ok();
}

Status SyncFile(int fd, const std::string& path) {
    for (;;) {
#if defined(__APPLE__)
        const int result = ::fsync(fd);
#else
        int result = ::fdatasync(fd);
        if (result != 0 && (errno == EINVAL || errno == ENOSYS)) {
            result = ::fsync(fd);
        }
#endif
        if (result == 0) return Status::Ok();
        if (errno == EINTR) continue;
        return ErrnoStatus("fdatasync/fsync failed", path);
    }
}

Status SyncDirectory(const std::filesystem::path& directory) {
    int flags = O_RDONLY | CloseOnExecFlag();
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
    const std::string path = directory.string();
    const int raw_fd = ::open(path.c_str(), flags);
    if (raw_fd < 0) return ErrnoStatus("open(state directory) failed", path);
    ScopedFd fd(raw_fd);
    MINO_RETURN_IF_ERROR(SetCloseOnExec(fd.get(), path));
    while (::fsync(fd.get()) != 0) {
        if (errno == EINTR) continue;
        return ErrnoStatus("fsync(state directory) failed", path);
    }
    return Status::Ok();
}

Result<std::filesystem::path> ResolveHwmPath(
    const RegionIdAllocatorOptions& options) {
    if (!options.hwm_path.empty()) return std::filesystem::path(options.hwm_path);
    if (const char* injected = std::getenv("MINO_REGION_ID_HWM_PATH");
        injected != nullptr && *injected != '\0') {
        return std::filesystem::path(injected);
    }
    if (const char* test_tmpdir = std::getenv("TEST_TMPDIR");
        test_tmpdir != nullptr && *test_tmpdir != '\0') {
        return std::filesystem::path(test_tmpdir) / "mino" / "region_id_hwm";
    }
    if (const char* state_home = std::getenv("XDG_STATE_HOME");
        state_home != nullptr && *state_home != '\0') {
        return std::filesystem::path(state_home) / "mino" / "region_id_hwm";
    }
    if (const char* home = std::getenv("HOME"); home != nullptr && *home != '\0') {
        return std::filesystem::path(home) / ".local" / "state" / "mino" /
               "region_id_hwm";
    }
    return std::filesystem::path("/var/tmp") /
           ("mino-" + std::to_string(static_cast<uint64_t>(::getuid()))) /
           "region_id_hwm";
}

Status EnsureStateDirectory(const std::filesystem::path& directory) {
    if (directory.empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region ID HWM has no containing directory");
    }
    std::error_code error;
    const bool existed = std::filesystem::exists(directory, error);
    if (error) {
        return Status::Error(StatusCode::kInternal,
                             "cannot inspect Region ID state directory '" +
                                 directory.string() + "': " + error.message());
    }
    if (!existed) {
        const bool created = std::filesystem::create_directories(directory, error);
        if (error || (!created && !std::filesystem::is_directory(directory, error))) {
            return Status::Error(StatusCode::kInternal,
                                 "cannot create Region ID state directory '" +
                                     directory.string() + "': " +
                                     error.message());
        }
        if (created) {
            if (::chmod(directory.c_str(), 0700) != 0) {
                return ErrnoStatus("chmod(state directory) failed",
                                   directory.string());
            }
            MINO_RETURN_IF_ERROR(SyncDirectory(directory));
            const std::filesystem::path parent = directory.parent_path();
            if (!parent.empty()) MINO_RETURN_IF_ERROR(SyncDirectory(parent));
        }
    } else if (!std::filesystem::is_directory(directory, error) || error) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region ID HWM parent is not a directory: '" +
                                 directory.string() + "'");
    }
    return Status::Ok();
}

Result<uint64_t> ReadStoredHwm(int fd, off_t size, const std::string& path) {
    uint64_t next_id = 0;
    if (size == static_cast<off_t>(sizeof(uint32_t))) {
        uint32_t legacy = 0;
        MINO_RETURN_IF_ERROR(ReadExact(fd, &legacy, sizeof(legacy), path));
        next_id = legacy;
    } else if (size == static_cast<off_t>(sizeof(uint64_t))) {
        MINO_RETURN_IF_ERROR(ReadExact(fd, &next_id, sizeof(next_id), path));
    } else {
        return Status::Error(StatusCode::kCorruption,
                             "unexpected Region ID HWM size " +
                                 std::to_string(static_cast<long long>(size)) +
                                 " for '" + path + "'");
    }
    return next_id;
}

Result<uint64_t> ReadLegacyPosixHwm(const std::string& name,
                                    ScopedFd* held_lock) {
    if (name.empty()) {
        return Status::Error(StatusCode::kNotFound, "legacy migration disabled");
    }
    const int raw_fd = ::shm_open(name.c_str(), O_RDWR | CloseOnExecFlag(), 0600);
    if (raw_fd < 0) {
        if (errno == ENOENT) {
            return Status::Error(StatusCode::kNotFound,
                                 "legacy Region ID HWM does not exist");
        }
        return ErrnoStatus("shm_open(legacy HWM) failed", name);
    }
    ScopedFd fd(raw_fd);
    MINO_RETURN_IF_ERROR(SetCloseOnExec(fd.get(), name));
    MINO_RETURN_IF_ERROR(LockExclusive(fd.get(), name));
    struct stat stat_buffer {};
    if (::fstat(fd.get(), &stat_buffer) != 0) {
        return ErrnoStatus("fstat(legacy HWM) failed", name);
    }
    if (stat_buffer.st_size == 0) {
        return Status::Error(StatusCode::kNotFound,
                             "legacy Region ID HWM is empty");
    }
    MINO_ASSIGN_OR_RETURN(const uint64_t next_id,
                          ReadStoredHwm(fd.get(), stat_buffer.st_size, name));
    *held_lock = std::move(fd);
    return next_id;
}
#endif

}  // namespace

Result<uint32_t> AllocateRegionId(const RegionIdAllocatorOptions& options) {
    if (options.first_id == 0 ||
        options.first_id > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region ID HWM first_id must be in [1, UINT32_MAX]");
    }

#if defined(__unix__) || defined(__APPLE__)
    MINO_ASSIGN_OR_RETURN(const std::filesystem::path hwm_path,
                          ResolveHwmPath(options));
    if (hwm_path.empty() || hwm_path.filename().empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region ID HWM path must name a file");
    }
    MINO_RETURN_IF_ERROR(EnsureStateDirectory(hwm_path.parent_path()));
    const std::string path = hwm_path.string();

    int open_flags = O_RDWR | O_CREAT | O_EXCL | CloseOnExecFlag();
#ifdef O_NOFOLLOW
    open_flags |= O_NOFOLLOW;
#endif
    int raw_fd = ::open(path.c_str(), open_flags, 0600);
    if (raw_fd < 0 && errno == EEXIST) {
        open_flags = O_RDWR | CloseOnExecFlag();
#ifdef O_NOFOLLOW
        open_flags |= O_NOFOLLOW;
#endif
        raw_fd = ::open(path.c_str(), open_flags);
    }
    if (raw_fd < 0) return ErrnoStatus("open failed", path);
    ScopedFd fd(raw_fd);
    MINO_RETURN_IF_ERROR(SetCloseOnExec(fd.get(), path));
    MINO_RETURN_IF_ERROR(LockExclusive(fd.get(), path));

    struct stat stat_buffer {};
    if (::fstat(fd.get(), &stat_buffer) != 0) {
        return ErrnoStatus("fstat failed", path);
    }
    if (!S_ISREG(stat_buffer.st_mode)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region ID HWM is not a regular file: '" + path + "'");
    }

    uint64_t next_id = 0;
    bool migrated_legacy = false;
    ScopedFd legacy_lock;
    if (stat_buffer.st_size == 0) {
        const Result<uint64_t> legacy =
            ReadLegacyPosixHwm(options.legacy_shm_name, &legacy_lock);
        if (legacy.ok()) {
            next_id = *legacy;
            migrated_legacy = true;
        } else if (legacy.status().code() == StatusCode::kNotFound) {
            next_id = options.first_id;
        } else {
            return legacy.status();
        }
    } else {
        MINO_ASSIGN_OR_RETURN(next_id,
                              ReadStoredHwm(fd.get(), stat_buffer.st_size, path));
    }

    if (next_id == 0) {
        return Status::Error(StatusCode::kCorruption,
                             "Region ID HWM contains reserved ID 0: '" + path + "'");
    }
    if (next_id == kExhaustedRegionIdHwm) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "Region ID HWM exhausted at UINT32_MAX: '" + path + "'");
    }
    if (next_id > kExhaustedRegionIdHwm) {
        return Status::Error(StatusCode::kCorruption,
                             "Region ID HWM is beyond exhaustion marker: '" +
                                 path + "'");
    }

    if (migrated_legacy) {
        // First make the imported legacy position durable. Only then poison the
        // old namespace while still holding its flock, so an already-open old
        // allocator fails closed instead of issuing an ID also present here.
        MINO_RETURN_IF_ERROR(WriteExact(fd.get(), &next_id, sizeof(next_id), path));
        if (::ftruncate(fd.get(), static_cast<off_t>(sizeof(next_id))) != 0) {
            return ErrnoStatus("ftruncate(imported HWM) failed", path);
        }
        MINO_RETURN_IF_ERROR(SyncFile(fd.get(), path));
        MINO_RETURN_IF_ERROR(SyncDirectory(hwm_path.parent_path()));
        const uint64_t poison = 0;
        MINO_RETURN_IF_ERROR(WriteExact(legacy_lock.get(), &poison,
                                        sizeof(poison),
                                        options.legacy_shm_name));
        if (::ftruncate(legacy_lock.get(), static_cast<off_t>(sizeof(poison))) !=
            0) {
            return ErrnoStatus("ftruncate(legacy HWM) failed",
                               options.legacy_shm_name);
        }
        MINO_RETURN_IF_ERROR(
            SyncFile(legacy_lock.get(), options.legacy_shm_name));
    }

    const uint64_t following_id = next_id + 1;
    MINO_RETURN_IF_ERROR(
        WriteExact(fd.get(), &following_id, sizeof(following_id), path));
    if (::ftruncate(fd.get(), static_cast<off_t>(sizeof(following_id))) != 0) {
        return ErrnoStatus("ftruncate failed", path);
    }
    MINO_RETURN_IF_ERROR(SyncFile(fd.get(), path));
    MINO_RETURN_IF_ERROR(SyncDirectory(hwm_path.parent_path()));

    if (migrated_legacy && !options.legacy_shm_name.empty()) {
        if (::shm_unlink(options.legacy_shm_name.c_str()) != 0 && errno != ENOENT) {
            // The durable file is already committed and the legacy object is
            // poisoned, so retaining its name fails closed rather than reusing.
        }
    }
    return static_cast<uint32_t>(next_id);
#else
    return Status::Error(StatusCode::kUnsupported,
                         "Region ID allocation requires durable POSIX file I/O");
#endif
}

}  // namespace mino::region_internal
