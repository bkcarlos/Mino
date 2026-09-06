// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/region_name_registry.h"

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <string_view>

#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino::region_internal {
namespace {

constexpr size_t kMaxPosixShmNameBytes = 255;

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
    } else if (error_number == EEXIST) {
        code = StatusCode::kAlreadyExists;
    }
    return Status::Error(code, std::string(operation) + " for Region name "
                                                       "registry '" +
                                   path + "': " + std::strerror(error_number));
}

Status ValidateRegistryName(std::string_view name) {
    if (name.empty() || name.front() != '/') {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region registry name must start with '/'");
    }
    if (name.size() < 2) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region registry name must include a token");
    }
    if (name.size() > kMaxPosixShmNameBytes) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region registry name exceeds POSIX shm limit");
    }
    if (name.find('/', 1) != std::string_view::npos) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "Region registry name must not contain additional '/'");
    }
    if (name.find('\0') != std::string_view::npos ||
        name.find('\n') != std::string_view::npos) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region registry name contains forbidden bytes");
    }
    return Status::Ok();
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
    int release() {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }

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
    if (raw_fd < 0) return ErrnoStatus("open(registry directory) failed", path);
    ScopedFd fd(raw_fd);
    MINO_RETURN_IF_ERROR(SetCloseOnExec(fd.get(), path));
    while (::fsync(fd.get()) != 0) {
        if (errno == EINTR) continue;
        return ErrnoStatus("fsync(registry directory) failed", path);
    }
    return Status::Ok();
}

Status EnsureDirectory(const std::filesystem::path& directory) {
    if (directory.empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region name registry directory is empty");
    }
    std::error_code error;
    const bool existed = std::filesystem::exists(directory, error);
    if (error) {
        return Status::Error(StatusCode::kInternal,
                             "cannot inspect Region name registry '" +
                                 directory.string() + "': " + error.message());
    }
    if (!existed) {
        const bool created =
            std::filesystem::create_directories(directory, error);
        if (error ||
            (!created && !std::filesystem::is_directory(directory, error))) {
            return Status::Error(StatusCode::kInternal,
                                 "cannot create Region name registry '" +
                                     directory.string() + "': " +
                                     error.message());
        }
        if (created) {
            if (::chmod(directory.c_str(), 0700) != 0) {
                return ErrnoStatus("chmod(registry directory) failed",
                                   directory.string());
            }
            MINO_RETURN_IF_ERROR(SyncDirectory(directory));
            const std::filesystem::path parent = directory.parent_path();
            if (!parent.empty()) MINO_RETURN_IF_ERROR(SyncDirectory(parent));
        }
    } else if (!std::filesystem::is_directory(directory, error) || error) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region name registry path is not a directory: '" +
                                 directory.string() + "'");
    }
    return Status::Ok();
}

Result<std::filesystem::path> ResolveRegistryDir(
    const RegionNameRegistryOptions& options) {
    if (!options.registry_dir.empty()) {
        return std::filesystem::path(options.registry_dir);
    }
    if (const char* injected = std::getenv("MINO_REGION_NAME_REGISTRY_DIR");
        injected != nullptr && *injected != '\0') {
        return std::filesystem::path(injected);
    }
    // Keep the name map beside the durable Region ID HWM so one TEST_TMPDIR /
    // XDG_STATE_HOME / HOME identity domain owns both.
    if (const char* hwm = std::getenv("MINO_REGION_ID_HWM_PATH");
        hwm != nullptr && *hwm != '\0') {
        return std::filesystem::path(hwm).parent_path() / "region_names";
    }
    if (const char* test_tmpdir = std::getenv("TEST_TMPDIR");
        test_tmpdir != nullptr && *test_tmpdir != '\0') {
        return std::filesystem::path(test_tmpdir) / "mino" / "region_names";
    }
    if (const char* state_home = std::getenv("XDG_STATE_HOME");
        state_home != nullptr && *state_home != '\0') {
        return std::filesystem::path(state_home) / "mino" / "region_names";
    }
    if (const char* home = std::getenv("HOME");
        home != nullptr && *home != '\0') {
        return std::filesystem::path(home) / ".local" / "state" / "mino" /
               "region_names";
    }
    return std::filesystem::path("/var/tmp") /
           ("mino-" + std::to_string(static_cast<uint64_t>(::getuid()))) /
           "region_names";
}

std::filesystem::path EntryPath(const std::filesystem::path& dir,
                                uint32_t region_id) {
    return dir / std::to_string(region_id);
}

Status WriteExact(int fd, const void* value, size_t size,
                  const std::string& path) {
    const auto* bytes = static_cast<const unsigned char*>(value);
    size_t consumed = 0;
    while (consumed < size) {
        const ssize_t count = ::pwrite(fd, bytes + consumed, size - consumed,
                                       static_cast<off_t>(consumed));
        if (count < 0) {
            if (errno == EINTR) continue;
            return ErrnoStatus("pwrite failed", path);
        }
        if (count == 0) {
            return Status::Error(StatusCode::kInternal,
                                 "short write to Region name registry '" +
                                     path + "'");
        }
        consumed += static_cast<size_t>(count);
    }
    return Status::Ok();
}

Status ReadAll(int fd, std::string* out, const std::string& path) {
    out->clear();
    for (;;) {
        char buffer[256];
        const ssize_t count = ::read(fd, buffer, sizeof(buffer));
        if (count < 0) {
            if (errno == EINTR) continue;
            return ErrnoStatus("read failed", path);
        }
        if (count == 0) break;
        out->append(buffer, static_cast<size_t>(count));
        if (out->size() > kMaxPosixShmNameBytes + 1) {
            return Status::Error(StatusCode::kCorruption,
                                 "Region name registry entry is too large: '" +
                                     path + "'");
        }
    }
    return Status::Ok();
}
#endif

}  // namespace

Status RegisterRegionName(uint32_t region_id, std::string_view name,
                          const RegionNameRegistryOptions& options) {
    if (region_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region name registry rejects reserved ID 0");
    }
    MINO_RETURN_IF_ERROR(ValidateRegistryName(name));

#if defined(__unix__) || defined(__APPLE__)
    MINO_ASSIGN_OR_RETURN(const std::filesystem::path dir,
                          ResolveRegistryDir(options));
    MINO_RETURN_IF_ERROR(EnsureDirectory(dir));
    const std::filesystem::path entry = EntryPath(dir, region_id);
    const std::string path = entry.string();

    int open_flags = O_RDWR | O_CREAT | O_EXCL | CloseOnExecFlag();
#ifdef O_NOFOLLOW
    open_flags |= O_NOFOLLOW;
#endif
    const int raw_fd = ::open(path.c_str(), open_flags, 0600);
    if (raw_fd < 0) {
        if (errno == EEXIST) {
            return Status::Error(
                StatusCode::kAlreadyExists,
                "Region ID already has a durable name mapping: '" + path + "'");
        }
        return ErrnoStatus("open(create registry entry) failed", path);
    }
    ScopedFd fd(raw_fd);
    MINO_RETURN_IF_ERROR(SetCloseOnExec(fd.get(), path));
    MINO_RETURN_IF_ERROR(LockExclusive(fd.get(), path));

    const std::string payload(name);
    MINO_RETURN_IF_ERROR(
        WriteExact(fd.get(), payload.data(), payload.size(), path));
    if (::ftruncate(fd.get(), static_cast<off_t>(payload.size())) != 0) {
        return ErrnoStatus("ftruncate(registry entry) failed", path);
    }
    MINO_RETURN_IF_ERROR(SyncFile(fd.get(), path));
    MINO_RETURN_IF_ERROR(SyncDirectory(dir));
    return Status::Ok();
#else
    (void)options;
    return Status::Error(StatusCode::kUnsupported,
                         "Region name registry requires durable POSIX file I/O");
#endif
}

Result<std::string> LookupRegionName(uint32_t region_id,
                                     const RegionNameRegistryOptions& options) {
    if (region_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region name lookup rejects reserved ID 0");
    }

#if defined(__unix__) || defined(__APPLE__)
    MINO_ASSIGN_OR_RETURN(const std::filesystem::path dir,
                          ResolveRegistryDir(options));
    const std::filesystem::path entry = EntryPath(dir, region_id);
    const std::string path = entry.string();

    int open_flags = O_RDONLY | CloseOnExecFlag();
#ifdef O_NOFOLLOW
    open_flags |= O_NOFOLLOW;
#endif
    const int raw_fd = ::open(path.c_str(), open_flags);
    if (raw_fd < 0) {
        if (errno == ENOENT) {
            return Status::Error(StatusCode::kNotFound,
                                 "no Region name registered for ID " +
                                     std::to_string(region_id));
        }
        return ErrnoStatus("open(registry entry) failed", path);
    }
    ScopedFd fd(raw_fd);
    MINO_RETURN_IF_ERROR(SetCloseOnExec(fd.get(), path));

    std::string payload;
    MINO_RETURN_IF_ERROR(ReadAll(fd.get(), &payload, path));
    // Tolerate a single trailing newline from hand-edited deployments.
    if (!payload.empty() && payload.back() == '\n') {
        payload.pop_back();
    }
    MINO_RETURN_IF_ERROR(ValidateRegistryName(payload));
    return payload;
#else
    (void)options;
    return Status::Error(StatusCode::kUnsupported,
                         "Region name registry requires durable POSIX file I/O");
#endif
}

Status UnregisterRegionName(uint32_t region_id,
                            const RegionNameRegistryOptions& options) {
    if (region_id == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Region name unregister rejects reserved ID 0");
    }

#if defined(__unix__) || defined(__APPLE__)
    MINO_ASSIGN_OR_RETURN(const std::filesystem::path dir,
                          ResolveRegistryDir(options));
    const std::filesystem::path entry = EntryPath(dir, region_id);
    const std::string path = entry.string();
    if (::unlink(path.c_str()) != 0) {
        if (errno == ENOENT) return Status::Ok();
        return ErrnoStatus("unlink(registry entry) failed", path);
    }
    // Directory sync is best-effort for rollback; missing parent is OK.
    std::error_code error;
    if (std::filesystem::is_directory(dir, error) && !error) {
        (void)SyncDirectory(dir);
    }
    return Status::Ok();
#else
    (void)options;
    return Status::Error(StatusCode::kUnsupported,
                         "Region name registry requires durable POSIX file I/O");
#endif
}

}  // namespace mino::region_internal
