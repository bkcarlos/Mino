// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "tools/minoc/output_commit.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <map>
#include <new>
#include <set>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino::tools::minoc {
namespace {

Status IoError(std::string_view operation,
               const std::filesystem::path& path,
               int error = errno) {
    return Status::Error(StatusCode::kUnavailable,
                         std::string(operation) + " '" + path.string() +
                             "': " + std::strerror(error));
}

class FileDescriptor {
public:
    explicit FileDescriptor(int fd = -1) noexcept : fd_(fd) {}
    ~FileDescriptor() {
        if (fd_ >= 0) (void)::close(fd_);
    }
    FileDescriptor(FileDescriptor&& other) noexcept : fd_(other.fd_) {
        other.fd_ = -1;
    }
    FileDescriptor& operator=(FileDescriptor&& other) noexcept {
        if (this == &other) return *this;
        if (fd_ >= 0) (void)::close(fd_);
        fd_ = other.fd_;
        other.fd_ = -1;
        return *this;
    }
    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;
    int get() const noexcept { return fd_; }
    int release() noexcept {
        const int result = fd_;
        fd_ = -1;
        return result;
    }

private:
    int fd_;
};

struct UniqueFile {
    std::filesystem::path path;
    FileDescriptor fd;
};

Result<std::filesystem::path> NormalizeOutput(
    const std::filesystem::path& path) {
    if (path.empty() || path.filename().empty() || path.filename() == "." ||
        path.filename() == "..") {
        return Status::Error(StatusCode::kInvalidArgument,
                             "output path has no valid filename");
    }
    std::error_code error;
    const auto status = std::filesystem::symlink_status(path, error);
    if (!error && std::filesystem::is_symlink(status)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "output leaf must not be a symlink");
    }
    error.clear();
    const std::filesystem::path parent = std::filesystem::weakly_canonical(
        path.has_parent_path() ? path.parent_path() : std::filesystem::path("."),
        error);
    if (error || !std::filesystem::is_directory(parent)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "output parent directory is unavailable");
    }
    return (parent / path.filename()).lexically_normal();
}

Result<std::filesystem::path> NormalizeExisting(
    const std::filesystem::path& path) {
    std::error_code error;
    const std::filesystem::path canonical = std::filesystem::canonical(path, error);
    if (error) {
        return Status::Error(StatusCode::kNotFound,
                             "cannot canonicalize protected path '" +
                                 path.string() + "'");
    }
    return canonical;
}

Result<std::pair<uint64_t, uint64_t>> PhysicalKey(
    const std::filesystem::path& path) {
    struct stat info {};
    if (::stat(path.c_str(), &info) != 0) {
        return IoError("cannot stat path", path);
    }
    return std::pair<uint64_t, uint64_t>(
        static_cast<uint64_t>(info.st_dev), static_cast<uint64_t>(info.st_ino));
}

Result<UniqueFile> CreateUnique(const std::filesystem::path& target,
                                std::string_view role) {
    const std::filesystem::path pattern_path =
        target.parent_path() /
        ("." + target.filename().string() + ".minoc." + std::string(role) +
         ".XXXXXX");
    std::string pattern = pattern_path.string();
    pattern.push_back('\0');
    const int fd = ::mkstemp(pattern.data());
    if (fd < 0) return IoError("cannot create unique file", pattern_path);
    return UniqueFile{std::filesystem::path(pattern.c_str()), FileDescriptor(fd)};
}

Status WriteAndSync(int fd, const std::filesystem::path& path,
                    std::string_view bytes) {
    if (::fchmod(fd, 0644) != 0) {
        return IoError("cannot set temporary output mode", path);
    }
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t written = ::write(fd, bytes.data() + offset,
                                        bytes.size() - offset);
        if (written < 0) {
            if (errno == EINTR) continue;
            return IoError("cannot write temporary output", path);
        }
        if (written == 0) {
            return Status::Error(StatusCode::kUnavailable,
                                 "zero-byte write to temporary output");
        }
        offset += static_cast<size_t>(written);
    }
    if (::fsync(fd) != 0) return IoError("cannot fsync temporary output", path);
    return Status::Ok();
}

Status SyncDirectory(const std::filesystem::path& directory) {
    FileDescriptor fd(::open(directory.c_str(), O_RDONLY));
    if (fd.get() < 0) return IoError("cannot open output directory", directory);
    if (::fsync(fd.get()) != 0) return IoError("cannot fsync output directory", directory);
    return Status::Ok();
}

void RemoveNoThrow(const std::filesystem::path& path) noexcept {
    if (path.empty()) return;
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
}

Status Rename(const std::filesystem::path& from,
              const std::filesystem::path& to) {
    std::error_code error;
    std::filesystem::rename(from, to, error);
    if (error) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot rename '" + from.string() + "' to '" +
                                 to.string() + "': " + error.message());
    }
    return Status::Ok();
}

Status Restore(const std::vector<std::filesystem::path>& targets,
               const std::vector<std::filesystem::path>& backups,
               size_t committed) noexcept {
    bool failed = false;
    for (size_t i = 0; i < committed; ++i) RemoveNoThrow(targets[i]);
    for (size_t i = 0; i < backups.size(); ++i) {
        if (backups[i].empty()) continue;
        const Status restored = Rename(backups[i], targets[i]);
        failed = failed || !restored.ok();
    }
    return failed ? Status::Error(StatusCode::kInternal,
                                  "output rollback could not restore all files")
                  : Status::Ok();
}

Result<std::vector<FileDescriptor>> LockDirectories(
    const std::vector<std::filesystem::path>& targets) {
    std::set<std::filesystem::path> directories;
    for (const auto& target : targets) directories.insert(target.parent_path());
    std::vector<FileDescriptor> locks;
    locks.reserve(directories.size());
    for (const auto& directory : directories) {
        FileDescriptor fd(::open(directory.c_str(), O_RDONLY));
        if (fd.get() < 0) return IoError("cannot open output directory", directory);
        while (::flock(fd.get(), LOCK_EX) != 0) {
            if (errno == EINTR) continue;
            return IoError("cannot lock output directory", directory);
        }
        locks.push_back(std::move(fd));
    }
    return locks;
}

}  // namespace

Status ValidateOutputPaths(
    std::span<const std::filesystem::path> outputs,
    std::span<const std::filesystem::path> protected_paths) noexcept {
    try {
        std::set<std::filesystem::path> normalized;
        std::set<std::pair<uint64_t, uint64_t>> physical_keys;
        for (const auto& output : outputs) {
            auto path = NormalizeOutput(output);
            if (!path.ok()) return path.status();
            if (!normalized.insert(*path).second) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "output paths physically alias");
            }
            std::error_code exists_error;
            if (std::filesystem::exists(*path, exists_error) && !exists_error) {
                auto key = PhysicalKey(*path);
                if (!key.ok()) return key.status();
                if (!physical_keys.insert(*key).second) {
                    return Status::Error(StatusCode::kInvalidArgument,
                                         "output files physically alias");
                }
            }
        }
        std::set<std::filesystem::path> protected_normalized;
        for (const auto& path : protected_paths) {
            auto canonical = NormalizeExisting(path);
            if (!canonical.ok()) return canonical.status();
            auto key = PhysicalKey(*canonical);
            if (!key.ok()) return key.status();
            if (!protected_normalized.insert(*canonical).second ||
                !physical_keys.insert(*key).second) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "input/import paths physically alias");
            }
            if (normalized.contains(*canonical)) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "an output physically aliases input/import");
            }
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status CommitOutputFiles(std::span<const OutputFile> files,
                         const CommitOptions& options) noexcept {
    try {
        if (files.empty()) return Status::Ok();
        std::vector<std::filesystem::path> requested;
        requested.reserve(files.size());
        for (const OutputFile& file : files) requested.push_back(file.path);
        MINO_RETURN_IF_ERROR(ValidateOutputPaths(requested));

        std::vector<std::filesystem::path> targets;
        targets.reserve(files.size());
        for (const auto& path : requested) {
            auto normalized = NormalizeOutput(path);
            if (!normalized.ok()) return normalized.status();
            targets.push_back(std::move(*normalized));
        }
        auto locks = LockDirectories(targets);
        if (!locks.ok()) return locks.status();
        MINO_RETURN_IF_ERROR(ValidateOutputPaths(targets));

        std::vector<std::filesystem::path> temporaries(files.size());
        std::vector<std::filesystem::path> backups(files.size());
        for (size_t i = 0; i < files.size(); ++i) {
            auto temporary = CreateUnique(targets[i], "tmp");
            if (!temporary.ok()) {
                for (const auto& path : temporaries) RemoveNoThrow(path);
                return temporary.status();
            }
            temporaries[i] = temporary->path;
            const Status written = WriteAndSync(temporary->fd.get(),
                                                temporary->path, files[i].bytes);
            if (!written.ok()) {
                for (const auto& path : temporaries) RemoveNoThrow(path);
                return written;
            }
        }

        size_t backed_up = 0;
        for (size_t i = 0; i < targets.size(); ++i) {
            std::error_code error;
            const auto status = std::filesystem::symlink_status(targets[i], error);
            if (error && error != std::errc::no_such_file_or_directory) {
                (void)Restore(targets, backups, 0);
                for (const auto& path : temporaries) RemoveNoThrow(path);
                return Status::Error(StatusCode::kUnavailable,
                                     "cannot inspect existing output");
            }
            if (!error && std::filesystem::exists(status)) {
                if (!std::filesystem::is_regular_file(status)) {
                    (void)Restore(targets, backups, 0);
                    for (const auto& path : temporaries) RemoveNoThrow(path);
                    return Status::Error(StatusCode::kInvalidArgument,
                                         "existing output is not a regular file");
                }
                auto backup = CreateUnique(targets[i], "backup");
                if (!backup.ok()) {
                    (void)Restore(targets, backups, 0);
                    for (const auto& path : temporaries) RemoveNoThrow(path);
                    return backup.status();
                }
                const std::filesystem::path backup_path = backup->path;
                const int backup_fd = backup->fd.release();
                if (::close(backup_fd) != 0) {
                    RemoveNoThrow(backup_path);
                    (void)Restore(targets, backups, 0);
                    for (const auto& path : temporaries) RemoveNoThrow(path);
                    return IoError("cannot close backup placeholder", backup_path);
                }
                RemoveNoThrow(backup_path);
                const Status moved = Rename(targets[i], backup_path);
                if (!moved.ok()) {
                    (void)Restore(targets, backups, 0);
                    for (const auto& path : temporaries) RemoveNoThrow(path);
                    return moved;
                }
                backups[i] = backup_path;
            }
            backed_up = i + 1;
        }
        static_cast<void>(backed_up);

        size_t committed = 0;
        for (size_t i = 0; i < targets.size(); ++i) {
            Status renamed = Status::Ok();
            if (options.fail_before_rename == i + 1) {
                renamed = Status::Error(StatusCode::kUnavailable,
                                        "injected output rename failure");
            } else {
                renamed = Rename(temporaries[i], targets[i]);
            }
            if (!renamed.ok()) {
                const Status rollback = Restore(targets, backups, committed);
                for (size_t remaining = i; remaining < temporaries.size(); ++remaining) {
                    RemoveNoThrow(temporaries[remaining]);
                }
                return rollback.ok() ? renamed : rollback;
            }
            temporaries[i].clear();
            ++committed;
        }

        std::set<std::filesystem::path> directories;
        for (const auto& target : targets) directories.insert(target.parent_path());
        for (const auto& directory : directories) {
            const Status synced = SyncDirectory(directory);
            if (!synced.ok()) {
                const Status rollback = Restore(targets, backups, committed);
                return rollback.ok() ? synced : rollback;
            }
        }
        for (const auto& backup : backups) RemoveNoThrow(backup);
        for (const auto& directory : directories) {
            MINO_RETURN_IF_ERROR(SyncDirectory(directory));
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::tools::minoc
