// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/snapshot_store.h"

#include <sys/file.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace mino::storage {
namespace {

constexpr std::string_view kSnapshotFilename = "snapshot.mino";
constexpr std::string_view kOwnerLockFilename = ".snapshot.owner.lock";

class FileDescriptor final {
public:
    explicit FileDescriptor(int fd = -1) noexcept : fd_(fd) {}
    ~FileDescriptor() {
        if (fd_ >= 0) static_cast<void>(::close(fd_));
    }

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    FileDescriptor(FileDescriptor&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)) {}
    FileDescriptor& operator=(FileDescriptor&& other) noexcept {
        if (this == &other) return *this;
        if (fd_ >= 0) static_cast<void>(::close(fd_));
        fd_ = std::exchange(other.fd_, -1);
        return *this;
    }

    int get() const noexcept { return fd_; }
    int release() noexcept { return std::exchange(fd_, -1); }

private:
    int fd_ = -1;
};

struct TemporaryFile final {
    TemporaryFile(std::filesystem::path file_path, FileDescriptor file_descriptor)
        : path(std::move(file_path)), descriptor(std::move(file_descriptor)) {}

    ~TemporaryFile() {
        if (path.empty()) return;
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
    }

    TemporaryFile(const TemporaryFile&) = delete;
    TemporaryFile& operator=(const TemporaryFile&) = delete;
    TemporaryFile(TemporaryFile&&) = default;
    TemporaryFile& operator=(TemporaryFile&&) = default;

    std::filesystem::path path;
    FileDescriptor descriptor;
};

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

Status Unavailable(std::string_view message) {
    return Status::Error(StatusCode::kUnavailable, message);
}

Status IoError(std::string_view operation,
               const std::filesystem::path& path) {
    const int error = errno;
    StatusCode code = StatusCode::kUnavailable;
    if (error == EACCES || error == EPERM || error == EROFS) {
        code = StatusCode::kPermissionDenied;
    } else if (error == ENOSPC || error == EDQUOT) {
        code = StatusCode::kResourceExhausted;
    } else if (error == ENOENT) {
        code = StatusCode::kNotFound;
    } else if (error == ELOOP) {
        code = StatusCode::kCorruption;
    }
    return Status::Error(code, std::string(operation) + " '" + path.string() +
                                   "': " + std::strerror(error));
}

int OpenFlags(int flags) noexcept {
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    return flags;
}

Status SetCloseOnExec(int fd) noexcept {
#ifdef O_CLOEXEC
    static_cast<void>(fd);
    return Status::Ok();
#else
    const int flags = ::fcntl(fd, F_GETFD);
    if (flags < 0 || ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0) {
        return Unavailable("cannot set snapshot close-on-exec flag");
    }
    return Status::Ok();
#endif
}

Status VerifyDirectory(const std::filesystem::path& path) {
    struct stat info {};
    if (::lstat(path.c_str(), &info) != 0) {
        return IoError("cannot inspect snapshot directory", path);
    }
    if (S_ISLNK(info.st_mode) || !S_ISDIR(info.st_mode)) {
        return Invalid("snapshot partition root is not a real directory");
    }
    return Status::Ok();
}

Status EnsureDirectory(const std::filesystem::path& path) {
    std::error_code error;
    std::filesystem::create_directories(path, error);
    if (error) return Unavailable("cannot create snapshot partition root");
    return VerifyDirectory(path);
}

Result<FileDescriptor> AcquireOwnerLock(
    const std::filesystem::path& partition_root) {
    const std::filesystem::path path =
        partition_root / std::filesystem::path(kOwnerLockFilename);
    struct stat link_info {};
    if (::lstat(path.c_str(), &link_info) == 0) {
        if (S_ISLNK(link_info.st_mode) || !S_ISREG(link_info.st_mode)) {
            return Corruption("snapshot owner lock is not a regular file");
        }
    } else if (errno != ENOENT) {
        return IoError("cannot inspect snapshot owner lock", path);
    }

    FileDescriptor descriptor(
        ::open(path.c_str(), OpenFlags(O_RDWR | O_CREAT), 0644));
    if (descriptor.get() < 0) {
        return IoError("cannot open snapshot owner lock", path);
    }
    struct stat info {};
    if (::fstat(descriptor.get(), &info) != 0) {
        return IoError("cannot stat snapshot owner lock", path);
    }
    if (!S_ISREG(info.st_mode)) {
        return Corruption("snapshot owner lock type changed");
    }
    if (::flock(descriptor.get(), LOCK_EX | LOCK_NB) != 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            return Unavailable("snapshot store already has an owner");
        }
        return IoError("cannot acquire snapshot owner lock", path);
    }
    return descriptor;
}

Status ValidateIdentity(const SnapshotStoreIdentity& identity) {
    if (identity.recording_id == 0 || identity.topic_id == 0 ||
        identity.writer_id == 0 || identity.schema_refs.empty()) {
        return Invalid("snapshot identity is incomplete");
    }
    if (std::find(identity.schema_refs.begin(), identity.schema_refs.end(), 0) !=
        identity.schema_refs.end()) {
        return Invalid("snapshot schema allowlist contains the invalid ref");
    }
    std::vector<uint32_t> refs = identity.schema_refs;
    std::sort(refs.begin(), refs.end());
    if (std::adjacent_find(refs.begin(), refs.end()) != refs.end()) {
        return Invalid("snapshot schema allowlist contains a duplicate ref");
    }
    return Status::Ok();
}

bool ContainsSchema(const SnapshotStoreIdentity& identity,
                    uint32_t schema_ref) noexcept {
    return std::find(identity.schema_refs.begin(), identity.schema_refs.end(),
                     schema_ref) != identity.schema_refs.end();
}

Result<std::vector<std::byte>> ReadSnapshot(
    const std::filesystem::path& path,
    const SegmentFormatLimits& limits) {
    struct stat link_info {};
    if (::lstat(path.c_str(), &link_info) != 0) {
        if (errno == ENOENT) {
            return Status::Error(StatusCode::kNotFound,
                                 "snapshot file does not exist");
        }
        return IoError("cannot inspect snapshot", path);
    }
    if (S_ISLNK(link_info.st_mode) || !S_ISREG(link_info.st_mode)) {
        return Corruption("snapshot is not a regular non-symlink file");
    }

    FileDescriptor descriptor(::open(path.c_str(), OpenFlags(O_RDONLY)));
    if (descriptor.get() < 0) return IoError("cannot open snapshot", path);
    struct stat info {};
    if (::fstat(descriptor.get(), &info) != 0) {
        return IoError("cannot stat snapshot", path);
    }
    if (!S_ISREG(info.st_mode) || info.st_size < 0) {
        return Corruption("snapshot type or size changed");
    }
    if (limits.max_encoded_record_size >
        std::numeric_limits<uint64_t>::max() - kEncodedSegmentHeaderSize) {
        return Invalid("snapshot format byte limit overflows");
    }
    const uint64_t max_file_size =
        limits.max_encoded_record_size + kEncodedSegmentHeaderSize;
    if (static_cast<uint64_t>(info.st_size) > max_file_size) {
        return Exhausted("snapshot exceeds configured byte limit");
    }
    if (static_cast<uint64_t>(info.st_size) >
        std::numeric_limits<size_t>::max()) {
        return Exhausted("snapshot does not fit addressable memory");
    }

    std::vector<std::byte> bytes(static_cast<size_t>(info.st_size));
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t count =
            ::read(descriptor.get(), bytes.data() + offset, bytes.size() - offset);
        if (count < 0) {
            if (errno == EINTR) continue;
            return IoError("cannot read snapshot", path);
        }
        if (count == 0) return Corruption("snapshot is truncated");
        offset += static_cast<size_t>(count);
    }
    std::byte extra{};
    while (true) {
        const ssize_t count = ::read(descriptor.get(), &extra, 1);
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) return IoError("cannot finish reading snapshot", path);
        if (count != 0) return Corruption("snapshot grew while being read");
        break;
    }
    return bytes;
}

Result<Record> DecodeSnapshot(std::span<const std::byte> bytes,
                              const SnapshotStoreIdentity& identity,
                              const SegmentFormatLimits& limits) {
    if (bytes.size() < kEncodedSegmentHeaderSize + kMinimumEncodedRecordSize) {
        return Corruption("snapshot does not contain one complete record");
    }
    Result<SegmentHeader> header =
        DecodeSegmentHeader(bytes.first(kEncodedSegmentHeaderSize));
    if (!header.ok()) return header.status();
    Result<Record> record =
        DecodeRecord(bytes.subspan(kEncodedSegmentHeaderSize), limits);
    if (!record.ok()) return record.status();
    if (header->recording_id != identity.recording_id ||
        header->topic_id != identity.topic_id ||
        header->partition_id != identity.partition_id ||
        header->writer_id != identity.writer_id) {
        return Corruption("snapshot segment identity does not match its owner");
    }
    if (record->header.topic_id != identity.topic_id ||
        record->header.partition_id != identity.partition_id ||
        record->header.ingestion_sequence == 0 ||
        header->first_ingestion_sequence !=
            record->header.ingestion_sequence) {
        return Corruption("snapshot record identity or sequence is invalid");
    }
    if (!ContainsSchema(identity, record->header.schema_ref)) {
        return Corruption("snapshot record uses an unknown schema ref");
    }
    return record;
}

Result<TemporaryFile> CreateTemporaryFile(
    const std::filesystem::path& partition_root) {
    std::filesystem::path pattern_path = partition_root / ".snapshot.tmp.XXXXXX";
    std::string pattern = pattern_path.string();
    pattern.push_back('\0');
    const int fd = ::mkstemp(pattern.data());
    if (fd < 0) {
        return IoError("cannot create snapshot temp file", pattern_path);
    }
    FileDescriptor descriptor(fd);
    Status close_on_exec = SetCloseOnExec(fd);
    if (!close_on_exec.ok()) return close_on_exec;
    if (::fchmod(fd, 0644) != 0) {
        return IoError("cannot set snapshot temp file mode", pattern_path);
    }
    return TemporaryFile(std::filesystem::path(pattern.c_str()),
                         std::move(descriptor));
}

Status WriteAll(int fd, const std::filesystem::path& path,
                std::span<const std::byte> bytes) {
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t written =
            ::write(fd, bytes.data() + offset, bytes.size() - offset);
        if (written < 0) {
            if (errno == EINTR) continue;
            return IoError("cannot write snapshot temp file", path);
        }
        if (written == 0) return Unavailable("zero-byte snapshot write");
        offset += static_cast<size_t>(written);
    }
    return Status::Ok();
}

Status DataSync(int fd, const std::filesystem::path& path) {
    while (::fdatasync(fd) != 0) {
        if (errno == EINTR) continue;
        return IoError("cannot fdatasync snapshot temp file", path);
    }
    return Status::Ok();
}

Status SyncDirectory(const std::filesystem::path& path) {
    int flags = O_RDONLY;
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
    FileDescriptor descriptor(::open(path.c_str(), OpenFlags(flags)));
    if (descriptor.get() < 0) {
        return IoError("cannot open snapshot directory", path);
    }
    while (::fsync(descriptor.get()) != 0) {
        if (errno == EINTR) continue;
        return IoError("cannot fsync snapshot directory", path);
    }
    return Status::Ok();
}

Status RunFaultHook(const SnapshotStoreOptions& options,
                    SnapshotStoreFaultPoint point) {
    if (options.fault_hook == nullptr) return Status::Ok();
    return options.fault_hook(point, options.fault_hook_context);
}

Status VerifyDestination(const std::filesystem::path& path) {
    struct stat info {};
    if (::lstat(path.c_str(), &info) == 0) {
        if (S_ISLNK(info.st_mode) || !S_ISREG(info.st_mode)) {
            return Corruption("snapshot destination is not a regular file");
        }
    } else if (errno != ENOENT) {
        return IoError("cannot inspect snapshot destination", path);
    }
    return Status::Ok();
}

}  // namespace

SnapshotStore::SnapshotStore(std::filesystem::path partition_root,
                             SnapshotStoreIdentity identity,
                             SnapshotStoreOptions options, int owner_lock_fd,
                             std::optional<Record> latest_record,
                             uint64_t next_ingestion_sequence) noexcept
    : partition_root_(std::move(partition_root)),
      path_(partition_root_ / std::filesystem::path(kSnapshotFilename)),
      identity_(std::move(identity)),
      options_(options),
      owner_lock_fd_(owner_lock_fd),
      latest_record_(std::move(latest_record)),
      next_ingestion_sequence_(next_ingestion_sequence) {}

SnapshotStore::~SnapshotStore() {
    if (owner_lock_fd_ >= 0) static_cast<void>(::close(owner_lock_fd_));
}

Result<std::unique_ptr<SnapshotStore>> SnapshotStore::Open(
    const std::filesystem::path& partition_root,
    SnapshotStoreIdentity identity,
    const SnapshotStoreOptions& options) noexcept {
    try {
        if (partition_root.empty()) {
            return Invalid("snapshot partition root is empty");
        }
        Status valid = ValidateIdentity(identity);
        if (!valid.ok()) return valid;
        Result<size_t> minimum = EncodedRecordSize(0, options.format_limits);
        if (!minimum.ok()) return minimum.status();
        Status directory = EnsureDirectory(partition_root);
        if (!directory.ok()) return directory;
        Result<FileDescriptor> owner = AcquireOwnerLock(partition_root);
        if (!owner.ok()) return owner.status();

        const std::filesystem::path path =
            partition_root / std::filesystem::path(kSnapshotFilename);
        Result<std::vector<std::byte>> bytes =
            ReadSnapshot(path, options.format_limits);
        std::optional<Record> latest;
        uint64_t next_sequence = 1;
        if (bytes.ok()) {
            Result<Record> decoded =
                DecodeSnapshot(*bytes, identity, options.format_limits);
            if (!decoded.ok()) return decoded.status();
            if (decoded->header.ingestion_sequence ==
                std::numeric_limits<uint64_t>::max()) {
                next_sequence = std::numeric_limits<uint64_t>::max();
            } else {
                next_sequence = decoded->header.ingestion_sequence + 1;
            }
            latest = std::move(*decoded);
        } else if (bytes.status().code() != StatusCode::kNotFound) {
            return bytes.status();
        }

        return std::unique_ptr<SnapshotStore>(new SnapshotStore(
            partition_root, std::move(identity), options, owner->release(),
            std::move(latest), next_sequence));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate snapshot store state");
    } catch (const std::length_error&) {
        return Exhausted("snapshot store exceeds a container bound");
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Result<uint64_t> SnapshotStore::Put(Record record) noexcept {
    try {
        if (record.header.topic_id != identity_.topic_id ||
            record.header.partition_id != identity_.partition_id) {
            return Invalid("snapshot record topic or partition is incorrect");
        }
        if (!ContainsSchema(identity_, record.header.schema_ref)) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "snapshot record schema ref is unknown");
        }
        if (latest_record_.has_value() &&
            latest_record_->header.ingestion_sequence ==
                std::numeric_limits<uint64_t>::max()) {
            return Exhausted("snapshot ingestion sequence is exhausted");
        }

        const uint64_t sequence = next_ingestion_sequence_;
        record.header.ingestion_sequence = sequence;
        SegmentHeader header;
        header.recording_id = identity_.recording_id;
        header.topic_id = identity_.topic_id;
        header.partition_id = identity_.partition_id;
        header.writer_id = identity_.writer_id;
        header.first_ingestion_sequence = sequence;
        header.created_at_ns = record.header.ingestion_timestamp_ns;

        Result<std::vector<std::byte>> encoded_header =
            EncodeSegmentHeader(header);
        if (!encoded_header.ok()) return encoded_header.status();
        Result<std::vector<std::byte>> encoded_record =
            EncodeRecord(record, options_.format_limits);
        if (!encoded_record.ok()) return encoded_record.status();
        if (encoded_header->size() >
            std::numeric_limits<size_t>::max() - encoded_record->size()) {
            return Exhausted("snapshot encoded size overflows");
        }
        std::vector<std::byte> bytes;
        bytes.reserve(encoded_header->size() + encoded_record->size());
        bytes.insert(bytes.end(), encoded_header->begin(), encoded_header->end());
        bytes.insert(bytes.end(), encoded_record->begin(), encoded_record->end());

        Status destination = VerifyDestination(path_);
        if (!destination.ok()) return destination;
        Result<TemporaryFile> temporary = CreateTemporaryFile(partition_root_);
        if (!temporary.ok()) return temporary.status();
        Status status = WriteAll(temporary->descriptor.get(), temporary->path,
                                 bytes);
        if (!status.ok()) return status;
        status = RunFaultHook(options_,
                              SnapshotStoreFaultPoint::kAfterTempWrite);
        if (!status.ok()) return status;
        status = DataSync(temporary->descriptor.get(), temporary->path);
        if (!status.ok()) return status;
        status = RunFaultHook(options_,
                              SnapshotStoreFaultPoint::kAfterTempDataSync);
        if (!status.ok()) return status;
        if (::rename(temporary->path.c_str(), path_.c_str()) != 0) {
            return IoError("cannot rename snapshot temp file", path_);
        }
        temporary->path.clear();

        latest_record_ = record;
        if (sequence != std::numeric_limits<uint64_t>::max()) {
            next_ingestion_sequence_ = sequence + 1;
        }
        status = RunFaultHook(options_, SnapshotStoreFaultPoint::kAfterRename);
        if (!status.ok()) return status;
        status = SyncDirectory(partition_root_);
        if (!status.ok()) return status;
        status = RunFaultHook(
            options_, SnapshotStoreFaultPoint::kAfterParentDirectorySync);
        if (!status.ok()) return status;
        return sequence;
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate snapshot replacement");
    } catch (const std::length_error&) {
        return Exhausted("snapshot replacement exceeds a container bound");
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

}  // namespace mino::storage
