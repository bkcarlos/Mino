// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/dedup_store.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <limits>
#include <new>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "mino/bridge/crc32c.h"

namespace mino::bridge {
namespace {

constexpr char kMagic[8] = {'M', 'I', 'N', 'O', 'D', 'E', 'D', 'U'};
constexpr uint32_t kLegacyFormatVersion = 1;
constexpr uint32_t kRetirementFormatVersion = 2;
constexpr size_t kHeaderBytes = 16;  // magic(8) + version(4) + count(4)
constexpr size_t kEntryBytes = 32;   // 3x u64 identity + u64 hwm
constexpr size_t kCrcBytes = 4;

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}
Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}
Status Resource(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

bool ValidSource(const SourceIdentity& source) noexcept {
    return source.node_id != 0 && source.publisher_id != 0 &&
           source.publisher_epoch != 0;
}

using SourceMap = std::unordered_map<SourceIdentity, uint64_t, SourceIdentityHash>;

SourceIdentity PublisherKey(const SourceIdentity& source) noexcept {
    return {source.node_id, source.publisher_id, 0};
}

bool Retired(const SourceMap& retired, const SourceIdentity& source) noexcept {
    const auto it = retired.find(PublisherKey(source));
    return it != retired.end() && source.publisher_epoch <= it->second;
}

bool SourceLess(const SourceIdentity& left,
                const SourceIdentity& right) noexcept {
    if (left.node_id != right.node_id) return left.node_id < right.node_id;
    if (left.publisher_id != right.publisher_id) {
        return left.publisher_id < right.publisher_id;
    }
    return left.publisher_epoch < right.publisher_epoch;
}

void WriteBe32(std::vector<std::byte>* bytes, uint32_t value) {
    for (size_t i = 0; i < 4; ++i) {
        bytes->push_back(
            static_cast<std::byte>((value >> (24 - 8 * i)) & 0xffu));
    }
}

void WriteBe64(std::vector<std::byte>* bytes, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) {
        bytes->push_back(
            static_cast<std::byte>((value >> (56 - 8 * i)) & 0xffu));
    }
}

uint32_t ReadBe32(std::span<const std::byte> bytes, size_t offset) noexcept {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
        value = (value << 8) | static_cast<uint8_t>(bytes[offset + i]);
    }
    return value;
}

uint64_t ReadBe64(std::span<const std::byte> bytes, size_t offset) noexcept {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) {
        value = (value << 8) | static_cast<uint8_t>(bytes[offset + i]);
    }
    return value;
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
    int release() noexcept { return std::exchange(fd_, -1); }

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
    return Status::Error(code, std::string(operation) + " for dedup store '" +
                                   path + "': " + std::strerror(error_number));
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
    if (raw_fd < 0) return ErrnoStatus("open(dedup store directory) failed", path);
    ScopedFd fd(raw_fd);
    MINO_RETURN_IF_ERROR(SetCloseOnExec(fd.get(), path));
    while (::fsync(fd.get()) != 0) {
        if (errno == EINTR) continue;
        return ErrnoStatus("fsync(dedup store directory) failed", path);
    }
    return Status::Ok();
}

Status EnsureParentDirectory(const std::filesystem::path& file_path) {
    auto directory = file_path.parent_path();
    if (directory.empty()) return Status::Ok();
    std::vector<std::filesystem::path> missing;
    std::error_code error;
    while (!std::filesystem::exists(directory, error)) {
        if (error) return ErrnoStatus("inspect(dedup directory) failed", directory.string(), error.value());
        missing.push_back(directory);
        directory = directory.parent_path();
        if (directory.empty()) directory = ".";
    }
    if (error || !std::filesystem::is_directory(directory, error) || error) {
        return Invalid("dedup store parent path is not a directory");
    }
    // Persist each newly created directory entry in its parent. Syncing only
    // the leaf and its immediate parent can lose ancestors after a power cut.
    for (auto it = missing.rbegin(); it != missing.rend(); ++it) {
        const bool created = std::filesystem::create_directory(*it, error);
        if (error || (!created && !std::filesystem::is_directory(*it, error))) {
            return Status::Error(StatusCode::kInternal, "cannot create dedup store directory");
        }
        if (created && ::chmod(it->c_str(), 0700) != 0) {
            return ErrnoStatus("chmod(dedup directory) failed", it->string());
        }
        MINO_RETURN_IF_ERROR(SyncDirectory(*it));
        const auto parent = it->parent_path();
        MINO_RETURN_IF_ERROR(SyncDirectory(parent.empty() ? "." : parent));
    }
    return Status::Ok();
}

Status WriteExact(int fd, const std::byte* bytes, size_t size,
                  const std::string& path) {
    size_t consumed = 0;
    while (consumed < size) {
        const ssize_t count = ::pwrite(
            fd, bytes + consumed, size - consumed, static_cast<off_t>(consumed));
        if (count < 0) {
            if (errno == EINTR) continue;
            return ErrnoStatus("pwrite failed", path);
        }
        if (count == 0) {
            return Status::Error(StatusCode::kInternal,
                                 "short write to dedup store '" + path + "'");
        }
        consumed += static_cast<size_t>(count);
    }
    return Status::Ok();
}

Status ReadAll(int fd, std::vector<std::byte>* out, const std::string& path,
               size_t max_bytes) {
    out->clear();
    for (;;) {
        std::byte buffer[4096];
        const ssize_t count = ::read(fd, buffer, sizeof(buffer));
        if (count < 0) {
            if (errno == EINTR) continue;
            return ErrnoStatus("read failed", path);
        }
        if (count == 0) break;
        if (out->size() + static_cast<size_t>(count) > max_bytes) {
            return Corruption("dedup store file exceeds size bound");
        }
        out->insert(out->end(), buffer, buffer + count);
    }
    return Status::Ok();
}

Status DecodeSnapshot(
    std::span<const std::byte> bytes, size_t max_sources,
    SourceMap* out, SourceMap* retired) {
    if (bytes.size() < kHeaderBytes + kCrcBytes) {
        return Corruption("dedup store snapshot is truncated");
    }
    for (size_t i = 0; i < sizeof(kMagic); ++i) {
        if (static_cast<char>(bytes[i]) != kMagic[i]) {
            return Corruption("dedup store magic mismatch");
        }
    }
    const uint32_t version = ReadBe32(bytes, 8);
    if (version != kLegacyFormatVersion && version != kRetirementFormatVersion) {
        return Corruption("dedup store version unsupported");
    }
    const uint32_t count = ReadBe32(bytes, 12);
    if (count > max_sources) {
        return Corruption("dedup store entry count exceeds max_sources");
    }
    const size_t expected =
        kHeaderBytes + static_cast<size_t>(count) * kEntryBytes + kCrcBytes;
    if (bytes.size() != expected) {
        return Corruption("dedup store snapshot length mismatch");
    }
    const uint32_t expected_crc = ReadBe32(bytes, expected - kCrcBytes);
    const uint32_t actual_crc =
        Crc32c(bytes.subspan(0, expected - kCrcBytes));
    if (expected_crc != actual_crc) {
        return Corruption("dedup store CRC mismatch");
    }

    out->clear();
    retired->clear();
    out->reserve(count);
    SourceIdentity previous{};
    bool have_previous = false;
    for (uint32_t i = 0; i < count; ++i) {
        const size_t offset = kHeaderBytes + static_cast<size_t>(i) * kEntryBytes;
        SourceIdentity source{
            .node_id = ReadBe64(bytes, offset),
            .publisher_id = ReadBe64(bytes, offset + 8),
            .publisher_epoch = ReadBe64(bytes, offset + 16),
        };
        const uint64_t highest = ReadBe64(bytes, offset + 24);
        if (!ValidSource(source) || (highest == 0 && version == kLegacyFormatVersion)) {
            return Corruption("dedup store contains an invalid entry");
        }
        if (have_previous && !SourceLess(previous, source)) {
            return Corruption("dedup store entries are not strictly ordered");
        }
        const bool inserted = highest == 0 ?
            retired->emplace(PublisherKey(source), source.publisher_epoch).second :
            out->emplace(source, highest).second;
        if (!inserted) {
            return Corruption("dedup store contains a duplicate source or retirement boundary");
        }
        previous = source;
        have_previous = true;
    }
    for (const auto& [source, highest] : *out) {
        (void)highest;
        if (Retired(*retired, source)) {
            return Corruption("dedup store contains an HWM within a retired epoch range");
        }
    }
    return Status::Ok();
}

Result<std::vector<std::byte>> EncodeSnapshot(
    const std::unordered_map<SourceIdentity, uint64_t, SourceIdentityHash>&
        entries,
    const SourceMap& retired, size_t max_sources) {
    try {
        if (entries.size() > max_sources || retired.size() > max_sources - entries.size()) {
            return Resource("dedup store would exceed max_sources");
        }
        if (entries.size() + retired.size() > std::numeric_limits<uint32_t>::max()) {
            return Resource("dedup store entry count overflows");
        }
        std::vector<DedupResumeEntry> ordered;
        ordered.reserve(entries.size() + retired.size());
        for (const auto& [source, highest] : entries) {
            ordered.push_back(DedupResumeEntry{
                .source = source,
                .highest_contiguous_sequence = highest,
            });
        }
        for (const auto& [publisher, epoch] : retired) {
            ordered.push_back({{publisher.node_id, publisher.publisher_id, epoch}, 0});
        }
        std::sort(ordered.begin(), ordered.end(),
                  [](const DedupResumeEntry& left,
                     const DedupResumeEntry& right) {
                      return SourceLess(left.source, right.source);
                  });

        std::vector<std::byte> bytes;
        bytes.reserve(kHeaderBytes + ordered.size() * kEntryBytes + kCrcBytes);
        for (char c : kMagic) bytes.push_back(static_cast<std::byte>(c));
        WriteBe32(&bytes, retired.empty() ? kLegacyFormatVersion : kRetirementFormatVersion);
        WriteBe32(&bytes, static_cast<uint32_t>(ordered.size()));
        for (const DedupResumeEntry& entry : ordered) {
            WriteBe64(&bytes, entry.source.node_id);
            WriteBe64(&bytes, entry.source.publisher_id);
            WriteBe64(&bytes, entry.source.publisher_epoch);
            WriteBe64(&bytes, entry.highest_contiguous_sequence);
        }
        const uint32_t crc = Crc32c(bytes);
        WriteBe32(&bytes, crc);
        return bytes;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Status AtomicPublish(const std::filesystem::path& path,
                     std::span<const std::byte> bytes,
                     const DedupStoreTestHooks* hooks) {
    const auto checkpoint = [hooks](DedupStoreIoStage stage, bool after) {
        return hooks == nullptr ? Status::Ok() : hooks->Checkpoint(stage, after);
    };
    MINO_RETURN_IF_ERROR(EnsureParentDirectory(path));
    const std::filesystem::path tmp_path =
        path.string() + ".tmp." + std::to_string(static_cast<uint64_t>(::getpid()));
    const std::string tmp = tmp_path.string();
    const std::string final_path = path.string();

    int open_flags = O_WRONLY | O_CREAT | O_TRUNC | CloseOnExecFlag();
#ifdef O_NOFOLLOW
    open_flags |= O_NOFOLLOW;
#endif
    const int raw_fd = ::open(tmp.c_str(), open_flags, 0600);
    if (raw_fd < 0) return ErrnoStatus("open(dedup store tmp) failed", tmp);
    ScopedFd fd(raw_fd);
    MINO_RETURN_IF_ERROR(SetCloseOnExec(fd.get(), tmp));
    MINO_RETURN_IF_ERROR(checkpoint(DedupStoreIoStage::kWrite, false));
    MINO_RETURN_IF_ERROR(WriteExact(fd.get(), bytes.data(), bytes.size(), tmp));
    if (::ftruncate(fd.get(), static_cast<off_t>(bytes.size())) != 0) {
        return ErrnoStatus("ftruncate(dedup store tmp) failed", tmp);
    }
    MINO_RETURN_IF_ERROR(checkpoint(DedupStoreIoStage::kWrite, true));
    MINO_RETURN_IF_ERROR(checkpoint(DedupStoreIoStage::kFileSync, false));
    MINO_RETURN_IF_ERROR(SyncFile(fd.get(), tmp));
    MINO_RETURN_IF_ERROR(checkpoint(DedupStoreIoStage::kFileSync, true));
    fd = ScopedFd();  // close before rename

    MINO_RETURN_IF_ERROR(checkpoint(DedupStoreIoStage::kRename, false));
    while (::rename(tmp.c_str(), final_path.c_str()) != 0) {
        if (errno == EINTR) continue;
        (void)::unlink(tmp.c_str());
        return ErrnoStatus("rename(dedup store) failed", final_path);
    }
    MINO_RETURN_IF_ERROR(checkpoint(DedupStoreIoStage::kRename, true));
    const std::filesystem::path parent = path.parent_path();
    MINO_RETURN_IF_ERROR(checkpoint(DedupStoreIoStage::kDirectorySync, false));
    MINO_RETURN_IF_ERROR(SyncDirectory(parent.empty() ? "." : parent));
    MINO_RETURN_IF_ERROR(checkpoint(DedupStoreIoStage::kDirectorySync, true));
    return Status::Ok();
}
#endif  // unix

}  // namespace

DedupStore::DedupStore(std::string path, size_t max_sources) noexcept
    : path_(std::move(path)), max_sources_(max_sources) {}

DedupStore::~DedupStore() {
#if defined(__unix__) || defined(__APPLE__)
    if (lock_fd_ >= 0) (void)::close(lock_fd_);
#endif
}

Status DedupStore::AcquireLock() {
#if defined(__unix__) || defined(__APPLE__)
    MINO_RETURN_IF_ERROR(EnsureParentDirectory(std::filesystem::path(path_)));
    const std::string lock_path = path_ + ".lock";
    int flags = O_RDWR | O_CREAT | CloseOnExecFlag();
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    const int raw_fd = ::open(lock_path.c_str(), flags, 0600);
    if (raw_fd < 0) return ErrnoStatus("open(dedup lock) failed", lock_path);
    ScopedFd fd(raw_fd);
    MINO_RETURN_IF_ERROR(SetCloseOnExec(fd.get(), lock_path));
    struct stat info {};
    if (::fstat(fd.get(), &info) != 0) return ErrnoStatus("stat(dedup lock) failed", lock_path);
    if (!S_ISREG(info.st_mode)) return Invalid("dedup lock must be a regular file");
    while (::flock(fd.get(), LOCK_EX | LOCK_NB) != 0) {
        if (errno == EINTR) continue;
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "dedup snapshot already has an owner");
        }
        return ErrnoStatus("flock(dedup lock) failed", lock_path);
    }
    lock_fd_ = fd.release();
    return Status::Ok();
#else
    return Status::Error(StatusCode::kUnsupported);
#endif
}

Result<std::unique_ptr<DedupStore>> DedupStore::Open(
    DedupStoreOptions options) noexcept {
    try {
        if (options.path.empty()) {
            return Invalid("dedup store path must be non-empty");
        }
        if (options.max_sources == 0 ||
            options.max_sources > std::numeric_limits<uint32_t>::max() ||
            options.max_sources > (std::numeric_limits<size_t>::max() - kHeaderBytes - kCrcBytes) / kEntryBytes) {
            return Invalid("dedup store max_sources is outside snapshot bounds");
        }
#if defined(__unix__) || defined(__APPLE__)
        auto store = std::unique_ptr<DedupStore>(
            new DedupStore(std::move(options.path), options.max_sources));
        store->test_hooks_ = options.test_hooks;
        MINO_RETURN_IF_ERROR(store->AcquireLock());
        MINO_RETURN_IF_ERROR(store->LoadFromDisk());
        return store;
#else
        return Status::Error(StatusCode::kUnsupported,
                             "dedup store requires durable POSIX file I/O");
#endif
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::filesystem::filesystem_error&) {
        return Status::Error(StatusCode::kInternal, "dedup store open filesystem failure");
    }
}

Status DedupStore::LoadFromDisk() noexcept {
#if defined(__unix__) || defined(__APPLE__)
    try {
        const std::filesystem::path path(path_);
        MINO_RETURN_IF_ERROR(EnsureParentDirectory(path));

        int open_flags = O_RDONLY | CloseOnExecFlag();
#ifdef O_NOFOLLOW
        open_flags |= O_NOFOLLOW;
#endif
        const int raw_fd = ::open(path_.c_str(), open_flags);
        if (raw_fd < 0) {
            if (errno == ENOENT) {
                entries_.clear();
                return Status::Ok();
            }
            return ErrnoStatus("open(dedup store) failed", path_);
        }
        ScopedFd fd(raw_fd);
        MINO_RETURN_IF_ERROR(SetCloseOnExec(fd.get(), path_));

        const size_t max_bytes =
            kHeaderBytes + max_sources_ * kEntryBytes + kCrcBytes;
        std::vector<std::byte> bytes;
        MINO_RETURN_IF_ERROR(ReadAll(fd.get(), &bytes, path_, max_bytes));
        if (bytes.empty()) {
            // A zero-length file is treated as corrupt rather than empty so a
            // crash mid-create cannot silently drop prior durable state after a
            // partial replace that truncated the live path (rename is atomic,
            // but operators may create empty placeholders).
            return Corruption("dedup store file is empty");
        }
        MINO_RETURN_IF_ERROR(DecodeSnapshot(bytes, max_sources_, &entries_, &retired_));
        // A previous owner may have renamed successfully and then failed or
        // died before directory fsync. Do not advertise that visible HWM as
        // durable on reopen until the rename is committed (even for a no-op
        // RecordAcceptedBatch or SessionHello).
        if (test_hooks_ != nullptr) {
            MINO_RETURN_IF_ERROR(test_hooks_->Checkpoint(
                DedupStoreIoStage::kRecoveryDirectorySync, false));
        }
        const auto parent = path.parent_path();
        MINO_RETURN_IF_ERROR(SyncDirectory(parent.empty() ? "." : parent));
        if (test_hooks_ != nullptr) {
            MINO_RETURN_IF_ERROR(test_hooks_->Checkpoint(
                DedupStoreIoStage::kRecoveryDirectorySync, true));
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
#else
    return Status::Error(StatusCode::kUnsupported,
                         "dedup store requires durable POSIX file I/O");
#endif
}

Result<std::vector<DedupResumeEntry>> DedupStore::Load() const noexcept {
    std::lock_guard lock(mutex_);
    if (retirement_failed_) return Status::Error(StatusCode::kUnavailable);
    try {
        std::vector<DedupResumeEntry> snapshot;
        snapshot.reserve(entries_.size());
        for (const auto& [source, highest] : entries_) {
            snapshot.push_back(DedupResumeEntry{
                .source = source,
                .highest_contiguous_sequence = highest,
            });
        }
        std::sort(snapshot.begin(), snapshot.end(),
                  [](const DedupResumeEntry& left,
                     const DedupResumeEntry& right) {
                      return SourceLess(left.source, right.source);
                  });
        return snapshot;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

uint64_t DedupStore::HighestAccepted(const SourceIdentity& source) const noexcept {
    std::lock_guard lock(mutex_);
    const auto found = entries_.find(source);
    return found == entries_.end() ? 0 : found->second;
}

DedupStoreStats DedupStore::stats() const noexcept {
    std::lock_guard lock(mutex_);
    auto result = stats_;
    result.sources = entries_.size() + retired_.size();
    result.retired_publishers = retired_.size();
    result.capacity = max_sources_;
    result.persistence_count = persistence_count_;
    return result;
}

Status DedupStore::PersistLocked() noexcept {
    const auto begin = std::chrono::steady_clock::now();
    const Status status = [this]() noexcept -> Status {
#if defined(__unix__) || defined(__APPLE__)
        try {
            MINO_ASSIGN_OR_RETURN(auto bytes, EncodeSnapshot(entries_, retired_, max_sources_));
            return AtomicPublish(std::filesystem::path(path_), bytes, test_hooks_);
        } catch (const std::bad_alloc&) {
            return Status::Error(StatusCode::kResourceExhausted);
        } catch (const std::length_error&) {
            return Status::Error(StatusCode::kResourceExhausted);
        } catch (const std::filesystem::filesystem_error&) {
            return Status::Error(StatusCode::kInternal, "dedup store filesystem failure");
        }
#else
        return Status::Error(StatusCode::kUnsupported,
                             "dedup store requires durable POSIX file I/O");
#endif
    }();
    const auto elapsed = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - begin).count());
    stats_.persistence_total_ns += elapsed;
    stats_.persistence_max_ns = std::max(stats_.persistence_max_ns, elapsed);
    if (status.ok()) ++persistence_count_;
    else ++stats_.persistence_failures;
    return status;
}

Status DedupStore::RecordAccepted(
    const SourceIdentity& source,
    uint64_t highest_contiguous_sequence) noexcept {
    const DedupResumeEntry entry{source, highest_contiguous_sequence};
    return RecordAcceptedBatch(std::span(&entry, 1));
}

Status DedupStore::RecordAcceptedBatch(
    std::span<const DedupResumeEntry> updates) noexcept {
    std::lock_guard lock(mutex_);
    if (retirement_failed_) return Status::Error(StatusCode::kUnavailable);
    try {
        bool changed = false;
        for (const auto& entry : updates) {
            if (!ValidSource(entry.source) || entry.highest_contiguous_sequence == 0) {
                return Invalid("dedup store update identity/sequence is invalid");
            }
            if (Retired(retired_, entry.source)) {
                return Status::Error(StatusCode::kPermissionDenied, "publisher epoch is retired");
            }
            const auto it = entries_.find(entry.source);
            changed |= it == entries_.end() ||
                entry.highest_contiguous_sequence > it->second;
        }
        if (!changed) return Status::Ok();
        // Build before swapping so allocation/capacity failures are atomic too.
        auto replacement = entries_;
        for (const auto& entry : updates) {
            auto it = replacement.find(entry.source);
            if (it == replacement.end()) {
                if (replacement.size() + retired_.size() >= max_sources_) {
                    ++stats_.capacity_rejections;
                    return Resource("dedup store is full; increase capacity or retire fenced sources offline");
                }
                replacement.emplace(entry.source, entry.highest_contiguous_sequence);
            } else {
                it->second = std::max(it->second, entry.highest_contiguous_sequence);
            }
        }
        entries_.swap(replacement);
        const Status persisted = PersistLocked();
        if (!persisted.ok()) entries_.swap(replacement);
        return persisted;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Status DedupStore::ReplaceAll(
    const std::vector<DedupResumeEntry>& entries) noexcept {
    std::lock_guard lock(mutex_);
    if (retirement_failed_ || attached_pipelines_ != 0) {
        return Status::Error(StatusCode::kUnavailable);
    }
    try {
        if (entries.size() > max_sources_ - retired_.size()) {
            ++stats_.capacity_rejections;
            return Resource("dedup store replace exceeds max_sources");
        }
        std::unordered_map<SourceIdentity, uint64_t, SourceIdentityHash>
            replacement;
        replacement.reserve(entries.size());
        for (const DedupResumeEntry& entry : entries) {
            if (!ValidSource(entry.source) ||
                entry.highest_contiguous_sequence == 0) {
                return Invalid("dedup store replace entry is invalid");
            }
            if (Retired(retired_, entry.source)) {
                return Status::Error(StatusCode::kPermissionDenied, "publisher epoch is retired");
            }
            const auto inserted = replacement.emplace(
                entry.source, entry.highest_contiguous_sequence);
            if (!inserted.second) {
                return Invalid("dedup store replace contains a duplicate source");
            }
        }
        auto previous = std::move(entries_);
        entries_ = std::move(replacement);
        const Status persisted = PersistLocked();
        if (!persisted.ok()) {
            entries_ = std::move(previous);
            return persisted;
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

bool DedupStore::IsRetired(const SourceIdentity& source) const noexcept {
    std::lock_guard lock(mutex_);
    // An uncertain commit cannot authorize traffic; callers must reopen.
    return retirement_failed_ || Retired(retired_, source);
}

Status DedupStore::AttachPipeline() noexcept {
    std::lock_guard lock(mutex_);
    if (retirement_failed_) return Status::Error(StatusCode::kUnavailable);
    if (attached_pipelines_ == std::numeric_limits<size_t>::max()) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
    ++attached_pipelines_;
    return Status::Ok();
}

void DedupStore::DetachPipeline() noexcept {
    std::lock_guard lock(mutex_);
    --attached_pipelines_;
}

Status DedupStore::RetireEpochsThrough(const SourceIdentity& last_retired) noexcept {
    std::lock_guard lock(mutex_);
    if (retirement_failed_ || attached_pipelines_ != 0) {
        return Status::Error(StatusCode::kUnavailable);
    }
    try {
        if (!ValidSource(last_retired)) return Invalid("retirement identity must be nonzero");
        if (Retired(retired_, last_retired)) return Status::Ok();
        auto replacement = entries_;
        auto boundaries = retired_;
        boundaries.insert_or_assign(PublisherKey(last_retired), last_retired.publisher_epoch);
        std::erase_if(replacement, [&](const auto& entry) {
            return Retired(boundaries, entry.first);
        });
        if (replacement.size() > max_sources_ ||
            boundaries.size() > max_sources_ - replacement.size()) {
            ++stats_.capacity_rejections;
            return Resource("dedup store has no capacity for retirement boundary");
        }
        entries_.swap(replacement);
        retired_.swap(boundaries);
        const Status persisted = PersistLocked();
        if (!persisted.ok()) {
            entries_.swap(replacement);
            retired_.swap(boundaries);
            // In particular, directory-sync failure can leave the NEW boundary
            // on disk. Do not allow another write to resurrect its retired HWMs.
            retirement_failed_ = true;
        }
        return persisted;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Status SeedDedupWindowFromStore(DedupWindow* window, DedupStore* store,
                                uint64_t peer_session_epoch,
                                uint64_t now_ns, uint16_t lane_index,
                                uint16_t lane_count) noexcept {
    if (window == nullptr || store == nullptr) {
        return Invalid("dedup store seed requires window and store");
    }
    if (lane_count == 0 || lane_count > kMaxBridgeLaneCount ||
        lane_index >= lane_count) {
        return Invalid("dedup store seed lane is invalid");
    }
    MINO_ASSIGN_OR_RETURN(auto snapshot, store->Load());
    for (const DedupResumeEntry& entry : snapshot) {
        if (BridgeLaneFor(entry.source, lane_count) != lane_index) continue;
        MINO_RETURN_IF_ERROR(window->SeedAccepted(
            peer_session_epoch, entry.source,
            entry.highest_contiguous_sequence, now_ns));
    }
    return Status::Ok();
}

}  // namespace mino::bridge
