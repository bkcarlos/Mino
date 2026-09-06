// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/dedup_store.h"

#include <algorithm>
#include <cerrno>
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
#include <sys/types.h>
#include <unistd.h>
#endif

#include "mino/bridge/crc32c.h"

namespace mino::bridge {
namespace {

constexpr char kMagic[8] = {'M', 'I', 'N', 'O', 'D', 'E', 'D', 'U'};
constexpr uint32_t kFormatVersion = 1;
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
    const std::filesystem::path directory = file_path.parent_path();
    if (directory.empty()) return Status::Ok();
    std::error_code error;
    const bool existed = std::filesystem::exists(directory, error);
    if (error) {
        return Status::Error(StatusCode::kInternal,
                             "cannot inspect dedup store directory '" +
                                 directory.string() + "': " + error.message());
    }
    if (!existed) {
        const bool created =
            std::filesystem::create_directories(directory, error);
        if (error ||
            (!created && !std::filesystem::is_directory(directory, error))) {
            return Status::Error(StatusCode::kInternal,
                                 "cannot create dedup store directory '" +
                                     directory.string() + "': " +
                                     error.message());
        }
        if (created) {
            if (::chmod(directory.c_str(), 0700) != 0) {
                return ErrnoStatus("chmod(dedup store directory) failed",
                                   directory.string());
            }
            MINO_RETURN_IF_ERROR(SyncDirectory(directory));
            const std::filesystem::path parent = directory.parent_path();
            if (!parent.empty()) MINO_RETURN_IF_ERROR(SyncDirectory(parent));
        }
    } else if (!std::filesystem::is_directory(directory, error) || error) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "dedup store parent path is not a directory: '" +
                                 directory.string() + "'");
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
    std::unordered_map<SourceIdentity, uint64_t, SourceIdentityHash>* out) {
    if (bytes.size() < kHeaderBytes + kCrcBytes) {
        return Corruption("dedup store snapshot is truncated");
    }
    for (size_t i = 0; i < sizeof(kMagic); ++i) {
        if (static_cast<char>(bytes[i]) != kMagic[i]) {
            return Corruption("dedup store magic mismatch");
        }
    }
    const uint32_t version = ReadBe32(bytes, 8);
    if (version != kFormatVersion) {
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
        if (!ValidSource(source) || highest == 0) {
            return Corruption("dedup store contains an invalid entry");
        }
        if (have_previous && !SourceLess(previous, source)) {
            return Corruption("dedup store entries are not strictly ordered");
        }
        const auto inserted = out->emplace(source, highest);
        if (!inserted.second) {
            return Corruption("dedup store contains a duplicate source");
        }
        previous = source;
        have_previous = true;
    }
    return Status::Ok();
}

Result<std::vector<std::byte>> EncodeSnapshot(
    const std::unordered_map<SourceIdentity, uint64_t, SourceIdentityHash>&
        entries,
    size_t max_sources) {
    try {
        if (entries.size() > max_sources) {
            return Resource("dedup store would exceed max_sources");
        }
        if (entries.size() > std::numeric_limits<uint32_t>::max()) {
            return Resource("dedup store entry count overflows");
        }
        std::vector<DedupResumeEntry> ordered;
        ordered.reserve(entries.size());
        for (const auto& [source, highest] : entries) {
            ordered.push_back(DedupResumeEntry{
                .source = source,
                .highest_contiguous_sequence = highest,
            });
        }
        std::sort(ordered.begin(), ordered.end(),
                  [](const DedupResumeEntry& left,
                     const DedupResumeEntry& right) {
                      return SourceLess(left.source, right.source);
                  });

        std::vector<std::byte> bytes;
        bytes.reserve(kHeaderBytes + ordered.size() * kEntryBytes + kCrcBytes);
        for (char c : kMagic) bytes.push_back(static_cast<std::byte>(c));
        WriteBe32(&bytes, kFormatVersion);
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
                     std::span<const std::byte> bytes) {
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
    MINO_RETURN_IF_ERROR(WriteExact(fd.get(), bytes.data(), bytes.size(), tmp));
    if (::ftruncate(fd.get(), static_cast<off_t>(bytes.size())) != 0) {
        return ErrnoStatus("ftruncate(dedup store tmp) failed", tmp);
    }
    MINO_RETURN_IF_ERROR(SyncFile(fd.get(), tmp));
    fd = ScopedFd();  // close before rename

    while (::rename(tmp.c_str(), final_path.c_str()) != 0) {
        if (errno == EINTR) continue;
        (void)::unlink(tmp.c_str());
        return ErrnoStatus("rename(dedup store) failed", final_path);
    }
    const std::filesystem::path parent = path.parent_path();
    if (!parent.empty()) {
        MINO_RETURN_IF_ERROR(SyncDirectory(parent));
    }
    return Status::Ok();
}
#endif  // unix

}  // namespace

DedupStore::DedupStore(std::string path, size_t max_sources) noexcept
    : path_(std::move(path)), max_sources_(max_sources) {}

Result<std::unique_ptr<DedupStore>> DedupStore::Open(
    DedupStoreOptions options) noexcept {
    try {
        if (options.path.empty()) {
            return Invalid("dedup store path must be non-empty");
        }
        if (options.max_sources == 0) {
            return Invalid("dedup store max_sources must be nonzero");
        }
#if defined(__unix__) || defined(__APPLE__)
        auto store = std::unique_ptr<DedupStore>(
            new DedupStore(std::move(options.path), options.max_sources));
        MINO_RETURN_IF_ERROR(store->LoadFromDisk());
        return store;
#else
        return Status::Error(StatusCode::kUnsupported,
                             "dedup store requires durable POSIX file I/O");
#endif
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
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
        return DecodeSnapshot(bytes, max_sources_, &entries_);
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

Status DedupStore::PersistLocked() noexcept {
#if defined(__unix__) || defined(__APPLE__)
    MINO_ASSIGN_OR_RETURN(auto bytes, EncodeSnapshot(entries_, max_sources_));
    return AtomicPublish(std::filesystem::path(path_), bytes);
#else
    return Status::Error(StatusCode::kUnsupported,
                         "dedup store requires durable POSIX file I/O");
#endif
}

Status DedupStore::RecordAccepted(
    const SourceIdentity& source,
    uint64_t highest_contiguous_sequence) noexcept {
    try {
        if (!ValidSource(source)) {
            return Invalid("dedup store source identity is incomplete");
        }
        if (highest_contiguous_sequence == 0) {
            return Invalid("dedup store rejects sequence zero");
        }
        const auto it = entries_.find(source);
        if (it != entries_.end()) {
            if (highest_contiguous_sequence <= it->second) {
                return Status::Ok();
            }
            const uint64_t previous = it->second;
            it->second = highest_contiguous_sequence;
            const Status persisted = PersistLocked();
            if (!persisted.ok()) {
                it->second = previous;
                return persisted;
            }
            return Status::Ok();
        }
        if (entries_.size() >= max_sources_) {
            return Resource("dedup store is full");
        }
        entries_.emplace(source, highest_contiguous_sequence);
        const Status persisted = PersistLocked();
        if (!persisted.ok()) {
            entries_.erase(source);
            return persisted;
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Status DedupStore::ReplaceAll(
    const std::vector<DedupResumeEntry>& entries) noexcept {
    try {
        if (entries.size() > max_sources_) {
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

Status SeedDedupWindowFromStore(DedupWindow* window, DedupStore* store,
                                uint64_t peer_session_epoch,
                                uint64_t now_ns) noexcept {
    if (window == nullptr || store == nullptr) {
        return Invalid("dedup store seed requires window and store");
    }
    MINO_ASSIGN_OR_RETURN(auto snapshot, store->Load());
    for (const DedupResumeEntry& entry : snapshot) {
        MINO_RETURN_IF_ERROR(window->SeedAccepted(
            peer_session_epoch, entry.source,
            entry.highest_contiguous_sequence, now_ns));
    }
    return Status::Ok();
}

}  // namespace mino::bridge
