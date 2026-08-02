// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recording_manifest.h"

#include <sys/file.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mino/bridge/crc32c.h"
#include "mino/common/status.h"

namespace mino::storage {
namespace {

constexpr std::array<std::byte, 8> kRecordingMagic = {
    std::byte{'M'}, std::byte{'I'}, std::byte{'N'}, std::byte{'O'},
    std::byte{'R'}, std::byte{'E'}, std::byte{'C'}, std::byte{'M'},
};
constexpr std::array<std::byte, 8> kPartitionMagic = {
    std::byte{'M'}, std::byte{'I'}, std::byte{'N'}, std::byte{'O'},
    std::byte{'P'}, std::byte{'A'}, std::byte{'R'}, std::byte{'M'},
};
constexpr size_t kRecordingHeaderSize = 76;
constexpr size_t kRecordingCrcOffset = 72;
constexpr size_t kPartitionHeaderSize = 104;
constexpr size_t kPartitionCrcOffset = 100;
constexpr size_t kTopicFixedSize = 24;
constexpr size_t kSchemaFixedSize = 48;
constexpr size_t kSegmentFixedSize = 64;
constexpr uint64_t kEncodedSegmentHeaderSize = 52;
constexpr size_t kHardMaxManifestBytes = 256u * 1024u * 1024u;
constexpr size_t kHardMaxEntries = 1u << 20;
constexpr size_t kHardMaxStringBytes = 1u << 20;
constexpr std::string_view kManifestFilename = "manifest";
constexpr std::string_view kOwnerLockFilename = ".manifest.owner.lock";

class FileDescriptor {
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
    int fd_;
};

struct TemporaryFile {
    std::filesystem::path path;
    FileDescriptor descriptor;
    ~TemporaryFile() {
        if (path.empty()) return;
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
    }
    TemporaryFile(std::filesystem::path value, FileDescriptor fd) noexcept
        : path(std::move(value)), descriptor(std::move(fd)) {}
    TemporaryFile(const TemporaryFile&) = delete;
    TemporaryFile& operator=(const TemporaryFile&) = delete;
    TemporaryFile(TemporaryFile&&) = default;
    TemporaryFile& operator=(TemporaryFile&&) = default;
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

Status IoError(std::string_view operation, const std::filesystem::path& path) {
    const int error = errno;
    return Status::Error(
        error == EACCES || error == EPERM ? StatusCode::kPermissionDenied
                                         : StatusCode::kUnavailable,
        std::string(operation) + " '" + path.string() + "': " +
            std::strerror(error));
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
        return Unavailable("cannot set close-on-exec flag");
    }
    return Status::Ok();
#endif
}

Status ValidateLimits(const ManifestLimits& limits) {
    if (limits.max_manifest_bytes < kPartitionHeaderSize ||
        limits.max_manifest_bytes > kHardMaxManifestBytes) {
        return Invalid("manifest byte limit is out of bounds");
    }
    if (limits.max_topics == 0 || limits.max_topics > kHardMaxEntries ||
        limits.max_schemas_per_topic == 0 ||
        limits.max_schemas_per_topic > kHardMaxEntries ||
        limits.max_segments == 0 || limits.max_segments > kHardMaxEntries) {
        return Invalid("manifest entry limit is out of bounds");
    }
    if (limits.max_topic_name_bytes == 0 ||
        limits.max_topic_name_bytes > kHardMaxStringBytes ||
        limits.max_relative_path_bytes == 0 ||
        limits.max_relative_path_bytes > kHardMaxStringBytes) {
        return Invalid("manifest string limit is out of bounds");
    }
    return Status::Ok();
}

bool AddSize(size_t value, size_t addition, size_t* result) noexcept {
    if (addition > std::numeric_limits<size_t>::max() - value) return false;
    *result = value + addition;
    return true;
}

void AppendU8(std::vector<std::byte>* out, uint8_t value) {
    out->push_back(static_cast<std::byte>(value));
}
void AppendU16(std::vector<std::byte>* out, uint16_t value) {
    for (size_t index = 0; index < 2; ++index) {
        out->push_back(static_cast<std::byte>(value & 0xffu));
        value >>= 8;
    }
}
void AppendU32(std::vector<std::byte>* out, uint32_t value) {
    for (size_t index = 0; index < 4; ++index) {
        out->push_back(static_cast<std::byte>(value & 0xffu));
        value >>= 8;
    }
}
void AppendU64(std::vector<std::byte>* out, uint64_t value) {
    for (size_t index = 0; index < 8; ++index) {
        out->push_back(static_cast<std::byte>(value & 0xffu));
        value >>= 8;
    }
}
void WriteU32(std::vector<std::byte>* out, size_t offset,
              uint32_t value) noexcept {
    for (size_t index = 0; index < 4; ++index) {
        (*out)[offset + index] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
}

class Cursor {
public:
    explicit Cursor(std::span<const std::byte> bytes, size_t offset = 0) noexcept
        : bytes_(bytes), offset_(offset) {}

    Result<uint8_t> U8() noexcept {
        if (!Available(1)) return Corruption("manifest is truncated");
        return static_cast<uint8_t>(bytes_[offset_++]);
    }
    Result<uint16_t> U16() noexcept {
        if (!Available(2)) return Corruption("manifest is truncated");
        uint16_t value = 0;
        for (size_t index = 0; index < 2; ++index) {
            value |= static_cast<uint16_t>(
                         static_cast<uint8_t>(bytes_[offset_ + index]))
                     << (index * 8);
        }
        offset_ += 2;
        return value;
    }
    Result<uint32_t> U32() noexcept {
        if (!Available(4)) return Corruption("manifest is truncated");
        uint32_t value = 0;
        for (size_t index = 0; index < 4; ++index) {
            value |= static_cast<uint32_t>(
                         static_cast<uint8_t>(bytes_[offset_ + index]))
                     << (index * 8);
        }
        offset_ += 4;
        return value;
    }
    Result<uint64_t> U64() noexcept {
        if (!Available(8)) return Corruption("manifest is truncated");
        uint64_t value = 0;
        for (size_t index = 0; index < 8; ++index) {
            value |= static_cast<uint64_t>(
                         static_cast<uint8_t>(bytes_[offset_ + index]))
                     << (index * 8);
        }
        offset_ += 8;
        return value;
    }
    Result<std::string> String(size_t length, size_t limit) {
        if (length > limit) return Exhausted("manifest string exceeds limit");
        if (!Available(length)) return Corruption("manifest string is truncated");
        const char* begin = reinterpret_cast<const char*>(bytes_.data() + offset_);
        std::string value(begin, length);
        offset_ += length;
        return value;
    }
    Result<std::array<std::byte, kSchemaDigestSize>> Digest() noexcept {
        if (!Available(kSchemaDigestSize)) {
            return Corruption("schema digest is truncated");
        }
        std::array<std::byte, kSchemaDigestSize> value{};
        std::copy_n(bytes_.begin() + static_cast<std::ptrdiff_t>(offset_),
                    kSchemaDigestSize, value.begin());
        offset_ += kSchemaDigestSize;
        return value;
    }
    Status Skip(size_t count) noexcept {
        if (!Available(count)) return Corruption("manifest is truncated");
        offset_ += count;
        return Status::Ok();
    }
    size_t offset() const noexcept { return offset_; }

private:
    bool Available(size_t count) const noexcept {
        return count <= bytes_.size() - std::min(offset_, bytes_.size());
    }
    std::span<const std::byte> bytes_;
    size_t offset_;
};

uint32_t ReadU32At(std::span<const std::byte> bytes, size_t offset) noexcept {
    uint32_t value = 0;
    for (size_t index = 0; index < 4; ++index) {
        value |= static_cast<uint32_t>(static_cast<uint8_t>(bytes[offset + index]))
                 << (index * 8);
    }
    return value;
}
uint64_t ReadU64At(std::span<const std::byte> bytes, size_t offset) noexcept {
    uint64_t value = 0;
    for (size_t index = 0; index < 8; ++index) {
        value |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[offset + index]))
                 << (index * 8);
    }
    return value;
}

uint32_t ManifestCrc(std::span<const std::byte> bytes, size_t crc_offset) {
    bridge::Crc32cAccumulator crc;
    crc.Update(bytes.first(crc_offset));
    constexpr std::array<std::byte, 4> zeros{};
    crc.Update(zeros);
    crc.Update(bytes.subspan(crc_offset + 4));
    return crc.Finish();
}

std::string DigestHex(
    const std::array<std::byte, kSchemaDigestSize>& digest) {
    constexpr std::string_view digits = "0123456789abcdef";
    std::string result;
    result.reserve(kSchemaDigestSize * 2);
    for (std::byte byte : digest) {
        const uint8_t value = static_cast<uint8_t>(byte);
        result.push_back(digits[value >> 4]);
        result.push_back(digits[value & 0x0fu]);
    }
    return result;
}

Status ValidateBasicRelativePath(const std::filesystem::path& path,
                                 size_t max_bytes) {
    const std::string text = path.generic_string();
    if (text.empty() || text.size() > max_bytes || path.is_absolute() ||
        path.has_root_name() || text.find('\0') != std::string::npos) {
        return Invalid("manifest path is not a bounded relative path");
    }
    for (const std::filesystem::path& component : path) {
        if (component.empty() || component == "." || component == "..") {
            return Invalid("manifest path traversal is forbidden");
        }
    }
    if (path.lexically_normal().generic_string() != text) {
        return Invalid("manifest path is not canonical");
    }
    return Status::Ok();
}

Status ValidateSchemaPath(const SchemaRefSnapshot& schema, size_t max_bytes) {
    MINO_RETURN_IF_ERROR(
        ValidateBasicRelativePath(schema.descriptor_path, max_bytes));
    auto iterator = schema.descriptor_path.begin();
    if (iterator == schema.descriptor_path.end() || *iterator != "schemas") {
        return Invalid("schema path must be below schemas");
    }
    ++iterator;
    if (iterator == schema.descriptor_path.end()) {
        return Invalid("schema descriptor filename is missing");
    }
    const std::string filename = iterator->string();
    ++iterator;
    if (iterator != schema.descriptor_path.end() ||
        filename != DigestHex(schema.canonical_digest) + ".schema") {
        return Invalid("schema path does not match canonical digest");
    }
    return Status::Ok();
}

Status ValidateSegmentPath(const std::filesystem::path& path,
                           size_t max_bytes) {
    MINO_RETURN_IF_ERROR(ValidateBasicRelativePath(path, max_bytes));
    auto iterator = path.begin();
    if (iterator == path.end() || *iterator != "segments") {
        return Invalid("segment path must be below segments");
    }
    ++iterator;
    if (iterator == path.end()) return Invalid("segment filename is missing");
    const std::filesystem::path filename = *iterator;
    ++iterator;
    if (iterator != path.end() || filename.extension() != ".mino") {
        return Invalid("segment path must be segments/<name>.mino");
    }
    return Status::Ok();
}

bool ValidState(SegmentPersistentState state) noexcept {
    const uint8_t value = static_cast<uint8_t>(state);
    return value >= static_cast<uint8_t>(SegmentPersistentState::kCreating) &&
           value <= static_cast<uint8_t>(SegmentPersistentState::kDeleted);
}

bool AllowedTransition(SegmentPersistentState from,
                       SegmentPersistentState to) noexcept {
    switch (from) {
        case SegmentPersistentState::kCreating:
            return to == SegmentPersistentState::kOpen;
        case SegmentPersistentState::kOpen:
            return to == SegmentPersistentState::kSealed;
        case SegmentPersistentState::kSealed:
            return to == SegmentPersistentState::kIndexed;
        case SegmentPersistentState::kIndexed:
            return to == SegmentPersistentState::kRetained ||
                   to == SegmentPersistentState::kDeleted;
        case SegmentPersistentState::kRetained:
            return to == SegmentPersistentState::kDeleted;
        case SegmentPersistentState::kDeleted:
            return false;
    }
    return false;
}

Status ValidateRecordingSnapshot(const RecordingManifestSnapshot& snapshot,
                                 const ManifestLimits& limits) {
    MINO_RETURN_IF_ERROR(ValidateLimits(limits));
    if (snapshot.generation == 0 || snapshot.session.recording_id == 0 ||
        snapshot.session.owner_id == 0 || snapshot.session.owner_epoch == 0) {
        return Invalid("recording manifest metadata contains a zero identity");
    }
    if (snapshot.topics.size() > limits.max_topics) {
        return Exhausted("recording manifest has too many topics");
    }
    uint32_t previous_topic_id = 0;
    bool have_topic = false;
    std::set<std::string> names;
    for (const TopicTableEntry& topic : snapshot.topics) {
        if (topic.topic_id == 0 || topic.topic_name.empty() ||
            topic.topic_name.size() > limits.max_topic_name_bytes ||
            topic.topic_name.find('\0') != std::string::npos) {
            return Invalid("topic identity is invalid");
        }
        if ((have_topic && topic.topic_id <= previous_topic_id) ||
            !names.insert(topic.topic_name).second) {
            return Corruption("topic ID/name mapping is duplicate or unordered");
        }
        have_topic = true;
        previous_topic_id = topic.topic_id;
        if (topic.schema_snapshot.size() > limits.max_schemas_per_topic) {
            return Exhausted("topic schema snapshot exceeds entry limit");
        }
        uint32_t previous_ref = 0;
        std::set<std::array<std::byte, kSchemaDigestSize>> digests;
        std::set<std::string> paths;
        for (const SchemaRefSnapshot& schema : topic.schema_snapshot) {
            if (schema.schema_ref == 0 || schema.schema_ref <= previous_ref) {
                return Corruption("schema refs are duplicate or not increasing");
            }
            MINO_RETURN_IF_ERROR(
                ValidateSchemaPath(schema, limits.max_relative_path_bytes));
            if (!digests.insert(schema.canonical_digest).second ||
                !paths.insert(schema.descriptor_path.generic_string()).second) {
                return Corruption("schema snapshot contains a duplicate");
            }
            previous_ref = schema.schema_ref;
        }
    }
    return Status::Ok();
}

Status ValidatePartitionSnapshot(const PartitionManifestSnapshot& snapshot,
                                 const ManifestLimits& limits) {
    MINO_RETURN_IF_ERROR(ValidateLimits(limits));
    if (snapshot.generation == 0 || snapshot.partition.recording_id == 0 ||
        snapshot.partition.topic_id == 0 || snapshot.partition.writer_id == 0 ||
        snapshot.partition.owner_epoch == 0) {
        return Invalid("partition manifest metadata contains a zero identity");
    }
    if (snapshot.segments.size() > limits.max_segments) {
        return Exhausted("partition manifest has too many segments");
    }
    uint64_t previous_id = 0;
    uint64_t previous_last = 0;
    bool have_segment = false;
    bool active = false;
    std::set<std::string> paths;
    for (size_t index = 0; index < snapshot.segments.size(); ++index) {
        const SegmentManifestEntry& segment = snapshot.segments[index];
        if (segment.segment_id == 0 || !ValidState(segment.state) ||
            segment.last_ingestion_sequence <
                segment.first_ingestion_sequence) {
            return Corruption("segment metadata is invalid");
        }
        if ((have_segment && segment.segment_id <= previous_id) ||
            (have_segment &&
             segment.first_ingestion_sequence <= previous_last)) {
            return Corruption("segments are duplicate, overlapping, or unordered");
        }
        MINO_RETURN_IF_ERROR(
            ValidateSegmentPath(segment.relative_path,
                                limits.max_relative_path_bytes));
        if (!paths.insert(segment.relative_path.generic_string()).second) {
            return Corruption("segment path is duplicated");
        }
        const bool is_active =
            segment.state == SegmentPersistentState::kCreating ||
            segment.state == SegmentPersistentState::kOpen;
        if (is_active && (active || index + 1 != snapshot.segments.size())) {
            return Corruption("only the final segment may be active");
        }
        active = active || is_active;
        if (static_cast<uint8_t>(segment.state) >=
            static_cast<uint8_t>(SegmentPersistentState::kSealed)) {
            if (segment.sealed_at_ns < segment.created_at_ns ||
                segment.size_bytes < kEncodedSegmentHeaderSize) {
                return Corruption("sealed segment metadata is incomplete");
            }
        } else if (segment.sealed_at_ns != 0) {
            return Corruption("unsealed segment has a sealed timestamp");
        }
        have_segment = true;
        previous_id = segment.segment_id;
        previous_last = segment.last_ingestion_sequence;
    }
    if (snapshot.checkpoint.has_value()) {
        const DurableCheckpoint& checkpoint = *snapshot.checkpoint;
        const auto found = std::find_if(
            snapshot.segments.begin(), snapshot.segments.end(),
            [&](const SegmentManifestEntry& entry) {
                return entry.segment_id == checkpoint.segment_id;
            });
        if (found == snapshot.segments.end() ||
            checkpoint.durable_offset < kEncodedSegmentHeaderSize ||
            checkpoint.durable_offset > found->size_bytes ||
            checkpoint.durable_sequence < found->first_ingestion_sequence ||
            checkpoint.durable_sequence > found->last_ingestion_sequence) {
            return Corruption("durable checkpoint is outside its segment");
        }
    }
    return Status::Ok();
}

Status VerifyDirectory(const std::filesystem::path& path) {
    struct stat info {};
    if (::lstat(path.c_str(), &info) != 0) return IoError("cannot inspect", path);
    if (S_ISLNK(info.st_mode) || !S_ISDIR(info.st_mode)) {
        return Invalid("manifest root is not a real directory");
    }
    return Status::Ok();
}

Status InspectRelativePath(const std::filesystem::path& root,
                           const std::filesystem::path& relative,
                           bool require_regular_file) {
    std::filesystem::path current = root;
    size_t index = 0;
    const size_t count = static_cast<size_t>(
        std::distance(relative.begin(), relative.end()));
    for (const std::filesystem::path& component : relative) {
        ++index;
        current /= component;
        struct stat info {};
        if (::lstat(current.c_str(), &info) != 0) {
            if (errno == ENOENT && !require_regular_file) return Status::Ok();
            return IoError("cannot inspect manifest path", current);
        }
        if (S_ISLNK(info.st_mode)) {
            return Corruption("manifest path resolves through a symlink");
        }
        if (index < count && !S_ISDIR(info.st_mode)) {
            return Corruption("manifest path parent is not a directory");
        }
        if (index == count && !S_ISREG(info.st_mode)) {
            return Corruption("manifest path is not a regular file");
        }
    }
    return Status::Ok();
}

Status VerifySnapshotPaths(const std::filesystem::path& root,
                           const RecordingManifestSnapshot& snapshot) {
    for (const TopicTableEntry& topic : snapshot.topics) {
        for (const SchemaRefSnapshot& schema : topic.schema_snapshot) {
            MINO_RETURN_IF_ERROR(InspectRelativePath(
                root, schema.descriptor_path, false));
        }
    }
    return Status::Ok();
}

Status VerifySnapshotPaths(const std::filesystem::path& root,
                           const PartitionManifestSnapshot& snapshot) {
    for (const SegmentManifestEntry& segment : snapshot.segments) {
        MINO_RETURN_IF_ERROR(InspectRelativePath(
            root, segment.relative_path, false));
    }
    return Status::Ok();
}

Result<FileDescriptor> AcquireOwnerLock(const std::filesystem::path& root) {
    const std::filesystem::path path = root / kOwnerLockFilename;
    FileDescriptor descriptor(
        ::open(path.c_str(), OpenFlags(O_RDWR | O_CREAT), 0644));
    if (descriptor.get() < 0) return IoError("cannot open owner lock", path);
    if (::flock(descriptor.get(), LOCK_EX | LOCK_NB) != 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            return Unavailable("manifest already has an owner");
        }
        return IoError("cannot acquire owner lock", path);
    }
    return descriptor;
}

Result<std::vector<std::byte>> ReadRegularFile(
    const std::filesystem::path& path, size_t max_bytes) {
    struct stat link_info {};
    if (::lstat(path.c_str(), &link_info) != 0) {
        if (errno == ENOENT) {
            return Status::Error(StatusCode::kNotFound,
                                 "manifest file does not exist");
        }
        return IoError("cannot inspect manifest", path);
    }
    if (S_ISLNK(link_info.st_mode) || !S_ISREG(link_info.st_mode)) {
        return Corruption("manifest is not a regular non-symlink file");
    }
    FileDescriptor descriptor(::open(path.c_str(), OpenFlags(O_RDONLY)));
    if (descriptor.get() < 0) return IoError("cannot open manifest", path);
    struct stat info {};
    if (::fstat(descriptor.get(), &info) != 0) {
        return IoError("cannot stat manifest", path);
    }
    if (!S_ISREG(info.st_mode)) return Corruption("manifest type changed");
    if (info.st_size < 0 || static_cast<uintmax_t>(info.st_size) > max_bytes) {
        return Exhausted("manifest exceeds configured byte limit");
    }
    std::vector<std::byte> bytes(static_cast<size_t>(info.st_size));
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t count =
            ::read(descriptor.get(), bytes.data() + offset, bytes.size() - offset);
        if (count < 0) {
            if (errno == EINTR) continue;
            return IoError("cannot read manifest", path);
        }
        if (count == 0) return Corruption("manifest is truncated");
        offset += static_cast<size_t>(count);
    }
    std::byte extra{};
    while (true) {
        const ssize_t count = ::read(descriptor.get(), &extra, 1);
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) return IoError("cannot finish reading manifest", path);
        if (count != 0) return Corruption("manifest grew while being read");
        break;
    }
    return bytes;
}

Result<TemporaryFile> CreateTemporaryFile(
    const std::filesystem::path& root) {
    std::filesystem::path pattern_path = root / ".manifest.tmp.XXXXXX";
    std::string pattern = pattern_path.string();
    pattern.push_back('\0');
    const int fd = ::mkstemp(pattern.data());
    if (fd < 0) return IoError("cannot create manifest temp file", pattern_path);
    FileDescriptor descriptor(fd);
    MINO_RETURN_IF_ERROR(SetCloseOnExec(fd));
    if (::fchmod(fd, 0644) != 0) {
        return IoError("cannot chmod manifest temp file", pattern_path);
    }
    return TemporaryFile(std::filesystem::path(pattern.c_str()),
                         std::move(descriptor));
}

Status WriteAll(int fd, const std::filesystem::path& path,
                std::span<const std::byte> bytes) {
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t count =
            ::write(fd, bytes.data() + offset, bytes.size() - offset);
        if (count < 0) {
            if (errno == EINTR) continue;
            return IoError("cannot write manifest", path);
        }
        if (count == 0) return Unavailable("zero-byte manifest write");
        offset += static_cast<size_t>(count);
    }
    return Status::Ok();
}

Status DataSync(int fd, const std::filesystem::path& path) {
#if defined(__APPLE__)
    if (::fsync(fd) != 0) return IoError("cannot sync manifest data", path);
#else
    if (::fdatasync(fd) != 0) return IoError("cannot fdatasync manifest", path);
#endif
    return Status::Ok();
}

Status SyncDirectory(const std::filesystem::path& path) {
    int flags = O_RDONLY;
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
    FileDescriptor descriptor(::open(path.c_str(), OpenFlags(flags)));
    if (descriptor.get() < 0) return IoError("cannot open directory", path);
    if (::fsync(descriptor.get()) != 0) {
        return IoError("cannot fsync directory", path);
    }
    return Status::Ok();
}

Status RunFaultHook(const ManifestOptions& options, ManifestFaultPoint point) {
    if (options.fault_hook == nullptr) return Status::Ok();
    return options.fault_hook(point, options.fault_hook_context);
}

Status AtomicWriteManifest(const std::filesystem::path& root,
                           std::span<const std::byte> bytes,
                           const ManifestOptions& options) {
    const std::filesystem::path manifest = root / kManifestFilename;
    struct stat existing {};
    if (::lstat(manifest.c_str(), &existing) == 0) {
        if (S_ISLNK(existing.st_mode) || !S_ISREG(existing.st_mode)) {
            return Corruption("manifest destination is not a regular file");
        }
    } else if (errno != ENOENT) {
        return IoError("cannot inspect manifest destination", manifest);
    }
    MINO_ASSIGN_OR_RETURN(TemporaryFile temporary, CreateTemporaryFile(root));
    MINO_RETURN_IF_ERROR(
        WriteAll(temporary.descriptor.get(), temporary.path, bytes));
    MINO_RETURN_IF_ERROR(
        RunFaultHook(options, ManifestFaultPoint::kAfterTempWrite));
    MINO_RETURN_IF_ERROR(DataSync(temporary.descriptor.get(), temporary.path));
    MINO_RETURN_IF_ERROR(
        RunFaultHook(options, ManifestFaultPoint::kAfterTempDataSync));
    if (::rename(temporary.path.c_str(), manifest.c_str()) != 0) {
        return IoError("cannot rename manifest", manifest);
    }
    temporary.path.clear();
    MINO_RETURN_IF_ERROR(RunFaultHook(options, ManifestFaultPoint::kAfterRename));
    MINO_RETURN_IF_ERROR(SyncDirectory(root));
    return RunFaultHook(options,
                        ManifestFaultPoint::kAfterParentDirectorySync);
}

Status ValidateEnvelope(std::span<const std::byte> encoded,
                        std::span<const std::byte> magic, uint16_t version,
                        size_t header_size, size_t crc_offset,
                        const ManifestLimits& limits) {
    MINO_RETURN_IF_ERROR(ValidateLimits(limits));
    if (encoded.size() < header_size) return Corruption("manifest is truncated");
    if (encoded.size() > limits.max_manifest_bytes) {
        return Exhausted("manifest exceeds configured byte limit");
    }
    if (!std::equal(magic.begin(), magic.end(), encoded.begin())) {
        return Corruption("manifest magic is invalid");
    }
    Cursor cursor(encoded, 8);
    MINO_ASSIGN_OR_RETURN(const uint16_t actual_version, cursor.U16());
    MINO_ASSIGN_OR_RETURN(const uint16_t actual_header_size, cursor.U16());
    if (actual_version != version) {
        return Status::Error(StatusCode::kUnsupported,
                             "manifest version is unsupported");
    }
    if (actual_header_size != header_size ||
        ReadU64At(encoded, 12) != encoded.size()) {
        return Corruption("manifest size fields are inconsistent");
    }
    if (ReadU32At(encoded, crc_offset) !=
        ManifestCrc(encoded, crc_offset)) {
        return Corruption("manifest CRC32C mismatch");
    }
    return Status::Ok();
}

Result<std::vector<std::byte>> EncodeRecordingInternal(
    const RecordingManifestSnapshot& snapshot, const ManifestLimits& limits) {
    MINO_RETURN_IF_ERROR(ValidateRecordingSnapshot(snapshot, limits));
    size_t total = kRecordingHeaderSize;
    for (const TopicTableEntry& topic : snapshot.topics) {
        if (!AddSize(total, kTopicFixedSize, &total) ||
            !AddSize(total, topic.topic_name.size(), &total)) {
            return Exhausted("recording manifest size overflows");
        }
        for (const SchemaRefSnapshot& schema : topic.schema_snapshot) {
            if (!AddSize(total, kSchemaFixedSize, &total) ||
                !AddSize(total, schema.descriptor_path.generic_string().size(),
                         &total)) {
                return Exhausted("recording manifest size overflows");
            }
        }
    }
    if (total > limits.max_manifest_bytes) {
        return Exhausted("recording manifest exceeds byte limit");
    }
    std::vector<std::byte> out;
    out.reserve(total);
    out.insert(out.end(), kRecordingMagic.begin(), kRecordingMagic.end());
    AppendU16(&out, kRecordingManifestFormatVersion);
    AppendU16(&out, static_cast<uint16_t>(kRecordingHeaderSize));
    AppendU64(&out, total);
    AppendU64(&out, snapshot.generation);
    AppendU64(&out, snapshot.session.recording_id);
    AppendU64(&out, snapshot.session.created_at_ns);
    AppendU64(&out, snapshot.session.owner_id);
    AppendU64(&out, snapshot.session.owner_epoch);
    AppendU64(&out, snapshot.session.config_version);
    AppendU32(&out, static_cast<uint32_t>(snapshot.topics.size()));
    AppendU32(&out, 0);
    for (const TopicTableEntry& topic : snapshot.topics) {
        AppendU32(&out, topic.topic_id);
        AppendU32(&out, static_cast<uint32_t>(topic.topic_name.size()));
        AppendU64(&out, topic.config_version);
        AppendU32(&out, static_cast<uint32_t>(topic.schema_snapshot.size()));
        AppendU32(&out, 0);
        const std::span<const char> topic_chars(topic.topic_name.data(),
                                                topic.topic_name.size());
        const std::span<const std::byte> topic_bytes =
            std::as_bytes(topic_chars);
        out.insert(out.end(), topic_bytes.begin(), topic_bytes.end());
        for (const SchemaRefSnapshot& schema : topic.schema_snapshot) {
            const std::string path = schema.descriptor_path.generic_string();
            AppendU32(&out, schema.schema_ref);
            AppendU32(&out, schema.schema_version);
            AppendU32(&out, schema.layout_version);
            AppendU32(&out, static_cast<uint32_t>(path.size()));
            out.insert(out.end(), schema.canonical_digest.begin(),
                       schema.canonical_digest.end());
            const std::span<const char> path_chars(path.data(), path.size());
            const std::span<const std::byte> path_bytes =
                std::as_bytes(path_chars);
            out.insert(out.end(), path_bytes.begin(), path_bytes.end());
        }
    }
    if (out.size() != total) return Corruption("recording encoder size mismatch");
    WriteU32(&out, kRecordingCrcOffset,
             ManifestCrc(out, kRecordingCrcOffset));
    return out;
}

Result<RecordingManifestSnapshot> DecodeRecordingInternal(
    std::span<const std::byte> encoded, const ManifestLimits& limits) {
    MINO_RETURN_IF_ERROR(ValidateEnvelope(
        encoded, kRecordingMagic, kRecordingManifestFormatVersion,
        kRecordingHeaderSize, kRecordingCrcOffset, limits));
    Cursor cursor(encoded, 20);
    RecordingManifestSnapshot snapshot;
    MINO_ASSIGN_OR_RETURN(snapshot.generation, cursor.U64());
    MINO_ASSIGN_OR_RETURN(snapshot.session.recording_id, cursor.U64());
    MINO_ASSIGN_OR_RETURN(snapshot.session.created_at_ns, cursor.U64());
    MINO_ASSIGN_OR_RETURN(snapshot.session.owner_id, cursor.U64());
    MINO_ASSIGN_OR_RETURN(snapshot.session.owner_epoch, cursor.U64());
    MINO_ASSIGN_OR_RETURN(snapshot.session.config_version, cursor.U64());
    MINO_ASSIGN_OR_RETURN(const uint32_t topic_count, cursor.U32());
    MINO_ASSIGN_OR_RETURN(const uint32_t header_crc, cursor.U32());
    static_cast<void>(header_crc);
    if (topic_count > limits.max_topics) {
        return Exhausted("recording manifest has too many topics");
    }
    snapshot.topics.reserve(topic_count);
    for (uint32_t topic_index = 0; topic_index < topic_count; ++topic_index) {
        TopicTableEntry topic;
        MINO_ASSIGN_OR_RETURN(topic.topic_id, cursor.U32());
        MINO_ASSIGN_OR_RETURN(const uint32_t name_length, cursor.U32());
        MINO_ASSIGN_OR_RETURN(topic.config_version, cursor.U64());
        MINO_ASSIGN_OR_RETURN(const uint32_t schema_count, cursor.U32());
        MINO_ASSIGN_OR_RETURN(const uint32_t reserved, cursor.U32());
        if (reserved != 0) return Corruption("topic reserved bits are non-zero");
        if (schema_count > limits.max_schemas_per_topic) {
            return Exhausted("topic schema snapshot exceeds entry limit");
        }
        MINO_ASSIGN_OR_RETURN(
            topic.topic_name,
            cursor.String(name_length, limits.max_topic_name_bytes));
        topic.schema_snapshot.reserve(schema_count);
        for (uint32_t schema_index = 0; schema_index < schema_count;
             ++schema_index) {
            SchemaRefSnapshot schema;
            MINO_ASSIGN_OR_RETURN(schema.schema_ref, cursor.U32());
            MINO_ASSIGN_OR_RETURN(schema.schema_version, cursor.U32());
            MINO_ASSIGN_OR_RETURN(schema.layout_version, cursor.U32());
            MINO_ASSIGN_OR_RETURN(const uint32_t path_length, cursor.U32());
            MINO_ASSIGN_OR_RETURN(schema.canonical_digest, cursor.Digest());
            MINO_ASSIGN_OR_RETURN(
                std::string path,
                cursor.String(path_length, limits.max_relative_path_bytes));
            schema.descriptor_path = std::filesystem::path(std::move(path));
            topic.schema_snapshot.push_back(std::move(schema));
        }
        snapshot.topics.push_back(std::move(topic));
    }
    if (cursor.offset() != encoded.size()) {
        return Corruption("recording manifest has trailing bytes");
    }
    MINO_RETURN_IF_ERROR(ValidateRecordingSnapshot(snapshot, limits));
    return snapshot;
}

Result<std::vector<std::byte>> EncodePartitionInternal(
    const PartitionManifestSnapshot& snapshot, const ManifestLimits& limits) {
    MINO_RETURN_IF_ERROR(ValidatePartitionSnapshot(snapshot, limits));
    size_t total = kPartitionHeaderSize;
    for (const SegmentManifestEntry& segment : snapshot.segments) {
        if (!AddSize(total, kSegmentFixedSize, &total) ||
            !AddSize(total, segment.relative_path.generic_string().size(),
                     &total)) {
            return Exhausted("partition manifest size overflows");
        }
    }
    if (total > limits.max_manifest_bytes) {
        return Exhausted("partition manifest exceeds byte limit");
    }
    std::vector<std::byte> out;
    out.reserve(total);
    out.insert(out.end(), kPartitionMagic.begin(), kPartitionMagic.end());
    AppendU16(&out, kPartitionManifestFormatVersion);
    AppendU16(&out, static_cast<uint16_t>(kPartitionHeaderSize));
    AppendU64(&out, total);
    AppendU64(&out, snapshot.generation);
    AppendU64(&out, snapshot.partition.recording_id);
    AppendU32(&out, snapshot.partition.topic_id);
    AppendU32(&out, snapshot.partition.partition_id);
    AppendU64(&out, snapshot.partition.writer_id);
    AppendU64(&out, snapshot.partition.owner_epoch);
    AppendU64(&out, snapshot.partition.config_version);
    AppendU32(&out, snapshot.checkpoint.has_value() ? 1u : 0u);
    AppendU32(&out, static_cast<uint32_t>(snapshot.segments.size()));
    AppendU64(&out, snapshot.checkpoint.has_value()
                        ? snapshot.checkpoint->segment_id
                        : 0);
    AppendU64(&out, snapshot.checkpoint.has_value()
                        ? snapshot.checkpoint->durable_offset
                        : 0);
    AppendU64(&out, snapshot.checkpoint.has_value()
                        ? snapshot.checkpoint->durable_sequence
                        : 0);
    AppendU32(&out, 0);
    for (const SegmentManifestEntry& segment : snapshot.segments) {
        const std::string path = segment.relative_path.generic_string();
        AppendU64(&out, segment.segment_id);
        AppendU8(&out, static_cast<uint8_t>(segment.state));
        for (size_t index = 0; index < 7; ++index) AppendU8(&out, 0);
        AppendU64(&out, segment.first_ingestion_sequence);
        AppendU64(&out, segment.last_ingestion_sequence);
        AppendU64(&out, segment.created_at_ns);
        AppendU64(&out, segment.sealed_at_ns);
        AppendU64(&out, segment.size_bytes);
        AppendU32(&out, static_cast<uint32_t>(path.size()));
        AppendU32(&out, 0);
        const std::span<const char> path_chars(path.data(), path.size());
        const std::span<const std::byte> path_bytes =
            std::as_bytes(path_chars);
        out.insert(out.end(), path_bytes.begin(), path_bytes.end());
    }
    if (out.size() != total) return Corruption("partition encoder size mismatch");
    WriteU32(&out, kPartitionCrcOffset,
             ManifestCrc(out, kPartitionCrcOffset));
    return out;
}

Result<PartitionManifestSnapshot> DecodePartitionInternal(
    std::span<const std::byte> encoded, const ManifestLimits& limits) {
    MINO_RETURN_IF_ERROR(ValidateEnvelope(
        encoded, kPartitionMagic, kPartitionManifestFormatVersion,
        kPartitionHeaderSize, kPartitionCrcOffset, limits));
    Cursor cursor(encoded, 20);
    PartitionManifestSnapshot snapshot;
    MINO_ASSIGN_OR_RETURN(snapshot.generation, cursor.U64());
    MINO_ASSIGN_OR_RETURN(snapshot.partition.recording_id, cursor.U64());
    MINO_ASSIGN_OR_RETURN(snapshot.partition.topic_id, cursor.U32());
    MINO_ASSIGN_OR_RETURN(snapshot.partition.partition_id, cursor.U32());
    MINO_ASSIGN_OR_RETURN(snapshot.partition.writer_id, cursor.U64());
    MINO_ASSIGN_OR_RETURN(snapshot.partition.owner_epoch, cursor.U64());
    MINO_ASSIGN_OR_RETURN(snapshot.partition.config_version, cursor.U64());
    MINO_ASSIGN_OR_RETURN(const uint32_t has_checkpoint, cursor.U32());
    MINO_ASSIGN_OR_RETURN(const uint32_t segment_count, cursor.U32());
    DurableCheckpoint checkpoint;
    MINO_ASSIGN_OR_RETURN(checkpoint.segment_id, cursor.U64());
    MINO_ASSIGN_OR_RETURN(checkpoint.durable_offset, cursor.U64());
    MINO_ASSIGN_OR_RETURN(checkpoint.durable_sequence, cursor.U64());
    MINO_ASSIGN_OR_RETURN(const uint32_t header_crc, cursor.U32());
    static_cast<void>(header_crc);
    if (has_checkpoint > 1) return Corruption("checkpoint flag is invalid");
    if (has_checkpoint == 0 &&
        (checkpoint.segment_id != 0 || checkpoint.durable_offset != 0 ||
         checkpoint.durable_sequence != 0)) {
        return Corruption("absent checkpoint contains values");
    }
    if (has_checkpoint != 0) snapshot.checkpoint = checkpoint;
    if (segment_count > limits.max_segments) {
        return Exhausted("partition manifest has too many segments");
    }
    snapshot.segments.reserve(segment_count);
    for (uint32_t segment_index = 0; segment_index < segment_count;
         ++segment_index) {
        SegmentManifestEntry segment;
        MINO_ASSIGN_OR_RETURN(segment.segment_id, cursor.U64());
        MINO_ASSIGN_OR_RETURN(const uint8_t state, cursor.U8());
        segment.state = static_cast<SegmentPersistentState>(state);
        for (size_t index = 0; index < 7; ++index) {
            MINO_ASSIGN_OR_RETURN(const uint8_t reserved_byte, cursor.U8());
            if (reserved_byte != 0) {
                return Corruption("segment reserved bits are non-zero");
            }
        }
        MINO_ASSIGN_OR_RETURN(segment.first_ingestion_sequence, cursor.U64());
        MINO_ASSIGN_OR_RETURN(segment.last_ingestion_sequence, cursor.U64());
        MINO_ASSIGN_OR_RETURN(segment.created_at_ns, cursor.U64());
        MINO_ASSIGN_OR_RETURN(segment.sealed_at_ns, cursor.U64());
        MINO_ASSIGN_OR_RETURN(segment.size_bytes, cursor.U64());
        MINO_ASSIGN_OR_RETURN(const uint32_t path_length, cursor.U32());
        MINO_ASSIGN_OR_RETURN(const uint32_t reserved, cursor.U32());
        if (reserved != 0) return Corruption("segment reserved field is non-zero");
        MINO_ASSIGN_OR_RETURN(
            std::string path,
            cursor.String(path_length, limits.max_relative_path_bytes));
        segment.relative_path = std::filesystem::path(std::move(path));
        snapshot.segments.push_back(std::move(segment));
    }
    if (cursor.offset() != encoded.size()) {
        return Corruption("partition manifest has trailing bytes");
    }
    MINO_RETURN_IF_ERROR(ValidatePartitionSnapshot(snapshot, limits));
    return snapshot;
}

Status AllocationFailure() {
    return Exhausted("manifest operation ran out of memory");
}

}  // namespace

Result<std::vector<std::byte>> EncodeRecordingManifest(
    const RecordingManifestSnapshot& snapshot,
    const ManifestLimits& limits) noexcept {
    try {
        return EncodeRecordingInternal(snapshot, limits);
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Result<RecordingManifestSnapshot> DecodeRecordingManifest(
    std::span<const std::byte> encoded, const ManifestLimits& limits) noexcept {
    try {
        return DecodeRecordingInternal(encoded, limits);
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Corruption(error.what());
    }
}

Result<std::vector<std::byte>> EncodePartitionManifest(
    const PartitionManifestSnapshot& snapshot,
    const ManifestLimits& limits) noexcept {
    try {
        return EncodePartitionInternal(snapshot, limits);
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Result<PartitionManifestSnapshot> DecodePartitionManifest(
    std::span<const std::byte> encoded, const ManifestLimits& limits) noexcept {
    try {
        return DecodePartitionInternal(encoded, limits);
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Corruption(error.what());
    }
}

RecordingManifest::RecordingManifest(
    std::filesystem::path root, ManifestOptions options, int lock_fd,
    RecordingManifestSnapshot snapshot) noexcept
    : root_(std::move(root)),
      options_(options),
      lock_fd_(lock_fd),
      snapshot_(std::move(snapshot)) {}

RecordingManifest::~RecordingManifest() {
    if (lock_fd_ >= 0) static_cast<void>(::close(lock_fd_));
}

Result<std::unique_ptr<RecordingManifest>> RecordingManifest::Create(
    const std::filesystem::path& session_root,
    const RecordingSessionMetadata& metadata,
    const ManifestOptions& options) noexcept {
    try {
        MINO_RETURN_IF_ERROR(VerifyDirectory(session_root));
        MINO_RETURN_IF_ERROR(ValidateLimits(options.limits));
        RecordingManifestSnapshot snapshot;
        snapshot.generation = 1;
        snapshot.session = metadata;
        MINO_RETURN_IF_ERROR(
            ValidateRecordingSnapshot(snapshot, options.limits));
        if (snapshot.generation < options.minimum_generation ||
            metadata.config_version < options.minimum_config_version) {
            return Corruption("new recording manifest is below recovery watermark");
        }
        MINO_ASSIGN_OR_RETURN(FileDescriptor lock,
                              AcquireOwnerLock(session_root));
        const std::filesystem::path path = session_root / kManifestFilename;
        struct stat existing {};
        if (::lstat(path.c_str(), &existing) == 0) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "recording manifest already exists");
        }
        if (errno != ENOENT) return IoError("cannot inspect manifest", path);
        MINO_ASSIGN_OR_RETURN(
            std::vector<std::byte> encoded,
            EncodeRecordingInternal(snapshot, options.limits));
        MINO_RETURN_IF_ERROR(
            AtomicWriteManifest(session_root, encoded, options));
        return std::unique_ptr<RecordingManifest>(new RecordingManifest(
            session_root, options, lock.release(), std::move(snapshot)));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Result<std::unique_ptr<RecordingManifest>> RecordingManifest::Open(
    const std::filesystem::path& session_root,
    const ManifestOptions& options) noexcept {
    try {
        MINO_RETURN_IF_ERROR(VerifyDirectory(session_root));
        MINO_RETURN_IF_ERROR(ValidateLimits(options.limits));
        MINO_ASSIGN_OR_RETURN(FileDescriptor lock,
                              AcquireOwnerLock(session_root));
        MINO_ASSIGN_OR_RETURN(
            std::vector<std::byte> encoded,
            ReadRegularFile(session_root / kManifestFilename,
                            options.limits.max_manifest_bytes));
        MINO_ASSIGN_OR_RETURN(
            RecordingManifestSnapshot snapshot,
            DecodeRecordingInternal(encoded, options.limits));
        if (snapshot.generation < options.minimum_generation ||
            snapshot.session.config_version < options.minimum_config_version) {
            return Corruption("recording manifest rollback detected");
        }
        MINO_RETURN_IF_ERROR(VerifySnapshotPaths(session_root, snapshot));
        return std::unique_ptr<RecordingManifest>(new RecordingManifest(
            session_root, options, lock.release(), std::move(snapshot)));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Corruption(error.what());
    }
}

Status RecordingManifest::Commit(RecordingManifestSnapshot next) noexcept {
    try {
        if (poisoned_) return Unavailable("recording manifest is poisoned");
        if (snapshot_.generation == std::numeric_limits<uint64_t>::max()) {
            return Exhausted("recording manifest generation is exhausted");
        }
        next.generation = snapshot_.generation + 1;
        auto encoded = EncodeRecordingInternal(next, options_.limits);
        if (!encoded.ok()) return encoded.status();
        const Status written = AtomicWriteManifest(root_, *encoded, options_);
        if (!written.ok()) {
            poisoned_ = true;
            return written;
        }
        snapshot_ = std::move(next);
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Status RecordingManifest::AddTopic(TopicTableEntry topic) noexcept {
    try {
        if (poisoned_) return Unavailable("recording manifest is poisoned");
        for (const TopicTableEntry& existing : snapshot_.topics) {
            if (existing.topic_id == topic.topic_id ||
                existing.topic_name == topic.topic_name) {
                return Status::Error(StatusCode::kAlreadyExists,
                                     "topic ID or name already exists");
            }
        }
        RecordingManifestSnapshot next = snapshot_;
        next.topics.push_back(std::move(topic));
        std::sort(next.topics.begin(), next.topics.end(),
                  [](const TopicTableEntry& lhs, const TopicTableEntry& rhs) {
                      return lhs.topic_id < rhs.topic_id;
                  });
        return Commit(std::move(next));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Status RecordingManifest::UpdateTopic(TopicTableEntry topic) noexcept {
    try {
        if (poisoned_) return Unavailable("recording manifest is poisoned");
        RecordingManifestSnapshot next = snapshot_;
        auto found = std::find_if(
            next.topics.begin(), next.topics.end(),
            [&](const TopicTableEntry& entry) {
                return entry.topic_id == topic.topic_id;
            });
        if (found == next.topics.end()) {
            return Status::Error(StatusCode::kNotFound, "topic ID is unknown");
        }
        if (found->topic_name != topic.topic_name) {
            return Invalid("topic ID/name mapping is immutable");
        }
        if (topic.config_version < found->config_version) {
            return Invalid("topic config version cannot move backward");
        }
        for (const SchemaRefSnapshot& old_schema : found->schema_snapshot) {
            const auto retained = std::find_if(
                topic.schema_snapshot.begin(), topic.schema_snapshot.end(),
                [&](const SchemaRefSnapshot& value) {
                    return value.schema_ref == old_schema.schema_ref;
                });
            if (retained == topic.schema_snapshot.end() ||
                *retained != old_schema) {
                return Invalid("persisted topic schema refs are immutable");
            }
        }
        if (topic.config_version == found->config_version && topic != *found) {
            return Invalid("topic snapshot change requires a newer config version");
        }
        if (topic == *found) return Status::Ok();
        *found = std::move(topic);
        return Commit(std::move(next));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Status RecordingManifest::UpdateSessionConfigVersion(
    uint64_t config_version) noexcept {
    try {
        if (config_version < snapshot_.session.config_version) {
            return Invalid("session config version cannot move backward");
        }
        if (config_version == snapshot_.session.config_version) {
            return Status::Ok();
        }
        RecordingManifestSnapshot next = snapshot_;
        next.session.config_version = config_version;
        return Commit(std::move(next));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Result<TopicTableEntry> RecordingManifest::FindTopic(
    uint32_t topic_id) const noexcept {
    try {
        const auto found = std::find_if(
            snapshot_.topics.begin(), snapshot_.topics.end(),
            [&](const TopicTableEntry& entry) {
                return entry.topic_id == topic_id;
            });
        if (found == snapshot_.topics.end()) {
            return Status::Error(StatusCode::kNotFound, "topic ID is unknown");
        }
        return *found;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<TopicTableEntry> RecordingManifest::FindTopic(
    std::string_view topic_name) const noexcept {
    try {
        const auto found = std::find_if(
            snapshot_.topics.begin(), snapshot_.topics.end(),
            [&](const TopicTableEntry& entry) {
                return entry.topic_name == topic_name;
            });
        if (found == snapshot_.topics.end()) {
            return Status::Error(StatusCode::kNotFound, "topic name is unknown");
        }
        return *found;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

PartitionManifest::PartitionManifest(
    std::filesystem::path root, ManifestOptions options, int lock_fd,
    PartitionManifestSnapshot snapshot) noexcept
    : root_(std::move(root)),
      options_(options),
      lock_fd_(lock_fd),
      snapshot_(std::move(snapshot)) {}

PartitionManifest::~PartitionManifest() {
    if (lock_fd_ >= 0) static_cast<void>(::close(lock_fd_));
}

Result<std::unique_ptr<PartitionManifest>> PartitionManifest::Create(
    const std::filesystem::path& partition_root,
    const PartitionMetadata& metadata,
    const ManifestOptions& options) noexcept {
    try {
        MINO_RETURN_IF_ERROR(VerifyDirectory(partition_root));
        MINO_RETURN_IF_ERROR(ValidateLimits(options.limits));
        PartitionManifestSnapshot snapshot;
        snapshot.generation = 1;
        snapshot.partition = metadata;
        MINO_RETURN_IF_ERROR(
            ValidatePartitionSnapshot(snapshot, options.limits));
        if (snapshot.generation < options.minimum_generation ||
            metadata.config_version < options.minimum_config_version ||
            options.minimum_durable_sequence != 0) {
            return Corruption("new partition manifest is below recovery watermark");
        }
        MINO_ASSIGN_OR_RETURN(FileDescriptor lock,
                              AcquireOwnerLock(partition_root));
        const std::filesystem::path path = partition_root / kManifestFilename;
        struct stat existing {};
        if (::lstat(path.c_str(), &existing) == 0) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "partition manifest already exists");
        }
        if (errno != ENOENT) return IoError("cannot inspect manifest", path);
        MINO_ASSIGN_OR_RETURN(
            std::vector<std::byte> encoded,
            EncodePartitionInternal(snapshot, options.limits));
        MINO_RETURN_IF_ERROR(
            AtomicWriteManifest(partition_root, encoded, options));
        return std::unique_ptr<PartitionManifest>(new PartitionManifest(
            partition_root, options, lock.release(), std::move(snapshot)));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Result<std::unique_ptr<PartitionManifest>> PartitionManifest::Open(
    const std::filesystem::path& partition_root,
    const ManifestOptions& options) noexcept {
    try {
        MINO_RETURN_IF_ERROR(VerifyDirectory(partition_root));
        MINO_RETURN_IF_ERROR(ValidateLimits(options.limits));
        MINO_ASSIGN_OR_RETURN(FileDescriptor lock,
                              AcquireOwnerLock(partition_root));
        MINO_ASSIGN_OR_RETURN(
            std::vector<std::byte> encoded,
            ReadRegularFile(partition_root / kManifestFilename,
                            options.limits.max_manifest_bytes));
        MINO_ASSIGN_OR_RETURN(
            PartitionManifestSnapshot snapshot,
            DecodePartitionInternal(encoded, options.limits));
        const uint64_t durable_sequence =
            snapshot.checkpoint.has_value()
                ? snapshot.checkpoint->durable_sequence
                : 0;
        if (snapshot.generation < options.minimum_generation ||
            snapshot.partition.config_version <
                options.minimum_config_version ||
            durable_sequence < options.minimum_durable_sequence) {
            return Corruption("partition manifest rollback detected");
        }
        MINO_RETURN_IF_ERROR(VerifySnapshotPaths(partition_root, snapshot));
        return std::unique_ptr<PartitionManifest>(new PartitionManifest(
            partition_root, options, lock.release(), std::move(snapshot)));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Corruption(error.what());
    }
}

Status PartitionManifest::Commit(PartitionManifestSnapshot next) noexcept {
    try {
        if (poisoned_) return Unavailable("partition manifest is poisoned");
        if (snapshot_.generation == std::numeric_limits<uint64_t>::max()) {
            return Exhausted("partition manifest generation is exhausted");
        }
        next.generation = snapshot_.generation + 1;
        auto encoded = EncodePartitionInternal(next, options_.limits);
        if (!encoded.ok()) return encoded.status();
        const Status written = AtomicWriteManifest(root_, *encoded, options_);
        if (!written.ok()) {
            poisoned_ = true;
            return written;
        }
        snapshot_ = std::move(next);
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Status PartitionManifest::AddSegment(SegmentManifestEntry segment) noexcept {
    try {
        if (poisoned_) return Unavailable("partition manifest is poisoned");
        PartitionManifestSnapshot next = snapshot_;
        next.segments.push_back(std::move(segment));
        return Commit(std::move(next));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Status PartitionManifest::UpdateSegment(
    SegmentManifestEntry segment) noexcept {
    try {
        if (poisoned_) return Unavailable("partition manifest is poisoned");
        PartitionManifestSnapshot next = snapshot_;
        auto found = std::find_if(
            next.segments.begin(), next.segments.end(),
            [&](const SegmentManifestEntry& entry) {
                return entry.segment_id == segment.segment_id;
            });
        if (found == next.segments.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "segment ID is unknown");
        }
        if (segment.relative_path != found->relative_path ||
            segment.first_ingestion_sequence !=
                found->first_ingestion_sequence ||
            segment.created_at_ns != found->created_at_ns) {
            return Invalid("segment identity fields are immutable");
        }
        if (segment.last_ingestion_sequence <
                found->last_ingestion_sequence ||
            segment.size_bytes < found->size_bytes ||
            segment.sealed_at_ns < found->sealed_at_ns) {
            return Invalid("segment progress cannot move backward");
        }
        if (segment.state == found->state) {
            if (static_cast<uint8_t>(segment.state) >=
                    static_cast<uint8_t>(SegmentPersistentState::kSealed) &&
                segment != *found) {
                return Invalid("sealed segment metadata is immutable");
            }
        } else if (!AllowedTransition(found->state, segment.state)) {
            return Invalid("invalid persistent segment state transition");
        }
        if (segment == *found) return Status::Ok();
        *found = std::move(segment);
        return Commit(std::move(next));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Status PartitionManifest::UpdateCheckpoint(
    DurableCheckpoint checkpoint) noexcept {
    try {
        if (snapshot_.checkpoint.has_value()) {
            const DurableCheckpoint& current = *snapshot_.checkpoint;
            if (checkpoint.segment_id < current.segment_id ||
                checkpoint.durable_sequence < current.durable_sequence ||
                (checkpoint.segment_id == current.segment_id &&
                 checkpoint.durable_offset < current.durable_offset)) {
                return Invalid("durable checkpoint cannot move backward");
            }
            if (checkpoint == current) return Status::Ok();
        }
        PartitionManifestSnapshot next = snapshot_;
        next.checkpoint = checkpoint;
        return Commit(std::move(next));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Status PartitionManifest::AdoptSealedOrphan(
    SegmentManifestEntry sealed_segment,
    std::optional<uint64_t> expected_device,
    std::optional<uint64_t> expected_inode) noexcept {
    try {
        if (sealed_segment.state != SegmentPersistentState::kSealed) {
            return Invalid("only a verified sealed orphan may be adopted");
        }
        MINO_RETURN_IF_ERROR(ValidateSegmentPath(
            sealed_segment.relative_path,
            options_.limits.max_relative_path_bytes));
        for (const SegmentManifestEntry& existing : snapshot_.segments) {
            if (existing.segment_id == sealed_segment.segment_id ||
                existing.relative_path == sealed_segment.relative_path) {
                return Status::Error(StatusCode::kAlreadyExists,
                                     "orphan is already tracked");
            }
        }
        const uint64_t expected_first = snapshot_.segments.empty()
                                            ? 1
                                            : snapshot_.segments.back()
                                                      .last_ingestion_sequence ==
                                                  std::numeric_limits<uint64_t>::max()
                                              ? 0
                                              : snapshot_.segments.back()
                                                        .last_ingestion_sequence +
                                                    1;
        if (expected_first == 0 ||
            sealed_segment.first_ingestion_sequence != expected_first) {
            return Invalid(
                "orphan first sequence must exactly follow the previous segment; "
                "non-contiguous adoption requires an explicit validated Gap");
        }
        MINO_RETURN_IF_ERROR(InspectRelativePath(
            root_, sealed_segment.relative_path, true));
        if (expected_device.has_value() != expected_inode.has_value()) {
            return Invalid("orphan identity requires both device and inode");
        }
        if (expected_device.has_value()) {
            struct stat attributes {};
            const std::filesystem::path path =
                root_ / sealed_segment.relative_path;
            if (::lstat(path.c_str(), &attributes) != 0) {
                return IoError("cannot revalidate orphan", path);
            }
            if (!S_ISREG(attributes.st_mode) ||
                static_cast<uint64_t>(attributes.st_dev) != *expected_device ||
                static_cast<uint64_t>(attributes.st_ino) != *expected_inode) {
                return Unavailable(
                    "orphan inode/device changed after validation");
            }
        }
        return AddSegment(std::move(sealed_segment));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Result<std::filesystem::path> PartitionManifest::QuarantineOrphan(
    const std::filesystem::path& relative_path,
    std::optional<uint64_t> expected_device,
    std::optional<uint64_t> expected_inode) noexcept {
    try {
        if (poisoned_) return Unavailable("partition manifest is poisoned");
        MINO_RETURN_IF_ERROR(ValidateSegmentPath(
            relative_path, options_.limits.max_relative_path_bytes));
        for (const SegmentManifestEntry& existing : snapshot_.segments) {
            if (existing.relative_path == relative_path) {
                return Invalid("tracked segment cannot be quarantined as orphan");
            }
        }
        MINO_RETURN_IF_ERROR(InspectRelativePath(root_, relative_path, true));
        if (expected_device.has_value() != expected_inode.has_value()) {
            return Invalid("orphan identity requires both device and inode");
        }
        if (expected_device.has_value()) {
            struct stat attributes {};
            const std::filesystem::path path = root_ / relative_path;
            if (::lstat(path.c_str(), &attributes) != 0) {
                return IoError("cannot revalidate orphan", path);
            }
            if (!S_ISREG(attributes.st_mode) ||
                static_cast<uint64_t>(attributes.st_dev) != *expected_device ||
                static_cast<uint64_t>(attributes.st_ino) != *expected_inode) {
                return Unavailable(
                    "orphan inode/device changed after validation");
            }
        }
        const std::filesystem::path quarantined =
            std::filesystem::path(relative_path.generic_string() + ".orphan");
        MINO_RETURN_IF_ERROR(ValidateBasicRelativePath(
            quarantined, options_.limits.max_relative_path_bytes));
        const std::filesystem::path source = root_ / relative_path;
        const std::filesystem::path destination = root_ / quarantined;
        struct stat existing {};
        if (::lstat(destination.c_str(), &existing) == 0) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "orphan quarantine destination exists");
        }
        if (errno != ENOENT) {
            return IoError("cannot inspect quarantine destination", destination);
        }
        if (::rename(source.c_str(), destination.c_str()) != 0) {
            return IoError("cannot quarantine orphan", source);
        }
        Status status =
            RunFaultHook(options_, ManifestFaultPoint::kAfterOrphanRename);
        if (status.ok()) status = SyncDirectory(destination.parent_path());
        if (status.ok()) {
            status = RunFaultHook(
                options_, ManifestFaultPoint::kAfterOrphanDirectorySync);
        }
        if (!status.ok()) {
            poisoned_ = true;
            return status;
        }
        return quarantined;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Result<SegmentManifestEntry> PartitionManifest::FindSegment(
    uint64_t segment_id) const noexcept {
    try {
        const auto found = std::find_if(
            snapshot_.segments.begin(), snapshot_.segments.end(),
            [&](const SegmentManifestEntry& entry) {
                return entry.segment_id == segment_id;
            });
        if (found == snapshot_.segments.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "segment ID is unknown");
        }
        return *found;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

}  // namespace mino::storage