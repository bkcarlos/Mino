// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "tools/mino/storage_commands.h"

#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/segment_recovery.h"

namespace mino::tools {
namespace {

using storage::PartitionManifestSnapshot;
using storage::RecordingManifestSnapshot;
using storage::SegmentRecoveryDisposition;
using storage::SegmentRecoveryReason;
using storage::SegmentRecoveryReport;

constexpr size_t kMaximumArguments = 4096;
constexpr size_t kMaximumArgumentBytes = 4096;
constexpr size_t kMaximumSegments = 65536;
constexpr uint32_t kMaximumRecordPartitions = 1024;
constexpr size_t kManifestReadLimit = 64u * 1024u * 1024u;

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status IoStatus(std::string_view operation,
                const std::filesystem::path& path) {
    const int error = errno;
    StatusCode code = StatusCode::kUnavailable;
    if (error == EACCES || error == EPERM || error == EROFS) {
        code = StatusCode::kPermissionDenied;
    } else if (error == ENOENT) {
        code = StatusCode::kNotFound;
    }
    return Status::Error(code, std::string(operation) + " '" + path.string() +
                                   "': " + std::strerror(error));
}

int ExitCodeFor(const Status& status) noexcept {
    switch (status.code()) {
        case StatusCode::kInvalidArgument:
            return kStorageExitUsage;
        case StatusCode::kCorruption:
        case StatusCode::kSchemaMismatch:
            return kStorageExitInvalidData;
        case StatusCode::kPermissionDenied:
            return kStorageExitPermissionDenied;
        default:
            return kStorageExitFailure;
    }
}

int Fail(std::string_view command, const Status& status, std::ostream& err) {
    err << "storage " << command << ": " << status.ToString() << '\n';
    return ExitCodeFor(status);
}

bool IsBoundedPath(const std::filesystem::path& path) {
    const std::string text = path.string();
    return !text.empty() && text.size() <= kMaximumArgumentBytes &&
           text.find('\0') == std::string::npos;
}

int ReadFlags() noexcept {
    int flags = O_RDONLY;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    return flags;
}

class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) static_cast<void>(::close(fd_));
    }
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ScopedFd(ScopedFd&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)) {}
    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this == &other) return *this;
        if (fd_ >= 0) static_cast<void>(::close(fd_));
        fd_ = std::exchange(other.fd_, -1);
        return *this;
    }

    int get() const noexcept { return fd_; }

private:
    int fd_ = -1;
};

Result<ScopedFd> AcquireDestructiveRepairLock(
    const std::filesystem::path& segment_path) {
    const std::filesystem::path lock_path =
        std::filesystem::path(segment_path.string() + ".repair.owner.lock");
    int flags = O_RDWR | O_CREAT;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    const int fd = ::open(lock_path.c_str(), flags, 0600);
    if (fd < 0) return IoStatus("cannot open destructive repair lock", lock_path);
    ScopedFd lock(fd);
    struct stat attributes {};
    if (::fstat(fd, &attributes) != 0 || !S_ISREG(attributes.st_mode)) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "destructive repair lock is not a regular file");
    }
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            return Status::Error(StatusCode::kUnavailable,
                                 "segment already has a destructive repair owner");
        }
        return IoStatus("cannot acquire destructive repair lock", lock_path);
    }
    return lock;
}

Status ValidateScannedIdentity(const std::filesystem::path& path,
                               const SegmentRecoveryReport& report) {
    struct stat attributes {};
    if (::lstat(path.c_str(), &attributes) != 0) {
        return IoStatus("cannot revalidate segment identity", path);
    }
    if (!S_ISREG(attributes.st_mode) ||
        static_cast<uint64_t>(attributes.st_dev) != report.file_device ||
        static_cast<uint64_t>(attributes.st_ino) != report.file_inode) {
        return Status::Error(
            StatusCode::kUnavailable,
            "segment inode/device changed after validation");
    }
    return Status::Ok();
}

Status ValidateNoSymlinkAncestors(const std::filesystem::path& path,
                                  bool final_must_be_regular) {
    if (!IsBoundedPath(path)) return Invalid("path is empty or too long");
    std::error_code error;
    const std::filesystem::path absolute =
        std::filesystem::absolute(path, error).lexically_normal();
    if (error || absolute.empty()) return Invalid("path cannot be made absolute");
    std::filesystem::path current = absolute.root_path();
    for (const std::filesystem::path& component : absolute.relative_path()) {
        current /= component;
        struct stat info {};
        if (::lstat(current.c_str(), &info) != 0) {
            return IoStatus("cannot inspect path", current);
        }
        if (S_ISLNK(info.st_mode)) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "path traverses a symbolic link");
        }
    }
    struct stat final_info {};
    if (::lstat(absolute.c_str(), &final_info) != 0) {
        return IoStatus("cannot inspect path", absolute);
    }
    if (final_must_be_regular && !S_ISREG(final_info.st_mode)) {
        return Invalid("path is not a regular file");
    }
    return Status::Ok();
}

Status ValidateDirectory(const std::filesystem::path& path) {
    if (!IsBoundedPath(path)) return Invalid("path is empty or too long");
    MINO_RETURN_IF_ERROR(ValidateNoSymlinkAncestors(path, false));
    struct stat info {};
    if (::lstat(path.c_str(), &info) != 0) {
        return IoStatus("cannot inspect directory", path);
    }
    if (S_ISLNK(info.st_mode)) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "symbolic-link directories are forbidden");
    }
    if (!S_ISDIR(info.st_mode)) return Invalid("path is not a directory");
    return Status::Ok();
}

Status ValidatePathBelowRoot(const std::filesystem::path& root,
                             const std::filesystem::path& relative,
                             bool final_is_regular, bool allow_missing) {
    const std::string text = relative.generic_string();
    if (text.empty() || relative.is_absolute() || relative.has_root_name() ||
        relative.lexically_normal().generic_string() != text) {
        return Invalid("path below session root is not canonical and relative");
    }
    std::filesystem::path current = root;
    size_t index = 0;
    const size_t count = static_cast<size_t>(
        std::distance(relative.begin(), relative.end()));
    for (const std::filesystem::path& component : relative) {
        if (component.empty() || component == "." || component == "..") {
            return Invalid("path traversal below session root is forbidden");
        }
        ++index;
        current /= component;
        struct stat info {};
        if (::lstat(current.c_str(), &info) != 0) {
            if (errno == ENOENT && allow_missing) return Status::Ok();
            return IoStatus("cannot inspect session path", current);
        }
        if (S_ISLNK(info.st_mode)) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "session paths may not traverse symbolic links");
        }
        const bool should_be_directory = index < count || !final_is_regular;
        if ((should_be_directory && !S_ISDIR(info.st_mode)) ||
            (!should_be_directory && !S_ISREG(info.st_mode))) {
            return Corruption("session path has an unexpected file type");
        }
    }
    return Status::Ok();
}

Result<std::vector<std::byte>> ReadRegularFile(
    const std::filesystem::path& path, size_t maximum_bytes) {
    if (!IsBoundedPath(path)) return Invalid("path is empty or too long");
    struct stat link_info {};
    if (::lstat(path.c_str(), &link_info) != 0) {
        return IoStatus("cannot inspect file", path);
    }
    if (S_ISLNK(link_info.st_mode)) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "symbolic-link files are forbidden");
    }
    if (!S_ISREG(link_info.st_mode)) return Invalid("path is not a regular file");

    const int raw_fd = ::open(path.c_str(), ReadFlags());
    if (raw_fd < 0) return IoStatus("cannot open file", path);
    ScopedFd fd(raw_fd);
    struct stat info {};
    if (::fstat(fd.get(), &info) != 0) return IoStatus("cannot stat file", path);
    if (!S_ISREG(info.st_mode) || info.st_size < 0) {
        return Invalid("path is not a bounded regular file");
    }
    const uint64_t size = static_cast<uint64_t>(info.st_size);
    if (size > maximum_bytes ||
        size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "file exceeds the configured read limit");
    }
    std::vector<std::byte> bytes(static_cast<size_t>(size));
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t count = ::read(fd.get(), bytes.data() + offset,
                                     bytes.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) return IoStatus("cannot read file", path);
        if (count == 0) return Corruption("file shrank while being read");
        offset += static_cast<size_t>(count);
    }
    std::byte extra{};
    while (true) {
        const ssize_t count = ::read(fd.get(), &extra, 1);
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) return IoStatus("cannot finish reading file", path);
        if (count != 0) return Corruption("file grew while being read");
        break;
    }
    return bytes;
}

template <typename T>
Result<T> ParseUnsigned(std::string_view text, std::string_view name) {
    if (text.empty() || text.size() > 20 || text.front() == '-') {
        return Invalid(std::string(name) + " must be an unsigned decimal integer");
    }
    T value = 0;
    const char* begin = text.data();
    const char* end = text.data() + text.size();
    const auto parsed = std::from_chars(begin, end, value, 10);
    if (parsed.ec != std::errc{} || parsed.ptr != end) {
        return Invalid(std::string(name) + " must be an unsigned decimal integer");
    }
    return value;
}

Result<double> ParseSpeed(std::string_view text) {
    if (text.empty() || text.size() > 64) return Invalid("invalid replay speed");
    std::string copy(text);
    char* end = nullptr;
    errno = 0;
    const double value = std::strtod(copy.c_str(), &end);
    if (errno == ERANGE || end == copy.c_str() || *end != '\0' ||
        !std::isfinite(value) || value <= 0.0) {
        return Invalid("replay speed must be finite and greater than zero");
    }
    return value;
}

Result<std::string_view> FlagValue(const std::vector<std::string>& args,
                                   size_t* index) {
    if (*index + 1 >= args.size()) {
        return Invalid(args[*index] + " requires a value");
    }
    ++*index;
    return std::string_view(args[*index]);
}

Result<RecordingManifestSnapshot> ReadRecordingManifest(
    const std::filesystem::path& root) {
    Result<std::vector<std::byte>> bytes =
        ReadRegularFile(root / "manifest", kManifestReadLimit);
    if (!bytes.ok()) return bytes.status();
    return storage::DecodeRecordingManifest(*bytes);
}

Result<PartitionManifestSnapshot> ReadPartitionManifest(
    const std::filesystem::path& root) {
    Result<std::vector<std::byte>> bytes =
        ReadRegularFile(root / "manifest", kManifestReadLimit);
    if (!bytes.ok()) return bytes.status();
    return storage::DecodePartitionManifest(*bytes);
}

struct LoadedPartition {
    std::filesystem::path root;
    PartitionManifestSnapshot manifest;
    bool has_snapshot = false;
};

struct LoadedSession {
    std::filesystem::path root;
    RecordingManifestSnapshot manifest;
    std::vector<LoadedPartition> partitions;
};

Result<uint32_t> PartitionDirectoryId(std::string_view name) {
    if (name.empty() || name.size() > 10 ||
        !std::all_of(name.begin(), name.end(), [](char value) {
            return value >= '0' && value <= '9';
        })) {
        return Invalid("partition directory name is not a decimal ID");
    }
    return ParseUnsigned<uint32_t>(name, "partition directory ID");
}

Status ValidateDescriptorFiles(const LoadedSession& session) {
    std::map<uint32_t, storage::SchemaRefSnapshot> refs;
    std::map<std::filesystem::path,
             std::vector<const storage::SchemaRefSnapshot*>> by_artifact;
    for (const storage::TopicTableEntry& topic : session.manifest.topics) {
        for (const storage::SchemaRefSnapshot& schema : topic.schema_snapshot) {
            if (schema.schema_ref == 0) {
                return Corruption("schema snapshot uses reserved ref zero");
            }
            const auto [known, inserted] = refs.emplace(schema.schema_ref, schema);
            if (!inserted && known->second != schema) {
                return Corruption(
                    "session schema ref maps to multiple descriptor identities");
            }
            const Status safe_path = ValidatePathBelowRoot(
                session.root, schema.descriptor_path, true, false);
            if (!safe_path.ok()) return safe_path;
            by_artifact[schema.descriptor_path].push_back(&schema);
        }
    }

    for (const auto& [relative_path, expected] : by_artifact) {
        Result<std::vector<std::byte>> descriptor = ReadRegularFile(
            session.root / relative_path, 16u * 1024u * 1024u);
        if (!descriptor.ok()) return descriptor.status();
        const std::string_view bytes(
            reinterpret_cast<const char*>(descriptor->data()),
            descriptor->size());
        Result<schema::codegen::DecodedDescriptorArtifact> decoded =
            schema::codegen::DecodeAndValidate(bytes);
        if (!decoded.ok()) {
            return Status::Error(StatusCode::kCorruption,
                                 "schema descriptor artifact is invalid: " +
                                     decoded.status().ToString());
        }
        if (decoded->version != schema::codegen::kDescriptorArtifactVersion) {
            return Corruption("schema descriptor artifact version is unsupported");
        }
        for (const storage::SchemaRefSnapshot* snapshot : expected) {
            const bool matched = std::any_of(
                decoded->types.begin(), decoded->types.end(),
                [snapshot](const schema::codegen::DecodedTypeArtifact& type) {
                    if (type.descriptor == nullptr) return false;
                    const schema::SchemaIdentity& identity =
                        type.descriptor->identity();
                    return identity.canonical_digest() ==
                               snapshot->canonical_digest &&
                           identity.schema_version() ==
                               snapshot->schema_version &&
                           identity.layout_version() ==
                               snapshot->layout_version &&
                           type.layout.layout_version ==
                               snapshot->layout_version;
                });
            if (!matched) {
                return Status::Error(
                    StatusCode::kSchemaMismatch,
                    "descriptor artifact digest/version/layout does not match schema ref " +
                        std::to_string(snapshot->schema_ref));
            }
        }
    }
    return Status::Ok();
}

Result<LoadedSession> LoadSession(const std::filesystem::path& root,
                                  bool validate_descriptors) {
    const Status root_status = ValidateDirectory(root);
    if (!root_status.ok()) return root_status;
    Result<RecordingManifestSnapshot> recording = ReadRecordingManifest(root);
    if (!recording.ok()) return recording.status();

    LoadedSession session{.root = root,
                          .manifest = std::move(*recording),
                          .partitions = {}};
    size_t partition_count = 0;
    for (const storage::TopicTableEntry& topic : session.manifest.topics) {
        const std::filesystem::path relative_partitions =
            std::filesystem::path("topics") /
            std::to_string(topic.topic_id) / "partitions";
        const Status safe_partitions = ValidatePathBelowRoot(
            root, relative_partitions, false, true);
        if (!safe_partitions.ok()) return safe_partitions;
        const std::filesystem::path partitions = root / relative_partitions;
        struct stat info {};
        if (::lstat(partitions.c_str(), &info) != 0) {
            if (errno == ENOENT) continue;
            return IoStatus("cannot inspect partitions directory", partitions);
        }
        if (S_ISLNK(info.st_mode)) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "symbolic-link partitions directory is forbidden");
        }
        if (!S_ISDIR(info.st_mode)) {
            return Corruption("partitions path is not a directory");
        }

        std::error_code error;
        std::filesystem::directory_iterator iterator(partitions, error);
        const std::filesystem::directory_iterator end;
        while (!error && iterator != end) {
            if (++partition_count > kMaximumSegments) {
                return Status::Error(StatusCode::kResourceExhausted,
                                     "session has too many partitions");
            }
            const std::filesystem::directory_entry entry = *iterator;
            const std::filesystem::file_status entry_status =
                entry.symlink_status(error);
            if (error) break;
            if (std::filesystem::is_symlink(entry_status)) {
                return Status::Error(StatusCode::kPermissionDenied,
                                     "symbolic-link partition is forbidden");
            }
            if (!std::filesystem::is_directory(entry_status)) {
                return Corruption("partitions directory contains a non-directory");
            }
            Result<uint32_t> directory_id =
                PartitionDirectoryId(entry.path().filename().string());
            if (!directory_id.ok()) return directory_id.status();
            Result<PartitionManifestSnapshot> partition =
                ReadPartitionManifest(entry.path());
            if (!partition.ok()) return partition.status();
            if (partition->partition.recording_id !=
                    session.manifest.session.recording_id ||
                partition->partition.owner_epoch !=
                    session.manifest.session.owner_epoch ||
                partition->partition.topic_id != topic.topic_id ||
                partition->partition.partition_id != *directory_id ||
                partition->partition.config_version > topic.config_version) {
                return Corruption(
                    "partition manifest identity differs from its session path");
            }
            const std::filesystem::path snapshot_path =
                entry.path() / "snapshot.mino";
            struct stat snapshot_info {};
            bool has_snapshot = false;
            if (::lstat(snapshot_path.c_str(), &snapshot_info) == 0) {
                if (S_ISLNK(snapshot_info.st_mode)) {
                    return Status::Error(
                        StatusCode::kPermissionDenied,
                        "symbolic-link snapshot file is forbidden");
                }
                if (!S_ISREG(snapshot_info.st_mode)) {
                    return Corruption("snapshot path is not a regular file");
                }
                has_snapshot = true;
            } else if (errno != ENOENT) {
                return IoStatus("cannot inspect snapshot file", snapshot_path);
            }
            session.partitions.push_back(LoadedPartition{
                entry.path(), std::move(*partition), has_snapshot});
            iterator.increment(error);
        }
        if (error) {
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot enumerate partitions: " +
                                     error.message());
        }
    }
    std::sort(session.partitions.begin(), session.partitions.end(),
              [](const LoadedPartition& left, const LoadedPartition& right) {
                  return std::pair(left.manifest.partition.topic_id,
                                   left.manifest.partition.partition_id) <
                         std::pair(right.manifest.partition.topic_id,
                                   right.manifest.partition.partition_id);
              });
    for (size_t index = 1; index < session.partitions.size(); ++index) {
        const auto& previous = session.partitions[index - 1].manifest.partition;
        const auto& current = session.partitions[index].manifest.partition;
        if (previous.topic_id == current.topic_id &&
            previous.partition_id == current.partition_id) {
            return Corruption("session contains a duplicate partition identity");
        }
    }
    if (validate_descriptors) {
        const Status descriptors = ValidateDescriptorFiles(session);
        if (!descriptors.ok()) return descriptors;
    }
    return session;
}

std::string_view SegmentStateName(storage::SegmentPersistentState state) {
    switch (state) {
        case storage::SegmentPersistentState::kCreating:
            return "creating";
        case storage::SegmentPersistentState::kOpen:
            return "open";
        case storage::SegmentPersistentState::kSealed:
            return "sealed";
        case storage::SegmentPersistentState::kIndexed:
            return "indexed";
        case storage::SegmentPersistentState::kRetained:
            return "retained";
        case storage::SegmentPersistentState::kDeleted:
            return "deleted";
    }
    return "unknown";
}

std::string_view DispositionName(SegmentRecoveryDisposition disposition) {
    switch (disposition) {
        case SegmentRecoveryDisposition::kClean:
            return "clean";
        case SegmentRecoveryDisposition::kIncompleteTail:
            return "incomplete-tail";
        case SegmentRecoveryDisposition::kCorruption:
            return "corruption";
    }
    return "unknown";
}

std::string_view ReasonName(SegmentRecoveryReason reason) {
    switch (reason) {
        case SegmentRecoveryReason::kNone: return "none";
        case SegmentRecoveryReason::kIncompleteSegmentHeader: return "incomplete-segment-header";
        case SegmentRecoveryReason::kSegmentHeaderCorruption: return "segment-header-corruption";
        case SegmentRecoveryReason::kIncompleteLeadingLength: return "incomplete-leading-length";
        case SegmentRecoveryReason::kRecordLengthOverflow: return "record-length-overflow";
        case SegmentRecoveryReason::kRecordTooSmall: return "record-too-small";
        case SegmentRecoveryReason::kRecordTooLarge: return "record-too-large";
        case SegmentRecoveryReason::kIncompleteRecord: return "incomplete-record";
        case SegmentRecoveryReason::kHeaderLengthMismatch: return "header-length-mismatch";
        case SegmentRecoveryReason::kRecordHeaderCorruption: return "record-header-corruption";
        case SegmentRecoveryReason::kHeaderCrcMismatch: return "header-crc-mismatch";
        case SegmentRecoveryReason::kPayloadSizeMismatch: return "payload-size-mismatch";
        case SegmentRecoveryReason::kPayloadCrcMismatch: return "payload-crc-mismatch";
        case SegmentRecoveryReason::kPaddingCorruption: return "padding-corruption";
        case SegmentRecoveryReason::kTrailingLengthMismatch: return "trailing-length-mismatch";
        case SegmentRecoveryReason::kTrailerSequenceMismatch: return "trailer-sequence-mismatch";
        case SegmentRecoveryReason::kRecordCrcMismatch: return "record-crc-mismatch";
        case SegmentRecoveryReason::kCommitMarkerMismatch: return "commit-marker-mismatch";
        case SegmentRecoveryReason::kTopicPartitionMismatch: return "topic-partition-mismatch";
        case SegmentRecoveryReason::kIngestionSequenceMismatch: return "ingestion-sequence-mismatch";
        case SegmentRecoveryReason::kUnknownSchemaRef: return "unknown-schema-ref";
    }
    return "unknown";
}

void PrintSegmentReport(const std::filesystem::path& path,
                        const SegmentRecoveryReport& report,
                        std::ostream& out) {
    out << path.string() << ": " << DispositionName(report.disposition)
        << " records=" << report.records_scanned
        << " bytes=" << report.file_size
        << " topic=" << report.segment_header.topic_id
        << " partition=" << report.segment_header.partition_id;
    if (!report.clean()) out << " reason=" << ReasonName(report.reason);
    if (report.repaired) out << " repaired=true truncated=" << report.truncated_bytes;
    out << '\n';
}

const storage::TopicTableEntry* FindTopic(const LoadedSession& session,
                                          uint32_t topic_id) {
    const auto found = std::find_if(
        session.manifest.topics.begin(), session.manifest.topics.end(),
        [topic_id](const storage::TopicTableEntry& topic) {
            return topic.topic_id == topic_id;
        });
    return found == session.manifest.topics.end() ? nullptr : &*found;
}

const LoadedPartition* FindPartition(const LoadedSession& session,
                                     uint32_t topic_id,
                                     uint32_t partition_id) {
    const auto found = std::find_if(
        session.partitions.begin(), session.partitions.end(),
        [topic_id, partition_id](const LoadedPartition& partition) {
            return partition.manifest.partition.topic_id == topic_id &&
                   partition.manifest.partition.partition_id == partition_id;
        });
    return found == session.partitions.end() ? nullptr : &*found;
}

std::optional<std::filesystem::path> InferSessionRoot(
    const std::filesystem::path& segment_path) {
    const std::filesystem::path absolute =
        std::filesystem::absolute(segment_path).lexically_normal();
    std::filesystem::path partition;
    if (absolute.filename() == "snapshot.mino") {
        partition = absolute.parent_path();
    } else {
        const std::filesystem::path segments = absolute.parent_path();
        if (segments.filename() != "segments") return std::nullopt;
        partition = segments.parent_path();
    }
    const std::filesystem::path partitions = partition.parent_path();
    const std::filesystem::path topic = partitions.parent_path();
    const std::filesystem::path topics = topic.parent_path();
    const std::filesystem::path session = topics.parent_path();
    if (partitions.filename() != "partitions" ||
        topics.filename() != "topics" || session.empty()) {
        return std::nullopt;
    }
    return session;
}

Status ValidateAgainstSession(const std::filesystem::path& segment_path,
                              const SegmentRecoveryReport& initial,
                              SegmentRecoveryReport* validated) {
    const std::optional<std::filesystem::path> root =
        InferSessionRoot(segment_path);
    if (!root.has_value()) {
        *validated = initial;
        return Status::Ok();
    }
    Result<LoadedSession> session = LoadSession(*root, true);
    if (!session.ok()) return session.status();
    if (initial.segment_header.recording_id !=
        session->manifest.session.recording_id) {
        return Corruption("segment recording_id differs from session manifest");
    }
    const storage::TopicTableEntry* topic =
        FindTopic(*session, initial.segment_header.topic_id);
    if (topic == nullptr) return Corruption("segment topic is absent from manifest");
    const LoadedPartition* partition = FindPartition(
        *session, initial.segment_header.topic_id,
        initial.segment_header.partition_id);
    if (partition == nullptr) {
        return Corruption("segment partition is absent from manifest");
    }

    const std::filesystem::path absolute =
        std::filesystem::absolute(segment_path).lexically_normal();
    const std::filesystem::path snapshot_path =
        std::filesystem::absolute(partition->root / "snapshot.mino")
            .lexically_normal();
    const bool is_snapshot = partition->has_snapshot && absolute == snapshot_path;
    const storage::SegmentManifestEntry* tracked = nullptr;
    for (const storage::SegmentManifestEntry& entry :
         partition->manifest.segments) {
        const std::filesystem::path expected =
            std::filesystem::absolute(partition->root / entry.relative_path)
                .lexically_normal();
        if (expected == absolute) {
            tracked = &entry;
            break;
        }
    }
    if (!is_snapshot &&
        (tracked == nullptr ||
         tracked->state == storage::SegmentPersistentState::kDeleted)) {
        return Corruption("segment is not an active partition-manifest entry");
    }
    if (initial.segment_header.writer_id !=
        partition->manifest.partition.writer_id) {
        return Corruption("segment writer differs from partition manifest");
    }
    if (!is_snapshot &&
        (tracked->first_ingestion_sequence !=
             initial.segment_header.first_ingestion_sequence ||
         tracked->created_at_ns != initial.segment_header.created_at_ns)) {
        return Corruption("segment header differs from partition manifest");
    }

    const std::filesystem::path relative_path =
        is_snapshot ? std::filesystem::path("snapshot.mino")
                    : tracked->relative_path;
    const Status safe_segment = ValidatePathBelowRoot(
        partition->root, relative_path, true, false);
    if (!safe_segment.ok()) return safe_segment;

    std::unordered_set<uint32_t> known_schema_refs;
    for (const storage::SchemaRefSnapshot& schema : topic->schema_snapshot) {
        known_schema_refs.insert(schema.schema_ref);
    }
    storage::SegmentRecoveryOptions options;
    options.known_schema_refs = &known_schema_refs;
    Result<SegmentRecoveryReport> rescanned =
        storage::ScanSegment(segment_path, options);
    if (!rescanned.ok()) return rescanned.status();
    if (!is_snapshot && rescanned->clean() &&
        tracked->state != storage::SegmentPersistentState::kCreating &&
        tracked->state != storage::SegmentPersistentState::kOpen &&
        (tracked->size_bytes != rescanned->file_size ||
         !rescanned->has_last_complete_sequence ||
         tracked->last_ingestion_sequence !=
             rescanned->last_complete_sequence)) {
        return Corruption("sealed segment progress differs from manifest");
    }
    for (const storage::SegmentRecordOffset& record : rescanned->records) {
        if ((record.flags & storage::kRecordFlagGap) != 0) continue;
        const auto schema = std::find_if(
            topic->schema_snapshot.begin(), topic->schema_snapshot.end(),
            [&record](const storage::SchemaRefSnapshot& candidate) {
                return candidate.schema_ref == record.schema_ref;
            });
        if (schema == topic->schema_snapshot.end() ||
            schema->schema_version != record.schema_version ||
            schema->layout_version != record.layout_version) {
            return Status::Error(
                StatusCode::kSchemaMismatch,
                "segment record schema version/layout differs from manifest");
        }
    }
    *validated = std::move(*rescanned);
    return Status::Ok();
}

int CmdInspect(const std::vector<std::string>& args, std::ostream& out,
               std::ostream& err) {
    if (args.size() != 2) {
        return Fail("inspect", Invalid("usage: storage inspect <session_root>"),
                    err);
    }
    Result<LoadedSession> session = LoadSession(args[1], false);
    if (!session.ok()) return Fail("inspect", session.status(), err);

    size_t segment_count = 0;
    uint64_t segment_bytes = 0;
    for (const LoadedPartition& partition : session->partitions) {
        segment_count += partition.manifest.segments.size();
        for (const storage::SegmentManifestEntry& segment :
             partition.manifest.segments) {
            segment_bytes += segment.size_bytes;
        }
    }
    out << "session: " << session->root.string() << '\n'
        << "recording_id: " << session->manifest.session.recording_id << '\n'
        << "manifest_generation: " << session->manifest.generation << '\n'
        << "config_version: " << session->manifest.session.config_version << '\n'
        << "topics: " << session->manifest.topics.size() << '\n'
        << "partitions: " << session->partitions.size() << '\n'
        << "segments: " << segment_count << '\n'
        << "segment_bytes: " << segment_bytes << '\n';
    for (const storage::TopicTableEntry& topic : session->manifest.topics) {
        out << "topic: " << topic.topic_name << " (id=" << topic.topic_id
            << ") schemas=" << topic.schema_snapshot.size() << '\n';
        for (const LoadedPartition& partition : session->partitions) {
            if (partition.manifest.partition.topic_id != topic.topic_id) continue;
            out << "  partition: " << partition.manifest.partition.partition_id
                << " generation=" << partition.manifest.generation
                << " segments=" << partition.manifest.segments.size()
                << " snapshot="
                << (partition.has_snapshot ? "current" : "none") << '\n';
            for (const storage::SegmentManifestEntry& segment :
                 partition.manifest.segments) {
                out << "    segment: " << segment.segment_id
                    << " state=" << SegmentStateName(segment.state)
                    << " sequence=" << segment.first_ingestion_sequence << '-'
                    << segment.last_ingestion_sequence
                    << " bytes=" << segment.size_bytes
                    << " path=" << segment.relative_path.generic_string() << '\n';
            }
        }
    }
    return kStorageExitSuccess;
}

int CmdVerify(const std::vector<std::string>& args, std::ostream& out,
              std::ostream& err) {
    if (args.size() < 2 || args.size() - 1 > kMaximumSegments) {
        return Fail("verify",
                    Invalid("usage: storage verify <segment> [<segment> ...]"),
                    err);
    }
    bool invalid = false;
    for (size_t index = 1; index < args.size(); ++index) {
        const std::filesystem::path path(args[index]);
        Result<SegmentRecoveryReport> scanned = storage::ScanSegment(path);
        if (!scanned.ok()) return Fail("verify", scanned.status(), err);
        SegmentRecoveryReport validated;
        const Status cross_checked =
            ValidateAgainstSession(path, *scanned, &validated);
        if (!cross_checked.ok()) return Fail("verify", cross_checked, err);
        PrintSegmentReport(path, validated, out);
        invalid = invalid || !validated.clean();
    }
    return invalid ? kStorageExitInvalidData : kStorageExitSuccess;
}

int CmdRepair(const std::vector<std::string>& args, std::ostream& out,
              std::ostream& err) {
    bool apply = false;
    bool explicit_dry_run = false;
    bool standalone = false;
    std::optional<std::filesystem::path> path;
    for (size_t index = 1; index < args.size(); ++index) {
        if (args[index] == "--apply") {
            if (apply) return Fail("repair", Invalid("duplicate --apply"), err);
            apply = true;
        } else if (args[index] == "--dry-run") {
            if (explicit_dry_run) {
                return Fail("repair", Invalid("duplicate --dry-run"), err);
            }
            explicit_dry_run = true;
        } else if (args[index] == "--standalone") {
            if (standalone) {
                return Fail("repair", Invalid("duplicate --standalone"), err);
            }
            standalone = true;
        } else if (!args[index].empty() && args[index].front() == '-') {
            return Fail("repair", Invalid("unknown flag: " + args[index]), err);
        } else if (path.has_value()) {
            return Fail("repair", Invalid("repair accepts exactly one segment"), err);
        } else {
            path = args[index];
        }
    }
    if (apply && explicit_dry_run) {
        return Fail("repair", Invalid("--apply conflicts with --dry-run"), err);
    }
    if (!path.has_value()) {
        return Fail("repair",
                    Invalid("usage: storage repair <segment> [--apply --standalone]"),
                    err);
    }
    if (standalone && !apply) {
        return Fail("repair", Invalid("--standalone requires --apply"), err);
    }
    const Status safe_path = ValidateNoSymlinkAncestors(*path, true);
    if (!safe_path.ok()) return Fail("repair", safe_path, err);
    const std::optional<std::filesystem::path> session_root =
        InferSessionRoot(*path);
    if (apply && session_root.has_value()) {
        return Fail(
            "repair",
            Status::Error(
                StatusCode::kPermissionDenied,
                "session-shaped/tracked segments must be repaired by SessionRecoveryCoordinator"),
            err);
    }
    if (apply && !standalone) {
        return Fail(
            "repair",
            Status::Error(
                StatusCode::kPermissionDenied,
                "destructive standalone repair requires explicit --standalone"),
            err);
    }

    std::optional<ScopedFd> repair_lock;
    if (apply) {
        Result<ScopedFd> acquired = AcquireDestructiveRepairLock(*path);
        if (!acquired.ok()) return Fail("repair", acquired.status(), err);
        repair_lock.emplace(std::move(*acquired));
    }

    const uintmax_t before_size = std::filesystem::file_size(*path);
    Result<SegmentRecoveryReport> initial = storage::ScanSegment(*path);
    if (!initial.ok()) return Fail("repair", initial.status(), err);
    SegmentRecoveryReport validated;
    if (!apply) {
        const Status cross_checked =
            ValidateAgainstSession(*path, *initial, &validated);
        if (!cross_checked.ok()) return Fail("repair", cross_checked, err);
    } else {
        validated = *initial;
    }
    Result<SegmentRecoveryReport> report = validated;
    if (apply) {
        const Status same_inode = ValidateScannedIdentity(*path, *initial);
        if (!same_inode.ok()) return Fail("repair", same_inode, err);
        storage::SegmentRepairOptions repair_options;
        repair_options.expected_device = initial->file_device;
        repair_options.expected_inode = initial->file_inode;
        repair_options.require_single_link = true;
        report = storage::RepairSegment(*path, {}, repair_options);
    }
    if (!report.ok()) return Fail("repair", report.status(), err);
    PrintSegmentReport(*path, *report, out);
    const uintmax_t after_size = std::filesystem::file_size(*path);
    out << "audit: command=storage-repair mode="
        << (apply ? "apply-standalone" : "dry-run")
        << " path=" << path->string()
        << " before_bytes=" << before_size
        << " after_bytes=" << after_size
        << " modified=" << (before_size != after_size ? "true" : "false")
        << " repairable=" << (report->repairable() ? "true" : "false")
        << '\n';
    if (!apply && !report->clean()) return kStorageExitInvalidData;
    return kStorageExitSuccess;
}

std::vector<std::filesystem::path> TrackedSegments(
    const LoadedSession& session) {
    std::vector<std::filesystem::path> result;
    for (const LoadedPartition& partition : session.partitions) {
        if (partition.has_snapshot) {
            result.push_back(partition.root / "snapshot.mino");
        }
        for (const storage::SegmentManifestEntry& segment :
             partition.manifest.segments) {
            if (segment.state != storage::SegmentPersistentState::kDeleted) {
                result.push_back(partition.root / segment.relative_path);
            }
        }
    }
    return result;
}

int CmdReplay(const std::vector<std::string>& args, std::ostream& out,
              std::ostream& err,
              storage::ReplayPublisherAdapter* replay_adapter) {
    if (args.size() < 2) {
        return Fail("replay", Invalid("replay requires <session_root>"), err);
    }
    const std::filesystem::path session_root(args[1]);
    storage::ReplayOptions options;
    bool validate_only = false;
    bool step = false;
    bool explicit_segments = false;
    bool unsafe_standalone_segment = false;
    std::vector<std::filesystem::path> segment_paths;
    for (size_t index = 2; index < args.size(); ++index) {
        const std::string& flag = args[index];
        if (flag == "--validate-only") {
            validate_only = true;
        } else if (flag == "--step") {
            step = true;
            options.playback.mode = storage::ReplayPlaybackMode::kStep;
        } else if (flag == "--live") {
            options.publish_target = storage::ReplayPublishTarget::kLive;
        } else if (flag == "--authorize-live") {
            options.live_injection_authorized = true;
        } else if (flag == "--unsafe-standalone-segment") {
            unsafe_standalone_segment = true;
        } else if (flag == "--segment") {
            Result<std::string_view> value = FlagValue(args, &index);
            if (!value.ok()) return Fail("replay", value.status(), err);
            explicit_segments = true;
            segment_paths.emplace_back(*value);
        } else if (flag == "--topic") {
            Result<std::string_view> value = FlagValue(args, &index);
            if (!value.ok()) return Fail("replay", value.status(), err);
            Result<uint32_t> id = ParseUnsigned<uint32_t>(*value, "topic");
            if (id.ok() && *id != 0) {
                options.filter.topic_ids.push_back(*id);
            } else {
                options.filter.topic_names.emplace_back(*value);
            }
        } else if (flag == "--node") {
            Result<std::string_view> value = FlagValue(args, &index);
            if (!value.ok()) return Fail("replay", value.status(), err);
            Result<uint64_t> id = ParseUnsigned<uint64_t>(*value, "node");
            if (!id.ok()) return Fail("replay", id.status(), err);
            options.filter.node_ids.push_back(*id);
        } else if (flag == "--speed") {
            Result<std::string_view> value = FlagValue(args, &index);
            if (!value.ok()) return Fail("replay", value.status(), err);
            Result<double> speed = ParseSpeed(*value);
            if (!speed.ok()) return Fail("replay", speed.status(), err);
            options.playback.speed = *speed;
        } else if (flag == "--namespace") {
            Result<std::string_view> value = FlagValue(args, &index);
            if (!value.ok()) return Fail("replay", value.status(), err);
            options.replay_namespace = *value;
        } else if (flag == "--session-id") {
            Result<std::string_view> value = FlagValue(args, &index);
            if (!value.ok()) return Fail("replay", value.status(), err);
            Result<uint64_t> id = ParseUnsigned<uint64_t>(*value, "session ID");
            if (!id.ok()) return Fail("replay", id.status(), err);
            options.replay_session_id = *id;
        } else if (flag == "--from-ingestion-ns" ||
                   flag == "--to-ingestion-ns" ||
                   flag == "--from-source-sequence" ||
                   flag == "--to-source-sequence" ||
                   flag == "--from-ingestion-sequence" ||
                   flag == "--to-ingestion-sequence") {
            Result<std::string_view> value = FlagValue(args, &index);
            if (!value.ok()) return Fail("replay", value.status(), err);
            Result<uint64_t> parsed = ParseUnsigned<uint64_t>(*value, flag);
            if (!parsed.ok()) return Fail("replay", parsed.status(), err);
            if (flag == "--from-ingestion-ns")
                options.filter.ingestion_timestamp_ns.minimum = *parsed;
            else if (flag == "--to-ingestion-ns")
                options.filter.ingestion_timestamp_ns.maximum = *parsed;
            else if (flag == "--from-source-sequence")
                options.filter.source_sequence.minimum = *parsed;
            else if (flag == "--to-source-sequence")
                options.filter.source_sequence.maximum = *parsed;
            else if (flag == "--from-ingestion-sequence")
                options.filter.ingestion_sequence.minimum = *parsed;
            else
                options.filter.ingestion_sequence.maximum = *parsed;
        } else {
            return Fail("replay", Invalid("unknown flag: " + flag), err);
        }
    }
    if (segment_paths.size() > kMaximumSegments ||
        options.filter.topic_ids.size() + options.filter.topic_names.size() >
            kMaximumSegments ||
        options.filter.node_ids.size() > kMaximumSegments) {
        return Fail("replay", Invalid("replay input exceeds bounded limits"), err);
    }
    Result<LoadedSession> session = LoadSession(session_root, true);
    if (!session.ok()) return Fail("replay", session.status(), err);
    if (unsafe_standalone_segment && !explicit_segments) {
        return Fail("replay",
                    Invalid("--unsafe-standalone-segment requires --segment"),
                    err);
    }
    if (unsafe_standalone_segment &&
        options.publish_target == storage::ReplayPublishTarget::kLive) {
        return Fail(
            "replay",
            Status::Error(
                StatusCode::kPermissionDenied,
                "unsafe standalone segments may not be injected into live"),
            err);
    }
    const std::vector<std::filesystem::path> tracked_segments =
        TrackedSegments(*session);
    if (segment_paths.empty()) segment_paths = tracked_segments;
    if (segment_paths.size() > kMaximumSegments) {
        return Fail("replay", Invalid("session has too many segments"), err);
    }
    for (const std::filesystem::path& path : segment_paths) {
        const Status safe_path = ValidateNoSymlinkAncestors(path, true);
        if (!safe_path.ok()) return Fail("replay", safe_path, err);
        if (!unsafe_standalone_segment && explicit_segments) {
            const std::filesystem::path selected =
                std::filesystem::absolute(path).lexically_normal();
            const bool tracked = std::any_of(
                tracked_segments.begin(), tracked_segments.end(),
                [&selected](const std::filesystem::path& candidate) {
                    return std::filesystem::absolute(candidate).lexically_normal() ==
                           selected;
                });
            if (!tracked) {
                return Fail(
                    "replay",
                    Status::Error(
                        StatusCode::kPermissionDenied,
                        "--segment is not a tracked non-deleted manifest entry"),
                    err);
            }
        }
        // Default replay is not a weaker path: every tracked Segment (including
        // snapshots) is rescanned with the session's schema refs and checked
        // against its exact partition-manifest entry before ReplayEngine opens it.
        if (!unsafe_standalone_segment) {
            Result<SegmentRecoveryReport> scanned = storage::ScanSegment(path);
            if (!scanned.ok()) return Fail("replay", scanned.status(), err);
            SegmentRecoveryReport validated;
            const Status cross_checked =
                ValidateAgainstSession(path, *scanned, &validated);
            if (!cross_checked.ok()) return Fail("replay", cross_checked, err);
        }
    }
    if (replay_adapter == nullptr && !validate_only) {
        return Fail(
            "replay",
            Status::Error(StatusCode::kUnsupported,
                          "no Bus replay adapter is installed; use --validate-only"),
            err);
    }

    Result<std::unique_ptr<storage::ReplayEngine>> engine =
        storage::ReplayEngine::Create(segment_paths, session->manifest, options);
    if (!engine.ok()) return Fail("replay", engine.status(), err);
    if (validate_only) {
        out << "replay validated: segments=" << segment_paths.size()
            << " topics=" << session->manifest.topics.size() << '\n';
        return kStorageExitSuccess;
    }
    const Status installed =
        (*engine)->InstallPublisherAdapter(replay_adapter);
    if (!installed.ok()) return Fail("replay", installed, err);

    size_t published = 0;
    if (step) {
        while (true) {
            Result<bool> advanced = (*engine)->Step();
            if (!advanced.ok()) return Fail("replay", advanced.status(), err);
            if (!*advanced) break;
            ++published;
        }
    } else {
        Result<size_t> count = (*engine)->Run();
        if (!count.ok()) return Fail("replay", count.status(), err);
        published = *count;
    }
    out << "replay complete: published=" << published << '\n';
    return kStorageExitSuccess;
}

struct RecordTopic {
    uint32_t id = 0;
    std::string name;
};

Result<RecordTopic> ParseRecordTopic(std::string_view value) {
    const size_t separator = value.find(':');
    if (separator == std::string_view::npos || separator == 0 ||
        separator + 1 >= value.size()) {
        return Invalid("--topic must use <id>:<name>");
    }
    Result<uint32_t> id =
        ParseUnsigned<uint32_t>(value.substr(0, separator), "topic ID");
    if (!id.ok()) return id.status();
    if (*id == 0) return Invalid("topic ID must be non-zero");
    std::string name(value.substr(separator + 1));
    if (name.size() > 1024 || name.find('\0') != std::string::npos) {
        return Invalid("topic name is empty or too long");
    }
    return RecordTopic{.id = *id, .name = std::move(name)};
}

std::string PartitionDirectoryName(uint32_t id) {
    std::ostringstream name;
    name << std::setw(4) << std::setfill('0') << id;
    return name.str();
}

Status CreateDirectoryTree(const std::filesystem::path& path) {
    std::error_code error;
    std::filesystem::create_directories(path, error);
    if (error) {
        return Status::Error(error == std::errc::permission_denied
                                 ? StatusCode::kPermissionDenied
                                 : StatusCode::kUnavailable,
                             "cannot create directory '" + path.string() +
                                 "': " + error.message());
    }
    return ValidateDirectory(path);
}

int CmdRecord(const std::vector<std::string>& args, std::ostream& out,
              std::ostream& err,
              RecorderServiceLauncher* service_launcher) {
    if (args.size() < 3 || (args[1] != "create" && args[1] != "run")) {
        return Fail("record",
                    Invalid("usage: record <create|run> <session_root> ..."),
                    err);
    }
    const std::filesystem::path root(args[2]);
    if (args[1] == "run") {
        if (args.size() != 3) {
            return Fail("record", Invalid("record run accepts only <session_root>"),
                        err);
        }
        if (service_launcher == nullptr) {
            return Fail(
                "record",
                Status::Error(
                    StatusCode::kUnsupported,
                    "no RecorderService launcher is installed; Bus-backed process assembly is required"),
                err);
        }
        const Status started = service_launcher->Run(root);
        if (!started.ok()) return Fail("record", started, err);
        out << "recording service exited: " << root.string() << '\n';
        return kStorageExitSuccess;
    }
    std::optional<uint64_t> recording_id;
    std::optional<uint64_t> owner_id;
    std::optional<uint64_t> owner_epoch;
    std::optional<uint64_t> config_version;
    std::optional<uint64_t> created_at_ns;
    std::optional<uint64_t> writer_id;
    uint32_t partition_count = 1;
    bool validate_only = false;
    std::vector<RecordTopic> topics;

    for (size_t index = 3; index < args.size(); ++index) {
        const std::string& flag = args[index];
        if (flag == "--validate-only") {
            validate_only = true;
            continue;
        }
        Result<std::string_view> value = FlagValue(args, &index);
        if (!value.ok()) return Fail("record", value.status(), err);
        if (flag == "--topic") {
            Result<RecordTopic> topic = ParseRecordTopic(*value);
            if (!topic.ok()) return Fail("record", topic.status(), err);
            topics.push_back(std::move(*topic));
        } else if (flag == "--recording-id" || flag == "--owner-id" ||
                   flag == "--owner-epoch" || flag == "--config-version" ||
                   flag == "--created-at-ns" || flag == "--writer-id") {
            Result<uint64_t> parsed = ParseUnsigned<uint64_t>(*value, flag);
            if (!parsed.ok()) return Fail("record", parsed.status(), err);
            std::optional<uint64_t>* destination = nullptr;
            if (flag == "--recording-id") destination = &recording_id;
            else if (flag == "--owner-id") destination = &owner_id;
            else if (flag == "--owner-epoch") destination = &owner_epoch;
            else if (flag == "--config-version") destination = &config_version;
            else if (flag == "--created-at-ns") destination = &created_at_ns;
            else destination = &writer_id;
            if (destination->has_value()) {
                return Fail("record", Invalid("duplicate flag: " + flag), err);
            }
            *destination = *parsed;
        } else if (flag == "--partitions") {
            Result<uint32_t> parsed = ParseUnsigned<uint32_t>(*value, flag);
            if (!parsed.ok()) return Fail("record", parsed.status(), err);
            partition_count = *parsed;
        } else {
            return Fail("record", Invalid("unknown flag: " + flag), err);
        }
    }

    if (validate_only) {
        Result<LoadedSession> session = LoadSession(root, true);
        if (!session.ok()) return Fail("record", session.status(), err);
        if (recording_id.has_value() &&
            *recording_id != session->manifest.session.recording_id) {
            return Fail("record", Corruption("recording ID differs from manifest"),
                        err);
        }
        if (config_version.has_value() &&
            *config_version != session->manifest.session.config_version) {
            return Fail("record", Corruption("config version differs from manifest"),
                        err);
        }
        for (const RecordTopic& expected : topics) {
            const storage::TopicTableEntry* actual =
                FindTopic(*session, expected.id);
            if (actual == nullptr || actual->topic_name != expected.name) {
                return Fail("record", Corruption("topic config differs from manifest"),
                            err);
            }
        }
        out << "recording configuration valid: recording_id="
            << session->manifest.session.recording_id
            << " topics=" << session->manifest.topics.size()
            << " partitions=" << session->partitions.size() << '\n';
        return kStorageExitSuccess;
    }

    if (!recording_id.has_value() || !owner_id.has_value() ||
        !owner_epoch.has_value() || !config_version.has_value() || topics.empty()) {
        return Fail(
            "record",
            Invalid("record creation requires --recording-id, --owner-id, "
                    "--owner-epoch, --config-version, and at least one --topic"),
            err);
    }
    if (*recording_id == 0 || *owner_id == 0 || *owner_epoch == 0 ||
        partition_count == 0 || partition_count > kMaximumRecordPartitions ||
        topics.size() > 1024) {
        return Fail("record", Invalid("record identities/counts are out of bounds"),
                    err);
    }
    if (!writer_id.has_value()) writer_id = *owner_id;
    if (*writer_id == 0) {
        return Fail("record", Invalid("writer ID must be non-zero"), err);
    }
    std::sort(topics.begin(), topics.end(),
              [](const RecordTopic& left, const RecordTopic& right) {
                  return std::pair(left.id, left.name) <
                         std::pair(right.id, right.name);
              });
    for (size_t index = 1; index < topics.size(); ++index) {
        if (topics[index - 1].id == topics[index].id ||
            topics[index - 1].name == topics[index].name) {
            return Fail("record", Invalid("topic IDs and names must be unique"),
                        err);
        }
    }

    if (!created_at_ns.has_value()) {
        created_at_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
    }
    Status created = CreateDirectoryTree(root);
    if (!created.ok()) return Fail("record", created, err);
    created = CreateDirectoryTree(root / "schemas");
    if (!created.ok()) return Fail("record", created, err);
    created = CreateDirectoryTree(root / "topics");
    if (!created.ok()) return Fail("record", created, err);

    const storage::RecordingSessionMetadata metadata{
        .recording_id = *recording_id,
        .created_at_ns = *created_at_ns,
        .owner_id = *owner_id,
        .owner_epoch = *owner_epoch,
        .config_version = *config_version,
    };
    Result<std::unique_ptr<storage::RecordingManifest>> manifest =
        storage::RecordingManifest::Create(root, metadata);
    if (!manifest.ok()) return Fail("record", manifest.status(), err);
    for (const RecordTopic& topic : topics) {
        const Status added = (*manifest)->AddTopic(storage::TopicTableEntry{
            .topic_id = topic.id,
            .topic_name = topic.name,
            .config_version = *config_version,
            .schema_snapshot = {},
        });
        if (!added.ok()) return Fail("record", added, err);
        const std::filesystem::path partitions_root =
            root / "topics" / std::to_string(topic.id) / "partitions";
        created = CreateDirectoryTree(partitions_root);
        if (!created.ok()) return Fail("record", created, err);
        for (uint32_t partition_id = 0; partition_id < partition_count;
             ++partition_id) {
            const std::filesystem::path partition_root =
                partitions_root / PartitionDirectoryName(partition_id);
            created = CreateDirectoryTree(partition_root / "segments");
            if (!created.ok()) return Fail("record", created, err);
            Result<std::unique_ptr<storage::PartitionManifest>> partition =
                storage::PartitionManifest::Create(
                    partition_root,
                    storage::PartitionMetadata{
                        .recording_id = *recording_id,
                        .topic_id = topic.id,
                        .partition_id = partition_id,
                        .writer_id = *writer_id,
                        .owner_epoch = *owner_epoch,
                        .config_version = *config_version,
                    });
            if (!partition.ok()) return Fail("record", partition.status(), err);
        }
    }
    out << "recording session created: " << root.string()
        << " recording_id=" << *recording_id << " topics=" << topics.size()
        << " partitions=" << topics.size() * partition_count
        << " service_state=not-started\n";
    return kStorageExitSuccess;
}

void PrintStorageUsage(std::ostream& err) {
    err << "storage commands:\n"
           "  storage inspect <session_root>\n"
           "  storage verify <segment> [<segment> ...]\n"
           "  storage repair <segment> [--apply --standalone]\n"
           "  replay <session_root> [--validate-only] [--topic <name|id>] "
           "[--segment <tracked>] ...\n"
           "  record create <session_root> --recording-id N --owner-id N "
           "--owner-epoch N --config-version N --topic <id>:<name> ...\n"
           "  record run <session_root>\n";
}

}  // namespace

int RunStorageCommand(const std::vector<std::string>& args, std::ostream& out,
                      std::ostream& err,
                      const StorageCommandServices& services) {
    try {
        if (args.empty()) {
            PrintStorageUsage(err);
            return kStorageExitUsage;
        }
        if (args.size() > kMaximumArguments ||
            std::any_of(args.begin(), args.end(), [](const std::string& value) {
                return value.size() > kMaximumArgumentBytes ||
                       value.find('\0') != std::string::npos;
            })) {
            return Fail(args.front(), Invalid("argument limits exceeded"), err);
        }
        if (args[0] == "inspect") return CmdInspect(args, out, err);
        if (args[0] == "verify") return CmdVerify(args, out, err);
        if (args[0] == "repair") return CmdRepair(args, out, err);
        if (args[0] == "replay") {
            return CmdReplay(args, out, err, services.replay_adapter);
        }
        if (args[0] == "record") {
            return CmdRecord(args, out, err,
                             services.recorder_service_launcher);
        }
        PrintStorageUsage(err);
        return Fail(args[0], Invalid("unknown storage command"), err);
    } catch (const std::bad_alloc&) {
        return Fail(args.empty() ? "command" : args.front(),
                    Status::Error(StatusCode::kResourceExhausted,
                                  "storage command allocation failed"),
                    err);
    } catch (const std::filesystem::filesystem_error& error) {
        return Fail(args.empty() ? "command" : args.front(),
                    Status::Error(StatusCode::kUnavailable, error.what()), err);
    } catch (const std::exception& error) {
        return Fail(args.empty() ? "command" : args.front(),
                    Status::Error(StatusCode::kInternal, error.what()), err);
    }
}

int RunStorageCommand(const std::vector<std::string>& args, std::ostream& out,
                      std::ostream& err,
                      storage::ReplayPublisherAdapter* replay_adapter) {
    return RunStorageCommand(
        args, out, err,
        StorageCommandServices{.replay_adapter = replay_adapter});
}

}  // namespace mino::tools
