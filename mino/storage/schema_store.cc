// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/schema_store.h"

#include <sys/file.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mino/bridge/crc32c.h"
#include "mino/common/status.h"
#include "mino/schema/canonical.h"
#include "mino/schema/registry.h"

namespace mino::storage {
namespace {

constexpr std::array<std::byte, 8> kManifestMagic = {
    std::byte{'M'}, std::byte{'I'}, std::byte{'N'}, std::byte{'O'},
    std::byte{'S'}, std::byte{'C'}, std::byte{'H'}, std::byte{'M'},
};
constexpr std::string_view kManifestFilename = "manifest";
constexpr std::string_view kOwnerLockFilename = ".owner.lock";
constexpr std::string_view kDescriptorSuffix = ".schema";
constexpr size_t kDigestHexLength = 64;
constexpr size_t kDescriptorFilenameLength =
    kDigestHexLength + kDescriptorSuffix.size();
constexpr size_t kManifestEntryFixedSize = 60;
constexpr size_t kHardMaxEntries = 1u << 20;
constexpr size_t kHardMaxManifestBytes = 256u << 20;
constexpr size_t kHardMaxDescriptorBytes = 16u << 20;
constexpr size_t kCrcOffset = 28;

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
    TemporaryFile(std::filesystem::path file_path,
                  FileDescriptor file_descriptor) noexcept
        : path(std::move(file_path)),
          descriptor(std::move(file_descriptor)) {}

    std::filesystem::path path;
    FileDescriptor descriptor;

    ~TemporaryFile() {
        if (path.empty()) return;
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
    }

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

Status Mismatch(std::string_view message) {
    return Status::Error(StatusCode::kSchemaMismatch, message);
}

Status IoError(std::string_view operation,
               const std::filesystem::path& path) {
    const int error = errno;
    return Status::Error(
        error == EACCES || error == EPERM ? StatusCode::kPermissionDenied
                                         : StatusCode::kUnavailable,
        std::string(operation) + " '" + path.string() + "': " +
            std::strerror(error));
}

int OpenFlags(int base) noexcept {
#ifdef O_CLOEXEC
    base |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    base |= O_NOFOLLOW;
#endif
    return base;
}

Status SetCloseOnExec(int fd) noexcept {
#ifdef O_CLOEXEC
    static_cast<void>(fd);
    return Status::Ok();
#else
    const int flags = ::fcntl(fd, F_GETFD);
    if (flags < 0 || ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot set close-on-exec flag");
    }
    return Status::Ok();
#endif
}

bool SameIdentity(const schema::SchemaIdentity& lhs,
                  const schema::SchemaIdentity& rhs) noexcept {
    return lhs.short_id() == rhs.short_id() &&
           lhs.canonical_digest() == rhs.canonical_digest() &&
           lhs.schema_version() == rhs.schema_version() &&
           lhs.layout_version() == rhs.layout_version();
}

bool ArtifactContainsIdentity(
    const schema::ValidatedDescriptorArtifact& artifact,
    const schema::SchemaIdentity& identity) noexcept {
    for (const schema::SchemaHandle& descriptor : artifact.descriptors()) {
        if (descriptor != nullptr &&
            SameIdentity(descriptor->identity(), identity)) {
            return true;
        }
    }
    return false;
}

Status ValidateOptions(const SchemaStoreOptions& options) {
    if (options.max_entries == 0 ||
        options.max_entries > kHardMaxEntries) {
        return Invalid("schema store max_entries is out of bounds");
    }
    if (options.max_manifest_bytes < kSchemaManifestHeaderSize ||
        options.max_manifest_bytes > kHardMaxManifestBytes) {
        return Invalid("schema store max_manifest_bytes is out of bounds");
    }
    if (options.max_descriptor_bytes == 0 ||
        options.max_descriptor_bytes > kHardMaxDescriptorBytes) {
        return Invalid("schema store max_descriptor_bytes is out of bounds");
    }
    return Status::Ok();
}

Status VerifyDirectory(const std::filesystem::path& path,
                       std::string_view role) {
    struct stat info {};
    if (::lstat(path.c_str(), &info) != 0) return IoError("cannot inspect", path);
    if (S_ISLNK(info.st_mode) || !S_ISDIR(info.st_mode)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             std::string(role) + " is not a real directory");
    }
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

Result<TemporaryFile> CreateTemporaryFile(
    const std::filesystem::path& directory, std::string_view role) {
    std::filesystem::path pattern_path =
        directory / ("." + std::string(role) + ".tmp.XXXXXX");
    std::string pattern = pattern_path.string();
    pattern.push_back('\0');
    const int fd = ::mkstemp(pattern.data());
    if (fd < 0) return IoError("cannot create temporary file", pattern_path);
    FileDescriptor descriptor(fd);
    const Status close_on_exec = SetCloseOnExec(fd);
    if (!close_on_exec.ok()) return close_on_exec;
    if (::fchmod(fd, 0644) != 0) {
        return IoError("cannot set temporary file mode", pattern_path);
    }
    return TemporaryFile{std::filesystem::path(pattern.c_str()),
                         std::move(descriptor)};
}

Status WriteAll(int fd, const std::filesystem::path& path,
                std::span<const std::byte> bytes) {
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t written =
            ::write(fd, bytes.data() + offset, bytes.size() - offset);
        if (written < 0) {
            if (errno == EINTR) continue;
            return IoError("cannot write", path);
        }
        if (written == 0) {
            return Status::Error(StatusCode::kUnavailable,
                                 "zero-byte schema store write");
        }
        offset += static_cast<size_t>(written);
    }
    return Status::Ok();
}

Status DataSync(int fd, const std::filesystem::path& path) {
#if defined(__APPLE__)
    if (::fsync(fd) != 0) return IoError("cannot fsync", path);
#else
    if (::fdatasync(fd) != 0) return IoError("cannot fdatasync", path);
#endif
    return Status::Ok();
}

Status AtomicRename(const std::filesystem::path& from,
                    const std::filesystem::path& to) {
    if (::rename(from.c_str(), to.c_str()) != 0) {
        return IoError("cannot rename to", to);
    }
    return Status::Ok();
}

Status RunFaultHook(const SchemaStoreOptions& options,
                    SchemaStoreFaultPoint point) {
    if (options.fault_hook == nullptr) return Status::Ok();
    return options.fault_hook(point, options.fault_hook_context);
}

Result<std::vector<std::byte>> ReadRegularFile(
    const std::filesystem::path& path, size_t max_bytes,
    StatusCode invalid_type_code, StatusCode missing_code) {
    struct stat link_info {};
    if (::lstat(path.c_str(), &link_info) != 0) {
        if (errno == ENOENT) {
            return Status::Error(missing_code, "schema store file is missing");
        }
        return IoError("cannot inspect", path);
    }
    if (S_ISLNK(link_info.st_mode) || !S_ISREG(link_info.st_mode)) {
        return Status::Error(invalid_type_code,
                             "schema store path is not a regular file");
    }

    FileDescriptor descriptor(::open(path.c_str(), OpenFlags(O_RDONLY)));
    if (descriptor.get() < 0) return IoError("cannot open", path);
    struct stat info {};
    if (::fstat(descriptor.get(), &info) != 0) {
        return IoError("cannot stat open file", path);
    }
    if (!S_ISREG(info.st_mode)) {
        return Status::Error(invalid_type_code,
                             "schema store path changed file type");
    }
    if (info.st_size < 0 || static_cast<uintmax_t>(info.st_size) > max_bytes) {
        return Exhausted("schema store file exceeds configured byte limit");
    }

    std::vector<std::byte> bytes(static_cast<size_t>(info.st_size));
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t count =
            ::read(descriptor.get(), bytes.data() + offset, bytes.size() - offset);
        if (count < 0) {
            if (errno == EINTR) continue;
            return IoError("cannot read", path);
        }
        if (count == 0) return Corruption("schema store file is truncated");
        offset += static_cast<size_t>(count);
    }
    std::byte extra{};
    while (true) {
        const ssize_t count = ::read(descriptor.get(), &extra, 1);
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) return IoError("cannot finish reading", path);
        if (count != 0) return Corruption("schema store file grew while read");
        break;
    }
    return bytes;
}

void AppendU16(std::vector<std::byte>* bytes, uint16_t value) {
    for (size_t i = 0; i < 2; ++i) {
        bytes->push_back(static_cast<std::byte>(value & 0xffu));
        value >>= 8;
    }
}

void AppendU32(std::vector<std::byte>* bytes, uint32_t value) {
    for (size_t i = 0; i < 4; ++i) {
        bytes->push_back(static_cast<std::byte>(value & 0xffu));
        value >>= 8;
    }
}

void AppendU64(std::vector<std::byte>* bytes, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) {
        bytes->push_back(static_cast<std::byte>(value & 0xffu));
        value >>= 8;
    }
}

void WriteU32(std::vector<std::byte>* bytes, size_t offset,
              uint32_t value) noexcept {
    for (size_t i = 0; i < 4; ++i) {
        (*bytes)[offset + i] = static_cast<std::byte>(value & 0xffu);
        value >>= 8;
    }
}

uint16_t ReadU16(std::span<const std::byte> bytes, size_t offset) noexcept {
    uint16_t value = 0;
    for (size_t i = 0; i < 2; ++i) {
        value |= static_cast<uint16_t>(static_cast<uint8_t>(bytes[offset + i]))
                 << (i * 8);
    }
    return value;
}

uint32_t ReadU32(std::span<const std::byte> bytes, size_t offset) noexcept {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
        value |= static_cast<uint32_t>(static_cast<uint8_t>(bytes[offset + i]))
                 << (i * 8);
    }
    return value;
}

uint64_t ReadU64(std::span<const std::byte> bytes, size_t offset) noexcept {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) {
        value |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[offset + i]))
                 << (i * 8);
    }
    return value;
}

uint32_t ManifestCrc(std::span<const std::byte> bytes) noexcept {
    bridge::Crc32cAccumulator crc;
    crc.Update(bytes.first(kCrcOffset));
    constexpr std::array<std::byte, 4> zeros{};
    crc.Update(zeros);
    crc.Update(bytes.subspan(kCrcOffset + zeros.size()));
    return crc.Finish();
}

std::string DescriptorFilename(const schema::CanonicalDigest& digest) {
    return schema::DigestHex(digest) + std::string(kDescriptorSuffix);
}

Result<std::vector<std::byte>> EncodeManifest(
    const std::map<SchemaRef, SchemaStoreEntry>& entries,
    SchemaRef high_watermark, const SchemaStoreOptions& options) {
    if (entries.size() > options.max_entries ||
        entries.size() > std::numeric_limits<uint32_t>::max()) {
        return Exhausted("schema manifest entry limit reached");
    }
    if ((!entries.empty() && high_watermark < entries.rbegin()->first) ||
        (entries.empty() && high_watermark != kInvalidSchemaRef)) {
        return Status::Error(StatusCode::kInternal,
                             "schema manifest high watermark is inconsistent");
    }

    size_t encoded_size = kSchemaManifestHeaderSize;
    for (const auto& [ref, entry] : entries) {
        static_cast<void>(ref);
        const std::string filename = entry.descriptor_path.filename().string();
        if (filename.size() != kDescriptorFilenameLength ||
            encoded_size > options.max_manifest_bytes -
                               kManifestEntryFixedSize - filename.size()) {
            return Exhausted("schema manifest exceeds configured byte limit");
        }
        encoded_size += kManifestEntryFixedSize + filename.size();
    }
    if (encoded_size > options.max_manifest_bytes) {
        return Exhausted("schema manifest exceeds configured byte limit");
    }

    std::vector<std::byte> encoded;
    encoded.reserve(encoded_size);
    encoded.insert(encoded.end(), kManifestMagic.begin(), kManifestMagic.end());
    AppendU16(&encoded, kSchemaManifestVersion);
    AppendU16(&encoded, static_cast<uint16_t>(kSchemaManifestHeaderSize));
    AppendU32(&encoded, static_cast<uint32_t>(entries.size()));
    AppendU32(&encoded, high_watermark);
    AppendU64(&encoded,
              static_cast<uint64_t>(encoded_size - kSchemaManifestHeaderSize));
    AppendU32(&encoded, 0);
    AppendU32(&encoded, 0);

    for (const auto& [ref, entry] : entries) {
        const std::string filename = entry.descriptor_path.filename().string();
        AppendU32(&encoded,
                  static_cast<uint32_t>(kManifestEntryFixedSize +
                                        filename.size()));
        AppendU32(&encoded, ref);
        AppendU64(&encoded, entry.identity.short_id());
        encoded.insert(encoded.end(), entry.identity.canonical_digest().begin(),
                       entry.identity.canonical_digest().end());
        AppendU32(&encoded, entry.identity.schema_version());
        AppendU32(&encoded, entry.identity.layout_version());
        AppendU32(&encoded, static_cast<uint32_t>(filename.size()));
        encoded.insert(encoded.end(),
                       reinterpret_cast<const std::byte*>(filename.data()),
                       reinterpret_cast<const std::byte*>(filename.data()) +
                           filename.size());
    }
    WriteU32(&encoded, kCrcOffset, ManifestCrc(encoded));
    return encoded;
}

Result<std::vector<SchemaStoreEntry>> DecodeManifest(
    std::span<const std::byte> bytes, const std::filesystem::path& schemas_path,
    const SchemaStoreOptions& options, SchemaRef* high_watermark) {
    if (bytes.size() < kSchemaManifestHeaderSize) {
        return Corruption("schema manifest is truncated");
    }
    if (!std::equal(kManifestMagic.begin(), kManifestMagic.end(),
                    bytes.begin())) {
        return Corruption("schema manifest magic is unknown");
    }
    if (ReadU16(bytes, 8) != kSchemaManifestVersion) {
        return Corruption("schema manifest version is unsupported");
    }
    if (ReadU16(bytes, 10) != kSchemaManifestHeaderSize) {
        return Corruption("schema manifest header size is invalid");
    }
    const uint32_t entry_count = ReadU32(bytes, 12);
    const SchemaRef stored_high_watermark = ReadU32(bytes, 16);
    const uint64_t payload_size = ReadU64(bytes, 20);
    const uint32_t stored_crc = ReadU32(bytes, kCrcOffset);
    if (ReadU32(bytes, 32) != 0) {
        return Corruption("schema manifest has unknown flags");
    }
    if (entry_count > options.max_entries) {
        return Exhausted("schema manifest entry count exceeds configured limit");
    }
    if (payload_size != bytes.size() - kSchemaManifestHeaderSize) {
        return Corruption("schema manifest payload length is invalid");
    }
    if (stored_crc != ManifestCrc(bytes)) {
        return Corruption("schema manifest CRC32C mismatch");
    }

    std::vector<SchemaStoreEntry> entries;
    entries.reserve(entry_count);
    std::map<schema::CanonicalDigest, SchemaRef> seen_digests;
    size_t offset = kSchemaManifestHeaderSize;
    SchemaRef previous_ref = kInvalidSchemaRef;
    for (uint32_t index = 0; index < entry_count; ++index) {
        if (offset > bytes.size() ||
            bytes.size() - offset < kManifestEntryFixedSize) {
            return Corruption("schema manifest entry is truncated");
        }
        const uint32_t entry_size = ReadU32(bytes, offset);
        if (entry_size < kManifestEntryFixedSize ||
            entry_size > bytes.size() - offset) {
            return Corruption("schema manifest entry length is invalid");
        }
        const SchemaRef ref = ReadU32(bytes, offset + 4);
        if (ref == kInvalidSchemaRef || ref <= previous_ref) {
            return Corruption("schema manifest refs are not unique and increasing");
        }
        const uint64_t short_id = ReadU64(bytes, offset + 8);
        schema::CanonicalDigest digest{};
        for (size_t byte = 0; byte < digest.size(); ++byte) {
            digest[byte] = bytes[offset + 16 + byte];
        }
        const uint32_t schema_version = ReadU32(bytes, offset + 48);
        const uint32_t layout_version = ReadU32(bytes, offset + 52);
        const uint32_t filename_size = ReadU32(bytes, offset + 56);
        if (filename_size != kDescriptorFilenameLength ||
            entry_size != kManifestEntryFixedSize + filename_size) {
            return Corruption("schema manifest descriptor path length is invalid");
        }
        const char* filename_data = reinterpret_cast<const char*>(
            bytes.data() + offset + kManifestEntryFixedSize);
        const std::string filename(filename_data, filename_size);
        if (filename != DescriptorFilename(digest)) {
            return Corruption("schema manifest descriptor filename is not canonical");
        }
        if (!seen_digests.emplace(digest, ref).second) {
            return Corruption("schema manifest contains a duplicate digest");
        }
        schema::SchemaIdentity identity(short_id, digest, schema_version,
                                        layout_version);
        entries.push_back(
            SchemaStoreEntry{ref, identity, schemas_path / filename});
        previous_ref = ref;
        offset += entry_size;
    }
    if (offset != bytes.size()) {
        return Corruption("schema manifest contains trailing bytes");
    }
    if ((entries.empty() && stored_high_watermark != kInvalidSchemaRef) ||
        (!entries.empty() && stored_high_watermark < previous_ref)) {
        return Corruption("schema manifest high watermark is invalid");
    }
    *high_watermark = stored_high_watermark;
    return entries;
}

Status ValidateRecoveredDescriptor(
    const SchemaStoreEntry& entry, schema::SchemaRegistry* registry,
    size_t max_descriptor_bytes) {
    auto bytes = ReadRegularFile(entry.descriptor_path, max_descriptor_bytes,
                                 StatusCode::kCorruption,
                                 StatusCode::kCorruption);
    if (!bytes.ok()) return bytes.status();
    auto validated = registry->ValidateDescriptorArtifact(*bytes);
    if (!validated.ok()) {
        if (validated.status().code() == StatusCode::kResourceExhausted) {
            return validated.status();
        }
        return Corruption("stored descriptor artifact is invalid");
    }
    if (!ArtifactContainsIdentity(*validated, entry.identity)) {
        return Corruption("stored descriptor artifact identity does not match manifest");
    }
    return Status::Ok();
}

Result<int> AcquireOwnerLock(const std::filesystem::path& schemas_path) {
    const std::filesystem::path lock_path =
        schemas_path / std::string(kOwnerLockFilename);
    const int fd = ::open(lock_path.c_str(),
                          OpenFlags(O_RDWR | O_CREAT), 0644);
    if (fd < 0) return IoError("cannot open schema store owner lock", lock_path);
    FileDescriptor descriptor(fd);
    struct stat info {};
    if (::fstat(fd, &info) != 0) {
        return IoError("cannot stat schema store owner lock", lock_path);
    }
    if (!S_ISREG(info.st_mode)) {
        return Invalid("schema store owner lock is not a regular file");
    }
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            return Status::Error(StatusCode::kUnavailable,
                                 "schema store already has an owner");
        }
        return IoError("cannot lock schema store", lock_path);
    }
    return descriptor.release();
}

}  // namespace

SchemaStore::SchemaStore(std::filesystem::path session_root,
                         std::filesystem::path schemas_directory,
                         schema::SchemaRegistry* registry,
                         SchemaStoreOptions options,
                         int owner_lock_fd) noexcept
    : session_root_(std::move(session_root)),
      schemas_directory_(std::move(schemas_directory)),
      registry_(registry),
      options_(options),
      owner_lock_fd_(owner_lock_fd) {}

SchemaStore::~SchemaStore() {
    if (owner_lock_fd_ >= 0) static_cast<void>(::close(owner_lock_fd_));
}

Result<std::unique_ptr<SchemaStore>> SchemaStore::Open(
    const std::filesystem::path& session_root,
    schema::SchemaRegistry* registry,
    const SchemaStoreOptions& options) noexcept {
    try {
        if (session_root.empty()) return Invalid("schema store root is empty");
        if (registry == nullptr) return Invalid("schema registry is null");
        const Status valid_options = ValidateOptions(options);
        if (!valid_options.ok()) return valid_options;

        std::error_code error;
        std::filesystem::create_directories(session_root, error);
        if (error) {
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot create schema store session root: " +
                                     error.message());
        }
        const Status valid_root = VerifyDirectory(session_root, "session root");
        if (!valid_root.ok()) return valid_root;

        const std::filesystem::path schemas_path = session_root / "schemas";
        const bool schemas_existed = std::filesystem::exists(schemas_path, error);
        if (error) {
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot inspect schemas directory: " +
                                     error.message());
        }
        std::filesystem::create_directory(schemas_path, error);
        if (error) {
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot create schemas directory: " +
                                     error.message());
        }
        const Status valid_schemas = VerifyDirectory(schemas_path, "schemas path");
        if (!valid_schemas.ok()) return valid_schemas;
        if (!schemas_existed) {
            const Status synced_root = SyncDirectory(session_root);
            if (!synced_root.ok()) return synced_root;
        }

        auto owner_lock = AcquireOwnerLock(schemas_path);
        if (!owner_lock.ok()) return owner_lock.status();
        FileDescriptor owner_lock_guard(*owner_lock);
        const Status synced_schemas = SyncDirectory(schemas_path);
        if (!synced_schemas.ok()) return synced_schemas;

        auto store = std::unique_ptr<SchemaStore>(new SchemaStore(
            session_root, schemas_path, registry, options,
            owner_lock_guard.release()));

        const std::filesystem::path manifest_path =
            schemas_path / std::string(kManifestFilename);
        struct stat manifest_info {};
        if (::lstat(manifest_path.c_str(), &manifest_info) != 0) {
            if (errno != ENOENT) {
                return IoError("cannot inspect schema manifest", manifest_path);
            }

            for (const auto& entry :
                 std::filesystem::directory_iterator(schemas_path)) {
                if (entry.path().extension() == kDescriptorSuffix) {
                    return Corruption(
                        "schema manifest is missing while descriptors exist");
                }
            }

            const std::map<SchemaRef, SchemaStoreEntry> empty_entries;
            auto encoded = EncodeManifest(empty_entries, kInvalidSchemaRef,
                                          options);
            if (!encoded.ok()) return encoded.status();
            auto temporary = CreateTemporaryFile(schemas_path, "manifest");
            if (!temporary.ok()) return temporary.status();
            const Status written = WriteAll(temporary->descriptor.get(),
                                            temporary->path, *encoded);
            if (!written.ok()) return written;
            const Status synced =
                DataSync(temporary->descriptor.get(), temporary->path);
            if (!synced.ok()) return synced;
            const Status renamed =
                AtomicRename(temporary->path, manifest_path);
            if (!renamed.ok()) return renamed;
            temporary->path.clear();
            const Status directory_synced = SyncDirectory(schemas_path);
            if (!directory_synced.ok()) return directory_synced;
            return store;
        }
        auto manifest = ReadRegularFile(
            manifest_path, options.max_manifest_bytes, StatusCode::kCorruption,
            StatusCode::kCorruption);
        if (!manifest.ok()) return manifest.status();
        SchemaRef recovered_high_watermark = kInvalidSchemaRef;
        auto entries = DecodeManifest(*manifest, schemas_path, options,
                                      &recovered_high_watermark);
        if (!entries.ok()) return entries.status();

        for (const SchemaStoreEntry& entry : *entries) {
            const Status valid_descriptor = ValidateRecoveredDescriptor(
                entry, registry, options.max_descriptor_bytes);
            if (!valid_descriptor.ok()) return valid_descriptor;
            store->by_ref_.emplace(entry.ref, entry);
            store->by_digest_.emplace(entry.identity.canonical_digest(),
                                      entry.ref);
        }
        store->high_watermark_ = recovered_high_watermark;
        return store;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaRef> SchemaStore::Persist(
    const schema::SchemaIdentity& identity,
    std::span<const std::byte> descriptor_artifact) noexcept {
    try {
        if (poisoned_) {
            return Status::Error(StatusCode::kUnavailable,
                                 "schema store requires reopen after uncertain commit");
        }
        if (descriptor_artifact.empty()) {
            return Invalid("descriptor artifact is empty");
        }
        if (descriptor_artifact.size() > options_.max_descriptor_bytes) {
            return Exhausted("descriptor artifact exceeds configured byte limit");
        }

        auto validated =
            registry_->ValidateDescriptorArtifact(descriptor_artifact);
        if (!validated.ok()) return validated.status();
        if (!ArtifactContainsIdentity(*validated, identity)) {
            return Mismatch("descriptor artifact does not contain identity");
        }

        const std::string filename =
            DescriptorFilename(identity.canonical_digest());
        const std::filesystem::path descriptor_path =
            schemas_directory_ / filename;

        const auto known = by_digest_.find(identity.canonical_digest());
        if (known != by_digest_.end()) {
            const auto entry = by_ref_.find(known->second);
            if (entry == by_ref_.end()) {
                return Status::Error(StatusCode::kInternal,
                                     "schema store index is inconsistent");
            }
            if (!SameIdentity(entry->second.identity, identity)) {
                return Mismatch("canonical digest has a different identity");
            }
            auto existing = ReadRegularFile(
                descriptor_path, options_.max_descriptor_bytes,
                StatusCode::kCorruption, StatusCode::kCorruption);
            if (!existing.ok()) return existing.status();
            if (!std::equal(existing->begin(), existing->end(),
                            descriptor_artifact.begin(),
                            descriptor_artifact.end())) {
                return Mismatch("canonical digest has different artifact bytes");
            }
            return known->second;
        }

        if (by_ref_.size() >= options_.max_entries) {
            return Exhausted("schema store entry limit reached");
        }
        if (high_watermark_ == std::numeric_limits<SchemaRef>::max()) {
            return Exhausted("schema ref space is exhausted");
        }
        const SchemaRef new_ref = high_watermark_ + 1;

        std::map<SchemaRef, SchemaStoreEntry> next_by_ref = by_ref_;
        std::map<schema::CanonicalDigest, SchemaRef> next_by_digest = by_digest_;
        SchemaStoreEntry new_entry{new_ref, identity, descriptor_path};
        next_by_ref.emplace(new_ref, new_entry);
        next_by_digest.emplace(identity.canonical_digest(), new_ref);

        struct stat descriptor_info {};
        bool descriptor_exists = true;
        if (::lstat(descriptor_path.c_str(), &descriptor_info) != 0) {
            if (errno != ENOENT) {
                return IoError("cannot inspect descriptor", descriptor_path);
            }
            descriptor_exists = false;
        }
        if (descriptor_exists) {
            auto existing = ReadRegularFile(
                descriptor_path, options_.max_descriptor_bytes,
                StatusCode::kCorruption, StatusCode::kCorruption);
            if (!existing.ok()) return existing.status();
            if (!std::equal(existing->begin(), existing->end(),
                            descriptor_artifact.begin(),
                            descriptor_artifact.end())) {
                return Mismatch("existing digest file has different bytes");
            }
        } else {
            auto temporary = CreateTemporaryFile(schemas_directory_, "descriptor");
            if (!temporary.ok()) return temporary.status();
            const Status written = WriteAll(temporary->descriptor.get(),
                                            temporary->path,
                                            descriptor_artifact);
            if (!written.ok()) return written;
            const Status after_write = RunFaultHook(
                options_, SchemaStoreFaultPoint::kAfterDescriptorTempWrite);
            if (!after_write.ok()) return after_write;
            const Status synced =
                DataSync(temporary->descriptor.get(), temporary->path);
            if (!synced.ok()) return synced;
            const Status after_sync = RunFaultHook(
                options_, SchemaStoreFaultPoint::kAfterDescriptorSync);
            if (!after_sync.ok()) return after_sync;
            const Status renamed = AtomicRename(temporary->path, descriptor_path);
            if (!renamed.ok()) return renamed;
            temporary->path.clear();
            const Status after_rename = RunFaultHook(
                options_, SchemaStoreFaultPoint::kAfterDescriptorRename);
            if (!after_rename.ok()) return after_rename;
        }
        // This sync is also required when reusing an orphan whose prior attempt
        // failed after rename but before syncing the directory entry.
        const Status directory_synced = SyncDirectory(schemas_directory_);
        if (!directory_synced.ok()) return directory_synced;
        const Status after_directory_sync = RunFaultHook(
            options_, SchemaStoreFaultPoint::kAfterDescriptorDirectorySync);
        if (!after_directory_sync.ok()) return after_directory_sync;

        auto encoded_manifest = EncodeManifest(next_by_ref, new_ref, options_);
        if (!encoded_manifest.ok()) return encoded_manifest.status();
        auto manifest_temporary =
            CreateTemporaryFile(schemas_directory_, "manifest");
        if (!manifest_temporary.ok()) return manifest_temporary.status();
        const Status manifest_written =
            WriteAll(manifest_temporary->descriptor.get(),
                     manifest_temporary->path, *encoded_manifest);
        if (!manifest_written.ok()) return manifest_written;
        const Status after_manifest_write = RunFaultHook(
            options_, SchemaStoreFaultPoint::kAfterManifestTempWrite);
        if (!after_manifest_write.ok()) return after_manifest_write;
        const Status manifest_synced = DataSync(
            manifest_temporary->descriptor.get(), manifest_temporary->path);
        if (!manifest_synced.ok()) return manifest_synced;
        const Status after_manifest_sync = RunFaultHook(
            options_, SchemaStoreFaultPoint::kAfterManifestSync);
        if (!after_manifest_sync.ok()) return after_manifest_sync;
        const std::filesystem::path manifest_path =
            schemas_directory_ / std::string(kManifestFilename);
        const Status manifest_renamed =
            AtomicRename(manifest_temporary->path, manifest_path);
        if (!manifest_renamed.ok()) return manifest_renamed;
        manifest_temporary->path.clear();
        const Status after_manifest_rename = RunFaultHook(
            options_, SchemaStoreFaultPoint::kAfterManifestRename);
        if (!after_manifest_rename.ok()) {
            poisoned_ = true;
            return after_manifest_rename;
        }
        const Status manifest_directory_synced =
            SyncDirectory(schemas_directory_);
        if (!manifest_directory_synced.ok()) {
            poisoned_ = true;
            return manifest_directory_synced;
        }
        const Status after_manifest_directory_sync = RunFaultHook(
            options_, SchemaStoreFaultPoint::kAfterManifestDirectorySync);
        if (!after_manifest_directory_sync.ok()) {
            poisoned_ = true;
            return after_manifest_directory_sync;
        }

        by_ref_.swap(next_by_ref);
        by_digest_.swap(next_by_digest);
        high_watermark_ = new_ref;
        return new_ref;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaStoreEntry> SchemaStore::Resolve(SchemaRef ref) const noexcept {
    try {
        if (ref == kInvalidSchemaRef) {
            return Invalid("schema ref zero is reserved");
        }
        const auto entry = by_ref_.find(ref);
        if (entry == by_ref_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "schema ref is not committed");
        }
        return entry->second;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaRef> SchemaStore::FindRef(
    const schema::SchemaIdentity& identity) const noexcept {
    try {
        const auto found = by_digest_.find(identity.canonical_digest());
        if (found == by_digest_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "schema identity is not committed");
        }
        const auto entry = by_ref_.find(found->second);
        if (entry == by_ref_.end()) {
            return Status::Error(StatusCode::kInternal,
                                 "schema store index is inconsistent");
        }
        if (!SameIdentity(entry->second.identity, identity)) {
            return Mismatch("canonical digest has a different identity");
        }
        return found->second;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaRef> SchemaStore::FindRef(
    const schema::CanonicalDigest& digest) const noexcept {
    try {
        const auto found = by_digest_.find(digest);
        if (found == by_digest_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "schema digest is not committed");
        }
        return found->second;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::storage
