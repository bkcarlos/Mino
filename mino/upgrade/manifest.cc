// Copyright 2026 The Mino Authors

#include "mino/upgrade/manifest.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <string>
#include <utility>

#include "mino/bridge/crc32c.h"
#include "mino/common/status.h"

namespace mino::upgrade {
namespace {

constexpr std::array<std::byte, 8> kMagic = {
    std::byte{'M'}, std::byte{'I'}, std::byte{'N'}, std::byte{'O'},
    std::byte{'U'}, std::byte{'P'}, std::byte{'G'}, std::byte{'1'},
};
constexpr size_t kCrcBytes = 4;

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}
Status Corrupt(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}
Status IoError(std::string_view operation, const std::filesystem::path& path) {
    const int error = errno;
    return Status::Error(
        error == EACCES || error == EPERM || error == ELOOP
            ? StatusCode::kPermissionDenied
            : StatusCode::kUnavailable,
        std::string(operation) + " '" + path.string() + "': " +
            std::strerror(error));
}

bool ValidPhase(UpgradePhase phase) noexcept {
    switch (phase) {
        case UpgradePhase::kPrepare:
        case UpgradePhase::kValidate:
        case UpgradePhase::kDrain:
        case UpgradePhase::kCutover:
        case UpgradePhase::kObserve:
        case UpgradePhase::kCommit:
        case UpgradePhase::kRollback:
        case UpgradePhase::kFail:
            return true;
    }
    return false;
}

bool LegalTransition(UpgradePhase from, UpgradePhase to) noexcept {
    if (IsTerminalPhase(from)) return false;
    if (to == UpgradePhase::kFail) return true;
    if (to == UpgradePhase::kRollback) {
        return from == UpgradePhase::kPrepare ||
               from == UpgradePhase::kValidate ||
               from == UpgradePhase::kDrain ||
               from == UpgradePhase::kCutover ||
               from == UpgradePhase::kObserve;
    }
    return (from == UpgradePhase::kPrepare && to == UpgradePhase::kValidate) ||
           (from == UpgradePhase::kValidate && to == UpgradePhase::kDrain) ||
           (from == UpgradePhase::kDrain && to == UpgradePhase::kCutover) ||
           (from == UpgradePhase::kCutover && to == UpgradePhase::kObserve) ||
           (from == UpgradePhase::kObserve && to == UpgradePhase::kCommit);
}

Status ValidateOptions(const UpgradeManifestOptions& options) {
    if (options.maximum_file_bytes < 256 ||
        options.maximum_file_bytes > kMaximumUpgradeFileBytes ||
        options.maximum_journal_entries == 0 ||
        options.maximum_journal_entries > kMaximumUpgradeJournalEntries) {
        return Invalid("upgrade manifest bounds are invalid");
    }
    return Status::Ok();
}

Status ValidateSnapshot(const UpgradeSnapshot& snapshot,
                        const UpgradeManifestOptions& options) {
    MINO_RETURN_IF_ERROR(ValidateOptions(options));
    MINO_RETURN_IF_ERROR(ValidateUpgradePlan(snapshot.plan));
    if (snapshot.generation == 0 || snapshot.created_at_ns == 0 ||
        snapshot.updated_at_ns < snapshot.created_at_ns ||
        !ValidPhase(snapshot.phase) || snapshot.journal.empty() ||
        snapshot.journal.size() > options.maximum_journal_entries ||
        snapshot.terminal_reason.size() > kMaximumUpgradeStringBytes) {
        return Invalid("upgrade snapshot header or journal is invalid");
    }
    UpgradePhase previous = UpgradePhase::kPrepare;
    for (size_t index = 0; index < snapshot.journal.size(); ++index) {
        const UpgradeJournalEntry& entry = snapshot.journal[index];
        if (entry.sequence != index + 1 || !ValidPhase(entry.phase) ||
            entry.timestamp_ns < snapshot.created_at_ns ||
            entry.timestamp_ns > snapshot.updated_at_ns ||
            entry.detail.size() > kMaximumUpgradeStringBytes) {
            return Invalid("upgrade journal entry is invalid");
        }
        if (index == 0) {
            if (entry.phase != UpgradePhase::kPrepare) {
                return Invalid("upgrade journal must begin at prepare");
            }
        } else if (!LegalTransition(previous, entry.phase)) {
            return Invalid("upgrade journal contains an illegal transition");
        }
        previous = entry.phase;
    }
    if (snapshot.journal.back().phase != snapshot.phase ||
        snapshot.generation != snapshot.journal.size() ||
        ((snapshot.phase == UpgradePhase::kFail) !=
         !snapshot.terminal_reason.empty())) {
        return Invalid("upgrade snapshot phase, generation, or terminal reason disagrees with journal");
    }
    return Status::Ok();
}

class Writer {
public:
    void U8(uint8_t value) { bytes_.push_back(static_cast<std::byte>(value)); }
    void U16(uint16_t value) {
        for (size_t i = 0; i < 2; ++i) {
            U8(static_cast<uint8_t>(value));
            value >>= 8;
        }
    }
    void U32(uint32_t value) {
        for (size_t i = 0; i < 4; ++i) {
            U8(static_cast<uint8_t>(value));
            value >>= 8;
        }
    }
    void U64(uint64_t value) {
        for (size_t i = 0; i < 8; ++i) {
            U8(static_cast<uint8_t>(value));
            value >>= 8;
        }
    }
    void String(std::string_view value) {
        U32(static_cast<uint32_t>(value.size()));
        bytes_.insert(bytes_.end(),
                      reinterpret_cast<const std::byte*>(value.data()),
                      reinterpret_cast<const std::byte*>(value.data() + value.size()));
    }
    void Raw(std::span<const std::byte> value) {
        bytes_.insert(bytes_.end(), value.begin(), value.end());
    }
    std::vector<std::byte> Finish() && { return std::move(bytes_); }

private:
    std::vector<std::byte> bytes_;
};

class Cursor {
public:
    explicit Cursor(std::span<const std::byte> bytes) noexcept : bytes_(bytes) {}
    Result<uint8_t> U8() noexcept {
        if (!Available(1)) return Corrupt("upgrade manifest is truncated");
        return static_cast<uint8_t>(bytes_[offset_++]);
    }
    Result<uint16_t> U16() noexcept {
        uint16_t value = 0;
        for (size_t i = 0; i < 2; ++i) {
            MINO_ASSIGN_OR_RETURN(const uint8_t byte, U8());
            value |= static_cast<uint16_t>(byte) << (i * 8);
        }
        return value;
    }
    Result<uint32_t> U32() noexcept {
        uint32_t value = 0;
        for (size_t i = 0; i < 4; ++i) {
            MINO_ASSIGN_OR_RETURN(const uint8_t byte, U8());
            value |= static_cast<uint32_t>(byte) << (i * 8);
        }
        return value;
    }
    Result<uint64_t> U64() noexcept {
        uint64_t value = 0;
        for (size_t i = 0; i < 8; ++i) {
            MINO_ASSIGN_OR_RETURN(const uint8_t byte, U8());
            value |= static_cast<uint64_t>(byte) << (i * 8);
        }
        return value;
    }
    Result<std::string> String() {
        MINO_ASSIGN_OR_RETURN(const uint32_t size, U32());
        if (size > kMaximumUpgradeStringBytes || !Available(size)) {
            return Corrupt("upgrade manifest string is invalid or truncated");
        }
        const char* begin = reinterpret_cast<const char*>(bytes_.data() + offset_);
        std::string value(begin, size);
        offset_ += size;
        if (value.find('\0') != std::string::npos) {
            return Corrupt("upgrade manifest string contains NUL");
        }
        return value;
    }
    Result<schema::CanonicalDigest> Digest() noexcept {
        schema::CanonicalDigest digest{};
        if (!Available(digest.size())) {
            return Corrupt("upgrade schema digest is truncated");
        }
        std::copy_n(bytes_.begin() + static_cast<std::ptrdiff_t>(offset_),
                    digest.size(), digest.begin());
        offset_ += digest.size();
        return digest;
    }
    bool done() const noexcept { return offset_ == bytes_.size(); }

private:
    bool Available(size_t count) const noexcept {
        return offset_ <= bytes_.size() && count <= bytes_.size() - offset_;
    }
    std::span<const std::byte> bytes_;
    size_t offset_ = 0;
};

void EncodeRegion(const RegionIdentity& region, Writer* writer) {
    writer->String(region.name);
    writer->U32(region.region_id);
    writer->U64(region.uuid_lo);
    writer->U64(region.uuid_hi);
    writer->U16(region.layout_version);
    writer->U64(region.security_domain.value);
}

Result<RegionIdentity> DecodeRegion(Cursor* cursor) {
    RegionIdentity region;
    MINO_ASSIGN_OR_RETURN(region.name, cursor->String());
    MINO_ASSIGN_OR_RETURN(region.region_id, cursor->U32());
    MINO_ASSIGN_OR_RETURN(region.uuid_lo, cursor->U64());
    MINO_ASSIGN_OR_RETURN(region.uuid_hi, cursor->U64());
    MINO_ASSIGN_OR_RETURN(region.layout_version, cursor->U16());
    MINO_ASSIGN_OR_RETURN(region.security_domain.value, cursor->U64());
    return region;
}

void EncodeTopic(const TopicBinding& topic, Writer* writer) {
    writer->U32(topic.topic_id.value);
    writer->String(topic.name);
    writer->U64(topic.config_version);
    writer->U64(topic.region_version);
    writer->U64(topic.channel_version);
    writer->U64(topic.acl_version);
    writer->U64(topic.schema.short_id());
    writer->Raw(topic.schema.canonical_digest());
    writer->U32(topic.schema.schema_version());
    writer->U32(topic.schema.layout_version());
    writer->U32(static_cast<uint32_t>(topic.acl.entries.size()));
    for (const registry::TopicAclEntry& entry : topic.acl.entries) {
        writer->U64(entry.node_id.value);
        writer->U64(entry.security_domain_id.value);
        writer->U32(entry.permissions);
    }
}

Result<TopicBinding> DecodeTopic(Cursor* cursor) {
    TopicBinding topic;
    MINO_ASSIGN_OR_RETURN(topic.topic_id.value, cursor->U32());
    MINO_ASSIGN_OR_RETURN(topic.name, cursor->String());
    MINO_ASSIGN_OR_RETURN(topic.config_version, cursor->U64());
    MINO_ASSIGN_OR_RETURN(topic.region_version, cursor->U64());
    MINO_ASSIGN_OR_RETURN(topic.channel_version, cursor->U64());
    MINO_ASSIGN_OR_RETURN(topic.acl_version, cursor->U64());
    uint64_t short_id = 0;
    schema::CanonicalDigest digest{};
    uint32_t schema_version = 0;
    uint32_t layout_version = 0;
    MINO_ASSIGN_OR_RETURN(short_id, cursor->U64());
    MINO_ASSIGN_OR_RETURN(digest, cursor->Digest());
    MINO_ASSIGN_OR_RETURN(schema_version, cursor->U32());
    MINO_ASSIGN_OR_RETURN(layout_version, cursor->U32());
    topic.schema = schema::SchemaIdentity(short_id, digest, schema_version,
                                          layout_version);
    uint32_t acl_count = 0;
    MINO_ASSIGN_OR_RETURN(acl_count, cursor->U32());
    if (acl_count == 0 || acl_count > registry::kMaxTopicAclEntries) {
        return Corrupt("upgrade Topic ACL count is invalid");
    }
    topic.acl.entries.reserve(acl_count);
    for (uint32_t i = 0; i < acl_count; ++i) {
        registry::TopicAclEntry entry;
        MINO_ASSIGN_OR_RETURN(entry.node_id.value, cursor->U64());
        MINO_ASSIGN_OR_RETURN(entry.security_domain_id.value, cursor->U64());
        MINO_ASSIGN_OR_RETURN(entry.permissions, cursor->U32());
        topic.acl.entries.push_back(entry);
    }
    return topic;
}

Status RunHook(const UpgradeManifestOptions& options,
               UpgradePersistenceFaultPoint point) noexcept {
    return options.fault_hook == nullptr
               ? Status::Ok()
               : options.fault_hook(point, options.fault_hook_context);
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

class ScopedFd {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) static_cast<void>(::close(fd_));
    }
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    int get() const noexcept { return fd_; }
    int release() noexcept { return std::exchange(fd_, -1); }

private:
    int fd_;
};

Result<int> AcquireOwnerLock(const std::filesystem::path& path) {
    const std::filesystem::path lock_path = path.string() + ".lock";
    ScopedFd fd(::open(lock_path.c_str(), OpenFlags(O_RDWR | O_CREAT), 0600));
    if (fd.get() < 0) return IoError("cannot open upgrade owner lock", lock_path);
    struct stat state {};
    if (::fstat(fd.get(), &state) != 0 || !S_ISREG(state.st_mode) ||
        state.st_nlink != 1 || state.st_uid != ::geteuid()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "upgrade owner lock must be a single-link regular file owned by this uid");
    }
    if (::flock(fd.get(), LOCK_EX | LOCK_NB) != 0) {
        return Status::Error(StatusCode::kWouldBlock,
                             "another rolling-upgrade owner holds the manifest lock");
    }
    return fd.release();
}

Status DataSync(int fd, const std::filesystem::path& path) {
    int result = 0;
    do {
#if defined(__APPLE__)
        result = ::fsync(fd);
#else
        result = ::fdatasync(fd);
#endif
    } while (result != 0 && errno == EINTR);
    return result == 0 ? Status::Ok() : IoError("cannot sync upgrade manifest", path);
}

Status SyncDirectory(const std::filesystem::path& parent) {
    ScopedFd fd(::open(parent.c_str(), OpenFlags(O_RDONLY)
#ifdef O_DIRECTORY
                                     | O_DIRECTORY
#endif
                                     ));
    if (fd.get() < 0) return IoError("cannot open upgrade directory", parent);
    int result = 0;
    do {
        result = ::fsync(fd.get());
    } while (result != 0 && errno == EINTR);
    return result == 0 ? Status::Ok()
                       : IoError("cannot sync upgrade directory", parent);
}

Result<std::vector<std::byte>> ReadFile(const std::filesystem::path& path,
                                        size_t maximum) {
    ScopedFd fd(::open(path.c_str(), OpenFlags(O_RDONLY)));
    if (fd.get() < 0) return IoError("cannot open upgrade manifest", path);
    struct stat state {};
    if (::fstat(fd.get(), &state) != 0 || !S_ISREG(state.st_mode) ||
        state.st_nlink != 1 || state.st_uid != ::geteuid() ||
        (state.st_mode & (S_IWGRP | S_IWOTH)) != 0 || state.st_size <= 0 ||
        static_cast<uint64_t>(state.st_size) > maximum) {
        return Status::Error(
            StatusCode::kPermissionDenied,
            "upgrade manifest must be bounded, single-link, owner-controlled, and not group/world writable");
    }
    std::vector<std::byte> bytes(static_cast<size_t>(state.st_size));
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t count = ::pread(fd.get(), bytes.data() + offset,
                                      bytes.size() - offset,
                                      static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return IoError("cannot read upgrade manifest", path);
        offset += static_cast<size_t>(count);
    }
    return bytes;
}

Status AtomicWrite(const std::filesystem::path& path,
                   std::span<const std::byte> bytes,
                   const UpgradeManifestOptions& options,
                   bool* renamed) noexcept {
    *renamed = false;
    const std::filesystem::path parent =
        path.parent_path().empty() ? std::filesystem::path(".")
                                   : path.parent_path();
    const std::filesystem::path temporary =
        parent / (path.filename().string() + ".tmp." +
                  std::to_string(static_cast<uint64_t>(::getpid())));
    ScopedFd fd(::open(temporary.c_str(),
                       OpenFlags(O_WRONLY | O_CREAT | O_EXCL), 0600));
    if (fd.get() < 0) return IoError("cannot create upgrade temporary", temporary);
    bool remove_temporary = true;
    auto cleanup = [&]() {
        if (remove_temporary) static_cast<void>(::unlink(temporary.c_str()));
    };
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t count =
            ::write(fd.get(), bytes.data() + offset, bytes.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) {
            cleanup();
            return IoError("cannot write upgrade temporary", temporary);
        }
        offset += static_cast<size_t>(count);
    }
    Status status = RunHook(options,
                            UpgradePersistenceFaultPoint::kAfterTemporaryWrite);
    if (!status.ok()) {
        cleanup();
        return status;
    }
    status = DataSync(fd.get(), temporary);
    if (!status.ok()) {
        cleanup();
        return status;
    }
    status = RunHook(options,
                     UpgradePersistenceFaultPoint::kAfterTemporaryDataSync);
    if (!status.ok()) {
        cleanup();
        return status;
    }
    if (::close(fd.release()) != 0) {
        cleanup();
        return IoError("cannot close upgrade temporary", temporary);
    }
    if (::rename(temporary.c_str(), path.c_str()) != 0) {
        cleanup();
        return IoError("cannot atomically rename upgrade manifest", path);
    }
    remove_temporary = false;
    *renamed = true;
    status = RunHook(options, UpgradePersistenceFaultPoint::kAfterAtomicRename);
    if (!status.ok()) return status;
    MINO_RETURN_IF_ERROR(SyncDirectory(parent));
    return RunHook(options,
                   UpgradePersistenceFaultPoint::kAfterParentDirectorySync);
}

}  // namespace

Result<std::vector<std::byte>> EncodeUpgradeSnapshot(
    const UpgradeSnapshot& snapshot,
    const UpgradeManifestOptions& options) noexcept {
    try {
        MINO_RETURN_IF_ERROR(ValidateSnapshot(snapshot, options));
        Writer writer;
        writer.Raw(kMagic);
        writer.U16(kUpgradeFormatVersion);
        writer.U8(static_cast<uint8_t>(snapshot.phase));
        writer.U8(0);
        writer.U64(snapshot.generation);
        writer.U64(snapshot.created_at_ns);
        writer.U64(snapshot.updated_at_ns);
        writer.String(snapshot.plan.operation_id);
        writer.String(snapshot.plan.commit_token);
        EncodeRegion(snapshot.plan.source_region, &writer);
        EncodeRegion(snapshot.plan.target_region, &writer);
        writer.U32(static_cast<uint32_t>(snapshot.plan.topics.size()));
        for (const TopicUpgrade& topic : snapshot.plan.topics) {
            EncodeTopic(topic.source, &writer);
            EncodeTopic(topic.target, &writer);
        }
        writer.U64(snapshot.plan.required_shm_bytes);
        writer.U32(snapshot.plan.required_publisher_slots);
        writer.U32(snapshot.plan.required_subscriber_slots);
        writer.U64(snapshot.plan.minimum_observation_samples);
        writer.U32(static_cast<uint32_t>(snapshot.journal.size()));
        for (const UpgradeJournalEntry& entry : snapshot.journal) {
            writer.U64(entry.sequence);
            writer.U8(static_cast<uint8_t>(entry.phase));
            writer.U64(entry.timestamp_ns);
            writer.String(entry.detail);
        }
        writer.String(snapshot.terminal_reason);
        std::vector<std::byte> bytes = std::move(writer).Finish();
        const uint32_t crc = bridge::Crc32c(bytes);
        for (size_t i = 0; i < 4; ++i) {
            bytes.push_back(static_cast<std::byte>(crc >> (i * 8)));
        }
        if (bytes.size() > options.maximum_file_bytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "upgrade manifest exceeds configured byte bound");
        }
        return bytes;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<UpgradeSnapshot> DecodeUpgradeSnapshot(
    std::span<const std::byte> encoded,
    const UpgradeManifestOptions& options) noexcept {
    try {
        MINO_RETURN_IF_ERROR(ValidateOptions(options));
        if (encoded.size() < kMagic.size() + kCrcBytes ||
            encoded.size() > options.maximum_file_bytes ||
            !std::equal(kMagic.begin(), kMagic.end(), encoded.begin())) {
            return Corrupt("upgrade manifest magic or size is invalid");
        }
        uint32_t stored_crc = 0;
        for (size_t i = 0; i < 4; ++i) {
            stored_crc |= static_cast<uint32_t>(
                              static_cast<uint8_t>(encoded[encoded.size() - 4 + i]))
                          << (i * 8);
        }
        const std::span<const std::byte> payload = encoded.first(encoded.size() - 4);
        if (bridge::Crc32c(payload) != stored_crc) {
            return Corrupt("upgrade manifest CRC32C mismatch");
        }
        Cursor cursor(payload.subspan(kMagic.size()));
        uint16_t format = 0;
        uint8_t phase = 0;
        uint8_t reserved = 0;
        UpgradeSnapshot snapshot;
        MINO_ASSIGN_OR_RETURN(format, cursor.U16());
        MINO_ASSIGN_OR_RETURN(phase, cursor.U8());
        MINO_ASSIGN_OR_RETURN(reserved, cursor.U8());
        if (format != kUpgradeFormatVersion || reserved != 0) {
            return Corrupt("upgrade manifest format is unsupported");
        }
        snapshot.phase = static_cast<UpgradePhase>(phase);
        MINO_ASSIGN_OR_RETURN(snapshot.generation, cursor.U64());
        MINO_ASSIGN_OR_RETURN(snapshot.created_at_ns, cursor.U64());
        MINO_ASSIGN_OR_RETURN(snapshot.updated_at_ns, cursor.U64());
        MINO_ASSIGN_OR_RETURN(snapshot.plan.operation_id, cursor.String());
        MINO_ASSIGN_OR_RETURN(snapshot.plan.commit_token, cursor.String());
        MINO_ASSIGN_OR_RETURN(snapshot.plan.source_region, DecodeRegion(&cursor));
        MINO_ASSIGN_OR_RETURN(snapshot.plan.target_region, DecodeRegion(&cursor));
        uint32_t topic_count = 0;
        MINO_ASSIGN_OR_RETURN(topic_count, cursor.U32());
        if (topic_count == 0 || topic_count > kMaximumUpgradeTopics) {
            return Corrupt("upgrade Topic count is invalid");
        }
        snapshot.plan.topics.reserve(topic_count);
        for (uint32_t i = 0; i < topic_count; ++i) {
            TopicUpgrade topic;
            MINO_ASSIGN_OR_RETURN(topic.source, DecodeTopic(&cursor));
            MINO_ASSIGN_OR_RETURN(topic.target, DecodeTopic(&cursor));
            snapshot.plan.topics.push_back(std::move(topic));
        }
        MINO_ASSIGN_OR_RETURN(snapshot.plan.required_shm_bytes, cursor.U64());
        MINO_ASSIGN_OR_RETURN(snapshot.plan.required_publisher_slots, cursor.U32());
        MINO_ASSIGN_OR_RETURN(snapshot.plan.required_subscriber_slots, cursor.U32());
        MINO_ASSIGN_OR_RETURN(snapshot.plan.minimum_observation_samples,
                              cursor.U64());
        uint32_t journal_count = 0;
        MINO_ASSIGN_OR_RETURN(journal_count, cursor.U32());
        if (journal_count == 0 ||
            journal_count > options.maximum_journal_entries) {
            return Corrupt("upgrade journal count is invalid");
        }
        snapshot.journal.reserve(journal_count);
        for (uint32_t i = 0; i < journal_count; ++i) {
            UpgradeJournalEntry entry;
            uint8_t entry_phase = 0;
            MINO_ASSIGN_OR_RETURN(entry.sequence, cursor.U64());
            MINO_ASSIGN_OR_RETURN(entry_phase, cursor.U8());
            entry.phase = static_cast<UpgradePhase>(entry_phase);
            MINO_ASSIGN_OR_RETURN(entry.timestamp_ns, cursor.U64());
            MINO_ASSIGN_OR_RETURN(entry.detail, cursor.String());
            snapshot.journal.push_back(std::move(entry));
        }
        MINO_ASSIGN_OR_RETURN(snapshot.terminal_reason, cursor.String());
        if (!cursor.done()) return Corrupt("upgrade manifest has trailing fields");
        MINO_RETURN_IF_ERROR(ValidateSnapshot(snapshot, options));
        return snapshot;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

UpgradeManifestStore::UpgradeManifestStore(
    std::filesystem::path path, UpgradeManifestOptions options, int lock_fd,
    UpgradeSnapshot snapshot) noexcept
    : path_(std::move(path)), options_(options), lock_fd_(lock_fd),
      snapshot_(std::move(snapshot)) {}

UpgradeManifestStore::~UpgradeManifestStore() {
    if (lock_fd_ >= 0) {
        static_cast<void>(::flock(lock_fd_, LOCK_UN));
        static_cast<void>(::close(lock_fd_));
    }
}

Result<std::unique_ptr<UpgradeManifestStore>> UpgradeManifestStore::Create(
    const std::filesystem::path& manifest_path, UpgradePlan plan,
    uint64_t now_ns, const UpgradeManifestOptions& options) noexcept {
    try {
        MINO_RETURN_IF_ERROR(ValidateOptions(options));
        MINO_RETURN_IF_ERROR(ValidateUpgradePlan(plan));
        if (manifest_path.empty() || manifest_path.filename().empty() || now_ns == 0) {
            return Invalid("upgrade manifest path or creation time is invalid");
        }
        std::error_code error;
        if (std::filesystem::exists(manifest_path, error)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "upgrade manifest already exists");
        }
        if (error) return Status::Error(StatusCode::kUnavailable, error.message());
        MINO_ASSIGN_OR_RETURN(const int lock_fd, AcquireOwnerLock(manifest_path));
        UpgradeSnapshot snapshot{
            .generation = 1,
            .created_at_ns = now_ns,
            .updated_at_ns = now_ns,
            .phase = UpgradePhase::kPrepare,
            .plan = std::move(plan),
            .journal = {{.sequence = 1,
                         .phase = UpgradePhase::kPrepare,
                         .timestamp_ns = now_ns,
                         .detail = "upgrade plan durably created"}},
            .terminal_reason = {},
        };
        auto store = std::unique_ptr<UpgradeManifestStore>(new UpgradeManifestStore(
            manifest_path, options, lock_fd, UpgradeSnapshot{}));
        const Status status = store->Persist(std::move(snapshot));
        if (!status.ok()) return status;
        return store;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<std::unique_ptr<UpgradeManifestStore>> UpgradeManifestStore::Open(
    const std::filesystem::path& manifest_path,
    const UpgradeManifestOptions& options) noexcept {
    try {
        MINO_RETURN_IF_ERROR(ValidateOptions(options));
        MINO_ASSIGN_OR_RETURN(const int lock_fd, AcquireOwnerLock(manifest_path));
        auto bytes = ReadFile(manifest_path, options.maximum_file_bytes);
        if (!bytes.ok()) {
            static_cast<void>(::flock(lock_fd, LOCK_UN));
            static_cast<void>(::close(lock_fd));
            return bytes.status();
        }
        auto snapshot = DecodeUpgradeSnapshot(*bytes, options);
        if (!snapshot.ok()) {
            static_cast<void>(::flock(lock_fd, LOCK_UN));
            static_cast<void>(::close(lock_fd));
            return snapshot.status();
        }
        return std::unique_ptr<UpgradeManifestStore>(new UpgradeManifestStore(
            manifest_path, options, lock_fd, std::move(*snapshot)));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status UpgradeManifestStore::Persist(UpgradeSnapshot next) noexcept {
    if (poisoned_) {
        return Status::Error(StatusCode::kUnavailable,
                             "upgrade store is poisoned; reopen and resume");
    }
    auto encoded = EncodeUpgradeSnapshot(next, options_);
    if (!encoded.ok()) return encoded.status();
    bool renamed = false;
    const Status status = AtomicWrite(path_, *encoded, options_, &renamed);
    if (!status.ok()) {
        if (renamed) poisoned_ = true;
        return status;
    }
    snapshot_ = std::move(next);
    return Status::Ok();
}

Status UpgradeManifestStore::Advance(UpgradePhase next, uint64_t now_ns,
                                     std::string_view detail) noexcept {
    try {
        if (now_ns < snapshot_.updated_at_ns ||
            detail.size() > kMaximumUpgradeStringBytes ||
            !LegalTransition(snapshot_.phase, next) ||
            snapshot_.journal.size() >= options_.maximum_journal_entries ||
            snapshot_.generation == std::numeric_limits<uint64_t>::max()) {
            return Invalid("upgrade phase transition, time, or journal bound is invalid");
        }
        UpgradeSnapshot replacement = snapshot_;
        ++replacement.generation;
        replacement.updated_at_ns = now_ns;
        replacement.phase = next;
        replacement.journal.push_back(UpgradeJournalEntry{
            .sequence = replacement.generation,
            .phase = next,
            .timestamp_ns = now_ns,
            .detail = std::string(detail),
        });
        return Persist(std::move(replacement));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status UpgradeManifestStore::Fail(uint64_t now_ns,
                                  std::string_view reason) noexcept {
    try {
        if (reason.empty() || reason.size() > kMaximumUpgradeStringBytes) {
            return Invalid("upgrade failure reason is empty or out of bounds");
        }
        UpgradeSnapshot replacement = snapshot_;
        if (!LegalTransition(replacement.phase, UpgradePhase::kFail) ||
            replacement.journal.size() >= options_.maximum_journal_entries ||
            replacement.generation == std::numeric_limits<uint64_t>::max() ||
            now_ns < replacement.updated_at_ns) {
            return Invalid("upgrade cannot enter fail from the current phase");
        }
        ++replacement.generation;
        replacement.updated_at_ns = now_ns;
        replacement.phase = UpgradePhase::kFail;
        replacement.terminal_reason = std::string(reason);
        replacement.journal.push_back(UpgradeJournalEntry{
            .sequence = replacement.generation,
            .phase = UpgradePhase::kFail,
            .timestamp_ns = now_ns,
            .detail = replacement.terminal_reason,
        });
        return Persist(std::move(replacement));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::upgrade
