// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recorder_subscriber.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <string>
#include <string_view>
#include <utility>

namespace mino::storage {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Exhausted(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

Status Unavailable(std::string_view message) {
    return Status::Error(StatusCode::kUnavailable, message);
}

bool IsValidPolicy(BufferFullPolicy policy) noexcept {
    switch (policy) {
        case BufferFullPolicy::kBlock:
        case BufferFullPolicy::kDropNewest:
        case BufferFullPolicy::kDropOldest:
        case BufferFullPolicy::kFailRecording:
            return true;
    }
    return false;
}

bool HasNonzeroDigest(const schema::CanonicalDigest& digest) noexcept {
    return std::any_of(digest.begin(), digest.end(),
                       [](std::byte value) { return value != std::byte{0}; });
}

uint64_t DigestShortId(const schema::CanonicalDigest& digest) noexcept {
    uint64_t short_id = 0;
    for (size_t i = 0; i < sizeof(short_id); ++i) {
        short_id |= static_cast<uint64_t>(static_cast<uint8_t>(digest[i]))
                    << (i * 8u);
    }
    return short_id;
}

const std::array<uint32_t, 256>& Crc32cTable() noexcept {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> values{};
        for (uint32_t i = 0; i < values.size(); ++i) {
            uint32_t value = i;
            for (int bit = 0; bit < 8; ++bit) {
                value = (value >> 1) ^
                        ((value & 1u) != 0 ? 0x82f63b78u : 0u);
            }
            values[i] = value;
        }
        return values;
    }();
    return table;
}

constexpr std::array<std::byte, 8> kPendingMagic = {
    std::byte{'M'}, std::byte{'I'}, std::byte{'N'}, std::byte{'O'},
    std::byte{'R'}, std::byte{'P'}, std::byte{'N'}, std::byte{'D'},
};
constexpr uint16_t kPendingVersion = 2;
constexpr size_t kPendingCrcOffset = 136;
constexpr size_t kPendingHeaderSize = 140;
constexpr size_t kMaximumPendingJournalBytes = 64u * 1024u * 1024u;

void AppendU8(std::vector<std::byte>* output, uint8_t value) {
    output->push_back(static_cast<std::byte>(value));
}

void AppendU16(std::vector<std::byte>* output, uint16_t value) {
    AppendU8(output, static_cast<uint8_t>(value));
    AppendU8(output, static_cast<uint8_t>(value >> 8));
}

void AppendU32(std::vector<std::byte>* output, uint32_t value) {
    for (int shift = 0; shift < 32; shift += 8) {
        AppendU8(output, static_cast<uint8_t>(value >> shift));
    }
}

void AppendU64(std::vector<std::byte>* output, uint64_t value) {
    for (int shift = 0; shift < 64; shift += 8) {
        AppendU8(output, static_cast<uint8_t>(value >> shift));
    }
}

uint16_t ReadU16(std::span<const std::byte> input, size_t offset) {
    return static_cast<uint16_t>(static_cast<uint8_t>(input[offset])) |
           static_cast<uint16_t>(static_cast<uint8_t>(input[offset + 1])) << 8;
}

uint32_t ReadU32(std::span<const std::byte> input, size_t offset) {
    uint32_t value = 0;
    for (size_t index = 0; index < 4; ++index) {
        value |= static_cast<uint32_t>(static_cast<uint8_t>(input[offset + index]))
                 << (index * 8u);
    }
    return value;
}

uint64_t ReadU64(std::span<const std::byte> input, size_t offset) {
    uint64_t value = 0;
    for (size_t index = 0; index < 8; ++index) {
        value |= static_cast<uint64_t>(static_cast<uint8_t>(input[offset + index]))
                 << (index * 8u);
    }
    return value;
}

Result<std::vector<std::byte>> EncodePending(
    const RecorderPersistedPendingRecord& record) {
    if (record.schema_ref == 0 || record.metadata.topic_id.value == 0 ||
        record.metadata.source.node_id == 0 ||
        record.metadata.source.publisher_id == 0 ||
        record.metadata.source.publisher_epoch == 0 ||
        record.metadata.payload_size != record.payload.size() ||
        record.metadata.payload_crc != RecorderPayloadCrc32c(record.payload) ||
        record.payload.size() > kMaximumPendingJournalBytes - kPendingHeaderSize) {
        return Invalid("pending recorder journal record is inconsistent");
    }
    std::vector<std::byte> output;
    output.reserve(kPendingHeaderSize + record.payload.size());
    output.insert(output.end(), kPendingMagic.begin(), kPendingMagic.end());
    AppendU16(&output, kPendingVersion);
    AppendU16(&output, 0);
    AppendU64(&output, record.metadata.schema.short_id);
    output.insert(output.end(), record.metadata.schema.canonical_digest.begin(),
                  record.metadata.schema.canonical_digest.end());
    AppendU32(&output, record.metadata.schema.schema_version);
    AppendU32(&output, record.metadata.schema.layout_version);
    AppendU32(&output, record.metadata.topic_id.value);
    AppendU32(&output, record.schema_ref);
    AppendU64(&output, record.metadata.source.node_id);
    AppendU64(&output, record.metadata.source.publisher_id);
    AppendU64(&output, record.metadata.source.publisher_epoch);
    AppendU64(&output, record.metadata.source.source_sequence);
    AppendU64(&output, record.metadata.source.observed_timestamp_ns);
    AppendU64(&output, record.metadata.ingestion_timestamp_ns);
    AppendU32(&output, record.metadata.payload_size);
    AppendU32(&output, record.metadata.payload_crc);
    AppendU64(&output, record.user_tag);
    AppendU8(&output, record.ack_attempted ? 1 : 0);
    AppendU16(&output, static_cast<uint16_t>(record.ack_code));
    AppendU8(&output, 0);
    AppendU32(&output, 0);
    if (output.size() != kPendingHeaderSize) {
        return Status::Error(StatusCode::kInternal,
                             "pending recorder journal header size changed");
    }
    output.insert(output.end(), record.payload.begin(), record.payload.end());
    const uint32_t crc = RecorderPayloadCrc32c(output);
    for (size_t index = 0; index < 4; ++index) {
        output[kPendingCrcOffset + index] =
            static_cast<std::byte>(crc >> (index * 8u));
    }
    return output;
}

Result<RecorderPersistedPendingRecord> DecodePending(
    std::vector<std::byte> encoded) {
    if (encoded.size() < kPendingHeaderSize ||
        encoded.size() > kMaximumPendingJournalBytes ||
        !std::equal(kPendingMagic.begin(), kPendingMagic.end(), encoded.begin()) ||
        ReadU16(encoded, 8) != kPendingVersion || ReadU16(encoded, 10) != 0) {
        return Status::Error(StatusCode::kCorruption,
                             "pending recorder journal header is invalid");
    }
    const uint32_t stored_crc = ReadU32(encoded, kPendingCrcOffset);
    for (size_t index = 0; index < 4; ++index) {
        encoded[kPendingCrcOffset + index] = std::byte{0};
    }
    if (stored_crc != RecorderPayloadCrc32c(encoded)) {
        return Status::Error(StatusCode::kCorruption,
                             "pending recorder journal CRC mismatch");
    }
    RecorderPersistedPendingRecord record;
    record.metadata.schema.short_id = ReadU64(encoded, 12);
    std::copy_n(encoded.begin() + 20,
                record.metadata.schema.canonical_digest.size(),
                record.metadata.schema.canonical_digest.begin());
    record.metadata.schema.schema_version = ReadU32(encoded, 52);
    record.metadata.schema.layout_version = ReadU32(encoded, 56);
    record.metadata.topic_id = TopicId{ReadU32(encoded, 60)};
    record.schema_ref = ReadU32(encoded, 64);
    record.metadata.source.node_id = ReadU64(encoded, 68);
    record.metadata.source.publisher_id = ReadU64(encoded, 76);
    record.metadata.source.publisher_epoch = ReadU64(encoded, 84);
    record.metadata.source.source_sequence = ReadU64(encoded, 92);
    record.metadata.source.observed_timestamp_ns = ReadU64(encoded, 100);
    record.metadata.ingestion_timestamp_ns = ReadU64(encoded, 108);
    record.metadata.payload_size = ReadU32(encoded, 116);
    record.metadata.payload_crc = ReadU32(encoded, 120);
    record.user_tag = ReadU64(encoded, 124);
    const uint8_t ack_attempted = static_cast<uint8_t>(encoded[132]);
    const uint16_t ack_code = ReadU16(encoded, 133);
    if (ack_attempted > 1 || static_cast<uint8_t>(encoded[135]) != 0 ||
        ack_code > static_cast<uint16_t>(StatusCode::kInternal) ||
        record.schema_ref == 0 ||
        record.metadata.payload_size != encoded.size() - kPendingHeaderSize) {
        return Status::Error(StatusCode::kCorruption,
                             "pending recorder journal fields are invalid");
    }
    record.ack_attempted = ack_attempted != 0;
    record.ack_code = static_cast<StatusCode>(ack_code);
    record.payload.assign(encoded.begin() + kPendingHeaderSize, encoded.end());
    if (record.metadata.payload_crc != RecorderPayloadCrc32c(record.payload)) {
        return Status::Error(StatusCode::kCorruption,
                             "pending recorder journal payload CRC mismatch");
    }
    return record;
}

int JournalOpenFlags(int flags) noexcept {
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    return flags;
}

Status JournalIoError(std::string_view operation,
                      const std::filesystem::path& path) {
    return Status::Error(StatusCode::kUnavailable,
                         std::string(operation) + " '" + path.string() +
                             "': " + std::strerror(errno));
}

Status VerifyJournalPath(const std::filesystem::path& path) {
    if (path.empty() || path.filename().empty()) {
        return Invalid("pending recorder journal path is empty");
    }
    std::error_code error;
    const std::filesystem::path absolute =
        std::filesystem::absolute(path, error).lexically_normal();
    if (error) return Invalid("pending journal path cannot be made absolute");
    std::filesystem::path current = absolute.root_path();
    for (const std::filesystem::path& component :
         absolute.parent_path().relative_path()) {
        current /= component;
        struct stat info {};
        if (::lstat(current.c_str(), &info) != 0) {
            return JournalIoError("cannot inspect pending journal path", current);
        }
        if (S_ISLNK(info.st_mode) || !S_ISDIR(info.st_mode)) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "pending journal parent is not a real directory");
        }
    }
    struct stat existing {};
    if (::lstat(absolute.c_str(), &existing) == 0) {
        if (S_ISLNK(existing.st_mode) || !S_ISREG(existing.st_mode)) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "pending journal is not a regular file");
        }
    } else if (errno != ENOENT) {
        return JournalIoError("cannot inspect pending journal", absolute);
    }
    return Status::Ok();
}

Status SyncJournalDirectory(int directory_fd,
                            const std::filesystem::path& directory) {
    int result = 0;
    do {
        result = ::fsync(directory_fd);
    } while (result != 0 && errno == EINTR);
    if (result != 0) {
        return JournalIoError("cannot sync pending journal directory", directory);
    }
    return Status::Ok();
}

Status SetJournalCloseOnExec(int fd) {
#ifdef O_CLOEXEC
    static_cast<void>(fd);
    return Status::Ok();
#else
    const int flags = ::fcntl(fd, F_GETFD);
    if (flags < 0 || ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot set pending journal close-on-exec flag");
    }
    return Status::Ok();
#endif
}

std::string JournalTemporaryName() {
    static std::atomic<uint64_t> sequence{0};
    return ".pending.tmp." + std::to_string(static_cast<uint64_t>(::getpid())) +
           "." + std::to_string(sequence.fetch_add(1));
}

Status SyncJournalFile(int fd, const std::filesystem::path& path) {
    int result = 0;
    do {
#if defined(__APPLE__)
        result = ::fsync(fd);
#else
        result = ::fdatasync(fd);
#endif
    } while (result != 0 && errno == EINTR);
    return result == 0 ? Status::Ok()
                       : JournalIoError("cannot sync pending journal", path);
}

}  // namespace



Status ValidateRecorderSubscriberOptions(
    const RecorderSubscriberOptions& options) noexcept {
    constexpr auto kMaximumPendingWait = std::chrono::minutes(1);
    if (options.topic_id.value == 0) {
        return Invalid("recorder topic ID must be non-zero");
    }
    if (options.schema.short_id == 0 ||
        options.schema.schema_version == 0 ||
        options.schema.layout_version == 0 ||
        !HasNonzeroDigest(options.schema.canonical_digest)) {
        return Invalid("recorder schema identity is incomplete");
    }
    if (options.schema.short_id !=
        DigestShortId(options.schema.canonical_digest)) {
        return Invalid("recorder schema short ID does not match digest");
    }
    if (options.pending_store != nullptr &&
        (options.schema_ref == 0 || !options.source_identity.has_value() ||
         options.source_identity->node_id == 0 ||
         options.source_identity->publisher_id == 0 ||
         options.source_identity->publisher_epoch == 0)) {
        return Invalid(
            "durable pending journal requires schema ref and source identity");
    }
    if (!IsValidPolicy(options.full_policy)) {
        return Invalid("recorder buffer full policy is invalid");
    }
    if (options.max_canonical_payload_bytes == 0 ||
        options.max_canonical_payload_bytes >
            static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return Invalid("recorder canonical payload bound is invalid");
    }
    if (options.pending_retry_timeout < std::chrono::nanoseconds::zero() ||
        options.pending_retry_timeout > kMaximumPendingWait) {
        return Invalid("recorder pending retry timeout is out of bounds");
    }
    return Status::Ok();
}

uint32_t RecorderPayloadCrc32c(
    std::span<const std::byte> payload) noexcept {
    uint32_t state = 0xffffffffu;
    const auto& table = Crc32cTable();
    for (std::byte byte : payload) {
        const uint8_t value = static_cast<uint8_t>(byte);
        state = table[(state ^ value) & 0xffu] ^ (state >> 8);
    }
    return state ^ 0xffffffffu;
}

uint64_t SystemRecorderClock::NowNs() noexcept {
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    if (elapsed.count() <= 0) return 0;
    return static_cast<uint64_t>(elapsed.count());
}

Result<std::unique_ptr<FileRecorderPendingStore>>
FileRecorderPendingStore::Open(std::filesystem::path journal_path) noexcept {
    try {
        if (journal_path.empty() || journal_path.filename().empty()) {
            return Invalid("pending recorder journal path is empty");
        }
        std::error_code error;
        std::filesystem::path parent = journal_path.parent_path();
        if (parent.empty()) parent = ".";
        std::filesystem::create_directories(parent, error);
        if (error) {
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot create pending journal directory: " +
                                     error.message());
        }
        journal_path = std::filesystem::absolute(journal_path, error)
                           .lexically_normal();
        if (error) return Invalid("pending journal path cannot be made absolute");
        MINO_RETURN_IF_ERROR(VerifyJournalPath(journal_path));

        int directory_flags = O_RDONLY;
#ifdef O_DIRECTORY
        directory_flags |= O_DIRECTORY;
#endif
        const int directory_fd = ::open(
            journal_path.parent_path().c_str(),
            JournalOpenFlags(directory_flags));
        if (directory_fd < 0) {
            return JournalIoError("cannot open pending journal directory",
                                  journal_path.parent_path());
        }
        struct stat directory_info {};
        if (::fstat(directory_fd, &directory_info) != 0 ||
            !S_ISDIR(directory_info.st_mode)) {
            const Status failure = JournalIoError(
                "cannot stat pending journal directory",
                journal_path.parent_path());
            static_cast<void>(::close(directory_fd));
            return failure;
        }

        const std::string filename = journal_path.filename().string();
        const std::string lock_name = filename + ".owner.lock";
        const int owner_lock_fd = ::openat(
            directory_fd, lock_name.c_str(),
            JournalOpenFlags(O_RDWR | O_CREAT), 0600);
        if (owner_lock_fd < 0) {
            const Status failure = JournalIoError(
                "cannot open pending journal owner lock", journal_path);
            static_cast<void>(::close(directory_fd));
            return failure;
        }
        struct stat lock_info {};
        if (::fstat(owner_lock_fd, &lock_info) != 0 ||
            !S_ISREG(lock_info.st_mode)) {
            static_cast<void>(::close(owner_lock_fd));
            static_cast<void>(::close(directory_fd));
            return Status::Error(StatusCode::kPermissionDenied,
                                 "pending journal owner lock is not regular");
        }
        const Status close_on_exec = SetJournalCloseOnExec(owner_lock_fd);
        if (!close_on_exec.ok()) {
            static_cast<void>(::close(owner_lock_fd));
            static_cast<void>(::close(directory_fd));
            return close_on_exec;
        }
        if (::flock(owner_lock_fd, LOCK_EX | LOCK_NB) != 0) {
            const int lock_error = errno;
            static_cast<void>(::close(owner_lock_fd));
            static_cast<void>(::close(directory_fd));
            if (lock_error == EWOULDBLOCK || lock_error == EAGAIN) {
                return Status::Error(
                    StatusCode::kUnavailable,
                    "pending journal path already has an installed source");
            }
            errno = lock_error;
            return JournalIoError("cannot lock pending journal source",
                                  journal_path);
        }
        const Status directory_synced =
            SyncJournalDirectory(directory_fd, journal_path.parent_path());
        if (!directory_synced.ok()) {
            static_cast<void>(::close(owner_lock_fd));
            static_cast<void>(::close(directory_fd));
            return directory_synced;
        }
        return std::unique_ptr<FileRecorderPendingStore>(
            new FileRecorderPendingStore(std::move(journal_path), filename,
                                         directory_fd, owner_lock_fd));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate pending recorder store");
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

FileRecorderPendingStore::~FileRecorderPendingStore() {
    if (owner_lock_fd_ >= 0) static_cast<void>(::close(owner_lock_fd_));
    if (directory_fd_ >= 0) static_cast<void>(::close(directory_fd_));
}

Status FileRecorderPendingStore::Save(
    const RecorderPersistedPendingRecord& record) noexcept {
    try {
        Result<std::vector<std::byte>> encoded = EncodePending(record);
        if (!encoded.ok()) return encoded.status();

        std::string temporary;
        int fd = -1;
        for (size_t attempt = 0; attempt < 16 && fd < 0; ++attempt) {
            temporary = JournalTemporaryName();
            fd = ::openat(directory_fd_, temporary.c_str(),
                          JournalOpenFlags(O_WRONLY | O_CREAT | O_EXCL), 0600);
            if (fd < 0 && errno != EEXIST) {
                return JournalIoError("cannot create pending journal temp",
                                      journal_path_);
            }
        }
        if (fd < 0) {
            return Unavailable("cannot allocate a unique pending journal temp");
        }
        Status status = SetJournalCloseOnExec(fd);
        size_t completed = 0;
        while (status.ok() && completed < encoded->size()) {
            const ssize_t count = ::write(fd, encoded->data() + completed,
                                          encoded->size() - completed);
            if (count < 0) {
                if (errno == EINTR) continue;
                status = JournalIoError("cannot write pending journal",
                                        journal_path_);
                break;
            }
            if (count == 0) {
                status = Unavailable("zero-byte pending journal write");
                break;
            }
            completed += static_cast<size_t>(count);
        }
        if (status.ok()) status = SyncJournalFile(fd, journal_path_);
        if (::close(fd) != 0 && status.ok()) {
            status = JournalIoError("cannot close pending journal", journal_path_);
        }
        if (status.ok() &&
            ::renameat(directory_fd_, temporary.c_str(), directory_fd_,
                       filename_.c_str()) != 0) {
            status = JournalIoError("cannot publish pending journal", journal_path_);
        }
        if (status.ok()) {
            status = SyncJournalDirectory(directory_fd_,
                                          journal_path_.parent_path());
        }
        if (!status.ok()) {
            static_cast<void>(
                ::unlinkat(directory_fd_, temporary.c_str(), 0));
        }
        return status;
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate pending recorder journal");
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Result<std::optional<RecorderPersistedPendingRecord>>
FileRecorderPendingStore::Load() noexcept {
    try {
        const int fd = ::openat(directory_fd_, filename_.c_str(),
                                JournalOpenFlags(O_RDONLY));
        if (fd < 0) {
            if (errno == ENOENT) {
                return std::optional<RecorderPersistedPendingRecord>{};
            }
            return JournalIoError("cannot open pending journal", journal_path_);
        }
        struct stat info {};
        if (::fstat(fd, &info) != 0 || !S_ISREG(info.st_mode) ||
            info.st_size < 0 ||
            static_cast<uint64_t>(info.st_size) > kMaximumPendingJournalBytes) {
            static_cast<void>(::close(fd));
            return Status::Error(StatusCode::kCorruption,
                                 "pending journal size/type is invalid");
        }
        std::vector<std::byte> encoded(static_cast<size_t>(info.st_size));
        size_t completed = 0;
        while (completed < encoded.size()) {
            const ssize_t count = ::read(fd, encoded.data() + completed,
                                         encoded.size() - completed);
            if (count < 0) {
                if (errno == EINTR) continue;
                const Status failure =
                    JournalIoError("cannot read pending journal", journal_path_);
                static_cast<void>(::close(fd));
                return failure;
            }
            if (count == 0) {
                static_cast<void>(::close(fd));
                return Status::Error(StatusCode::kCorruption,
                                     "pending journal is truncated");
            }
            completed += static_cast<size_t>(count);
        }
        static_cast<void>(::close(fd));
        Result<RecorderPersistedPendingRecord> decoded =
            DecodePending(std::move(encoded));
        if (!decoded.ok()) return decoded.status();
        return std::optional<RecorderPersistedPendingRecord>(
            std::move(*decoded));
    } catch (const std::bad_alloc&) {
        return Exhausted("cannot allocate pending recorder journal load");
    } catch (const std::exception& error) {
        return Invalid(error.what());
    }
}

Status FileRecorderPendingStore::Clear() noexcept {
    if (::unlinkat(directory_fd_, filename_.c_str(), 0) != 0) {
        if (errno == ENOENT) return Status::Ok();
        return JournalIoError("cannot remove pending journal", journal_path_);
    }
    return SyncJournalDirectory(directory_fd_, journal_path_.parent_path());
}

Result<MessageSource> FixedRecorderSourceResolver::Resolve(
    const MessageMetadata& metadata) noexcept {
    if (node_id_ == 0 || publisher_id_ == 0 || publisher_epoch_ == 0) {
        return Invalid("fixed recorder source identity is incomplete");
    }
    return MessageSource{
        .node_id = node_id_,
        .publisher_id = publisher_id_,
        .publisher_epoch = publisher_epoch_,
        .source_sequence = metadata.sequence_num,
        .observed_timestamp_ns = metadata.timestamp_ns,
    };
}

Result<RecorderCopyResult> RecorderBufferPoolSink::ReserveCopyCommit(
    const RecorderCopyRequest& request) noexcept {
    try {
        if (pool_ == nullptr || request.metadata == nullptr ||
            request.metadata->topic_id.value == 0 ||
            request.metadata->payload_size != request.payload.size() ||
            request.metadata->payload_crc !=
                RecorderPayloadCrc32c(request.payload)) {
            return Invalid("recorder copy request metadata is inconsistent");
        }

        Result<BufferReserveResult> reserved = pool_->Reserve(
            BufferReservationRequest{
                .topic_id = request.metadata->topic_id,
                .payload_size = request.payload.size(),
                .user_tag = request.user_tag,
                .full_policy = request.full_policy,
                .timeout = request.timeout,
                .metadata = *request.metadata,
            });
        if (!reserved.ok()) return reserved.status();

        RecorderCopyResult result{
            .admission = reserved->admission,
            .discarded = std::move(reserved->discarded),
        };
        if (!reserved->accepted()) return result;
        if (!reserved->reservation.active() ||
            reserved->reservation.bytes().size() != request.payload.size()) {
            reserved->reservation.Cancel();
            return Status::Error(StatusCode::kInternal,
                                 "recorder pool returned an invalid reservation");
        }

        std::copy(request.payload.begin(), request.payload.end(),
                  reserved->reservation.bytes().begin());
        const Status committed = std::move(reserved->reservation).Commit();
        if (!committed.ok()) return committed;
        return result;
    } catch (const std::bad_alloc&) {
        return Exhausted("recorder copy allocation failed");
    } catch (const std::length_error&) {
        return Exhausted("recorder copy exceeded a container bound");
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "recorder copy failed unexpectedly");
    }
}

}  // namespace mino::storage
