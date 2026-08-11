// Copyright 2026 The Mino Authors

#include "mino/upgrade/routing_catalog.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <limits>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "mino/bridge/crc32c.h"
#include "mino/common/status.h"

namespace mino::upgrade {
namespace {

constexpr std::array<std::byte, 8> kMagic = {
    std::byte{'M'}, std::byte{'I'}, std::byte{'N'}, std::byte{'O'},
    std::byte{'R'}, std::byte{'O'}, std::byte{'U'}, std::byte{'1'},
};

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

class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) static_cast<void>(::close(fd_));
    }
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    int get() const noexcept { return fd_; }
    int release() noexcept {
        const int result = fd_;
        fd_ = -1;
        return result;
    }

private:
    int fd_;
};

int OpenFlags(int flags) noexcept {
#ifdef O_CLOEXEC
    return flags | O_CLOEXEC;
#else
    return flags;
#endif
}

Status WriteAll(int fd, std::span<const std::byte> bytes,
                const std::filesystem::path& path) {
    size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t count = ::write(fd, bytes.data() + written,
                                      bytes.size() - written);
        if (count < 0) {
            if (errno == EINTR) continue;
            return IoError("cannot write routing catalog", path);
        }
        if (count == 0) return Status::Error(StatusCode::kUnavailable);
        written += static_cast<size_t>(count);
    }
    return Status::Ok();
}

Status DataSync(int fd, const std::filesystem::path& path) {
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
        return IoError("cannot sync routing catalog", path);
    }
}

Status SyncDirectory(const std::filesystem::path& path) {
    int flags = OpenFlags(O_RDONLY);
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
    ScopedFd fd(::open(path.c_str(), flags));
    if (fd.get() < 0) return IoError("cannot open routing catalog directory", path);
    while (::fsync(fd.get()) != 0) {
        if (errno == EINTR) continue;
        return IoError("cannot sync routing catalog directory", path);
    }
    return Status::Ok();
}

Status AtomicWrite(const std::filesystem::path& path,
                   std::span<const std::byte> bytes) {
    const std::filesystem::path parent =
        path.parent_path().empty() ? std::filesystem::path(".")
                                   : path.parent_path();
    const std::filesystem::path temporary =
        parent / (path.filename().string() + ".tmp." +
                  std::to_string(static_cast<uint64_t>(::getpid())));
    ScopedFd fd(::open(temporary.c_str(), OpenFlags(O_WRONLY | O_CREAT | O_EXCL),
                       0600));
    if (fd.get() < 0) return IoError("cannot create routing catalog temporary", temporary);
    bool remove = true;
    auto cleanup = [&]() {
        if (remove) static_cast<void>(::unlink(temporary.c_str()));
    };
    Status status = WriteAll(fd.get(), bytes, temporary);
    if (status.ok()) status = DataSync(fd.get(), temporary);
    if (!status.ok()) {
        cleanup();
        return status;
    }
    if (::close(fd.release()) != 0) {
        cleanup();
        return IoError("cannot close routing catalog temporary", temporary);
    }
    if (::rename(temporary.c_str(), path.c_str()) != 0) {
        cleanup();
        return IoError("cannot atomically rename routing catalog", path);
    }
    remove = false;
    return SyncDirectory(parent);
}

void U16(uint16_t value, std::vector<std::byte>* bytes) {
    for (size_t index = 0; index < 2; ++index) {
        bytes->push_back(static_cast<std::byte>(value >> (index * 8)));
    }
}

void U32(uint32_t value, std::vector<std::byte>* bytes) {
    for (size_t index = 0; index < 4; ++index) {
        bytes->push_back(static_cast<std::byte>(value >> (index * 8)));
    }
}

void U64(uint64_t value, std::vector<std::byte>* bytes) {
    for (size_t index = 0; index < 8; ++index) {
        bytes->push_back(static_cast<std::byte>(value >> (index * 8)));
    }
}

void String(std::string_view value, std::vector<std::byte>* bytes) {
    U32(static_cast<uint32_t>(value.size()), bytes);
    bytes->insert(bytes->end(),
                  reinterpret_cast<const std::byte*>(value.data()),
                  reinterpret_cast<const std::byte*>(value.data() + value.size()));
}

class Cursor final {
public:
    explicit Cursor(std::span<const std::byte> bytes) noexcept : bytes_(bytes) {}

    Result<uint8_t> U8() noexcept {
        if (!Available(1)) return Corrupt("routing catalog is truncated");
        return static_cast<uint8_t>(bytes_[offset_++]);
    }
    Result<uint16_t> U16() noexcept {
        uint16_t value = 0;
        for (size_t index = 0; index < 2; ++index) {
            MINO_ASSIGN_OR_RETURN(const uint8_t byte, U8());
            value |= static_cast<uint16_t>(byte) << (index * 8);
        }
        return value;
    }
    Result<uint32_t> U32() noexcept {
        uint32_t value = 0;
        for (size_t index = 0; index < 4; ++index) {
            MINO_ASSIGN_OR_RETURN(const uint8_t byte, U8());
            value |= static_cast<uint32_t>(byte) << (index * 8);
        }
        return value;
    }
    Result<uint64_t> U64() noexcept {
        uint64_t value = 0;
        for (size_t index = 0; index < 8; ++index) {
            MINO_ASSIGN_OR_RETURN(const uint8_t byte, U8());
            value |= static_cast<uint64_t>(byte) << (index * 8);
        }
        return value;
    }
    Result<std::string> String() {
        MINO_ASSIGN_OR_RETURN(const uint32_t size, U32());
        if (size > kMaximumUpgradeStringBytes || !Available(size)) {
            return Corrupt("routing catalog string is invalid");
        }
        const char* begin =
            reinterpret_cast<const char*>(bytes_.data() + offset_);
        std::string value(begin, size);
        offset_ += size;
        if (value.find('\0') != std::string::npos) {
            return Corrupt("routing catalog string contains NUL");
        }
        return value;
    }
    bool done() const noexcept { return offset_ == bytes_.size(); }

private:
    bool Available(size_t count) const noexcept {
        return offset_ <= bytes_.size() && count <= bytes_.size() - offset_;
    }
    std::span<const std::byte> bytes_;
    size_t offset_ = 0;
};

Result<std::vector<std::byte>> ReadFile(const std::filesystem::path& path) {
    struct stat state {};
    if (::lstat(path.c_str(), &state) != 0) {
        return IoError("cannot stat routing catalog", path);
    }
    if (!S_ISREG(state.st_mode) || state.st_nlink != 1 ||
        state.st_uid != ::geteuid() || (state.st_mode & (S_IWGRP | S_IWOTH)) != 0 ||
        state.st_size <= 0 ||
        state.st_size > static_cast<off_t>(kMaximumRoutingCatalogBytes)) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "routing catalog must be a bounded owner-only regular file");
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) return IoError("cannot open routing catalog", path);
    std::vector<std::byte> bytes(static_cast<size_t>(state.st_size));
    input.read(reinterpret_cast<char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
    if (!input || input.gcount() != static_cast<std::streamsize>(bytes.size())) {
        return Corrupt("routing catalog short read");
    }
    return bytes;
}

Result<int> AcquireLock(const std::filesystem::path& path) {
    const std::filesystem::path lock_path = path.string() + ".lock";
    ScopedFd fd(::open(lock_path.c_str(), OpenFlags(O_RDWR | O_CREAT), 0600));
    if (fd.get() < 0) return IoError("cannot open routing catalog lock", lock_path);
    if (::flock(fd.get(), LOCK_EX | LOCK_NB) != 0) {
        return Status::Error(errno == EWOULDBLOCK ? StatusCode::kWouldBlock
                                                  : StatusCode::kUnavailable,
                             "routing catalog is owned by another supervisor");
    }
    return fd.release();
}

bool ValidRegion(const RegionIdentity& region) {
    return ValidateRegionIdentity(region).ok();
}

}  // namespace

Result<std::vector<std::byte>> EncodeRegionRoutingSnapshot(
    const RegionRoutingSnapshot& snapshot) noexcept {
    try {
        if (snapshot.generation == 0 || !ValidRegion(snapshot.active_region) ||
            snapshot.commit_token.size() > kMaximumUpgradeStringBytes ||
            snapshot.commit_token.find('\0') != std::string::npos) {
            return Invalid("routing catalog snapshot is invalid");
        }
        std::vector<std::byte> bytes(kMagic.begin(), kMagic.end());
        U16(1, &bytes);
        U64(snapshot.generation, &bytes);
        bytes.push_back(snapshot.source_fenced ? std::byte{1} : std::byte{0});
        String(snapshot.active_region.name, &bytes);
        U32(snapshot.active_region.region_id, &bytes);
        U64(snapshot.active_region.uuid_lo, &bytes);
        U64(snapshot.active_region.uuid_hi, &bytes);
        U16(snapshot.active_region.layout_version, &bytes);
        U64(snapshot.active_region.security_domain.value, &bytes);
        String(snapshot.commit_token, &bytes);
        const uint32_t crc = bridge::Crc32c(bytes);
        U32(crc, &bytes);
        if (bytes.size() > kMaximumRoutingCatalogBytes) {
            return Invalid("routing catalog exceeds its size bound");
        }
        return bytes;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<RegionRoutingSnapshot> DecodeRegionRoutingSnapshot(
    std::span<const std::byte> encoded) noexcept {
    try {
        if (encoded.size() < kMagic.size() + 4 ||
            encoded.size() > kMaximumRoutingCatalogBytes ||
            !std::equal(kMagic.begin(), kMagic.end(), encoded.begin())) {
            return Corrupt("routing catalog magic or size is invalid");
        }
        uint32_t stored_crc = 0;
        for (size_t index = 0; index < 4; ++index) {
            stored_crc |= static_cast<uint32_t>(static_cast<uint8_t>(
                              encoded[encoded.size() - 4 + index]))
                          << (index * 8);
        }
        const auto payload = encoded.first(encoded.size() - 4);
        if (bridge::Crc32c(payload) != stored_crc) {
            return Corrupt("routing catalog CRC32C mismatch");
        }
        Cursor cursor(payload.subspan(kMagic.size()));
        MINO_ASSIGN_OR_RETURN(const uint16_t version, cursor.U16());
        if (version != 1) return Corrupt("unsupported routing catalog version");
        RegionRoutingSnapshot snapshot;
        MINO_ASSIGN_OR_RETURN(snapshot.generation, cursor.U64());
        MINO_ASSIGN_OR_RETURN(const uint8_t fenced, cursor.U8());
        if (fenced > 1) return Corrupt("routing catalog fence value is invalid");
        snapshot.source_fenced = fenced != 0;
        MINO_ASSIGN_OR_RETURN(snapshot.active_region.name, cursor.String());
        MINO_ASSIGN_OR_RETURN(snapshot.active_region.region_id, cursor.U32());
        MINO_ASSIGN_OR_RETURN(snapshot.active_region.uuid_lo, cursor.U64());
        MINO_ASSIGN_OR_RETURN(snapshot.active_region.uuid_hi, cursor.U64());
        MINO_ASSIGN_OR_RETURN(snapshot.active_region.layout_version, cursor.U16());
        MINO_ASSIGN_OR_RETURN(snapshot.active_region.security_domain.value,
                              cursor.U64());
        MINO_ASSIGN_OR_RETURN(snapshot.commit_token, cursor.String());
        if (!cursor.done() || snapshot.generation == 0 ||
            !ValidRegion(snapshot.active_region)) {
            return Corrupt("routing catalog payload is invalid or trailing");
        }
        return snapshot;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<RegionRoutingSnapshot> LoadRegionRoutingSnapshot(
    const std::filesystem::path& path) noexcept {
    auto bytes = ReadFile(path);
    if (!bytes.ok()) return bytes.status();
    return DecodeRegionRoutingSnapshot(*bytes);
}

RegionRoutingCatalog::RegionRoutingCatalog(std::filesystem::path path,
                                           int lock_fd,
                                           RegionRoutingSnapshot snapshot) noexcept
    : path_(std::move(path)), lock_fd_(lock_fd), snapshot_(std::move(snapshot)) {}

RegionRoutingCatalog::~RegionRoutingCatalog() {
    if (lock_fd_ >= 0) static_cast<void>(::close(lock_fd_));
}

Result<std::unique_ptr<RegionRoutingCatalog>> RegionRoutingCatalog::Create(
    const std::filesystem::path& path,
    const RegionIdentity& initial_region) noexcept {
    try {
        if (path.empty() || std::filesystem::exists(path)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "routing catalog already exists or path is empty");
        }
        MINO_ASSIGN_OR_RETURN(const int lock_fd, AcquireLock(path));
        RegionRoutingSnapshot snapshot{
            .generation = 1,
            .active_region = initial_region,
            .commit_token = {},
            .source_fenced = false,
        };
        auto encoded = EncodeRegionRoutingSnapshot(snapshot);
        if (!encoded.ok()) {
            static_cast<void>(::close(lock_fd));
            return encoded.status();
        }
        const Status status = AtomicWrite(path, *encoded);
        if (!status.ok()) {
            static_cast<void>(::close(lock_fd));
            return status;
        }
        return std::unique_ptr<RegionRoutingCatalog>(
            new RegionRoutingCatalog(path, lock_fd, std::move(snapshot)));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<std::unique_ptr<RegionRoutingCatalog>> RegionRoutingCatalog::Open(
    const std::filesystem::path& path) noexcept {
    try {
        MINO_ASSIGN_OR_RETURN(const int lock_fd, AcquireLock(path));
        auto bytes = ReadFile(path);
        if (!bytes.ok()) {
            static_cast<void>(::close(lock_fd));
            return bytes.status();
        }
        auto snapshot = DecodeRegionRoutingSnapshot(*bytes);
        if (!snapshot.ok()) {
            static_cast<void>(::close(lock_fd));
            return snapshot.status();
        }
        return std::unique_ptr<RegionRoutingCatalog>(new RegionRoutingCatalog(
            path, lock_fd, std::move(*snapshot)));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Status RegionRoutingCatalog::Persist(RegionRoutingSnapshot next) noexcept {
    auto encoded = EncodeRegionRoutingSnapshot(next);
    if (!encoded.ok()) return encoded.status();
    const Status status = AtomicWrite(path_, *encoded);
    if (!status.ok()) {
        // A parent-directory sync failure can happen after rename. Re-read the
        // CRC-valid catalog so an in-process retry makes the same decision a
        // restarted supervisor would make instead of guessing the side effect.
        auto observed = LoadRegionRoutingSnapshot(path_);
        if (observed.ok()) snapshot_ = std::move(*observed);
        return status;
    }
    snapshot_ = std::move(next);
    return Status::Ok();
}

Result<RegionRoutingSnapshot> RegionRoutingCatalog::CompareExchange(
    uint64_t expected_generation, const RegionIdentity& expected_active,
    RegionRoutingSnapshot replacement) noexcept {
    if (expected_generation == 0 || replacement.generation != expected_generation + 1 ||
        snapshot_.generation != expected_generation ||
        !(snapshot_.active_region == expected_active)) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "routing catalog generation/active Region CAS failed");
    }
    MINO_RETURN_IF_ERROR(Persist(std::move(replacement)));
    return snapshot_;
}

Status RegionRoutingCatalog::FenceSource(const UpgradePlan& plan) noexcept {
    if (snapshot_.commit_token == plan.commit_token && snapshot_.source_fenced &&
        (snapshot_.active_region == plan.source_region ||
         snapshot_.active_region == plan.target_region)) {
        return Status::Ok();
    }
    if (!(snapshot_.active_region == plan.source_region) ||
        (!snapshot_.commit_token.empty() &&
         snapshot_.commit_token != plan.commit_token)) {
        return Status::Error(StatusCode::kAlreadyExists,
                             "routing catalog is owned by another route/token");
    }
    RegionRoutingSnapshot next = snapshot_;
    if (next.generation == std::numeric_limits<uint64_t>::max()) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "routing catalog generation exhausted");
    }
    ++next.generation;
    next.commit_token = plan.commit_token;
    next.source_fenced = true;
    return CompareExchange(snapshot_.generation, plan.source_region,
                           std::move(next))
        .status();
}

Status RegionRoutingCatalog::Cutover(const UpgradePlan& plan) noexcept {
    if (snapshot_.active_region == plan.target_region &&
        snapshot_.commit_token == plan.commit_token && snapshot_.source_fenced) {
        return Status::Ok();
    }
    if (!(snapshot_.active_region == plan.source_region) ||
        snapshot_.commit_token != plan.commit_token || !snapshot_.source_fenced) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "source fence/token is not durably established");
    }
    RegionRoutingSnapshot next = snapshot_;
    if (next.generation == std::numeric_limits<uint64_t>::max()) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "routing catalog generation exhausted");
    }
    ++next.generation;
    next.active_region = plan.target_region;
    return CompareExchange(snapshot_.generation, plan.source_region,
                           std::move(next))
        .status();
}

Status RegionRoutingCatalog::RestoreSource(const UpgradePlan& plan) noexcept {
    if (snapshot_.active_region == plan.source_region &&
        snapshot_.commit_token.empty() && !snapshot_.source_fenced) {
        return Status::Ok();
    }
    if (snapshot_.commit_token != plan.commit_token ||
        (snapshot_.active_region != plan.source_region &&
         snapshot_.active_region != plan.target_region)) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "routing rollback token/Region mismatch");
    }
    RegionRoutingSnapshot next = snapshot_;
    if (next.generation == std::numeric_limits<uint64_t>::max()) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "routing catalog generation exhausted");
    }
    const RegionIdentity expected = snapshot_.active_region;
    ++next.generation;
    next.active_region = plan.source_region;
    next.commit_token.clear();
    next.source_fenced = false;
    return CompareExchange(snapshot_.generation, expected, std::move(next)).status();
}

}  // namespace mino::upgrade
