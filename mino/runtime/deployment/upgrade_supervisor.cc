// Copyright 2026 The Mino Authors

#include "mino/runtime/deployment/upgrade_supervisor.h"

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

#include "mino/common/status.h"
#include "mino/upgrade/manifest.h"
#include "mino/upgrade/orchestrator.h"

namespace mino::deployment {
namespace {

Status SocketError(std::string_view operation) {
    return Status::Error(
        errno == EACCES || errno == EPERM ? StatusCode::kPermissionDenied
                                          : StatusCode::kUnavailable,
        std::string(operation) + ": " + std::strerror(errno));
}

Status WriteAll(int fd, std::string_view value) {
    size_t written = 0;
    while (written < value.size()) {
        const ssize_t count =
            ::write(fd, value.data() + written, value.size() - written);
        if (count < 0) {
            if (errno == EINTR) continue;
            return SocketError("write(upgrade control response)");
        }
        if (count == 0) {
            return Status::Error(StatusCode::kUnavailable,
                                 "short upgrade control response write");
        }
        written += static_cast<size_t>(count);
    }
    return Status::Ok();
}

Result<std::string> ReadRequest(int fd) {
    std::string request;
    request.reserve(1024);
    char buffer[1024];
    for (;;) {
        const ssize_t count = ::read(fd, buffer, sizeof(buffer));
        if (count < 0) {
            if (errno == EINTR) continue;
            return SocketError("read(upgrade control request)");
        }
        if (count == 0) break;
        if (request.size() + static_cast<size_t>(count) >
            kMaximumUpgradeControlRequestBytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "upgrade control request exceeds its bound");
        }
        request.append(buffer, static_cast<size_t>(count));
        if (request.ends_with("\n\n")) break;
    }
    return request;
}

Status ValidatePeer(int fd) {
#if defined(__APPLE__)
    uid_t uid = 0;
    gid_t gid = 0;
    if (::getpeereid(fd, &uid, &gid) != 0) {
        return SocketError("getpeereid(upgrade control)");
    }
    static_cast<void>(gid);
    if (uid != ::geteuid()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "upgrade control peer uid differs from supervisor");
    }
#elif defined(__linux__)
    struct ucred credentials {};
    socklen_t size = sizeof(credentials);
    if (::getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &credentials, &size) != 0) {
        return SocketError("getsockopt(SO_PEERCRED)");
    }
    if (credentials.uid != ::geteuid()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "upgrade control peer uid differs from supervisor");
    }
#else
    static_cast<void>(fd);
    return Status::Error(StatusCode::kUnsupported,
                         "upgrade control peer credentials are unsupported");
#endif
    return Status::Ok();
}

struct ParsedRequest {
    std::string command;
    std::filesystem::path manifest;
};

Result<ParsedRequest> ParseRequest(std::string_view request) {
    const size_t first_newline = request.find('\n');
    if (first_newline == std::string_view::npos ||
        request.substr(0, first_newline) != "MINO-UPGRADE/1") {
        return Status::Error(StatusCode::kInvalidArgument,
                             "invalid upgrade control protocol header");
    }
    const size_t second_newline = request.find('\n', first_newline + 1);
    if (second_newline == std::string_view::npos) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "upgrade control command is missing");
    }
    ParsedRequest parsed;
    parsed.command = std::string(
        request.substr(first_newline + 1, second_newline - first_newline - 1));
    const size_t third_newline = request.find('\n', second_newline + 1);
    if (third_newline == std::string_view::npos) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "upgrade control manifest is missing");
    }
    const std::string_view manifest = request.substr(
        second_newline + 1, third_newline - second_newline - 1);
    constexpr std::string_view prefix = "manifest=";
    if (!manifest.starts_with(prefix) || manifest.size() == prefix.size() ||
        manifest.find('\0') != std::string_view::npos) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "upgrade control manifest field is invalid");
    }
    parsed.manifest = std::string(manifest.substr(prefix.size()));
    if (parsed.command != "execute" && parsed.command != "resume" &&
        parsed.command != "rollback") {
        return Status::Error(StatusCode::kUnsupported,
                             "unsupported upgrade control command");
    }
    return parsed;
}

std::string SnapshotResponse(const upgrade::UpgradeSnapshot& snapshot) {
    return "OK\nphase=" + std::string(upgrade::UpgradePhaseName(snapshot.phase)) +
           "\ngeneration=" + std::to_string(snapshot.generation) + "\n\n";
}

}  // namespace

ProductionUpgradeSupervisor::ProductionUpgradeSupervisor(
    std::filesystem::path socket_path,
    upgrade::UpgradeControlPlane* control, int listen_fd) noexcept
    : socket_path_(std::move(socket_path)),
      control_(control),
      listen_fd_(listen_fd) {}

Result<std::unique_ptr<ProductionUpgradeSupervisor>>
ProductionUpgradeSupervisor::Create(
    std::filesystem::path socket_path,
    upgrade::UpgradeControlPlane* production_control) noexcept {
    try {
        if (socket_path.empty() || production_control == nullptr ||
            socket_path.string().size() >= sizeof(sockaddr_un::sun_path)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "upgrade supervisor socket configuration is invalid");
        }
        const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) return SocketError("socket(upgrade control)");
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        std::memcpy(address.sun_path, socket_path.c_str(),
                    socket_path.string().size() + 1);
        static_cast<void>(::unlink(socket_path.c_str()));
        if (::bind(fd, reinterpret_cast<const sockaddr*>(&address),
                   sizeof(address)) != 0 ||
            ::chmod(socket_path.c_str(), 0600) != 0 || ::listen(fd, 16) != 0) {
            const Status status = SocketError("bind/listen(upgrade control)");
            static_cast<void>(::close(fd));
            static_cast<void>(::unlink(socket_path.c_str()));
            return status;
        }
        return std::unique_ptr<ProductionUpgradeSupervisor>(
            new ProductionUpgradeSupervisor(std::move(socket_path),
                                            production_control, fd));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

ProductionUpgradeSupervisor::~ProductionUpgradeSupervisor() {
    if (listen_fd_ >= 0) static_cast<void>(::close(listen_fd_));
    if (!socket_path_.empty()) static_cast<void>(::unlink(socket_path_.c_str()));
}

Status ProductionUpgradeSupervisor::ServeOne(uint64_t now_ns) noexcept {
    if (now_ns == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "upgrade supervisor time is zero");
    }
    int connection = -1;
    do {
        connection = ::accept(listen_fd_, nullptr, nullptr);
    } while (connection < 0 && errno == EINTR);
    if (connection < 0) return SocketError("accept(upgrade control)");
    struct CloseConnection final {
        int fd;
        ~CloseConnection() { static_cast<void>(::close(fd)); }
    } close_connection{connection};

    Status status = ValidatePeer(connection);
    Result<std::string> request = status.ok()
                                      ? ReadRequest(connection)
                                      : Result<std::string>(status);
    Result<ParsedRequest> parsed =
        request.ok() ? ParseRequest(*request)
                     : Result<ParsedRequest>(request.status());
    if (!parsed.ok()) {
        static_cast<void>(WriteAll(connection,
                                   "ERR\n" + parsed.status().ToString() + "\n\n"));
        return parsed.status();
    }
    auto store = upgrade::UpgradeManifestStore::Open(parsed->manifest);
    if (!store.ok()) {
        static_cast<void>(WriteAll(connection,
                                   "ERR\n" + store.status().ToString() + "\n\n"));
        return store.status();
    }
    upgrade::UpgradeOrchestrator orchestrator(store->get(), control_);
    status = parsed->command == "rollback" ? orchestrator.Rollback(now_ns)
                                             : orchestrator.Execute(now_ns);
    if (!status.ok()) {
        static_cast<void>(WriteAll(connection,
                                   "ERR\n" + status.ToString() + "\n\n"));
        return status;
    }
    return WriteAll(connection, SnapshotResponse((*store)->snapshot()));
}

}  // namespace mino::deployment
