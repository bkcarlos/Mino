// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_MP_STRESS_HARNESS_H_
#define MINO_RUNTIME_MP_STRESS_HARNESS_H_

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <spawn.h>
#include <string>
#include <string_view>
#include <thread>
#include <signal.h>
#include <unistd.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <vector>

extern char** environ;

namespace mino::mp_stress {

inline constexpr auto kChildTimeout = std::chrono::seconds(90);

inline std::filesystem::path Runfile(std::string_view relative) {
    const char* srcdir = std::getenv("TEST_SRCDIR");
    const char* workspace = std::getenv("TEST_WORKSPACE");
    return std::filesystem::path(srcdir == nullptr ? "" : srcdir) /
           (workspace == nullptr ? "_main" : workspace) / relative;
}

inline std::filesystem::path FindWorker(std::string_view relative) {
    const std::filesystem::path primary = Runfile(relative);
    if (std::filesystem::exists(primary)) return primary;
    const char* runfiles = std::getenv("RUNFILES_DIR");
    if (runfiles != nullptr) {
        const std::filesystem::path alt =
            std::filesystem::path(runfiles) / "_main" / relative;
        if (std::filesystem::exists(alt)) return alt;
    }
    return primary;
}

inline std::string UniqueShmName(const char* tag) {
    static std::atomic<uint32_t> sequence{0};
    return std::string("/mns") + std::to_string(::getpid()) + "_" +
           std::to_string(sequence.fetch_add(1) + 1) + "_" + tag;
}

inline std::filesystem::path UniqueIpcPath(const char* tag) {
    static std::atomic<uint32_t> sequence{0};
    const std::string file = std::string("zmqipc_") +
                             std::to_string(::getpid()) + "_" +
                             std::to_string(sequence.fetch_add(1) + 1) + "_" +
                             tag + ".sock";
    constexpr size_t kMaxSunPath = sizeof(sockaddr_un::sun_path) - 1;
    const char* tmp = std::getenv("TEST_TMPDIR");
    if (tmp != nullptr && *tmp != '\0') {
        std::filesystem::path candidate = std::filesystem::path(tmp) / file;
        if (candidate.string().size() <= kMaxSunPath) return candidate;
    }
    return std::filesystem::path("/tmp") / file;
}

inline std::filesystem::path TmpPath(const std::string& file) {
    const char* tmp = std::getenv("TEST_TMPDIR");
    return std::filesystem::path(tmp == nullptr ? "." : tmp) / file;
}

inline bool ExtractU64(std::string_view json, std::string_view key, uint64_t* out) {
    const std::string needle = "\"" + std::string(key) + "\":";
    const auto pos = json.find(needle);
    if (pos == std::string_view::npos || out == nullptr) return false;
    size_t i = pos + needle.size();
    while (i < json.size() && json[i] == ' ') ++i;
    if (i >= json.size() || json[i] < '0' || json[i] > '9') return false;
    uint64_t value = 0;
    while (i < json.size() && json[i] >= '0' && json[i] <= '9') {
        value = value * 10ull + static_cast<uint64_t>(json[i] - '0');
        ++i;
    }
    *out = value;
    return true;
}

inline bool ExtractF64(std::string_view json, std::string_view key, double* out) {
    const std::string needle = "\"" + std::string(key) + "\":";
    const auto pos = json.find(needle);
    if (pos == std::string_view::npos || out == nullptr) return false;
    size_t i = pos + needle.size();
    while (i < json.size() && json[i] == ' ') ++i;
    errno = 0;
    char* end = nullptr;
    const std::string slice(json.substr(i));
    const double value = std::strtod(slice.c_str(), &end);
    if (errno != 0 || end == slice.c_str()) return false;
    *out = value;
    return true;
}

inline bool ExtractQuoted(std::string_view json, std::string_view key,
                          std::string* out) {
    const std::string needle = "\"" + std::string(key) + "\":";
    const auto pos = json.find(needle);
    if (pos == std::string_view::npos || out == nullptr) return false;
    size_t i = pos + needle.size();
    while (i < json.size() && json[i] == ' ') ++i;
    if (i >= json.size() || json[i] != '"') return false;
    ++i;
    const size_t start = i;
    while (i < json.size() && json[i] != '"') ++i;
    if (i >= json.size()) return false;
    *out = std::string(json.substr(start, i - start));
    return true;
}

inline std::string ReadFd(int fd) {
    std::string out;
    char buf[4096];
    for (;;) {
        const ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n > 0) {
            out.append(buf, static_cast<size_t>(n));
            continue;
        }
        if (n == 0) break;
        if (errno == EINTR) continue;
        break;
    }
    return out;
}

inline std::string ReadFile(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return {};
    std::string text = ReadFd(fd);
    ::close(fd);
    return text;
}

struct Child {
    pid_t pid = -1;
    int stdout_rd = -1;
    std::filesystem::path err_path;

    Child() = default;
    Child(const Child&) = delete;
    Child& operator=(const Child&) = delete;
    Child(Child&& other) noexcept { *this = std::move(other); }
    Child& operator=(Child&& other) noexcept {
        if (this == &other) return *this;
        Reset();
        pid = other.pid;
        stdout_rd = other.stdout_rd;
        err_path = std::move(other.err_path);
        other.pid = -1;
        other.stdout_rd = -1;
        return *this;
    }
    ~Child() { Reset(); }

    void Reset() {
        if (stdout_rd >= 0) {
            ::close(stdout_rd);
            stdout_rd = -1;
        }
        if (pid > 0) {
            ::kill(pid, SIGKILL);
            int status = 0;
            ::waitpid(pid, &status, 0);
            pid = -1;
        }
    }
};

inline int SpawnWorker(const std::filesystem::path& worker,
                       const std::vector<std::string>& args,
                       const std::filesystem::path& err_path, Child* child) {
    std::vector<char*> argv;
    argv.reserve(args.size() + 2);
    argv.push_back(const_cast<char*>(worker.c_str()));
    for (const std::string& arg : args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);

    int pipefd[2];
    if (::pipe2(pipefd, O_CLOEXEC) != 0) return errno;
    const int errfd =
        ::open(err_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (errfd < 0) {
        const int saved = errno;
        ::close(pipefd[0]);
        ::close(pipefd[1]);
        return saved;
    }

    posix_spawn_file_actions_t actions;
    const int init_rc = posix_spawn_file_actions_init(&actions);
    if (init_rc != 0) {
        ::close(pipefd[0]);
        ::close(pipefd[1]);
        ::close(errfd);
        return init_rc;
    }
    posix_spawn_file_actions_adddup2(&actions, pipefd[1], STDOUT_FILENO);
    posix_spawn_file_actions_adddup2(&actions, errfd, STDERR_FILENO);
    posix_spawn_file_actions_addclose(&actions, pipefd[0]);
    posix_spawn_file_actions_addclose(&actions, pipefd[1]);
    posix_spawn_file_actions_addclose(&actions, errfd);

    pid_t pid = -1;
    const int rc = posix_spawn(&pid, worker.c_str(), &actions, nullptr,
                               argv.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    ::close(pipefd[1]);
    ::close(errfd);
    if (rc != 0) {
        ::close(pipefd[0]);
        return rc;
    }
    child->pid = pid;
    child->stdout_rd = pipefd[0];
    child->err_path = err_path;
    return 0;
}

inline bool WaitChild(Child* child, int* exit_code, std::chrono::seconds timeout) {
    if (child == nullptr || child->pid <= 0) return false;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    int status = 0;
    for (;;) {
        const pid_t r = ::waitpid(child->pid, &status, WNOHANG);
        if (r == child->pid) {
            child->pid = -1;
            if (WIFEXITED(status)) {
                *exit_code = WEXITSTATUS(status);
                return true;
            }
            *exit_code = 128 + (WIFSIGNALED(status) ? WTERMSIG(status) : 0);
            return false;
        }
        if (r < 0 && errno != EINTR) {
            *exit_code = -1;
            return false;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            ::kill(child->pid, SIGKILL);
            ::waitpid(child->pid, &status, 0);
            child->pid = -1;
            *exit_code = -1;
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

}  // namespace mino::mp_stress

#endif  // MINO_RUNTIME_MP_STRESS_HARNESS_H_
