// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/simple_node.h"

#include <gtest/gtest.h>

#include <iostream>

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
#include <sys/wait.h>
#include <vector>

extern char** environ;

namespace mino {
namespace {

constexpr uint32_t kTopicSlots = 4;
constexpr uint64_t kShmBudgetBytes = 64ull << 20;
constexpr auto kChildTimeout = std::chrono::seconds(90);

std::filesystem::path Runfile(std::string_view relative) {
    const char* srcdir = std::getenv("TEST_SRCDIR");
    const char* workspace = std::getenv("TEST_WORKSPACE");
    return std::filesystem::path(srcdir == nullptr ? "" : srcdir) /
           (workspace == nullptr ? "_main" : workspace) / relative;
}

std::filesystem::path WorkerPath() {
    const std::filesystem::path primary =
        Runfile("examples/simple_mp_pubsub_stress");
    if (std::filesystem::exists(primary)) return primary;
    const char* runfiles = std::getenv("RUNFILES_DIR");
    if (runfiles != nullptr) {
        const std::filesystem::path alt =
            std::filesystem::path(runfiles) / "_main" /
            "examples/simple_mp_pubsub_stress";
        if (std::filesystem::exists(alt)) return alt;
    }
    return primary;
}

std::string UniqueName(const char* tag) {
    static std::atomic<uint32_t> sequence{0};
    return std::string("/mns") + std::to_string(::getpid()) + "_" +
           std::to_string(sequence.fetch_add(1) + 1) + "_" + tag;
}

std::filesystem::path TmpPath(const std::string& file) {
    const char* tmp = std::getenv("TEST_TMPDIR");
    return std::filesystem::path(tmp == nullptr ? "." : tmp) / file;
}

bool ExtractU64(std::string_view json, std::string_view key, uint64_t* out) {
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

bool ExtractF64(std::string_view json, std::string_view key, double* out) {
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

std::string ReadFd(int fd) {
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

std::string ReadFile(const std::filesystem::path& path) {
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

int SpawnWorker(const std::filesystem::path& worker, const std::vector<std::string>& args,
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

bool WaitChild(Child* child, int* exit_code, std::chrono::seconds timeout) {
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

class SimpleNodeMpStressTest : public ::testing::Test {
protected:
    void TearDown() override {
        for (const auto& name : created_) {
            (void)SimpleNode::Unlink(name);
        }
    }

    std::string MakeName(const char* tag) {
        std::string name = UniqueName(tag);
        created_.push_back(name);
        return name;
    }

    void RunProfile(uint32_t payload_bytes, uint64_t messages,
                    uint32_t queue_depth, const char* tag) {
        SimpleNodeOptions options;
        options.topic_slots = kTopicSlots;
        options.queue_depth = queue_depth;
        options.max_payload_bytes = payload_bytes;
        auto required = SimpleNode::RequiredBytes(options);
        ASSERT_TRUE(required.ok()) << required.status().ToString();
        ASSERT_LT(*required, kShmBudgetBytes)
            << "profile " << tag << " needs " << *required
            << " bytes; must fit in a 64 MiB /dev/shm";

        const std::filesystem::path worker = WorkerPath();
        ASSERT_TRUE(std::filesystem::exists(worker)) << worker;

        const std::string name = MakeName(tag);
        const std::string messages_s = std::to_string(messages);
        const std::string payload_s = std::to_string(payload_bytes);
        const std::string depth_s = std::to_string(queue_depth);
        const std::vector<std::string> sub_args = {
            "sub", name, "--messages", messages_s, "--payload-bytes",
            payload_s, "--queue-depth", depth_s,
        };
        const std::vector<std::string> pub_args = {
            "pub", name, "--messages", messages_s, "--payload-bytes",
            payload_s, "--queue-depth", depth_s,
        };

        Child sub;
        Child pub;
        ASSERT_EQ(SpawnWorker(worker, sub_args, TmpPath(std::string(tag) + ".sub.err"),
                              &sub),
                  0)
            << "posix_spawn sub failed";
        ASSERT_EQ(SpawnWorker(worker, pub_args, TmpPath(std::string(tag) + ".pub.err"),
                              &pub),
                  0)
            << "posix_spawn pub failed";

        int sub_exit = -1;
        int pub_exit = -1;
        const bool pub_ok = WaitChild(&pub, &pub_exit, kChildTimeout);
        const bool sub_ok = WaitChild(&sub, &sub_exit, kChildTimeout);
        const std::string json = ReadFd(sub.stdout_rd);
        const std::string sub_err = ReadFile(sub.err_path);
        const std::string pub_err = ReadFile(pub.err_path);
        if (sub.stdout_rd >= 0) {
            ::close(sub.stdout_rd);
            sub.stdout_rd = -1;
        }
        if (pub.stdout_rd >= 0) {
            ::close(pub.stdout_rd);
            pub.stdout_rd = -1;
        }

        std::cout << "profile=" << tag << " payload_bytes=" << payload_bytes
                  << " messages=" << messages << " queue_depth=" << queue_depth
                  << " required_bytes=" << *required << "\n"
                  << "json=" << json << "\n"
                  << "pub_err=" << pub_err << "\n"
                  << "sub_err=" << sub_err << "\n";

        ASSERT_TRUE(pub_ok) << "publisher did not exit cleanly, code " << pub_exit
                            << "\n"
                            << pub_err;
        ASSERT_TRUE(sub_ok) << "subscriber did not exit cleanly, code " << sub_exit
                            << "\n"
                            << sub_err;
        EXPECT_EQ(pub_exit, 0) << pub_err;
        EXPECT_EQ(sub_exit, 0) << sub_err << "\n" << json;

        uint64_t received = 0;
        uint64_t lost = 0;
        uint64_t expected = 0;
        uint64_t p50_ns = 0;
        uint64_t p95_ns = 0;
        double msgs_per_s = 0.0;
        ASSERT_TRUE(ExtractU64(json, "received", &received)) << json;
        ASSERT_TRUE(ExtractU64(json, "lost", &lost)) << json;
        ASSERT_TRUE(ExtractU64(json, "expected", &expected)) << json;
        ASSERT_TRUE(ExtractU64(json, "p50_ns", &p50_ns)) << json;
        ASSERT_TRUE(ExtractU64(json, "p95_ns", &p95_ns)) << json;
        ASSERT_TRUE(ExtractF64(json, "msgs_per_s", &msgs_per_s)) << json;
        EXPECT_EQ(expected, messages);
        EXPECT_EQ(received, messages);
        EXPECT_EQ(lost, 0u);
        EXPECT_GT(msgs_per_s, 0.0);
        RecordProperty("payload_bytes", static_cast<int>(payload_bytes));
        RecordProperty("messages", static_cast<int>(messages));
        RecordProperty("p50_ns", std::to_string(p50_ns));
        RecordProperty("p95_ns", std::to_string(p95_ns));
        RecordProperty("msgs_per_s", std::to_string(msgs_per_s));
    }

    std::vector<std::string> created_;
};

TEST_F(SimpleNodeMpStressTest, SmallPayloadIndependentProcesses) {
    RunProfile(256, 20000, 32, "s256");
}

TEST_F(SimpleNodeMpStressTest, MediumPayloadIndependentProcesses) {
    RunProfile(64u * 1024u, 2000, 32, "m64k");
}

}  // namespace
}  // namespace mino
