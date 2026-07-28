// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.gnu.org/licenses/lgpl-3.0.txt
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include "mino/platform/process_identity.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <random>
#include <string>

#if defined(__unix__) || defined(__APPLE__)
#define MINO_HAS_POSIX 1
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#else
#define MINO_HAS_POSIX 0
#endif

#if defined(__linux__)
#include <cerrno>
#include <cstdlib>
#endif

namespace mino {
namespace {

// Reads the process start time in nanoseconds since boot.
//
// On Linux this reads the 22nd field of /proc/self/stat (starttime in clock
// ticks since boot) and converts using sysconf(_SC_CLK_TCK). On other POSIX
// systems there is no portable /proc; fall back to the current time, which
// still yields a unique-per-boot value in practice for the first version but
// is less robust to PID reuse.
uint64_t ReadProcessStartTimeNs() {
#if defined(__linux__)
    // /proc/self/stat format: pid (comm) state ppid ... starttime ...
    // The comm field may contain spaces/parens, so find the last ')' and
    // parse fields after it. starttime is field 22 overall, i.e. the 20th
    // field after ')' (fields after ')' start at field 3 = state).
    FILE* f = std::fopen("/proc/self/stat", "r");
    if (f != nullptr) {
        char buf[4096];
        const size_t n = std::fread(buf, 1, sizeof(buf) - 1, f);
        std::fclose(f);
        if (n > 0) {
            buf[n] = '\0';
            const char* close_paren = std::strrchr(buf, ')');
            if (close_paren != nullptr) {
                // Tokenize fields after ") " starting at field 3 (state).
                // We need field 22 => index 19 in zero-based tokens where
                // token 0 is field 3.
                const char* p = close_paren + 1;
                // Skip spaces.
                while (*p == ' ') {
                    ++p;
                }
                unsigned long long starttime_ticks = 0;
                int field_index = 0;  // token 0 == field 3 (state)
                bool found = false;
                // Copy the tail so we can tokenize with strtok.
                char tail[3500];
                std::snprintf(tail, sizeof(tail), "%s", p);
                char* saveptr = nullptr;
                for (char* tok = strtok_r(tail, " ", &saveptr);
                     tok != nullptr;
                     tok = strtok_r(nullptr, " ", &saveptr), ++field_index) {
                    if (field_index == 19) {  // field 22 = starttime
                        starttime_ticks = std::strtoull(tok, nullptr, 10);
                        found = true;
                        break;
                    }
                }
                if (found) {
                    const long ticks_per_sec = ::sysconf(_SC_CLK_TCK);
                    if (ticks_per_sec > 0) {
                        const uint64_t ticks = starttime_ticks;
                        const uint64_t secs = ticks / static_cast<uint64_t>(ticks_per_sec);
                        const uint64_t rem = ticks % static_cast<uint64_t>(ticks_per_sec);
                        return secs * 1000000000ull +
                               rem * (1000000000ull / static_cast<uint64_t>(ticks_per_sec));
                    }
                }
            }
        }
    }
    // Fall through to clock-based fallback below if parsing failed.
#endif
    // Fallback: current realtime. Not robust across PID reuse but provides a
    // monotonically-distinct value per process start on non-Linux systems.
    struct timespec ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull +
           static_cast<uint64_t>(ts.tv_nsec);
}

// Reads a machine-stable node identifier.
//
// On Linux this prefers /etc/machine-id (or /var/lib/dbus/machine-id), hashed
// into 64 bits. Falls back to gethostid(), then to gethostname() hash.
uint64_t ReadNodeId() {
#if defined(__linux__)
    const char* candidates[] = {"/etc/machine-id", "/var/lib/dbus/machine-id"};
    for (const char* path : candidates) {
        FILE* f = std::fopen(path, "r");
        if (f == nullptr) {
            continue;
        }
        char buf[128];
        char* line = std::fgets(buf, sizeof(buf), f);
        std::fclose(f);
        if (line != nullptr) {
            // FNV-1a hash of the machine-id string.
            uint64_t hash = 1469598103934665603ull;
            for (const unsigned char* p =
                     reinterpret_cast<const unsigned char*>(line);
                 *p != '\0' && *p != '\n'; ++p) {
                hash ^= *p;
                hash *= 1099511628211ull;
            }
            if (hash != 0) {
                return hash;
            }
        }
    }
    // Fall back to gethostid().
    const long hostid = ::gethostid();
    if (hostid > 0) {
        return static_cast<uint64_t>(hostid);
    }
#endif
    // Portable fallback: hash the hostname.
    char hostname[256];
    if (::gethostname(hostname, sizeof(hostname)) == 0) {
        hostname[sizeof(hostname) - 1] = '\0';
        uint64_t hash = 1469598103934665603ull;
        for (const unsigned char* p =
                 reinterpret_cast<const unsigned char*>(hostname);
             *p != '\0'; ++p) {
            hash ^= *p;
            hash *= 1099511628211ull;
        }
        if (hash != 0) {
            return hash;
        }
    }
    return 0;
}

// Generates a random per-boot/per-process epoch component.
uint64_t RandomEpochComponent() {
    std::random_device rd;
    // Combine two 32-bit draws; random_device is typically backed by
    // /dev/urandom on Linux.
    const uint64_t hi = static_cast<uint64_t>(rd()) << 32;
    const uint64_t lo = static_cast<uint64_t>(rd());
    return hi | lo;
}

ProcessIdentity ComputeCurrentIdentity() {
    ProcessIdentity id;
    id.node_id = ReadNodeId();
    id.process_id = static_cast<uint64_t>(::getpid());
    id.start_time_ns = ReadProcessStartTimeNs();

    // process_epoch must distinguish PID reuse. Combine:
    //  - PID (so two processes never share an epoch),
    //  - start time in ns (so a reused PID at a different time differs),
    //  - a random component (so two processes started in the same ns still
    //    differ, and so an attacker cannot predict the epoch).
    //
    // We mix them with a simple bijective combine (splitmix64 finalizer) so
    // the result is well-distributed.
    uint64_t mixed = id.process_id;
    mixed ^= id.start_time_ns + 0x9e3779b97f4a7c15ull + (mixed << 6) +
             (mixed >> 2);
    mixed ^= RandomEpochComponent() + 0x9e3779b97f4a7c15ull + (mixed << 6) +
             (mixed >> 2);
    // splitmix64 finalizer for avalanche.
    mixed += 0x9e3779b97f4a7c15ull;
    mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ull;
    mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebull;
    mixed = mixed ^ (mixed >> 31);
    id.process_epoch = mixed;
    return id;
}

}  // namespace

const ProcessIdentity& ProcessIdentity::Current() {
    // A fork inherits function-local statics. Cache the PID alongside the
    // identity and recompute in the child so process_epoch never aliases the
    // parent's epoch.
    static ProcessIdentity identity = ComputeCurrentIdentity();
    static uint64_t cached_pid = identity.process_id;
    const uint64_t current_pid = static_cast<uint64_t>(::getpid());
    if (cached_pid != current_pid) {
        identity = ComputeCurrentIdentity();
        cached_pid = current_pid;
    }
    return identity;
}

std::string ProcessIdentity::ToString() const {
    char buf[128];
    std::snprintf(buf, sizeof(buf),
                  "ProcessIdentity(node=%llu pid=%llu epoch=%llu start_ns=%llu)",
                  static_cast<unsigned long long>(node_id),
                  static_cast<unsigned long long>(process_id),
                  static_cast<unsigned long long>(process_epoch),
                  static_cast<unsigned long long>(start_time_ns));
    return std::string(buf);
}

void ProcessIdentity::SerializeTo(
    std::array<std::byte, kSerializedSize>& dest) const noexcept {
    // Write fields in declaration order, 8 raw bytes each. This keeps the
    // encoding stable as long as the field order and widths are unchanged.
    static_assert(kSerializedSize == 32);
    std::memcpy(dest.data() + 0, &node_id, 8);
    std::memcpy(dest.data() + 8, &process_id, 8);
    std::memcpy(dest.data() + 16, &process_epoch, 8);
    std::memcpy(dest.data() + 24, &start_time_ns, 8);
}

ProcessIdentity ProcessIdentity::DeserializeFrom(
    const std::array<std::byte, kSerializedSize>& src) noexcept {
    ProcessIdentity id;
    std::memcpy(&id.node_id, src.data() + 0, 8);
    std::memcpy(&id.process_id, src.data() + 8, 8);
    std::memcpy(&id.process_epoch, src.data() + 16, 8);
    std::memcpy(&id.start_time_ns, src.data() + 24, 8);
    return id;
}

}  // namespace mino
