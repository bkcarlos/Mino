// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/common/provenance.h"

#include <sys/utsname.h>
#include <unistd.h>

#include <array>
#include <ctime>
#include <limits>
#include <string>
#include <thread>

namespace mino::benchmarks::validation {

std::string RunTimestampUtc() {
    const std::time_t now = std::time(nullptr);
    std::tm utc{};
    if (now == static_cast<std::time_t>(-1) ||
        ::gmtime_r(&now, &utc) == nullptr) {
        return "PENDING";
    }
    std::array<char, 32> buffer{};
    if (std::strftime(buffer.data(), buffer.size(), "%Y-%m-%dT%H:%M:%SZ",
                      &utc) == 0) {
        return "PENDING";
    }
    return buffer.data();
}

std::string OperatingSystem() {
    struct utsname value {};
    if (::uname(&value) != 0) return "PENDING";
    return std::string(value.sysname) + " " + value.release + " " + value.machine;
}

uint64_t PhysicalMemoryBytes() {
    const long pages = ::sysconf(_SC_PHYS_PAGES);
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if (pages <= 0 || page_size <= 0) return 0;
    const auto unsigned_pages = static_cast<uint64_t>(pages);
    const auto unsigned_page_size = static_cast<uint64_t>(page_size);
    if (unsigned_pages > std::numeric_limits<uint64_t>::max() /
                             unsigned_page_size) {
        return 0;
    }
    return unsigned_pages * unsigned_page_size;
}

unsigned int LogicalCpuCount() { return std::thread::hardware_concurrency(); }

}  // namespace mino::benchmarks::validation
