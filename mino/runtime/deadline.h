// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_DEADLINE_H_
#define MINO_RUNTIME_DEADLINE_H_

#include <chrono>

namespace mino {

// Process-local steady-clock deadline used by blocking Runtime operations.
// Deadline values are never stored in shared memory or compared across hosts.
class Deadline {
public:
    using Clock = std::chrono::steady_clock;

    static Deadline Infinite() noexcept { return Deadline(Clock::time_point::max()); }

    static Deadline At(Clock::time_point time) noexcept { return Deadline(time); }

    template <typename Rep, typename Period>
    static Deadline FromNow(std::chrono::duration<Rep, Period> duration) noexcept {
        return Deadline(Clock::now() +
                        std::chrono::duration_cast<Clock::duration>(duration));
    }

    bool expired() const noexcept {
        return time_ != Clock::time_point::max() && Clock::now() >= time_;
    }

    Clock::time_point time_point() const noexcept { return time_; }

private:
    explicit Deadline(Clock::time_point time) noexcept : time_(time) {}

    Clock::time_point time_;
};

}  // namespace mino

#endif  // MINO_RUNTIME_DEADLINE_H_
