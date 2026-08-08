// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_OBSERVABILITY_BOUNDED_QUEUE_H_
#define MINO_OBSERVABILITY_BOUNDED_QUEUE_H_

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

namespace mino::observability {

// A fixed-storage bounded MPMC queue. TryPush/TryPop never allocate, never take
// a lock, and never wait for the peer. Capacity must be a power of two.
//
// T must be default constructible and nothrow copy assignable. Queue lifetime
// must cover every producer and consumer operation.
template <typename T, size_t Capacity>
class BoundedQueue {
    static_assert(Capacity >= 2 && (Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of two and at least two");
    static_assert(std::is_default_constructible_v<T>);
    static_assert(std::is_nothrow_copy_assignable_v<T>);

public:
    BoundedQueue() noexcept {
        for (size_t i = 0; i < Capacity; ++i) {
            cells_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

    bool TryPush(const T& value) noexcept {
        size_t position = enqueue_position_.load(std::memory_order_relaxed);
        Cell* cell = nullptr;
        for (;;) {
            cell = &cells_[position & kMask];
            const size_t sequence =
                cell->sequence.load(std::memory_order_acquire);
            const intptr_t difference = static_cast<intptr_t>(sequence) -
                                        static_cast<intptr_t>(position);
            if (difference == 0) {
                if (enqueue_position_.compare_exchange_weak(
                        position, position + 1, std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    break;
                }
            } else if (difference < 0) {
                return false;
            } else {
                position = enqueue_position_.load(std::memory_order_relaxed);
            }
        }
        cell->value = value;
        cell->sequence.store(position + 1, std::memory_order_release);
        return true;
    }

    bool TryPop(T* value) noexcept {
        if (value == nullptr) return false;
        size_t position = dequeue_position_.load(std::memory_order_relaxed);
        Cell* cell = nullptr;
        for (;;) {
            cell = &cells_[position & kMask];
            const size_t sequence =
                cell->sequence.load(std::memory_order_acquire);
            const intptr_t difference = static_cast<intptr_t>(sequence) -
                                        static_cast<intptr_t>(position + 1);
            if (difference == 0) {
                if (dequeue_position_.compare_exchange_weak(
                        position, position + 1, std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    break;
                }
            } else if (difference < 0) {
                return false;
            } else {
                position = dequeue_position_.load(std::memory_order_relaxed);
            }
        }
        *value = cell->value;
        cell->sequence.store(position + Capacity, std::memory_order_release);
        return true;
    }

    static constexpr size_t capacity() noexcept { return Capacity; }

private:
    static constexpr size_t kMask = Capacity - 1;

    struct Cell {
        std::atomic<size_t> sequence{0};
        T value{};
    };

    alignas(64) std::array<Cell, Capacity> cells_{};
    alignas(64) std::atomic<size_t> enqueue_position_{0};
    alignas(64) std::atomic<size_t> dequeue_position_{0};
};

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_BOUNDED_QUEUE_H_
