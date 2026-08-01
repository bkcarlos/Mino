// Copyright 2026 The Mino Authors

#include "mino/registry/id_allocator.h"

#include <limits>
#include <new>

namespace mino::registry {

InMemoryMonotonicIdAllocator::InMemoryMonotonicIdAllocator(
    uint32_t initial_high_watermark) noexcept
    : high_watermark_(initial_high_watermark) {}

Result<TopicId> InMemoryMonotonicIdAllocator::AllocateTopicId() {
    uint32_t observed = high_watermark_.load(std::memory_order_relaxed);
    for (;;) {
        if (observed == std::numeric_limits<uint32_t>::max()) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "TopicId high-water mark exhausted");
        }
        const uint32_t next = observed + 1;
        if (high_watermark_.compare_exchange_weak(
                observed, next, std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            return TopicId{next};
        }
    }
}

uint32_t InMemoryMonotonicIdAllocator::high_watermark() const noexcept {
    return high_watermark_.load(std::memory_order_acquire);
}

Result<std::shared_ptr<IdAllocator>> DefaultProcessIdAllocator() {
    try {
        static std::shared_ptr<IdAllocator> allocator =
            std::make_shared<InMemoryMonotonicIdAllocator>();
        return allocator;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

}  // namespace mino::registry
