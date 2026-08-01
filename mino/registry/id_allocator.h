// Copyright 2026 The Mino Authors

#ifndef MINO_REGISTRY_ID_ALLOCATOR_H_
#define MINO_REGISTRY_ID_ALLOCATOR_H_

#include <atomic>
#include <cstdint>
#include <memory>

#include "mino/common/ids.h"
#include "mino/common/result.h"

namespace mino::registry {

// Production durability boundary for never-reused Topic IDs.
//
// AllocateTopicId() MUST durably persist the new high-water mark before it
// returns the ID to its caller. A failed allocation may burn IDs, but an ID
// that was ever returned must never be returned again, including after process
// restart. Implementations backed by WAL/SQLite/consensus storage belong behind
// this interface; Coordinator intentionally does not fake persistence.
enum class IdAllocatorDurability : uint8_t {
    kEphemeral = 0,
    kDurable = 1,
};

class IdAllocator {
public:
    virtual ~IdAllocator() = default;
    virtual IdAllocatorDurability durability() const noexcept = 0;
    virtual Result<TopicId> AllocateTopicId() = 0;
};

// Process-memory monotonic allocator for unit tests and single-process
// development. It is not crash-durable and therefore must be replaced by a
// durable IdAllocator in production.
class InMemoryMonotonicIdAllocator final : public IdAllocator {
public:
    explicit InMemoryMonotonicIdAllocator(uint32_t initial_high_watermark = 0)
        noexcept;

    IdAllocatorDurability durability() const noexcept override {
        return IdAllocatorDurability::kEphemeral;
    }
    Result<TopicId> AllocateTopicId() override;
    uint32_t high_watermark() const noexcept;

private:
    std::atomic<uint32_t> high_watermark_;
};

// Shared process-wide development default. Sharing prevents accidental reuse
// when multiple Coordinator objects are constructed in one process.
Result<std::shared_ptr<IdAllocator>> DefaultProcessIdAllocator();

}  // namespace mino::registry

#endif  // MINO_REGISTRY_ID_ALLOCATOR_H_
