// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_MESSAGE_TRAITS_H_
#define MINO_RUNTIME_MESSAGE_TRAITS_H_

#include <cstddef>
#include <cstdint>
#include <span>

#include "mino/abi/shm_handle.h"
#include "mino/common/ids.h"
#include "mino/common/status.h"

namespace mino {

// Bounded structural collector for one allocation graph. It guarantees root-
// first insertion and rejects cycles, duplicate children, and shared metadata
// by requiring every handle to be unique. It deliberately does not inspect the
// allocator: callers must separately validate generation freshness, owner
// epoch, allocation transaction, and slab state for every returned handle.
class OwnedGraphCollector final {
public:
    explicit OwnedGraphCollector(std::span<ShmHandle> output) noexcept
        : output_(output) {}

    Status AddRoot(ShmHandle root) noexcept {
        if (size_ != 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "owned graph root must be added first");
        }
        if (!HasNonNullShape(root)) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "owned graph root handle shape is invalid");
        }
        return Append(root);
    }

    Status AddOwnedChild(ShmHandle child) noexcept {
        if (size_ == 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "owned graph root is missing");
        }
        if (!HasNonNullShape(child)) {
            return Status::Error(StatusCode::kCorruption,
                                 "owned child handle shape is invalid");
        }
        for (size_t i = 0; i < size_; ++i) {
            if (output_[i] == child) {
                return Status::Error(
                    StatusCode::kCorruption,
                    i == 0 ? "owned graph metadata contains a cycle"
                           : "owned graph metadata is duplicate or shared");
            }
        }
        return Append(child);
    }

    size_t size() const noexcept { return size_; }

private:
    static bool HasNonNullShape(ShmHandle handle) noexcept {
        return handle.offset != 0;
    }

    Status Append(ShmHandle handle) noexcept {
        if (size_ == output_.size()) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "owned graph output is too small");
        }
        output_[size_++] = handle;
        return Status::Ok();
    }

    std::span<ShmHandle> output_;
    size_t size_ = 0;
};

// Bridge between fixed-layout SHM messages and the static Schema CodeGen
// contract. Applications may specialize this trait directly; generated types
// emit equivalent specializations. Runtime intentionally does not invent a
// partial Schema Registry here.
template <typename T>
struct StaticMessageTraits {
    static constexpr bool kIsSpecialized = false;
};

// Documents the required shape without forcing a C++ concept into every public
// signature. Publisher<T> and Subscriber<T> validate this flag and the fixed
// layout constraints with static_asserts.
template <typename T>
inline constexpr bool kHasStaticMessageTraits =
    StaticMessageTraits<T>::kIsSpecialized;

// Enumerates the allocation graph described by a generated static message.
// Successful output is deterministic and starts with root. This operation only
// validates serialized metadata shape and graph topology; allocator ownership
// and liveness checks remain the caller's responsibility.
template <typename T>
Status CollectOwnedGraph(ShmHandle root, const T& value,
                         std::span<ShmHandle> output,
                         size_t& handle_count) noexcept {
    return StaticMessageTraits<T>::CollectOwnedGraph(root, value, output,
                                                     handle_count);
}

}  // namespace mino

#endif  // MINO_RUNTIME_MESSAGE_TRAITS_H_
