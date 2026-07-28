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

#ifndef MINO_COMMON_RESULT_H_
#define MINO_COMMON_RESULT_H_

#include <cassert>
#include <memory>
#include <type_traits>
#include <utility>
#include <variant>

#include "mino/common/status.h"

namespace mino {

// Result<T> represents either a value of type T or an error Status.
// It is the primary error-propagation mechanism across Mino module boundaries.
//
// Usage:
//   Result<int> r = ComputeSomething();
//   if (!r.ok()) { log(r.status()); return; }
//   int v = r.value();  // or *r, r->member, r.ValueOr(default)
//
// Design doc: section 5.1.
template <typename T>
class Result {
public:
    // Constructs a Result holding a value. The value is moved/copied in.
    // Implicit conversion from T is allowed for convenience.
    Result(T value) : data_(std::move(value)) {}

    // Constructs a Result holding an error Status. The Status must not be OK.
    // Implicit conversion from Status is allowed for convenience.
    Result(Status status) : data_(std::move(status)) {
        // A Result constructed from a Status must represent an error.
        assert(!std::get<Status>(data_).ok());
    }

    // Copy semantics (deleted if T is not copyable).
    Result(const Result&) = default;
    Result& operator=(const Result&) = default;

    // Move semantics.
    Result(Result&&) noexcept = default;
    Result& operator=(Result&&) noexcept = default;

    // Returns true if this Result holds a value (not an error).
    bool ok() const noexcept { return std::holds_alternative<T>(data_); }

    // Returns the error Status. Only meaningful when ok() is false.
    // If called when ok() is true, returns Status::Ok().
    const Status& status() const noexcept {
        if (auto* s = std::get_if<Status>(&data_)) {
            return *s;
        }
        static const Status kOkStatus = Status::Ok();
        return kOkStatus;
    }

    // Returns a reference to the contained value.
    // Behavior is undefined if ok() is false (asserts in debug builds).
    T& value() & noexcept {
        assert(ok());
        return std::get<T>(data_);
    }

    const T& value() const& noexcept {
        assert(ok());
        return std::get<T>(data_);
    }

    T&& value() && noexcept {
        assert(ok());
        return std::move(std::get<T>(data_));
    }

    // Returns a reference to the contained value, with explicit "unsafe"
    // semantics: no assertion, undefined behavior if not ok().
    // Provided for symmetry with absl::StatusOr and for use in contexts
    // where the caller has already verified ok().
    T& ValueOrDie() & noexcept { return value(); }
    const T& ValueOrDie() const& noexcept { return value(); }
    T&& ValueOrDie() && noexcept { return std::move(*this).value(); }

    // Returns the contained value if ok(), otherwise returns default_value.
    template <typename U>
    T ValueOr(U&& default_value) const& {
        if (ok()) {
            return value();
        }
        return static_cast<T>(std::forward<U>(default_value));
    }

    template <typename U>
    T ValueOr(U&& default_value) && {
        if (ok()) {
            return std::move(*this).value();
        }
        return static_cast<T>(std::forward<U>(default_value));
    }

    // Access the contained value via pointer semantics.
    // Behavior is undefined if ok() is false.
    T* operator->() noexcept {
        assert(ok());
        return &std::get<T>(data_);
    }

    const T* operator->() const noexcept {
        assert(ok());
        return &std::get<T>(data_);
    }

    T& operator*() & noexcept {
        assert(ok());
        return std::get<T>(data_);
    }

    const T& operator*() const& noexcept {
        assert(ok());
        return std::get<T>(data_);
    }

    T&& operator*() && noexcept {
        assert(ok());
        return std::move(std::get<T>(data_));
    }

private:
    std::variant<T, Status> data_;
};

// Specialization of Result for void. It wraps only a Status, representing
// either success (OK) or failure (error Status).
template <>
class Result<void> {
public:
    // Constructs an OK Result.
    Result() : status_(Status::Ok()) {}

    // Constructs a Result from a Status.
    Result(Status status) : status_(std::move(status)) {}

    // Copy and move semantics.
    Result(const Result&) = default;
    Result& operator=(const Result&) = default;
    Result(Result&&) noexcept = default;
    Result& operator=(Result&&) noexcept = default;

    // Returns true if the operation succeeded.
    bool ok() const noexcept { return status_.ok(); }

    // Returns the Status.
    const Status& status() const noexcept { return status_; }

private:
    Status status_;
};

}  // namespace mino

// MINO_ASSIGN_OR_RETURN(lhs, rexpr) evaluates rexpr, which must return a
// mino::Result<T>. If the Result is not ok(), its status is returned from
// the enclosing function (which must return Status or Result<...>).
// Otherwise, the contained value is assigned to lhs.
//
// Example:
//   MINO_ASSIGN_OR_RETURN(auto handle, channel.Reserve());
//   // use handle
//
// The temporary name is uniquified with __LINE__ via two-level indirection
// so that the line number macro expands before token pasting.
#define MINO_DETAIL_CONCAT_INNER_(a, b) a##b
#define MINO_DETAIL_CONCAT_(a, b) MINO_DETAIL_CONCAT_INNER_(a, b)
#define MINO_ASSIGN_OR_RETURN(lhs, rexpr)                                 \
    auto MINO_DETAIL_CONCAT_(_result_, __LINE__) = (rexpr);               \
    if (!MINO_DETAIL_CONCAT_(_result_, __LINE__).ok()) {                  \
        return MINO_DETAIL_CONCAT_(_result_, __LINE__).status();          \
    }                                                                     \
    lhs = std::move(MINO_DETAIL_CONCAT_(_result_, __LINE__)).value()

#endif  // MINO_COMMON_RESULT_H_
