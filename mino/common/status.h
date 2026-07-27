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

#ifndef MINO_COMMON_STATUS_H_
#define MINO_COMMON_STATUS_H_

#include <cstdint>
#include <string>
#include <string_view>

namespace mino {

// StatusCode enumerates all error codes used across Mino modules.
// The underlying type is uint16_t to keep the enum compact in wire formats
// and shared-memory layouts.
//
// Design doc: section 5.1.
enum class StatusCode : uint16_t {
    kOk = 0,
    kInvalidArgument = 1,
    kNotFound = 2,
    kAlreadyExists = 3,
    kResourceExhausted = 4,
    kWouldBlock = 5,
    kTimeout = 6,
    kSchemaMismatch = 7,
    kCorruption = 8,
    kUnavailable = 9,
    kPermissionDenied = 10,
    kUnsupported = 11,
    kDegraded = 12,  // Service continues with degraded guarantees.
    kInternal = 13,
};

// Status represents the outcome of an operation. It is either OK or an error
// with a code and an optional human-readable message.
//
// On hot paths (shared-memory data plane), prefer Status::Error(StatusCode)
// which avoids heap allocation for the message string. The message is
// intended for logging and debugging, not for programmatic inspection.
class Status {
public:
    // Constructs an OK status.
    Status() noexcept : code_(StatusCode::kOk) {}

    // Constructs a status with the given code and message.
    // The message is copied into an internal std::string.
    Status(StatusCode code, std::string_view message)
        : code_(code), message_(message) {}

    // Copy and move semantics.
    Status(const Status&) = default;
    Status(Status&&) noexcept = default;
    Status& operator=(const Status&) = default;
    Status& operator=(Status&&) noexcept = default;

    // Returns a Status representing success.
    static Status Ok() { return Status(); }

    // Returns an error Status with the given code and no message.
    // This overload avoids heap allocation and is suitable for hot paths.
    static Status Error(StatusCode code) { return Status(code, ""); }

    // Returns an error Status with the given code and message.
    static Status Error(StatusCode code, std::string_view message) {
        return Status(code, message);
    }

    // Returns true if the status is OK.
    bool ok() const noexcept { return code_ == StatusCode::kOk; }

    // Returns the error code.
    StatusCode code() const noexcept { return code_; }

    // Returns the human-readable message. May be empty.
    std::string_view message() const noexcept { return message_; }

    // Returns a string of the form "code: message" for logging.
    // Not intended for hot paths.
    std::string ToString() const;

    // Equality comparison: two Status objects are equal iff they have the
    // same code and message.
    friend bool operator==(const Status& lhs, const Status& rhs) {
        return lhs.code_ == rhs.code_ && lhs.message_ == rhs.message_;
    }

    friend bool operator!=(const Status& lhs, const Status& rhs) {
        return !(lhs == rhs);
    }

private:
    StatusCode code_;
    std::string message_;
};

}  // namespace mino

// MINO_RETURN_IF_ERROR evaluates expr, which must return a mino::Status.
// If the status is not OK, it is returned from the enclosing function.
//
// Example:
//   MINO_RETURN_IF_ERROR(channel.Commit(...));
//   // continue on success
#define MINO_RETURN_IF_ERROR(expr)                          \
    do {                                                    \
        const ::mino::Status _status = (expr);              \
        if (!_status.ok()) {                                \
            return _status;                                 \
        }                                                   \
    } while (false)

#endif  // MINO_COMMON_STATUS_H_
