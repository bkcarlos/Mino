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

#include "mino/common/status.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

namespace mino {
namespace {

// ---------------------------------------------------------------------------
// Default construction and Ok()
// ---------------------------------------------------------------------------

TEST(StatusTest, DefaultConstructorIsOk) {
    Status s;
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(s.code(), StatusCode::kOk);
    EXPECT_TRUE(s.message().empty());
}

TEST(StatusTest, OkFactoryReturnsOk) {
    Status s = Status::Ok();
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(s.code(), StatusCode::kOk);
    EXPECT_TRUE(s.message().empty());
}

// ---------------------------------------------------------------------------
// Error construction
// ---------------------------------------------------------------------------

TEST(StatusTest, ErrorWithCodeOnly) {
    Status s = Status::Error(StatusCode::kNotFound);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), StatusCode::kNotFound);
    EXPECT_TRUE(s.message().empty());
}

TEST(StatusTest, ErrorWithCodeAndMessage) {
    Status s = Status::Error(StatusCode::kInvalidArgument, "bad input");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(s.message(), "bad input");
}

TEST(StatusTest, ConstructorWithMessage) {
    Status s(StatusCode::kTimeout, "deadline exceeded");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), StatusCode::kTimeout);
    EXPECT_EQ(s.message(), "deadline exceeded");
}

// ---------------------------------------------------------------------------
// All error codes
// ---------------------------------------------------------------------------

TEST(StatusTest, AllErrorCodesAreNotOk) {
    constexpr StatusCode kAllCodes[] = {
        StatusCode::kInvalidArgument,
        StatusCode::kNotFound,
        StatusCode::kAlreadyExists,
        StatusCode::kResourceExhausted,
        StatusCode::kWouldBlock,
        StatusCode::kTimeout,
        StatusCode::kSchemaMismatch,
        StatusCode::kCorruption,
        StatusCode::kUnavailable,
        StatusCode::kPermissionDenied,
        StatusCode::kUnsupported,
        StatusCode::kDegraded,
        StatusCode::kInternal,
    };

    for (StatusCode code : kAllCodes) {
        Status s = Status::Error(code);
        EXPECT_FALSE(s.ok()) << "code=" << static_cast<int>(code);
        EXPECT_EQ(s.code(), code);
    }
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

TEST(StatusTest, EqualitySameCodeAndMessage) {
    Status a = Status::Error(StatusCode::kNotFound, "missing");
    Status b = Status::Error(StatusCode::kNotFound, "missing");
    EXPECT_EQ(a, b);
    EXPECT_FALSE(a != b);
}

TEST(StatusTest, InequalityDifferentCode) {
    Status a = Status::Error(StatusCode::kNotFound);
    Status b = Status::Error(StatusCode::kInternal);
    EXPECT_NE(a, b);
}

TEST(StatusTest, InequalityDifferentMessage) {
    Status a = Status::Error(StatusCode::kNotFound, "foo");
    Status b = Status::Error(StatusCode::kNotFound, "bar");
    EXPECT_NE(a, b);
}

TEST(StatusTest, OkStatusesAreEqual) {
    Status a;
    Status b = Status::Ok();
    EXPECT_EQ(a, b);
}

// ---------------------------------------------------------------------------
// ToString
// ---------------------------------------------------------------------------

TEST(StatusTest, ToStringOk) {
    Status s;
    EXPECT_EQ(s.ToString(), "0: OK");
}

TEST(StatusTest, ToStringErrorWithMessage) {
    Status s = Status::Error(StatusCode::kInvalidArgument, "bad input");
    EXPECT_EQ(s.ToString(), "1: bad input");
}

TEST(StatusTest, ToStringErrorWithoutMessage) {
    Status s = Status::Error(StatusCode::kInternal);
    EXPECT_EQ(s.ToString(), "13: Internal error");
}

// ---------------------------------------------------------------------------
// Copy and move
// ---------------------------------------------------------------------------

TEST(StatusTest, CopyPreservesState) {
    Status original = Status::Error(StatusCode::kCorruption, "bad crc");
    Status copy = original;
    EXPECT_EQ(copy.code(), StatusCode::kCorruption);
    EXPECT_EQ(copy.message(), "bad crc");
    EXPECT_EQ(original, copy);
}

TEST(StatusTest, MovePreservesState) {
    Status original = Status::Error(StatusCode::kUnavailable, "try later");
    Status moved = std::move(original);
    EXPECT_EQ(moved.code(), StatusCode::kUnavailable);
    EXPECT_EQ(moved.message(), "try later");
}

// ---------------------------------------------------------------------------
// MINO_RETURN_IF_ERROR macro
// ---------------------------------------------------------------------------

Status ReturnIfErrorHelper(bool should_fail) {
    MINO_RETURN_IF_ERROR(should_fail
                             ? Status::Error(StatusCode::kInternal, "fail")
                             : Status::Ok());
    return Status::Ok();
}

TEST(StatusTest, ReturnIfErrorPropagatesError) {
    Status s = ReturnIfErrorHelper(true);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), StatusCode::kInternal);
}

TEST(StatusTest, ReturnIfErrorPassesThroughOnOk) {
    Status s = ReturnIfErrorHelper(false);
    EXPECT_TRUE(s.ok());
}

// ---------------------------------------------------------------------------
// Message storage (string_view input, std::string storage)
// ---------------------------------------------------------------------------

TEST(StatusTest, MessageFromStringView) {
    std::string_view sv = "transient message";
    Status s = Status::Error(StatusCode::kDegraded, sv);
    EXPECT_EQ(s.message(), sv);
}

TEST(StatusTest, MessageFromStdString) {
    std::string msg = "heap-allocated message";
    Status s = Status::Error(StatusCode::kSchemaMismatch, msg);
    EXPECT_EQ(s.message(), msg);
}

TEST(StatusTest, EmptyMessageIsEmpty) {
    Status s = Status::Error(StatusCode::kWouldBlock);
    EXPECT_TRUE(s.message().empty());
    EXPECT_EQ(s.message().size(), 0u);
}

}  // namespace
}  // namespace mino
