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

#include "mino/common/result.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>

#include "mino/common/status.h"

namespace mino {
namespace {

// ---------------------------------------------------------------------------
// Value construction
// ---------------------------------------------------------------------------

TEST(ResultTest, ValueConstruction) {
    Result<int> r = 42;
    EXPECT_TRUE(r.ok());
    EXPECT_EQ(r.value(), 42);
}

TEST(ResultTest, StringValue) {
    Result<std::string> r = std::string("hello");
    EXPECT_TRUE(r.ok());
    EXPECT_EQ(r.value(), "hello");
}

TEST(ResultTest, UniquePtrValue) {
    auto ptr = std::make_unique<int>(99);
    Result<std::unique_ptr<int>> r = std::move(ptr);
    EXPECT_TRUE(r.ok());
    EXPECT_EQ(**r, 99);
}

// ---------------------------------------------------------------------------
// Error construction
// ---------------------------------------------------------------------------

TEST(ResultTest, ErrorConstruction) {
    Result<int> r = Status::Error(StatusCode::kNotFound, "missing");
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.status().code(), StatusCode::kNotFound);
    EXPECT_EQ(r.status().message(), "missing");
}

TEST(ResultTest, ErrorFromCodeOnly) {
    Result<int> r = Status::Error(StatusCode::kInternal);
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.status().code(), StatusCode::kInternal);
    EXPECT_TRUE(r.status().message().empty());
}

// ---------------------------------------------------------------------------
// ok() and status()
// ---------------------------------------------------------------------------

TEST(ResultTest, OkResultHasOkStatus) {
    Result<int> r = 7;
    EXPECT_TRUE(r.ok());
    EXPECT_TRUE(r.status().ok());
}

// ---------------------------------------------------------------------------
// value() access
// ---------------------------------------------------------------------------

TEST(ResultTest, ValueLValue) {
    Result<int> r = 10;
    int& v = r.value();
    EXPECT_EQ(v, 10);
    v = 20;
    EXPECT_EQ(r.value(), 20);
}

TEST(ResultTest, ValueConstLValue) {
    const Result<int> r = 30;
    const int& v = r.value();
    EXPECT_EQ(v, 30);
}

TEST(ResultTest, ValueRValue) {
    Result<std::string> r = std::string("move me");
    std::string s = std::move(r).value();
    EXPECT_EQ(s, "move me");
}

TEST(ResultTest, ValueOrDie) {
    Result<int> r = 55;
    EXPECT_EQ(r.ValueOrDie(), 55);

    const Result<int> cr = 66;
    EXPECT_EQ(cr.ValueOrDie(), 66);
}

// ---------------------------------------------------------------------------
// operator-> and operator*
// ---------------------------------------------------------------------------

TEST(ResultTest, ArrowOperator) {
    Result<std::string> r = std::string("abc");
    EXPECT_EQ(r->size(), 3u);
    EXPECT_EQ(r->at(0), 'a');
}

TEST(ResultTest, ConstArrowOperator) {
    const Result<std::string> r = std::string("xyz");
    EXPECT_EQ(r->size(), 3u);
}

TEST(ResultTest, DereferenceOperator) {
    Result<int> r = 77;
    EXPECT_EQ(*r, 77);
    *r = 88;
    EXPECT_EQ(*r, 88);
}

TEST(ResultTest, DereferenceUniquePtr) {
    auto ptr = std::make_unique<std::string>("deep");
    Result<std::unique_ptr<std::string>> r = std::move(ptr);
    EXPECT_EQ(**r, "deep");
    EXPECT_EQ((*r)->size(), 4u);
}

// ---------------------------------------------------------------------------
// ValueOr
// ---------------------------------------------------------------------------

TEST(ResultTest, ValueOrOnOk) {
    Result<int> r = 5;
    EXPECT_EQ(r.ValueOr(100), 5);
}

TEST(ResultTest, ValueOrOnError) {
    Result<int> r = Status::Error(StatusCode::kTimeout);
    EXPECT_EQ(r.ValueOr(100), 100);
}

TEST(ResultTest, ValueOrMove) {
    Result<std::string> r = Status::Error(StatusCode::kInternal);
    std::string s = std::move(r).ValueOr(std::string("fallback"));
    EXPECT_EQ(s, "fallback");
}

// ---------------------------------------------------------------------------
// Copy semantics
// ---------------------------------------------------------------------------

TEST(ResultTest, CopyValue) {
    Result<int> original = 42;
    Result<int> copy = original;
    EXPECT_TRUE(copy.ok());
    EXPECT_EQ(copy.value(), 42);
}

TEST(ResultTest, CopyError) {
    Result<int> original = Status::Error(StatusCode::kCorruption, "bad");
    Result<int> copy = original;
    EXPECT_FALSE(copy.ok());
    EXPECT_EQ(copy.status().code(), StatusCode::kCorruption);
    EXPECT_EQ(copy.status().message(), "bad");
}

// ---------------------------------------------------------------------------
// Move semantics
// ---------------------------------------------------------------------------

TEST(ResultTest, MoveValue) {
    Result<std::string> original = std::string("moved");
    Result<std::string> moved = std::move(original);
    EXPECT_TRUE(moved.ok());
    EXPECT_EQ(moved.value(), "moved");
}

TEST(ResultTest, MoveUniquePtr) {
    auto ptr = std::make_unique<int>(123);
    Result<std::unique_ptr<int>> original = std::move(ptr);
    Result<std::unique_ptr<int>> moved = std::move(original);
    EXPECT_TRUE(moved.ok());
    EXPECT_EQ(**moved, 123);
}

TEST(ResultTest, MoveError) {
    Result<int> original = Status::Error(StatusCode::kUnavailable, "gone");
    Result<int> moved = std::move(original);
    EXPECT_FALSE(moved.ok());
    EXPECT_EQ(moved.status().code(), StatusCode::kUnavailable);
}

// ---------------------------------------------------------------------------
// Result<void>
// ---------------------------------------------------------------------------

TEST(ResultVoidTest, DefaultIsOk) {
    Result<void> r;
    EXPECT_TRUE(r.ok());
    EXPECT_TRUE(r.status().ok());
}

TEST(ResultVoidTest, ErrorResult) {
    Result<void> r = Status::Error(StatusCode::kPermissionDenied, "denied");
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.status().code(), StatusCode::kPermissionDenied);
    EXPECT_EQ(r.status().message(), "denied");
}

TEST(ResultVoidTest, OkFromStatus) {
    Result<void> r = Status::Ok();
    EXPECT_TRUE(r.ok());
}

TEST(ResultVoidTest, CopyAndMove) {
    Result<void> original = Status::Error(StatusCode::kTimeout, "t");
    Result<void> copy = original;
    EXPECT_FALSE(copy.ok());
    EXPECT_EQ(copy.status().code(), StatusCode::kTimeout);

    Result<void> moved = std::move(original);
    EXPECT_FALSE(moved.ok());
    EXPECT_EQ(moved.status().code(), StatusCode::kTimeout);
}

// ---------------------------------------------------------------------------
// MINO_ASSIGN_OR_RETURN macro
// ---------------------------------------------------------------------------

Status AssignOrReturnHelper(bool should_fail) {
    MINO_ASSIGN_OR_RETURN(auto val, should_fail
                                        ? Result<int>(Status::Error(
                                              StatusCode::kInternal, "fail"))
                                        : Result<int>(42));
    // On success, val should be 42.
    EXPECT_EQ(val, 42);
    return Status::Ok();
}

TEST(ResultTest, AssignOrReturnSuccess) {
    Status s = AssignOrReturnHelper(false);
    EXPECT_TRUE(s.ok());
}

TEST(ResultTest, AssignOrReturnPropagatesError) {
    Status s = AssignOrReturnHelper(true);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), StatusCode::kInternal);
    EXPECT_EQ(s.message(), "fail");
}

// Test with unique_ptr to verify move-only types work with the macro.
Status AssignOrReturnUniquePtrHelper(bool should_fail) {
    MINO_ASSIGN_OR_RETURN(
        auto ptr,
        should_fail
            ? Result<std::unique_ptr<int>>(
                  Status::Error(StatusCode::kResourceExhausted, "full"))
            : Result<std::unique_ptr<int>>(std::make_unique<int>(77)));
    EXPECT_EQ(*ptr, 77);
    return Status::Ok();
}

TEST(ResultTest, AssignOrReturnUniquePtrSuccess) {
    Status s = AssignOrReturnUniquePtrHelper(false);
    EXPECT_TRUE(s.ok());
}

TEST(ResultTest, AssignOrReturnUniquePtrError) {
    Status s = AssignOrReturnUniquePtrHelper(true);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), StatusCode::kResourceExhausted);
}

// ---------------------------------------------------------------------------
// Chained usage pattern (common in Mino APIs)
// ---------------------------------------------------------------------------

Result<int> Divide(int a, int b) {
    if (b == 0) {
        return Status::Error(StatusCode::kInvalidArgument, "division by zero");
    }
    return a / b;
}

TEST(ResultTest, ChainedUsageSuccess) {
    Result<int> r = Divide(10, 2);
    EXPECT_TRUE(r.ok());
    EXPECT_EQ(r.value(), 5);
}

TEST(ResultTest, ChainedUsageError) {
    Result<int> r = Divide(10, 0);
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.status().code(), StatusCode::kInvalidArgument);
}

// ---------------------------------------------------------------------------
// Struct value type
// ---------------------------------------------------------------------------

struct Point {
    int x;
    int y;
};

TEST(ResultTest, StructValue) {
    Result<Point> r = Point{1, 2};
    EXPECT_TRUE(r.ok());
    EXPECT_EQ(r->x, 1);
    EXPECT_EQ(r->y, 2);
    EXPECT_EQ((*r).x, 1);
}

}  // namespace
}  // namespace mino
