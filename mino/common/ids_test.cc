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

#include "mino/common/ids.h"

#include <gtest/gtest.h>

#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

namespace mino {
namespace {

// ---------------------------------------------------------------------------
// Compile-time properties
// ---------------------------------------------------------------------------

TEST(IdsTest, UnderlyingWidths) {
    static_assert(sizeof(TopicId) == sizeof(uint32_t));
    static_assert(sizeof(TypeId) == sizeof(uint32_t));
    static_assert(sizeof(NodeId) == sizeof(uint64_t));
    static_assert(sizeof(PublisherId) == sizeof(uint64_t));
    static_assert(sizeof(SubscriberId) == sizeof(uint32_t));
    static_assert(sizeof(SchemaId) == sizeof(uint64_t));
}

TEST(IdsTest, IsAggregateAndTrivial) {
    // Aggregate initialization must work and the types must stay POD so they
    // can be embedded in shared-memory layouts.
    [[maybe_unused]] TopicId t{7};
    [[maybe_unused]] NodeId n{9};
    EXPECT_TRUE(std::is_aggregate_v<TopicId>);
    EXPECT_TRUE(std::is_trivially_copyable_v<NodeId>);
    EXPECT_TRUE(std::is_standard_layout_v<SchemaId>);
}

TEST(IdsTest, NoImplicitConversionFromInt) {
    // Each ID must be explicitly constructible from its integer type only;
    // there is no implicit conversion.
    static_assert(!std::is_convertible_v<uint32_t, TopicId>);
    static_assert(!std::is_convertible_v<int, TopicId>);
    static_assert(std::is_constructible_v<TopicId, uint32_t>);

    // Different ID types are distinct types and not inter-convertible.
    static_assert(!std::is_convertible_v<TopicId, TypeId>);
    static_assert(!std::is_same_v<TopicId, TypeId>);
    static_assert(!std::is_same_v<NodeId, PublisherId>);  // both uint64_t
}

TEST(IdsTest, DistinctTypesWithSameUnderlyingAreNotEqual) {
    // NodeId and PublisherId share uint64_t but must not be comparable to
    // each other (no operator== across types). This is enforced by the type
    // system; verify the types differ.
    EXPECT_FALSE((std::is_same_v<NodeId, PublisherId>));
    EXPECT_TRUE((std::is_same_v<decltype(NodeId{}.value),
                                decltype(PublisherId{}.value)>));
}

// ---------------------------------------------------------------------------
// Default construction
// ---------------------------------------------------------------------------

TEST(IdsTest, DefaultConstructedIsZero) {
    EXPECT_EQ(TopicId{}.value, 0u);
    EXPECT_EQ(TypeId{}.value, 0u);
    EXPECT_EQ(NodeId{}.value, 0u);
    EXPECT_EQ(PublisherId{}.value, 0u);
    EXPECT_EQ(SubscriberId{}.value, 0u);
    EXPECT_EQ(SchemaId{}.value, 0u);
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

TEST(IdsTest, Equality) {
    TopicId a{1};
    TopicId b{1};
    TopicId c{2};
    EXPECT_EQ(a, b);
    EXPECT_NE(a, c);
    EXPECT_FALSE(a != b);
}

TEST(IdsTest, LessThan) {
    NodeId small{10};
    NodeId large{20};
    EXPECT_LT(small, large);
    EXPECT_FALSE(large < small);
    EXPECT_FALSE(small < small);
}

TEST(IdsTest, ComparisonIsConstexpr) {
    constexpr TopicId a{5};
    constexpr TopicId b{5};
    constexpr TopicId c{6};
    static_assert(a == b);
    static_assert(a != c);
    static_assert(a < c);
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

TEST(IdsTest, ToStringFormatsWithTypeName) {
    EXPECT_EQ(TopicId{42}.ToString(), "TopicId(42)");
    EXPECT_EQ(NodeId{7}.ToString(), "NodeId(7)");
    EXPECT_EQ(PublisherId{10000000000ull}.ToString(),
              "PublisherId(10000000000)");
    EXPECT_EQ(SchemaId{0}.ToString(), "SchemaId(0)");
}

// ---------------------------------------------------------------------------
// Hash usage
// ---------------------------------------------------------------------------

TEST(IdsTest, UsableAsUnorderedKeys) {
    std::unordered_map<TopicId, int> by_topic;
    by_topic[TopicId{1}] = 10;
    by_topic[TopicId{2}] = 20;
    EXPECT_EQ(by_topic.at(TopicId{1}), 10);
    EXPECT_EQ(by_topic.at(TopicId{2}), 20);

    std::unordered_set<NodeId> nodes;
    nodes.insert(NodeId{5});
    nodes.insert(NodeId{5});
    nodes.insert(NodeId{6});
    EXPECT_EQ(nodes.size(), 2u);
}

}  // namespace
}  // namespace mino
