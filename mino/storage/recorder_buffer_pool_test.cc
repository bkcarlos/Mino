// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/recorder_buffer_pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <utility>
#include <vector>

namespace mino::storage {
namespace {

using namespace std::chrono_literals;

BufferReservationRequest Request(
    TopicId topic_id, size_t payload_size, uint64_t user_tag,
    BufferFullPolicy policy = BufferFullPolicy::kBlock,
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds::max()) {
    return BufferReservationRequest{topic_id, payload_size, user_tag, policy,
                                    timeout};
}

std::unique_ptr<RecorderBufferPool> NewPool(
    const RecorderBufferPoolOptions& options) {
    auto created = RecorderBufferPool::Create(options);
    EXPECT_TRUE(created.ok()) << created.status().ToString();
    return created.ok() ? std::move(*created) : nullptr;
}

void CommitAccepted(BufferReserveResult* result) {
    ASSERT_NE(result, nullptr);
    ASSERT_TRUE(result->accepted());
    ASSERT_TRUE(result->reservation.active());
    const Status status = std::move(result->reservation).Commit();
    ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(RecorderBufferPoolTest, ValidatesOptionsAndUsesBoundedSizeClasses) {
    RecorderBufferPoolOptions invalid;
    invalid.global_byte_limit = 0;
    EXPECT_EQ(RecorderBufferPool::Create(invalid).status().code(),
              StatusCode::kInvalidArgument);

    invalid = {};
    invalid.low_watermark_bytes = 10;
    invalid.high_watermark_bytes = 10;
    EXPECT_EQ(RecorderBufferPool::Create(invalid).status().code(),
              StatusCode::kInvalidArgument);

    RecorderBufferPoolOptions options;
    options.global_byte_limit = 4u * 1024u * 1024u;
    options.default_topic_byte_limit = options.global_byte_limit;
    options.queue_capacity = 8;
    options.max_large_object_bytes = 2u * 1024u * 1024u;
    auto pool = NewPool(options);
    ASSERT_NE(pool, nullptr);

    auto small = pool->Reserve(Request(TopicId{1}, 1, 1));
    auto medium = pool->Reserve(Request(TopicId{1}, 4097, 2));
    auto large = pool->Reserve(Request(TopicId{1}, 65537, 3));
    auto oversized_class =
        pool->Reserve(Request(TopicId{1}, 1024u * 1024u + 1, 4));
    ASSERT_TRUE(small.ok() && medium.ok() && large.ok() &&
                oversized_class.ok());
    ASSERT_TRUE(small->accepted() && medium->accepted() && large->accepted() &&
                oversized_class->accepted());
    EXPECT_EQ(small->reservation.capacity(), 4u * 1024u);
    EXPECT_EQ(medium->reservation.capacity(), 64u * 1024u);
    EXPECT_EQ(large->reservation.capacity(), 1024u * 1024u);
    EXPECT_EQ(oversized_class->reservation.capacity(),
              1024u * 1024u + 4u * 1024u);
    EXPECT_EQ(small->reservation.bytes().size(), 1u);

    const RecorderBufferPoolStats held = pool->stats();
    EXPECT_EQ(held.reserved_records, 4u);
    EXPECT_EQ(held.bytes_in_use,
              4u * 1024u + 64u * 1024u + 1024u * 1024u +
                  1024u * 1024u + 4u * 1024u);
    EXPECT_LE(held.allocated_bytes, options.global_byte_limit);

    EXPECT_TRUE(small->reservation.Cancel().has_value());
    EXPECT_TRUE(medium->reservation.Cancel().has_value());
    EXPECT_TRUE(large->reservation.Cancel().has_value());
    EXPECT_TRUE(oversized_class->reservation.Cancel().has_value());
    EXPECT_EQ(pool->stats().bytes_in_use, 0u);
    EXPECT_EQ(pool->stats().cancelled_reservations, 4u);

    auto too_large = pool->Reserve(
        Request(TopicId{1}, options.max_large_object_bytes + 1, 5));
    ASSERT_FALSE(too_large.ok());
    EXPECT_EQ(too_large.status().code(), StatusCode::kResourceExhausted);
}

TEST(RecorderBufferPoolTest, EnforcesTopicGlobalAndQueueCapacity) {
    RecorderBufferPoolOptions options;
    options.global_byte_limit = 8u * 1024u;
    options.default_topic_byte_limit = 8u * 1024u;
    options.topic_byte_limits.emplace(TopicId{1}, 4u * 1024u);
    options.queue_capacity = 2;
    auto pool = NewPool(options);
    ASSERT_NE(pool, nullptr);

    auto topic_one = pool->Reserve(Request(TopicId{1}, 1, 10));
    ASSERT_TRUE(topic_one.ok());
    CommitAccepted(&*topic_one);

    auto topic_drop = pool->Reserve(Request(
        TopicId{1}, 1, 11, BufferFullPolicy::kDropNewest));
    ASSERT_TRUE(topic_drop.ok());
    EXPECT_EQ(topic_drop->admission, BufferAdmission::kDroppedNewest);
    ASSERT_EQ(topic_drop->discarded.size(), 1u);
    EXPECT_EQ(topic_drop->discarded[0].reason,
              BufferDiscardReason::kDropNewest);
    EXPECT_EQ(topic_drop->discarded[0].user_tag, 11u);

    auto topic_two = pool->Reserve(Request(TopicId{2}, 1, 20));
    ASSERT_TRUE(topic_two.ok());
    CommitAccepted(&*topic_two);
    EXPECT_EQ(pool->stats().bytes_in_use, 8u * 1024u);
    EXPECT_EQ(pool->TopicBytesInUse(TopicId{1}), 4u * 1024u);
    EXPECT_EQ(pool->TopicBytesInUse(TopicId{2}), 4u * 1024u);

    auto globally_full = pool->Reserve(Request(
        TopicId{3}, 1, 30, BufferFullPolicy::kDropNewest));
    ASSERT_TRUE(globally_full.ok());
    EXPECT_EQ(globally_full->admission, BufferAdmission::kDroppedNewest);

    auto first = pool->TryDequeue();
    ASSERT_TRUE(first.ok());
    EXPECT_EQ(first->user_tag(), 10u);
    // Dequeue frees the queue slot, but the consumer's RAII handle continues
    // to own and charge the bytes until it is released.
    EXPECT_EQ(pool->TopicBytesInUse(TopicId{1}), 4u * 1024u);
    auto held_by_consumer = pool->Reserve(Request(
        TopicId{1}, 1, 12, BufferFullPolicy::kDropNewest));
    ASSERT_TRUE(held_by_consumer.ok());
    EXPECT_EQ(held_by_consumer->admission, BufferAdmission::kDroppedNewest);
    first->Reset();
    EXPECT_EQ(pool->TopicBytesInUse(TopicId{1}), 0u);

    // One byte class is now available globally, but the remaining committed
    // record still occupies the final queue slot after this reservation.
    auto queue_slot = pool->Reserve(Request(TopicId{3}, 1, 31));
    ASSERT_TRUE(queue_slot.ok());
    CommitAccepted(&*queue_slot);
    auto queue_full = pool->Reserve(Request(
        TopicId{4}, 0, 40, BufferFullPolicy::kDropNewest));
    ASSERT_TRUE(queue_full.ok());
    EXPECT_EQ(queue_full->admission, BufferAdmission::kDroppedNewest);
    EXPECT_EQ(queue_full->discarded[0].charged_bytes, 0u);
}

TEST(RecorderBufferPoolTest, DropOldestReturnsEveryDiscardedRecord) {
    RecorderBufferPoolOptions options;
    options.global_byte_limit = 12u * 1024u;
    options.default_topic_byte_limit = 12u * 1024u;
    options.topic_byte_limits.emplace(TopicId{1}, 4u * 1024u);
    options.queue_capacity = 3;
    auto pool = NewPool(options);
    ASSERT_NE(pool, nullptr);

    auto other_topic = pool->Reserve(Request(TopicId{2}, 1, 100));
    ASSERT_TRUE(other_topic.ok());
    CommitAccepted(&*other_topic);
    auto same_topic = pool->Reserve(Request(TopicId{1}, 1, 101));
    ASSERT_TRUE(same_topic.ok());
    CommitAccepted(&*same_topic);

    // Satisfying Topic 1's limit requires dropping through the global oldest
    // prefix: Topic 2's record and then Topic 1's old record.
    auto replacement = pool->Reserve(Request(
        TopicId{1}, 1, 102, BufferFullPolicy::kDropOldest));
    ASSERT_TRUE(replacement.ok()) << replacement.status().ToString();
    ASSERT_TRUE(replacement->accepted());
    ASSERT_EQ(replacement->discarded.size(), 2u);
    EXPECT_EQ(replacement->discarded[0].reason,
              BufferDiscardReason::kDropOldest);
    EXPECT_EQ(replacement->discarded[0].user_tag, 100u);
    EXPECT_EQ(replacement->discarded[1].user_tag, 101u);
    CommitAccepted(&*replacement);

    auto remaining = pool->TryDequeue();
    ASSERT_TRUE(remaining.ok());
    EXPECT_EQ(remaining->user_tag(), 102u);
    EXPECT_EQ(pool->stats().dropped_oldest_records, 2u);
    EXPECT_EQ(pool->TryDequeue().status().code(), StatusCode::kWouldBlock);
}

TEST(RecorderBufferPoolTest, DropOldestReportsFallbackWhenNothingIsDroppable) {
    RecorderBufferPoolOptions options;
    options.global_byte_limit = 4u * 1024u;
    options.default_topic_byte_limit = 4u * 1024u;
    options.queue_capacity = 2;
    auto pool = NewPool(options);
    ASSERT_NE(pool, nullptr);

    // The limiting buffer is still producer-owned, so kDropOldest must not
    // silently steal it or discard unrelated committed data.
    auto held = pool->Reserve(Request(TopicId{1}, 1, 1));
    ASSERT_TRUE(held.ok());
    auto fallback = pool->Reserve(Request(
        TopicId{2}, 1, 2, BufferFullPolicy::kDropOldest));
    ASSERT_TRUE(fallback.ok());
    EXPECT_EQ(fallback->admission, BufferAdmission::kDroppedNewest);
    ASSERT_EQ(fallback->discarded.size(), 1u);
    EXPECT_EQ(fallback->discarded[0].reason,
              BufferDiscardReason::kDropNewestNoDroppableOldest);
    EXPECT_EQ(fallback->discarded[0].user_tag, 2u);
}

TEST(RecorderBufferPoolTest, FailRecordingIsStickyAndDrainable) {
    RecorderBufferPoolOptions options;
    options.global_byte_limit = 4u * 1024u;
    options.default_topic_byte_limit = 4u * 1024u;
    options.queue_capacity = 1;
    auto pool = NewPool(options);
    ASSERT_NE(pool, nullptr);

    auto first = pool->Reserve(Request(TopicId{1}, 1, 1));
    ASSERT_TRUE(first.ok());
    CommitAccepted(&*first);

    auto failed = pool->Reserve(Request(
        TopicId{1}, 1, 2, BufferFullPolicy::kFailRecording));
    ASSERT_TRUE(failed.ok());
    EXPECT_EQ(failed->admission, BufferAdmission::kRecordingFailed);
    ASSERT_EQ(failed->discarded.size(), 1u);
    EXPECT_EQ(failed->discarded[0].reason,
              BufferDiscardReason::kFailRecording);
    EXPECT_TRUE(pool->stats().recording_failed);
    EXPECT_EQ(pool->stats().recording_failures, 1u);

    auto after_failure = pool->Reserve(Request(TopicId{1}, 0, 3));
    ASSERT_FALSE(after_failure.ok());
    EXPECT_EQ(after_failure.status().code(), StatusCode::kUnavailable);

    auto drain = pool->TryDequeue();
    ASSERT_TRUE(drain.ok());
    EXPECT_EQ(drain->user_tag(), 1u);
    drain->Reset();
    EXPECT_EQ(pool->TryDequeue().status().code(), StatusCode::kUnavailable);
}

TEST(RecorderBufferPoolTest, BlockTimesOutOnTopicBudget) {
    RecorderBufferPoolOptions options;
    options.global_byte_limit = 8u * 1024u;
    options.default_topic_byte_limit = 4u * 1024u;
    options.queue_capacity = 2;
    auto pool = NewPool(options);
    ASSERT_NE(pool, nullptr);

    auto first = pool->Reserve(Request(TopicId{1}, 1, 1));
    ASSERT_TRUE(first.ok());
    CommitAccepted(&*first);

    const auto before = std::chrono::steady_clock::now();
    auto timed_out = pool->Reserve(
        Request(TopicId{1}, 1, 2, BufferFullPolicy::kBlock, 25ms));
    const auto elapsed = std::chrono::steady_clock::now() - before;
    ASSERT_FALSE(timed_out.ok());
    EXPECT_EQ(timed_out.status().code(), StatusCode::kTimeout);
    EXPECT_GE(elapsed, 15ms);
    EXPECT_EQ(pool->stats().block_timeouts, 1u);
}

TEST(RecorderBufferPoolTest, BlockWakesWhenConsumerOpensCapacity) {
    RecorderBufferPoolOptions options;
    options.global_byte_limit = 8u * 1024u;
    options.default_topic_byte_limit = 8u * 1024u;
    options.queue_capacity = 1;
    auto pool = NewPool(options);
    ASSERT_NE(pool, nullptr);

    auto first = pool->Reserve(Request(TopicId{1}, 1, 1));
    ASSERT_TRUE(first.ok());
    CommitAccepted(&*first);

    std::atomic<bool> started{false};
    std::atomic<bool> committed{false};
    std::thread producer([&] {
        started.store(true, std::memory_order_release);
        auto second = pool->Reserve(
            Request(TopicId{1}, 1, 2, BufferFullPolicy::kBlock, 2s));
        if (!second.ok() || !second->accepted()) return;
        committed.store(std::move(second->reservation).Commit().ok(),
                        std::memory_order_release);
    });
    while (!started.load(std::memory_order_acquire)) std::this_thread::yield();
    std::this_thread::sleep_for(20ms);
    EXPECT_FALSE(committed.load(std::memory_order_acquire));

    auto old = pool->Dequeue(100ms);
    ASSERT_TRUE(old.ok());
    EXPECT_EQ(old->user_tag(), 1u);
    producer.join();
    EXPECT_TRUE(committed.load(std::memory_order_acquire));
    old->Reset();

    auto next = pool->Dequeue(100ms);
    ASSERT_TRUE(next.ok());
    EXPECT_EQ(next->user_tag(), 2u);
}

TEST(RecorderBufferPoolTest, CloseWakesBlockedProducerAndConsumer) {
    RecorderBufferPoolOptions options;
    options.global_byte_limit = 4u * 1024u;
    options.default_topic_byte_limit = 4u * 1024u;
    options.queue_capacity = 1;
    auto pool = NewPool(options);
    ASSERT_NE(pool, nullptr);

    auto first = pool->Reserve(Request(TopicId{1}, 1, 1));
    ASSERT_TRUE(first.ok());
    CommitAccepted(&*first);

    std::atomic<bool> producer_started{false};
    std::atomic<StatusCode> producer_status{StatusCode::kOk};
    std::thread producer([&] {
        producer_started.store(true, std::memory_order_release);
        auto blocked = pool->Reserve(Request(TopicId{1}, 1, 2));
        producer_status.store(blocked.ok() ? StatusCode::kOk
                                           : blocked.status().code(),
                              std::memory_order_release);
    });
    while (!producer_started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(20ms);
    pool->Close();
    producer.join();
    EXPECT_EQ(producer_status.load(std::memory_order_acquire),
              StatusCode::kUnavailable);

    auto drain = pool->TryDequeue();
    ASSERT_TRUE(drain.ok());
    drain->Reset();
    EXPECT_EQ(pool->TryDequeue().status().code(), StatusCode::kUnavailable);

    auto empty_pool = NewPool(options);
    ASSERT_NE(empty_pool, nullptr);
    std::atomic<bool> consumer_started{false};
    std::atomic<StatusCode> consumer_status{StatusCode::kOk};
    std::thread consumer([&] {
        consumer_started.store(true, std::memory_order_release);
        auto record = empty_pool->Dequeue();
        consumer_status.store(record.ok() ? StatusCode::kOk
                                           : record.status().code(),
                              std::memory_order_release);
    });
    while (!consumer_started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(20ms);
    empty_pool->Close();
    consumer.join();
    EXPECT_EQ(consumer_status.load(std::memory_order_acquire),
              StatusCode::kUnavailable);
}

TEST(RecorderBufferPoolTest, CommitAfterCloseExplicitlyFailsAndReleases) {
    RecorderBufferPoolOptions options;
    options.global_byte_limit = 4u * 1024u;
    options.default_topic_byte_limit = 4u * 1024u;
    options.queue_capacity = 1;
    auto pool = NewPool(options);
    ASSERT_NE(pool, nullptr);

    auto reserved = pool->Reserve(Request(TopicId{1}, 1, 7));
    ASSERT_TRUE(reserved.ok());
    ASSERT_TRUE(reserved->accepted());
    pool->Close();
    const Status commit = std::move(reserved->reservation).Commit();
    EXPECT_EQ(commit.code(), StatusCode::kUnavailable);
    EXPECT_EQ(pool->stats().bytes_in_use, 0u);
    EXPECT_EQ(pool->stats().reserved_records, 0u);
}

TEST(RecorderBufferPoolTest, ReportsLowNormalAndHighWatermarks) {
    RecorderBufferPoolOptions options;
    options.global_byte_limit = 16u * 1024u;
    options.default_topic_byte_limit = 16u * 1024u;
    options.queue_capacity = 4;
    options.low_watermark_bytes = 4u * 1024u;
    options.high_watermark_bytes = 12u * 1024u;
    auto pool = NewPool(options);
    ASSERT_NE(pool, nullptr);
    EXPECT_EQ(pool->stats().pressure, BufferPressureState::kLow);

    auto one = pool->Reserve(Request(TopicId{1}, 1, 1));
    ASSERT_TRUE(one.ok());
    EXPECT_EQ(pool->stats().pressure, BufferPressureState::kLow);
    auto two = pool->Reserve(Request(TopicId{1}, 1, 2));
    ASSERT_TRUE(two.ok());
    EXPECT_EQ(pool->stats().pressure, BufferPressureState::kNormal);
    auto three = pool->Reserve(Request(TopicId{1}, 1, 3));
    ASSERT_TRUE(three.ok());
    EXPECT_EQ(pool->stats().pressure, BufferPressureState::kHigh);

    ASSERT_TRUE(three->reservation.Cancel().has_value());
    EXPECT_EQ(pool->stats().pressure, BufferPressureState::kNormal);
    ASSERT_TRUE(two->reservation.Cancel().has_value());
    EXPECT_EQ(pool->stats().pressure, BufferPressureState::kLow);
}

TEST(RecorderBufferPoolTest, ConcurrentProducersPreserveEveryCommittedBuffer) {
    constexpr size_t kProducerCount = 4;
    constexpr size_t kRecordsPerProducer = 250;
    constexpr size_t kTotalRecords = kProducerCount * kRecordsPerProducer;

    RecorderBufferPoolOptions options;
    options.global_byte_limit = 256u * 1024u;
    options.default_topic_byte_limit = options.global_byte_limit;
    options.queue_capacity = 64;
    auto pool = NewPool(options);
    ASSERT_NE(pool, nullptr);

    std::atomic<bool> failed{false};
    std::atomic<size_t> producers_done{0};
    std::vector<std::thread> producers;
    producers.reserve(kProducerCount);
    for (size_t producer = 0; producer < kProducerCount; ++producer) {
        producers.emplace_back([&, producer] {
            for (size_t index = 0; index < kRecordsPerProducer; ++index) {
                const uint64_t tag = producer * kRecordsPerProducer + index;
                auto result = pool->Reserve(Request(
                    TopicId{1}, sizeof(uint64_t), tag,
                    BufferFullPolicy::kBlock, 5s));
                if (!result.ok() || !result->accepted()) {
                    failed.store(true, std::memory_order_release);
                    break;
                }
                for (std::byte& byte : result->reservation.bytes()) {
                    byte = static_cast<std::byte>(tag & 0xffu);
                }
                if (!std::move(result->reservation).Commit().ok()) {
                    failed.store(true, std::memory_order_release);
                    break;
                }
            }
            producers_done.fetch_add(1, std::memory_order_acq_rel);
        });
    }

    std::vector<bool> observed(kTotalRecords, false);
    size_t received = 0;
    while (received < kTotalRecords) {
        auto record = pool->Dequeue(100ms);
        if (!record.ok()) {
            if (record.status().code() != StatusCode::kTimeout) {
                failed.store(true, std::memory_order_release);
                break;
            }
            if (producers_done.load(std::memory_order_acquire) ==
                kProducerCount) {
                break;
            }
            continue;
        }
        const uint64_t tag = record->user_tag();
        if (tag >= kTotalRecords || observed[tag]) {
            failed.store(true, std::memory_order_release);
        } else {
            observed[tag] = true;
        }
        ++received;
    }

    for (std::thread& producer : producers) producer.join();
    EXPECT_FALSE(failed.load(std::memory_order_acquire));
    EXPECT_EQ(received, kTotalRecords);
    EXPECT_EQ(pool->stats().accepted_records, kTotalRecords);
    EXPECT_EQ(pool->stats().dequeued_records, kTotalRecords);
    EXPECT_EQ(pool->stats().bytes_in_use, 0u);
    EXPECT_EQ(pool->stats().queued_records, 0u);
    EXPECT_EQ(pool->stats().reserved_records, 0u);
}

}  // namespace
}  // namespace mino::storage
