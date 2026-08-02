// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/storage/snapshot_store.h"

#include <gtest/gtest.h>

#include <unistd.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "mino/storage/replay_engine.h"

namespace mino::storage {
namespace {

std::filesystem::path TestDirectory(std::string_view name) {
    static std::atomic<uint64_t> sequence{0};
    const char* temporary = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        temporary == nullptr ? std::filesystem::temp_directory_path()
                             : std::filesystem::path(temporary);
    const std::filesystem::path path =
        base / ("mino_snapshot_store_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    return path;
}

SnapshotStoreIdentity Identity() {
    return SnapshotStoreIdentity{
        .recording_id = 10,
        .topic_id = 20,
        .partition_id = 3,
        .writer_id = 40,
        .schema_refs = {7},
    };
}

Record SampleRecord(uint8_t value, uint64_t source_sequence) {
    Record record;
    record.header.schema_ref = 7;
    record.header.schema_version = 11;
    record.header.layout_version = 12;
    record.header.topic_id = 20;
    record.header.partition_id = 3;
    record.header.ingestion_timestamp_ns = 1000 + source_sequence;
    record.header.node_id = 21;
    record.header.publisher_id = 22;
    record.header.publisher_epoch = 23;
    record.header.source_sequence = source_sequence;
    record.header.observed_timestamp_ns = 2000 + source_sequence;
    record.payload = {static_cast<std::byte>(value),
                      static_cast<std::byte>(value + 1)};
    return record;
}

struct FaultState {
    std::optional<SnapshotStoreFaultPoint> fail_at;
};

Status FaultHook(SnapshotStoreFaultPoint point, void* context) noexcept {
    auto* state = static_cast<FaultState*>(context);
    if (state->fail_at == point) {
        return Status::Error(StatusCode::kUnavailable, "injected failure");
    }
    return Status::Ok();
}

TEST(SnapshotStoreTest, ReplacesWithExactlyOneLatestRecord) {
    const std::filesystem::path root = TestDirectory("replace");
    auto store = SnapshotStore::Open(root, Identity());
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    EXPECT_EQ((*store)->Put(SampleRecord(1, 101)).value(), 1u);
    EXPECT_EQ((*store)->Put(SampleRecord(2, 102)).value(), 2u);
    EXPECT_EQ((*store)->Put(SampleRecord(3, 103)).value(), 3u);
    store->reset();

    auto reader = SegmentReplayReader::Open(root / "snapshot.mino");
    ASSERT_TRUE(reader.ok()) << reader.status().ToString();
    ASSERT_EQ((*reader)->record_metadata().size(), 1u);
    EXPECT_EQ((*reader)->segment_header().first_ingestion_sequence, 3u);
    auto latest = (*reader)->Next();
    ASSERT_TRUE(latest.ok()) << latest.status().ToString();
    ASSERT_TRUE(latest->has_value());
    EXPECT_EQ((**latest).header.ingestion_sequence, 3u);
    EXPECT_EQ((**latest).header.source_sequence, 103u);
    EXPECT_EQ((**latest).header.node_id, 21u);
    EXPECT_EQ((**latest).header.publisher_id, 22u);
    EXPECT_EQ((**latest).header.publisher_epoch, 23u);
    EXPECT_EQ((**latest).header.observed_timestamp_ns, 2103u);
    EXPECT_EQ((**latest).payload, SampleRecord(3, 103).payload);
    auto eof = (*reader)->Next();
    ASSERT_TRUE(eof.ok()) << eof.status().ToString();
    EXPECT_FALSE(eof->has_value());
}

TEST(SnapshotStoreTest, AtomicFaultsRecoverOldOrNewSnapshot) {
    constexpr std::array<SnapshotStoreFaultPoint, 4> points = {
        SnapshotStoreFaultPoint::kAfterTempWrite,
        SnapshotStoreFaultPoint::kAfterTempDataSync,
        SnapshotStoreFaultPoint::kAfterRename,
        SnapshotStoreFaultPoint::kAfterParentDirectorySync,
    };
    size_t index = 0;
    for (SnapshotStoreFaultPoint point : points) {
        SCOPED_TRACE(index);
        const std::filesystem::path root =
            TestDirectory("atomic_" + std::to_string(index++));
        auto initial = SnapshotStore::Open(root, Identity());
        ASSERT_TRUE(initial.ok()) << initial.status().ToString();
        ASSERT_TRUE((*initial)->Put(SampleRecord(1, 100)).ok());
        initial->reset();

        FaultState state{.fail_at = point};
        SnapshotStoreOptions options;
        options.fault_hook = FaultHook;
        options.fault_hook_context = &state;
        auto faulted = SnapshotStore::Open(root, Identity(), options);
        ASSERT_TRUE(faulted.ok()) << faulted.status().ToString();
        auto put = (*faulted)->Put(SampleRecord(2, 200));
        EXPECT_FALSE(put.ok());
        faulted->reset();

        auto recovered = SnapshotStore::Open(root, Identity());
        ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
        ASSERT_TRUE((*recovered)->latest_record().has_value());
        const bool rename_completed =
            point == SnapshotStoreFaultPoint::kAfterRename ||
            point == SnapshotStoreFaultPoint::kAfterParentDirectorySync;
        EXPECT_EQ((*recovered)->latest_record()->header.source_sequence,
                  rename_completed ? 200u : 100u);
        EXPECT_EQ((*recovered)->latest_record()->payload,
                  SampleRecord(rename_completed ? 2 : 1,
                               rename_completed ? 200 : 100)
                      .payload);
    }
}

TEST(SnapshotStoreTest, OpenRestoresAndContinuesIngestionSequence) {
    const std::filesystem::path root = TestDirectory("sequence");
    auto first = SnapshotStore::Open(root, Identity());
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_EQ((*first)->Put(SampleRecord(1, 10)).value(), 1u);
    first->reset();

    auto reopened = SnapshotStore::Open(root, Identity());
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    EXPECT_EQ((*reopened)->latest_ingestion_sequence(), 1u);
    EXPECT_EQ((*reopened)->next_ingestion_sequence(), 2u);
    ASSERT_EQ((*reopened)->Put(SampleRecord(2, 11)).value(), 2u);
    EXPECT_EQ((*reopened)->next_ingestion_sequence(), 3u);
}

TEST(SnapshotStoreTest, EnforcesConfiguredPayloadBound) {
    SnapshotStoreOptions options;
    options.format_limits.max_payload_size = 1;
    options.format_limits.max_encoded_record_size = 4096;
    auto store = SnapshotStore::Open(TestDirectory("bound"), Identity(), options);
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    auto put = (*store)->Put(SampleRecord(1, 1));
    ASSERT_FALSE(put.ok());
    EXPECT_EQ(put.status().code(), StatusCode::kInvalidArgument);
    EXPECT_FALSE((*store)->has_snapshot());
}

}  // namespace
}  // namespace mino::storage
