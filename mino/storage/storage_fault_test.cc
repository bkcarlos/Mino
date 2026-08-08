// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <gtest/gtest.h>

#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"
#include "mino/schema/registry.h"
#include "mino/storage/recorder_buffer_pool.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/schema_store.h"
#include "mino/storage/segment_format.h"
#include "mino/storage/segment_recovery.h"
#include "mino/storage/segment_writer.h"

namespace mino::storage {
namespace {

using namespace std::chrono_literals;

constexpr auto kChildWatchdog = std::chrono::seconds(30);
constexpr auto kParentReapWatchdog = std::chrono::seconds(35);

void ArmStorageChildWatchdog() noexcept {
    ::signal(SIGALRM, SIG_DFL);
    ::alarm(static_cast<unsigned int>(kChildWatchdog.count()));
}

constexpr std::string_view kRoundsEnvironment =
    "MINO_STORAGE_FAULT_ROUNDS";
constexpr std::string_view kSeedEnvironment = "MINO_STORAGE_FAULT_SEED";

std::filesystem::path TestDirectory(std::string_view name) {
    static std::atomic<uint64_t> sequence{0};
    const char* temporary = std::getenv("TEST_TMPDIR");
    const std::filesystem::path base =
        temporary == nullptr ? std::filesystem::temp_directory_path()
                             : std::filesystem::path(temporary);
    const std::filesystem::path path =
        base / ("mino_storage_fault_" + std::string(name) + "_" +
                std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
                std::to_string(sequence.fetch_add(1)));
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    std::filesystem::create_directories(path);
    return path;
}

uint64_t EnvironmentUint64(std::string_view name, uint64_t fallback,
                           uint64_t maximum) {
    const std::string key(name);
    const char* raw = std::getenv(key.c_str());
    if (raw == nullptr || *raw == '\0') return fallback;
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(raw, &end, 10);
    if (errno != 0 || end == raw || *end != '\0' || parsed > maximum) {
        return fallback;
    }
    return static_cast<uint64_t>(parsed);
}

uint64_t NextRandom(uint64_t* state) {
    if (*state == 0) *state = 0x9e3779b97f4a7c15ULL;
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    return *state;
}

SegmentHeader SampleHeader() {
    SegmentHeader header;
    header.recording_id = 17;
    header.topic_id = 11;
    header.partition_id = 3;
    header.writer_id = 29;
    header.first_ingestion_sequence = 1;
    header.created_at_ns = 100;
    return header;
}

Record SampleRecord(uint64_t sequence, size_t payload_size = 31) {
    Record record;
    record.header.schema_ref = 1;
    record.header.schema_version = 0x00010000u;
    record.header.layout_version = 1;
    record.header.topic_id = 11;
    record.header.partition_id = 3;
    record.header.ingestion_sequence = sequence;
    record.header.ingestion_timestamp_ns = 1000 + sequence;
    record.header.node_id = 41;
    record.header.publisher_id = 43;
    record.header.publisher_epoch = 47;
    record.header.source_sequence = sequence;
    record.header.observed_timestamp_ns = 2000 + sequence;
    record.payload.resize(payload_size);
    for (size_t index = 0; index < payload_size; ++index) {
        record.payload[index] =
            static_cast<std::byte>((sequence + index) & 0xffu);
    }
    return record;
}

SegmentWriterOptions PerRecordOptions() {
    SegmentWriterOptions options;
    options.batch_bytes = 0;
    options.batch_records = 1;
    options.flush_interval_ns = 0;
    options.sync_policy = SegmentSyncPolicy::kPerRecord;
    return options;
}

struct Artifact {
    std::string bytes;
    schema::SchemaIdentity identity;
};

std::span<const std::byte> Bytes(const std::string& bytes) {
    return std::as_bytes(std::span<const char>(bytes.data(), bytes.size()));
}

Result<Artifact> CompileArtifact() {
    auto compiled = schema::SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package fault; "
        "message Durable { uint64 value = 1; }");
    if (!compiled.ok()) return compiled.status();
    std::vector<schema::LayoutPlan> layouts;
    layouts.reserve(compiled->types().size());
    for (const schema::SchemaHandle& descriptor : compiled->types()) {
        auto layout = schema::LayoutPlanner::Plan(*descriptor, {});
        if (!layout.ok()) return layout.status();
        layouts.push_back(std::move(*layout));
    }
    auto encoded =
        schema::codegen::EncodeDescriptorArtifact(*compiled, layouts);
    if (!encoded.ok()) return encoded.status();
    if (compiled->types().empty()) {
        return Status::Error(StatusCode::kInternal,
                             "test schema compiler returned no types");
    }
    return Artifact{std::move(*encoded), compiled->types()[0]->identity()};
}

struct SchemaFaultState {
    SchemaStoreFaultPoint fail_at =
        SchemaStoreFaultPoint::kAfterDescriptorTempWrite;
    bool injected = false;
};

Status SchemaFaultHook(SchemaStoreFaultPoint point, void* context) noexcept {
    auto* state = static_cast<SchemaFaultState*>(context);
    if (!state->injected && point == state->fail_at) {
        state->injected = true;
        return Status::Error(StatusCode::kUnavailable,
                             "injected schema persistence fault");
    }
    return Status::Ok();
}

struct ManifestFaultState {
    ManifestFaultPoint fail_at = ManifestFaultPoint::kAfterTempWrite;
    bool injected = false;
};

Status ManifestFaultHook(ManifestFaultPoint point, void* context) noexcept {
    auto* state = static_cast<ManifestFaultState*>(context);
    if (!state->injected && point == state->fail_at) {
        state->injected = true;
        return Status::Error(StatusCode::kUnavailable,
                             "injected manifest persistence fault");
    }
    return Status::Ok();
}

RecordingSessionMetadata RecordingMetadata() {
    return RecordingSessionMetadata{
        .recording_id = 101,
        .created_at_ns = 1000,
        .owner_id = 22,
        .owner_epoch = 3,
        .config_version = 1,
    };
}

PartitionMetadata PartitionMetadataForTest() {
    return PartitionMetadata{
        .recording_id = 101,
        .topic_id = 11,
        .partition_id = 3,
        .writer_id = 29,
        .owner_epoch = 3,
        .config_version = 1,
    };
}

void WriteOneByte(const std::filesystem::path& path) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.good());
    output.put('x');
    ASSERT_TRUE(output.good());
}

TEST(StorageFaultTest, ReopenObservesOnlyAtomicSchemaAndManifestStates) {
    auto artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();

    constexpr std::array<SchemaStoreFaultPoint, 8> schema_points = {
        SchemaStoreFaultPoint::kAfterDescriptorTempWrite,
        SchemaStoreFaultPoint::kAfterDescriptorSync,
        SchemaStoreFaultPoint::kAfterDescriptorRename,
        SchemaStoreFaultPoint::kAfterDescriptorDirectorySync,
        SchemaStoreFaultPoint::kAfterManifestTempWrite,
        SchemaStoreFaultPoint::kAfterManifestSync,
        SchemaStoreFaultPoint::kAfterManifestRename,
        SchemaStoreFaultPoint::kAfterManifestDirectorySync,
    };
    for (SchemaStoreFaultPoint point : schema_points) {
        SCOPED_TRACE(static_cast<int>(point));
        const std::filesystem::path root = TestDirectory("schema_atomic");
        schema::SchemaRegistry registry;
        SchemaFaultState fault{.fail_at = point, .injected = false};
        SchemaStoreOptions options;
        options.fault_hook = SchemaFaultHook;
        options.fault_hook_context = &fault;
        auto store = SchemaStore::Open(root, &registry, options);
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        auto persisted =
            (*store)->Persist(artifact->identity, Bytes(artifact->bytes));
        ASSERT_FALSE(persisted.ok());
        EXPECT_TRUE(fault.injected);
        store->reset();

        auto reopened = SchemaStore::Open(root, &registry);
        ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
        const bool manifest_renamed =
            point == SchemaStoreFaultPoint::kAfterManifestRename ||
            point == SchemaStoreFaultPoint::kAfterManifestDirectorySync;
        EXPECT_EQ((*reopened)->size(), manifest_renamed ? 1u : 0u);
        auto resolved = (*reopened)->Resolve(1);
        EXPECT_EQ(resolved.ok(), manifest_renamed);
        if (!manifest_renamed) {
            EXPECT_EQ(resolved.status().code(), StatusCode::kNotFound);
        }
    }

    constexpr std::array<ManifestFaultPoint, 4> commit_points = {
        ManifestFaultPoint::kAfterTempWrite,
        ManifestFaultPoint::kAfterTempDataSync,
        ManifestFaultPoint::kAfterRename,
        ManifestFaultPoint::kAfterParentDirectorySync,
    };
    for (ManifestFaultPoint point : commit_points) {
        SCOPED_TRACE(static_cast<int>(point));
        const std::filesystem::path root = TestDirectory("manifest_atomic");
        auto created = RecordingManifest::Create(root, RecordingMetadata());
        ASSERT_TRUE(created.ok()) << created.status().ToString();
        created->reset();

        ManifestFaultState fault{.fail_at = point, .injected = false};
        ManifestOptions options;
        options.fault_hook = ManifestFaultHook;
        options.fault_hook_context = &fault;
        auto manifest = RecordingManifest::Open(root, options);
        ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
        const Status failed = (*manifest)->UpdateSessionConfigVersion(2);
        ASSERT_FALSE(failed.ok());
        EXPECT_TRUE(fault.injected);
        EXPECT_TRUE((*manifest)->poisoned());
        EXPECT_EQ((*manifest)->UpdateSessionConfigVersion(3).code(),
                  StatusCode::kUnavailable);
        manifest->reset();

        auto reopened = RecordingManifest::Open(root);
        ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
        const bool rename_completed =
            point == ManifestFaultPoint::kAfterRename ||
            point == ManifestFaultPoint::kAfterParentDirectorySync;
        EXPECT_EQ((*reopened)->snapshot().session.config_version,
                  rename_completed ? 2u : 1u);
    }

    constexpr std::array<ManifestFaultPoint, 2> orphan_points = {
        ManifestFaultPoint::kAfterOrphanRename,
        ManifestFaultPoint::kAfterOrphanDirectorySync,
    };
    for (ManifestFaultPoint point : orphan_points) {
        SCOPED_TRACE(static_cast<int>(point));
        const std::filesystem::path root = TestDirectory("orphan_atomic");
        std::filesystem::create_directory(root / "segments");
        auto created =
            PartitionManifest::Create(root, PartitionMetadataForTest());
        ASSERT_TRUE(created.ok()) << created.status().ToString();
        created->reset();
        const std::filesystem::path candidate = "segments/orphan.mino";
        WriteOneByte(root / candidate);

        ManifestFaultState fault{.fail_at = point, .injected = false};
        ManifestOptions options;
        options.fault_hook = ManifestFaultHook;
        options.fault_hook_context = &fault;
        auto manifest = PartitionManifest::Open(root, options);
        ASSERT_TRUE(manifest.ok()) << manifest.status().ToString();
        auto quarantined = (*manifest)->QuarantineOrphan(candidate);
        ASSERT_FALSE(quarantined.ok());
        EXPECT_TRUE((*manifest)->poisoned());
        manifest->reset();

        auto reopened = PartitionManifest::Open(root);
        ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
        EXPECT_FALSE(std::filesystem::exists(root / candidate));
        EXPECT_TRUE(std::filesystem::is_regular_file(
            root / "segments/orphan.mino.orphan"));
    }
}

struct ScriptedIo {
    bool armed = false;
    size_t max_write = std::numeric_limits<size_t>::max();
    size_t bytes_before_write_error = 0;
    size_t write_interrupts = 0;
    int write_error = 0;
    size_t sync_interrupts = 0;
    int sync_error = 0;
    size_t write_calls = 0;
    size_t sync_calls = 0;
};

std::ptrdiff_t ScriptedWrite(int fd, const std::byte* data, size_t size,
                             void* context) noexcept {
    auto* state = static_cast<ScriptedIo*>(context);
    ++state->write_calls;
    if (!state->armed) {
        return static_cast<std::ptrdiff_t>(::write(fd, data, size));
    }
    if (state->write_interrupts != 0) {
        --state->write_interrupts;
        errno = EINTR;
        return -1;
    }
    if (state->bytes_before_write_error != 0) {
        const size_t count =
            std::min(size, state->bytes_before_write_error);
        const ssize_t written = ::write(fd, data, count);
        if (written > 0) {
            state->bytes_before_write_error -= static_cast<size_t>(written);
        }
        return static_cast<std::ptrdiff_t>(written);
    }
    if (state->write_error != 0) {
        errno = state->write_error;
        return -1;
    }
    const size_t count = std::min(size, state->max_write);
    return static_cast<std::ptrdiff_t>(::write(fd, data, count));
}

int ScriptedSync(int fd, void* context) noexcept {
    auto* state = static_cast<ScriptedIo*>(context);
    ++state->sync_calls;
    if (state->armed && state->sync_interrupts != 0) {
        --state->sync_interrupts;
        errno = EINTR;
        return -1;
    }
    if (state->armed && state->sync_error != 0) {
        errno = state->sync_error;
        return -1;
    }
#if defined(__APPLE__)
    return ::fsync(fd);
#else
    return ::fdatasync(fd);
#endif
}

struct ErrnoExpectation {
    int error;
    StatusCode code;
};

constexpr std::array<ErrnoExpectation, 3> kIoErrors = {
    ErrnoExpectation{ENOSPC, StatusCode::kResourceExhausted},
    ErrnoExpectation{EIO, StatusCode::kUnavailable},
    ErrnoExpectation{EROFS, StatusCode::kPermissionDenied},
};

TEST(StorageFaultTest, WriterIoFaultsAreStickyAndRecoveryLosesNoCommit) {
    {
        const std::filesystem::path root = TestDirectory("short_eintr");
        const std::filesystem::path path = root / "segment.mino";
        ScriptedIo io;
        SegmentWriterOptions options = PerRecordOptions();
        options.write_hook = ScriptedWrite;
        options.data_sync_hook = ScriptedSync;
        options.io_hook_context = &io;
        auto writer = SegmentWriter::Create(path, SampleHeader(), 100, options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        io.armed = true;
        io.max_write = 7;
        io.write_interrupts = 1;
        io.sync_interrupts = 1;
        auto appended = (*writer)->Append(SampleRecord(1), 101);
        ASSERT_TRUE(appended.ok()) << appended.status().ToString();
        ASSERT_TRUE((*writer)->Seal(102).ok());
        EXPECT_GT(io.write_calls, 3u);
        EXPECT_GE(io.sync_calls, 3u);
        writer->reset();
        auto scanned = ScanSegment(path);
        ASSERT_TRUE(scanned.ok()) << scanned.status().ToString();
        EXPECT_TRUE(scanned->clean()) << scanned->reason_detail;
        ASSERT_EQ(scanned->records.size(), 1u);
        EXPECT_EQ(scanned->records[0].ingestion_sequence, 1u);
    }

    for (const ErrnoExpectation expectation : kIoErrors) {
        SCOPED_TRACE(expectation.error);
        const std::filesystem::path root = TestDirectory("write_errno");
        const std::filesystem::path path = root / "segment.mino";
        ScriptedIo io;
        SegmentWriterOptions options = PerRecordOptions();
        options.write_hook = ScriptedWrite;
        options.data_sync_hook = ScriptedSync;
        options.io_hook_context = &io;
        auto writer = SegmentWriter::Create(path, SampleHeader(), 100, options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        ASSERT_TRUE((*writer)->Append(SampleRecord(1), 101).ok());
        EXPECT_EQ((*writer)->durable_records(), 1u);

        io.armed = true;
        io.bytes_before_write_error = 17;
        io.write_error = expectation.error;
        auto failed = (*writer)->Append(SampleRecord(2), 102);
        ASSERT_FALSE(failed.ok());
        EXPECT_EQ(failed.status().code(), expectation.code);
        EXPECT_EQ((*writer)->state(), SegmentWriterState::kError);
        EXPECT_EQ((*writer)->durable_records(), 1u);
        const Status sticky = (*writer)->error_status();
        const size_t calls_after_error = io.write_calls;
        EXPECT_EQ((*writer)->Flush(103), sticky);
        EXPECT_EQ((*writer)->Append(SampleRecord(3), 103).status(), sticky);
        EXPECT_EQ(io.write_calls, calls_after_error);
        writer->reset();

        auto damaged = ScanSegment(path);
        ASSERT_TRUE(damaged.ok()) << damaged.status().ToString();
        EXPECT_TRUE(damaged->repairable()) << damaged->reason_detail;
        EXPECT_EQ(damaged->last_complete_sequence, 1u);
        auto repaired = RepairSegment(path);
        ASSERT_TRUE(repaired.ok()) << repaired.status().ToString();
        EXPECT_TRUE(repaired->repaired);
        auto clean = ScanSegment(path);
        ASSERT_TRUE(clean.ok()) << clean.status().ToString();
        EXPECT_TRUE(clean->clean()) << clean->reason_detail;
        ASSERT_EQ(clean->records.size(), 1u);
        EXPECT_EQ(clean->records[0].ingestion_sequence, 1u);
    }

    for (const ErrnoExpectation expectation : kIoErrors) {
        SCOPED_TRACE(expectation.error);
        const std::filesystem::path root = TestDirectory("sync_errno");
        const std::filesystem::path path = root / "segment.mino";
        ScriptedIo io;
        SegmentWriterOptions options = PerRecordOptions();
        options.write_hook = ScriptedWrite;
        options.data_sync_hook = ScriptedSync;
        options.io_hook_context = &io;
        auto writer = SegmentWriter::Create(path, SampleHeader(), 100, options);
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        ASSERT_TRUE((*writer)->Append(SampleRecord(1), 101).ok());
        io.armed = true;
        io.sync_error = expectation.error;
        auto failed = (*writer)->Append(SampleRecord(2), 102);
        ASSERT_FALSE(failed.ok());
        EXPECT_EQ(failed.status().code(), expectation.code);
        EXPECT_EQ((*writer)->state(), SegmentWriterState::kError);
        EXPECT_EQ((*writer)->durable_records(), 1u);
        const Status sticky = (*writer)->error_status();
        const size_t syncs_after_error = io.sync_calls;
        EXPECT_EQ((*writer)->Seal(103), sticky);
        EXPECT_EQ(io.sync_calls, syncs_after_error);
        writer->reset();

        auto scanned = ScanSegment(path);
        ASSERT_TRUE(scanned.ok()) << scanned.status().ToString();
        EXPECT_TRUE(scanned->clean()) << scanned->reason_detail;
        ASSERT_EQ(scanned->records.size(), 2u);
        EXPECT_EQ(scanned->records[0].ingestion_sequence, 1u);
        EXPECT_EQ(scanned->records[1].ingestion_sequence, 2u);
        auto repaired = RepairSegment(path);
        ASSERT_TRUE(repaired.ok()) << repaired.status().ToString();
        EXPECT_FALSE(repaired->repaired);
    }
}

bool WriteExactly(int fd, const std::byte* data, size_t size) noexcept {
    size_t offset = 0;
    while (offset < size) {
        const ssize_t written = ::write(fd, data + offset, size - offset);
        if (written < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (written == 0) return false;
        offset += static_cast<size_t>(written);
    }
    return true;
}

struct KillWriteState {
    size_t target_record = 1;
    size_t cut_bytes = 1;
    size_t write_call = 0;
};

std::ptrdiff_t KillDuringRecordWrite(int fd, const std::byte* data, size_t size,
                                     void* context) noexcept {
    auto* state = static_cast<KillWriteState*>(context);
    ++state->write_call;
    if (state->write_call == 1) {
        if (!WriteExactly(fd, data, size)) ::_exit(91);
        return static_cast<std::ptrdiff_t>(size);
    }
    const size_t record_index = state->write_call - 1;
    if (record_index != state->target_record) {
        if (!WriteExactly(fd, data, size)) ::_exit(92);
        return static_cast<std::ptrdiff_t>(size);
    }

    const size_t count = std::min(size, state->cut_bytes);
    if (count != 0 && !WriteExactly(fd, data, count)) ::_exit(93);
    if (::kill(::getpid(), SIGKILL) != 0) ::_exit(94);
    ::_exit(95);
}

int WaitForChild(pid_t child) {
    const auto deadline = std::chrono::steady_clock::now() + kParentReapWatchdog;
    int status = 0;
    for (;;) {
        const pid_t result = ::waitpid(child, &status, WNOHANG);
        if (result == child) return status;
        if (result < 0 && errno != EINTR) return -1;
        if (std::chrono::steady_clock::now() >= deadline) {
            static_cast<void>(::kill(child, SIGKILL));
            const auto reap_deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (std::chrono::steady_clock::now() < reap_deadline) {
                const pid_t reaped = ::waitpid(child, &status, WNOHANG);
                if (reaped == child) break;
                if (reaped < 0 && errno != EINTR) break;
                std::this_thread::sleep_for(1ms);
            }
            errno = ETIMEDOUT;
            return -1;
        }
        std::this_thread::sleep_for(1ms);
    }
}

[[noreturn]] void KillCurrentProcess(int failure_exit) noexcept {
    if (::kill(::getpid(), SIGKILL) != 0) ::_exit(failure_exit);
    ::_exit(failure_exit + 1);
}

void ReportScenario(std::string_view scenario, uint64_t rounds,
                    uint64_t cases, uint64_t seed) {
    std::cout << "STORAGE_FAULT_SCENARIO_RESULT scenario=" << scenario
              << " rounds=" << rounds << " cases=" << cases
              << " seed=" << seed << std::endl;
}

size_t RoundOffset(uint64_t* random, size_t point_count) {
    return static_cast<size_t>(NextRandom(random) % point_count);
}

bool ChildWasKilled(int status) {
    return status != -1 && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
}

TEST(StorageFaultTest, SigkillAtRecordWritesRepairsToLastCompleteCommit) {
#if defined(__unix__) || defined(__APPLE__)
    constexpr size_t kRecordCount = 4;
    const size_t encoded_size = *EncodedRecordSize(SampleRecord(1).payload.size());
    ASSERT_GT(encoded_size, 1u);
    const uint64_t rounds =
        EnvironmentUint64(kRoundsEnvironment, 1, 1000);
    const uint64_t seed = EnvironmentUint64(
        kSeedEnvironment, 1, std::numeric_limits<uint64_t>::max());
    uint64_t random = seed;
    uint64_t cases = 0;
    RecordProperty("rounds", std::to_string(rounds));
    RecordProperty("seed", std::to_string(seed));

    for (uint64_t round = 0; round < rounds; ++round) {
        for (size_t target = 1; target <= kRecordCount; ++target) {
            const size_t partial_cut =
                1 + static_cast<size_t>(NextRandom(&random) % (encoded_size - 1));
            for (const size_t cut : {partial_cut, encoded_size}) {
                SCOPED_TRACE("round=" + std::to_string(round) +
                             " target=" + std::to_string(target) +
                             " cut=" + std::to_string(cut));
                ++cases;
                const std::filesystem::path root = TestDirectory("sigkill");
                const std::filesystem::path path = root / "segment.mino";
                const pid_t child = ::fork();
                ASSERT_GE(child, 0);
                if (child == 0) {
                    ArmStorageChildWatchdog();
                    KillWriteState state{
                        .target_record = target,
                        .cut_bytes = cut,
                        .write_call = 0,
                    };
                    SegmentWriterOptions options;
                    options.batch_bytes = 0;
                    options.batch_records = 1;
                    options.flush_interval_ns = 0;
                    options.sync_policy = SegmentSyncPolicy::kNone;
                    options.write_hook = KillDuringRecordWrite;
                    options.io_hook_context = &state;
                    auto writer =
                        SegmentWriter::Create(path, SampleHeader(), 100, options);
                    if (!writer.ok()) ::_exit(96);
                    for (size_t index = 1; index <= kRecordCount; ++index) {
                        auto appended = (*writer)->Append(
                            SampleRecord(static_cast<uint64_t>(index)),
                            100 + static_cast<uint64_t>(index));
                        if (!appended.ok()) ::_exit(97);
                    }
                    ::_exit(98);
                }

                const int child_status = WaitForChild(child);
                ASSERT_NE(child_status, -1);
                ASSERT_TRUE(WIFSIGNALED(child_status));
                EXPECT_EQ(WTERMSIG(child_status), SIGKILL);

                auto scanned = ScanSegment(path);
                ASSERT_TRUE(scanned.ok()) << scanned.status().ToString();
                const size_t expected_records =
                    cut == encoded_size ? target : target - 1;
                EXPECT_EQ(scanned->records.size(), expected_records);
                if (cut == encoded_size) {
                    EXPECT_TRUE(scanned->clean()) << scanned->reason_detail;
                } else {
                    EXPECT_TRUE(scanned->repairable()) << scanned->reason_detail;
                }

                auto repaired = RepairSegment(path);
                ASSERT_TRUE(repaired.ok()) << repaired.status().ToString();
                auto clean = ScanSegment(path);
                ASSERT_TRUE(clean.ok()) << clean.status().ToString();
                EXPECT_TRUE(clean->clean()) << clean->reason_detail;
                ASSERT_EQ(clean->records.size(), expected_records);
                if (expected_records != 0) {
                    EXPECT_EQ(clean->last_complete_sequence, expected_records);
                    EXPECT_EQ(clean->records.back().ingestion_sequence,
                              expected_records);
                } else {
                    EXPECT_FALSE(clean->has_last_complete_sequence);
                }
            }
        }
    }
    ReportScenario("record-write", rounds, cases, seed);
#else
    GTEST_SKIP() << "fork/SIGKILL storage campaign requires a POSIX platform";
#endif
}

struct KillSyncState {
    bool after_sync = false;
};

int KillAroundDataSync(int fd, void* context) noexcept {
    const auto* state = static_cast<KillSyncState*>(context);
    if (!state->after_sync) KillCurrentProcess(110);
    int result;
    do {
#if defined(__APPLE__)
        result = ::fsync(fd);
#else
        result = ::fdatasync(fd);
#endif
    } while (result != 0 && errno == EINTR);
    if (result != 0) return result;
    KillCurrentProcess(112);
}

TEST(StorageFaultTest, SigkillBeforeAndAfterRecordAndSealSync) {
#if defined(__unix__) || defined(__APPLE__)
    constexpr std::array<std::pair<bool, bool>, 4> cuts = {{
        {false, false},
        {false, true},
        {true, false},
        {true, true},
    }};
    const uint64_t rounds =
        EnvironmentUint64(kRoundsEnvironment, 1, 1000);
    const uint64_t seed = EnvironmentUint64(
        kSeedEnvironment, 1, std::numeric_limits<uint64_t>::max());
    uint64_t random = seed;
    uint64_t cases = 0;
    RecordProperty("rounds", std::to_string(rounds));
    RecordProperty("seed", std::to_string(seed));

    for (uint64_t round = 0; round < rounds; ++round) {
        const size_t offset = RoundOffset(&random, cuts.size());
        for (size_t index = 0; index < cuts.size(); ++index) {
            const auto [seal, after_sync] = cuts[(offset + index) % cuts.size()];
            SCOPED_TRACE("round=" + std::to_string(round) +
                         " seal=" + std::to_string(seal) +
                         " after_sync=" + std::to_string(after_sync));
            ++cases;
            const std::filesystem::path root = TestDirectory("sync_sigkill");
            const std::filesystem::path path = root / "segment.mino";
            const pid_t child = ::fork();
            ASSERT_GE(child, 0);
            if (child == 0) {
                ArmStorageChildWatchdog();
                KillSyncState state{.after_sync = after_sync};
                SegmentWriterOptions options;
                options.batch_bytes = 0;
                options.batch_records = 1;
                options.flush_interval_ns = 0;
                options.sync_policy = seal ? SegmentSyncPolicy::kNone
                                           : SegmentSyncPolicy::kPerRecord;
                options.data_sync_hook = KillAroundDataSync;
                options.io_hook_context = &state;
                auto writer =
                    SegmentWriter::Create(path, SampleHeader(), 100, options);
                if (!writer.ok()) ::_exit(114);
                auto appended = (*writer)->Append(SampleRecord(1), 101);
                if (!appended.ok()) ::_exit(115);
                if (seal) {
                    const Status sealed = (*writer)->Seal(102);
                    if (!sealed.ok()) ::_exit(116);
                }
                ::_exit(117);
            }

            const int child_status = WaitForChild(child);
            ASSERT_TRUE(ChildWasKilled(child_status)) << child_status;
            auto scanned = ScanSegment(path);
            ASSERT_TRUE(scanned.ok()) << scanned.status().ToString();
            EXPECT_TRUE(scanned->clean()) << scanned->reason_detail;
            ASSERT_EQ(scanned->records.size(), 1u);
            EXPECT_EQ(scanned->records[0].ingestion_sequence, 1u);
        }
    }
    ReportScenario("record-sync", rounds, cases, seed);
#else
    GTEST_SKIP() << "fork/SIGKILL storage campaign requires a POSIX platform";
#endif
}

struct KillSchemaState {
    SchemaStoreFaultPoint target;
};

Status KillAtSchemaPoint(SchemaStoreFaultPoint point, void* context) noexcept {
    const auto* state = static_cast<KillSchemaState*>(context);
    if (point == state->target) KillCurrentProcess(120);
    return Status::Ok();
}

TEST(StorageFaultTest, SigkillAcrossSchemaPersistencePhases) {
#if defined(__unix__) || defined(__APPLE__)
    constexpr std::array<SchemaStoreFaultPoint, 8> points = {
        SchemaStoreFaultPoint::kAfterDescriptorTempWrite,
        SchemaStoreFaultPoint::kAfterDescriptorSync,
        SchemaStoreFaultPoint::kAfterDescriptorRename,
        SchemaStoreFaultPoint::kAfterDescriptorDirectorySync,
        SchemaStoreFaultPoint::kAfterManifestTempWrite,
        SchemaStoreFaultPoint::kAfterManifestSync,
        SchemaStoreFaultPoint::kAfterManifestRename,
        SchemaStoreFaultPoint::kAfterManifestDirectorySync,
    };
    auto artifact = CompileArtifact();
    ASSERT_TRUE(artifact.ok()) << artifact.status().ToString();
    const uint64_t rounds =
        EnvironmentUint64(kRoundsEnvironment, 1, 1000);
    const uint64_t seed = EnvironmentUint64(
        kSeedEnvironment, 1, std::numeric_limits<uint64_t>::max());
    uint64_t random = seed;
    uint64_t cases = 0;
    RecordProperty("rounds", std::to_string(rounds));
    RecordProperty("seed", std::to_string(seed));

    for (uint64_t round = 0; round < rounds; ++round) {
        const size_t offset = RoundOffset(&random, points.size());
        for (size_t index = 0; index < points.size(); ++index) {
            const SchemaStoreFaultPoint point =
                points[(offset + index) % points.size()];
            SCOPED_TRACE("round=" + std::to_string(round) +
                         " point=" +
                         std::to_string(static_cast<int>(point)));
            ++cases;
            const std::filesystem::path root = TestDirectory("schema_sigkill");
            const pid_t child = ::fork();
            ASSERT_GE(child, 0);
            if (child == 0) {
                ArmStorageChildWatchdog();
                schema::SchemaRegistry registry;
                KillSchemaState state{.target = point};
                SchemaStoreOptions options;
                options.fault_hook = KillAtSchemaPoint;
                options.fault_hook_context = &state;
                auto store = SchemaStore::Open(root, &registry, options);
                if (!store.ok()) ::_exit(122);
                auto persisted =
                    (*store)->Persist(artifact->identity, Bytes(artifact->bytes));
                if (!persisted.ok()) ::_exit(123);
                ::_exit(124);
            }

            const int child_status = WaitForChild(child);
            ASSERT_TRUE(ChildWasKilled(child_status)) << child_status;
            schema::SchemaRegistry registry;
            auto reopened = SchemaStore::Open(root, &registry);
            ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
            const bool manifest_renamed =
                point == SchemaStoreFaultPoint::kAfterManifestRename ||
                point == SchemaStoreFaultPoint::kAfterManifestDirectorySync;
            EXPECT_EQ((*reopened)->size(), manifest_renamed ? 1u : 0u);
            EXPECT_EQ((*reopened)->Resolve(1).ok(), manifest_renamed);
        }
    }
    ReportScenario("schema", rounds, cases, seed);
#else
    GTEST_SKIP() << "fork/SIGKILL storage campaign requires a POSIX platform";
#endif
}

struct KillManifestState {
    ManifestFaultPoint target;
};

Status KillAtManifestPoint(ManifestFaultPoint point, void* context) noexcept {
    const auto* state = static_cast<KillManifestState*>(context);
    if (point == state->target) KillCurrentProcess(130);
    return Status::Ok();
}

TEST(StorageFaultTest, SigkillAcrossManifestPersistencePhases) {
#if defined(__unix__) || defined(__APPLE__)
    constexpr std::array<ManifestFaultPoint, 4> points = {
        ManifestFaultPoint::kAfterTempWrite,
        ManifestFaultPoint::kAfterTempDataSync,
        ManifestFaultPoint::kAfterRename,
        ManifestFaultPoint::kAfterParentDirectorySync,
    };
    const uint64_t rounds =
        EnvironmentUint64(kRoundsEnvironment, 1, 1000);
    const uint64_t seed = EnvironmentUint64(
        kSeedEnvironment, 1, std::numeric_limits<uint64_t>::max());
    uint64_t random = seed;
    uint64_t cases = 0;
    RecordProperty("rounds", std::to_string(rounds));
    RecordProperty("seed", std::to_string(seed));

    for (uint64_t round = 0; round < rounds; ++round) {
        const size_t offset = RoundOffset(&random, points.size());
        for (size_t index = 0; index < points.size(); ++index) {
            const ManifestFaultPoint point =
                points[(offset + index) % points.size()];
            SCOPED_TRACE("round=" + std::to_string(round) +
                         " point=" +
                         std::to_string(static_cast<int>(point)));
            ++cases;
            const std::filesystem::path root = TestDirectory("manifest_sigkill");
            auto created = RecordingManifest::Create(root, RecordingMetadata());
            ASSERT_TRUE(created.ok()) << created.status().ToString();
            created->reset();
            const pid_t child = ::fork();
            ASSERT_GE(child, 0);
            if (child == 0) {
                ArmStorageChildWatchdog();
                KillManifestState state{.target = point};
                ManifestOptions options;
                options.fault_hook = KillAtManifestPoint;
                options.fault_hook_context = &state;
                auto manifest = RecordingManifest::Open(root, options);
                if (!manifest.ok()) ::_exit(132);
                const Status updated =
                    (*manifest)->UpdateSessionConfigVersion(2);
                if (!updated.ok()) ::_exit(133);
                ::_exit(134);
            }

            const int child_status = WaitForChild(child);
            ASSERT_TRUE(ChildWasKilled(child_status)) << child_status;
            auto reopened = RecordingManifest::Open(root);
            ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
            const bool renamed = point == ManifestFaultPoint::kAfterRename ||
                                 point == ManifestFaultPoint::kAfterParentDirectorySync;
            EXPECT_EQ((*reopened)->snapshot().session.config_version,
                      renamed ? 2u : 1u);
        }
    }
    ReportScenario("manifest", rounds, cases, seed);
#else
    GTEST_SKIP() << "fork/SIGKILL storage campaign requires a POSIX platform";
#endif
}

SegmentManifestEntry CampaignSegment(SegmentPersistentState state) {
    return SegmentManifestEntry{
        .segment_id = 1,
        .state = state,
        .first_ingestion_sequence = 1,
        .last_ingestion_sequence = 1,
        .created_at_ns = 100,
        .sealed_at_ns = state == SegmentPersistentState::kSealed ? 200u : 0u,
        .size_bytes = kEncodedSegmentHeaderSize,
        .relative_path = "segments/00000001.mino",
    };
}

TEST(StorageFaultTest, SigkillAcrossSealAndCheckpointPersistence) {
#if defined(__unix__) || defined(__APPLE__)
    constexpr std::array<ManifestFaultPoint, 4> points = {
        ManifestFaultPoint::kAfterTempWrite,
        ManifestFaultPoint::kAfterTempDataSync,
        ManifestFaultPoint::kAfterRename,
        ManifestFaultPoint::kAfterParentDirectorySync,
    };
    const uint64_t rounds =
        EnvironmentUint64(kRoundsEnvironment, 1, 1000);
    const uint64_t seed = EnvironmentUint64(
        kSeedEnvironment, 1, std::numeric_limits<uint64_t>::max());
    uint64_t random = seed;
    uint64_t cases = 0;
    RecordProperty("rounds", std::to_string(rounds));
    RecordProperty("seed", std::to_string(seed));

    for (uint64_t round = 0; round < rounds; ++round) {
        const size_t offset = RoundOffset(&random, points.size());
        for (bool checkpoint : {false, true}) {
            for (size_t index = 0; index < points.size(); ++index) {
                const ManifestFaultPoint point =
                    points[(offset + index) % points.size()];
                SCOPED_TRACE("round=" + std::to_string(round) +
                             " checkpoint=" + std::to_string(checkpoint) +
                             " point=" +
                             std::to_string(static_cast<int>(point)));
                ++cases;
                const std::filesystem::path root =
                    TestDirectory("checkpoint_seal_sigkill");
                std::filesystem::create_directory(root / "segments");
                auto created =
                    PartitionManifest::Create(root, PartitionMetadataForTest());
                ASSERT_TRUE(created.ok()) << created.status().ToString();
                SegmentManifestEntry segment = CampaignSegment(
                    checkpoint ? SegmentPersistentState::kSealed
                               : SegmentPersistentState::kOpen);
                ASSERT_TRUE((*created)->AddSegment(segment).ok());
                created->reset();

                const pid_t child = ::fork();
                ASSERT_GE(child, 0);
                if (child == 0) {
                    ArmStorageChildWatchdog();
                    KillManifestState state{.target = point};
                    ManifestOptions options;
                    options.fault_hook = KillAtManifestPoint;
                    options.fault_hook_context = &state;
                    auto manifest = PartitionManifest::Open(root, options);
                    if (!manifest.ok()) ::_exit(136);
                    Status updated = Status::Ok();
                    if (checkpoint) {
                        updated = (*manifest)->UpdateCheckpoint(DurableCheckpoint{
                            .segment_id = 1,
                            .durable_offset = kEncodedSegmentHeaderSize,
                            .durable_sequence = 1,
                        });
                    } else {
                        segment.state = SegmentPersistentState::kSealed;
                        segment.sealed_at_ns = 200;
                        updated = (*manifest)->UpdateSegment(segment);
                    }
                    if (!updated.ok()) ::_exit(137);
                    ::_exit(138);
                }

                const int child_status = WaitForChild(child);
                ASSERT_TRUE(ChildWasKilled(child_status)) << child_status;
                auto reopened = PartitionManifest::Open(root);
                ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
                const bool renamed =
                    point == ManifestFaultPoint::kAfterRename ||
                    point == ManifestFaultPoint::kAfterParentDirectorySync;
                if (checkpoint) {
                    EXPECT_EQ((*reopened)->snapshot().checkpoint.has_value(),
                              renamed);
                } else {
                    ASSERT_EQ((*reopened)->snapshot().segments.size(), 1u);
                    EXPECT_EQ((*reopened)->snapshot().segments[0].state,
                              renamed ? SegmentPersistentState::kSealed
                                      : SegmentPersistentState::kOpen);
                }
            }
        }
    }
    ReportScenario("checkpoint-seal", rounds, cases, seed);
#else
    GTEST_SKIP() << "fork/SIGKILL storage campaign requires a POSIX platform";
#endif
}

TEST(StorageFaultTest, SigkillAcrossOrphanQuarantinePersistence) {
#if defined(__unix__) || defined(__APPLE__)
    constexpr std::array<ManifestFaultPoint, 2> points = {
        ManifestFaultPoint::kAfterOrphanRename,
        ManifestFaultPoint::kAfterOrphanDirectorySync,
    };
    const uint64_t rounds =
        EnvironmentUint64(kRoundsEnvironment, 1, 1000);
    const uint64_t seed = EnvironmentUint64(
        kSeedEnvironment, 1, std::numeric_limits<uint64_t>::max());
    uint64_t random = seed;
    uint64_t cases = 0;
    RecordProperty("rounds", std::to_string(rounds));
    RecordProperty("seed", std::to_string(seed));

    for (uint64_t round = 0; round < rounds; ++round) {
        const size_t offset = RoundOffset(&random, points.size());
        for (size_t index = 0; index < points.size(); ++index) {
            const ManifestFaultPoint point =
                points[(offset + index) % points.size()];
            SCOPED_TRACE("round=" + std::to_string(round) +
                         " point=" +
                         std::to_string(static_cast<int>(point)));
            ++cases;
            const std::filesystem::path root = TestDirectory("orphan_sigkill");
            std::filesystem::create_directory(root / "segments");
            auto created =
                PartitionManifest::Create(root, PartitionMetadataForTest());
            ASSERT_TRUE(created.ok()) << created.status().ToString();
            created->reset();
            const std::filesystem::path candidate = "segments/orphan.mino";
            WriteOneByte(root / candidate);

            const pid_t child = ::fork();
            ASSERT_GE(child, 0);
            if (child == 0) {
                ArmStorageChildWatchdog();
                KillManifestState state{.target = point};
                ManifestOptions options;
                options.fault_hook = KillAtManifestPoint;
                options.fault_hook_context = &state;
                auto manifest = PartitionManifest::Open(root, options);
                if (!manifest.ok()) ::_exit(140);
                auto quarantined = (*manifest)->QuarantineOrphan(candidate);
                if (!quarantined.ok()) ::_exit(141);
                ::_exit(142);
            }

            const int child_status = WaitForChild(child);
            ASSERT_TRUE(ChildWasKilled(child_status)) << child_status;
            auto reopened = PartitionManifest::Open(root);
            ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
            EXPECT_FALSE(std::filesystem::exists(root / candidate));
            EXPECT_TRUE(std::filesystem::is_regular_file(
                root / "segments/orphan.mino.orphan"));
        }
    }
    ReportScenario("orphan", rounds, cases, seed);
#else
    GTEST_SKIP() << "fork/SIGKILL storage campaign requires a POSIX platform";
#endif
}

class BlockingSyncState {
public:
    int Sync(int fd) noexcept {
        std::unique_lock lock(mutex_);
        entered_ = true;
        condition_.notify_all();
        condition_.wait(lock, [this] { return released_; });
        lock.unlock();
        while (true) {
#if defined(__APPLE__)
            const int result = ::fsync(fd);
#else
            const int result = ::fdatasync(fd);
#endif
            if (result == 0) break;
            if (errno != EINTR) return -1;
        }
        return 0;
    }

    bool WaitUntilEntered(std::chrono::milliseconds timeout) {
        std::unique_lock lock(mutex_);
        return condition_.wait_for(lock, timeout,
                                   [this] { return entered_; });
    }

    void Release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        condition_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool entered_ = false;
    bool released_ = false;
};

int BlockingSync(int fd, void* context) noexcept {
    return static_cast<BlockingSyncState*>(context)->Sync(fd);
}

BufferReservationRequest PoolRequest(
    uint64_t tag, BufferFullPolicy policy = BufferFullPolicy::kBlock,
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds::max()) {
    BufferReservationRequest request;
    request.topic_id = TopicId{1};
    request.payload_size = 1;
    request.user_tag = tag;
    request.full_policy = policy;
    request.timeout = timeout;
    return request;
}

bool ReserveAndCommit(RecorderBufferPool* pool, uint64_t tag) {
    auto reserved = pool->Reserve(PoolRequest(tag));
    if (!reserved.ok() || !reserved->accepted()) return false;
    reserved->reservation.bytes()[0] = static_cast<std::byte>(tag & 0xffu);
    return std::move(reserved->reservation).Commit().ok();
}

TEST(StorageFaultTest, PausedDiskKeepsBufferingBoundedAndAppliesPolicy) {
    RecorderBufferPoolOptions pool_options;
    pool_options.global_byte_limit = 12u * 1024u;
    pool_options.default_topic_byte_limit = pool_options.global_byte_limit;
    pool_options.queue_capacity = 2;
    auto pool_result = RecorderBufferPool::Create(pool_options);
    ASSERT_TRUE(pool_result.ok()) << pool_result.status().ToString();
    std::unique_ptr<RecorderBufferPool> pool = std::move(*pool_result);
    ASSERT_TRUE(ReserveAndCommit(pool.get(), 1));

    const std::filesystem::path root = TestDirectory("paused_disk");
    const std::filesystem::path path = root / "segment.mino";
    BlockingSyncState blocking;
    SegmentWriterOptions writer_options = PerRecordOptions();
    writer_options.data_sync_hook = BlockingSync;
    writer_options.io_hook_context = &blocking;
    auto writer =
        SegmentWriter::Create(path, SampleHeader(), 100, writer_options);
    ASSERT_TRUE(writer.ok()) << writer.status().ToString();
    SegmentWriter* writer_pointer = writer->get();

    std::atomic<StatusCode> consumer_status{StatusCode::kInternal};
    std::thread consumer([&] {
        auto handle = pool->Dequeue(1s);
        if (!handle.ok()) {
            consumer_status.store(handle.status().code(),
                                  std::memory_order_release);
            return;
        }
        Record record = SampleRecord(1, handle->size());
        std::copy(handle->bytes().begin(), handle->bytes().end(),
                  record.payload.begin());
        auto appended = writer_pointer->Append(record, 101);
        consumer_status.store(appended.ok() ? StatusCode::kOk
                                            : appended.status().code(),
                              std::memory_order_release);
        handle->Reset();
    });

    const bool disk_paused = blocking.WaitUntilEntered(1s);
    const bool second_committed = ReserveAndCommit(pool.get(), 2);
    const bool third_committed = ReserveAndCommit(pool.get(), 3);
    auto dropped = pool->Reserve(
        PoolRequest(4, BufferFullPolicy::kDropNewest, 0ns));
    auto timed_out =
        pool->Reserve(PoolRequest(5, BufferFullPolicy::kBlock, 25ms));
    const RecorderBufferPoolStats paused_stats = pool->stats();

    blocking.Release();
    consumer.join();

    ASSERT_TRUE(disk_paused);
    EXPECT_TRUE(second_committed);
    EXPECT_TRUE(third_committed);
    ASSERT_TRUE(dropped.ok()) << dropped.status().ToString();
    EXPECT_EQ(dropped->admission, BufferAdmission::kDroppedNewest);
    ASSERT_EQ(dropped->discarded.size(), 1u);
    EXPECT_EQ(dropped->discarded[0].user_tag, 4u);
    ASSERT_FALSE(timed_out.ok());
    EXPECT_EQ(timed_out.status().code(), StatusCode::kTimeout);
    EXPECT_EQ(paused_stats.bytes_in_use, pool_options.global_byte_limit);
    EXPECT_LE(paused_stats.allocated_bytes, pool_options.global_byte_limit);
    EXPECT_EQ(paused_stats.queued_records, pool_options.queue_capacity);
    EXPECT_EQ(paused_stats.dropped_newest_records, 1u);
    EXPECT_EQ(paused_stats.block_timeouts, 1u);
    EXPECT_EQ(consumer_status.load(std::memory_order_acquire), StatusCode::kOk);
    EXPECT_EQ((*writer)->durable_records(), 1u);

    auto append_buffered = [&](uint64_t expected_tag,
                               uint64_t ingestion_sequence) {
        auto handle = pool->TryDequeue();
        ASSERT_TRUE(handle.ok()) << handle.status().ToString();
        EXPECT_EQ(handle->user_tag(), expected_tag);
        Record record = SampleRecord(ingestion_sequence, handle->size());
        std::copy(handle->bytes().begin(), handle->bytes().end(),
                  record.payload.begin());
        ASSERT_TRUE((*writer)->Append(record, 200 + ingestion_sequence).ok());
        handle->Reset();
    };
    append_buffered(2, 2);
    append_buffered(3, 3);
    EXPECT_EQ(pool->TryDequeue().status().code(), StatusCode::kWouldBlock);
    EXPECT_EQ((*writer)->durable_records(), 3u);

    auto durable = ScanSegment(path);
    ASSERT_TRUE(durable.ok()) << durable.status().ToString();
    EXPECT_TRUE(durable->clean());
    EXPECT_EQ(durable->records_scanned, 3u);
    EXPECT_TRUE(durable->has_last_complete_sequence);
    EXPECT_EQ(durable->last_complete_sequence, 3u);
    EXPECT_EQ(durable->file_size, std::filesystem::file_size(path));
}

}  // namespace
}  // namespace mino::storage
