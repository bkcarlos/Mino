// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_REPLAY_ENGINE_H_
#define MINO_STORAGE_REPLAY_ENGINE_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/result.h"
#include "mino/storage/recording_manifest.h"
#include "mino/storage/segment_format.h"
#include "mino/storage/segment_recovery.h"

namespace mino::storage {

struct ReplayValueRange {
    std::optional<uint64_t> minimum;
    std::optional<uint64_t> maximum;

    bool Contains(uint64_t value) const noexcept;
};

struct ReplayFilter {
    // IDs and names are unioned into one allowed topic set. An empty set means
    // all topics. Unknown names fail Create rather than silently replaying none.
    std::vector<uint32_t> topic_ids;
    std::vector<std::string> topic_names;
    std::vector<uint64_t> node_ids;
    ReplayValueRange ingestion_timestamp_ns;
    ReplayValueRange observed_timestamp_ns;
    ReplayValueRange source_sequence;
    ReplayValueRange ingestion_sequence;
};

enum class ReplayPlaybackMode : uint8_t {
    kTimed,
    kStep,
};

struct ReplayPlaybackOptions {
    ReplayPlaybackMode mode = ReplayPlaybackMode::kTimed;
    // 1.0 is original speed, >1.0 is faster, and 0 < speed < 1.0 is slower.
    double speed = 1.0;
};

enum class ReplayTimestampMode : uint8_t {
    kPreserveOriginal,
    kReplayClock,
};

enum class ReplayPublishTarget : uint8_t {
    kReplayNamespace,
    kLive,
};

struct ReplayOptions {
    ReplayFilter filter;
    ReplayPlaybackOptions playback;
    ReplayTimestampMode timestamp_mode = ReplayTimestampMode::kReplayClock;
    ReplayPublishTarget publish_target = ReplayPublishTarget::kReplayNamespace;
    std::string replay_namespace = "replay";
    std::string live_namespace = "live";
    bool live_injection_authorized = false;
    uint64_t replay_session_id = 0;
    SegmentFormatLimits format_limits{};
};

class ReplayClock {
public:
    virtual ~ReplayClock() = default;
    virtual uint64_t NowNs() noexcept = 0;
};

class ReplaySleeper {
public:
    virtual ~ReplaySleeper() = default;
    virtual Status SleepFor(uint64_t duration_ns) noexcept = 0;
};

class SystemReplayClock final : public ReplayClock {
public:
    uint64_t NowNs() noexcept override;
};

class SystemReplaySleeper final : public ReplaySleeper {
public:
    Status SleepFor(uint64_t duration_ns) noexcept override;
};

// A resolver may load descriptors or select a validated target representation.
// Returning a schema with different ref/version/layout is rejected: conversion
// rules belong in a higher layer and must never be inferred by ReplayEngine.
class ReplaySchemaResolver {
public:
    virtual ~ReplaySchemaResolver() = default;
    virtual Result<SchemaRefSnapshot> Resolve(
        uint32_t topic_id, uint32_t schema_ref, uint32_t schema_version,
        uint32_t layout_version) noexcept = 0;
};

// Resolves exactly against the immutable Topic Table/schema snapshot captured in
// the recording manifest.
class ManifestReplaySchemaResolver final : public ReplaySchemaResolver {
public:
    explicit ManifestReplaySchemaResolver(
        RecordingManifestSnapshot manifest) noexcept;

    Result<SchemaRefSnapshot> Resolve(
        uint32_t topic_id, uint32_t schema_ref, uint32_t schema_version,
        uint32_t layout_version) noexcept override;

private:
    RecordingManifestSnapshot manifest_;
};

enum class ReplayMessageOrigin : uint8_t {
    kReplay,
};

struct ReplayMessageMetadata {
    ReplayMessageOrigin message_origin = ReplayMessageOrigin::kReplay;
    uint64_t replay_session_id = 0;
    uint64_t original_ingestion_timestamp_ns = 0;
    uint64_t original_observed_timestamp_ns = 0;
    uint64_t replay_timestamp_ns = 0;
    uint64_t publish_timestamp_ns = 0;
    uint64_t original_source_sequence = 0;
    uint64_t original_ingestion_sequence = 0;
    uint64_t original_node_id = 0;
    uint64_t original_publisher_id = 0;
    uint64_t original_publisher_epoch = 0;
    // Preserves Tombstone/control semantics for the destination adapter. Gap
    // records are consumed as discontinuity metadata and are not published.
    uint16_t record_flags = 0;
};

struct ReplayPublishRequest {
    std::string_view target_namespace;
    std::string_view topic_name;
    uint32_t topic_id = 0;
    uint32_t partition_id = 0;
    const SchemaRefSnapshot& schema;
    std::span<const std::byte> canonical_payload;
    ReplayMessageMetadata metadata;
};

// Implementations must allocate a fresh destination object and publish it via
// the normal publisher API. Historical handles and ring slots are never passed
// through this interface. Request references are valid only during Publish.
class ReplayPublisherAdapter {
public:
    virtual ~ReplayPublisherAdapter() = default;
    virtual Status Publish(const ReplayPublishRequest& request) noexcept = 0;
};

struct ReplayOrderKey {
    uint64_t ingestion_timestamp_ns = 0;
    uint32_t topic_id = 0;
    uint32_t partition_id = 0;
    uint64_t ingestion_sequence = 0;
    uint64_t node_id = 0;
    uint64_t publisher_id = 0;
    uint64_t publisher_epoch = 0;
    uint64_t source_sequence = 0;
    uint64_t observed_timestamp_ns = 0;
};

// Total, deterministic ordering for simultaneously eligible partition heads.
// Receive time is primary; the remaining recorded identity fields break ties.
struct ReplayOrderLess {
    bool operator()(const ReplayOrderKey& left,
                    const ReplayOrderKey& right) const noexcept;
};

class SegmentReplayReader final {
public:
    static Result<std::unique_ptr<SegmentReplayReader>> Open(
        const std::filesystem::path& path,
        const SegmentFormatLimits& limits = {});

    ~SegmentReplayReader();
    SegmentReplayReader(const SegmentReplayReader&) = delete;
    SegmentReplayReader& operator=(const SegmentReplayReader&) = delete;
    SegmentReplayReader(SegmentReplayReader&&) = delete;
    SegmentReplayReader& operator=(SegmentReplayReader&&) = delete;

    const std::filesystem::path& path() const noexcept { return path_; }
    const SegmentHeader& segment_header() const noexcept {
        return report_.segment_header;
    }
    const std::vector<SegmentRecordOffset>& record_metadata() const noexcept {
        return report_.records;
    }

    // Reads exactly one complete envelope and runs DecodeRecord over all bytes.
    // EOF is represented by an empty optional. No payload is retained by the
    // reader after the returned Record is destroyed.
    Result<std::optional<Record>> Next();
    void Reset() noexcept { next_record_ = 0; }

private:
    SegmentReplayReader(std::filesystem::path path, int fd,
                        SegmentRecoveryReport report,
                        SegmentFormatLimits limits) noexcept;

    std::filesystem::path path_;
    int fd_ = -1;
    SegmentRecoveryReport report_;
    SegmentFormatLimits limits_;
    size_t next_record_ = 0;
};

class ReplayEngine final {
public:
    static Result<std::unique_ptr<ReplayEngine>> Create(
        std::vector<std::filesystem::path> segment_paths,
        RecordingManifestSnapshot manifest, ReplayPublisherAdapter* publisher,
        ReplayOptions options = {}, ReplaySchemaResolver* schema_resolver = nullptr,
        ReplayClock* clock = nullptr, ReplaySleeper* sleeper = nullptr);

    ~ReplayEngine();
    ReplayEngine(const ReplayEngine&) = delete;
    ReplayEngine& operator=(const ReplayEngine&) = delete;
    ReplayEngine(ReplayEngine&&) = delete;
    ReplayEngine& operator=(ReplayEngine&&) = delete;

    // Timed mode drains all matching records. Step mode must use Step().
    Result<size_t> Run();
    // Publishes exactly one matching record without sleeping. Returns false at
    // clean EOF. It is valid only when playback.mode is kStep.
    Result<bool> Step();

    size_t published_records() const noexcept { return published_records_; }

private:
    struct Impl;
    explicit ReplayEngine(std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> impl_;
    size_t published_records_ = 0;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_REPLAY_ENGINE_H_
