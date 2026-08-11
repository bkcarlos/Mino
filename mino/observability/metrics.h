// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_OBSERVABILITY_METRICS_H_
#define MINO_OBSERVABILITY_METRICS_H_

#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <type_traits>

#include "mino/common/status.h"

namespace mino::observability {

constexpr size_t kMetricNameCapacity = 64;
constexpr size_t kMetricShards = 32;
constexpr size_t kLogHistogramBuckets = 65;
constexpr size_t kMaxSnapshotCounters = 64;
constexpr size_t kMaxSnapshotGauges = 32;
constexpr size_t kMaxSnapshotHistograms = 32;

static_assert(std::atomic<uint64_t>::is_always_lock_free,
              "Mino metrics require lock-free 64-bit atomics");

class MetricName {
public:
    bool Assign(std::string_view name) noexcept;
    const char* c_str() const noexcept { return bytes_.data(); }
    std::string_view view() const noexcept { return {bytes_.data(), size_}; }

private:
    std::array<char, kMetricNameCapacity> bytes_{};
    size_t size_ = 0;
};

template <size_t Shards = kMetricShards>
class ShardedCounter {
    static_assert(Shards > 0);

public:
    // Single-writer fixed-shard view. It replaces a contended atomic RMW with
    // local arithmetic plus a relaxed atomic store, while every snapshot still
    // observes a monotonic cumulative shard value. Exactly one LocalShard may
    // write a shard at a time; use Add() when writers must share a shard.
    class LocalShard {
    public:
        LocalShard() noexcept = default;
        LocalShard(const LocalShard&) = delete;
        LocalShard& operator=(const LocalShard&) = delete;
        LocalShard(LocalShard&&) noexcept = default;
        LocalShard& operator=(LocalShard&&) noexcept = default;

        void Add(uint64_t value) noexcept {
            value_ += value;
            target_->store(value_, std::memory_order_relaxed);
        }
        void Increment() noexcept { Add(1); }
        uint64_t value() const noexcept { return value_; }

    private:
        friend class ShardedCounter;
        explicit LocalShard(std::atomic<uint64_t>* target) noexcept
            : target_(target),
              value_(target->load(std::memory_order_relaxed)) {}

        std::atomic<uint64_t>* target_ = nullptr;
        uint64_t value_ = 0;
    };

    // Thread-local batching for counters whose snapshot boundary is explicit.
    // Flush is lossless and uses one relaxed RMW for the whole batch. Callers
    // must Flush before a snapshot that is required to include their current
    // batch; the destructor is a final conservation backstop.
    class LocalBatch {
    public:
        LocalBatch() noexcept = default;
        LocalBatch(const LocalBatch&) = delete;
        LocalBatch& operator=(const LocalBatch&) = delete;
        LocalBatch(LocalBatch&& other) noexcept
            : target_(other.target_), pending_(other.pending_),
              updates_(other.updates_), batch_size_(other.batch_size_) {
            other.target_ = nullptr;
            other.pending_ = 0;
            other.updates_ = 0;
        }
        LocalBatch& operator=(LocalBatch&& other) noexcept {
            if (this == &other) return *this;
            Flush();
            target_ = other.target_;
            pending_ = other.pending_;
            updates_ = other.updates_;
            batch_size_ = other.batch_size_;
            other.target_ = nullptr;
            other.pending_ = 0;
            other.updates_ = 0;
            return *this;
        }
        ~LocalBatch() { Flush(); }

        void Add(uint64_t value) noexcept {
            pending_ += value;
            if (++updates_ == batch_size_) Flush();
        }
        void Increment() noexcept { Add(1); }
        // For a caller that already owns a fixed batch boundary. This is only
        // local arithmetic; Flush() must be called at that boundary.
        void Accumulate(uint64_t value) noexcept {
            pending_ += value;
            updates_ = 1;
        }
        void Flush() noexcept {
            if (target_ == nullptr || updates_ == 0) return;
            target_->fetch_add(pending_, std::memory_order_relaxed);
            pending_ = 0;
            updates_ = 0;
        }
        uint64_t pending() const noexcept { return pending_; }

    private:
        friend class ShardedCounter;
        LocalBatch(std::atomic<uint64_t>* target, uint32_t batch_size) noexcept
            : target_(target), batch_size_(batch_size == 0 ? 1 : batch_size) {}

        std::atomic<uint64_t>* target_ = nullptr;
        uint64_t pending_ = 0;
        uint32_t updates_ = 0;
        uint32_t batch_size_ = 1;
    };

    LocalShard BindLocalShard(size_t shard) noexcept {
        return LocalShard(&shards_[shard % Shards].value);
    }
    LocalBatch BindLocalBatch(size_t shard, uint32_t batch_size = 256) noexcept {
        return LocalBatch(&shards_[shard % Shards].value, batch_size);
    }

    void Add(uint64_t value, size_t shard) noexcept {
        shards_[shard % Shards].value.fetch_add(value,
                                                std::memory_order_relaxed);
    }
    void Increment(size_t shard) noexcept { Add(1, shard); }

    uint64_t Value() const noexcept {
        uint64_t result = 0;
        for (const auto& shard : shards_) {
            result += shard.value.load(std::memory_order_relaxed);
        }
        return result;
    }

private:
    struct alignas(64) Shard {
        std::atomic<uint64_t> value{0};
    };
    std::array<Shard, Shards> shards_{};
};

// Bounded sharded gauge. Each owner sets its shard to the current contribution;
// snapshots sum all contributions. There is no dynamic label or allocation.
template <size_t Shards = kMetricShards>
class ShardedGauge {
    static_assert(Shards > 0);

public:
    void Set(uint64_t value, size_t shard) noexcept {
        shards_[shard % Shards].value.store(value, std::memory_order_relaxed);
    }

    uint64_t Value() const noexcept {
        uint64_t result = 0;
        for (const auto& shard : shards_) {
            result += shard.value.load(std::memory_order_relaxed);
        }
        return result;
    }

private:
    struct alignas(64) Shard {
        std::atomic<uint64_t> value{0};
    };
    std::array<Shard, Shards> shards_{};
};

struct LogHistogramSnapshot {
    std::array<uint64_t, kLogHistogramBuckets> buckets{};
    uint64_t count = 0;
    uint64_t sum = 0;
};

template <size_t Shards = kMetricShards>
class ShardedLogHistogram {
    static_assert(Shards > 0);

public:
    static constexpr size_t BucketFor(uint64_t value) noexcept {
        return value == 0
                   ? 0
                   : static_cast<size_t>(64 - std::countl_zero(value));
    }

    static constexpr uint64_t UpperBound(size_t bucket) noexcept {
        if (bucket == 0) return 0;
        if (bucket >= 64) return std::numeric_limits<uint64_t>::max();
        return (uint64_t{1} << bucket) - 1;
    }

    void Record(uint64_t value, size_t shard) noexcept {
        Shard& target = shards_[shard % Shards];
        target.buckets[BucketFor(value)].fetch_add(1,
                                                  std::memory_order_relaxed);
        target.sum.fetch_add(value, std::memory_order_relaxed);
    }

    LogHistogramSnapshot Snapshot() const noexcept {
        LogHistogramSnapshot result;
        for (const auto& shard : shards_) {
            result.sum += shard.sum.load(std::memory_order_relaxed);
            for (size_t bucket = 0; bucket < kLogHistogramBuckets; ++bucket) {
                const uint64_t value =
                    shard.buckets[bucket].load(std::memory_order_relaxed);
                result.buckets[bucket] += value;
                result.count += value;
            }
        }
        return result;
    }

private:
    struct alignas(64) Shard {
        std::array<std::atomic<uint64_t>, kLogHistogramBuckets> buckets{};
        std::atomic<uint64_t> sum{0};
    };
    std::array<Shard, Shards> shards_{};
};

struct CounterSnapshot {
    MetricName name;
    uint64_t value = 0;
};

struct GaugeSnapshot {
    MetricName name;
    uint64_t value = 0;
};

struct HistogramSnapshot {
    MetricName name;
    LogHistogramSnapshot value;
};

struct TelemetrySnapshot {
    uint64_t timestamp_unix_ns = 0;
    uint64_t process_start_unix_ns = 0;
    size_t counter_count = 0;
    size_t gauge_count = 0;
    size_t histogram_count = 0;
    std::array<CounterSnapshot, kMaxSnapshotCounters> counters{};
    std::array<GaugeSnapshot, kMaxSnapshotGauges> gauges{};
    std::array<HistogramSnapshot, kMaxSnapshotHistograms> histograms{};
};

static_assert(std::is_nothrow_copy_assignable_v<TelemetrySnapshot>);

class CounterMetric {
public:
    ShardedCounter<>& counter() noexcept { return counter_; }
    const MetricName& name() const noexcept { return name_; }

private:
    friend class MetricRegistry;
    MetricName name_;
    ShardedCounter<> counter_;
};

class GaugeMetric {
public:
    ShardedGauge<>& gauge() noexcept { return gauge_; }
    const MetricName& name() const noexcept { return name_; }

private:
    friend class MetricRegistry;
    MetricName name_;
    ShardedGauge<> gauge_;
};

class HistogramMetric {
public:
    ShardedLogHistogram<>& histogram() noexcept { return histogram_; }
    const MetricName& name() const noexcept { return name_; }

private:
    friend class MetricRegistry;
    MetricName name_;
    ShardedLogHistogram<> histogram_;
};

// Registration is cold-path and single-threaded. Counter and histogram values
// are process-lifetime cumulative and intentionally have no runtime Reset API;
// process_start_unix_ns lets cumulative exporters distinguish restarts.
class MetricRegistry {
public:
    explicit MetricRegistry(uint64_t process_start_unix_ns = 0) noexcept
        : process_start_unix_ns_(process_start_unix_ns) {}

    Status RegisterCounter(std::string_view name, CounterMetric** metric) noexcept;
    Status RegisterGauge(std::string_view name, GaugeMetric** metric) noexcept;
    Status RegisterHistogram(std::string_view name,
                             HistogramMetric** metric) noexcept;
    void TakeSnapshot(uint64_t timestamp_unix_ns,
                      TelemetrySnapshot* snapshot) const noexcept;

    size_t counter_count() const noexcept { return counter_count_; }
    size_t gauge_count() const noexcept { return gauge_count_; }
    size_t histogram_count() const noexcept { return histogram_count_; }

private:
    bool NameExists(std::string_view name) const noexcept;

    uint64_t process_start_unix_ns_ = 0;
    std::array<CounterMetric, kMaxSnapshotCounters> counters_{};
    std::array<GaugeMetric, kMaxSnapshotGauges> gauges_{};
    std::array<HistogramMetric, kMaxSnapshotHistograms> histograms_{};
    size_t counter_count_ = 0;
    size_t gauge_count_ = 0;
    size_t histogram_count_ = 0;
};

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_METRICS_H_
