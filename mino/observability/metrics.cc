// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/metrics.h"

#include <algorithm>

namespace mino::observability {

bool MetricName::Assign(std::string_view name) noexcept {
    if (name.empty() || name.size() >= bytes_.size()) return false;
    const auto valid_first = [](char character) {
        return (character >= 'a' && character <= 'z') ||
               (character >= 'A' && character <= 'Z') || character == '_' ||
               character == ':';
    };
    const auto valid_rest = [&](char character) {
        return valid_first(character) ||
               (character >= '0' && character <= '9');
    };
    if (!valid_first(name.front())) return false;
    for (const char character : name.substr(1)) {
        if (!valid_rest(character)) return false;
    }
    std::fill(bytes_.begin(), bytes_.end(), '\0');
    std::copy(name.begin(), name.end(), bytes_.begin());
    size_ = name.size();
    return true;
}

bool MetricRegistry::NameExists(std::string_view name) const noexcept {
    for (size_t i = 0; i < counter_count_; ++i) {
        if (counters_[i].name_.view() == name) return true;
    }
    for (size_t i = 0; i < gauge_count_; ++i) {
        if (gauges_[i].name_.view() == name) return true;
    }
    for (size_t i = 0; i < histogram_count_; ++i) {
        if (histograms_[i].name_.view() == name) return true;
    }
    return false;
}

Status MetricRegistry::RegisterCounter(std::string_view name,
                                       CounterMetric** metric) noexcept {
    if (metric == nullptr || NameExists(name)) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    if (counter_count_ == counters_.size()) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
    CounterMetric& target = counters_[counter_count_];
    if (!target.name_.Assign(name)) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    ++counter_count_;
    *metric = &target;
    return Status::Ok();
}

Status MetricRegistry::RegisterGauge(std::string_view name,
                                     GaugeMetric** metric) noexcept {
    if (metric == nullptr || NameExists(name)) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    if (gauge_count_ == gauges_.size()) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
    GaugeMetric& target = gauges_[gauge_count_];
    if (!target.name_.Assign(name)) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    ++gauge_count_;
    *metric = &target;
    return Status::Ok();
}

Status MetricRegistry::RegisterHistogram(std::string_view name,
                                         HistogramMetric** metric) noexcept {
    if (metric == nullptr || NameExists(name)) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    if (histogram_count_ == histograms_.size()) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
    HistogramMetric& target = histograms_[histogram_count_];
    if (!target.name_.Assign(name)) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    ++histogram_count_;
    *metric = &target;
    return Status::Ok();
}

void MetricRegistry::TakeSnapshot(uint64_t timestamp_unix_ns,
                                  TelemetrySnapshot* snapshot) const noexcept {
    if (snapshot == nullptr) return;
    *snapshot = TelemetrySnapshot{};
    snapshot->timestamp_unix_ns = timestamp_unix_ns;
    snapshot->process_start_unix_ns = process_start_unix_ns_;
    snapshot->counter_count = counter_count_;
    snapshot->gauge_count = gauge_count_;
    snapshot->histogram_count = histogram_count_;
    for (size_t i = 0; i < counter_count_; ++i) {
        snapshot->counters[i].name = counters_[i].name_;
        snapshot->counters[i].value = counters_[i].counter_.Value();
    }
    for (size_t i = 0; i < gauge_count_; ++i) {
        snapshot->gauges[i].name = gauges_[i].name_;
        snapshot->gauges[i].value = gauges_[i].gauge_.Value();
    }
    for (size_t i = 0; i < histogram_count_; ++i) {
        snapshot->histograms[i].name = histograms_[i].name_;
        snapshot->histograms[i].value = histograms_[i].histogram_.Snapshot();
    }
}

}  // namespace mino::observability
