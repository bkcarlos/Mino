# Mino observability

This package implements the bounded telemetry model from ADR-0009 without third-party runtime dependencies.

## Hot-path contract

The following operations are safe on message/data paths and perform no dynamic allocation, no system call, no clock read, and no global lock:

- `ShardedCounter::Add/Increment` (one relaxed atomic RMW);
- `ShardedGauge::Set` (one relaxed atomic store);
- `ShardedLogHistogram::Record` (two relaxed atomic RMWs);
- `TelemetryControl::{CountersEnabled,LatencyEnabled,ShouldTrace}`;
- `SidecarTraceQueue::TryPush` and `TelemetryTracer::TryRecordEvent`;
- `CrossNodeLatencyRecorder::Record`;
- `ExportPipeline::TrySubmit` (fixed-size snapshot copy into a bounded queue).

Callers provide a stable shard index, normally a worker index. Distinct active workers should use distinct shards to avoid false sharing. All counters use unsigned modulo arithmetic on overflow. Snapshots are weakly consistent: they merge relaxed atomics and need not correspond to one instant.

`BoundedQueue` is a fixed-storage lock-free MPMC queue. `TryPush` returns `false` immediately when full. Sidecar rejection only increments its dropped counter. `ExportPipeline::TrySubmit` similarly drops a snapshot; it never invokes an `Exporter`.

Metric registration and policy changes are cold-path operations. Registration is single-threaded and must finish before concurrent recording/snapshotting. Counter and Histogram values are process-lifetime cumulative and expose no runtime Reset operation; `process_start_unix_ns` identifies the cumulative epoch. Metric names are copied into 64-byte fixed storage and restricted to Prometheus-safe characters.

## Export isolation

`MetricRegistry::TakeSnapshot`, `Exporter::Export`, Prometheus formatting, and OTLP encoding are cold-path operations. Run `ExportPipeline::DrainOne` on an independent low-priority consumer thread. An exporter may block or fail on that thread, but it cannot block snapshot producers; queue saturation and exporter errors are counted separately.

`PrometheusTextExporter` streams Prometheus text exposition to a caller-owned `TextSink`. `OtlpJsonExporter` streams the OTLP/HTTP JSON mapping to a transactional `OtlpJsonSink`; the package performs no network I/O. A production OTLP sink must reserve bounded frame storage in `TryBegin`, make `TryAppend` non-blocking, and publish only on `Commit`.

`BridgeConnectionManagerOptions::telemetry` is the first production integration seam. When supplied, real pipeline send, receive, and reconnect completion stages are offered to a non-blocking `TraceEventSink`; null keeps the uninstrumented path free of telemetry clock reads.

## Clock rules

Local stage durations use monotonic timestamps supplied by the caller. `CrossNodeLatencyRecorder` accepts cross-node wall-clock subtraction only when `ClockQuality` is synchronized, in the same domain, within uncertainty and freshness thresholds, and no local wall/monotonic divergence indicates a clock jump. Uncertain, jump, and negative samples are counted separately and never enter the normal histogram.

## Validation

- `//mino/observability:metrics_test`
- `//mino/observability:tracing_test`
- `//mino/observability:clock_test`
- `//mino/observability:exporter_test`
- `//mino/observability:v23_telemetry_benchmark` compares baseline, Off, Counters, 1% Sampled, and Full modes. Pass an optional positive iteration count.
