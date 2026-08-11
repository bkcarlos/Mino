# Mino observability

This package implements the bounded telemetry model from ADR-0009 without third-party runtime dependencies.

## Hot-path contract

The following operations are safe on message/data paths and perform no dynamic allocation, no system call, no clock read, and no global lock:

- `ShardedCounter::Add/Increment` (shared-shard relaxed RMW), `LocalShard` (single-writer relaxed store), and `LocalBatch` (thread-local periodic relaxed merge);
- `ShardedGauge::Set` (one relaxed atomic store);
- `ShardedLogHistogram::Record` (two relaxed atomic RMWs);
- cached `TelemetryControl::Evaluate` (immediate one-epoch-load decisions), or fixed-batch `Synchronize` + `EvaluateCached`; fixed-source batches use `Synchronize(cache, topic, source)` + `EvaluateSequenceCached` so the timed boundary performs the epoch/prefix work once and messages perform no policy load or cache mutation;
- `SidecarTraceQueue::TryPush` and `TelemetryTracer::TryRecordEvent`;
- `CrossNodeLatencyRecorder::Record`;
- `ExportPipeline::TrySubmit` (fixed-size snapshot copy into a bounded queue).

Callers provide a stable shard index, normally a worker index. Distinct active workers must use distinct `LocalShard`/`LocalBatch` shards; use `Add` when writers share a shard. `LocalShard` publishes every cumulative value with a relaxed store. `LocalBatch` is lossless but the owner must `Flush` at a required snapshot boundary (and its destructor performs a final flush); the V-23 producer flushes inside the measured batch. All counters use unsigned modulo arithmetic on overflow. Registry snapshots are weakly consistent across shards but conserve every published/explicitly flushed increment.

Each worker owns one `TelemetryThreadCache`. `Evaluate` observes the dynamic policy epoch once per message. Throughput-oriented paths call `Synchronize` at a fixed batch boundary and use `EvaluateCached` inside that batch; fixed-topic/source paths use the fixed-source overload and `EvaluateSequenceCached`, making the complete policy epoch and sample prefix immutable for the batch. V-23 uses 256 operations, includes the epoch check in the timer, and therefore bounds policy activation delay to 256 operations without per-message atomic loads or cache writes. These forms retain the last coherent policy while a writer owns the odd seqlock epoch and emit a fully initialized `TraceDecision` only for sampled messages. Pass that decision to `MakeTraceContext` and `TelemetryTracer::TryRecordSampledEvent`; both trace IDs, the policy epoch, limiter values, and mode are reused without a second policy load or key hash. No cache, policy snapshot, counter, histogram, or sidecar operation owns a `shared_ptr`, lock, or dynamically allocated hot-path object.

`BoundedQueue` is a fixed-storage lock-free MPMC queue. `TryPush` returns `false` immediately when full. Sidecar rejection only increments its dropped counter. `ExportPipeline::TrySubmit` similarly drops a snapshot; it never invokes an `Exporter`.

Metric registration and policy changes are cold-path operations. Registration is single-threaded and must finish before concurrent recording/snapshotting. Counter and Histogram values are process-lifetime cumulative and expose no runtime Reset operation; `process_start_unix_ns` identifies the cumulative epoch. Metric names are copied into 64-byte fixed storage and restricted to Prometheus-safe characters.

## Export isolation

`MetricRegistry::TakeSnapshot`, `Exporter::Export`, Prometheus formatting, and OTLP encoding are cold-path operations. Run `ExportPipeline::DrainOne` on an independent low-priority consumer thread. An exporter may block or fail on that thread, but it cannot block snapshot producers; queue saturation and exporter errors are counted separately.

`PrometheusTextExporter` streams Prometheus text exposition to a caller-owned `TextSink`. `PrometheusHttpEndpoint` is the production socket assembly: it supports only `GET /metrics` and `GET /-/healthy`, serves one request per connection, and bounds request bytes, header count, response bytes, active/pending connections, worker count, and all socket waits. `Stop()` closes the listener and interrupts active sockets before joining its threads.

`OtlpJsonExporter` streams the OTLP/HTTP JSON mapping to a transactional `OtlpJsonSink`; the package performs no network I/O. A production OTLP sink must reserve bounded frame storage in `TryBegin`, make `TryAppend` non-blocking, and publish only on `Commit`. `ExportPipelineMetrics` exposes submitted, dropped, exported, failed, depth, and capacity health without adding labels.

`RegisterOperationalMetrics` installs the process-wide alert contract. It intentionally has no dynamic labels: node, topic, peer, and certificate identities belong in bounded service-discovery target metadata and security logs, not metric series.

`BridgeConnectionManagerOptions::telemetry` is the first production integration seam. When supplied, real pipeline send, receive, and reconnect completion stages are offered to a non-blocking `TraceEventSink`; null keeps the uninstrumented path free of telemetry clock reads.

## Clock rules

Local stage durations use monotonic timestamps supplied by the caller. `CrossNodeLatencyRecorder` accepts cross-node wall-clock subtraction only when `ClockQuality` is synchronized, in the same domain, within uncertainty and freshness thresholds, and no local wall/monotonic divergence indicates a clock jump. Uncertain, jump, and negative samples are counted separately and never enter the normal histogram.

## Validation

- `//mino/observability:metrics_test`
- `//mino/observability:tracing_test`
- `//mino/observability:clock_test`
- `//mino/observability:exporter_test`
- `//mino/observability:prometheus_http_endpoint_test`
- `//mino/observability:monitoring_drill_test`
- `//configs/alerts:alert_rules_test`
- `//mino/observability:v23_telemetry_benchmark` reports compile-off/runtime-Off/Counters/1% Sampled/Full for both the ADR-0009 real SPSC publish operation and a separate non-acceptance micro-op diagnostic. Defaults are 1M operations, 7 rounds, and 5 fresh worker processes; override with `--iterations=N --rounds=N --processes=N` (a lone positive iteration count remains supported). Relative results use symmetric `A-B-A`/`B-A-B` compile-off pairs and 95% Student-t confidence intervals across process means. `--json=PATH` writes schema `mino.v23_telemetry_benchmark.v1` with every process/round observation, paired baseline, counter conservation, accepted/drop counts, calibration, and summaries. The binary exits non-zero if the Counters/Sampled upper bound fails, a worker is invalid, or the artifact cannot be written.
