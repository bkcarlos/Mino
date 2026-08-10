# Mino versus ZeroMQ transport benchmark

`layer_comparison_benchmark` compares Mino and ZeroMQ with one shared workload,
message generator, validator, timing path, phase protocol, and JSON schema. It
supports two-process echo latency/throughput and one-way delivery throughput;
run one server and one client with identical flags.
See [`PROFILE_ANALYSIS.md`](PROFILE_ANALYSIS.md) for the current concurrency
scaling results, syscall/CPU profiles, hot code paths, and optimization plan.

## Build

```sh
bazel build -c opt //benchmarks/transport:layer_comparison_benchmark
```

The target uses C++20, `-Wall -Wextra -Werror`, production `TcpDriver`, production
`WireFrameCodec`, and native libzmq.

## Run

Example (Mino L2, two lanes):

```sh
bazel-bin/benchmarks/transport/layer_comparison_benchmark \
  --backend=mino --layer=l2 --role=server --address=127.0.0.1 \
  --port=19090 --lane-count=2 --output=/tmp/mino-l2-server.json

bazel-bin/benchmarks/transport/layer_comparison_benchmark \
  --backend=mino --layer=l2 --role=client --address=127.0.0.1 \
  --port=19090 --lane-count=2 --output=/tmp/mino-l2-client.json
```

Use `--backend=zmq` for ZeroMQ and `--layer=l1` for the raw transport-body test.
Each backend/layer pair needs its own server process and non-overlapping port
range. Lane `n` uses `port + n`.

For one-way throughput, select one explicit topic count. DATA is not echoed, but
each lane's completion barrier is returned only after the server has received,
decoded, fully validated, and sequence-checked every preceding DATA frame:

```sh
bazel-bin/benchmarks/transport/layer_comparison_benchmark \
  --backend=mino --layer=l2 --role=client --address=192.168.1.10 \
  --port=19090 --lane-count=2 --mode=one_way --topic-count=50 \
  --messages-per-topic=1000 --warmup-messages-per-topic=100 \
  --payload-bytes=65536 --output=/tmp/mino-l2-one-way.json
```

Important options and defaults:

| Option | Default | Meaning |
|---|---:|---|
| `--payload-bytes` | 256 | Exact L1 body size; canonical L2 application payload size |
| `--lane-count` | 2 | Independent TCP ports/connections |
| `--messages-per-topic` | `64` | Measured messages for every topic |
| `--warmup-messages-per-topic` | `8` | Warmup messages excluded from all records |
| `--mode` | `all` | `all`, `serial`, `per_topic_concurrent`, or `one_way`; one-way requires an explicit topic count |
| `--topic-count` | `0` | `0` runs the default `1,2,4,8,16,32` echo curve; explicit values from 1 through 4096 select one count |
| `--outstanding` | `1` | Maximum in-flight messages per topic in `per_topic_concurrent` |
| `--deadline-seconds` | 120 | Whole-process monotonic safety deadline |
| `--output` | `transport_benchmark.json` | Result JSON path |

By default every run executes topic counts `1,2,4,8,16,32` in `serial` and
`per_topic_concurrent` modes. `--mode` and `--topic-count` can select one
record for profiling without changing that record's workload semantics. `serial` intentionally has one global in-flight
message; the concurrent mode applies the configured per-topic outstanding
window. One-way mode sends each sequence across all topics in round-robin order
and uses transport backpressure rather than per-message acknowledgements. Topics
always map to lanes as `topic % lane_count`, independent of any backend identity
or connection construction.

## Fairness contract

### L1

* Mino sends the exact benchmark body through production `TcpDriver`, which adds
  and removes only its normal four-byte stream framing prefix.
* ZeroMQ sends the exact same benchmark body as one native DEALER/ROUTER message
  frame.
* The body is exactly `payload_bytes` (minimum 80 because that is the production
  `TcpDriver` body bound), with the same deterministic 40-byte benchmark header
  and deterministic remainder.
* The server parses and verifies the complete body before echoing its original
  bytes. The client parses and verifies the complete echo before completing the
  RTT sample.

### L2

* Both backends carry byte-for-byte the same production `WireFrameCodec` body.
* Every benchmark message is a DATA frame with payload CRC and an application
  payload of exactly `payload_bytes`.
* Both server and client call production `WireFrameCodec::Decode`, which checks
  header and payload CRCs, then verify every fixed WireFrame field and the full
  deterministic application payload.
* The server echoes the received encoded body without re-encoding it.

### Timing and phases

The shared send path records `origin_ns` before L1 body construction or before
L2 `WireFrame` construction and `Encode`. An echo sample ends only after backend
receive, parsing/`Decode`, CRC checks where applicable, correlation, lane check,
and full deterministic payload verification. Thus L2 encode cost is included on
both backends. Server-side validation is always before echo.

One-way elapsed time starts before the first measured encode/send and ends after
the client receives all per-lane phase completion echoes. FIFO ordering makes
each completion cover all earlier DATA on that lane. The server additionally
checks the exact configured count and contiguous sequence for every topic.
One-way records intentionally set RTT percentiles to zero; throughput and elapsed
time are the meaningful fields.

Each measured record is isolated by explicit per-lane `phase-start`,
`warmup-done`, and `phase-done` request/echo barriers. Startup uses `hello` and
shutdown uses `stop`, `stop-done`, and a final one-way acknowledgement so queued
phase or stop traffic cannot leak into another stage. Mino uses one `TcpDriver`
instance (one production worker) for all lanes; ZeroMQ fixes `io_threads=1`.

## Output

Client and server, including failed runs, write the same top-level JSON schema.
Client records contain `mode`, `topic_count`, `sample_count`, `p50_rtt_us`,
`p95_rtt_us`, `p99_rtt_us`, `max_rtt_us`, `elapsed_us`, and
`messages_per_second`. The top level records backend/layer/scope, payload and
encoded-body sizes, lane count, warmup, outstanding, `messages_received`,
`messages_echoed`, outcome, and error.

## Limitations

* This is an end-to-end process-separated RTT/throughput benchmark, not a
    CPU-cycle microbenchmark. It can run over loopback or physical hosts. Scheduler
    placement, CPU frequency, NIC/link limits, and unrelated host activity remain
    sources of variance; pin processes externally when collecting
    publication-quality data.
* The transports do not expose equivalent low-level socket knobs. The benchmark
  applies bounded queues, keepalive/heartbeat, send/receive deadlines, and
  bounded cleanup, but does not claim identical internal batching or syscall
  behavior.
* DEALER/ROUTER necessarily carries a routing envelope on the server side. The
  measured application body and validation are identical, but that native
  ZeroMQ routing overhead is part of its transport result.
* Percentiles use nearest-rank over integer microsecond RTT samples. Warmup and
  phase-control traffic are excluded from records and throughput.
