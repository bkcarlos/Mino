# Mino 72-hour long-soak qualification — STARTED

## Timing (Asia/Shanghai, UTC+8)

- **Start (CST):** 2026-08-25 23:55:28 CST (UTC+8)
- **Start (UTC):** 2026-08-25 15:55:28.725Z
- **Expected end (CST):** 2026-08-28 23:55:28 CST (UTC+8)
- **Expected end (UTC):** 2026-08-28 15:55:28.725Z
- **Duration:** 259200 seconds (72 hours)

## Processes

- **Runner pid:** 359481 (`python3 tools/ci/run_long_soak.py ...`)
- **Probe pid:** 359486 (`/workspace/mino-results/mino-soak-probe --seed=20260825 --report-interval-ms=30000`)
- Runner PPID=23 (detached; launched with nohup + disown)

## Paths

- **Out dir:** `/workspace/mino-results/long-soak`
- **Runner log:** `/workspace/mino-results/long-soak-run.log`
- **Probe binary:** `/workspace/mino-results/mino-soak-probe` (copied from bazel-bin, mode 0755)
- **Repo:** `/workspace/Mino` @ `c977bd18ab67b17aa98406674ba482817e812beb`

## Exact command

```
python3 tools/ci/run_long_soak.py \
  --duration-seconds=259200 \
  --seed=20260825 \
  --probe=/workspace/mino-results/mino-soak-probe \
  --expected-commit=c977bd18ab67b17aa98406674ba482817e812beb \
  --out=/workspace/mino-results/long-soak
```

Launched from `/workspace/Mino` as:

```
nohup python3 tools/ci/run_long_soak.py \
  --duration-seconds=259200 \
  --seed=20260825 \
  --probe=/workspace/mino-results/mino-soak-probe \
  --expected-commit=c977bd18ab67b17aa98406674ba482817e812beb \
  --out=/workspace/mino-results/long-soak \
  >> /workspace/mino-results/long-soak-run.log 2>&1 < /dev/null &
disown
```

## Probe binary

- Reused existing `bazel-bin` artifact (k8-opt / `--config=release`).
- Confirmed with `bazel build --config=release --jobs=4 //benchmarks/soak_probe:soak_probe` → up-to-date (0.00s critical path).
- Installed: `install -m 0755 bazel-bin/benchmarks/soak_probe/soak_probe /workspace/mino-results/mino-soak-probe`

## Health at start (first ~90s)

- `--expected-commit` accepted (tree clean, HEAD match; no retry needed).
- Probe `ready` workloads: allocator, channel, bridge, storage, observability.
- Probe heartbeats at elapsed_ms 0 / 30000 / 60000 / 90000; operations 0 → 3.46M → 7.00M → 10.51M.
- Runner sample[0] at 60.004s: rss_bytes=22040576, fd_count=3, telemetry_dropped=0, slab_bytes_in_use=20480 (stable).
- manifest `outcome=running`, `exit_reason=runner_active`, `evidence_errors=[]`.
