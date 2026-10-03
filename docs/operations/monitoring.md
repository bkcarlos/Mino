# Mino monitoring and alerting

Mino exposes process-level, label-free operational metrics. Node IDs, topic names,
peer identities, and certificate subjects are intentionally not labels; select a
process with the Prometheus target labels assigned by service discovery. This
keeps series cardinality bounded and prevents untrusted identities from entering
the metrics surface.

## Production configuration

The sample in `configs/mino.toml` binds Prometheus to loopback and leaves OTLP
disabled until a bounded `OtlpJsonSink` is installed. `MonitoringDeployment`
accepts only fixed bounds: a 100 ms–60 s aggregation interval, a 16-snapshot OTLP
queue, up to 64 accepted/pending Prometheus connections, up to 16 workers, at
most 16 KiB of request headers, and at most 512 KiB of response data.

Prometheus supports exactly `GET /metrics` and `GET /-/healthy`. It closes every
connection after one request, rejects bodies and transfer encoding, and applies
read/write timeouts. Keep the default loopback bind. If a non-loopback bind is
required, place the endpoint behind a host firewall or authenticated sidecar;
the endpoint intentionally has no TLS or authentication stack of its own.

A caller should treat `MonitoringDeployment::Start()` failure as a monitoring
failure, log it, and continue the business service. Metric scraping, formatting,
OTLP encoding, and sink calls run only on monitoring-owned threads. The OTLP sink
contract is non-blocking and transactional: reserve bounded storage in
`TryBegin()`, reject immediately when full, and publish only in `Commit()`.

Install `configs/alerts/mino.rules.yml` with Prometheus rule configuration and
name the scrape job `mino`; the endpoint-down rule intentionally uses the fixed,
low-cardinality selector `up{job="mino"}`. Tune storage backlog, lease age, and
capacity floors to the deployment SLO before paging. The checked-in defaults are
conservative starting points.

## Initial triage

1. Confirm `GET /-/healthy` and `GET /metrics` from the same network namespace.
2. Record process start time, deployment revision, and the alert expression.
3. Inspect counters with `increase(...[5m])`; do not compare cumulative counters
   directly across process restarts.
4. Preserve logs and local diagnostic snapshots before restarting.
5. Never disable ACL/TLS validation or allocator corruption checks to clear an
   alert.

## Monitoring down

Check `mino_monitoring_up`, snapshot age, process liveness, bind conflicts, file
descriptor exhaustion, and endpoint bounds. A bind failure must not stop the data
plane. Restore scraping first; if OTLP alone failed, inspect its bounded sink and
collector without increasing the queue beyond the documented fixed capacity.

## Queue saturation

Compare `mino_queue_depth` with `mino_queue_capacity`, then inspect
`mino_queue_dropped_total`. Identify the overloaded process using Prometheus
target metadata rather than adding topic labels. Reduce ingress, restore the slow
consumer, or add capacity. Do not switch a lossless queue to dropping as an alert
workaround.

## Slab failure

Allocation failures usually indicate exhausted or fragmented shared memory.
Stop new admissions and inspect capacity/headroom. Any
`mino_slab_corruption_total` increase is a data-integrity incident: quarantine
the affected region, preserve it for inspection, and follow recovery tooling;
do not reuse it in place.

## Lease expiration

Check scheduler stalls, process pauses, and heartbeat age. Distinguish a dead
participant from a temporarily paused process before eviction. Repeated expiry
usually indicates CPU starvation, a broken monotonic clock assumption, or an
undersized lease duration.

## Bridge disconnected

Verify remote reachability, the bridge process, negotiated schema availability,
and reconnect counters. Inspect TLS alerts before changing reconnect policy. Do
not bypass authentication, ACL, schema, session epoch, or replay protection.

## Storage failure

Check filesystem free space/inodes, permissions, durable state, and storage
latency. Stop admitting recording work when writes fail. Preserve manifests and
pending journals; do not delete them merely to reduce backlog.

## Exporter failure

For Prometheus, inspect rejected/failure counters and request/response bounds.
For OTLP, compare queue depth/capacity and dropped/failure counters, then inspect
the caller-owned sink and collector. Export failure is isolated from the business
path; recover telemetry without making the sink blocking or unbounded.

## Capacity exhaustion

Compare headroom with the configured minimum and inspect rejection rate. Shed
optional work, release leaked reservations through normal rollback, or increase
the validated node budget. Preserve the emergency reserve and never decrement
accounting out of band.

## TLS and ACL

For handshake failures, verify time, trust roots, SAN/policy expectations, and
certificate validity. For expiry, rotate through the normal credential rollout.
For ACL spikes, identify the source from security logs and target metadata; Mino
does not expose node/topic/certificate identities as metric labels. Never weaken
mutual TLS or ACL policy solely to clear an alert.

## Automated drill

Run:

```sh
bazel test //configs/alerts:alert_rules_test \
  //mino/observability:monitoring_drill_test
```

The first target parses the rule file, validates the supported PromQL grammar,
runbooks, domain coverage, and metric registration contract. The second creates
the real operational registry, triggers representative queue/slab/TLS/exporter
signals, renders Prometheus text, and verifies that the checked-in alert names,
expressions, and emitted metrics are present.

## Durable dedup

Pass each `DedupStore` once in `MonitoringSources::dedup_stores`, even when
several bridge lanes share it. Stores must outlive monitoring. The exporter
publishes retained record count (`mino_dedup_sources`, active HWMs plus retirement
boundaries), retired publisher count (`mino_dedup_retired_publishers`), capacity, highest per-store utilization
(`mino_dedup_max_utilization_permille`, 800 means 80%), durable commit count,
commit failures, capacity rejections and cumulative persistence nanoseconds.
The maximum utilization metric prevents a full store from being hidden by
unused capacity in another store. Alert rules cover 80% utilization and failed
commits/capacity rejections. Failed commits prevent durable ACK progress.

When capacity rises, increase `DedupStoreOptions::max_sources` at the next
controlled restart, accounting for snapshot memory and write costs. Do not
expire durable identities by age or assume a new publisher epoch retires an old
one. Controlled offline retirement is available through
`DedupStore::RetireEpochsThrough({node_id, publisher_id, last_retired_epoch})`:

1. Stop publication for **every epoch at or below the chosen boundary** for that
   node/publisher, and drain reliable retransmissions to all relevant receivers.
   New publications must use an epoch greater than the boundary. The API does
   not infer this proof from a larger number or the current registry contents.
2. Destroy every pipeline attached to the store. Both retirement and `ReplaceAll`
   return `kUnavailable` while any pipeline is attached, including other lanes.
3. Retire the chosen range. One atomic snapshot replaces its HWMs with a durable
   rejection boundary. One boundary uses one capacity slot per node/publisher,
   so repeated retirement of that publisher coalesces without growing the map.
4. Recreate pipelines only after success. If the persistence attempt fails,
   close/reopen the store and inspect/retry the retirement before resuming:
   rename may have succeeded even though directory sync failed. The failed
   instance rejects new attachments, loads and writes to prevent overwriting
   a possibly committed boundary.

On restart/reconnect, data in retired ranges is rejected with
`kPermissionDenied` before schema buffering, publication or Accepted ACK.
Boundaries never appear in SessionHello's accepted-prefix list. `ReplaceAll`
preserves all existing boundaries and rejects attempts to reintroduce retired
HWMs; removing other entries still requires external fencing. Boundaries cannot
be expired or cleared through these APIs. Monitor their capacity consumption,
particularly when publisher IDs themselves change on every restart.

Snapshots without retirement remain format v1. The first retirement writes v2,
which retains the 16-byte header, sorted 32-byte records and trailing CRC32C.
A v2 record with HWM zero encodes the inclusive retirement epoch; other records
remain active HWMs. The decoder rejects duplicate publisher boundaries and HWMs
inside retired ranges. Existing v1 snapshots are readable by the new code;
older binaries reject v2 snapshots, so rollback must not discard these fences.
Online automatic retirement still requires a sender drain/retirement handshake
and durable sender state; this offline interface does not add that protocol.

A store holds an exclusive advisory lock on `<snapshot>.lock`. A second opener
returns `kAlreadyExists`; stop the old owner before reopening. Never unlink the
lock file while any owner is running. It is intentionally retained after close
and process death. Use a host-local filesystem with working `flock`, atomic
rename and directory fsync semantics; independent hard-link aliases and remote
filesystems are outside this snapshot protocol. A bridge window's age/capacity
eviction does not retire durable state: the pipeline restores the durable
prefix on the next frame for that source.


Durability recovery also fsyncs the snapshot directory before trusting a loaded
HWM: a previous writer may have renamed and then failed before directory sync.
New nested directories are synced at each ancestor, including their parent
entries. The dedup tests inject write-space and sync/rename errors, kill a writer before
and after each of four publication operations, and reject reopen when its
recovery sync fails. These are process-crash tests; they do not qualify power-loss
behavior of a storage device, controller cache or filesystem.
