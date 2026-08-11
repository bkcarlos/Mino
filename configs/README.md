# configs

Runtime configuration samples (logging, bus, recording, transport).
See the detailed design document section 20 for the configuration schema.

Configuration files use TOML. `mino.toml` contains the current logging schema;
application code logs through `//mino/common/logging:logging`, while
`InitializeLogging()` installs the default spdlog backend from the parsed TOML
configuration. The backend can be replaced by installing another `Logger`
implementation without changing call sites.

`mino.toml` also documents the bounded monitoring schema used by
`//mino/runtime/deployment:monitoring`. Prometheus defaults to loopback and OTLP
remains disabled until the process installs a bounded transactional sink. Static
Prometheus rules live in `configs/alerts/mino.rules.yml`; deployment and runbook
guidance is in `docs/operations/monitoring.md`.

`[allocator.numa]` documents process-local allocator placement. `policy` accepts
`default`, `local`, `node`, or `stripe`; `failure_policy` accepts `strict` or
`fallback`. Linux resolves topology from sysfs plus process/cgroup cpusets and
uses direct `mbind(2)` syscalls, without `libnuma`. Non-Linux and an allowed
single-node topology explicitly fall back. The setting must be supplied to
Region allocator creation by the process assembly; it is never persisted in the
SHM ABI.

`[allocator.large_objects]` documents the D6-08 extent size, registered-object
minimum, registration quota, stable recovery scope, and HugePage `strict` or
`fallback` policy. `[allocator.large_objects.registration]` defaults to disabled;
process assembly must inject a concrete `MemoryRegistrationProvider` before it can
be enabled. The built-in unavailable provider never pretends that DMA/RDMA
registration succeeded. See `docs/large-object-pool.md`.

`[transport.rdma]` is fail-closed and disabled by default. Production assembly must
load an approved absolute-path dynamic device plugin, choose a real device, provide
a stable MR recovery scope/process incarnation, and configure bounded SQ/RQ/CQ,
byte, and registration quotas. `authentication_mode = "verified_peer"` requires a
provider-verified CM principal. Controlled-fabric mode is available only through a
code-injected deployment attestor and is intentionally not enabled by a TOML string
alone. RDMA Bridge assembly still requires expected Security Domain and live
Coordinator Topic ACL checks. See `docs/rdma-driver.md`.

`[transport.fabric]` is likewise disabled and fail-closed. Production accepts only
an approved absolute-path `kDevice` IPCF/NTB/CXL plugin with active physical-link
facts. Process assembly must inject a controlled attestation verifier binding the
peer Node/Security Domain, provider provenance, both device identities, window-set
ID, window generation and session epoch. Mock providers exist only in Bazel test
targets; same-domain endpoints must use local SHM. See `docs/fabric-driver.md`.

Recorder Topic Partition settings live per topic in the runtime TOML exported as
`//tools/mino:runtime.example.toml`: `record_partitions`,
`record_partition_strategy` (`key`, `hash`, `source`, or `manual`) and the stable
`record_partition_hash_seed`. Bus assembly accepts `source` and `manual` because
canonical Bus messages do not expose an application partition key or precomputed
hash. `record.buffer_bytes` is a topic-wide conserved budget split into isolated
per-partition pools; increasing partition count therefore does not multiply the
configured memory budget. Partition count/strategy/seed changes require a new
manifest generation followed by drain and cutover.

`configs/capacity/` contains a complete two-Topic D6-15 input set: a strict
production-node budget, reviewed-shape production inventory, and matching
Coordinator Topic snapshot. It is deliberately marked `qualification_approved=false`
and uses hardware architecture `any`; it demonstrates a passing nonqualification
report but can never close production qualification. See
`docs/operations/capacity.md` for formulas, what-if controls, and the exact-set gate.

`node.production-edge.toml` is the schema-version-1 deployment golden generated
by `//tools/deployment:mino_deploy`. `node.container-recorder.toml` is the
local-only recorder composition used by the image default; unlike the edge
golden, it deliberately disables RemoteBridge because the checked-in supervisor
does not assemble one. Both contain only runtime-mounted file references.

`security-domains.toml` is the reviewed deployment-wide isolation inventory. For
any pair involving an untrusted Security Domain, UID, GID, and deployment
namespace must each be unique. Preflight matches the node declaration, actual
EUID/EGID, inventory entry, and read-only namespace attestation. A matching
`SecurityDomainId` alone only catches accidental attachment and is not an OS
security boundary. The strict schema, preflight rules, supervisor scope,
container mounts, image digest/SBOM provenance, and health/readiness wiring are
documented in `docs/operations/deployment.md`.
