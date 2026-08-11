# Mino node deployment

This runbook covers the strict node and isolation configuration, deterministic
generator, preflight/start launcher, controlled node supervisor, health probes,
and container supply-chain contract. `//tools/deployment:mino_deploy` is the
preflight/PID-1 launcher; `//tools/deployment:mino_node` is a real local node
composition, not a validation shim. Deployment tools do not contain, print, or
copy TLS private keys. Credentials and namespace attestation are references to
files supplied as read-only runtime mounts.

## Build and test

```sh
bazel build //tools/mino:mino \
  //tools/deployment:mino_deploy \
  //tools/deployment:mino_node
bazel test //mino/config:deployment_config_test \
  //tools/deployment:deployment_contract_test \
  //tools/deployment:node_supervisor_smoke_test \
  //tools/deployment:container_smoke_test
```

All checked-in tests are bounded. The supervisor smoke uses `--dry-run` and
`--oneshot`; the optional container smoke runs only when
`MINO_CONTAINER_SMOKE_IMAGE` names an available image and otherwise reports an
explicit skip. No test leaves a node or monitoring server running.

## Strict node configuration

`mino/config/deployment_config.{h,cc}` parses schema version 1 TOML. Every root
and nested key is allowlisted; missing required fields, unknown keys, wrong
TOML types, values outside fixed bounds, and inconsistent cross-field budgets
are rejected. Configuration is limited to 1 MiB.

The checked-in golden example is `configs/node.production-edge.toml`.

| Table | Required contract |
| --- | --- |
| root | `schema_version = 1` |
| `node` | positive `id`; bounded token `name`, `environment`, and `role`; 100–120000 ms shutdown grace |
| `security_domain` | positive `id`, bounded token `name`, and explicit `trusted`; zero is never valid |
| `isolation` | non-root service `uid`/`gid`, bounded deployment `namespace`, absolute global `policy_file`, and absolute runtime-mounted `namespace_attestation_file` |
| `region` | positive `id`, bounded token `name`, and page-aligned 1 MiB–1 GiB `bytes` |
| `resources` | 1 MiB–1 TiB memory, 1 MiB–1 GiB SHM, 64–1048576 FDs, 1–65536 threads, and 0–65535 bridge connections; memory must cover SHM and SHM must cover the region |
| `bridge` | explicit enabled state, bounded address/port, positive expected peer security domain, and connection limit covered by the resource budget |
| `bridge.tls` | absolute `trust_anchors_file`, `certificate_chain_file`, and `private_key_file` references only |
| `monitoring` | Prometheus/OTLP enablement and bounded HTTP/aggregation limits |
| `supervisor` | explicit `local` mode, bounded built-in control Topic, and recorder role/recording identity agreement |
| `storage` | distinct absolute data, runtime, and schema directories plus 1 MiB–1 TiB minimum free space |

### One untrusted domain, one OS identity and namespace

`configs/security-domains.toml` is the deployment-wide isolation inventory. It
must enumerate every domain that can be scheduled into the deployment. Duplicate
SecurityDomain IDs are rejected. For every pair where either domain is
untrusted, UID, GID, and namespace must each be different; declaring two
untrusted domains with the same UID, the same GID, or the same namespace is a
hard configuration error.

This is intentionally stronger than checking `SecurityDomainId` in shared
memory. **`SecurityDomainId` only prevents accidental attachment and detects a
wrong declared identity; it is not a privilege boundary.** The actual local
boundary is the distinct OS UID/GID plus a separately provisioned container or
orchestrator namespace. Preflight checks the process EUID/EGID, exact inventory
binding, and the content of the read-only namespace attestation mount. The
attestation token records orchestrator intent; the orchestrator must still create
separate PID/IPC/mount namespaces and must not schedule two untrusted domains
into one namespace.

The inventory and node TOML must be root-controlled/read-only in production.
Never generate one inventory per node with only itself in order to evade global
uniqueness review. Trusted domains may share an identity only when both entries
are explicitly `trusted = true`; an untrusted domain never shares with either a
trusted or untrusted domain.

Any PEM begin marker in TOML is rejected before parsing. Keys such as
`private_key`, `private_key_pem`, or `certificate_chain_pem` are also rejected
as unknown. Secret values must not be placed in environment variables: mount
files and place only their absolute paths in TOML.

Validate a file without touching credentials:

```sh
bazel-bin/tools/deployment/mino_deploy validate \
  --config /etc/mino/node.toml
```

Exit code `3` means that the config file contract or parsed schema is invalid.
The tool rejects symlinked, multi-link, oversized, empty, or group/world-writable
configuration files.

## Deterministic generation

The generator supports `development`, `staging`, and `production` environments
and `core`, `edge`, and `recorder` role templates. IDs are always explicit.
Templates set bounded capacity defaults but never read or expand an environment
variable or secret.

```sh
bazel-bin/tools/deployment/mino_deploy generate \
  --environment production \
  --role edge \
  --node-id 1001 \
  --security-domain-id 7 \
  --region-id 17 \
  --service-uid 65532 \
  --service-gid 65532 \
  --namespace mino-domain-7 \
  --output node.toml
```

Without `--output`, canonical TOML is written to stdout. Repeated invocations
with the same arguments are byte-for-byte identical. `--output` creates a new
file with `O_EXCL` and does not overwrite an existing configuration. Review and
adjust capacity and absolute mount paths before deployment; add the exact domain
binding to the reviewed global isolation inventory; do not replace file
references with secret contents.

## Preflight and startup

Before checking storage, preflight verifies that the process EUID/GID exactly
match `[isolation]`, loads the global policy, matches ID/trust/UID/GID/namespace,
and reads the namespace attestation token. A matching `SecurityDomainId` without
those OS and namespace checks is rejected.

Create the three configured storage directories before startup. Each directory
must be a real directory owned by the service UID, grant owner `rwx`, and not be
world writable. The data filesystem must have at least `storage.min_free_bytes`
available.

When the bridge is enabled, all three TLS paths must resolve to non-empty,
single-link regular files of at most 1 MiB, owned by the service UID. The private
key must be `0600` or stricter (normally `0400`); trust anchors and certificate
chains must be owner-readable and not group/world writable. Symlink-based
projected secrets are intentionally rejected. Use a direct bind mount or CSI
provider that presents stable regular files with the service UID.

Preflight also checks `RLIMIT_NOFILE`, and, where supported, process/thread and
address-space limits. It verifies that the configured resource limits can host
the region. Run it directly with:

```sh
bazel-bin/tools/deployment/mino_deploy preflight \
  --config /etc/mino/node.toml
```

Start the checked-in concrete local node composition with an absolute executable
path:

```sh
tools/deployment/mino-node-start \
  /usr/local/bin/mino-node --config /etc/mino/node.toml
```

`mino-node --dry-run` compiles and validates its built-in control schema and
reports the planned components without creating runtime state. `--oneshot`
creates and starts the real components, then stops them immediately; it exists
for bounded smoke tests. Normal mode remains resident until SIGINT/SIGTERM.

`MINO_NODE_CONFIG` selects the deployment TOML and `MINO_DEPLOY_BIN` selects the
launcher binary for host packaging. Neither variable carries a secret. The
wrapper uses `exec`, and `mino-deploy start` owns the child lifecycle. It forwards
`SIGTERM`/`SIGINT`, waits up to `node.shutdown_grace_ms`, and sends `SIGKILL` only
after the grace period or after a second termination signal.

Use a bounded deployment check without starting the child:

```sh
MINO_NODE_CONFIG=/etc/mino/node.toml \
  tools/deployment/mino-node-start --dry-run /usr/local/bin/mino-node
```

The child path must be absolute, executable, a regular file, and not group/world
writable.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | validation, generation, preflight, dry-run, or probe succeeded |
| `2` | command-line usage error |
| `3` | config file or schema error |
| `4` | directory, credential, executable, free-space, or resource preflight failure |
| `5` | launcher/signal/fork/wait failure |
| `6` | health or readiness probe failure |
| child status | after successful launch, an ordinary child exit status is preserved |
| `128 + signal` | child termination or a signal forwarded by the launcher; `143` is SIGTERM |

## Controlled node supervisor and monitoring

`mino-node` assembles a real `LocalBusDeployment` with a bounded built-in
`mino/control` Topic and a real `MonitoringDeployment`. For the `recorder` role,
it also creates or opens a Recorder session at
`storage.data_dir/recorder`, installs the same control schema, starts Recorder,
pumps it in the supervisor loop, and stops it durably during shutdown. Core/edge
roles require `supervisor.recorder_enabled = false`; recorder role requires it to
be true with a positive recording ID.

The checked-in supervisor is deliberately local-only. It rejects
`bridge.enabled = true` because it does not own a `RemoteBridge`, TLS principal
loading, or distributed readiness proof. It also rejects OTLP enablement because
no bounded OTLP sink is injected. Use a separate bridge-capable composition for
those features; do not weaken these checks or claim that a configured-but-absent
Bridge is running. No `mino/security/**` or `mino/bridge/**` core behavior is
changed by this deployment layer.

Monitoring fields map one-for-one into `deployment::MonitoringConfig` and
`PrometheusHttpOptions`. Monitoring starts after LocalBus creation and stops
before process teardown. SIGTERM/SIGINT reaches `mino-deploy` as PID 1, is
forwarded to `mino-node`, and is escalated to SIGKILL only after
`node.shutdown_grace_ms`.

The existing monitoring endpoint exposes exactly:

- `GET /metrics` for Prometheus scraping;
- `GET /-/healthy` for process/monitoring liveness.

`mino-deploy probe --kind health` checks `/-/healthy`. `--kind readiness` first
repeats local preflight and then checks the same endpoint. This is **local
readiness only**: it proves that required files/resources remain usable and that
the monitoring endpoint is accepting requests. A distributed deployment must
add its node-registration/lease, topic, and bridge-specific readiness gate in
its orchestrator or concrete node composition. Mino does not currently expose a
`/-/ready` endpoint, and operators must not treat monitoring health alone as a
proof of Registry or peer readiness.

For Kubernetes, wire `livenessProbe.httpGet.path` to `/-/healthy` and the
configured monitoring port. Use an `exec` readiness probe invoking
`mino-deploy probe --kind readiness`, then combine it with the composition's
Registry/lease readiness before routing traffic. Keep the monitoring bind on
loopback unless an authenticated sidecar or network policy protects it.

## Hermetic multi-stage container

`tools/deployment/Dockerfile` has no mutable default images. Both build arguments
must be immutable `registry/name@sha256:<digest>` references. The build image
must contain Bazel and a C++20 compiler/runtime pair compatible with the selected
runtime image. `MODULE.bazel.lock` is enforced with `--lockfile_mode=error`;
Bazel downloads/actions remain only in the discarded build stage and are never
copied into the runtime image. The build stage actually compiles
`//tools/mino:mino`, `//tools/deployment:mino_deploy`, and
`//tools/deployment:mino_node`, then copies all three binaries into the runtime
stage. The Dockerfile rejects either base reference when it lacks `@sha256:`.

The image default is a real resident process:

```text
mino-deploy start --config /etc/mino/node.toml -- \
  /usr/local/bin/mino-node --config /etc/mino/node.toml
```

It is not `validate`. `mino-deploy` remains PID 1, performs preflight, forwards
SIGTERM/SIGINT, waits for the configured grace period, and reaps the child. The
checked-in container config is recorder/local-only so every configured component
is genuinely assembled. `tools/mino:mino` remains available for explicit
operations subcommands but is not presented as a daemon.

### Image contract and supply-chain evidence

`tools/deployment/image-contract.json` is the machine-readable contract for
binaries, default argv, UID/GID, read-only root, mounts, health, termination, and
secret delivery. OCI labels include source/revision/version/base references,
SBOM format, and the contract path. Supply release values through build args;
do not publish images with empty revision/version labels.

Use the release wrapper with digest-pinned bases and an installed `docker buildx`
and `syft`:

```sh
tools/deployment/build-image.sh \
  registry.example/mino:reviewed \
  registry.example/mino-build@sha256:<build-digest> \
  registry.example/mino-runtime@sha256:<runtime-digest> \
  out/mino-image
```

The command emits these evidence files and prints their paths:

- `image.iid`: BuildKit image ID;
- `buildkit-metadata.json`: BuildKit provenance/manifest metadata;
- `mino.spdx.json`: SPDX JSON SBOM generated by scanning the final image;
- `provenance.json`: image digest, source revision, dirty-worktree flag, both
  pinned base references, SBOM SHA-256, and BuildKit metadata SHA-256.

Retain all four files with the release. Production promotion must reject
`source_dirty = true`; the field exists so local evidence cannot silently claim
that an uncommitted source tree equals its HEAD commit. For registry publication,
also record the
registry-resolved manifest digest and verify it agrees with the BuildKit
`containerimage.digest`; never promote a tag alone. SBOM generation inspects the
local final image and does not send package contents to a service, but pulling
bases and publishing an image still have the registry's normal network,
authentication, rate-limit, and data-retention implications.

### Runtime hardening and mounts

The runtime runs as UID/GID `65532`, has no Linux capabilities requirement, and
is compatible with a read-only root filesystem. It contains no TLS key/cert
material. `.dockerignore` excludes common key/certificate formats and secret
directories; release automation must still scan the complete context and image
layers. Secrets are runtime files only—never Docker `ARG`, `ENV`, labels, copied
configuration values, or command-line values.

Run with explicit writable tmpfs/data mounts and a read-only attestation/secret
mount:

```sh
docker run --rm --read-only --user 65532:65532 \
  --cap-drop ALL --security-opt no-new-privileges \
  --mount type=bind,src=/srv/mino/node.toml,dst=/etc/mino/node.toml,readonly \
  --mount type=bind,src=/srv/mino/security-domains.toml,dst=/etc/mino/security-domains.toml,readonly \
  --mount type=bind,src=/srv/mino/data,dst=/var/lib/mino/data \
  --mount type=bind,src=/srv/mino/schemas,dst=/var/lib/mino/schemas \
  --mount type=tmpfs,dst=/run/mino,tmpfs-mode=0700 \
  --mount type=bind,src=/srv/mino/secrets,dst=/run/secrets/mino,readonly \
  registry.example/mino@sha256:<image-digest>
```

Provision writable mount ownership as UID/GID `65532`. The secret mount must at
least contain `security-domain.namespace` with exactly the configured namespace
token; a bridge-capable composition additionally mounts its CA, certificate, and
private key. Configure the orchestrator with separate PID/IPC/mount namespaces
for every untrusted domain and make its namespace inventory globally reviewed.
A token alone is not kernel isolation.

### Container smoke

After building or pulling an image locally, run the optional real-container
smoke:

```sh
bazel test //tools/deployment:container_smoke_test \
  --test_env=MINO_CONTAINER_SMOKE_IMAGE=registry.example/mino@sha256:<digest> \
  --test_output=all
```

The smoke starts with read-only root, UID/GID 65532, all capabilities dropped,
explicit tmpfs mounts, and a read-only namespace attestation. It waits for the
real `/-/healthy` endpoint, then sends Docker SIGTERM and requires the supervisor
log to contain `mino-node stopped cleanly`. If the environment variable or
Docker is unavailable, it prints `SKIP`; this is not release evidence.
