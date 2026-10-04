# IPsec transport (architecture §14.2)

Mino treats **IPsec as a kernel-offload path**, not a userspace ESP stack.
TLS 1.3 mTLS (`TcpDriver` + `TlsChannelFactory`) remains the in-tree D6 path;
IPsec is the alternate network protection option listed beside TLS and
controlled RDMA Fabric.

## Software pieces in this tree

| Component | Target | Role |
|---|---|---|
| `mino/security:ipsec` | `NetlinkXfrmSaProbe`, `TestLoopbackXfrmSession` | Query (and test-install) kernel XFRM SAs/policies via `NETLINK_XFRM` |
| `mino/security:socket_ipsec` | `SocketIpsecPolicy` | Install mandatory IN/OUT socket ESP policies before network I/O |
| `mino/transport:ipsec_transport` | `IpsecTransportDriver` | `TransportDriver` wrapper: mandatory policy/SA gate + forward to TCP/UDP |

`IpsecTransportDriver` does **not** encrypt payloads in process. The inner
driver (usually `TcpDriver` or `UdpDriver`) speaks plaintext to the socket;
the kernel applies ESP when matching XFRM state/policy exist.

## Enforcement modes

- `IpsecEnforcement::kRequireKernelSa` (**default**): Connect checks outbound
  protection; connected/accepted endpoints require both directions. Ordinary
  sockets recheck before every send and application receive. With mandatory
  socket policies and both SPIs pinned, checks are bounded by
  `protected_socket_recheck_ms` (default 1000, zero restores per-operation checks).
  Admission always checks. One background worker per wrapper refreshes admitted
  pinned connections after half the interval (at least 1 ms between retries).
  Send/Poll never run an expired pinned-connection probe: they return
  `kWouldBlock` until a fresh result is available, or `kUnavailable` after a
  failed check. Owned-send payloads remain with the caller on either error.
  Missing endpoints fail closed; failed or slow refreshes never extend the
  previous allow deadline.
- `NetlinkXfrmSaProbe` requires a matching policy with the effective priority,
  `ALLOW`, one mandatory ESP transport-mode template, and an encrypting SA with
  matching addresses, SPI (if specified), mode and reqid. IPv4/IPv6 prefixes,
  protocol and masked ports are checked. A higher-priority bypass/block policy,
  optional template, missing SA, truncated/interrupted dump, or ambiguous scope
  is rejected. Netlink send/receive calls have a one-second timeout; multipart
  dumps also check a one-second deadline between reads, so a stream of partial
  replies cannot hold the verifier indefinitely.
- The verifier supports ordinary **transport-mode** policies. Tunnel mode,
  stacked templates and policies scoped by marks, interfaces or security
  contexts require additional routing context and are rejected. Supply
  `ConnectRequest::local_bind` when policies constrain the local address; policies
  constraining the local port also require that port to be known.
- `IpsecEnforcement::kAssumedProtected`: without a probe, the operator explicitly
  assumes protection. With a probe, the same checks apply. This mode is intended
  for externally managed protection whose route context the verifier cannot
  establish; record it in the deployment runbook.

For ordinary sockets these are admission checks: they do not cancel queued
writes or heartbeats. Keep mandatory global policy installed through rekey or
use the socket guard below. The guard must be installed before connect/listen;
installing it only after Accept would leave the handshake unprotected.

## Mandatory socket policies and bounded verification

Configure `TcpDriverOptions::ipsec_policy` or `UdpDriverOptions::ipsec_policy`
before creating the inner driver. On Linux this installs `IP_XFRM_POLICY` or
`IPV6_XFRM_POLICY` for **both** directions before bind/connect/listen. Installation
errors close the socket. TCP children inherit listener policies; queued writes
and protocol heartbeats use the same protected socket. IPv6 is made V6-only to
prevent mapped IPv4 from missing an IPv6 selector. Other platforms fail with
`kUnsupported` when this option is used.

Example bindings for the SAs below (SPI values are host order):

```cpp
transport::TcpDriverOptions tcp;
tcp.ipsec_policy = security::SocketIpsecPolicy{
    .outbound_reqid = 0, .inbound_reqid = 0,
    .outbound_spi = 0x1001, .inbound_spi = 0x1002,
};
// Pass TcpDriver::Create(tcp) as IpsecTransportOptions::inner and supply a
// NetlinkXfrmSaProbe. Keep kRequireKernelSa; do not use a scripted production probe.
```

The key manager must install encrypting ESP transport-mode SAs with the matching
reqids/SPIs. Reqid zero is an exact outbound reqid, not a wildcard. Verification
checks the socket binding as well as global policy so an unrelated encrypting SA
cannot satisfy admission. Linux socket policy algorithm masks do not enforce a
non-null cipher; do not replace a live pinned SPI with a null-encryption SA.

Only connections with **both SPIs pinned** receive the query optimization. A
missing/expired pinned SA cannot fall back to another SPI or plaintext; rotate
keys using new SPIs and reconnect with new bindings. Zero SPIs permit SA selection
on rekey, retain mandatory ESP, and deliberately keep per-operation probes.
Without socket policies the wrapper also retains per-operation probes. Socket
policies do not expire on the recheck interval. Global policy removal may take
up to the configured interval to be reported by the wrapper, while the kernel
socket requirement remains. Socket policies take precedence over global SPD:
use connection close or an independent firewall rule for immediate traffic
revocation. The worker polls periodically; it does not subscribe to XFRM events.
A busy or slow verifier can pause expired connections even if their SAs remain
valid; size the interval for the number of connections and probe latency.
Closing/reusing an inner connection ID discards the old in-flight result.
Shutdown/destruction stops scheduling and joins the current probe before freeing
its state. Custom probes must support concurrent calls (including admission on
other threads) and complete within a bounded time. This is not an
IKE/key-management implementation.

Kernel implementation references: [socket policy compilation](https://github.com/torvalds/linux/blob/v6.8/net/xfrm/xfrm_user.c#L3285)
and [SA selection by reqid/SPI](https://github.com/torvalds/linux/blob/v6.8/net/xfrm/xfrm_state.c#L1072).

## Operator setup (external)

Example **transport-mode** ESP on a point-to-point pair (replace addresses,
SPIs, and keys; never commit real keys):

```bash
# Outbound SA (local -> peer)
sudo ip xfrm state add src 10.0.0.1 dst 10.0.0.2 proto esp spi 0x1001 \
  mode transport enc 'aes' 0x0123456789abcdef0123456789abcdef

# Inbound SA (peer -> local)
sudo ip xfrm state add src 10.0.0.2 dst 10.0.0.1 proto esp spi 0x1002 \
  mode transport enc 'aes' 0xfedcba9876543210fedcba9876543210

# Policies
sudo ip xfrm policy add src 10.0.0.1 dst 10.0.0.2 dir out \
  tmpl src 10.0.0.1 dst 10.0.0.2 proto esp mode transport
sudo ip xfrm policy add src 10.0.0.2 dst 10.0.0.1 dir in \
  tmpl src 10.0.0.2 dst 10.0.0.1 proto esp mode transport
```

IKE daemons (strongSwan / Libreswan) may install the same objects; Mino only
requires that `NetlinkXfrmSaProbe` can verify both mandatory policies and
covering SAs before admitting traffic.

## Composition sketch

```text
Bridge / Remote path
  └─ IpsecTransportDriver (kRequireKernelSa + NetlinkXfrmSaProbe)
       └─ TcpDriver (mandatory socket policies; pinned ESP SAs)
```

Wire-frame AEAD (`BridgePipeline` / `enable_aead`) remains orthogonal and may
still be layered on top of IPsec if policy requires application-level crypto.

## Tests

- `//mino/security:ipsec_test` — scripted probe, netlink dump/fail-closed,
  optional loopback SA install (`GTEST_SKIP` without `CAP_NET_ADMIN`), plus
  unprivileged parser/selector tests for policy absence, bypass, priority,
  optional ESP, SA binding, IPv4/IPv6, port mismatch and malformed attributes.
- `//mino/transport:ipsec_transport_test` — create validation, connect
  fail-closed, allowed probe end-to-end over loopback TCP, assumed mode,
  netlink fail-closed and post-connect revocation across all four send APIs.

## Non-goals

- Userspace ESP/IKE implementation
- Claiming hardware or carrier IPsec qualification
- Replacing mTLS for identity/ACL (IPsec protects the pipe; node principal
  still comes from TLS/`AuthenticatedPeer` when that path is enabled)

### Required kernel CI

The `ipsec-kernel` job in `.github/workflows/ci.yml` builds the Linux test and
runs `tools/ci/run_ipsec_kernel_test.sh` inside a disposable network namespace.
`MINO_REQUIRE_KERNEL_IPSEC=1` turns missing kernel/permission prerequisites into
failures. The runner verifies that all required cases ran with no skips and
uploads XML, logs and kernel identification. This includes removing policies
while keeping SAs, so an SA-only regression cannot pass. The ordinary test
matrix can still skip privileged cases on unprivileged hosts.

Kernel cases additionally exercise plaintext rejection, no-SA and wrong-SPI
send failure, policy removal with protected UDP delivery, IPv6 mapped-address
exclusion, TCP listener/accepted-socket protection, and bounded probe reuse with
failure refresh. These cases require loopback to be up inside the isolated
namespace. The CI runner enables it and checks that these cases actually ran.
