# IPsec transport (architecture §14.2)

Mino treats **IPsec as a kernel-offload path**, not a userspace ESP stack.
TLS 1.3 mTLS (`TcpDriver` + `TlsChannelFactory`) remains the in-tree D6 path;
IPsec is the alternate network protection option listed beside TLS and
controlled RDMA Fabric.

## Software pieces in this tree

| Component | Target | Role |
|---|---|---|
| `mino/security:ipsec` | `NetlinkXfrmSaProbe`, `TestLoopbackXfrmSession` | Query (and test-install) kernel XFRM SAs/policies via `NETLINK_XFRM` |
| `mino/transport:ipsec_transport` | `IpsecTransportDriver` | `TransportDriver` wrapper: fail-closed SA gate + forward to TCP/UDP |

`IpsecTransportDriver` does **not** encrypt payloads in process. The inner
driver (usually `TcpDriver` or `UdpDriver`) speaks plaintext to the socket;
the kernel applies ESP when matching XFRM state/policy exist.

## Enforcement modes

- `IpsecEnforcement::kRequireKernelSa` (**default**): `Connect` / optional
  `Listen` / `Accept` call `IpsecSaProbe::RequireProtection`. Missing covering
  SA → `StatusCode::kUnavailable` (fail-closed). Production should use
  `NetlinkXfrmSaProbe`.
- `IpsecEnforcement::kAssumedProtected`: operator asserts the interface is
  already IPsec-protected (VTI, policy-based strongSwan, etc.). If no probe is
  supplied, traffic is admitted without an SA check — document that choice in
  the deployment runbook. Prefer still attaching `NetlinkXfrmSaProbe`.

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
requires that `NetlinkXfrmSaProbe` can observe covering SAs before Bridge
connect.

## Composition sketch

```text
Bridge / Remote path
  └─ IpsecTransportDriver (kRequireKernelSa + NetlinkXfrmSaProbe)
       └─ TcpDriver  (plaintext sockets; kernel ESP)
```

Wire-frame AEAD (`BridgePipeline` / `enable_aead`) remains orthogonal and may
still be layered on top of IPsec if policy requires application-level crypto.

## Tests

- `//mino/security:ipsec_test` — scripted probe, netlink dump/fail-closed,
  optional loopback SA install (`GTEST_SKIP` without `CAP_NET_ADMIN`).
- `//mino/transport:ipsec_transport_test` — create validation, connect
  fail-closed, allowed probe end-to-end over loopback TCP, assumed mode,
  netlink fail-closed.

## Non-goals

- Userspace ESP/IKE implementation
- Claiming hardware or carrier IPsec qualification
- Replacing mTLS for identity/ACL (IPsec protects the pipe; node principal
  still comes from TLS/`AuthenticatedPeer` when that path is enabled)
