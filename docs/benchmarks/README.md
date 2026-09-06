# benchmarks

Benchmark methodology and result reports.

- `Validation_Benchmark_Methodology.md`: V-14/V-15/V-16/V-17/V-18/V-27 methods and qualification rules.
- `validation_benchmark.schema.json`: machine-readable result schema.
- `validation_benchmark_pending.json`: explicit no-run (`PENDING`) report template.
- `Storage_SLA.md`: existing measured storage SLA and baseline.
- `Storage_Partition_Qualification.md`: D6-09/V-24 multi-process,
  multi-round NVMe/ext4 qualification, artifact contract, and fail-closed SLA
  policy.
- `kvm-2026-08-25/`: 2026-08-25 same-machine KVM campaign (pipeline comparison, remaining tests, unimplemented inventory, 72h soak start).
- `../optimization-status.md`: 同机 hop / BytesView / DecodeView / owned-send / opt-closeout / P8–P9 关闭状态与 intentional KEEP（无新测量数字；对照当前 tip）。
- `kvm-2026-08-25/UNIMPLEMENTED.md`: A1–A12 / 资格门 / KEEP 仍 incomplete 清单（对照 tip；含 A12 IPsec 软件子集与 A8 fail-closed 再评估，非仅 2026-08-25 战役快照）。
- `../operations/ipsec.md`: IPsec 内核 XFRM 运维与 `IpsecTransportDriver` 组合说明。
- `NUMA_Allocator_Qualification.md`: D6-02 local/interleave/remote method,
  provenance contract, and fail-closed physical-host qualification.
- `../large-object-pool.md`: D6-08 ordinary/HugePage/device-registration
  multi-size/lifecycle benchmark; dynamic provider, NUMA/memlock/device provenance;
  SLA policy, hashed artifact schema, tamper tests, and fail-closed self-hosted
  physical qualification.
- `../rdma-driver.md`: D6-06 TCP/UDP/RDMA Canonical Wire payload matrix,
  registered zero-copy mode, CPU/RTT/throughput fields, dynamic-provider ABI, and
  two-host physical link/provenance qualification.
- `../fabric-driver.md`: D6-07 TCP/RDMA/Fabric shared Canonical Wire payload
  matrix, IPCF/NTB/CXL window protocol, fault coverage, dynamic-provider ABI, and
  fail-closed two-host physical device/link qualification.
