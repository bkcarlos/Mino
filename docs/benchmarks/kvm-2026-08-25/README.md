# 2026-08-25 same-machine KVM qualification

Same-host (8 vCPU KVM) campaign on commit `c977bd1`, plus the follow-up GCC 14 `transport_driver_test` compile fix.

| File | What it is |
|---|---|
| `REPORT.md` | Docker install, codegen, pipeline comparison, network smoke |
| `summary.json` | Machine-readable pipeline comparison numbers |
| `UNIMPLEMENTED.md` | D0–D6 gaps vs hardware/qualification remaining |
| `REMAINING-TESTS.md` | What this machine could and could not run |
| `remaining-tests-summary.json` | Machine-readable remaining-test inventory |
| `long-soak-STARTED.md` | 72h soak start record (still running at report time) |

Not included: raw bazel logs, soak probe binary, live soak artifacts.
