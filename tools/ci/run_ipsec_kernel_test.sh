#!/usr/bin/env bash
# Run only in a disposable network namespace; never alter host XFRM policy.
set -euo pipefail
if [[ $# != 2 ]]; then
  echo "usage: $0 IPSEC_TEST_BINARY EVIDENCE_DIRECTORY" >&2
  exit 2
fi
binary=$(realpath "$1")
mkdir -p "$2"
evidence=$(realpath "$2")
uname -a > "$evidence/kernel.txt"
sudo unshare --net -- bash -euc '
  ip link set lo up
  exec env MINO_REQUIRE_KERNEL_IPSEC=1 "$1" --gtest_filter="*" --gtest_output="xml:$2/results.xml"
' _ "$binary" "$evidence" 2>&1 | tee "$evidence/test.log"
python3 - "$evidence/results.xml" <<'PYXML'
import sys
import xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
cases = root.findall(".//testcase")
required = {"NetlinkProbeCreateAndList", "NetlinkFailClosedWithoutCoveringSa", "LoopbackInstallOrSkip", "MissingSaCannotFallBackToPlaintext", "GlobalPolicyRemovalKeepsSocketGuardAndRejectsPlaintext", "GuardedUdpAvoidsPerMessageProbe", "GuardedUdpRechecksAndDoesNotCacheFailure", "TcpListenerAndAcceptedSocketRemainGuarded", "Ipv6GuardDisablesMappedIpv4", "PinnedSpiCannotFallBackToAnotherSa"}
if not required.issubset({case.get("name") for case in cases}):
    raise SystemExit("required kernel test cases are missing")
for case in cases:
    if case.get("status") != "run" or case.get("result") != "completed" or any(
        case.find(tag) is not None for tag in ("skipped", "failure", "error")
    ):
        raise SystemExit(f"kernel test did not pass: {case.get('name')}")
print(f"Verified {len(cases)} kernel/policy tests; no skips or failures")
PYXML
