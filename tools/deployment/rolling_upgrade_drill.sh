#!/bin/sh
# D6-16 offline CLI safety rehearsal. Production progression is covered by the
# C++ integration test with a real supervisor-bound control plane; this script
# proves that files containing hand-written booleans cannot advance a manifest.
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <mino-binary>" >&2
  exit 2
fi
MINO="$1"
ROOT="${TEST_TMPDIR:-/tmp}/mino-rolling-upgrade-drill-$$"
trap 'rm -rf "$ROOT"' EXIT INT TERM
mkdir -m 700 "$ROOT"

OLD_DIGEST="0100000000000000000000000000000000000000000000000000000000000000"
NEW_DIGEST="0200000000000000000000000000000000000000000000000000000000000000"
PLAN="$ROOT/upgrade.plan"
EVIDENCE="$ROOT/offline.evidence"
MANIFEST="$ROOT/upgrade.manifest"
TOKEN="11111111111111111111111111111111"

cat >"$PLAN" <<EOF
operation_id=drill-offline-safety
commit_token=$TOKEN
source_region=/drill-old,101,1001,1002,6,77
target_region=/drill-new,202,2001,2002,6,77
required_shm_bytes=4096
required_publisher_slots=1
required_subscriber_slots=1
minimum_observation_samples=10
topic=1,2,drill/source,drill/target,1,1,1,2,1,2,1,1,11,1,1,12,2,1,$OLD_DIGEST,$NEW_DIGEST
topic_acl=0,91,77,31
EOF
chmod 600 "$PLAN"

cat >"$EVIDENCE" <<EOF
commit_token=$TOKEN
target_region=/drill-new,202,2001,2002,6,77
target_topic=2,drill/target,1,2,2,1,12,2,1,$NEW_DIGEST
prepared_ack=true
target_topics_ready=true
region_active=true
processes_ready=true
channels_ready=true
routes_ready=true
schema_bidirectionally_compatible=true
acl_exactly_preserved=true
capacity_admitted=true
available_shm_bytes=4096
available_publisher_slots=1
available_subscriber_slots=1
drain_ack=true
old_publishers_fenced=true
publishers=0
subscribers=0
pins=0
outstanding_receipts=0
outstanding_borrows=0
queue_depth=0
last_published_sequence=100
last_consumed_sequence=100
cutover_ack=true
old_publisher_count=0
new_publisher_count=1
observed_samples=10
duplicate_count=0
unexplained_loss_count=0
commit_ack=true
rollback_ack=true
target_publishers_fenced=true
source_ready=true
target_publications_after_cutover=0
sequence_receipt_reconciliation_complete=true
EOF
chmod 600 "$EVIDENCE"

"$MINO" upgrade plan --manifest "$MANIFEST" --plan "$PLAN" |
  grep -q '^dry-run:'
test ! -e "$MANIFEST"
"$MINO" upgrade plan --manifest "$MANIFEST" --plan "$PLAN" --apply >/dev/null

"$MINO" upgrade inspect --evidence "$EVIDENCE" |
  grep -q '^offline-evidence=inspect-only$'
"$MINO" upgrade execute --manifest "$MANIFEST" --evidence "$EVIDENCE" |
  grep -q 'offline-evidence=inspect-only dry-run:'

if "$MINO" upgrade execute --manifest "$MANIFEST" --evidence "$EVIDENCE" \
    --apply >/dev/null 2>&1; then
  echo "offline evidence unexpectedly advanced production upgrade" >&2
  exit 1
fi
if "$MINO" upgrade resume --manifest "$MANIFEST" --apply >/dev/null 2>&1; then
  echo "apply unexpectedly succeeded without production supervisor socket" >&2
  exit 1
fi
"$MINO" upgrade status --manifest "$MANIFEST" | grep -q '^phase=prepare$'

echo "rolling-upgrade CLI drill passed: offline evidence is inspect-only"
