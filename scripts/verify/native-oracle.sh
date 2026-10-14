#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCENARIO="$REPO_ROOT/testkit/scenarios/m0-native.yaml"
ARTIFACT_ROOT="$REPO_ROOT/.artifacts/m0/native/$(date -u +%Y%m%dT%H%M%SZ)-$$"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scenario)
      [[ $# -ge 2 ]] || { echo "--scenario requires a path" >&2; exit 2; }
      SCENARIO="$2"
      shift 2
      ;;
    --artifact-root)
      [[ $# -ge 2 ]] || { echo "--artifact-root requires a path" >&2; exit 2; }
      ARTIFACT_ROOT="$2"
      shift 2
      ;;
    *)
      echo "usage: $0 [--scenario path] [--artifact-root path]" >&2
      exit 2
      ;;
  esac
done

if [[ "$SCENARIO" != /* ]]; then
  SCENARIO="$REPO_ROOT/$SCENARIO"
fi
if [[ "$ARTIFACT_ROOT" != /* ]]; then
  ARTIFACT_ROOT="$REPO_ROOT/$ARTIFACT_ROOT"
fi

mkdir -p "$ARTIFACT_ROOT"
RUN_ONE="$ARTIFACT_ROOT/run-one"
RUN_TWO="$ARTIFACT_ROOT/run-two"

python3 "$REPO_ROOT/scripts/verify/go-module-lock.py"

"$REPO_ROOT/scripts/dev/native-etcd.sh" run \
  --scenario "$SCENARIO" \
  --artifact-root "$RUN_ONE" \
  --trace "$RUN_ONE/trace.jsonl" \
  --metadata "$RUN_ONE/metadata.json"
"$REPO_ROOT/scripts/dev/native-etcd.sh" run \
  --scenario "$SCENARIO" \
  --artifact-root "$RUN_TWO" \
  --trace "$RUN_TWO/trace.jsonl" \
  --metadata "$RUN_TWO/metadata.json"

"$REPO_ROOT/scripts/dev/native-etcd.sh" compare \
  --left "$RUN_ONE/trace.jsonl" --right "$RUN_TWO/trace.jsonl"
"$REPO_ROOT/scripts/dev/native-etcd.sh" normalize \
  --input "$RUN_ONE/trace.jsonl" --output "$ARTIFACT_ROOT/normalized-one.jsonl"
"$REPO_ROOT/scripts/dev/native-etcd.sh" normalize \
  --input "$RUN_TWO/trace.jsonl" --output "$ARTIFACT_ROOT/normalized-two.jsonl"
cmp "$ARTIFACT_ROOT/normalized-one.jsonl" "$ARTIFACT_ROOT/normalized-two.jsonl"

expect_rejected() {
  local mutation="$1"
  local output="$ARTIFACT_ROOT/mutated-$mutation.jsonl"
  python3 - "$RUN_ONE/trace.jsonl" "$output" "$mutation" <<'PY'
import base64
import json
import sys

source, destination, mutation = sys.argv[1:]
with open(source, encoding="utf-8") as stream:
    records = [json.loads(line) for line in stream]

def record_for(case_id, method):
    for record in records:
        if record["case_id"] == case_id and record["method"] == method:
            return record
    raise SystemExit(f"mutation target missing: {case_id} {method}")

if mutation == "watch-event":
    target = next((record for record in records if record["method"] == "etcdserverpb.Watch/Watch" and record["response"] and record["response"].get("fields", {}).get("events")), None)
    if target is None:
        raise SystemExit("watch event mutation target missing")
    target["response"]["fields"]["events"].pop(0)
elif mutation == "more":
    target = record_for("C03", "etcdserverpb.KV/Range")
    fields = target["response"]["fields"]
    fields["more"] = not fields["more"]
elif mutation == "deleted":
    target = record_for("C09", "etcdserverpb.KV/DeleteRange")
    fields = target["response"]["fields"]
    fields["deleted"] = "0" if fields["deleted"] != "0" else "1"
elif mutation == "revision":
    target = next(record for record in records if record["method"] == "etcdserverpb.Watch/Watch" and record["response"] and record["response"].get("fields", {}).get("events"))
    kv_fields = target["response"]["fields"]["events"][0]["fields"]["kv"]["fields"]
    kv_fields["modRevision"] = str(int(kv_fields["modRevision"]) + 1)
elif mutation == "txn-branch":
    target = record_for("C11", "etcdserverpb.KV/Txn")
    fields = target["response"]["fields"]
    fields["succeeded"] = not fields["succeeded"]
elif mutation == "prev-kv":
    target = record_for("C09", "etcdserverpb.KV/DeleteRange")
    previous = target["response"]["fields"]["prevKvs"][0]["fields"]
    previous["value"] = base64.b64encode(b"changed").decode("ascii")
elif mutation == "stream-terminal":
    index = next(index for index, record in enumerate(records) if record["method"] == "etcdserverpb.Watch/Watch" and record["stream_terminal"] is not None)
    records.pop(index)
else:
    raise SystemExit(f"unknown mutation {mutation}")

with open(destination, "x", encoding="utf-8") as stream:
    for record in records:
        stream.write(json.dumps(record, separators=(",", ":")) + "\n")
PY
  if "$REPO_ROOT/scripts/dev/native-etcd.sh" compare \
    --left "$RUN_ONE/trace.jsonl" --right "$output" \
    >"$ARTIFACT_ROOT/mutated-$mutation.log" 2>&1; then
    echo "native compare incorrectly accepted $mutation mutation" >&2
    exit 1
  fi
}

for mutation in watch-event more deleted revision txn-branch prev-kv stream-terminal; do
  expect_rejected "$mutation"
done

"$REPO_ROOT/scripts/dev/go-m0.sh" -C testkit/go test ./internal/compare ./internal/scenario
"$REPO_ROOT/scripts/dev/go-m0.sh" -C testkit/go vet ./cmd/native-etcd ./internal/compare ./internal/scenario
echo "native etcd oracle passed; artifacts=$ARTIFACT_ROOT"
