#!/usr/bin/env bash
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
CASES="$ROOT/testkit/cases/m0-wire.json"
ARTIFACT_ROOT="$ROOT/.artifacts/m0/wire/$(date -u +%Y%m%dT%H%M%SZ)-$$"

usage() {
  printf '%s\n' 'Usage: scripts/verify/wire.sh [--cases PATH] [--artifact-root PATH]' >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cases)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      CASES="$2"
      shift 2
      ;;
    --artifact-root)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      ARTIFACT_ROOT="$2"
      shift 2
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

[[ "$CASES" = /* ]] || CASES="$ROOT/$CASES"
[[ "$ARTIFACT_ROOT" = /* ]] || ARTIFACT_ROOT="$ROOT/$ARTIFACT_ROOT"
[[ -r "$CASES" ]] || { echo "case manifest is not readable: $CASES" >&2; exit 2; }

python3 - "$CASES" <<'PY'
import json
import sys

manifest = json.load(open(sys.argv[1], encoding="utf-8"))
if manifest.get("schema_version") != 1 or manifest.get("protocol") != "etcd-v3.5.23":
    raise SystemExit("wire case manifest has an unsupported schema or protocol")
methods = {item.get("method"): item for item in manifest.get("descriptor_methods", [])}
expected = {
    "etcdserverpb.KV/Range": (False, False),
    "etcdserverpb.Watch/Watch": (True, True),
    "etcdserverpb.Lease/LeaseKeepAlive": (True, True),
}
for name, (client_streaming, server_streaming) in expected.items():
    method = methods.get(name)
    if method is None:
        raise SystemExit(f"wire case manifest is missing {name}")
    if (method.get("client_streaming"), method.get("server_streaming")) != (
        client_streaming,
        server_streaming,
    ):
        raise SystemExit(f"wire case manifest has wrong streaming shape for {name}")
    if method.get("m0_behavior") != "UNIMPLEMENTED":
        raise SystemExit(f"wire case manifest must keep {name} explicitly unimplemented")
print("wire case manifest validated")
PY

mkdir -p "$ARTIFACT_ROOT"
DRIVER="$ARTIFACT_ROOT/m0driver"
TRACE="$ARTIFACT_ROOT/transport.jsonl"
export M0_WIRE_CASES="$CASES"
export M0_WIRE_ARTIFACT_ROOT="$ARTIFACT_ROOT"
export M0_WIRE_DRIVER="$DRIVER"
export M0_WIRE_TRACE="$TRACE"

GOROOT="$("$ROOT/scripts/dev/go-m0.sh" -C testkit/go env GOROOT)"
UNFORMATTED="$("$GOROOT/bin/gofmt" -l \
  "$ROOT/testkit/go/internal/wire/probe.go" \
  "$ROOT/testkit/go/internal/wire/probe_test.go" \
  "$ROOT/testkit/go/cmd/m0driver/main.go")"
if [[ -n "$UNFORMATTED" ]]; then
  printf 'Go sources need gofmt:\n%s\n' "$UNFORMATTED" >&2
  exit 1
fi
"$ROOT/scripts/dev/go-m0.sh" -C testkit/go test ./internal/wire ./cmd/m0driver
"$ROOT/scripts/dev/go-m0.sh" -C testkit/go vet ./internal/wire ./cmd/m0driver
"$ROOT/scripts/dev/go-m0.sh" -C testkit/go build -trimpath -o "$DRIVER" ./cmd/m0driver

"$ROOT/scripts/dev/mvn21.sh" -B -ntp -pl server -am \
  -Pm0-wire -Dit.test=GoInteropIT \
  verify

python3 - "$ROOT" "$ARTIFACT_ROOT" <<'PY'
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

root = Path(sys.argv[1])
artifacts = Path(sys.argv[2])
trace = artifacts / "transport.jsonl"
report = root / "server/target/failsafe-reports/TEST-io.etcdhbase.server.GoInteropIT.xml"
if not trace.is_file() or not report.is_file():
    raise SystemExit("wire integration trace or Failsafe report is missing")
suite = ET.parse(report).getroot()
tests = int(suite.attrib.get("tests", "0"))
failures = int(suite.attrib.get("failures", "0"))
errors = int(suite.attrib.get("errors", "0"))
skipped = int(suite.attrib.get("skipped", "0"))
if tests != 2 or failures or errors or skipped:
    raise SystemExit(
        f"GoInteropIT must execute exactly two passing tests (tests={tests}, failures={failures}, "
        f"errors={errors}, skipped={skipped})"
    )
records = [json.loads(line) for line in trace.read_text(encoding="utf-8").splitlines() if line]
if len(records) != 10:
    raise SystemExit(f"expected 10 wire trace records, got {len(records)}")
scopes = {record.get("evidence_scope") for record in records}
if scopes != {"transport", "transport-fixture"}:
    raise SystemExit(f"unexpected wire evidence scopes: {sorted(scopes)}")
print(f"Go/Java wire interoperability passed; tests={tests}, skipped={skipped}, trace_records={len(records)}, artifacts={artifacts}")
PY
