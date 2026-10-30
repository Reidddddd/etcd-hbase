#!/usr/bin/env bash
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
if [[ $# != 2 || "$1" != --through || "$2" != J12 ]]; then
  echo 'usage: m1.sh --through J12' >&2
  exit 2
fi
PHASE="$2"
REPORT="$ROOT/.artifacts/m1/$PHASE/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$REPORT"
python3 scripts/verify/test_evidence.py
python3 scripts/verify/j06-source.py "$REPORT"
python3 scripts/verify/source-frozen.py "$REPORT"
scripts/dev/mvn21.sh -B -ntp -pl hbase -am clean verify -Pj11-hbase-local > "$REPORT/maven.log" 2>&1 || {
  tail -n 60 "$REPORT/maven.log" >&2
  exit 1
}
python3 scripts/verify/m0-spi.py --jar spi/target/spi-0.1.0-SNAPSHOT.jar > "$REPORT/boundary.json"
python3 scripts/verify/m1-report.py "$REPORT" "$PHASE"
git diff --check
