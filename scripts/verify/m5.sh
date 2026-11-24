#!/usr/bin/env bash
set -euo pipefail
umask 077
export PYTHONDONTWRITEBYTECODE=1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"

scope=''
task=''
artifact_request=''
seen_scope=false
seen_task=false
seen_artifact=false
while (($#)); do
  case "$1" in
    --scope)
      [[ "$seen_scope" == false && $# -ge 2 ]] || { echo 'scope must be provided once' >&2; exit 2; }
      scope="$2"; seen_scope=true; shift 2 ;;
    --task)
      [[ "$seen_task" == false && $# -ge 2 ]] || { echo 'task must be provided once' >&2; exit 2; }
      task="$2"; seen_task=true; shift 2 ;;
    --artifact-root)
      [[ "$seen_artifact" == false && $# -ge 2 ]] || { echo 'artifact root must be provided once' >&2; exit 2; }
      artifact_request="$2"; seen_artifact=true; shift 2 ;;
    --help)
      cat <<'USAGE'
用法：scripts/verify/m5.sh --scope public --task J50 --artifact-root .artifacts/m5/<fresh-run-id>
USAGE
      exit 0 ;;
    *) echo "unknown M5 verification option: $1" >&2; exit 2 ;;
  esac
done

if [[ "$scope" != public || "$task" != J50 || "$seen_scope" != true || \
      "$seen_task" != true || "$seen_artifact" != true ]]; then
  echo 'M5 public verification currently requires --scope public, --task J50, and --artifact-root' >&2
  exit 2
fi

python3 scripts/verify/m5-report.py --validate-task J50
: "${ETCD_HBASE_JDK21_HOME:?Set ETCD_HBASE_JDK21_HOME to the pinned Temurin JDK 21 home.}"
"$ETCD_HBASE_JDK21_HOME/bin/java" -version

artifact_root="$(python3 - "$ROOT" "$artifact_request" <<'PY'
from pathlib import Path
import os
import re
import sys

repository = Path(sys.argv[1]).resolve()
requested = Path(sys.argv[2])
candidate = Path(os.path.abspath(requested if requested.is_absolute() else repository / requested))
allowed = repository / ".artifacts" / "m5"
try:
    relative = candidate.relative_to(allowed)
except ValueError as error:
    raise SystemExit("M5 artifact root must be under .artifacts/m5") from error
if not relative.parts or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", candidate.name):
    raise SystemExit("M5 artifact root must be a fresh named directory")
current = repository
for component in (".artifacts", "m5", *relative.parts):
    current = current / component
    if current.is_symlink():
        raise SystemExit("M5 artifact path must not traverse a symbolic link")
if candidate.exists():
    raise SystemExit("M5 artifact root already exists; use a fresh run ID")
candidate.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
candidate.mkdir(mode=0o700)
print(candidate)
PY
)"

run_file="$artifact_root/run.json"
python3 scripts/verify/m5-report.py --init-run --task J50 --scope public --artifact-root "$artifact_root"

finalize_run() {
  local final_exit="$?"
  trap - EXIT
  if python3 scripts/verify/m5-report.py --finalize-run --run-file "$run_file" --exit-code "$final_exit"; then
    exit "$final_exit"
  fi
  exit 2
}
trap finalize_run EXIT

run_logged() {
  local name="$1"
  local stdout_log="$2"
  local stderr_log="$3"
  shift 3
  local started_at finished_at command_exit
  started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  set +e
  "$@" > "$artifact_root/$stdout_log" 2> "$artifact_root/$stderr_log"
  command_exit=$?
  set -e
  finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  python3 scripts/verify/m5-report.py --record-command --run-file "$run_file" \
    --name "$name" --exit-code "$command_exit" --started-at "$started_at" \
    --finished-at "$finished_at" --stdout "$stdout_log" --stderr "$stderr_log"
  if (( command_exit != 0 )); then
    echo "M5 command failed: $name (exit $command_exit)" >&2
    tail -n 100 "$artifact_root/$stdout_log" >&2 || true
    tail -n 100 "$artifact_root/$stderr_log" >&2 || true
    return "$command_exit"
  fi
}

run_logged registry-check registry-check.log registry-check.stderr \
  python3 scripts/verify/m5-report.py --validate-registry
run_logged report-tests report-tests.log report-tests.stderr \
  python3 scripts/verify/test_m5_report.py
selectors="$(python3 scripts/verify/m5-report.py --maven-selectors J50)"
IFS=$'\t' read -r surefire_selector failsafe_selector <<< "$selectors"
[[ -n "$surefire_selector" && -n "$failsafe_selector" ]] || {
  echo 'J50 Maven selectors are incomplete' >&2
  exit 2
}
run_logged maven-verify maven.log maven.stderr \
  scripts/dev/mvn21.sh -B -ntp -pl server -am clean verify -Pm5-j50-public \
    "-Dtest=$surefire_selector" -Dsurefire.failIfNoSpecifiedTests=false \
    "-Dit.test=$failsafe_selector" -Dfailsafe.failIfNoSpecifiedTests=false
run_logged report-verify report-validation.log report-validation.stderr \
  python3 scripts/verify/m5-report.py --verify-reports --task J50 --artifact-root "$artifact_root"

git diff --check HEAD
echo "M5 J50 public verification passed: $artifact_root"
