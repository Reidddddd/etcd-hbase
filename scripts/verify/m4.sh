#!/usr/bin/env bash
set -euo pipefail
umask 077
export PYTHONDONTWRITEBYTECODE=1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"

scope=''
task=''
artifact_request=''
run_all=false
seen_scope=false
seen_task=false
seen_artifact=false
while (($#)); do
  case "$1" in
    --scope)
      [[ "$seen_scope" == false && $# -ge 2 ]] || { echo 'scope must be provided once' >&2; exit 2; }
      scope="$2"; seen_scope=true; shift 2 ;;
    --task)
      [[ "$seen_task" == false && "$run_all" == false && $# -ge 2 ]] || { echo 'task must be provided once and cannot be combined with --all' >&2; exit 2; }
      task="$2"; seen_task=true; shift 2 ;;
    --all)
      [[ "$run_all" == false && "$seen_task" == false ]] || { echo '--all cannot be repeated or combined with --task' >&2; exit 2; }
      run_all=true; shift ;;
    --artifact-root)
      [[ "$seen_artifact" == false && $# -ge 2 ]] || { echo 'artifact root must be provided once' >&2; exit 2; }
      artifact_request="$2"; seen_artifact=true; shift 2 ;;
    --help)
      cat <<'USAGE'
用法：scripts/verify/m4.sh --scope public --task J40 --artifact-root .artifacts/m4/<fresh-run-id>
     或 scripts/verify/m4.sh --scope public --all --artifact-root .artifacts/m4/<fresh-run-id>
USAGE
      exit 0 ;;
    *) echo "unknown M4 verification option: $1" >&2; exit 2 ;;
  esac
done

if [[ "$scope" != public || "$seen_artifact" != true || \
      ( "$seen_task" == false && "$run_all" == false ) || \
      ( "$seen_task" == true && "$run_all" == true ) ]]; then
  echo 'M4 verification requires --scope public, exactly one of --task/--all, and --artifact-root' >&2
  exit 2
fi

python3 scripts/verify/m4-report.py --validate-registry
if [[ "$run_all" == true ]]; then
  python3 scripts/verify/m4-report.py --validate-all
  echo 'M4 bulk execution is unavailable until every registered task has an execution profile.' >&2
  exit 2
fi
if [[ "$task" != J40 ]]; then
  echo "M4 task is not registered for public execution: $task" >&2
  exit 2
fi
: "${ETCD_HBASE_JDK21_HOME:?Set ETCD_HBASE_JDK21_HOME to the pinned Temurin JDK 21 home.}"

artifact_root="$(python3 - "$ROOT" "$artifact_request" <<'PY'
from pathlib import Path
import os
import re
import sys

repository = Path(sys.argv[1]).resolve()
requested = Path(sys.argv[2])
candidate = Path(os.path.abspath(requested if requested.is_absolute() else repository / requested))
allowed = repository / ".artifacts" / "m4"
try:
    relative = candidate.relative_to(allowed)
except ValueError as error:
    raise SystemExit("M4 artifact root must be under .artifacts/m4") from error
if not relative.parts or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", candidate.name):
    raise SystemExit("M4 artifact root must be a fresh named directory")
current = repository
for component in (".artifacts", "m4", *relative.parts):
    current = current / component
    if current.is_symlink():
        raise SystemExit("M4 artifact path must not traverse a symbolic link")
if candidate.exists():
    raise SystemExit("M4 artifact root already exists; use a fresh run ID")
candidate.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
candidate.mkdir(mode=0o700)
print(candidate)
PY
)"

run_id="$(basename "$artifact_root")"
run_file="$artifact_root/run.json"
source_commit="$(git rev-parse HEAD)"
base_commit="$(git merge-base HEAD origin/main)"
worktree_clean=true
if [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then
  worktree_clean=false
fi
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
python3 - "$run_file" "$run_id" "$source_commit" "$base_commit" "$worktree_clean" "$started_at" <<'PY'
import json
import sys

path, run_id, source, base, clean, started = sys.argv[1:]
record = {
    "schema": "etcd-hbase-m4-run/v1",
    "run_id": run_id,
    "scope": "public-local-minicluster",
    "tasks": ["J40"],
    "status": "running",
    "source_commit": source,
    "checkout": source,
    "base_commit": base,
    "worktree_clean": clean == "true",
    "started_at": started,
    "finished_at": "",
    "commands": [],
}
with open(path, "x", encoding="utf-8") as stream:
    json.dump(record, stream, ensure_ascii=False)
    stream.write("\n")
PY

append_command() {
  local command_name="$1"
  local command_start="$2"
  local command_end="$3"
  local command_exit="$4"
  local stdout_log="$5"
  local stderr_log="$6"
  shift 6
  python3 - "$run_file" "$command_name" "$command_start" "$command_end" "$command_exit" \
    "$stdout_log" "$stderr_log" "$@" <<'PY'
import json
import os
import sys
import tempfile

path, name, started, finished, exit_code, stdout_log, stderr_log, *argv = sys.argv[1:]
with open(path, encoding="utf-8") as stream:
    record = json.load(stream)
record["commands"].append({
    "name": name,
    "argv": argv,
    "exit_code": int(exit_code),
    "started_at": started,
    "finished_at": finished,
    "stdout_log": stdout_log,
    "stderr_log": stderr_log,
})
directory = os.path.dirname(path)
fd, temporary = tempfile.mkstemp(prefix=".run-", suffix=".json", dir=directory)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(record, stream, ensure_ascii=False)
        stream.write("\n")
    os.replace(temporary, path)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)
PY
}

run_logged() {
  local command_name="$1"
  local stdout_log="$2"
  local stderr_log="$3"
  shift 3
  local command_start
  local command_end
  local command_exit
  command_start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  set +e
  "$@" > "$artifact_root/$stdout_log" 2> "$artifact_root/$stderr_log"
  command_exit=$?
  set -e
  command_end="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  append_command "$command_name" "$command_start" "$command_end" "$command_exit" \
    "$stdout_log" "$stderr_log" "$@"
  if (( command_exit != 0 )); then
    echo "M4 command failed: $command_name (exit $command_exit)" >&2
    tail -n 100 "$artifact_root/$stdout_log" >&2 || true
    tail -n 100 "$artifact_root/$stderr_log" >&2 || true
    return "$command_exit"
  fi
}

finalize_run() {
  local final_exit="$?"
  trap - EXIT
  python3 - "$run_file" "$final_exit" <<'PY'
import json
import os
import sys
import tempfile
from datetime import datetime, timezone

path, code = sys.argv[1:]
with open(path, encoding="utf-8") as stream:
    record = json.load(stream)
record["status"] = "passed" if int(code) == 0 else "failed"
record["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
directory = os.path.dirname(path)
fd, temporary = tempfile.mkstemp(prefix=".run-", suffix=".json", dir=directory)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(record, stream, ensure_ascii=False)
        stream.write("\n")
    os.replace(temporary, path)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)
PY
  exit "$final_exit"
}
trap finalize_run EXIT

export M4_ARTIFACT_ROOT="$artifact_root"
export M4_RUN_ID="$run_id"
export M4_SOURCE_COMMIT="$source_commit"
run_logged registry-check registry-check.log registry-check.stderr python3 scripts/verify/m4-report.py --validate-registry
run_logged report-tests report-tests.log report-tests.stderr python3 scripts/verify/test_m4_report.py
run_logged go-module-lock go-module-lock.log go-module-lock.stderr python3 scripts/verify/go-module-lock.py
run_logged go-tests go-test.jsonl go-test.stderr scripts/dev/go-m0.sh test -json -count=1 \
  ./internal/trace ./internal/scenario ./internal/compare ./cmd/m4driver
run_logged maven-verify maven.log maven.stderr.log scripts/dev/mvn21.sh -B -ntp \
  -pl server -am clean verify -Pm4-j40-public

collection_start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
set +e
python3 - "$ROOT" "$artifact_root" > "$artifact_root/maven-reports.log" 2> "$artifact_root/maven-reports.stderr" <<'PY'
from pathlib import Path
import shutil
import sys

repository = Path(sys.argv[1])
artifact = Path(sys.argv[2])
modules = ("protocol", "spi", "core", "hbase", "grpc", "admin", "server")
kinds = ("surefire-reports", "failsafe-reports")
copied = 0
for module in modules:
    for kind in kinds:
        source = repository / module / "target" / kind
        if not source.is_dir():
            continue
        reports = sorted(source.glob("TEST-*.xml"))
        if not reports:
            continue
        destination = artifact / module / "target" / kind
        destination.mkdir(parents=True, exist_ok=True)
        for report in reports:
            shutil.copyfile(report, destination / report.name)
            copied += 1
if copied == 0:
    raise SystemExit("Maven verification produced no XML reports")
print(f"collected {copied} Maven XML reports")
PY
collection_exit=$?
set -e
collection_end="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
append_command collect-maven-reports "$collection_start" "$collection_end" "$collection_exit" \
  maven-reports.log maven-reports.stderr python3 -
if (( collection_exit != 0 )); then
  echo 'M4 Maven XML collection failed' >&2
  tail -n 100 "$artifact_root/maven-reports.stderr" >&2 || true
  exit "$collection_exit"
fi

python3 - "$run_file" <<'PY'
import json
import os
import sys
import tempfile
from datetime import datetime, timezone

path = sys.argv[1]
with open(path, encoding="utf-8") as stream:
    record = json.load(stream)
record["status"] = "passed"
record["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
directory = os.path.dirname(path)
fd, temporary = tempfile.mkstemp(prefix=".run-", suffix=".json", dir=directory)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(record, stream, ensure_ascii=False)
        stream.write("\n")
    os.replace(temporary, path)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)
PY
run_logged validate-artifact report-validation.log report-validation.stderr \
  python3 scripts/verify/m4-report.py --artifact-root "$artifact_root"
git diff --check HEAD
