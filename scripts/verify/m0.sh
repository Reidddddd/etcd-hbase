#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
SCOPE=""
ARTIFACT_ROOT="$ROOT/.artifacts/m0/public/$RUN_ID"

usage() {
  printf '%s\n' 'Usage: scripts/verify/m0.sh --scope public [--artifact-root PATH]' >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scope)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      SCOPE="$2"
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

[[ "$SCOPE" == "public" ]] || { usage; exit 2; }
[[ "$ARTIFACT_ROOT" = /* ]] || ARTIFACT_ROOT="$ROOT/$ARTIFACT_ROOT"

if [[ -z "${ETCD_HBASE_JDK21_HOME:-}" ]]; then
  if [[ -n "${JAVA_HOME:-}" ]]; then
    export ETCD_HBASE_JDK21_HOME="$JAVA_HOME"
  elif [[ -x "$ROOT/.artifacts/m0/jdk/jdk-21.0.12.1+1/Contents/Home/bin/java" ]]; then
    export ETCD_HBASE_JDK21_HOME="$ROOT/.artifacts/m0/jdk/jdk-21.0.12.1+1/Contents/Home"
  else
    echo "Set ETCD_HBASE_JDK21_HOME to the locked Temurin JDK 21 home." >&2
    exit 2
  fi
fi

mkdir -p "$ARTIFACT_ROOT/logs"
INDEXER="$ROOT/scripts/verify/report-index.py"
python3 "$INDEXER" begin \
  --repo-root "$ROOT" \
  --artifact-root "$ARTIFACT_ROOT" \
  --run-id "$RUN_ID" \
  --scope public

finish() {
  local run_status=$?
  local index_status=0
  trap - EXIT
  python3 "$INDEXER" finalize \
    --repo-root "$ROOT" \
    --artifact-root "$ARTIFACT_ROOT" \
    --exit-code "$run_status" || index_status=$?
  if [[ "$run_status" -eq 0 && "$index_status" -ne 0 ]]; then
    run_status="$index_status"
  fi
  exit "$run_status"
}
trap finish EXIT

run_step() {
  local step_id="$1"
  shift
  local log="logs/$step_id.log"
  local status=0
  if "$@" >"$ARTIFACT_ROOT/$log" 2>&1; then
    tail -n 60 "$ARTIFACT_ROOT/$log"
  else
    status=$?
    tail -n 200 "$ARTIFACT_ROOT/$log" >&2
  fi
  if [[ "$status" -eq 0 ]]; then
    python3 "$INDEXER" record \
      --artifact-root "$ARTIFACT_ROOT" \
      --step-id "$step_id" --state passed --exit-code 0 --log "$log"
  else
    python3 "$INDEXER" record \
      --artifact-root "$ARTIFACT_ROOT" \
      --step-id "$step_id" --state failed --exit-code "$status" --log "$log"
    return "$status"
  fi
}

run_step protocol "$ROOT/scripts/verify/protocol.sh"
run_step go_module_lock python3 "$ROOT/scripts/verify/go-module-lock.py"
run_step native_oracle \
  "$ROOT/scripts/verify/native-oracle.sh" --artifact-root "$ARTIFACT_ROOT/native"
run_step wire \
  "$ROOT/scripts/verify/wire.sh" \
  --cases "$ROOT/testkit/cases/m0-wire.json" \
  --artifact-root "$ARTIFACT_ROOT/wire"
run_step public_hbase "$ROOT/scripts/verify/hbase-probe.sh" --profile public-local
run_step effective_pom \
  "$ROOT/scripts/dev/mvn21.sh" -B -ntp \
  -f "$ROOT/server/pom.xml" \
  "-Doutput=$ARTIFACT_ROOT/effective-pom.xml" \
  org.apache.maven.plugins:maven-help-plugin:3.5.2:effective-pom
run_step dependency_tree \
  "$ROOT/scripts/dev/mvn21.sh" -B -ntp \
  -pl server -am \
  -Dverbose \
  "-DoutputFile=$ARTIFACT_ROOT/server-dependency-tree.txt" \
  org.apache.maven.plugins:maven-dependency-plugin:3.11.0:tree
run_step reproducible_build \
  python3 "$ROOT/scripts/verify/maven-reproducible-build.py" \
  --profile m0-hbase-local --profile m0-wire
run_step maven_verify \
  "$ROOT/scripts/dev/mvn21.sh" -B -ntp -Pm0-hbase-local,m0-wire clean verify

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  run_step runtime_image \
    "$ROOT/scripts/verify/runtime-image.sh" --artifact-root "$ARTIFACT_ROOT/runtime-image"
else
  if command -v docker >/dev/null 2>&1; then
    SKIP_REASON="docker-daemon-unavailable"
  else
    SKIP_REASON="docker-cli-unavailable"
  fi
  printf 'runtime image not run: %s\n' "$SKIP_REASON" >"$ARTIFACT_ROOT/logs/runtime_image.log"
  python3 "$INDEXER" record \
    --artifact-root "$ARTIFACT_ROOT" \
    --step-id runtime_image --state skipped \
    --reason "$SKIP_REASON" --log logs/runtime_image.log
fi

echo "M0 public verification steps completed; artifacts=$ARTIFACT_ROOT"
