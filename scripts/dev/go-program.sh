#!/usr/bin/env bash
set -euo pipefail
umask 077
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
PROGRAM="${1:-}"
case "$PROGRAM" in native-etcd|m0driver|m3driver|m4driver|m3history) shift ;; *) echo 'unknown testkit program' >&2; exit 2 ;; esac
BIN_ROOT="$REPO_ROOT/.artifacts/m0/go/bin"
mkdir -p "$BIN_ROOT"
"$SCRIPT_DIR/go-m0.sh" build -o "$BIN_ROOT/$PROGRAM" "./cmd/$PROGRAM"
# Replace the launcher with the owned program so SIGINT/SIGTERM reach its
# context and cleanup handlers directly, without a lingering go-run child.
exec "$BIN_ROOT/$PROGRAM" "$@"
