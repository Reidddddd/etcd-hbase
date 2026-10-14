#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOCK_FILE="$REPO_ROOT/build/dependency-lock.json"
ARTIFACT_ROOT="$REPO_ROOT/.artifacts/m0/go"

module_dir="$REPO_ROOT/testkit/go"
if [[ "${1:-}" == "-C" ]]; then
  if [[ $# -lt 3 ]]; then
    echo "usage: $0 [-C module-directory] <go-command> [arguments...]" >&2
    exit 2
  fi
  module_dir="$2"
  if [[ "$module_dir" != /* ]]; then
    module_dir="$REPO_ROOT/$module_dir"
  fi
  shift 2
fi

if [[ $# -eq 0 ]]; then
  echo "usage: $0 [-C module-directory] <go-command> [arguments...]" >&2
  exit 2
fi
if [[ ! -f "$module_dir/go.mod" ]]; then
  echo "Go module not found: $module_dir/go.mod" >&2
  exit 2
fi

platform="$(python3 - <<'PY'
import platform
system = platform.system()
machine = platform.machine().lower()
if system == "Darwin" and machine in ("arm64", "aarch64"):
    print("macos-aarch64")
elif system == "Linux" and machine in ("x86_64", "amd64"):
    print("linux-x64")
else:
    raise SystemExit(f"unsupported locked Go toolchain platform: {system}/{machine}")
PY
)"

read_lock_value() {
  python3 - "$LOCK_FILE" "$platform" "$1" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as stream:
    lock = json.load(stream)["go"]
platform = sys.argv[2]
key = sys.argv[3]
if key == "version":
    print(lock["version"])
else:
    print(lock["archives"][platform][key])
PY
}

go_version="$(read_lock_value version)"
archive_url="$(read_lock_value url)"
archive_sha256="$(read_lock_value sha256)"
tool_dir="$ARTIFACT_ROOT/toolchains/go$go_version"
go_bin="$tool_dir/bin/go"

if [[ ! -x "$go_bin" ]]; then
  if [[ -e "$tool_dir" ]]; then
    echo "locked Go toolchain path exists but is incomplete: $tool_dir" >&2
    exit 1
  fi
  mkdir -p "$ARTIFACT_ROOT/downloads" "$ARTIFACT_ROOT/tmp"
  archive="$ARTIFACT_ROOT/downloads/go$go_version.$platform.tar.gz"
  download_tmp="$ARTIFACT_ROOT/tmp/go$go_version.$platform.$$.tar.gz"
  extract_tmp="$ARTIFACT_ROOT/tmp/go$go_version.$platform.$$"
  trap 'rm -f "$download_tmp"; rm -rf "$extract_tmp"' EXIT

  if [[ -f "$archive" ]]; then
    download_source="$archive"
  else
    curl --fail --location --retry 3 --silent --show-error "$archive_url" --output "$download_tmp"
    download_source="$download_tmp"
  fi
  if command -v shasum >/dev/null 2>&1; then
    actual_sha256="$(shasum -a 256 "$download_source" | awk '{print $1}')"
  else
    actual_sha256="$(sha256sum "$download_source" | awk '{print $1}')"
  fi
  if [[ "$actual_sha256" != "$archive_sha256" ]]; then
    echo "locked Go archive SHA-256 mismatch: expected $archive_sha256, got $actual_sha256" >&2
    exit 1
  fi
  if [[ "$download_source" == "$download_tmp" ]]; then
    mv "$download_tmp" "$archive"
  fi
  mkdir -p "$extract_tmp"
  tar -xzf "$archive" -C "$extract_tmp"
  if [[ ! -x "$extract_tmp/go/bin/go" ]]; then
    echo "locked Go archive has no go/bin/go executable" >&2
    exit 1
  fi
  mkdir -p "$(dirname "$tool_dir")"
  mv "$extract_tmp/go" "$tool_dir"
fi

version_output="$("$go_bin" version)"
if [[ "$version_output" != "go version go$go_version "* ]]; then
  echo "locked Go version mismatch: expected go$go_version, got $version_output" >&2
  exit 1
fi

export GOTOOLCHAIN=local
export GOWORK=off
export GOFLAGS=-mod=readonly
export GOPATH="$ARTIFACT_ROOT/gopath"
export GOMODCACHE="$GOPATH/pkg/mod"
export GOCACHE="$ARTIFACT_ROOT/build-cache"
mkdir -p "$GOPATH" "$GOMODCACHE" "$GOCACHE"
cd "$module_dir"
exec "$go_bin" "$@"
