#!/usr/bin/env bash
set -euo pipefail

M0_REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

usage() {
  printf '%s\n' \
    'Usage:' \
    '  scripts/verify/hbase-probe.sh --profile public-local' \
    '  scripts/verify/hbase-probe.sh --profile target --config-ref PATH'
}

if [ "$#" -lt 2 ] || [ "$1" != "--profile" ]; then
  usage >&2
  exit 2
fi

M0_PROFILE="$2"
shift 2

case "$M0_PROFILE" in
  public-local)
    if [ "$#" -ne 0 ]; then
      usage >&2
      exit 2
    fi
    exec "$M0_REPO_ROOT/scripts/dev/mvn21.sh" \
      -B -ntp -pl hbase -am verify -Pm0-hbase-local
    ;;
  target)
    if [ "$#" -ne 2 ] || [ "$1" != "--config-ref" ] || [ -z "$2" ]; then
      printf '%s\n' 'missing-input: provide --config-ref using ETCD_HBASE_M0_TARGET_CONFIG' >&2
      exit 3
    fi
    M0_TARGET_CONFIG_REF="$2"
    if [ ! -f "$M0_TARGET_CONFIG_REF" ] || [ ! -r "$M0_TARGET_CONFIG_REF" ]; then
      printf '%s\n' 'missing-input: target config reference is not a readable local file' >&2
      exit 3
    fi
    exec "$M0_REPO_ROOT/scripts/dev/mvn21.sh" \
      -B -ntp -pl hbase -am verify -Pm0-hbase-target \
      "-Detcd.hbase.m0.target.configRef=$M0_TARGET_CONFIG_REF"
    ;;
  *)
    printf 'unknown profile: %s\n' "$M0_PROFILE" >&2
    usage >&2
    exit 2
    ;;
esac
