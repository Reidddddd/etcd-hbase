#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
JDK_HOME="${ETCD_HBASE_JDK21_HOME:-}"

if [[ -z "$JDK_HOME" ]]; then
  echo "Set ETCD_HBASE_JDK21_HOME to the pinned Temurin JDK 21 home." >&2
  exit 2
fi

JDK_HOME="$(cd "$JDK_HOME" && pwd -P)"
export JAVA_HOME="$JDK_HOME"
export PATH="$JAVA_HOME/bin:$PATH"

if [[ $# -lt 2 || "$1" != "init" ]]; then
  echo "usage: scripts/dev/m0-admin.sh init plan|apply [options]" >&2
  exit 2
fi
if [[ "$2" != "plan" && "$2" != "apply" ]]; then
  echo "init action must be plan or apply" >&2
  exit 2
fi

BUILD_DIR="$ROOT/admin/target"
DEPENDENCY_CLASSPATH="$BUILD_DIR/m0-admin-dependencies.txt"

"$ROOT/scripts/dev/mvn21.sh" \
  -B -ntp \
  -pl admin -am \
  -DskipTests \
  -Dmdep.outputFile="$DEPENDENCY_CLASSPATH" \
  -DincludeScope=runtime \
  compile \
  org.apache.maven.plugins:maven-dependency-plugin:3.11.0:build-classpath

MODULE_CLASSPATH="${ROOT}/admin/target/classes:${ROOT}/server/target/classes:${ROOT}/grpc/target/classes:\
${ROOT}/hbase/target/classes:${ROOT}/core/target/classes:${ROOT}/spi/target/classes:${ROOT}/protocol/target/classes"
DEPENDENCY_ENTRIES="$(<"$DEPENDENCY_CLASSPATH")"

exec "$JAVA_HOME/bin/java" \
  -cp "$MODULE_CLASSPATH:$DEPENDENCY_ENTRIES" \
  io.etcdhbase.admin.InitializeTablesCommand \
  "$@"
