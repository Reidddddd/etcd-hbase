#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
ENTRY="${1:-}"
if [[ "$ENTRY" != j03 && "$ENTRY" != runtime && "$ENTRY" != auth-registry ]]; then
  echo "usage: launch-server.sh <j03|runtime|auth-registry> <arguments>" >&2
  exit 2
fi
shift
JDK_HOME="${ETCD_HBASE_JDK21_HOME:-}"

if [[ -z "$JDK_HOME" ]]; then
  echo "Set ETCD_HBASE_JDK21_HOME to the pinned Temurin JDK 21 home." >&2
  exit 2
fi

JDK_HOME="$(cd "$JDK_HOME" && pwd -P)"
export JAVA_HOME="$JDK_HOME"
export PATH="$JAVA_HOME/bin:$PATH"

if [[ "$ENTRY" == j03 && $# -ne 6 ]] \
  || [[ "$ENTRY" == runtime && ( "${1:-}" != --config \
      || ( $# -ne 2 && ( $# -ne 4 || "${3:-}" != --m3-config ) ) ) ]]; then
  echo "invalid server arguments" >&2
  exit 2
fi
MAIN_CLASS=io.etcdhbase.server.Bootstrap
if [[ "$ENTRY" == runtime ]]; then MAIN_CLASS=io.etcdhbase.server.RuntimeServer; fi
if [[ "$ENTRY" == auth-registry ]]; then MAIN_CLASS=io.etcdhbase.server.AuthRegistryAdminCommand; fi

BUILD_DIR="$ROOT/server/target"
DEPENDENCY_CLASSPATH="$BUILD_DIR/m0-server-dependencies.txt"
MAVEN_REPOSITORY="$ROOT/.artifacts/m0/server-maven-repository"

"$ROOT/scripts/dev/mvn21.sh" \
  -B -ntp \
  -pl server -am \
  -DskipTests -DskipITs \
  -Dmaven.repo.local="$MAVEN_REPOSITORY" \
  clean install

"$ROOT/scripts/dev/mvn21.sh" \
  -B -ntp \
  -f "$ROOT/server/pom.xml" \
  -Dmaven.repo.local="$MAVEN_REPOSITORY" \
  -Dmdep.outputFile="$DEPENDENCY_CLASSPATH" \
  -DincludeScope=runtime \
  org.apache.maven.plugins:maven-dependency-plugin:3.11.0:build-classpath

if [[ ! -s "$DEPENDENCY_CLASSPATH" ]]; then
  echo "Maven did not write the packaged runtime dependency classpath: $DEPENDENCY_CLASSPATH" >&2
  exit 1
fi
DEPENDENCY_ENTRIES="$(<"$DEPENDENCY_CLASSPATH")"
if [[ "$DEPENDENCY_ENTRIES" == *"target/classes"* ]]; then
  echo "runtime classpath contains development classes instead of packaged module JARs" >&2
  exit 1
fi
for M0_MODULE in protocol spi core hbase grpc admin; do
  if [[ "$DEPENDENCY_ENTRIES" != *"/$M0_MODULE-0.1.0-SNAPSHOT.jar"* ]]; then
    echo "packaged runtime classpath is missing the $M0_MODULE module JAR" >&2
    exit 1
  fi
done
SERVER_JAR="$BUILD_DIR/server-0.1.0-SNAPSHOT.jar"
if [[ ! -s "$SERVER_JAR" ]]; then
  echo "packaged server JAR is missing: $SERVER_JAR" >&2
  exit 1
fi

exec "$JAVA_HOME/bin/java" \
  -cp "$SERVER_JAR:$DEPENDENCY_ENTRIES" \
  "$MAIN_CLASS" \
  "$@"
