#!/usr/bin/env python3
"""Check that the public SPI remains independent of storage and transport modules."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[2]
SPI_POM = ROOT / "spi/pom.xml"
SPI_SOURCE = ROOT / "spi/src"
MAVEN_NAMESPACE = {"m": "http://maven.apache.org/POM/4.0.0"}
FORBIDDEN_GROUPS = {
    "org.apache.hbase",
    "org.apache.hadoop",
    "io.grpc",
    "com.google.protobuf",
    "io.etcdhbase",
}
FORBIDDEN_PACKAGES = re.compile(
    r"\b(?:org\.apache\.(?:hadoop|hbase)"
    r"|io\.grpc|com\.google\.protobuf"
    r"|io\.etcdhbase\.(?:core|server|grpc|hbase))\b"
)


def verify() -> tuple[int, int]:
    try:
        pom = ElementTree.parse(SPI_POM).getroot()
    except (OSError, ElementTree.ParseError) as error:
        raise ValueError("cannot parse spi/pom.xml") from error

    violations: list[str] = []
    for dependency in pom.findall("m:dependencies/m:dependency", MAVEN_NAMESPACE):
        group_id = dependency.findtext("m:groupId", namespaces=MAVEN_NAMESPACE, default="").strip()
        artifact_id = dependency.findtext("m:artifactId", namespaces=MAVEN_NAMESPACE, default="").strip()
        if group_id in FORBIDDEN_GROUPS or artifact_id.startswith(("hbase-", "grpc-", "protobuf-")):
            violations.append(f"spi/pom.xml depends on forbidden artifact {group_id}:{artifact_id}")

    if not SPI_SOURCE.is_dir():
        raise ValueError("cannot find spi/src")
    sources = sorted(SPI_SOURCE.rglob("*.java"))
    if not sources:
        raise ValueError("no Java sources found in spi/src")

    for source in sources:
        try:
            content = source.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise ValueError(f"cannot read {source.relative_to(ROOT)}") from error
        if FORBIDDEN_PACKAGES.search(content):
            violations.append(f"{source.relative_to(ROOT)} imports or references a forbidden module")

    if violations:
        raise ValueError("; ".join(violations))
    return len(sources), len(pom.findall("m:dependencies/m:dependency", MAVEN_NAMESPACE))


def main() -> int:
    try:
        source_count, dependency_count = verify()
    except ValueError as error:
        print(f"M0 SPI boundary verification failed: {error}", file=sys.stderr)
        return 1
    print(
        "Verified SPI boundary across "
        f"{source_count} Java sources and {dependency_count} direct dependencies."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
