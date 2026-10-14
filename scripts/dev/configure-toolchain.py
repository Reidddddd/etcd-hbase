#!/usr/bin/env python3
"""Generate a project-local Maven toolchain entry for the pinned Temurin JDK 21."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import NoReturn
from xml.etree import ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / ".mvn" / "toolchains.xml"


def fail(message: str) -> NoReturn:
    print(f"configure-toolchain: {message}", file=sys.stderr)
    raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--java-home",
        default=os.environ.get("ETCD_HBASE_JDK21_HOME"),
        help="Temurin JDK 21 home (defaults to ETCD_HBASE_JDK21_HOME)",
    )
    args = parser.parse_args()
    if not args.java_home:
        fail("pass --java-home or set ETCD_HBASE_JDK21_HOME")

    java_home = Path(args.java_home).expanduser().resolve(strict=True)
    java = java_home / "bin" / ("java.exe" if os.name == "nt" else "java")
    if not java.is_file():
        fail(f"JDK executable not found under {java_home}")

    result = subprocess.run(
        [str(java), "-XshowSettings:properties", "-version"],
        check=False,
        capture_output=True,
        text=True,
    )
    output = result.stdout + result.stderr
    if result.returncode != 0:
        fail("could not execute the selected JDK")
    if not re.search(r"java\.specification\.version\s*=\s*21(?:\s|$)", output):
        fail("selected JDK must have Java specification version 21")
    if not re.search(r"Temurin|Adoptium", output, re.IGNORECASE):
        fail("selected JDK must be an Eclipse Temurin/Adoptium build")
    runtime_version = re.search(r"java\.runtime\.version\s*=\s*(\S+)", output)
    try:
        lock = json.loads((ROOT / "build" / "dependency-lock.json").read_text(encoding="utf-8"))
        expected_runtime = lock["jdk"]["release"]
    except (KeyError, OSError, TypeError, json.JSONDecodeError):
        fail("could not read the locked JDK release from build/dependency-lock.json")
    actual_runtime = runtime_version.group(1) if runtime_version else None
    if actual_runtime not in {expected_runtime, f"{expected_runtime}-LTS"}:
        fail(f"selected JDK runtime must match the locked release {expected_runtime}")

    if OUTPUT.is_symlink():
        fail(f"refusing to write through symlink: {OUTPUT}")
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    root = ET.Element("toolchains")
    toolchain = ET.SubElement(root, "toolchain")
    ET.SubElement(toolchain, "type").text = "jdk"
    provides = ET.SubElement(toolchain, "provides")
    ET.SubElement(provides, "version").text = "21"
    ET.SubElement(provides, "vendor").text = "temurin"
    configuration = ET.SubElement(toolchain, "configuration")
    ET.SubElement(configuration, "jdkHome").text = str(java_home)
    ET.indent(root, space="  ")
    content = ET.tostring(root, encoding="utf-8", xml_declaration=True) + b"\n"

    with tempfile.NamedTemporaryFile(
        mode="wb", dir=OUTPUT.parent, prefix="toolchains.", delete=False
    ) as temporary:
        temporary.write(content)
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, OUTPUT)
    print(f"Wrote project-local toolchain: {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
