#!/usr/bin/env python3
"""Run two clean Maven builds and compare the packaged module JARs."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MAVEN = ROOT / "scripts" / "dev" / "mvn21.sh"


def packaged_jars() -> dict[str, str]:
    jars: dict[str, str] = {}
    for path in sorted(ROOT.glob("*/target/*.jar")):
        if path.is_file():
            jars[str(path.relative_to(ROOT))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return jars


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        action="append",
        default=[],
        help="Maven profile to activate for both clean builds; may be repeated",
    )
    args = parser.parse_args()

    results: list[dict[str, str]] = []
    command = [str(MAVEN), "-B", "-ntp"]
    if args.profile:
        command.append("-P" + ",".join(args.profile))
    command.extend(("clean", "verify"))
    for run in (1, 2):
        print(f"\n=== reproducibility build {run}/2 ===", flush=True)
        completed = subprocess.run(command, cwd=ROOT, check=False)
        if completed.returncode != 0:
            print(f"build {run} failed with exit code {completed.returncode}", file=sys.stderr)
            return completed.returncode
        jars = packaged_jars()
        if not jars:
            print("no module JARs were produced", file=sys.stderr)
            return 1
        print(f"build {run}: captured {len(jars)} packaged JARs", flush=True)
        results.append(jars)

    first, second = results
    all_paths = sorted(set(first) | set(second))
    differences = [
        f"{path}: first={first.get(path, '<missing>')} second={second.get(path, '<missing>')}"
        for path in all_paths
        if first.get(path) != second.get(path)
    ]
    if differences:
        print("reproducibility check failed:\n  " + "\n  ".join(differences), file=sys.stderr)
        return 1
    print(f"reproducibility check passed: {len(first)} module JARs have identical SHA-256")
    return 0


if __name__ == "__main__":
    sys.exit(main())
