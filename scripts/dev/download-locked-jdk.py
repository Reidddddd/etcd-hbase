#!/usr/bin/env python3
"""Download the Linux x64 JDK archive recorded in the dependency lock."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sys
import tempfile
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOCK = ROOT / "build" / "dependency-lock.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="destination archive path")
    args = parser.parse_args()

    if sys.platform != "linux" or platform.machine().lower() not in {"x86_64", "amd64"}:
        parser.error("the public CI JDK download currently supports Linux x64 only")

    try:
        lock = json.loads(LOCK.read_text(encoding="utf-8"))
        jdk = lock["jdk"]
        release = jdk["release"]
        archive = jdk["archives"]["linux-x64"]
        url = archive["url"]
        expected_sha256 = archive["sha256"]
    except (KeyError, OSError, TypeError, json.JSONDecodeError) as exc:
        parser.error(f"invalid JDK lock entry: {exc}")

    if not isinstance(release, str) or not isinstance(url, str):
        parser.error("JDK release and archive URL must be strings")
    if (
        not url.startswith("https://github.com/adoptium/temurin21-binaries/releases/download/")
        or not isinstance(expected_sha256, str)
        or len(expected_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_sha256)
    ):
        parser.error("JDK archive must use the official HTTPS source and a lowercase SHA-256")

    destination = args.output.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    digest = hashlib.sha256()
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=destination.parent, prefix=f"{destination.name}.", delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
            request = urllib.request.Request(url, headers={"User-Agent": "etcd-hbase-m0-ci"})
            with urllib.request.urlopen(request, timeout=60) as response:
                while chunk := response.read(1024 * 1024):
                    temporary.write(chunk)
                    digest.update(chunk)
        actual_sha256 = digest.hexdigest()
        if actual_sha256 != expected_sha256:
            raise ValueError(
                f"JDK archive SHA-256 mismatch for {release}: expected {expected_sha256}, "
                f"received {actual_sha256}"
            )
        os.replace(temporary_path, destination)
        temporary_path = None
    except (OSError, ValueError) as exc:
        parser.error(f"could not download locked JDK {release}: {exc}")
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    print(f"verified locked Temurin {release} archive: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
