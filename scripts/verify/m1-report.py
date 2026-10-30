#!/usr/bin/env python3
"""Collect actual M1 suites, preserving the committed source and acceptance boundaries."""
import datetime
import json
from pathlib import Path
import shutil
import sys
import xml.etree.ElementTree as ET
from evidence import java_reports, require_unchanged_source

REQUIRED = {"io.etcdhbase.core.scan.BucketCursorTest",
            "io.etcdhbase.hbase.storage.HBasePageReaderTest",
            "io.etcdhbase.hbase.storage.M1StorageIT"}
CURSOR_CASE = "cursorReopensAfterOnlyConsumedKeysAndCancelsWithoutClosingSharedConnections"


def collect(root, output, phase):
    if phase != "J12":
        raise ValueError("unsupported M1 verification phase")
    source = json.loads((output / "source.json").read_text())
    require_unchanged_source(root, source)
    paths, suites = java_reports(root, REQUIRED, ("spi", "core", "hbase"))
    actual = ET.parse(root / "hbase/target/failsafe-reports/TEST-io.etcdhbase.hbase.storage.M1StorageIT.xml").getroot()
    if CURSOR_CASE not in {case.get("name") for case in actual.iter("testcase")}:
        raise ValueError("real cursor verification did not execute")
    for path in paths:
        destination = output / "reports" / path.relative_to(root)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)
    source.update(phase=phase, scope="public", ended_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  tests=sum(suite["tests"] for suite in suites.values()), failures=0, errors=0, skipped=0,
                  suites=suites, unverified=["target HBase and B01/B02/B03/B06", "B05 capacity",
                                           "M2 commit and ReadView", "remote scanner lease convergence",
                                           "maintainer review"])
    (output / "summary.json").write_text(json.dumps(source, indent=2) + "\n")
    print(json.dumps({key: source[key] for key in ("phase", "checkout", "pr_head", "base", "tests")}))


if __name__ == "__main__":
    try:
        collect(Path(__file__).resolve().parents[2], Path(sys.argv[1]), sys.argv[2])
    except (ValueError, OSError, KeyError):
        raise SystemExit("M1 verification evidence rejected") from None
