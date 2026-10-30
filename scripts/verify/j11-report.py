#!/usr/bin/env python3
"""Collect committed J11 physical-storage evidence; target and commit visibility remain separate."""
import datetime
import json
import re
from pathlib import Path
import shutil
import sys
import xml.etree.ElementTree as ET
from evidence import java_reports, require_unchanged_source

REQUIRED = {"io.etcdhbase.hbase.storage.M1StorageIT",
            "io.etcdhbase.hbase.storage.HBasePageReaderTest",
            "io.etcdhbase.hbase.storage.HBaseRecordStoreTest",
            "io.etcdhbase.core.request.ScopedCleanupTest",
            "io.etcdhbase.hbase.probe.LocalHBaseProbeIT",
            "io.etcdhbase.hbase.probe.SecureHBaseAuthenticationIT"}
CASES = {"realGetPutAtomicCasBatchAndRawByteBucketOrder",
         "bothHistoryLayoutsKeepDeleteRecreateAndVersionProjectionAfterFlush",
         "partialBatchResultsKeepIndexesAndValueMismatchIsNotAnAbsentKey",
         "nativeTimestampCollisionIsRecordedAsACandidateLimitation",
         "bothLayoutsRetainVersionsAfterObservedMinorAndMajorCompaction",
         "wallClockTtlAndSmallVersionRetentionCannotStandInForLogicalCompaction"}


def collect(root, output):
    source = json.loads((output / "source.json").read_text())
    require_unchanged_source(root, source)
    paths, suites = java_reports(root, REQUIRED, ("spi", "core", "hbase"))
    report = root / "hbase/target/failsafe-reports/TEST-io.etcdhbase.hbase.storage.M1StorageIT.xml"
    suite = ET.parse(report).getroot()
    observed = {case.get("name") for case in suite.iter("testcase")}
    if not CASES <= observed:
        raise ValueError("required real J11 cases did not execute")
    stdout = "\n".join(element.text or "" for element in suite.iter("system-out"))
    layouts = re.findall(r"j11-layout layout=(HISTORY|NATIVE).*?minorObserved=true majorFileObserved=true", stdout)
    if layouts.count("HISTORY") != 4 or layouts.count("NATIVE") != 4 or "j11-retention wallClockTtlRejected=true maxVersionsNotLogicalCompact=true" not in stdout:
        raise ValueError("actual compaction/retention matrix evidence is missing")
    for path in paths:
        destination = output / "reports" / path.relative_to(root)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)
    for path in (root / "hbase/target/failsafe-reports").glob("m0-hbase-cluster-*.log"):
        shutil.copyfile(path, output / ("server-" + path.name))
    source.update(phase="J11", scope="public", ended_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  tests=sum(suite["tests"] for suite in suites.values()), failures=0, errors=0, skipped=0,
                  suites=suites, unverified=["target HBase", "B01/B02/B03/B06", "M2 commit and ReadView",
                                           "J52 retention and recovery", "maintainer review"])
    (output / "summary.json").write_text(json.dumps(source, indent=2) + "\n")
    print(json.dumps({key: source[key] for key in ("phase", "checkout", "pr_head", "base", "tests")}))


if __name__ == "__main__":
    try:
        collect(Path(__file__).resolve().parents[2], Path(sys.argv[1]))
    except (ValueError, OSError, KeyError):
        raise SystemExit("J11 verification evidence rejected") from None
