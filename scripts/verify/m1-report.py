#!/usr/bin/env python3
"""Collect actual M1 suites, preserving the committed source and acceptance boundaries."""
import datetime
import json
from pathlib import Path
import shutil
import sys
import xml.etree.ElementTree as ET
from evidence import java_reports, require_unchanged_source
from benchmark_evidence import collect as collect_benchmark

REQUIRED = {12: {"io.etcdhbase.core.scan.BucketCursorTest",
                 "io.etcdhbase.hbase.storage.HBasePageReaderTest", "io.etcdhbase.hbase.storage.M1StorageIT"},
            13: {"io.etcdhbase.core.scan.BudgetManagerTest", "io.etcdhbase.core.scan.ScanSchedulerTest"},
            14: {"io.etcdhbase.core.scan.SortedMergeCursorTest"},
            15: {"io.etcdhbase.core.scan.ExistsExecutorTest", "io.etcdhbase.core.scan.PinnedValueReadTest"},
            16: {"io.etcdhbase.core.scan.DirectRecordReaderTest"},
            17: {"io.etcdhbase.core.scan.StorageCostTest", "io.etcdhbase.hbase.storage.FrozenScanDatasetTest",
                 "io.etcdhbase.hbase.storage.M1ScanBenchmarkIT"}}
CASES = {12: "cursorReopensAfterOnlyConsumedKeysAndCancelsWithoutClosingSharedConnections",
         13: "onePhysicalScannerCanSupplyEveryBucketHeadWithoutClosingTheRequest",
         14: "globallySortedLimitMoreAndLightCountMatchBothFrozenHistoryLayouts",
         15: "metadataExistsAvoidsUnselectedValuesAndFullResultsPinTheSelectedVersion",
         16: "directReaderDoesNotCacheAndRequestCancellationClosesRetainedResults",
         17: "cancelledPhysicalPageClosesTheObservedServerScannerBeforeLeaseExpiry"}


def collect(root, output, phase):
    if phase not in {"J" + str(number) for number in REQUIRED}:
        raise ValueError("unsupported M1 verification phase")
    source = json.loads((output / "source.json").read_text())
    require_unchanged_source(root, source)
    through = int(phase[1:])
    required = set().union(*(names for number, names in REQUIRED.items() if number <= through))
    paths, suites = java_reports(root, required, ("spi", "core", "hbase"))
    actual = ET.parse(root / "hbase/target/failsafe-reports/TEST-io.etcdhbase.hbase.storage.M1StorageIT.xml").getroot()
    required_cases = {name for number, name in CASES.items() if number <= through}
    if not required_cases <= {case.get("name") for case in actual.iter("testcase")}:
        raise ValueError("required real M1 cases did not execute")
    if through >= 17:
        benchmark = ET.parse(root / "hbase/target/failsafe-reports/TEST-io.etcdhbase.hbase.storage.M1ScanBenchmarkIT.xml").getroot()
        stdout = "\n".join(element.text or "" for element in benchmark.iter("system-out"))
        source["benchmark"] = collect_benchmark(stdout)
        actual_stdout = "\n".join(element.text or "" for element in actual.iter("system-out"))
        if "m1-scanner-close remoteObservedOpen=true remoteObservedClosed=true localDrain=true" not in actual_stdout:
            raise ValueError("actual server scanner convergence evidence missing")
    for path in paths:
        destination = output / "reports" / path.relative_to(root)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)
    source.update(phase=phase, scope="public", ended_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  tests=sum(suite["tests"] for suite in suites.values()), failures=0, errors=0, skipped=0,
                  suites=suites, unverified=["target HBase and B01/B02/B03/B06", "B05 capacity",
                                           "M2 commit and ReadView",
                                           "target/other-client scanner convergence" if through >= 17 else "remote scanner lease convergence",
                                           "maintainer review"])
    (output / "summary.json").write_text(json.dumps(source, indent=2) + "\n")
    print(json.dumps({key: source[key] for key in ("phase", "checkout", "pr_head", "base", "tests")}))
    if through >= 17:
        for row in source["benchmark"]["cases"]:
            print(json.dumps({"benchmark": row}))


if __name__ == "__main__":
    try:
        collect(Path(__file__).resolve().parents[2], Path(sys.argv[1]), sys.argv[2])
    except (ValueError, OSError, KeyError):
        raise SystemExit("M1 verification evidence rejected") from None
