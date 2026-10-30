"""Validate the complete frozen public matrix without treating sampled counters as total IO."""
import itertools
import json

DIGEST = "80e44c6c5c8acadad794501fa41b4bb3de0679454bfbd91b001fb3975e1dab96"
CASES = {"empty": 0, "small": 2, "recovery": 8, "tombstones": 4, "old_view": 16,
         "large_value": 1, "hot_fixed": 1, "binary": 8, "long_key": 1}
LAYOUTS = ("HISTORY", "NATIVE")
BUCKETS = (1, 4, 16)
PHASES = ("memstore_warming", "flushed_cache_reset_then_warming")
OBSERVATIONS = {"SERVER_ROWS_SCANNED", "BLOCK_BYTES_SCANNED", "FS_BYTES", "BLOCK_CACHE_BYTES", "MEMSTORE_BYTES", "SCANNER_RETRIES"}


def integer(value, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError("invalid observed metric")


def collect(stdout):
    profiles = [json.loads(line.removeprefix("m1-profile ")) for line in stdout.splitlines() if line.startswith("m1-profile ")]
    rows = [json.loads(line.removeprefix("m1-benchmark ")) for line in stdout.splitlines() if line.startswith("m1-benchmark ")]
    expected_profiles = set(itertools.product(LAYOUTS, BUCKETS))
    if len(profiles) != 6 or {(row["layout"], row["buckets"]) for row in profiles} != expected_profiles:
        raise ValueError("missing or duplicate public layout/bucket profile")
    for profile in profiles:
        if (profile["digest"], profile["seed"], profile["logicalKeys"], profile["mutations"], profile["servers"], profile["regionsPerTable"]) != (DIGEST, 20261030, 78, 118, 3, 16):
            raise ValueError("frozen dataset or resource profile changed")
        tables = 2 if profile["layout"] == "HISTORY" else 1
        if profile["tables"] != tables or profile["applicationPutCalls"] != 118 * tables or profile["hotObservedRegions"] != 1:
            raise ValueError("actual auxiliary writes or hot-key locality evidence missing")
        for name, total in (("uniqueKeysPerBucket", 78), ("writesPerBucket", 118)):
            values = profile[name]
            if len(values) != profile["buckets"] or sum(values) != total:
                raise ValueError("bucket distribution evidence changed")
            for value in values:
                integer(value)
    expected_rows = set(itertools.product(LAYOUTS, BUCKETS, PHASES, CASES))
    if len(rows) != len(expected_rows) or {(row["layout"], row["buckets"], row["phase"], row["case"]) for row in rows} != expected_rows:
        raise ValueError("missing or duplicate benchmark case")
    for row in rows:
        if row["oracleMatches"] is not True or row["samples"] != 3 or row["returnedPerSample"] != CASES[row["case"]]:
            raise ValueError("logical oracle evidence changed")
        for name in ("returnedPhysicalRows", "returnedCellBytes", "observedScanRpcCalls", "applicationPointReads", "peakLeasedBytes", "p50Micros", "p95Micros", "p99Micros"):
            integer(row[name])
        if row["peakLocalScannerPermits"] != 1 or row["peakLeasedBytes"] > (66 << 20) + (row["buckets"] + 1) * 16384:
            raise ValueError("declared application resource bound exceeded")
        if not row["p50Micros"] <= row["p95Micros"] <= row["p99Micros"]:
            raise ValueError("latency summary contradicts percentiles")
        if row["applicationPointReads"] != (3 if row["case"] == "large_value" else 0):
            raise ValueError("unselected values were fetched or value-read accounting missing")
        if set(row["observedServerCounters"]) != OBSERVATIONS:
            raise ValueError("sampled server counters missing")
        for value in row["observedServerCounters"].values():
            integer(value)
    return {"digest": DIGEST, "profiles": profiles, "cases": rows,
            "capacity_accepted": False, "missing_input": "B05",
            "uncollected": ["complete META, point, close and late RPC counts", "physical disk IO", "JVM/direct-memory peak", "target workload"]}
