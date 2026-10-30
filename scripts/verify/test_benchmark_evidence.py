#!/usr/bin/env python3
import copy
import itertools
import json
import unittest
import benchmark_evidence as evidence


def fixture():
    profiles, rows = [], []
    for layout, buckets in itertools.product(evidence.LAYOUTS, evidence.BUCKETS):
        tables = 2 if layout == "HISTORY" else 1
        profiles.append(dict(layout=layout, buckets=buckets, digest=evidence.DIGEST, seed=20261030,
                             logicalKeys=78, mutations=118, servers=3, regionsPerTable=16, tables=tables,
                             applicationPutCalls=118 * tables, hotObservedRegions=1,
                             uniqueKeysPerBucket=[78] + [0] * (buckets - 1), writesPerBucket=[118] + [0] * (buckets - 1)))
        for phase, case in itertools.product(evidence.PHASES, evidence.CASES):
            rows.append(dict(layout=layout, buckets=buckets, phase=phase, case=case, samples=3,
                             returnedPerSample=evidence.CASES[case], returnedPhysicalRows=0, returnedCellBytes=0,
                             observedScanRpcCalls=0, applicationPointReads=3 if case == "large_value" else 0,
                             peakLeasedBytes=0, peakLocalScannerPermits=1, p50Micros=0, p95Micros=0, p99Micros=0,
                             observedServerCounters=dict.fromkeys(evidence.OBSERVATIONS, 0), oracleMatches=True))
    return profiles, rows


def stdout(profiles, rows):
    return "\n".join([*("m1-profile " + json.dumps(row) for row in profiles),
                      *("m1-benchmark " + json.dumps(row) for row in rows)])


class BenchmarkEvidenceTest(unittest.TestCase):
    def test_complete_matrix_is_accepted_but_never_capacity_accepted(self):
        result = evidence.collect(stdout(*fixture()))
        self.assertEqual(108, len(result["cases"]))
        self.assertFalse(result["capacity_accepted"])

    def test_missing_duplicate_or_wrong_dataset_is_rejected(self):
        profiles, rows = fixture()
        for invalid in (rows[:-1], rows[:-1] + [rows[0]]):
            with self.assertRaises(ValueError):
                evidence.collect(stdout(profiles, invalid))
        profiles[0]["digest"] = "0" * 64
        with self.assertRaises(ValueError):
            evidence.collect(stdout(profiles, rows))

    def test_unbounded_resource_or_missing_cost_cannot_be_hidden_by_oracle_success(self):
        profiles, rows = fixture()
        for key, value in (("peakLocalScannerPermits", 2), ("observedServerCounters", {}), ("observedScanRpcCalls", -1)):
            invalid = copy.deepcopy(rows)
            invalid[0][key] = value
            with self.assertRaises(ValueError):
                evidence.collect(stdout(profiles, invalid))


if __name__ == "__main__":
    unittest.main()
