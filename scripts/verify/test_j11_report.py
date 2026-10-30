#!/usr/bin/env python3
"""Negative checks for the new real-scenario evidence gate, independent of Maven execution."""
import importlib.util
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

spec = importlib.util.spec_from_file_location("j11_report", Path(__file__).with_name("j11-report.py"))
reporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reporter)


class J11EvidenceTest(unittest.TestCase):
    def check_report(self, missing_case=False, missing_matrix=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "evidence"
            output.mkdir()
            (output / "source.json").write_text(json.dumps({"checkout": "0" * 40, "pr_head": "0" * 40, "base": None}))
            target = root / "hbase/target/failsafe-reports/TEST-io.etcdhbase.hbase.storage.M1StorageIT.xml"
            target.parent.mkdir(parents=True)
            suite = ET.Element("testsuite")
            cases = sorted(reporter.CASES)
            for name in cases[1:] if missing_case else cases:
                ET.SubElement(suite, "testcase", name=name)
            stdout = ET.SubElement(suite, "system-out")
            stdout.text = "\n".join(f"j11-layout layout={layout} minorObserved=true majorFileObserved=true"
                                    for layout in ("HISTORY", "NATIVE") for _ in range(4))
            stdout.text += "\nj11-retention wallClockTtlRejected=true maxVersionsNotLogicalCompact=true"
            if missing_matrix:
                stdout.text = ""
            ET.ElementTree(suite).write(target)
            counts = {"fixture": {"tests": len(cases), "failures": 0, "errors": 0, "skipped": 0}}
            with (
                patch.object(reporter, "require_unchanged_source"),
                patch.object(reporter, "java_reports", return_value=([target], counts)),
                redirect_stdout(io.StringIO()),
            ):
                reporter.collect(root, output)
            return json.loads((output / "summary.json").read_text())

    def test_complete_unit_fixture_passes_only_the_report_gate(self):
        summary = self.check_report()
        self.assertEqual("J11", summary["phase"])
        self.assertIn("target HBase", summary["unverified"])

    def test_missing_scenario_cannot_report_success(self):
        with self.assertRaisesRegex(ValueError, "cases did not execute"):
            self.check_report(missing_case=True)

    def test_missing_real_compaction_markers_cannot_report_success(self):
        with self.assertRaisesRegex(ValueError, "matrix evidence is missing"):
            self.check_report(missing_matrix=True)


if __name__ == "__main__":
    unittest.main()
