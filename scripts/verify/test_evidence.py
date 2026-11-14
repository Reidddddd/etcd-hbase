#!/usr/bin/env python3
"""Negative controls for shared Java/Go evidence readers."""
import json
import subprocess
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET
from evidence import GO_MODULE, GO_PACKAGES, go_report, java_reports, require_unchanged_source


class EvidenceTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def go_events(self, packages):
        events = []
        for package in sorted(packages):
            events.extend([{"Action": "pass", "Package": GO_MODULE + package, "Test": "TestContract"},
                           {"Action": "pass", "Package": GO_MODULE + package}])
        return events

    def write_go_events(self, events):
        path = self.root / "go.jsonl"
        path.write_text("".join(json.dumps(item) + "\n" for item in events))
        return path

    def test_java_requires_actual_cases_and_rejects_skips(self):
        path = self.root / "core/target/surefire-reports/TEST-scope.xml"
        path.parent.mkdir(parents=True)
        suite = ET.Element("testsuite", name="scope", tests="1", failures="0", errors="0", skipped="0")
        ET.SubElement(suite, "testcase", name="cancel")
        ET.ElementTree(suite).write(path)
        self.assertEqual(1, len(java_reports(self.root, {"scope"})[1]))
        for invalid in ("missing",):
            with self.assertRaises(ValueError):
                java_reports(self.root, {invalid})
        ET.SubElement(suite.find("testcase"), "skipped")
        ET.ElementTree(suite).write(path)
        with self.assertRaises(ValueError):
            java_reports(self.root, {"scope"})

    def test_java_rejects_skipped_counters_and_inconsistent_case_counts(self):
        path = self.root / "core/target/surefire-reports/TEST-scope.xml"
        path.parent.mkdir(parents=True)

        def write(tests="1", failures="0", errors="0", skipped="0", case_count=1):
            suite = ET.Element("testsuite", name="scope", tests=tests, failures=failures,
                               errors=errors, skipped=skipped)
            for index in range(case_count):
                ET.SubElement(suite, "testcase", name=f"case-{index}")
            ET.ElementTree(suite).write(path)

        invalid_reports = (
            {"tests": "0", "case_count": 0},
            {"skipped": "1"},
            {"failures": "1"},
            {"errors": "1"},
            {"tests": "2"},
            {"case_count": 2},
        )
        for changes in invalid_reports:
            with self.subTest(changes=changes):
                write(**changes)
                with self.assertRaises(ValueError):
                    java_reports(self.root, {"scope"})

    def test_java_can_restrict_report_kind_without_changing_default(self):
        for kind, name in (("surefire-reports", "unit"), ("failsafe-reports", "integration")):
            path = self.root / "core" / "target" / kind / f"TEST-{name}.xml"
            path.parent.mkdir(parents=True)
            suite = ET.Element("testsuite", name=name, tests="1", failures="0", errors="0", skipped="0")
            ET.SubElement(suite, "testcase", name="case")
            ET.ElementTree(suite).write(path)
        self.assertEqual({"unit", "integration"}, set(java_reports(self.root, {"unit"})[1]))
        self.assertEqual({"unit"}, set(java_reports(
            self.root, {"unit"}, report_kinds=("surefire-reports",))[1]))
        with self.assertRaises(ValueError):
            java_reports(self.root, {"integration"}, report_kinds=("surefire-reports",))

    def test_go_requires_every_package_and_executed_tests(self):
        events = self.go_events(GO_PACKAGES)
        path = self.write_go_events(events)
        self.assertEqual(len(GO_PACKAGES), go_report(path)["passed_test_events"])
        for damaged in (events[:-1], events + [{"Action": "skip"}], events + [events[-1]],
                        [event for event in events if "Test" not in event]):
            self.write_go_events(damaged)
            with self.assertRaises(ValueError):
                go_report(path)

    def test_go_requires_m3_driver(self):
        self.assertIn("cmd/m3driver", GO_PACKAGES)
        path = self.write_go_events(self.go_events(GO_PACKAGES - {"cmd/m3driver"}))
        with self.assertRaises(ValueError):
            go_report(path)

    def test_go_requires_m4_driver(self):
        self.assertIn("cmd/m4driver", GO_PACKAGES)
        path = self.write_go_events(self.go_events(GO_PACKAGES - {"cmd/m4driver"}))
        with self.assertRaises(ValueError):
            go_report(path)

        report = go_report(self.write_go_events(self.go_events(GO_PACKAGES)))
        self.assertEqual(GO_PACKAGES, set(report["packages"]))

    def test_go_rejects_unregistered_packages(self):
        events = self.go_events(GO_PACKAGES)
        events.extend(self.go_events({"cmd/unexpected"}))
        path = self.write_go_events(events)
        with self.assertRaises(ValueError):
            go_report(path)

    def test_go_requires_both_history_packages(self):
        path = self.root / "go.jsonl"
        history_packages = {"cmd/m3history", "internal/history"}
        self.assertTrue(history_packages.issubset(GO_PACKAGES))
        for missing in history_packages:
            events = []
            for package in sorted(GO_PACKAGES - {missing}):
                events.extend([{"Action": "pass", "Package": GO_MODULE + package, "Test": "TestContract"},
                               {"Action": "pass", "Package": GO_MODULE + package}])
            path.write_text("".join(json.dumps(item) + "\n" for item in events))
            with self.subTest(missing=missing), self.assertRaises(ValueError):
                go_report(path)


    def test_source_changes_and_untracked_code_invalidate_evidence(self):
        def git(*arguments):
            return subprocess.check_output(["git", "-C", str(self.root), *arguments], stderr=subprocess.DEVNULL).decode().strip()
        git("init", "-q")
        git("config", "user.name", "Evidence Fixture")
        git("config", "user.email", "fixture@example.invalid")
        path = self.root / "source.txt"
        path.write_text("original")
        git("add", "source.txt")
        git("commit", "-qm", "fixture")
        source = {"checkout": git("rev-parse", "HEAD"), "tracked_dirty": False}
        require_unchanged_source(self.root, source)
        path.write_text("changed")
        with self.assertRaises(ValueError):
            require_unchanged_source(self.root, source)
        path.write_text("original")
        (self.root / "untracked.py").write_text("uncommitted")
        with self.assertRaises(ValueError):
            require_unchanged_source(self.root, source)


if __name__ == "__main__":
    unittest.main()
