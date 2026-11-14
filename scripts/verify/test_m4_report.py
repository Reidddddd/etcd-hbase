#!/usr/bin/env python3
"""Regression tests for fail-closed M4 evidence validation."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET


SCRIPT = Path(__file__).with_name("m4-report.py")
SPEC = importlib.util.spec_from_file_location("m4_report", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
m4_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m4_report)


class M4ReportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = m4_report.load_registry()

    def test_registry_requires_j40_rpc_layout_and_regression_coverage(self) -> None:
        self.assertIsNotNone(m4_report.validate_registry_document(self.registry))

        without_trace = copy.deepcopy(self.registry)
        without_trace["tasks"][0]["trace_cases"].pop()
        with self.assertRaises(m4_report.ReportError):
            m4_report.validate_registry_document(without_trace)

        without_rpc = copy.deepcopy(self.registry)
        without_rpc["tasks"][0]["maven_tests"] = [
            item for item in without_rpc["tasks"][0]["maven_tests"]
            if item["class"] != "io.etcdhbase.server.M4LeaseRpcIT"
        ]
        with self.assertRaises(m4_report.ReportError):
            m4_report.validate_registry_document(without_rpc)

        without_layout = copy.deepcopy(self.registry)
        without_layout["tasks"][0]["evidence"]["targets"].pop()
        with self.assertRaises(m4_report.ReportError):
            m4_report.validate_registry_document(without_layout)

    def test_registry_requires_nonempty_binding_and_ttl_projection_cases(self) -> None:
        required_cases = {
            "io.etcdhbase.core.commit.LeaseReadViewTest":
                "bindingReadMergesStableHistoryWithSelectedChangesAtOneBarrier",
            "io.etcdhbase.grpc.protocol.LeaseTimeToLiveResponseMapperTest":
                "includesEveryRequestedKeyAsOriginalBytesWithThePinnedHeader",
        }
        for class_name, required_case in required_cases.items():
            with self.subTest(class_name=class_name):
                missing_case = copy.deepcopy(self.registry)
                suite = next(item for item in missing_case["tasks"][0]["maven_tests"]
                             if item["class"] == class_name)
                suite["tests"].remove(required_case)
                with self.assertRaisesRegex(m4_report.ReportError, "binding, TTL projection"):
                    m4_report.validate_registry_document(missing_case)

    def test_all_tasks_is_rejected_while_j41_to_j47_are_pending(self) -> None:
        with self.assertRaisesRegex(m4_report.ReportError, "not registered"):
            m4_report.validate_all_tasks_registered(self.registry)

    def test_complete_j40_artifact_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            summary = m4_report.validate_artifact(
                m4_report.REPO, artifact, self.registry
            )
            self.assertEqual(summary["task"], "J40")
            self.assertEqual(summary["target_layouts"], ["HISTORY", "NATIVE"])
            self.assertGreater(summary["go_test_count"], 0)

    def test_complete_j41_artifact_is_read_from_its_registered_subdirectory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J41")
            summary = m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)
            self.assertEqual(summary["task"], "J41")
            self.assertEqual(summary["target_layouts"], ["HISTORY", "NATIVE"])

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J41")
            evidence = artifact / "j41"
            (evidence / "native-trace.jsonl").replace(artifact / "native-trace.jsonl")
            with self.assertRaisesRegex(m4_report.ReportError, "required M4 trace"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_j41_registration_rejects_unsafe_evidence_subdirectories(self) -> None:
        for subdirectory in ("../outside", "/tmp/evidence", "j41\\nested"):
            with self.subTest(subdirectory=subdirectory):
                changed = copy.deepcopy(self.registry)
                task = next(item for item in changed["tasks"] if item["id"] == "J41")
                task["artifact_subdir"] = subdirectory
                with self.assertRaises(m4_report.ReportError):
                    m4_report.validate_registry_document(changed)

    def test_j41_registry_requires_binding_plan_and_owner_fence_regressions(self) -> None:
        required = {
            "io.etcdhbase.server.commit.LeaseBindingMutationPlannerTest":
                "revokedStateUpdatesDoNotWriteNewExpiryCandidates",
            "io.etcdhbase.server.commit.M3OwnerLeaseTest":
                "startupFormatUpgradeRotatesTheFenceWithoutLosingOwnerRenewal",
        }
        for class_name, method_name in required.items():
            with self.subTest(class_name=class_name):
                without_class = copy.deepcopy(self.registry)
                task = next(item for item in without_class["tasks"] if item["id"] == "J41")
                task["maven_tests"] = [item for item in task["maven_tests"]
                                        if item["class"] != class_name]
                with self.assertRaises(m4_report.ReportError):
                    m4_report.validate_registry_document(without_class)

                without_method = copy.deepcopy(self.registry)
                task = next(item for item in without_method["tasks"] if item["id"] == "J41")
                suite = next(item for item in task["maven_tests"] if item["class"] == class_name)
                suite["tests"].remove(method_name)
                with self.assertRaises(m4_report.ReportError):
                    m4_report.validate_registry_document(without_method)

    def test_j41_registry_requires_each_go_binding_regression(self) -> None:
        for package, required_tests in m4_report.J41_GO_TESTS.items():
            for test_name in required_tests:
                with self.subTest(package=package, test_name=test_name):
                    changed = copy.deepcopy(self.registry)
                    task = next(item for item in changed["tasks"] if item["id"] == "J41")
                    suite = next(item for item in task["go_tests"]
                                 if item["package"] == package)
                    suite["tests"].remove(test_name)
                    with self.assertRaisesRegex(
                        m4_report.ReportError, "Go binding regression coverage is incomplete"
                    ):
                        m4_report.validate_registry_document(changed)

    def test_missing_or_skipped_required_maven_case_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            report_path = artifact / "server/target/failsafe-reports/TEST-io.etcdhbase.server.M4LeaseRpcIT.xml"
            report_tree = ET.parse(report_path)
            suite = report_tree.getroot()
            suite.attrib["tests"] = "0"
            suite.clear()
            report_tree.write(report_path, encoding="utf-8", xml_declaration=True)
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            report_path = artifact / "server/target/failsafe-reports/TEST-io.etcdhbase.server.M4LeaseRpcIT.xml"
            report_tree = ET.parse(report_path)
            testcase = report_tree.getroot().find("testcase")
            assert testcase is not None
            ET.SubElement(testcase, "skipped")
            report_tree.getroot().attrib["skipped"] = "1"
            report_tree.write(report_path, encoding="utf-8", xml_declaration=True)
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_corrupt_reordered_or_missing_layout_trace_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            trace_path = artifact / "target-history-trace.jsonl"
            trace_path.write_text("{broken}\n", encoding="utf-8")
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            trace_path = artifact / "native-trace.jsonl"
            records = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
            records[0], records[1] = records[1], records[0]
            trace_path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            (artifact / "target-native-trace.jsonl").unlink()
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_trace_ordinals_are_monotonic_per_case_while_file_order_is_global(self) -> None:
        record = {
            "schema": "etcd-hbase-trace/v1",
            "case_id": "repeated-case",
            "ordinal": 1,
            "method": "etcdserverpb.Lease/LeaseLeases",
            "request": {"fields": {}, "oneofs": {}},
            "response": {"fields": {}, "oneofs": {}},
            "error": None,
            "stream_terminal": None,
            "evidence_scope": "native-etcd",
            "elapsed_ns": "1",
        }
        expected = [
            {"id": "repeated-case", "method": record["method"]},
            {"id": "repeated-case", "method": record["method"]},
        ]
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "trace.jsonl"
            second = {**record, "ordinal": 2}
            path.write_text(
                json.dumps(record) + "\n" + json.dumps(second) + "\n", encoding="utf-8"
            )
            m4_report._read_trace(path, expected, "native-etcd")

            second["ordinal"] = 1
            path.write_text(
                json.dumps(record) + "\n" + json.dumps(second) + "\n", encoding="utf-8"
            )
            with self.assertRaises(m4_report.ReportError):
                m4_report._read_trace(path, expected, "native-etcd")

    def test_trace_digest_and_source_identity_are_bound_to_run(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            metadata_path = artifact / "target-history-metadata.json"
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            metadata["source_commit"] = "f" * 40
            metadata_path.write_text(json.dumps(metadata) + "\n", encoding="utf-8")
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_go_skip_or_missing_required_test_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            path = artifact / "go-test.jsonl"
            events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            events[0]["Action"] = "skip"
            path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            path = artifact / "go-test.jsonl"
            events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            events[0]["Action"] = "unknown"
            path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            with self.assertRaisesRegex(m4_report.ReportError, "unknown action"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def _write_valid_artifact(self, artifact: Path, task_id: str = "J40") -> None:
        task = next(item for item in self.registry["tasks"] if item["id"] == task_id)
        run_id = artifact.name
        source_commit = "a" * 40
        base_commit = "b" * 40
        evidence = task["evidence"]
        evidence_root = artifact / task.get("artifact_subdir", "")
        evidence_root.mkdir(parents=True, exist_ok=True)
        command_logs = {
            "go-module-lock": "go-module-lock.log",
            "go-tests": "go-test.jsonl",
            "maven-verify": "maven.log",
            "collect-maven-reports": "maven-reports.log",
        }
        for path in command_logs.values():
            target = artifact / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("completed\n", encoding="utf-8")
        (artifact / "go-test.stderr").write_text("", encoding="utf-8")

        commands = [
            {
                "name": name,
                "argv": ["test-command", name],
                "exit_code": 0,
                "started_at": "2026-11-14T00:00:00Z",
                "finished_at": "2026-11-14T00:00:01Z",
                "stdout_log": path,
                "stderr_log": "go-test.stderr" if name == "go-tests" else None,
            }
            for name, path in command_logs.items()
        ]
        run = {
            "schema": "etcd-hbase-m4-run/v1",
            "run_id": run_id,
            "scope": "public-local-minicluster",
            "tasks": [task["id"]],
            "status": "passed",
            "source_commit": source_commit,
            "checkout": source_commit,
            "base_commit": base_commit,
            "worktree_clean": True,
            "started_at": "2026-11-14T00:00:00Z",
            "finished_at": "2026-11-14T00:00:02Z",
            "commands": commands,
        }
        (artifact / "run.json").write_text(json.dumps(run) + "\n", encoding="utf-8")

        go_events = []
        for package in task["go_tests"]:
            module = f"github.com/Reidddddd/etcd-hbase/testkit/go/{package['package']}"
            go_events.append({"Action": "start", "Package": module})
            for test in package["tests"]:
                go_events.append({"Action": "pass", "Package": module, "Test": test})
            go_events.append({"Action": "pass", "Package": module})
        (artifact / "go-test.jsonl").write_text(
            "".join(json.dumps(event) + "\n" for event in go_events), encoding="utf-8"
        )

        grouped: dict[tuple[str, str, str], set[str]] = {}
        for suite in task["maven_tests"]:
            grouped.setdefault((suite["module"], suite["kind"], suite["class"]), set()).update(suite["tests"])
        for (module, kind, class_name), test_names in grouped.items():
            report_dir = artifact / module / "target" / f"{kind}-reports"
            report_dir.mkdir(parents=True, exist_ok=True)
            root = ET.Element("testsuite", {
                "name": class_name,
                "tests": str(len(test_names)),
                "failures": "0",
                "errors": "0",
                "skipped": "0",
            })
            for name in sorted(test_names):
                ET.SubElement(root, "testcase", {"name": name, "classname": class_name})
            ET.ElementTree(root).write(
                report_dir / f"TEST-{class_name}.xml", encoding="utf-8", xml_declaration=True
            )

        trace_variants = [(evidence["native"], "native", "native-trace.jsonl", "native-metadata.json")]
        for target in evidence["targets"]:
            suffix = target["layout"].lower()
            trace_variants.append((target, suffix, f"target-{suffix}-trace.jsonl", f"target-{suffix}-metadata.json"))
        prefix = f"etcd-hbase-m4-test/{run_id}/"
        if task_id == "J41":
            prefix += "bindings/"
        prefix_hash = hashlib.sha256(prefix.encode()).hexdigest()
        suite_path = m4_report.REPO / "testkit/scenarios" / task["manifest"]
        suite_hash = hashlib.sha256(suite_path.read_bytes()).hexdigest()
        for descriptor, suffix, trace_name, metadata_name in trace_variants:
            records = []
            case_ordinals: dict[str, int] = {}
            for expected in task["trace_cases"]:
                case_ordinals[expected["id"]] = case_ordinals.get(expected["id"], 0) + 1
                error = None
                response = {"fields": {}, "oneofs": {}}
                if expected["id"] == "duplicate-id":
                    error = {"grpc_code": "FailedPrecondition", "consumer_kind": "grpc-status", "semantic_kind": "LEASE_EXISTS"}
                    response = None
                elif expected["id"] == "oversized-ttl":
                    error = {"grpc_code": "OutOfRange", "consumer_kind": "grpc-status", "semantic_kind": "OUT_OF_RANGE"}
                    response = None
                elif expected["id"] in {
                    "binding-invalid-lease-atomicity",
                    "binding-invalid-lease-put",
                }:
                    error = {"grpc_code": "NotFound", "consumer_kind": "grpc-status", "semantic_kind": "LEASE_NOT_FOUND"}
                    response = None
                records.append({
                    "schema": "etcd-hbase-trace/v1",
                    "case_id": expected["id"],
                    "ordinal": case_ordinals[expected["id"]],
                    "method": expected["method"],
                    "request": {"fields": {}, "oneofs": {}},
                    "response": response,
                    "error": error,
                    "stream_terminal": None,
                    "evidence_scope": descriptor["scope"],
                    "elapsed_ns": "1",
                })
            trace_bytes = "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records).encode()
            (evidence_root / trace_name).write_bytes(trace_bytes)
            embedded_native = descriptor["embedded_native"]
            layout = descriptor.get("layout", "")
            metadata = {
                "schema": "etcd-hbase-m4-run/v1",
                "run_id": run_id,
                "source_commit": source_commit,
                "suite_sha256": suite_hash,
                "evidence_scope": descriptor["scope"],
                "prefix_sha256": prefix_hash,
                "trace_sha256": hashlib.sha256(trace_bytes).hexdigest(),
                "embedded_native": embedded_native,
                "layout": layout,
                "status": "passed",
            }
            (evidence_root / metadata_name).write_text(json.dumps(metadata) + "\n", encoding="utf-8")
            log_name = (
                "native.log" if embedded_native else f"target-{suffix}.log"
            )
            (evidence_root / log_name).write_text("M4 Lease scenario passed\n", encoding="utf-8")
        for name in evidence["compare_logs"]:
            success = (m4_report.J41_COMPARE_SUCCESS if task_id == "J41"
                       else m4_report.COMPARE_SUCCESS)
            (evidence_root / name).write_text(success + "\n", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
