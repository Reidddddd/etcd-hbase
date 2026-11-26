#!/usr/bin/env python3
"""Regression tests for fail-closed M5 task and test evidence validation."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET


SCRIPT = Path(__file__).with_name("m5-report.py")
SPEC = importlib.util.spec_from_file_location("m5_report", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
m5_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m5_report)


class M5ReportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name)
        self.registry = self.repo / "testkit/cases/m5"
        self.registry.mkdir(parents=True)
        source = Path(__file__).parents[2] / "testkit/cases/m5/J50.json"
        self.task = json.loads(source.read_text(encoding="utf-8"))
        (self.registry / "J50.json").write_text(
            json.dumps(self.task, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        self._write_profile("hbase", "**/HBaseAuthControlIT.java")
        self._write_profile("server", "**/M5AuthProcessIT.java",
                            "**/M5AuthenticateGatewayProcessIT.java")
        self._write_test_sources()

    def _write_test_sources(self) -> None:
        source_groups: dict[tuple[str, str], set[str]] = {}
        for test in self.task["tests"]:
            key = (test["module"], test["class"])
            source_groups.setdefault(key, set()).update(test["methods"])
        for (module, class_name), methods in source_groups.items():
            path = self.repo / module / "src/test/java" / Path(*class_name.split(".")).with_suffix(".java")
            path.parent.mkdir(parents=True, exist_ok=True)
            declarations = "\n".join(f"  void {method}() {{}}" for method in sorted(methods))
            path.write_text(f"class TestSource {{\n{declarations}\n}}\n", encoding="utf-8")

    def _write_profile(self, module: str, *includes: str, profile_id: str = "m5-j50-public") -> None:
        include_xml = "\n".join(f"<include>{include}</include>" for include in includes)
        profile = f"""<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <profiles><profile><id>{profile_id}</id><build><plugins><plugin>
    <artifactId>maven-failsafe-plugin</artifactId><configuration>
      <skipITs>false</skipITs><includes>{include_xml}</includes>
      {"<reuseForks>false</reuseForks>" if module == "server" else ""}
    </configuration>
  </plugin></plugins></build></profile></profiles>
</project>
"""
        path = self.repo / module / "pom.xml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(profile, encoding="utf-8")

    def _write_reports(self, overrides: dict[str, tuple[int, int, int, int]] | None = None) -> None:
        overrides = overrides or {}
        for test in self.task["tests"]:
            module = test["module"]
            kind = test["kind"]
            class_name = test["class"]
            tests, failures, errors, skipped = overrides.get(
                class_name, (len(test["methods"]), 0, 0, 0))
            root = ET.Element("testsuite", {
                "name": class_name,
                "tests": str(tests),
                "failures": str(failures),
                "errors": str(errors),
                "skipped": str(skipped),
            })
            for method in test["methods"]:
                ET.SubElement(root, "testcase", {"classname": class_name, "name": method})
            path = self.repo / module / "target" / f"{kind}-reports" / f"TEST-{class_name}.xml"
            path.parent.mkdir(parents=True, exist_ok=True)
            ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)

    def _write_run(self, artifact: Path, maven_exit_code: int = 0, task_id: str = "J50") -> None:
        artifact.mkdir(parents=True)
        run = {
            "schema": m5_report.RUN_SCHEMA,
            "task": task_id,
            "scope": "public",
            "status": "running",
            "commands": [{"name": "maven-verify", "exit_code": maven_exit_code}],
            "reports": [],
        }
        (artifact / "run.json").write_text(
            json.dumps(run, ensure_ascii=False), encoding="utf-8")

    def test_registry_uses_repository_root_for_maven_profiles(self) -> None:
        registered = m5_report._validate_registry(self.registry)
        self.assertEqual({"J50"}, set(registered))

    def test_maven_selectors_are_derived_from_the_registered_j50_suites(self) -> None:
        surefire, failsafe = m5_report._maven_selectors(self.task)
        expected_surefire = sorted({
            test["class"].rsplit(".", 1)[-1]
            for test in self.task["tests"] if test["kind"] == "surefire"
        })
        self.assertEqual(",".join(expected_surefire), surefire)
        self.assertEqual(
            "HBaseAuthControlIT#provisionsExactDescriptorAndPersistsSessionAuthorityAndSlotCas,"
            "M5AuthProcessIT#sessionsActivateAllRegisteredHBaseMechanismsAcrossProcessesAndFullRestart,"
            "M5AuthenticateGatewayProcessIT#authenticatesOnAThenUsesBGetPutTxnAndReactivatesAfterFullStop",
            failsafe)

    def test_registry_rejects_duplicate_json_keys_and_unknown_tasks(self) -> None:
        path = self.registry / "J50.json"
        path.write_text('{"task":"J50","task":"J50"}\n', encoding="utf-8")
        with self.assertRaisesRegex(m5_report.ReportError, "duplicate JSON key"):
            m5_report._validate_registry(self.registry)

        path.write_text(json.dumps(self.task), encoding="utf-8")
        (self.registry / "J99.json").write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(m5_report.ReportError, "unknown M5 task ID"):
            m5_report._validate_registry(self.registry)

    def test_registry_rejects_method_names_missing_from_test_sources(self) -> None:
        first = self.task["tests"][0]
        source = (self.repo / first["module"] / "src/test/java"
                  / Path(*first["class"].split(".")).with_suffix(".java"))
        source.write_text("class TestSource {}\n", encoding="utf-8")
        with self.assertRaisesRegex(m5_report.ReportError, "methods are absent from source"):
            m5_report._validate_registry(self.registry)

    def test_registry_requires_authenticated_range_identity_and_drain_suite(self) -> None:
        required_class = "io.etcdhbase.server.commit.TxnGatewayAuthenticationTest"
        self.task["tests"] = [entry for entry in self.task["tests"]
                              if entry["class"] != required_class]
        (self.registry / "J50.json").write_text(
            json.dumps(self.task, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        with self.assertRaisesRegex(m5_report.ReportError,
                                    "J50 required test methods are missing: "
                                    "io.etcdhbase.server.commit.TxnGatewayAuthenticationTest"):
            m5_report._validate_registry(self.registry)

    def test_registry_requires_cancelled_authentication_producer_drain(self) -> None:
        required_class = "io.etcdhbase.server.auth.AuthSessionServiceTest"
        entry = next(test for test in self.task["tests"] if test["class"] == required_class)
        entry["methods"].remove("cancelledAuthenticationKeepsItsSlotUntilPhysicalProducersDrain")
        (self.registry / "J50.json").write_text(
            json.dumps(self.task, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        with self.assertRaisesRegex(m5_report.ReportError,
                                    "J50 required test methods are missing: " + required_class):
            m5_report._validate_registry(self.registry)

    def test_registry_requires_restorable_and_rpc_cleanup_regressions(self) -> None:
        required_class = "io.etcdhbase.server.auth.AuthSessionServiceTest"
        methods = ("failedRestorableVerificationDiscardsIdentityAndReleasesSlot",
                   "synchronousRestorableFailureDiscardsIdentityAndReleasesSlot",
                   "cancelledRpcReleasesReservationAfterRealProducerDrains")
        entry = next(test for test in self.task["tests"] if test["class"] == required_class)
        for method in methods:
            with self.subTest(method=method):
                entry["methods"].remove(method)
                (self.registry / "J50.json").write_text(
                    json.dumps(self.task, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                with self.assertRaisesRegex(m5_report.ReportError,
                                            "J50 required test methods are missing: " + required_class):
                    m5_report._validate_registry(self.registry)
                entry["methods"].append(method)

    def test_registry_requires_authenticate_to_non_owner_gateway_rpc_suite(self) -> None:
        required_class = "io.etcdhbase.server.commit.M5GatewayAuthenticationRpcTest"
        self.task["tests"] = [entry for entry in self.task["tests"]
                              if entry["class"] != required_class]
        (self.registry / "J50.json").write_text(
            json.dumps(self.task, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        with self.assertRaisesRegex(m5_report.ReportError,
                                    "J50 required test methods are missing: "
                                    "io.etcdhbase.server.commit.M5GatewayAuthenticationRpcTest"):
            m5_report._validate_registry(self.registry)

    def test_registry_requires_unauthenticated_put_and_txn_regression(self) -> None:
        required_class = "io.etcdhbase.server.commit.M5GatewayAuthenticationRpcTest"
        method = "noTokenPutAndTxnSucceedWithServiceIdentityWhenAuthenticationIsDisabled"
        entry = next(test for test in self.task["tests"] if test["class"] == required_class)
        entry["methods"].remove(method)
        (self.registry / "J50.json").write_text(
            json.dumps(self.task, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        with self.assertRaisesRegex(m5_report.ReportError,
                                    "J50 required test methods are missing: " + required_class):
            m5_report._validate_registry(self.registry)

    def test_registry_requires_real_cross_process_authenticate_gateway_suite(self) -> None:
        required_class = "io.etcdhbase.server.M5AuthenticateGatewayProcessIT"
        self.task["tests"] = [entry for entry in self.task["tests"]
                              if entry["class"] != required_class]
        (self.registry / "J50.json").write_text(
            json.dumps(self.task, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        with self.assertRaisesRegex(m5_report.ReportError,
                                    "J50 required test methods are missing: "
                                    "io.etcdhbase.server.M5AuthenticateGatewayProcessIT"):
            m5_report._validate_registry(self.registry)

    def test_server_profile_isolates_temporary_kerberos_realms_by_jvm(self) -> None:
        path = self.repo / "server/pom.xml"
        content = path.read_text(encoding="utf-8")
        path.write_text(content.replace("<reuseForks>false</reuseForks>", ""), encoding="utf-8")

        with self.assertRaisesRegex(m5_report.ReportError,
                                    "J50 server profile must isolate Kerberos MiniKDC tests by JVM"):
            m5_report._validate_registry(self.registry)

    def test_registry_rejects_missing_or_skippable_maven_profile(self) -> None:
        for content in (
            "<project xmlns=\"http://maven.apache.org/POM/4.0.0\"><modelVersion>4.0.0</modelVersion></project>",
            """<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
              <profiles><profile><id>m5-j50-public</id><build><plugins><plugin>
              <artifactId>maven-failsafe-plugin</artifactId><configuration><skipITs>true</skipITs>
              </configuration></plugin></plugins></build></profile></profiles></project>""",
        ):
            (self.repo / "hbase/pom.xml").write_text(content, encoding="utf-8")
            with self.assertRaises(m5_report.ReportError):
                m5_report._validate_registry(self.registry)
            self._write_profile("hbase", "**/HBaseAuthControlIT.java")

    def test_report_verification_copies_only_current_nonzero_successful_suites(self) -> None:
        self._write_reports()
        artifact = self.repo / ".artifacts/m5/success"
        self._write_run(artifact)

        m5_report.verify_reports(self.repo, "J50", artifact)

        run = json.loads((artifact / "run.json").read_text(encoding="utf-8"))
        self.assertEqual(len(self.task["tests"]), run["test_counts"]["suites"])
        self.assertEqual(sum(len(test["methods"]) for test in self.task["tests"]),
                         run["test_counts"]["tests"])
        copied = list((artifact / "reports").rglob("TEST-*.xml"))
        self.assertEqual(len(self.task["tests"]), len(copied))

    def test_report_verification_rejects_missing_zero_failed_and_skipped_reports(self) -> None:
        class_name = self.task["tests"][0]["class"]
        cases = (
            ("missing", None),
            ("zero", (0, 0, 0, 0)),
            ("failure", (1, 1, 0, 0)),
            ("skipped", (1, 0, 0, 1)),
        )
        for name, counters in cases:
            with self.subTest(name=name):
                self._write_reports()
                first_test = self.task["tests"][0]
                report = (self.repo / first_test["module"] / "target"
                          / f"{first_test['kind']}-reports"
                          / f"TEST-{class_name}.xml")
                if name == "missing":
                    report.unlink()
                else:
                    self._write_reports({class_name: counters})
                artifact = self.repo / f".artifacts/m5/{name}"
                self._write_run(artifact)
                with self.assertRaises(m5_report.ReportError):
                    m5_report.verify_reports(self.repo, "J50", artifact)

    def test_report_verification_requires_successful_maven_and_all_registered_methods(self) -> None:
        self._write_reports()
        artifact = self.repo / ".artifacts/m5/maven-failed"
        self._write_run(artifact, maven_exit_code=1)
        with self.assertRaisesRegex(m5_report.ReportError, "successful Maven command"):
            m5_report.verify_reports(self.repo, "J50", artifact)

        class_name = self.task["tests"][0]["class"]
        self._write_reports({class_name: (1, 0, 0, 0)})
        first_test = self.task["tests"][0]
        report = (self.repo / first_test["module"] / "target"
                  / f"{first_test['kind']}-reports" / f"TEST-{class_name}.xml")
        root = ET.parse(report).getroot()
        root.clear()
        root.attrib.update({"name": class_name, "tests": "1", "failures": "0", "errors": "0", "skipped": "0"})
        ET.SubElement(root, "testcase", {"classname": class_name, "name": "unexpectedMethod"})
        ET.ElementTree(root).write(report, encoding="utf-8", xml_declaration=True)

        artifact = self.repo / ".artifacts/m5/missing-method"
        self._write_run(artifact)
        with self.assertRaisesRegex(m5_report.ReportError, "test methods did not run"):
            m5_report.verify_reports(self.repo, "J50", artifact)

    def test_finalize_rejects_missing_or_zero_test_evidence(self) -> None:
        artifact = self.repo / ".artifacts/m5/finalize"
        self._write_run(artifact)
        run_file = artifact / "run.json"
        record = json.loads(run_file.read_text(encoding="utf-8"))
        record["commands"] = [
            {"name": command, "exit_code": 0}
            for command in ("registry-check", "report-tests", "maven-verify", "report-verify")
        ]
        record["reports"] = [{"class": "registered-test"}]
        record["test_counts"] = {
            "suites": 1, "tests": 1, "failures": 0, "errors": 0, "skipped": 0,
        }
        run_file.write_text(json.dumps(record), encoding="utf-8")
        m5_report.finalize_run(run_file, 0)
        self.assertEqual("passed", json.loads(run_file.read_text())["status"])

        record["status"] = "running"
        record["test_counts"]["tests"] = 0
        run_file.write_text(json.dumps(record), encoding="utf-8")
        with self.assertRaisesRegex(m5_report.ReportError, "missing command or test report"):
            m5_report.finalize_run(run_file, 0)
        self.assertEqual("failed", json.loads(run_file.read_text())["status"])

    def test_j51_reports_require_each_registered_suite_and_method(self) -> None:
        source = Path(__file__).parents[2] / "testkit/cases/m5/J51.json"
        self.task = json.loads(source.read_text(encoding="utf-8"))
        (self.registry / "J51.json").write_text(json.dumps(self.task), encoding="utf-8")
        self._write_profile("server", "**/M5AuthenticateGatewayProcessIT.java",
                            profile_id="m5-j51-public")
        self._write_test_sources()
        self._write_reports()
        success = self.repo / ".artifacts/m5/j51-success"
        self._write_run(success, task_id="J51")
        m5_report.verify_reports(self.repo, "J51", success)
        run = json.loads((success / "run.json").read_text(encoding="utf-8"))
        self.assertEqual(len(self.task["tests"]), run["test_counts"]["suites"])

        first = self.task["tests"][0]
        report = (self.repo / first["module"] / "target" / f"{first['kind']}-reports"
                  / f"TEST-{first['class']}.xml")
        report.unlink()
        missing = self.repo / ".artifacts/m5/j51-missing"
        self._write_run(missing, task_id="J51")
        with self.assertRaisesRegex(m5_report.ReportError, "registered Maven test report is missing"):
            m5_report.verify_reports(self.repo, "J51", missing)


class J51RegistryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = Path(__file__).parents[2]
        self.path = self.repo / "testkit/cases/m5/J51.json"
        self.task = json.loads(self.path.read_text(encoding="utf-8"))

    def test_j50_and_j51_registry_validate_together(self) -> None:
        registered = m5_report._validate_registry()
        self.assertEqual({"J50", "J51"}, set(registered))
        surefire, failsafe = m5_report._maven_selectors(registered["J51"])
        self.assertIn("CommitPublisherTest", surefire.split(","))
        self.assertEqual("", failsafe)

    def test_j51_rejects_missing_required_revocation_and_cache_tests(self) -> None:
        for method in ("installedRevocationBarrierRejectsASelectionUsingAnEarlierControlSnapshot",
                       "invalidationPreventsAnInFlightPhysicalReadFromRefillingTheCache",
                       "staleReservationRetriesAfterAnotherOperationOrConfigurationUpdate",
                       "deploymentRotationBlocksBothGroupsUntilTheirBarriersComplete",
                       "authStatusReturnsThePersistedBridgeRevisionAndUnavailableWhenMissing",
                       "configurationUpdateFencesAnAlreadyAuthorizedSelection"):
            with self.subTest(method=method):
                task = json.loads(self.path.read_text(encoding="utf-8"))
                entry = next(test for test in task["tests"] if method in test["methods"])
                entry["methods"].remove(method)
                with self.assertRaisesRegex(m5_report.ReportError, "J51 required test methods are missing"):
                    m5_report._validate_task(task, self.path, self.repo)

    def test_j51_rejects_non_string_module_and_test_kind(self) -> None:
        for key in ("module", "kind"):
            with self.subTest(field=key):
                task = json.loads(self.path.read_text(encoding="utf-8"))
                task["tests"][0][key] = []
                with self.assertRaisesRegex(m5_report.ReportError, "invalid module, class, kind"):
                    m5_report._validate_task(task, self.path, self.repo)


if __name__ == "__main__":
    unittest.main()
