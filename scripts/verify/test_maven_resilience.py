#!/usr/bin/env python3
"""Exercise Maven Resolver and Wrapper resilience to transient and permanent HTTP failures."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("public_build_policy", ROOT / "scripts/verify/public-build-policy.py")
POLICY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POLICY)
PREP_SPEC = importlib.util.spec_from_file_location(
    "maven_preparation", ROOT / "scripts/dev/prepare-maven-dependencies.py")
PREPARATION = importlib.util.module_from_spec(PREP_SPEC)
PREP_SPEC.loader.exec_module(PREPARATION)
REPRO_SPEC = importlib.util.spec_from_file_location(
    "maven_reproducible_build", ROOT / "scripts/verify/maven-reproducible-build.py")
REPRO = importlib.util.module_from_spec(REPRO_SPEC)
REPRO_SPEC.loader.exec_module(REPRO)

PARENT = b"""<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion><groupId>resilience.fixture</groupId>
  <artifactId>parent</artifactId><version>1</version><packaging>pom</packaging>
</project>"""


class MavenCachePolicyTest(unittest.TestCase):
    def test_actual_workflows_keep_public_pins_and_cache_barriers(self):
        result = POLICY.static_policy()
        self.assertTrue(result["public_maven_only"])
        self.assertEqual(4, result["immutable_action_pins"])
        self.assertEqual(1, result["maven_preparation_jobs"])
        self.assertEqual(1, result["maven_preparation_calls"])
        self.assertEqual(15, result["maven_consumer_workflows"])
        self.assertEqual(29, result["maven_validation_jobs"])
        self.assertEqual(31, result["locked_jdk_archive_jobs"])
        self.assertTrue(result["stable_summary_gate"])
        self.assertTrue(result["j09_independent"])

    def test_j09_wrapper_cache_and_failure_diagnostics_are_locked(self):
        workflow = (ROOT / ".github/workflows/j09.yml").read_text()
        pins = POLICY.approved_action_pins(ROOT, json.loads((ROOT / "build/dependency-lock.json").read_text()))
        self.assertIn("actions/cache", pins)
        self.assertIn("actions/upload-artifact", pins)
        POLICY.workflow_actions(workflow, ROOT, pins)
        self.assertIn("path: ~/.m2/wrapper/dists", workflow)
        self.assertIn("hashFiles('.mvn/wrapper/maven-wrapper.properties')", workflow)
        self.assertIn("if: failure()", workflow)
        self.assertIn("maven-wrapper-startup-*.log", workflow)
        self.assertIn("retention-days: 7", workflow)
        self.assertNotIn("build/ci-action-lock.json", POLICY.MAVEN_CACHE_INPUTS)

    def test_consumer_rejects_incomplete_keys_and_failed_preparation_bypass(self):
        workflow = ROOT / ".github/workflows/m3.yml"
        content = workflow.read_text()
        damages = (
            content.replace("            build/dependency-lock.json\n", "", 1),
            content.replace("    if: ${{ !cancelled() }}\n", "", 1),
            content.replace(
                "      maven_preparation_result:\n"
                "        description: Result of the shared Maven preparation job.\n"
                "        required: true\n"
                "        type: string\n",
                "      other_result:\n"
                "        description: Result of the shared Maven preparation job.\n"
                "        required: true\n"
                "        type: string\n",
                1,
            ),
            content.replace(
                "PREPARATION_RESULT: ${{ inputs.maven_preparation_result }}",
                "PREPARATION_RESULT: ${{ needs.maven-dependencies.result }}",
                1,
            ),
            content.replace('        run: test "$PREPARATION_RESULT" = success\n', "", 1),
            content.replace("          cache-read-only: true\n", "", 1),
            content.replace(
                "  j28-history:\n",
                "  j28-history:\n    needs: maven-dependencies\n",
                1,
            ),
        )
        for index, damaged in enumerate(damages):
            self.assertNotEqual(content, damaged)
            with self.subTest(damage=index), self.assertRaises(ValueError):
                POLICY.maven_cache_policy(workflow, damaged)

    def test_m5_consumer_rejects_failed_preparation_bypass(self):
        workflow = ROOT / ".github/workflows/m5.yml"
        content = workflow.read_text()
        damages = (
            content.replace('        run: test "$PREPARATION_RESULT" = success\n', "", 1),
            content.replace("          cache-read-only: true\n", "", 1),
            content.replace("            build/dependency-lock.json\n", "", 1),
        )
        for index, damaged in enumerate(damages):
            self.assertNotEqual(content, damaged)
            with self.subTest(damage=index), self.assertRaises(ValueError):
                POLICY.maven_cache_policy(workflow, damaged)

    def test_multi_job_summaries_reject_missing_or_ignored_results(self):
        cases = (
            (".github/workflows/m3.yml", "      - j34\n",
             "          J31_RESULT: ${{ needs.j31.result }}\n"),
            (".github/workflows/m4.yml", "      - j45\n",
             "          J43_RESULT: ${{ needs.j43.result }}\n"),
            (".github/workflows/m5.yml", "      - j50\n",
             "          J50_RESULT: ${{ needs.j50.result }}\n"),
            (".github/workflows/m5.yml", "      - j51\n",
             "          J51_RESULT: ${{ needs.j51.result }}\n"),
        )
        for workflow_path, missing_need, missing_result in cases:
            workflow = ROOT / workflow_path
            content = workflow.read_text()
            damages = (
                content.replace(missing_need, "", 1),
                content.replace(missing_result, "", 1),
                content.replace(
                    "    if: ${{ always() && !cancelled() }}\n",
                    "    if: ${{ always() }}\n",
                    1,
                ),
                content.replace(
                    'if [[ "$result" != success ]]; then',
                    'if [[ "$result" == failure ]]; then',
                    1,
                ),
            )
            for index, damaged in enumerate(damages):
                self.assertNotEqual(content, damaged)
                with self.subTest(workflow=workflow_path, damage=index), self.assertRaises(ValueError):
                    POLICY.maven_cache_policy(workflow, damaged)

    def test_ci_orchestration_rejects_missing_checks_and_j09_dependency(self):
        path = ROOT / POLICY.MAVEN_ORCHESTRATION
        content = path.read_text()
        damages = (
            content.replace(
                "  pull_request:\n    branches:\n      - main\n",
                "",
                1,
            ),
            content.replace(
                "  merge_group:\n    types:\n      - checks_requested\n",
                "",
                1,
            ),
            content.replace(
                "contains(fromJSON(needs.plan.outputs.groups), 'j03')",
                "contains(fromJSON(needs.plan.outputs.groups), 'j04')",
                1,
            ),
            content.replace(
                "uses: ./.github/workflows/j03.yml",
                "uses: ./.github/workflows/j04.yml",
                1,
            ),
            content.replace(
                "  j09:\n    name: J09 · 公开构建、运行包与镜像验证\n    needs: plan\n",
                "  j09:\n    name: J09 · 公开构建、运行包与镜像验证\n    needs: [plan, maven-dependencies]\n",
                1,
            ),
            content.replace(
                "          RESULT_J09: ${{ needs.j09.result }}\n",
                "",
                1,
            ),
            content.replace(
                "run: python3 scripts/verify/ci_impact.py --check-results\n",
                "run: python3 scripts/verify/ci_impact.py\n",
                1,
            ),
        )
        for index, damaged in enumerate(damages):
            self.assertNotEqual(content, damaged)
            with self.subTest(damage=index), self.assertRaises(ValueError):
                POLICY.ci_orchestration_policy(ROOT, damaged)

    def test_ci_orchestrator_is_the_only_push_and_pull_request_entrypoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow_dir = root / ".github/workflows"
            workflow_dir.mkdir(parents=True)
            paths = {POLICY.MAVEN_PREPARATION, *POLICY.CI_WORKFLOW_CALLS.values()}
            for workflow in paths:
                destination = workflow_dir / Path(workflow).name
                shutil.copyfile(ROOT / workflow, destination)

            j03_path = workflow_dir / "j03.yml"
            j03 = j03_path.read_text()
            j03_path.write_text(j03.replace(
                "on:\n  workflow_call:\n",
                "on:\n  push:\n    branches:\n      - main\n  workflow_call:\n",
                1,
            ))
            orchestration = (ROOT / POLICY.MAVEN_ORCHESTRATION).read_text()
            with self.assertRaises(ValueError):
                POLICY.ci_orchestration_policy(root, orchestration)

    def test_locked_jdk_cache_rejects_unverified_or_unlocked_archives(self):
        workflow_path = ROOT / ".github/workflows/m3.yml"
        content = workflow_path.read_text()
        damages = (
            content.replace(
                'run: python3 scripts/dev/download-locked-jdk.py --verify "$RUNNER_TEMP/locked-temurin-jdk.tar.gz"\n',
                "",
                1,
            ),
            content.replace(
                "key: locked-jdk21-${{ runner.os }}-${{ hashFiles('build/dependency-lock.json') }}\n",
                "key: locked-jdk21-${{ runner.os }}\n",
                1,
            ),
            content.replace(
                'run: python3 scripts/dev/download-locked-jdk.py --output "$RUNNER_TEMP/locked-temurin-jdk.tar.gz"\n',
                'run: python3 scripts/dev/download-locked-jdk.py --output "$RUNNER_TEMP/other-jdk.tar.gz"\n',
                1,
            ),
        )
        for index, damaged in enumerate(damages):
            self.assertNotEqual(content, damaged)
            with self.subTest(damage=index), self.assertRaises(ValueError):
                POLICY.locked_jdk_archive_policy(workflow_path, damaged)

    def test_only_approved_local_workflow_or_immutable_remote_actions_are_allowed(self):
        lock = json.loads((ROOT / "build/dependency-lock.json").read_text())
        pins = POLICY.approved_action_pins(ROOT, lock)
        for workflow in (POLICY.MAVEN_PREPARATION, ".github/workflows/j03.yml",
                         POLICY.INDEPENDENT_CI_WORKFLOWS["j09"]):
            POLICY.workflow_actions("    uses: ./" + workflow, ROOT, pins)
        for action in ("actions/setup-java@main", "./../unexpected.yml", "./.github/workflows/other.yml"):
            with self.subTest(action=action), self.assertRaises(ValueError):
                POLICY.workflow_actions("    uses: " + action, ROOT, pins)


class MavenOfflineCacheRepairTest(unittest.TestCase):
    def test_missing_coordinates_preserve_classifier_and_deduplicate(self):
        diagnostic = ("net.bytebuddy:byte-buddy:jar:1.18.3 (absent), "
                      "com.google.protobuf:protoc:exe:linux-x86_64:4.35.0 (absent), "
                      "net.bytebuddy:byte-buddy:jar:1.18.3 (absent)")
        self.assertEqual([
            "com.google.protobuf:protoc:4.35.0:exe:linux-x86_64",
            "net.bytebuddy:byte-buddy:1.18.3:jar",
        ], PREPARATION.missing_transfer_coordinates(diagnostic))
        with self.assertRaisesRegex(ValueError, "reactor SNAPSHOTs"):
            PREPARATION.missing_transfer_coordinates("io.etcdhbase:core:jar:0.1.0-SNAPSHOT (absent)")

    def test_non_transfer_failure_is_not_retried_or_ignored(self):
        result = subprocess.CompletedProcess([], 1, "unrelated offline resolution failure")
        with patch.object(PREPARATION.subprocess, "run", return_value=result) as probe:
            with patch.object(PREPARATION, "run_maven") as fetch:
                with self.assertRaises(subprocess.CalledProcessError):
                    PREPARATION.ensure_offline_cache()
                self.assertEqual(1, probe.call_count)
                fetch.assert_not_called()

    def test_persistently_missing_transfer_repair_has_a_fixed_bound(self):
        result = subprocess.CompletedProcess([], 1, "net.bytebuddy:byte-buddy:jar:1.18.3 (absent)")
        with patch.object(PREPARATION.subprocess, "run", return_value=result) as probe:
            with patch.object(PREPARATION, "run_maven") as fetch:
                with self.assertRaises(subprocess.CalledProcessError):
                    PREPARATION.ensure_offline_cache()
                self.assertEqual(4, probe.call_count)
                self.assertEqual(3, fetch.call_count)
                fetch.assert_called_with("-N", PREPARATION.GOAL + "get",
                                         "-Dartifact=net.bytebuddy:byte-buddy:1.18.3:jar")


class MavenWrapperStartupRetryTest(unittest.TestCase):
    def run_wrapper_startup(self, status):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        wrapper = root / ".mvn/wrapper"
        wrapper.mkdir(parents=True)
        shutil.copyfile(ROOT / "mvnw", root / "mvnw")
        (root / "mvnw").chmod(0o755)
        shutil.copyfile(ROOT / ".mvn/wrapper/maven-wrapper.jar", wrapper / "maven-wrapper.jar")

        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(self.path)
                payload = b"fixture rate limit" if status == 429 else b"fixture missing"
                self.send_response(status)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_arguments):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def close_server():
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.addCleanup(close_server)
        (wrapper / "maven-wrapper.properties").write_text(
            "wrapperVersion=3.3.4\n"
            "distributionType=bin\n"
            f"distributionUrl=http://127.0.0.1:{server.server_port}/apache-maven-3.9.16-bin.zip\n"
            "distributionSha256Sum=5af3b743dd8b876b5c45da33b676251e5f1687712644abb4ee519ca56e1d89ce\n"
        )
        runner = root / "scripts/dev/mvn21.sh"
        runner.parent.mkdir(parents=True)
        runner.write_text(
            '#!/usr/bin/env bash\n'
            'set -euo pipefail\n'
            'ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"\n'
            'exec "$ROOT/mvnw" "$@"\n'
        )
        runner.chmod(0o755)

        output = root / "output"
        output.mkdir()
        delays = []
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.object(REPRO, "ROOT", root), \
             patch.object(REPRO, "time", SimpleNamespace(sleep=delays.append)), \
             redirect_stdout(stdout), redirect_stderr(stderr):
            with self.assertRaisesRegex(ValueError, "pinned Maven Wrapper startup failed"):
                REPRO.prepare_maven_wrapper(output)
        return requests, delays, stdout.getvalue(), stderr.getvalue(), output

    def test_real_wrapper_retries_local_http_429_only_to_its_bound(self):
        requests, delays, stdout, stderr, output = self.run_wrapper_startup(429)
        self.assertEqual(4, len(requests))
        self.assertEqual([2, 4, 8], delays)
        self.assertEqual(3, stdout.count("retrying Wrapper startup"))
        self.assertIn("HTTP response code: 429", stderr)
        for attempt in range(1, 5):
            self.assertIn("HTTP response code: 429",
                          (output / f"maven-wrapper-startup-{attempt}.log").read_text(errors="replace"))

    def test_real_wrapper_does_not_retry_local_http_404(self):
        requests, delays, _stdout, stderr, _output = self.run_wrapper_startup(404)
        self.assertEqual(1, len(requests))
        self.assertEqual([], delays)
        self.assertIn("FileNotFoundException:", stderr)


class MavenHttpResilienceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.environment = dict(os.environ, MAVEN_ARGS="", MAVEN_OPTS="")
        java_home = cls.environment.get("ETCD_HBASE_JDK21_HOME") or cls.environment.get("JAVA_HOME")
        if not java_home:
            raise RuntimeError("Set ETCD_HBASE_JDK21_HOME or JAVA_HOME to the locked JDK")
        cls.environment["JAVA_HOME"] = java_home
        cls.environment.pop("MAVEN_PROJECTBASEDIR", None)
        cls.environment.pop("MAVEN_BASEDIR", None)
        result = subprocess.run([str(ROOT / "mvnw"), "--version"], cwd=ROOT,
                                env=cls.environment, capture_output=True, text=True, timeout=180, check=True)
        match = re.search(r"^Maven home: (.+)$", result.stdout, re.MULTILINE)
        if not match:
            raise RuntimeError("Pinned Maven distribution did not report its home")
        cls.maven = str(Path(match[1].strip()) / "bin/mvn")

    def fixture(self, status, failures):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / ".mvn").mkdir()
        shutil.copyfile(ROOT / ".mvn/maven.config", root / ".mvn/maven.config")
        (root / "pom.xml").write_text("""<project xmlns="http://maven.apache.org/POM/4.0.0">
          <modelVersion>4.0.0</modelVersion><parent><groupId>resilience.fixture</groupId>
          <artifactId>parent</artifactId><version>1</version><relativePath/></parent>
          <artifactId>child</artifactId><packaging>pom</packaging></project>""")
        observations = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                payload, code = self.response()
                self.send_response(code)
                if code == 429:
                    self.send_header("Retry-After", "1")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def response(self):
                if self.path.endswith("/parent-1.pom"):
                    observations.append(time.monotonic())
                    return (b"fixture failure", status) if len(observations) <= failures else (PARENT, 200)
                if self.path.endswith("/parent-1.pom.sha1"):
                    return hashlib.sha1(PARENT).hexdigest().encode(), 200
                return b"not found", 404

            def log_message(self, *_arguments):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def close_server():
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.addCleanup(close_server)
        (root / "settings.xml").write_text(f"""<settings><mirrors><mirror><id>fixture</id>
          <mirrorOf>*</mirrorOf><url>http://127.0.0.1:{server.server_port}</url>
          </mirror></mirrors></settings>""")
        return root, observations

    def validate(self, status, failures):
        root, observations = self.fixture(status, failures)
        started = time.monotonic()
        result = subprocess.run([self.maven, "-B", "-ntp", "-s", "settings.xml", "-gs", "settings.xml",
                                 "-Dmaven.repo.local=" + str(root / "repository"), "validate"],
                                cwd=root, env=self.environment, capture_output=True, text=True, timeout=25)
        return result, observations, time.monotonic() - started

    def test_four_rate_limited_requests_recover_and_obey_retry_after(self):
        result, observations, elapsed = self.validate(429, 4)
        self.assertEqual(0, result.returncode, result.stdout[-4000:] + result.stderr[-1000:])
        self.assertEqual(5, len(observations))
        self.assertGreaterEqual(observations[-1] - observations[0], 3.5)
        self.assertLess(elapsed, 20, "Retry-After must override the longer configured fallback interval")

    def test_persistent_rate_limit_stops_at_the_bound(self):
        result, observations, _elapsed = self.validate(429, 100)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(5, len(observations))

    def test_missing_artifact_is_not_retried(self):
        result, observations, _elapsed = self.validate(404, 100)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(1, len(observations))


if __name__ == "__main__":
    unittest.main(verbosity=2)
