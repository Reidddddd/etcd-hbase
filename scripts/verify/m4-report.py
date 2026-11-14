#!/usr/bin/env python3
"""Validate registered M4 tests, MiniCluster traces, and source-bound run evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath
import re
import subprocess
import sys
import xml.etree.ElementTree as ET


REPO = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO / "testkit/cases/m4-cases.json"
PROTOCOL = "etcd-v3.5.23"
KNOWN_TASKS = {"J40", "J41", "J42", "J43", "J44", "J45", "J46", "J47", "J29.4"}
GO_MODULE = "github.com/Reidddddd/etcd-hbase/testkit/go/"
JAVA_MODULES = {"protocol", "spi", "core", "hbase", "grpc", "admin", "server"}
JAVA_KINDS = {"surefire", "failsafe"}
RUN_SCHEMA = "etcd-hbase-m4-run/v1"
TRACE_SCHEMA = "etcd-hbase-trace/v1"
COMPARE_SUCCESS = "M4 Lease traces match the registered native comparison policy"
J41_COMPARE_SUCCESS = "M4 Lease binding traces match the registered native comparison policy"
REQUIRED_COMMANDS = {"go-module-lock", "go-tests", "maven-verify", "collect-maven-reports"}

J40_TRACE_CASES = (
    ("native-baseline", "etcdserverpb.Lease/LeaseLeases"),
    ("auto-grant", "etcdserverpb.Lease/LeaseGrant"),
    ("auto-ttl", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("explicit-grant", "etcdserverpb.Lease/LeaseGrant"),
    ("explicit-ttl", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("duplicate-id", "etcdserverpb.Lease/LeaseGrant"),
    ("missing-ttl", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("negative-id", "etcdserverpb.Lease/LeaseGrant"),
    ("leases-list", "etcdserverpb.Lease/LeaseLeases"),
    ("oversized-ttl", "etcdserverpb.Lease/LeaseGrant"),
    ("zero-id-ttl", "etcdserverpb.Lease/LeaseTimeToLive"),
)
J41_TRACE_CASES = (
    ("binding-baseline", "etcdserverpb.Lease/LeaseLeases"),
    ("binding-grant-a", "etcdserverpb.Lease/LeaseGrant"),
    ("binding-grant-b", "etcdserverpb.Lease/LeaseGrant"),
    ("binding-register-then", "etcdserverpb.KV/Txn"),
    ("binding-ttl-a-initial", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-ttl-b-initial", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-ignore-flags", "etcdserverpb.KV/Put"),
    ("binding-ttl-a-preserved", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-rebind-a-to-b", "etcdserverpb.KV/Put"),
    ("binding-ttl-a-after-rebind", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-ttl-b-after-rebind", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-register-else", "etcdserverpb.KV/Txn"),
    ("binding-ttl-b-after-else", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-register-nested", "etcdserverpb.KV/Txn"),
    ("binding-ttl-a-after-nested", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-ttl-b-after-nested", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-unbind", "etcdserverpb.KV/Put"),
    ("binding-ttl-a-after-unbind", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-delete", "etcdserverpb.KV/DeleteRange"),
    ("binding-ttl-b-after-delete", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-recreate-unleased", "etcdserverpb.KV/Put"),
    ("binding-ttl-b-after-recreate", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-delete-range", "etcdserverpb.KV/DeleteRange"),
    ("binding-ttl-a-after-delete-range", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-ttl-b-after-delete-range", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-role-prefix-after-delete-range", "etcdserverpb.KV/Range"),
    ("binding-version-after-delete-range", "etcdserverpb.KV/Range"),
    ("binding-survivor-put", "etcdserverpb.KV/Put"),
    ("binding-ttl-b-after-survivor-put", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-invalid-lease-atomicity", "etcdserverpb.KV/Txn"),
    ("binding-atomicity-range-first", "etcdserverpb.KV/Range"),
    ("binding-atomicity-range-second", "etcdserverpb.KV/Range"),
    ("binding-ttl-a-after-failed-txn", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-invalid-lease-put", "etcdserverpb.KV/Put"),
    ("binding-atomicity-range-invalid-put", "etcdserverpb.KV/Range"),
    ("binding-invalid-lease-unselected-branch", "etcdserverpb.KV/Txn"),
    ("binding-unselected-then-absent", "etcdserverpb.KV/Range"),
    ("binding-unselected-else-visible", "etcdserverpb.KV/Range"),
    ("binding-ttl-b-after-unselected-branch", "etcdserverpb.Lease/LeaseTimeToLive"),
)
J40_GO_TESTS = {
    "cmd/m4driver": {
        "TestRunNativeLeaseWritesFreshTraceAndVerifiableMetadata",
        "TestParseRunOptionsRejectsMissingLayoutAndUnscopedPrefix",
        "TestCompareRejectsCorruptOrEmptyTrace",
    },
    "internal/scenario": {
        "TestM4LeaseScenarioMatchesItsPinnedNativeOracle",
        "TestLoadM4LeaseSuiteRejectsMissingOrChangedCoverage",
        "TestValidateM4LeaseRejectsUnscopedPrefixBeforeRPC",
    },
    "internal/compare": {
        "TestCompareM4LeaseAllowsConfiguredMinimumAndAtMostOneSecondRemainingTTLSkew",
        "TestCompareM4LeaseRejectsRemainingTTLSkewOverOneSecond",
        "TestCompareM4LeaseKeepsStatusAndErrorContractsStrict",
    },
    "internal/trace": {
        "TestSnapshotProtoPreservesBinaryAndInt64Values",
        "TestReadJSONLRejectsInvalidOrderingAndSecrets",
        "TestStreamTerminalCannotContradictItsOutcome",
    },
}
J40_JAVA_CLASSES = {
    "io.etcdhbase.core.lease.LeaseStateTest",
    "io.etcdhbase.core.lease.LeaseGrantPlannerTest",
    "io.etcdhbase.core.lease.LeaseIdAllocatorTest",
    "io.etcdhbase.core.commit.AuxiliaryFormatUpgradeTest",
    "io.etcdhbase.core.commit.AuxiliaryLeasePlanVerifierTest",
    "io.etcdhbase.core.commit.SystemRecordAuxiliaryMaterializationStoreTest",
    "io.etcdhbase.core.commit.LeaseReadViewTest",
    "io.etcdhbase.core.commit.CommitFinalizationServiceTest",
    "io.etcdhbase.core.commit.ManifestPreparationTest",
    "io.etcdhbase.grpc.service.LeaseServiceTest",
    "io.etcdhbase.grpc.protocol.LeaseGrantResponseMapperTest",
    "io.etcdhbase.grpc.protocol.LeaseTimeToLiveResponseMapperTest",
    "io.etcdhbase.grpc.RequestLifecycleOutcomeTest",
    "io.etcdhbase.server.commit.LeaseGrantPreparationTest",
    "io.etcdhbase.server.config.M3RuntimeConfigurationTest",
    "io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanIT",
    "io.etcdhbase.server.M4LeaseRpcIT",
}
J40_CRITICAL_JAVA_TESTS = {
    "io.etcdhbase.core.lease.LeaseStateTest": {"deadlineUsesUpperBoundAndExpiryUsesLowerBound"},
    "io.etcdhbase.core.commit.LeaseReadViewTest": {
        "bindingReadMergesStableHistoryWithSelectedChangesAtOneBarrier"
    },
    "io.etcdhbase.grpc.protocol.LeaseTimeToLiveResponseMapperTest": {
        "includesEveryRequestedKeyAsOriginalBytesWithThePinnedHeader"
    },
    "io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanIT": {
        "scansOneBucketInBoundedExclusiveResumePages"
    },
    "io.etcdhbase.server.M4LeaseRpcIT": {
        "realGoLeaseCallsMatchNativeAndSurviveRestartAcrossLayouts"
    },
}
J41_GO_TESTS = {
    "cmd/m4driver": {
        "TestRunNativeLeaseBindingWritesFreshTraceAndVerifiableMetadata",
        "TestCompareBindingsRejectsCorruptOrEmptyTrace",
    },
    "internal/scenario": {
        "TestM4LeaseBindingScenarioMatchesItsPinnedNativeOracle",
        "TestLoadM4LeaseBindingSuiteRejectsMissingOrChangedCoverage",
    },
    "internal/compare": {
        "TestCompareM4LeaseBindingRecordsNormalizesTransportAndScenarioRelativeTtl",
        "TestCompareM4LeaseBindingRecordsRejectsRevisionAndPartialResult",
        "TestCompareM4LeaseBindingRecordsRejectsInvalidTtlAndUnexpectedRenewal",
        "TestCompareM4LeaseBindingRequiresNotFoundForInvalidLeaseOperations",
    },
    "internal/trace": {
        "TestSnapshotProtoPreservesBinaryAndInt64Values",
        "TestReadJSONLRejectsInvalidOrderingAndSecrets",
        "TestStreamTerminalCannotContradictItsOutcome",
    },
}
J41_JAVA_CLASSES = {
    "io.etcdhbase.core.commit.LeaseBindingFormatUpgradeTest",
    "io.etcdhbase.core.commit.CommitFinalizationServiceTest",
    "io.etcdhbase.core.kv.LeaseBindingChangeSetTest",
    "io.etcdhbase.core.lease.LeaseBindingIdentityTest",
    "io.etcdhbase.core.lease.LeaseCostAdmissionTest",
    "io.etcdhbase.core.commit.LeaseReadViewTest",
    "io.etcdhbase.core.commit.AuxiliaryLeasePlanVerifierTest",
    "io.etcdhbase.hbase.codec.LeaseStateMessagesTest",
    "io.etcdhbase.hbase.codec.SystemRecordKeyCodecTest",
    "io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanIT",
    "io.etcdhbase.server.M4LeaseRpcIT",
    "io.etcdhbase.server.M4LeaseBindingRpcIT",
    "io.etcdhbase.server.commit.LeaseBindingMutationPlannerTest",
    "io.etcdhbase.server.commit.M3OwnerLeaseTest",
    "io.etcdhbase.server.config.M3RuntimeConfigurationTest",
}
J41_CRITICAL_JAVA_TESTS = {
    "io.etcdhbase.core.kv.LeaseBindingChangeSetTest": {
        "deleteAndUnleasedRecreationRetainTheOriginalUnbind"
    },
    "io.etcdhbase.core.lease.LeaseBindingIdentityTest": {
        "isolatesBindingRowsAcrossIncarnationsOfTheSameLeaseId"
    },
    "io.etcdhbase.core.commit.CommitFinalizationServiceTest": {
        "selectedLeaseBindingRecordIsReplayableAfterRestartAtItsMaterializationBoundary"
    },
    "io.etcdhbase.core.lease.LeaseCostAdmissionTest": {
        "acceptsAccountingExactlyAtEachConfiguredBoundary",
        "rejectsCountKeyBytesAndRevokeCostOverTheConfiguredBoundary",
    },
    "io.etcdhbase.server.M4LeaseBindingRpcIT": {
        "realGoLeaseBindingCallsMatchNativeAndSurviveRestartAcrossLayouts"
    },
    "io.etcdhbase.server.commit.LeaseBindingMutationPlannerTest": {
        "activeStateUpdateIncludesItsCurrentExpiryCandidate",
        "revokedStateUpdatesDoNotWriteNewExpiryCandidates",
    },
    "io.etcdhbase.server.commit.M3OwnerLeaseTest": {
        "startupFormatUpgradeRotatesTheFenceWithoutLosingOwnerRenewal"
    },
    "io.etcdhbase.server.config.M3RuntimeConfigurationTest": {
        "m4ActivationRequiresExplicitLeaseRevocationLimits"
    },
}


class ReportError(ValueError):
    """Raised when a run cannot prove its registered M4 requirements."""


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ReportError("duplicate JSON object key")
        result[key] = value
    return result


def read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ReportError(f"cannot read {path.name}") from error


def validate_registry_document(document: object) -> dict[str, object]:
    if not isinstance(document, dict) or document.get("schema") != "etcd-hbase-m4-cases/v1":
        raise ReportError("M4 registry schema is invalid")
    if document.get("protocol") != PROTOCOL:
        raise ReportError("M4 registry protocol is not pinned")
    tasks = document.get("tasks")
    pending = document.get("pending_tasks")
    if not isinstance(tasks, list) or not isinstance(pending, list):
        raise ReportError("M4 registry tasks and pending_tasks must be lists")

    task_map: dict[str, dict[str, object]] = {}
    for task in tasks:
        if not isinstance(task, dict):
            raise ReportError("M4 registry task is malformed")
        task_id = task.get("id")
        if not isinstance(task_id, str) or task_id not in KNOWN_TASKS or task_id in task_map:
            raise ReportError("M4 registry task ID is unknown or duplicated")
        _validate_task_shape(task)
        task_map[task_id] = task
    if any(not isinstance(item, str) for item in pending):
        raise ReportError("M4 pending task list is malformed")
    pending_ids = set(pending)
    if len(pending_ids) != len(pending):
        raise ReportError("M4 pending task list is malformed or duplicated")
    if task_map.keys() & pending_ids or set(task_map) | pending_ids != KNOWN_TASKS:
        raise ReportError("M4 task registration and pending list do not cover the plan")

    j40 = task_map.get("J40")
    if j40 is None:
        raise ReportError("J40 evidence registration is required")
    _validate_j40_registration(j40)
    j41 = task_map.get("J41")
    if j41 is not None:
        _validate_j41_registration(j41)
    return document


def _validate_task_shape(task: dict[str, object]) -> None:
    if not isinstance(task.get("api_ids"), list) or not task["api_ids"]:
        raise ReportError("registered M4 task must identify API coverage")
    if not isinstance(task.get("scenario"), str) or not task["scenario"]:
        raise ReportError("registered M4 task must identify its scenario")
    for field in ("trace_cases", "go_tests", "maven_tests"):
        if not isinstance(task.get(field), list) or not task[field]:
            raise ReportError(f"registered M4 task must have {field}")
    evidence = task.get("evidence")
    if not isinstance(evidence, dict) or not isinstance(evidence.get("native"), dict):
        raise ReportError("registered M4 task must describe native evidence")
    if not isinstance(evidence.get("targets"), list) or not evidence["targets"]:
        raise ReportError("registered M4 task must describe target evidence")
    if not isinstance(evidence.get("compare_logs"), list) or not evidence["compare_logs"]:
        raise ReportError("registered M4 task must require comparison evidence")


def _validate_j40_registration(task: dict[str, object]) -> None:
    if (task.get("api_ids") != ["L01", "L04", "L05"]
            or task.get("scenario") != "m4-lease-v1"
            or task.get("manifest") != "m4-lease.yaml"):
        raise ReportError("J40 API or scenario registration changed")
    traces = task["trace_cases"]
    if any(not isinstance(item, dict) for item in traces):
        raise ReportError("J40 Lease trace case is malformed")
    if [(item.get("id"), item.get("method")) for item in traces] != list(J40_TRACE_CASES):
        raise ReportError("J40 Lease trace coverage is missing, reordered, or changed")
    if any(not isinstance(item.get("api_id"), str) or not item["api_id"] for item in traces):
        raise ReportError("J40 trace case has no API mapping")

    go_tests: dict[str, set[str]] = {}
    for package in task["go_tests"]:
        if not isinstance(package, dict) or not isinstance(package.get("package"), str):
            raise ReportError("M4 Go test registration is malformed")
        names = package.get("tests")
        if not isinstance(names, list) or not names or any(not isinstance(name, str) for name in names):
            raise ReportError("M4 Go package has no named required tests")
        if package["package"] in go_tests:
            raise ReportError("M4 Go package is registered more than once")
        go_tests[package["package"]] = set(names)
    if any(not expected.issubset(go_tests.get(package, set()))
           for package, expected in J40_GO_TESTS.items()):
        raise ReportError("J40 Go regression coverage is incomplete")

    java_classes: set[str] = set()
    java_methods: dict[str, set[str]] = {}
    for suite in task["maven_tests"]:
        if not isinstance(suite, dict):
            raise ReportError("M4 Maven test registration is malformed")
        module, kind, class_name, tests = (
            suite.get("module"), suite.get("kind"), suite.get("class"), suite.get("tests")
        )
        if module not in JAVA_MODULES or kind not in JAVA_KINDS or not isinstance(class_name, str):
            raise ReportError("M4 Maven test registration has an invalid module, kind, or class")
        if not isinstance(tests, list) or not tests or any(not isinstance(name, str) for name in tests):
            raise ReportError("M4 Maven suite has no named required tests")
        java_classes.add(class_name)
        java_methods.setdefault(class_name, set()).update(tests)
    if not J40_JAVA_CLASSES.issubset(java_classes):
        raise ReportError("J40 Maven unit or MiniCluster suites are not all registered")
    if any(not methods.issubset(java_methods.get(class_name, set()))
           for class_name, methods in J40_CRITICAL_JAVA_TESTS.items()):
        raise ReportError("J40 clock, binding, TTL projection, bounded-scan, or real RPC test is not registered")

    evidence = task["evidence"]
    native = evidence["native"]
    targets = evidence["targets"]
    if (native.get("scope"), native.get("layout"), native.get("embedded_native")) != (
        "native-etcd", "", True
    ):
        raise ReportError("J40 native evidence is not an isolated embedded etcd run")
    target_layouts = {
        (target.get("scope"), target.get("layout"), target.get("embedded_native"))
        for target in targets if isinstance(target, dict)
    }
    if target_layouts != {
        ("target-hbase", "HISTORY", False), ("target-hbase", "NATIVE", False)
    }:
        raise ReportError("J40 must compare both real HBase layouts")
    if evidence.get("compare_logs") != ["compare-history.log", "compare-native.log"]:
        raise ReportError("J40 comparison log registration changed")


def _validate_j41_registration(task: dict[str, object]) -> None:
    if (task.get("api_ids") != ["L01", "L04", "L05", "P01", "T01", "D01", "R01"]
            or task.get("scenario") != "m4-lease-bindings-v1"
            or task.get("manifest") != "m4-lease-bindings.yaml"
            or task.get("artifact_subdir") != "j41"):
        raise ReportError("J41 API or scenario registration changed")
    traces = task["trace_cases"]
    if any(not isinstance(item, dict) for item in traces):
        raise ReportError("J41 binding trace case is malformed")
    if [(item.get("id"), item.get("method")) for item in traces] != list(J41_TRACE_CASES):
        raise ReportError("J41 lease-binding trace coverage is missing, reordered, or changed")
    if any(not isinstance(item.get("api_id"), str) or not item["api_id"] for item in traces):
        raise ReportError("J41 trace case has no API mapping")

    go_tests: dict[str, set[str]] = {}
    for package in task["go_tests"]:
        if not isinstance(package, dict) or not isinstance(package.get("package"), str):
            raise ReportError("J41 Go test registration is malformed")
        names = package.get("tests")
        if not isinstance(names, list) or not names or any(not isinstance(name, str) for name in names):
            raise ReportError("J41 Go package has no named required tests")
        if package["package"] in go_tests:
            raise ReportError("J41 Go package is registered more than once")
        go_tests[package["package"]] = set(names)
    if any(not expected.issubset(go_tests.get(package, set()))
           for package, expected in J41_GO_TESTS.items()):
        raise ReportError("J41 Go binding regression coverage is incomplete")

    java_classes: set[str] = set()
    java_methods: dict[str, set[str]] = {}
    for suite in task["maven_tests"]:
        if not isinstance(suite, dict):
            raise ReportError("J41 Maven test registration is malformed")
        module, kind, class_name, tests = (
            suite.get("module"), suite.get("kind"), suite.get("class"), suite.get("tests")
        )
        if module not in JAVA_MODULES or kind not in JAVA_KINDS or not isinstance(class_name, str):
            raise ReportError("J41 Maven test registration has an invalid module, kind, or class")
        if not isinstance(tests, list) or not tests or any(not isinstance(name, str) for name in tests):
            raise ReportError("J41 Maven suite has no named required tests")
        java_classes.add(class_name)
        java_methods.setdefault(class_name, set()).update(tests)
    if not J41_JAVA_CLASSES.issubset(java_classes):
        raise ReportError("J41 Maven unit or MiniCluster suites are not all registered")
    if any(not methods.issubset(java_methods.get(class_name, set()))
           for class_name, methods in J41_CRITICAL_JAVA_TESTS.items()):
        raise ReportError("J41 binding budget, restart, or atomic registration regression is not registered")

    evidence = task["evidence"]
    native = evidence["native"]
    target_layouts = {
        (target.get("scope"), target.get("layout"), target.get("embedded_native"))
        for target in evidence["targets"] if isinstance(target, dict)
    }
    if (native.get("scope"), native.get("layout"), native.get("embedded_native")) != (
        "native-etcd", "", True
    ) or target_layouts != {
        ("target-hbase", "HISTORY", False), ("target-hbase", "NATIVE", False)
    }:
        raise ReportError("J41 must compare native etcd with both HBase layouts")
    if evidence.get("compare_logs") != ["compare-history.log", "compare-native.log"]:
        raise ReportError("J41 comparison log registration changed")


def load_registry(path: Path = REGISTRY_PATH) -> dict[str, object]:
    return validate_registry_document(read_json(path))


def validate_all_tasks_registered(registry: dict[str, object]) -> None:
    pending = registry.get("pending_tasks")
    if pending:
        raise ReportError("--all is unavailable while M4 tasks are not registered")
    if {task["id"] for task in registry["tasks"]} != KNOWN_TASKS:
        raise ReportError("--all requires runnable registrations for every M4 task")


def artifact_location(repository: Path, requested: str, *, require_existing: bool = True) -> Path:
    root = repository.resolve()
    raw = Path(requested)
    candidate = Path(os.path.abspath(raw if raw.is_absolute() else root / raw))
    allowed = root / ".artifacts" / "m4"
    try:
        relative = candidate.relative_to(allowed)
    except ValueError as error:
        raise ReportError("M4 artifact directory must be below .artifacts/m4") from error
    if not relative.parts or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", candidate.name):
        raise ReportError("M4 artifact directory must have a fresh named run ID")
    current = root
    for component in (".artifacts", "m4", *relative.parts):
        current = current / component
        if current.is_symlink():
            raise ReportError("M4 artifact path must not traverse a symbolic link")
    if require_existing and not candidate.is_dir():
        raise ReportError("M4 artifact directory does not exist")
    return candidate


def validate_repository_binding(repository: Path, run: dict[str, object]) -> None:
    def git(*arguments: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(repository), *arguments], text=True, stderr=subprocess.DEVNULL
        ).strip()

    try:
        head = git("rev-parse", "HEAD")
        dirty = bool(git("status", "--porcelain", "--untracked-files=normal"))
    except (OSError, subprocess.CalledProcessError) as error:
        raise ReportError("cannot verify the source checkout for this M4 run") from error
    if head != run["source_commit"] or head != run["checkout"]:
        raise ReportError("M4 run source SHA does not match the current checkout")
    if run["worktree_clean"] and dirty:
        raise ReportError("clean M4 evidence no longer has a clean source tree")


def _validate_commands(artifact: Path, run: dict[str, object]) -> None:
    commands = run.get("commands")
    if not isinstance(commands, list) or not commands:
        raise ReportError("M4 run has no command evidence")
    names: set[str] = set()
    for command in commands:
        if not isinstance(command, dict):
            raise ReportError("M4 command record is malformed")
        name = command.get("name")
        argv = command.get("argv")
        if not isinstance(name, str) or name in names or not isinstance(argv, list) or not argv:
            raise ReportError("M4 command name or argv is missing or duplicated")
        names.add(name)
        if command.get("exit_code") != 0:
            raise ReportError(f"M4 command {name} did not exit successfully")
        for field in ("started_at", "finished_at"):
            if not isinstance(command.get(field), str) or not command[field]:
                raise ReportError(f"M4 command {name} has no {field}")
        for field in ("stdout_log", "stderr_log"):
            log_name = command.get(field)
            if log_name is None:
                continue
            if not isinstance(log_name, str):
                raise ReportError(f"M4 command {name} has an invalid {field}")
            log_path = Path(os.path.abspath(artifact / log_name))
            try:
                log_path.relative_to(artifact)
            except ValueError as error:
                raise ReportError("M4 command log path escapes its artifact directory") from error
            if not log_path.is_file() or log_path.is_symlink():
                raise ReportError(f"M4 command log for {name} is missing or unsafe")
    if not REQUIRED_COMMANDS.issubset(names):
        raise ReportError("M4 run is missing a required Go, Maven, or evidence collection command")


def _validate_go_tests(artifact: Path, task: dict[str, object]) -> int:
    path = artifact / "go-test.jsonl"
    try:
        if path.stat().st_size > 128 << 20:
            raise ReportError("M4 Go test log exceeds its size limit")
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise ReportError("M4 Go test JSONL is missing or unreadable") from error
    required = {item["package"]: set(item["tests"]) for item in task["go_tests"]}
    observed_packages: set[str] = set()
    started_packages: set[str] = set()
    passed: dict[str, set[str]] = {}
    event_count = 0
    for line_number, line in enumerate(lines, start=1):
        if not line or len(line.encode("utf-8")) > 2 << 20:
            raise ReportError(f"M4 Go test JSONL line {line_number} is empty or oversized")
        try:
            event = json.loads(line, object_pairs_hook=_unique_object)
        except (json.JSONDecodeError, ReportError) as error:
            raise ReportError(f"M4 Go test JSONL line {line_number} is invalid") from error
        if not isinstance(event, dict):
            raise ReportError("M4 Go test JSONL contains a non-object event")
        action = event.get("Action")
        package = event.get("Package")
        test_name = event.get("Test")
        if action in {"fail", "skip"}:
            raise ReportError("M4 Go tests contain a failure or skip")
        if action not in {"start", "run", "output", "pass"}:
            raise ReportError("M4 Go test JSONL contains an unknown action")
        if not isinstance(package, str) or not package.startswith(GO_MODULE):
            raise ReportError("M4 Go test JSONL package is outside the pinned module")
        package = package.removeprefix(GO_MODULE)
        if package not in required:
            raise ReportError("M4 Go test JSONL contains an unregistered package")
        event_count += 1
        if action == "start":
            if test_name is not None or package in started_packages:
                raise ReportError("M4 Go package start event is malformed or duplicated")
            started_packages.add(package)
        elif action == "run" and not isinstance(test_name, str):
            raise ReportError("M4 Go test run event has no test name")
        elif action == "output" and test_name is not None and not isinstance(test_name, str):
            raise ReportError("M4 Go test output event has a malformed test name")
        elif action == "pass":
            if test_name is None:
                if package in observed_packages:
                    raise ReportError("M4 Go package pass appears more than once")
                observed_packages.add(package)
            elif isinstance(test_name, str):
                passed.setdefault(package, set()).add(test_name)
            else:
                raise ReportError("M4 Go test event has a malformed test name")
    if (event_count == 0 or started_packages != set(required)
            or observed_packages != set(required)):
        raise ReportError("M4 Go package completion evidence is missing")
    for package, tests in required.items():
        if not tests.issubset(passed.get(package, set())):
            raise ReportError(f"M4 required Go tests did not pass in {package}")
    return sum(len(tests) for tests in passed.values())


def _testsuite_elements(path: Path) -> list[ET.Element]:
    try:
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError) as error:
        raise ReportError(f"Maven XML report {path.name} is invalid") from error
    if root.tag == "testsuite":
        return [root]
    if root.tag == "testsuites":
        return list(root.iter("testsuite"))
    raise ReportError(f"Maven XML report {path.name} has an unknown root")


def _validate_java_tests(artifact: Path, task: dict[str, object]) -> int:
    suites: dict[str, tuple[str, str, set[str]]] = {}
    report_count = 0
    for module in sorted(JAVA_MODULES):
        for kind in sorted(JAVA_KINDS):
            directory = artifact / module / "target" / f"{kind}-reports"
            for path in sorted(directory.glob("TEST-*.xml")) if directory.is_dir() else []:
                report_count += 1
                for suite in _testsuite_elements(path):
                    name = suite.get("name")
                    if not name or name in suites:
                        raise ReportError("Maven report has an empty or duplicate suite name")
                    try:
                        counts = {
                            field: int(suite.get(field, "-1"))
                            for field in ("tests", "failures", "errors", "skipped")
                        }
                    except ValueError as error:
                        raise ReportError("Maven report has a non-numeric counter") from error
                    cases = suite.findall("testcase")
                    if (counts["tests"] < 1 or counts["tests"] != len(cases)
                            or counts["failures"] or counts["errors"] or counts["skipped"]):
                        raise ReportError("Maven report is empty, failed, inconsistent, or skipped")
                    if any(case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped")):
                        raise ReportError("Maven testcase contains failure, error, or skip evidence")
                    suites[name] = (module, kind, {case.get("name", "") for case in cases})
    if report_count == 0:
        raise ReportError("Maven XML evidence is missing")

    required_by_class: dict[str, tuple[str, str, set[str]]] = {}
    for item in task["maven_tests"]:
        class_name = item["class"]
        required_by_class.setdefault(class_name, (item["module"], item["kind"], set()))
        required_by_class[class_name][2].update(item["tests"])
    for class_name, (module, kind, required_names) in required_by_class.items():
        actual = suites.get(class_name)
        if actual is None or actual[:2] != (module, kind):
            raise ReportError(f"required Maven suite {class_name} is missing")
        if not required_names.issubset(actual[2]):
            raise ReportError(f"required Maven test cases are missing in {class_name}")
    return sum(len(item[2]) for item in suites.values())


def _task_artifact_root(artifact: Path, task: dict[str, object]) -> Path:
    subdirectory = task.get("artifact_subdir", "")
    if not isinstance(subdirectory, str):
        raise ReportError("M4 evidence subdirectory is malformed")
    if not subdirectory:
        return artifact
    relative = PurePosixPath(subdirectory)
    if (relative.is_absolute() or "\\" in subdirectory
            or any(part in {"", ".", ".."} for part in relative.parts)):
        raise ReportError("M4 evidence subdirectory is unsafe")
    current = artifact
    if current.is_symlink():
        raise ReportError("M4 evidence root must not be a symlink")
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ReportError("M4 evidence subdirectory must not traverse a symlink")
    if not current.is_dir():
        raise ReportError("M4 task evidence subdirectory is missing")
    return current


def _read_trace(path: Path, expected_cases: list[dict[str, object]], scope: str) -> bytes:
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise ReportError(f"required M4 trace {path.name} is missing") from error
    if not raw or len(raw) > 64 << 20 or not raw.endswith(b"\n"):
        raise ReportError(f"M4 trace {path.name} is empty, oversized, or unterminated")
    lines = raw.splitlines()
    if len(lines) != len(expected_cases):
        raise ReportError(f"M4 trace {path.name} does not contain every registered case")
    expected_ordinals: dict[str, int] = {}
    for ordinal, (line, expected) in enumerate(zip(lines, expected_cases), start=1):
        try:
            record = json.loads(line, object_pairs_hook=_unique_object)
        except (json.JSONDecodeError, ReportError) as error:
            raise ReportError(f"M4 trace {path.name} line {ordinal} is invalid") from error
        required_fields = {
            "schema", "case_id", "ordinal", "method", "request", "response", "error",
            "stream_terminal", "evidence_scope", "elapsed_ns",
        }
        if not isinstance(record, dict) or set(record) != required_fields:
            raise ReportError("M4 trace record envelope is incomplete or has unknown fields")
        expected_case_id = expected["id"]
        expected_case_ordinal = expected_ordinals.get(expected_case_id, 0) + 1
        expected_ordinals[expected_case_id] = expected_case_ordinal
        if (record["schema"] != TRACE_SCHEMA or record["case_id"] != expected_case_id
                or record["ordinal"] != expected_case_ordinal or record["method"] != expected["method"]
                or record["evidence_scope"] != scope):
            raise ReportError("M4 trace has a changed case, order, method, or evidence scope")
        if not isinstance(record["request"], dict):
            raise ReportError("M4 trace request snapshot is missing")
        expected_error = {
            "duplicate-id": "FailedPrecondition",
            "oversized-ttl": "OutOfRange",
            "binding-invalid-lease-atomicity": "NotFound",
            "binding-invalid-lease-put": "NotFound",
        }.get(expected_case_id)
        if expected_error is not None:
            if (not isinstance(record["error"], dict)
                    or record["error"].get("grpc_code") != expected_error
                    or record["response"] is not None):
                raise ReportError("M4 trace did not preserve its required native error")
        elif record["error"] is not None or not isinstance(record["response"], dict):
            raise ReportError("M4 successful Lease case has no response or has an error")
        if record["stream_terminal"] is not None:
            raise ReportError("M4 unary Lease trace unexpectedly has a stream terminal")
    return raw


def _validate_run_metadata(
    path: Path, *, run: dict[str, object], descriptor: dict[str, object], trace_bytes: bytes,
    suite_hash: str, prefix_hash: str, layout: str,
) -> None:
    metadata = read_json(path)
    if not isinstance(metadata, dict):
        raise ReportError("M4 driver metadata is malformed")
    if (metadata.get("schema") != RUN_SCHEMA or metadata.get("status") != "passed"
            or metadata.get("run_id") != run["run_id"]
            or metadata.get("source_commit") != run["source_commit"]
            or metadata.get("suite_sha256") != suite_hash
            or metadata.get("evidence_scope") != descriptor["scope"]
            or metadata.get("prefix_sha256") != prefix_hash
            or metadata.get("trace_sha256") != hashlib.sha256(trace_bytes).hexdigest()
            or metadata.get("embedded_native") is not descriptor["embedded_native"]
            or metadata.get("layout") != layout):
        raise ReportError(f"M4 driver metadata {path.name} does not match this run")


def validate_artifact(repository: Path, artifact: Path, registry: dict[str, object]) -> dict[str, object]:
    run = read_json(artifact / "run.json")
    if not isinstance(run, dict):
        raise ReportError("M4 run manifest is malformed")
    tasks = run.get("tasks")
    if (run.get("schema") != RUN_SCHEMA or run.get("status") != "passed"
            or run.get("scope") != "public-local-minicluster"
            or not isinstance(tasks, list) or len(tasks) != 1):
        raise ReportError("M4 run manifest is incomplete or did not pass")
    task_id = tasks[0]
    task = next((item for item in registry["tasks"] if item["id"] == task_id), None)
    if task is None:
        raise ReportError("M4 run task is not registered")
    evidence_root = _task_artifact_root(artifact, task)
    run_id = run.get("run_id")
    source_commit = run.get("source_commit")
    if (not isinstance(run_id, str) or run_id != artifact.name
            or not isinstance(source_commit, str) or not re.fullmatch(r"[0-9a-f]{40}", source_commit)
            or run.get("checkout") != source_commit
            or not re.fullmatch(r"[0-9a-f]{40}", str(run.get("base_commit", "")))
            or not isinstance(run.get("worktree_clean"), bool)
            or not isinstance(run.get("started_at"), str)
            or not isinstance(run.get("finished_at"), str)):
        raise ReportError("M4 run identity or source binding is invalid")

    _validate_commands(artifact, run)
    go_test_count = _validate_go_tests(artifact, task)
    java_test_count = _validate_java_tests(artifact, task)
    manifest_name = task.get("manifest")
    if not isinstance(manifest_name, str) or Path(manifest_name).name != manifest_name:
        raise ReportError("registered M4 scenario manifest path is unsafe")
    suite_path = repository / "testkit/scenarios" / manifest_name
    try:
        suite_hash = hashlib.sha256(suite_path.read_bytes()).hexdigest()
    except OSError as error:
        raise ReportError("registered M4 scenario manifest is missing") from error
    prefix = f"etcd-hbase-m4-test/{run_id}/"
    if task_id == "J41":
        prefix += "bindings/"
    prefix_hash = hashlib.sha256(prefix.encode()).hexdigest()

    expected_cases = task["trace_cases"]
    native = task["evidence"]["native"]
    native_bytes = _read_trace(evidence_root / "native-trace.jsonl", expected_cases, native["scope"])
    _validate_run_metadata(
        evidence_root / "native-metadata.json", run=run, descriptor=native, trace_bytes=native_bytes,
        suite_hash=suite_hash, prefix_hash=prefix_hash, layout="",
    )
    native_log = evidence_root / "native.log"
    if not native_log.is_file() or "M4 Lease scenario passed" not in native_log.read_text(encoding="utf-8"):
        raise ReportError("native Go Lease run has no success record")

    layouts: list[str] = []
    for descriptor in task["evidence"]["targets"]:
        layout = descriptor["layout"]
        suffix = layout.lower()
        trace_bytes = _read_trace(
            evidence_root / f"target-{suffix}-trace.jsonl", expected_cases, descriptor["scope"]
        )
        _validate_run_metadata(
            evidence_root / f"target-{suffix}-metadata.json", run=run, descriptor=descriptor,
            trace_bytes=trace_bytes, suite_hash=suite_hash, prefix_hash=prefix_hash, layout=layout,
        )
        target_log = evidence_root / f"target-{suffix}.log"
        if not target_log.is_file() or "M4 Lease scenario passed" not in target_log.read_text(encoding="utf-8"):
            raise ReportError(f"target Go Lease run for {layout} has no success record")
        layouts.append(layout)

    compare_success = J41_COMPARE_SUCCESS if task_id == "J41" else COMPARE_SUCCESS
    for compare_log in task["evidence"]["compare_logs"]:
        path = evidence_root / compare_log
        if not path.is_file() or compare_success not in path.read_text(encoding="utf-8"):
            raise ReportError(f"M4 native comparison {compare_log} is missing or failed")

    return {
        "task": task_id,
        "run_id": run_id,
        "source_commit": source_commit,
        "base_commit": run["base_commit"],
        "evidence_class": "official-clean" if run["worktree_clean"] else "development-dirty",
        "go_test_count": go_test_count,
        "java_test_count": java_test_count,
        "native_trace_cases": len(expected_cases),
        "target_layouts": sorted(layouts),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--validate-registry", action="store_true")
    group.add_argument("--validate-all", action="store_true")
    group.add_argument("--artifact-root")
    arguments = parser.parse_args(argv)
    try:
        registry = load_registry()
        if arguments.validate_registry:
            print("M4 evidence registry is valid")
            return 0
        if arguments.validate_all:
            validate_all_tasks_registered(registry)
            print("All M4 task registrations are available")
            return 0
        artifact = artifact_location(REPO, arguments.artifact_root)
        summary = validate_artifact(REPO, artifact, registry)
        run = read_json(artifact / "run.json")
        if not isinstance(run, dict):
            raise ReportError("M4 run manifest is malformed")
        validate_repository_binding(REPO, run)
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
        return 0
    except ReportError as error:
        print(f"M4 evidence rejected: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
