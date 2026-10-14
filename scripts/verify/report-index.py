#!/usr/bin/env python3
"""Record public M0 verification runs and validate their JSON evidence index."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REQUIRED_STEPS = (
    "protocol",
    "go_module_lock",
    "native_oracle",
    "wire",
    "public_hbase",
    "maven_verify",
    "effective_pom",
    "dependency_tree",
    "reproducible_build",
    "runtime_image",
)


class IndexError(Exception):
    pass


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IndexError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise IndexError(f"expected a JSON object in {path}")
    return value


def command_output(*command: str, cwd: Path) -> str:
    try:
        return subprocess.check_output(command, cwd=cwd, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise IndexError(f"could not collect {command[0]} metadata") from exc


def begin(args: argparse.Namespace) -> int:
    artifact_root = Path(args.artifact_root).resolve()
    repo_root = Path(args.repo_root).resolve()
    artifact_root.mkdir(parents=True, exist_ok=True)
    state = {
        "schema_version": 1,
        "run_id": args.run_id,
        "scope": args.scope,
        "git_commit": command_output("git", "rev-parse", "HEAD", cwd=repo_root),
        "started_at_utc": utc_now(),
        "steps": [],
    }
    write_json(artifact_root / "execution.json", state)
    print(f"M0 public run started: {args.run_id}")
    return 0


def record(args: argparse.Namespace) -> int:
    artifact_root = Path(args.artifact_root).resolve()
    path = artifact_root / "execution.json"
    state = read_json(path)
    steps = state.get("steps")
    if not isinstance(steps, list):
        raise IndexError("execution.json has no step list")
    if any(isinstance(step, dict) and step.get("id") == args.step_id for step in steps):
        raise IndexError(f"step was recorded more than once: {args.step_id}")
    step: dict[str, Any] = {
        "id": args.step_id,
        "state": args.state,
        "exit_code": args.exit_code,
    }
    if args.log:
        log_path = Path(args.log)
        step["log"] = str(log_path)
    if args.reason:
        step["reason"] = args.reason
    steps.append(step)
    write_json(path, state)
    return 0


def test_summary(repo_root: Path) -> dict[str, Any]:
    reports: list[dict[str, Any]] = []
    for module in sorted(repo_root.iterdir()):
        if not module.is_dir():
            continue
        for kind in ("surefire-reports", "failsafe-reports"):
            report_root = module / "target" / kind
            if not report_root.is_dir():
                continue
            for report in sorted(report_root.glob("TEST-*.xml")):
                try:
                    suite = ET.parse(report).getroot()
                    counts = {
                        key: int(suite.attrib.get(key, "0"))
                        for key in ("tests", "failures", "errors", "skipped")
                    }
                except (ET.ParseError, OSError, ValueError) as exc:
                    raise IndexError(f"invalid test report {report}: {exc}") from exc
                reports.append(
                    {
                        "file": str(report.relative_to(repo_root)),
                        "suite": suite.attrib.get("name", report.stem.removeprefix("TEST-")),
                        **counts,
                    }
                )
    totals = {
        key: sum(report[key] for report in reports)
        for key in ("tests", "failures", "errors", "skipped")
    }
    return {"suite_files": len(reports), **totals, "suites": reports}


def trace_summary(path: Path, artifact_root: Path) -> dict[str, Any]:
    records = []
    if path.is_file():
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise IndexError(f"invalid wire trace at line {line_number}: {exc}") from exc
    scopes = sorted({record.get("evidence_scope") for record in records if isinstance(record, dict)})
    return {
        "path": str(path.relative_to(artifact_root)),
        "records": len(records),
        "evidence_scopes": scopes,
    }


def finalize(args: argparse.Namespace) -> int:
    artifact_root = Path(args.artifact_root).resolve()
    repo_root = Path(args.repo_root).resolve()
    execution = read_json(artifact_root / "execution.json")
    steps = execution.get("steps", [])
    if not isinstance(steps, list):
        raise IndexError("execution.json has no step list")
    by_id = {step.get("id"): step for step in steps if isinstance(step, dict)}
    tests = test_summary(repo_root)
    trace = trace_summary(artifact_root / "wire" / "transport.jsonl", artifact_root)
    native_metadata = artifact_root / "native" / "run-one" / "metadata.json"

    validation_errors: list[str] = []
    if args.exit_code == 0:
        for step_id in REQUIRED_STEPS:
            step = by_id.get(step_id)
            if step is None:
                validation_errors.append(f"required step is missing: {step_id}")
            elif step.get("state") == "passed":
                if step.get("exit_code") != 0:
                    validation_errors.append(f"passing step has a nonzero exit code: {step_id}")
                log = step.get("log")
                if not isinstance(log, str) or not (artifact_root / log).is_file():
                    validation_errors.append(f"step log is missing: {step_id}")
            elif step_id == "runtime_image" and step.get("state") == "skipped":
                if not step.get("reason"):
                    validation_errors.append("runtime image skip must include a reason")
            else:
                validation_errors.append(f"required step did not pass: {step_id}")

        if tests["tests"] <= 0:
            validation_errors.append("the final Maven build produced no test cases")
        if tests["failures"] or tests["errors"]:
            validation_errors.append("the final Maven build contains failed test cases")
        if tests["skipped"]:
            validation_errors.append("the final Maven build contains skipped test cases")
        if trace["records"] != 10:
            validation_errors.append("the wire trace must contain all 10 required cases")
        if trace["evidence_scopes"] != ["transport", "transport-fixture"]:
            validation_errors.append("the wire trace has unexpected evidence scopes")
        if not native_metadata.is_file():
            validation_errors.append("native etcd metadata is missing")
        for relative in ("effective-pom.xml", "server-dependency-tree.txt"):
            if not (artifact_root / relative).is_file():
                validation_errors.append(f"Maven evidence is missing: {relative}")

    report = {
        "schema_version": 1,
        "run_id": execution.get("run_id"),
        "scope": execution.get("scope"),
        "git_commit": execution.get("git_commit"),
        "started_at_utc": execution.get("started_at_utc"),
        "finished_at_utc": utc_now(),
        "exit_code": args.exit_code,
        "state": "passed" if args.exit_code == 0 and not validation_errors else "failed_or_incomplete",
        "steps": steps,
        "test_summary": tests,
        "evidence": {
            "transport": trace,
            "native_etcd": {
                "state": "collected" if native_metadata.is_file() else "missing",
                "path": str(native_metadata.relative_to(artifact_root)),
            },
            "target_hbase": {
                "state": "not_run_missing_inputs",
                "missing_inputs": ["B02", "B03", "B06"],
            },
        },
        "validation_errors": validation_errors,
    }
    write_json(artifact_root / "INDEX.json", report)
    print(f"M0 evidence index: {artifact_root / 'INDEX.json'}")
    if validation_errors:
        for error in validation_errors:
            print(f"validation error: {error}", file=sys.stderr)
        return 1
    return 0


def progress_value(progress: Path, key: str) -> str:
    prefix = f"{key}:"
    values = [
        line[len(prefix) :].strip()
        for line in progress.read_text(encoding="utf-8").splitlines()
        if line.startswith(prefix)
    ]
    if len(values) != 1 or not values[0]:
        raise IndexError(f"{progress} must contain exactly one non-empty {key}: entry")
    return values[0]


def validate_progress_index(args: argparse.Namespace) -> int:
    if args.phase != "M0":
        raise IndexError(f"unsupported phase: {args.phase}")

    repo_root = Path(__file__).resolve().parents[2]
    progress = Path(args.progress)
    if not progress.is_absolute():
        progress = repo_root / progress
    progress = progress.resolve()
    try:
        progress.relative_to(repo_root)
    except ValueError as exc:
        raise IndexError("progress file must be inside the repository") from exc
    if not progress.is_file():
        raise IndexError(f"progress file does not exist: {progress}")

    index_reference = Path(progress_value(progress, "verification_index"))
    if not index_reference.is_absolute():
        index_reference = repo_root / index_reference
    index_path = index_reference.resolve()
    try:
        index_path.relative_to(repo_root / ".artifacts" / "m0" / "public")
    except ValueError as exc:
        raise IndexError("verification_index must point inside .artifacts/m0/public") from exc

    report = read_json(index_path)
    run_id = progress_value(progress, "verification_run_id")
    if report.get("run_id") != run_id:
        raise IndexError("progress run id does not match the referenced evidence index")
    if (
        report.get("scope") != "public"
        or report.get("state") != "passed"
        or report.get("exit_code") != 0
    ):
        raise IndexError("referenced public evidence index did not pass")
    if report.get("validation_errors") != []:
        raise IndexError("referenced public evidence index contains validation errors")

    current_commit = command_output("git", "rev-parse", "HEAD", cwd=repo_root)
    if report.get("git_commit") != current_commit:
        raise IndexError("evidence index was generated for a different commit")
    tracked_changes = command_output(
        "git", "status", "--porcelain", "--untracked-files=no", cwd=repo_root
    )
    if tracked_changes:
        raise IndexError("tracked worktree changes exist after the evidence run")

    steps = report.get("steps")
    if not isinstance(steps, list):
        raise IndexError("evidence index has no step list")
    by_id = {step.get("id"): step for step in steps if isinstance(step, dict)}
    for step_id in REQUIRED_STEPS:
        step = by_id.get(step_id)
        if step is None:
            raise IndexError(f"evidence index is missing required step: {step_id}")
        if step.get("state") == "passed" and step.get("exit_code") == 0:
            log = step.get("log")
            if not isinstance(log, str) or not (index_path.parent / log).is_file():
                raise IndexError(f"evidence log is missing for step: {step_id}")
        elif step_id == "runtime_image" and step.get("state") == "skipped" and step.get("reason"):
            pass
        else:
            raise IndexError(f"required step did not pass or record a reasoned skip: {step_id}")

    tests = report.get("test_summary")
    if not isinstance(tests, dict) or tests.get("tests", 0) <= 0:
        raise IndexError("evidence index has no executed test cases")
    if tests.get("failures", 0) or tests.get("errors", 0) or tests.get("skipped", 0):
        raise IndexError("evidence index contains failed or skipped test cases")
    transport = report.get("evidence", {}).get("transport", {})
    if transport.get("records") != 10 or transport.get("evidence_scopes") != [
        "transport",
        "transport-fixture",
    ]:
        raise IndexError("wire evidence is incomplete or has an unexpected scope")
    if report.get("evidence", {}).get("native_etcd", {}).get("state") != "collected":
        raise IndexError("native etcd evidence is missing")
    target_hbase = report.get("evidence", {}).get("target_hbase", {})
    if target_hbase.get("state") != "not_run_missing_inputs":
        raise IndexError("target HBase evidence state is missing or misclassified")
    if not {"B02", "B03", "B06"}.issubset(set(target_hbase.get("missing_inputs", []))):
        raise IndexError("target HBase evidence does not list the outstanding inputs")

    print(
        f"M0 evidence index validated: run={run_id}, commit={current_commit}, "
        f"tests={tests['tests']}, target_hbase={target_hbase['state']}"
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", help="phase to validate when using the progress-index mode")
    parser.add_argument("--progress", help="local execution progress Markdown file")
    commands = parser.add_subparsers(dest="command")

    start = commands.add_parser("begin")
    start.add_argument("--repo-root", required=True)
    start.add_argument("--artifact-root", required=True)
    start.add_argument("--run-id", required=True)
    start.add_argument("--scope", choices=("public",), default="public")
    start.set_defaults(handler=begin)

    add_step = commands.add_parser("record")
    add_step.add_argument("--artifact-root", required=True)
    add_step.add_argument("--step-id", required=True)
    add_step.add_argument("--state", choices=("passed", "failed", "skipped"), required=True)
    add_step.add_argument("--exit-code", type=int)
    add_step.add_argument("--log")
    add_step.add_argument("--reason")
    add_step.set_defaults(handler=record)

    finish = commands.add_parser("finalize")
    finish.add_argument("--repo-root", required=True)
    finish.add_argument("--artifact-root", required=True)
    finish.add_argument("--exit-code", type=int, required=True)
    finish.set_defaults(handler=finalize)
    args = parser.parse_args()
    if args.command is None:
        if args.phase is None or args.progress is None:
            parser.error("use a begin/record/finalize subcommand or provide --phase and --progress")
        args.handler = validate_progress_index
    elif args.phase is not None or args.progress is not None:
        parser.error("--phase and --progress are only valid without a subcommand")
    return args


def main() -> int:
    args = parse_args()
    try:
        return args.handler(args)
    except IndexError as exc:
        print(f"report-index: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
