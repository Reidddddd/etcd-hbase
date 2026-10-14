#!/usr/bin/env python3
"""Check that the API matrix matches the pinned descriptor and test cases."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


CASE_RE = re.compile(r"^C\d{2}$")
USAGE_RE = re.compile(r"^U\d{2}$")
TASK_RE = re.compile(r"^J\d{2}(?:\.\d+)?$")
METHOD_RE = re.compile(r"^[A-Za-z0-9_.]+/[A-Za-z0-9_]+$")
REQUIRED_COUNT_RULES = {
    "exact_get",
    "unlimited_range",
    "limited_range",
    "exact_count_only",
    "prefix_count_only_limit_one",
    "other_range_count_only",
}


class MatrixError(Exception):
    pass


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise MatrixError(f"cannot read {path}: {error}") from error


def descriptor_inventory(wire: dict[str, Any]) -> dict[str, dict[str, Any]]:
    messages: dict[str, list[dict[str, Any]]] = {}
    for source in wire.get("files", []):
        for message in source.get("messages", []):
            messages[message["name"]] = message.get("fields", [])

    methods: dict[str, dict[str, Any]] = {}
    for source in wire.get("files", []):
        package = source.get("package", "")
        for service in source.get("services", []):
            for method in service.get("methods", []):
                full_name = f"{package}.{service['name']}/{method['name']}"
                request = method["input"]
                response = method["output"]
                request_fields = messages.get(request)
                response_fields = messages.get(response)
                if request_fields is None or response_fields is None:
                    raise MatrixError(f"wire descriptor has no field inventory for {full_name}")
                if full_name in methods:
                    raise MatrixError(f"duplicate descriptor method {full_name}")
                methods[full_name] = {
                    "request_type": request,
                    "response_type": response,
                    "request_fields": request_fields,
                    "response_fields": response_fields,
                    "client_streaming": method["client_streaming"],
                    "server_streaming": method["server_streaming"],
                }
    if not methods:
        raise MatrixError("wire contract contains no RPC methods")
    return methods


def streaming_shape(method: dict[str, Any]) -> str:
    client = method["client_streaming"]
    server = method["server_streaming"]
    if client and server:
        return "bidirectional-streaming"
    if client:
        return "client-streaming"
    if server:
        return "server-streaming"
    return "unary"


def pipe_cells(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def codes_in(cell: str, pattern: re.Pattern[str]) -> list[str]:
    return sorted(set(re.findall(r"\b(?:U\d{2}|J\d{2}(?:\.\d+)?|C\d{2})\b", cell)))


def read_method_table(matrix_path: Path, text: str) -> dict[str, list[str]]:
    heading = "## 完整 gRPC 方法清单"
    if heading not in text:
        raise MatrixError(f"{matrix_path} is missing the complete RPC inventory heading")
    section = text.split(heading, 1)[1].split("\n## ", 1)[0]
    rows: dict[str, list[str]] = {}
    for line in section.splitlines():
        if not line.startswith("|"):
            continue
        cells = pipe_cells(line)
        if len(cells) != 8:
            continue
        match = re.fullmatch(r"`([^`]+)`", cells[0])
        if not match:
            continue
        method = match.group(1)
        if method in rows:
            raise MatrixError(f"duplicate matrix method row {method}")
        rows[method] = cells
    return rows


def validate_case_and_usage_refs(
    matrix_path: Path,
    matrix_text: str,
    cases_data: dict[str, Any],
    methods: dict[str, dict[str, Any]],
    additional: dict[str, dict[str, Any]],
) -> None:
    documented_cases = set(re.findall(r"^\|\s*(C\d{2})\s*\|", matrix_text, re.MULTILINE))
    cases = cases_data.get("cases", [])
    case_ids = [case.get("id") for case in cases]
    if len(case_ids) != len(set(case_ids)):
        raise MatrixError("m0-wire.json contains duplicate case IDs")
    if set(case_ids) != documented_cases:
        missing = sorted(documented_cases - set(case_ids))
        extra = sorted(set(case_ids) - documented_cases)
        raise MatrixError(f"case coverage differs from docs: missing={missing}, extra={extra}")

    usage_path = matrix_path.with_name("milvus-etcd-usage.md")
    try:
        usage_text = usage_path.read_text(encoding="utf-8")
    except OSError as error:
        raise MatrixError(f"cannot read usage baseline {usage_path}: {error}") from error
    documented_usage = set(re.findall(r"^\|\s*(U\d{2})\s*\|", usage_text, re.MULTILINE))

    case_by_id = {case["id"]: case for case in cases}
    expected_methods = set(methods) | set(additional)
    referenced_by_case: dict[str, set[str]] = {}
    for case in cases:
        case_id = case["id"]
        method_refs = case.get("methods", [])
        if not method_refs:
            raise MatrixError(f"case {case_id} has no RPC method mapping")
        unknown_methods = sorted(set(method_refs) - expected_methods)
        if unknown_methods:
            raise MatrixError(f"case {case_id} references unknown methods: {unknown_methods}")
        usage_refs = case.get("usage_ids", [])
        if len(usage_refs) != len(set(usage_refs)):
            raise MatrixError(f"case {case_id} has duplicate usage IDs")
        unknown_usage = sorted(set(usage_refs) - documented_usage)
        if unknown_usage:
            raise MatrixError(f"case {case_id} references unknown usage IDs: {unknown_usage}")
        if case.get("verification_state") not in {"planned", "native-pass", "transport-pass", "target-pass"}:
            raise MatrixError(f"case {case_id} has invalid verification_state")
        referenced_by_case[case_id] = set(method_refs)

    for method, record in list(methods.items()) + list(additional.items()):
        refs = record.get("case_ids", [])
        if not refs or len(refs) != len(set(refs)):
            raise MatrixError(f"method {method} must reference unique case IDs")
        for case_id in refs:
            if case_id not in case_by_id:
                raise MatrixError(f"method {method} references unknown case {case_id}")
            if method not in referenced_by_case[case_id]:
                raise MatrixError(f"case {case_id} does not map back to method {method}")
        usage_refs = record.get("usage_ids", [])
        if len(usage_refs) != len(set(usage_refs)):
            raise MatrixError(f"method {method} has duplicate usage IDs")
        unknown_usage = sorted(set(usage_refs) - documented_usage)
        if unknown_usage:
            raise MatrixError(f"method {method} references unknown usage IDs: {unknown_usage}")


def validate_count_policy(path: Path) -> None:
    policy = read_json(path)
    rules = policy.get("rules", [])
    ids = [rule.get("id") for rule in rules]
    if len(rules) != 6 or set(ids) != REQUIRED_COUNT_RULES or len(ids) != len(set(ids)):
        raise MatrixError("count-policy.json must define the six required, unique Count rules")
    for rule in rules:
        for required in ("request", "count", "more", "case_ids"):
            if not rule.get(required):
                raise MatrixError(f"Count rule {rule.get('id')} lacks {required}")
    difference = policy.get("accepted_difference", {})
    if (
        difference.get("method") != "etcdserverpb.KV/Range"
        or difference.get("case_id") != "C05"
        or difference.get("field") != "Count"
        or not {"Kvs", "More", "Header"}.issubset(set(difference.get("preserved_fields", [])))
    ):
        raise MatrixError("accepted Count difference must be limited to Count for C05")
    other_count_only = next(rule for rule in rules if rule["id"] == "other_range_count_only")
    if "UNIMPLEMENTED" not in str(other_count_only.get("error", "")):
        raise MatrixError("other range CountOnly must explicitly return UNIMPLEMENTED")


def validate(args: argparse.Namespace) -> tuple[int, int, int, int]:
    wire = read_json(args.wire_contract)
    cases_data = read_json(args.cases)
    matrix_text = args.matrix.read_text(encoding="utf-8")
    if cases_data.get("protocol") != wire.get("protocol"):
        raise MatrixError("wire contract and case file protocol versions differ")
    if cases_data.get("source_revision") != wire.get("source_revision"):
        raise MatrixError("wire contract and case file source revisions differ")

    expected = descriptor_inventory(wire)
    documented = read_method_table(args.matrix, matrix_text)
    descriptor_methods = cases_data.get("descriptor_methods", [])
    additional_methods = cases_data.get("additional_methods", [])
    case_methods = {entry.get("method"): entry for entry in descriptor_methods}
    additional = {entry.get("method"): entry for entry in additional_methods}
    if len(case_methods) != len(descriptor_methods) or len(additional) != len(additional_methods):
        raise MatrixError("m0-wire.json contains duplicate method entries")

    missing = sorted(set(expected) - set(case_methods))
    extra = sorted(set(case_methods) - set(expected))
    if missing or extra:
        raise MatrixError(f"descriptor coverage mismatch: missing={missing}, extra={extra}")
    for method, descriptor in expected.items():
        entry = case_methods[method]
        for field in ("request_type", "response_type", "request_fields", "response_fields", "client_streaming", "server_streaming"):
            if entry.get(field) != descriptor[field]:
                raise MatrixError(f"{method} {field} differs from pinned wire contract")

    if len(additional_methods) != 7:
        raise MatrixError("exactly seven external Lock/Election methods must be inventoried")
    for method, entry in additional.items():
        if not METHOD_RE.fullmatch(method):
            raise MatrixError(f"invalid additional RPC path {method!r}")
        if entry.get("source_revision") != wire.get("source_revision") or not entry.get("source_path"):
            raise MatrixError(f"additional method {method} lacks a fixed upstream source")

    expected_rows = set(expected) | set(additional)
    missing_rows = sorted(expected_rows - set(documented))
    extra_rows = sorted(set(documented) - expected_rows)
    if missing_rows or extra_rows:
        raise MatrixError(f"matrix inventory mismatch: missing={missing_rows}, extra={extra_rows}")

    for method, entry in list(case_methods.items()) + list(additional.items()):
        row = documented[method]
        disposition = entry.get("first_release_disposition")
        if disposition not in {"supported", "unsupported", "conditional", "mixed"}:
            raise MatrixError(f"{method} has invalid first_release_disposition")
        if row[3] != disposition:
            raise MatrixError(f"{method} disposition differs between docs and JSON")
        expected_shape = streaming_shape(entry)
        if row[2] != expected_shape:
            raise MatrixError(f"{method} streaming shape differs between docs and JSON")
        if row[1] != "external proto" and method in expected:
            expected_types = f"`{entry['request_type']}` → `{entry['response_type']}`"
            if row[1] != expected_types:
                raise MatrixError(f"{method} request/response types differ between docs and JSON")
        if codes_in(row[4], USAGE_RE) != sorted(entry.get("usage_ids", [])):
            raise MatrixError(f"{method} usage IDs differ between docs and JSON")
        if codes_in(row[5], TASK_RE) != sorted(entry.get("tasks", [])):
            raise MatrixError(f"{method} task IDs differ between docs and JSON")
        if codes_in(row[6], CASE_RE) != sorted(entry.get("case_ids", [])):
            raise MatrixError(f"{method} case IDs differ between docs and JSON")
        if "UNIMPLEMENTED" not in row[7] or "UNIMPLEMENTED" not in entry.get("m0_behavior", ""):
            raise MatrixError(f"{method} must clearly state the M0 skeleton rejection")
        if disposition in {"unsupported", "conditional", "mixed"}:
            error = entry.get("error_policy", "")
            if not error or "UNIMPLEMENTED" not in error or "UNIMPLEMENTED" not in row[7]:
                raise MatrixError(f"{method} has no explicit rejection/error policy")

    validate_case_and_usage_refs(args.matrix, matrix_text, cases_data, case_methods, additional)
    count_policy_path = args.cases.parent.parent / "count-policy.json"
    validate_count_policy(count_policy_path)
    return len(expected), len(additional), len(cases_data.get("cases", [])), len(documented)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--wire-contract", type=Path, required=True)
    parser.add_argument("--cases", type=Path, required=True)
    args = parser.parse_args()
    try:
        descriptor_count, additional_count, case_count, row_count = validate(args)
    except (MatrixError, OSError, KeyError, TypeError) as error:
        print(f"api-matrix: {error}", file=sys.stderr)
        return 1
    print(
        f"api-matrix: {descriptor_count} descriptor methods, {additional_count} external methods, "
        f"{case_count} cases, {row_count} complete inventory rows; Count policy and usage references valid"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
