#!/usr/bin/env python3
"""Verify J00 source identities, hashes, inventory, and pinned documentation links."""

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path


PROJECT = Path(__file__).resolve().parents[1]


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args]).decode().strip()


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def client_inventory(root):
    paths = subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "-z", "--", "*.go"]
    ).decode().split("\0")
    inventory = []
    for name in sorted(filter(None, paths)):
        if name.endswith("_test.go") or "/mock" in name or name.startswith("tests/"):
            continue
        text = (root / name).read_text()
        if '"go.etcd.io/etcd/client/v3"' not in text:
            continue
        if name.startswith("cmd/tools/migration/"):
            scope = "migration-excluded"
        elif name.endswith("embed_etcd_kv.go"):
            scope = "embedded-excluded"
        elif name.startswith("pkg/streaming/walimpls/impls/wp/"):
            scope = "woodpecker-excluded"
        elif name.endswith("test_utility.go"):
            scope = "test-helper-excluded"
        else:
            scope = "first-party-audit-entry"
        inventory.append({
            "path": name,
            "scope": scope,
            "sha256": sha256(root / name),
            "option_tokens": sorted(set(re.findall(
                r"clientv3\.(With\w+|Op\w+|Compare|Version|Value|ModRevision|CreateRevision|LeaseValue)",
                text,
            ))),
        })
    return inventory


def rpc_catalog(root, paths):
    catalog = []
    for name in paths:
        text = (root / name).read_text()
        package = re.search(r"^package\s+(\w+);", text, re.M).group(1)
        service = None
        for number, line in enumerate(text.splitlines(), 1):
            match = re.match(r"^service\s+(\w+)\s*\{", line)
            if match:
                service = match.group(1)
            match = re.match(
                r"\s*rpc\s+(\w+)\((stream\s+)?(\w+)\)\s+returns\s+\((stream\s+)?(\w+)\)",
                line,
            )
            if match:
                catalog.append({
                    "path": f"/{package}.{service}/{match.group(1)}",
                    "request": match.group(3),
                    "response": match.group(5),
                    "client_streaming": bool(match.group(2)),
                    "server_streaming": bool(match.group(4)),
                    "source": name,
                    "line": number,
                })
    return catalog


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify_links(root, commit, repository, documents):
    count = 0
    pattern = re.compile(
        r"https://github\.com/" + re.escape(repository)
        + r"/blob/([0-9a-f]{40})/([^\s)#]+)(?:#L(\d+))?"
    )
    for document in documents:
        for match in pattern.finditer(document.read_text()):
            revision, name, number = match.groups()
            require(revision == commit, f"Wrong source revision in {document}: {revision}")
            path = root / name
            require(path.is_file(), f"Source link missing: {name}")
            if number:
                require(1 <= int(number) <= len(path.read_text().splitlines()),
                        f"Source link line out of bounds: {name}:{number}")
            count += 1
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--milvus-source", required=True, type=Path)
    parser.add_argument("--etcd-source", required=True, type=Path)
    args = parser.parse_args()
    baseline = json.loads((PROJECT / "docs/baselines/j00-source-baseline.json").read_text())
    roots = {"milvus": args.milvus_source.resolve(), "etcd": args.etcd_source.resolve()}
    for key, root in roots.items():
        source = baseline["sources"][key]
        require(git(root, "rev-parse", "HEAD") == source["commit"], f"Wrong {key} HEAD")
        require(not git(root, "status", "--porcelain", "--untracked-files=no"),
                f"Tracked {key} sources have local changes")
        for name, expected in source["files"].items():
            require(sha256(root / name) == expected, f"Hash mismatch: {key}/{name}")

    require(client_inventory(roots["milvus"]) == baseline["direct_client_imports"],
            "Milvus client import inventory differs")
    require(rpc_catalog(roots["etcd"], baseline["rpc_proto_files"]) == baseline["rpc_catalog"],
            "etcd RPC catalog differs")
    for name in ("go.mod", "pkg/go.mod"):
        text = (roots["milvus"] / name).read_text()
        for module in ("api/v3", "client/v3", "server/v3"):
            require(re.search(r"^\s*go\.etcd\.io/etcd/" + re.escape(module)
                              + r"\s+v3\.5\.23\s*$", text, re.M),
                    f"Unexpected etcd requirement in {name}: {module}")

    documents = [PROJECT / "docs" / name for name in (
        "api-matrix.md", "milvus-etcd-usage.md", "acceptance-targets.md",
    )]
    link_count = sum(verify_links(
        roots[key], baseline["sources"][key]["commit"],
        baseline["sources"][key]["repository"], documents,
    ) for key in roots)
    matrix = documents[0].read_text()
    cases = re.findall(r"^\| (C\d{2}) \|", matrix, re.M)
    require(cases == [f"C{i:02d}" for i in range(1, 34)], "Validation case IDs differ")
    for document in documents:
        text = document.read_text()
        require(not any(line.rstrip() != line for line in text.splitlines()),
                f"Trailing whitespace: {document}")
        definitions = set(re.findall(r"^\[([^]]+)\]:", text, re.M))
        references = set(re.findall(r"\[[^]\n]+\]\[([^]\n]+)\]", text))
        require(references <= definitions, f"Undefined link references: {document}")
        for target in re.findall(r"\]\(([^\s)]+)\)", text):
            if not target.startswith(("http:", "https:", "#")):
                require((document.parent / target.split("#", 1)[0]).is_file(),
                        f"Missing local link: {document}: {target}")
    print(f"Verified fixed source commits and hashes; {len(baseline['direct_client_imports'])} "
          f"client-import entries; {len(baseline['rpc_catalog'])} RPCs; {link_count} "
          "pinned source links; 33 validation case definitions.")
    print("Static audit only: no Java service, Go client, HBase, or Milvus runtime tests executed.")


if __name__ == "__main__":
    main()
