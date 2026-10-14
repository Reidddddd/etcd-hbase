#!/usr/bin/env python3
"""Regression tests for the Go module graph lock verifier."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "scripts" / "verify" / "go-module-lock.py"
SPEC = importlib.util.spec_from_file_location("go_module_lock", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
go_module_lock = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = go_module_lock
SPEC.loader.exec_module(go_module_lock)


class GoModuleLockTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="m0-go-lock-")
        self.addCleanup(self.temp_dir.cleanup)
        root = Path(self.temp_dir.name)
        self.lock_path = root / "dependency-lock.json"
        self.go_mod_path = root / "go.mod"
        self.go_sum_path = root / "go.sum"
        self.lock_path.write_text(
            json.dumps(
                {
                    "go": {
                        "goDirectiveVersion": "1.26.6",
                        "version": "1.27.2",
                        "moduleVersions": {"example.org/locked": "v1.2.3"},
                        "moduleChecksums": {
                            "example.org/locked@v1.2.3": {
                                "module": "h1:module-checksum",
                                "goMod": "h1:gomod-checksum",
                            }
                        },
                    }
                }
            ),
            encoding="utf-8",
        )
        self.go_mod_path.write_text(
            "module example.org/test\ngo 1.26.6\ntoolchain go1.27.2\n",
            encoding="utf-8",
        )
        self.go_sum_path.write_text(
            "example.org/locked v1.2.3 h1:module-checksum\n"
            "example.org/locked v1.2.3/go.mod h1:gomod-checksum\n",
            encoding="utf-8",
        )
        for name, value in (
            ("LOCK_PATH", self.lock_path),
            ("GO_MOD_PATH", self.go_mod_path),
            ("GO_SUM_PATH", self.go_sum_path),
        ):
            patcher = patch.object(go_module_lock, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_exact_module_graph_matches_lock(self) -> None:
        with patch.object(
            go_module_lock,
            "module_graph",
            return_value={
                "example.org/test": {"Main": True},
                "example.org/locked": {"Version": "v1.2.3"},
                "example.org/transitive": {"Version": "v9.9.9", "Indirect": True},
            },
        ):
            self.assertEqual(1, go_module_lock.validate())

    def test_unlocked_direct_module_in_effective_graph_is_rejected(self) -> None:
        with patch.object(
            go_module_lock,
            "module_graph",
            return_value={
                "example.org/test": {"Main": True},
                "example.org/locked": {"Version": "v1.2.3"},
                "example.org/unlocked": {"Version": "v9.9.9", "Indirect": False},
            },
        ):
            with self.assertRaisesRegex(
                go_module_lock.LockError,
                "module graph contains unlocked direct modules: example.org/unlocked",
            ):
                go_module_lock.validate()


if __name__ == "__main__":
    unittest.main(verbosity=2)
