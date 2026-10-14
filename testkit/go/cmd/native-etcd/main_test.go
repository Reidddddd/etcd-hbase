package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInsideRootRejectsPathsOutsideArtifactDirectory(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "trace.jsonl")

	for _, path := range []string{"../escape/trace.jsonl", outside} {
		t.Run(path, func(t *testing.T) {
			if _, err := insideRoot(root, path); err == nil {
				t.Fatalf("insideRoot(%q) accepted path outside %q", path, root)
			}
		})
	}
}

func TestInsideRootRejectsSymlinkedOutputComponents(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "external")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("cannot create symlink in this environment: %v", err)
	}

	for _, path := range []string{"external", filepath.Join("external", "trace.jsonl")} {
		t.Run(path, func(t *testing.T) {
			if _, err := insideRoot(root, path); err == nil {
				t.Fatalf("insideRoot(%q) accepted a symlinked path", path)
			}
		})
	}
}

func TestInsideRootAllowsNewNestedArtifactPath(t *testing.T) {
	root := filepath.Join(t.TempDir(), "artifacts", "native")
	want := filepath.Join(root, "run-one", "trace.jsonl")

	got, err := insideRoot(root, filepath.Join("run-one", "trace.jsonl"))
	if err != nil {
		t.Fatalf("insideRoot rejected a new nested artifact path: %v", err)
	}
	if got != want {
		t.Fatalf("insideRoot path = %q, want %q", got, want)
	}
}
