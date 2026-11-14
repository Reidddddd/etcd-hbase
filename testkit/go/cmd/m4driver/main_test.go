package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

func TestRunNativeLeaseWritesFreshTraceAndVerifiableMetadata(t *testing.T) {
	root := repositoryRoot(t)
	artifact := t.TempDir()
	tracePath := filepath.Join(artifact, "native.jsonl")
	metadataPath := filepath.Join(artifact, "native.json")
	dataPath := filepath.Join(artifact, "native-data")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := []string{"run", "--scenario", filepath.Join(root, "testkit/scenarios/m4-lease.yaml"),
		"--native-data-dir", dataPath, "--scope", "native-etcd",
		"--prefix", "etcd-hbase-m4-test/driver/", "--trace", tracePath,
		"--metadata", metadataPath}
	if err := run(ctx, args, io.Discard, io.Discard); err != nil {
		t.Fatalf("run native M4 scenario: %v", err)
	}
	traceBytes, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	records, err := trace.ReadJSONL(bytes.NewReader(traceBytes))
	if err != nil || len(records) != 11 {
		t.Fatalf("native M4 trace has %d records, err=%v", len(records), err)
	}
	var metadata runMetadata
	encoded, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(traceBytes)
	if metadata.Schema != "etcd-hbase-m4-run/v1" || metadata.Status != "passed" ||
		metadata.EvidenceScope != "native-etcd" || !metadata.EmbeddedNative ||
		metadata.TraceSHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("M4 native metadata does not describe the trace: %#v", metadata)
	}

	copyPath := filepath.Join(artifact, "native-copy.jsonl")
	if err := os.WriteFile(copyPath, traceBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := compareLeaseTraces([]string{"--left", tracePath, "--right", copyPath}, io.Discard, io.Discard); err != nil {
		t.Fatalf("compare equivalent native traces: %v", err)
	}
}

func TestParseRunOptionsRejectsMissingLayoutAndUnscopedPrefix(t *testing.T) {
	base := []string{"--scenario", "suite.yaml", "--scope", "target-hbase",
		"--prefix", "etcd-hbase-m4-test/run/", "--endpoint", "127.0.0.1:2379",
		"--trace", "trace.jsonl", "--metadata", "metadata.json"}
	if _, err := parseRunOptions(base, io.Discard); err == nil {
		t.Fatal("target mode without a storage layout was accepted")
	}
	base[5] = "customer/production/"
	base = append(base, "--layout", "HISTORY")
	if _, err := parseRunOptions(base, io.Discard); err == nil {
		t.Fatal("unscoped M4 prefix was accepted")
	}
}

func TestCompareRejectsCorruptOrEmptyTrace(t *testing.T) {
	directory := t.TempDir()
	left := filepath.Join(directory, "left.jsonl")
	right := filepath.Join(directory, "right.jsonl")
	if err := os.WriteFile(left, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(right, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := compareLeaseTraces([]string{"--left", left, "--right", right}, io.Discard, io.Discard); err == nil {
		t.Fatal("incomplete M4 traces were accepted")
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("test source path is unavailable")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../../../.."))
}
