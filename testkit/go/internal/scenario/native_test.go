package scenario

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"io.etcdhbase/testkit/internal/compare"
	"io.etcdhbase/testkit/internal/trace"
)

func TestNativeScenarioRunsTwiceAndPreservesExpectedEvidence(t *testing.T) {
	suitePath := filepath.Join("..", "..", "..", "..", "testkit", "scenarios", "m0-native.yaml")
	suite, err := LoadSuite(suitePath)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	firstTrace := new(bytes.Buffer)
	metadata, err := Run(testContext(t), suite, filepath.Join(root, "first"), firstTrace)
	if err != nil {
		t.Fatalf("first native run: %v", err)
	}
	if metadata.Protocol != protocolVersion || metadata.SourceRevision != sourceRevision || len(metadata.Modules) != 3 {
		t.Fatalf("unexpected native build metadata: %#v", metadata)
	}
	firstRecords, err := trace.ReadJSONL(bytes.NewReader(firstTrace.Bytes()))
	if err != nil {
		t.Fatalf("read first native trace: %v", err)
	}
	assertNativeEvidence(t, firstRecords)

	secondTrace := new(bytes.Buffer)
	if _, err := Run(testContext(t), suite, filepath.Join(root, "second"), secondTrace); err != nil {
		t.Fatalf("second native run: %v", err)
	}
	secondRecords, err := trace.ReadJSONL(bytes.NewReader(secondTrace.Bytes()))
	if err != nil {
		t.Fatalf("read second native trace: %v", err)
	}
	if err := compare.Compare(firstRecords, secondRecords); err != nil {
		t.Fatalf("independent native traces differ: %v", err)
	}
}

func TestLoadSuiteRejectsChangedProtocolOrSteps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scenario.yaml")
	content := "schema_version: 1\nid: m0-native-v1\nprotocol: etcd-v3.5.22\nsteps: []\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSuite(path); err == nil {
		t.Fatal("scenario with a different protocol was accepted")
	}
}

func TestRunRejectsChangedInMemorySteps(t *testing.T) {
	suite := Suite{
		SchemaVersion: 1,
		ID:            "m0-native-v1",
		Protocol:      protocolVersion,
		Steps:         append([]Step(nil), expectedSteps...),
	}
	suite.Steps[0].ID = "unreviewed-step"
	if _, err := Run(testContext(t), suite, filepath.Join(t.TempDir(), "must-not-start"), &bytes.Buffer{}); err == nil {
		t.Fatal("Run accepted steps that differ from the pinned native manifest")
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func assertNativeEvidence(t *testing.T, records []trace.Record) {
	t.Helper()
	var sawMore, sawCountOnly, sawTxnSuccess, sawTxnFailure, sawPrevKV, sawLeaseKeepAlive, sawRevoke, sawWatchCreated, sawWatchEvent, sawWatchTerminal, sawCompactionError bool
	for _, record := range records {
		if record.EvidenceScope != "native-etcd" {
			t.Fatalf("record %s has wrong evidence scope %q", record.CaseID, record.EvidenceScope)
		}
		switch record.CaseID {
		case "C03":
			if response := decodeSnapshot(t, record.Response); responseBool(response, "more") {
				sawMore = true
			}
		case "C05":
			request := decodeSnapshot(t, record.Request)
			response := decodeSnapshot(t, record.Response)
			count, countOK := response["count"].(string)
			more, moreOK := response["more"].(bool)
			if requestBool(request, "countOnly") && countOK && count == "3" && moreOK && !more && responseArrayLen(response, "kvs") == 0 {
				sawCountOnly = true
			}
		case "C11":
			sawTxnSuccess = responseBool(decodeSnapshot(t, record.Response), "succeeded")
		case "C12":
			sawTxnFailure = !responseBool(decodeSnapshot(t, record.Response), "succeeded")
		case "C09":
			if record.Method == "etcdserverpb.KV/DeleteRange" {
				sawPrevKV = responseArrayLen(decodeSnapshot(t, record.Response), "prevKvs") == 1
			}
		case "C17":
			if record.StreamTerminal != nil && record.StreamTerminal.Kind == "eof" {
				sawLeaseKeepAlive = true
			}
		case "C18":
			if record.Method == "etcdserverpb.Lease/LeaseRevoke" && record.Error == nil {
				sawRevoke = true
			}
		case "C19":
			if record.Response != nil && len(record.Response) > 0 && string(record.Response) != "null" {
				response := decodeSnapshot(t, record.Response)
				if responseBool(response, "created") {
					watchID, watchIDOK := response["watchId"].(string)
					sawWatchCreated = watchIDOK && watchID == "0" && response["header"] != nil
				}
				if events, ok := response["events"].([]any); ok && len(events) == 1 {
					request := decodeSnapshot(t, record.Request)
					create := snapshotChildFields(request, "createRequest")
					event, _ := events[0].(map[string]any)
					eventFields, _ := event["fields"].(map[string]any)
					keyValue := snapshotChildFields(eventFields, "kv")
					header := snapshotChildFields(response, "header")
					startRevision, startOK := create["startRevision"].(string)
					modRevision, modOK := keyValue["modRevision"].(string)
					headerRevision, headerOK := header["revision"].(string)
					if startOK && modOK && headerOK && startRevision == modRevision && modRevision == headerRevision &&
						keyValue["value"] == "YWZ0ZXItd2F0Y2gtcmV2aXNpb24=" {
						sawWatchEvent = true
					}
				}
			}
			if record.StreamTerminal != nil && record.StreamTerminal.Kind == "context-canceled" {
				sawWatchTerminal = true
			}
		case "C06":
			if record.Error != nil && record.Error.GRPCCode == "OutOfRange" {
				sawCompactionError = true
			}
		}
	}
	checks := map[string]bool{
		"Limit/More": sawMore, "CountOnly": sawCountOnly, "Txn success branch": sawTxnSuccess,
		"Txn failure branch": sawTxnFailure, "Delete PrevKV": sawPrevKV, "LeaseKeepAlive EOF": sawLeaseKeepAlive,
		"LeaseRevoke": sawRevoke, "Watch created response": sawWatchCreated, "Watch event": sawWatchEvent,
		"Watch cancellation terminal": sawWatchTerminal, "compacted revision error": sawCompactionError,
	}
	for name, passed := range checks {
		if !passed {
			t.Errorf("native trace is missing %s evidence", name)
		}
	}
}

func decodeSnapshot(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var snapshot map[string]any
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	fields, ok := snapshot["fields"].(map[string]any)
	if !ok {
		t.Fatalf("snapshot fields missing: %s", raw)
	}
	return fields
}

func responseBool(fields map[string]any, key string) bool {
	value, _ := fields[key].(bool)
	return value
}

func requestBool(fields map[string]any, key string) bool {
	value, _ := fields[key].(bool)
	return value
}

func responseArrayLen(fields map[string]any, key string) int {
	value, _ := fields[key].([]any)
	return len(value)
}

func snapshotChildFields(fields map[string]any, key string) map[string]any {
	snapshot, _ := fields[key].(map[string]any)
	child, _ := snapshot["fields"].(map[string]any)
	return child
}
