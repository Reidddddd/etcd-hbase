package scenario

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/compare"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"google.golang.org/grpc/codes"
)

func TestM4LeaseScenarioMatchesItsPinnedNativeOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var traces [][]trace.Record
	for _, name := range []string{"native-one", "native-two"} {
		var output bytes.Buffer
		if err := RunM4NativeLease(ctx, filepath.Join(t.TempDir(), name),
			"etcd-hbase-m4-test/isolated/", &output); err != nil {
			t.Fatalf("native M4 lease scenario failed: %v", err)
		}
		records, err := trace.ReadJSONL(bytes.NewReader(output.Bytes()))
		if err != nil {
			t.Fatalf("read native M4 lease trace: %v", err)
		}
		if len(records) != len(expectedM4LeaseSteps) {
			t.Fatalf("native M4 trace has %d records, want %d", len(records), len(expectedM4LeaseSteps))
		}
		for index, step := range expectedM4LeaseSteps {
			if records[index].CaseID != step.ID || records[index].EvidenceScope != "native-etcd" {
				t.Fatalf("record %d does not match the pinned M4 step: %#v", index, records[index])
			}
		}
		if records[5].Error == nil || records[5].Error.GRPCCode != codes.FailedPrecondition.String() {
			t.Fatal("duplicate lease ID did not preserve the native FailedPrecondition status")
		}
		if records[9].Error == nil || records[9].Error.GRPCCode != codes.OutOfRange.String() {
			t.Fatal("oversized lease TTL did not preserve the native OutOfRange status")
		}
		traces = append(traces, records)
	}
	if err := compare.CompareM4LeaseRecords(traces[0], traces[1]); err != nil {
		t.Fatalf("independent native M4 traces differ: %v; first Grant request=%s response=%s",
			err, traces[0][1].Request, traces[0][1].Response)
	}
}

func TestLoadM4LeaseSuiteRejectsMissingOrChangedCoverage(t *testing.T) {
	original, err := os.ReadFile("../../../scenarios/m4-lease.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{
		"",
		strings.Replace(string(original), "etcd-v3.5.23", "etcd-v3.6.0", 1),
		strings.Replace(string(original), "    id: negative-id\n", "", 1),
		string(original) + "unknown: true\n",
	} {
		path := filepath.Join(t.TempDir(), "suite.yaml")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := LoadM4LeaseSuite(path); err == nil {
			t.Fatal("modified M4 lease coverage was accepted")
		}
	}
}

func TestValidateM4LeaseRejectsUnscopedPrefixBeforeRPC(t *testing.T) {
	for _, input := range []struct{ scope, prefix string }{
		{"remote", "etcd-hbase-m4-test/run/"},
		{"target-hbase", "customer/service/"},
		{"target-hbase", "etcd-hbase-m4-test/../production/"},
		{"target-hbase", "etcd-hbase-m4-test/run"},
	} {
		if err := ValidateM4Lease(input.scope, input.prefix); err == nil {
			t.Fatalf("accepted unsafe M4 input %#v", input)
		}
	}
}
