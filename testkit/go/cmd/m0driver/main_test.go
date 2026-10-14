package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestRunRejectsEvidenceScopesOutsideTheGoDriver(t *testing.T) {
	for _, scope := range []string{"native-etcd", "target-hbase"} {
		t.Run(scope, func(t *testing.T) {
			err := run([]string{"--scope", scope}, &bytes.Buffer{}, &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), "scope must be transport or transport-fixture") {
				t.Fatalf("scope %q was not rejected: %v", scope, err)
			}
		})
	}
}

func TestSnapshotResponseTreatsTypedNilAsAbsent(t *testing.T) {
	var response *etcdserverpb.RangeResponse

	snapshot, err := snapshotResponse(response)
	if err != nil {
		t.Fatalf("snapshotResponse() error = %v", err)
	}
	if !json.Valid(snapshot) || string(snapshot) != "null" {
		t.Fatalf("snapshotResponse() = %s, want null", snapshot)
	}
}

func TestSnapshotResponseSerializesNonNilMessage(t *testing.T) {
	snapshot, err := snapshotResponse(&etcdserverpb.RangeResponse{Count: 3})
	if err != nil {
		t.Fatalf("snapshotResponse() error = %v", err)
	}
	if !json.Valid(snapshot) || !strings.Contains(string(snapshot), `"count":"3"`) {
		t.Fatalf("snapshotResponse() = %s, want Count 3", snapshot)
	}
}
