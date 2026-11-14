package compare

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

func TestCompareM4LeaseAllowsConfiguredMinimumAndAtMostOneSecondRemainingTTLSkew(t *testing.T) {
	native := m4LeaseFixture("native-etcd", 2, 1, 29, 2, 11, 21)
	target := m4LeaseFixture("target-hbase", 5, 2, 28, 0, 91, 92)
	if err := CompareM4LeaseRecords(native, target); err != nil {
		t.Fatalf("M4 Lease comparison rejected its documented TTL differences: %v", err)
	}
}

func TestCompareM4LeaseRejectsRemainingTTLSkewOverOneSecond(t *testing.T) {
	native := m4LeaseFixture("native-etcd", 2, 2, 30, 2, 11, 21)
	target := m4LeaseFixture("target-hbase", 5, 4, 30, 0, 91, 92)
	if err := CompareM4LeaseRecords(native, target); err == nil {
		t.Fatal("M4 Lease comparison accepted a two-second remaining-TTL difference")
	}
}

func TestCompareM4LeaseKeepsStatusAndErrorContractsStrict(t *testing.T) {
	t.Run("listed IDs", func(t *testing.T) {
		native := m4LeaseFixture("native-etcd", 2, 2, 30, 2, 11, 21)
		target := m4LeaseFixture("target-hbase", 5, 2, 30, 0, 91, 92)
		target[8].Response = m4Snapshot(map[string]any{
			"header": m4Header("901", "902", "0"),
			"leases": []any{
				m4Status("901"), m4Status("203"), m4Status("-303"),
			},
		})
		if err := CompareM4LeaseRecords(native, target); err == nil {
			t.Fatal("M4 Lease comparison accepted a different LeaseLeases result")
		}
	})

	t.Run("duplicate ID error", func(t *testing.T) {
		native := m4LeaseFixture("native-etcd", 2, 2, 30, 2, 11, 21)
		target := m4LeaseFixture("target-hbase", 5, 2, 30, 0, 91, 92)
		target[5].Error = &trace.Error{
			GRPCCode: "AlreadyExists", ConsumerKind: "etcd-client-error", SemanticKind: "etcd/LeaseExist",
		}
		if err := CompareM4LeaseRecords(native, target); err == nil {
			t.Fatal("M4 Lease comparison accepted a different duplicate-ID error")
		}
	})
}

func m4LeaseFixture(scope string, minimumTTL, autoRemainingTTL, explicitRemainingTTL int64,
	raftTerm, clusterID, memberID int64) []trace.Record {
	autoID := int64(101)
	if scope == "target-hbase" {
		autoID = 901
	}
	explicitID := int64(202)
	negativeID := int64(-303)
	header := func() map[string]any {
		return m4Header(
			jsonInt64(clusterID), jsonInt64(memberID), jsonInt64(raftTerm),
		)
	}
	response := func(fields map[string]any) json.RawMessage {
		fields["header"] = header()
		return m4Snapshot(fields)
	}
	record := func(caseID, method string, request, responseBytes json.RawMessage,
		callError *trace.Error) trace.Record {
		return trace.Record{
			Schema: trace.SchemaVersion, CaseID: caseID, Ordinal: 1, Method: method,
			Request: request, Response: responseBytes, Error: callError,
			EvidenceScope: scope, ElapsedNS: "1",
		}
	}
	grantMethod := "etcdserverpb.Lease/LeaseGrant"
	ttlMethod := "etcdserverpb.Lease/LeaseTimeToLive"
	listMethod := "etcdserverpb.Lease/LeaseLeases"
	return []trace.Record{
		record("native-baseline", listMethod,
			m4Snapshot(map[string]any{}), response(map[string]any{"leases": []any{}}), nil),
		record("auto-grant", grantMethod,
			m4Snapshot(map[string]any{"ID": "0", "TTL": "0"}),
			response(map[string]any{"ID": jsonInt64(autoID), "TTL": jsonInt64(minimumTTL), "error": ""}), nil),
		record("auto-ttl", ttlMethod,
			m4Snapshot(map[string]any{"ID": jsonInt64(autoID), "keys": true}),
			response(map[string]any{"ID": jsonInt64(autoID), "TTL": jsonInt64(autoRemainingTTL),
				"GrantedTTL": jsonInt64(minimumTTL), "Keys": []any{}}), nil),
		record("explicit-grant", grantMethod,
			m4Snapshot(map[string]any{"ID": jsonInt64(explicitID), "TTL": "30"}),
			response(map[string]any{"ID": jsonInt64(explicitID), "TTL": "30", "error": ""}), nil),
		record("explicit-ttl", ttlMethod,
			m4Snapshot(map[string]any{"ID": jsonInt64(explicitID), "keys": true}),
			response(map[string]any{"ID": jsonInt64(explicitID), "TTL": jsonInt64(explicitRemainingTTL),
				"GrantedTTL": "30", "Keys": []any{}}), nil),
		record("duplicate-id", grantMethod,
			m4Snapshot(map[string]any{"ID": jsonInt64(explicitID), "TTL": "30"}), json.RawMessage("null"),
			&trace.Error{GRPCCode: "FailedPrecondition", ConsumerKind: "etcd-client-error", SemanticKind: "etcd/LeaseExist"}),
		record("missing-ttl", ttlMethod,
			m4Snapshot(map[string]any{"ID": "404", "keys": true}),
			response(map[string]any{"ID": "404", "TTL": "-1", "GrantedTTL": "0", "Keys": []any{}}), nil),
		record("negative-id", grantMethod,
			m4Snapshot(map[string]any{"ID": jsonInt64(negativeID), "TTL": "30"}),
			response(map[string]any{"ID": jsonInt64(negativeID), "TTL": "30", "error": ""}), nil),
		record("leases-list", listMethod,
			m4Snapshot(map[string]any{}), response(map[string]any{"leases": []any{
				m4Status(jsonInt64(autoID)), m4Status(jsonInt64(explicitID)), m4Status(jsonInt64(negativeID)),
			}}), nil),
		record("oversized-ttl", grantMethod,
			m4Snapshot(map[string]any{"ID": "0", "TTL": "9000000001"}), json.RawMessage("null"),
			&trace.Error{GRPCCode: "OutOfRange", ConsumerKind: "etcd-client-error", SemanticKind: "etcd/LeaseTTLTooLarge"}),
		record("zero-id-ttl", ttlMethod,
			m4Snapshot(map[string]any{"ID": "0", "keys": true}),
			response(map[string]any{"ID": "0", "TTL": "-1", "GrantedTTL": "0", "Keys": []any{}}), nil),
	}
}

func m4Status(id string) map[string]any {
	return map[string]any{"fields": map[string]any{"ID": id}, "oneofs": map[string]any{}}
}

func m4Header(clusterID, memberID, raftTerm string) map[string]any {
	return map[string]any{"fields": map[string]any{
		"clusterId": clusterID, "memberId": memberID, "raftTerm": raftTerm, "revision": "1",
	}, "oneofs": map[string]any{}}
}

func m4Snapshot(fields map[string]any) json.RawMessage {
	encoded, _ := json.Marshal(map[string]any{"fields": fields, "oneofs": map[string]any{}})
	return encoded
}

func jsonInt64(value int64) string {
	return strconv.FormatInt(value, 10)
}
