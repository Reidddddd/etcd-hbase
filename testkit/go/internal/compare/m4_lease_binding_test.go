package compare

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

func TestCompareM4LeaseBindingRecordsNormalizesTransportAndScenarioRelativeTtl(t *testing.T) {
	left, right := bindingTraceFixture(10), bindingTraceFixture(7)
	left[0].Response = bindingResponse(map[string]any{
		"header": bindingHeader("1", "11", "1"),
		"id":     "1",
	})
	right[0].Response = bindingResponse(map[string]any{
		"header": bindingHeader("99", "22", "1"),
		"id":     "1",
	})
	if err := CompareM4LeaseBindingRecords(left, right); err != nil {
		t.Fatalf("independent run timing skew was rejected: %v", err)
	}
}

func TestCompareM4LeaseBindingRecordsTreatsTtlKeysAsAnUnorderedSet(t *testing.T) {
	left, right := bindingTraceFixture(10), bindingTraceFixture(10)
	left[4].Response = bindingTtlResponse(10, []string{"cm9sZS9tZW1iZXI=", "cm9sZS9xdWVyeQ=="})
	right[4].Response = bindingTtlResponse(10, []string{"cm9sZS9xdWVyeQ==", "cm9sZS9tZW1iZXI="})
	if err := CompareM4LeaseBindingRecords(left, right); err != nil {
		t.Fatalf("equivalent LeaseTimeToLive key sets with different order were rejected: %v", err)
	}
}

func TestCompareM4LeaseBindingRecordsRejectsMissingOrDuplicateTtlKeys(t *testing.T) {
	t.Run("missing-key", func(t *testing.T) {
		left, right := bindingTraceFixture(10), bindingTraceFixture(10)
		right[4].Response = bindingTtlResponse(10, []string{"cm9sZS9tZW1iZXI="})
		if err := CompareM4LeaseBindingRecords(left, right); err == nil {
			t.Fatal("a missing committed Lease key was accepted")
		}
	})
	t.Run("duplicate-key", func(t *testing.T) {
		left, right := bindingTraceFixture(10), bindingTraceFixture(10)
		right[4].Response = bindingTtlResponse(10, []string{"cm9sZS9tZW1iZXI=", "cm9sZS9tZW1iZXI="})
		if err := CompareM4LeaseBindingRecords(left, right); err == nil {
			t.Fatal("a duplicate Lease key was accepted")
		}
	})
}

func TestCompareM4LeaseBindingRecordsRejectsRevisionAndPartialResult(t *testing.T) {
	t.Run("revision", func(t *testing.T) {
		left, right := bindingTraceFixture(10), bindingTraceFixture(10)
		left[0].Response = bindingResponse(map[string]any{"header": bindingHeader("1", "11", "1"), "id": "1"})
		right[0].Response = bindingResponse(map[string]any{"header": bindingHeader("1", "11", "2"), "id": "1"})
		if err := CompareM4LeaseBindingRecords(left, right); err == nil {
			t.Fatal("different response revisions were accepted")
		}
	})
	t.Run("partial-result", func(t *testing.T) {
		left, right := bindingTraceFixture(10), bindingTraceFixture(10)
		left[3].Response = bindingResponse(map[string]any{"succeeded": true})
		right[3].Response = bindingResponse(map[string]any{"succeeded": false})
		if err := CompareM4LeaseBindingRecords(left, right); err == nil {
			t.Fatal("different transaction outcomes were accepted")
		}
	})
}

func TestCompareM4LeaseBindingRecordsRejectsInvalidTtlAndUnexpectedRenewal(t *testing.T) {
	t.Run("expired", func(t *testing.T) {
		left, right := bindingTraceFixture(10), bindingTraceFixture(10)
		left[4].Response = bindingTtlResponse(0, bindingExpectedKeys("binding-ttl-a-initial"))
		if err := CompareM4LeaseBindingRecords(left, right); err == nil {
			t.Fatal("an expired Lease with a successful TTL response was accepted")
		}
	})
	t.Run("above-granted-ttl", func(t *testing.T) {
		left, right := bindingTraceFixture(10), bindingTraceFixture(10)
		left[4].Response = bindingTtlResponse(61, bindingExpectedKeys("binding-ttl-a-initial"))
		if err := CompareM4LeaseBindingRecords(left, right); err == nil {
			t.Fatal("remaining TTL above granted TTL was accepted")
		}
	})
	t.Run("unexpected-renewal", func(t *testing.T) {
		left, right := bindingTraceFixture(10), bindingTraceFixture(10)
		caseID := "binding-ttl-a-preserved"
		left[bindingCaseIndex(caseID)].Response = bindingTtlResponse(12, bindingExpectedKeys(caseID))
		if err := CompareM4LeaseBindingRecords(left, right); err == nil {
			t.Fatal("a multi-second TTL increase without KeepAlive was accepted")
		}
	})
}

func TestCompareM4LeaseBindingRequiresNotFoundForInvalidLeaseOperations(t *testing.T) {
	for _, caseID := range []string{"binding-invalid-lease-atomicity", "binding-invalid-lease-put"} {
		t.Run(caseID, func(t *testing.T) {
			left, right := bindingTraceFixture(10), bindingTraceFixture(10)
			index := bindingCaseIndex(caseID)
			wrongError := &trace.Error{GRPCCode: "Internal", ConsumerKind: "grpc-status"}
			left[index].Error = wrongError
			right[index].Error = wrongError
			if err := CompareM4LeaseBindingRecords(left, right); err == nil {
				t.Fatal("an invalid Lease operation without NotFound was accepted")
			}
		})
	}
}

func bindingTraceFixture(ttl int) []trace.Record {
	records := make([]trace.Record, len(m4LeaseBindingCases))
	for index, expected := range m4LeaseBindingCases {
		request := bindingSnapshot(map[string]any{"key": expected.id})
		var response json.RawMessage
		var callError *trace.Error
		if isM4InvalidLeaseCase(expected.id) {
			callError = &trace.Error{GRPCCode: "NotFound", ConsumerKind: "grpc-status", SemanticKind: "etcd/LeaseNotFound"}
		} else {
			fields := map[string]any{"header": bindingHeader("1", "11", "1")}
			if expected.method == "etcdserverpb.Lease/LeaseTimeToLive" {
				fields["TTL"] = strconv.Itoa(ttl)
				fields["grantedTTL"] = "60"
				fields["keys"] = bindingExpectedKeys(expected.id)
			}
			response = bindingSnapshot(fields)
		}
		records[index] = trace.Record{CaseID: expected.id, Method: expected.method,
			Request: request, Response: response, Error: callError}
	}
	return records
}

func bindingTtlResponse(ttl int, keys []string) json.RawMessage {
	return bindingResponse(map[string]any{
		"header":     bindingHeader("1", "11", "1"),
		"TTL":        strconv.Itoa(ttl),
		"grantedTTL": "60",
		"keys":       keys,
	})
}

func bindingExpectedKeys(caseID string) []string {
	switch caseID {
	case "binding-ttl-a-initial", "binding-ttl-a-preserved":
		return []string{"cm9sZS9tZW1iZXI=", "cm9sZS9xdWVyeQ=="}
	case "binding-ttl-b-initial":
		return []string{"cm9sZS9jb29yZGluYXRvcg=="}
	case "binding-ttl-a-after-rebind":
		return []string{"cm9sZS9xdWVyeQ=="}
	case "binding-ttl-b-after-rebind":
		return []string{"cm9sZS9tZW1iZXI=", "cm9sZS9jb29yZGluYXRvcg=="}
	case "binding-ttl-b-after-else":
		return []string{"cm9sZS9mYWxsYmFjaw==", "cm9sZS9tZW1iZXI=", "cm9sZS9jb29yZGluYXRvcg=="}
	case "binding-ttl-a-after-nested":
		return []string{"cm9sZS9xdWVyeQ==", "cm9sZS9uZXN0ZWQvbWVtYmVy"}
	case "binding-ttl-b-after-nested":
		return []string{"cm9sZS9mYWxsYmFjaw==", "cm9sZS9tZW1iZXI=", "cm9sZS9jb29yZGluYXRvcg==", "cm9sZS9uZXN0ZWQvcXVlcnk="}
	case "binding-ttl-b-after-delete", "binding-ttl-b-after-recreate":
		return []string{"cm9sZS9mYWxsYmFjaw==", "cm9sZS9jb29yZGluYXRvcg==", "cm9sZS9uZXN0ZWQvcXVlcnk="}
	case "binding-ttl-a-after-unbind":
		return []string{"cm9sZS9uZXN0ZWQvbWVtYmVy"}
	case "binding-ttl-b-after-survivor-put":
		return []string{"c3Vydml2b3IvcmVnaXN0ZXJlZA=="}
	case "binding-ttl-b-after-unselected-branch":
		return []string{"YXRvbWljL3Vuc2VsZWN0ZWQtZWxzZQ==", "c3Vydml2b3IvcmVnaXN0ZXJlZA=="}
	case "binding-ttl-a-after-delete-range", "binding-ttl-b-after-delete-range", "binding-ttl-a-after-failed-txn":
		return []string{}
	default:
		return nil
	}
}

func bindingCaseIndex(caseID string) int {
	for index, expected := range m4LeaseBindingCases {
		if expected.id == caseID {
			return index
		}
	}
	panic("unknown M4 lease binding case")
}

func bindingSnapshot(fields map[string]any) json.RawMessage {
	encoded, err := json.Marshal(map[string]any{"type": "fixture", "fields": fields})
	if err != nil {
		panic(err)
	}
	return encoded
}

func bindingResponse(fields map[string]any) json.RawMessage {
	return bindingSnapshot(fields)
}

func bindingHeader(cluster, member, revision string) map[string]any {
	return map[string]any{"type": "etcdserverpb.ResponseHeader", "fields": map[string]any{
		"clusterId": cluster, "memberId": member, "raftTerm": "1", "revision": revision,
	}}
}
