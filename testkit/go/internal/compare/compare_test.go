package compare

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"io.etcdhbase/testkit/internal/trace"
)

func TestCompareIgnoresOnlyElapsedIDsAndRelativeRevisionOffset(t *testing.T) {
	left := baselineTrace(11, 101, 202, 7)
	right := baselineTrace(41, 909, 808, 99)
	if err := Compare(left, right); err != nil {
		t.Fatalf("equivalent traces did not compare: %v", err)
	}
}

func TestCompareAllowsOnlyTheCountFieldForC05(t *testing.T) {
	left := baselineTrace(1, 101, 202, 7)
	right := baselineTrace(1, 101, 202, 7)
	right[1].Response = replace(right[1].Response, `"count":"2"`, `"count":"3"`)
	if err := Compare(left, right); err != nil {
		t.Fatalf("C05 Count difference was rejected: %v", err)
	}
	if !bytes.Contains(right[1].Response, []byte(`"count":"3"`)) {
		t.Fatal("Compare mutated the caller's trace while applying the Count allowance")
	}
	right[1].Response = replace(right[1].Response, `"count":"3"`, `"count":"-1"`)
	if err := Compare(left, right); err == nil {
		t.Fatal("C05 accepted a non-canonical uint64 Count difference")
	}
	right[1].Response = replace(right[1].Response, `"count":"-1"`, `"count":"3"`)
	right[1].Response = replace(right[1].Response, `"more":false`, `"more":true`)
	if err := Compare(left, right); err == nil {
		t.Fatal("C05 Count difference masked a More difference")
	}
}

func TestCompareRejectsDamagedProtocolTraces(t *testing.T) {
	cases := []struct {
		name   string
		mutate func([]trace.Record) []trace.Record
	}{
		{name: "watch event removed", mutate: func(records []trace.Record) []trace.Record {
			records[3].Response = raw(`{"fields":{"events":[]},"oneofs":{}}`)
			return records
		}},
		{name: "More changed", mutate: func(records []trace.Record) []trace.Record {
			records[1].Response = replace(records[1].Response, `"more":false`, `"more":true`)
			return records
		}},
		{name: "deleted changed", mutate: func(records []trace.Record) []trace.Record {
			records[2].Response = replace(records[2].Response, `"deleted":"1"`, `"deleted":"0"`)
			return records
		}},
		{name: "revision changed", mutate: func(records []trace.Record) []trace.Record {
			records[3].Response = replace(records[3].Response, `"modRevision":"3"`, `"modRevision":"4"`)
			return records
		}},
		{name: "transaction branch changed", mutate: func(records []trace.Record) []trace.Record {
			records[4].Response = replace(records[4].Response, `"succeeded":true`, `"succeeded":false`)
			return records
		}},
		{name: "PrevKV bytes changed", mutate: func(records []trace.Record) []trace.Record {
			records[2].Response = replace(records[2].Response, `"value":"YQ=="`, `"value":"Yg=="`)
			return records
		}},
		{name: "stream truncated", mutate: func(records []trace.Record) []trace.Record {
			return records[:len(records)-1]
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			left := baselineTrace(1, 101, 202, 7)
			right := test.mutate(baselineTrace(1, 101, 202, 7))
			if err := Compare(left, right); err == nil {
				t.Fatal("damaged trace was accepted")
			}
		})
	}
}

func TestCompareMismatchDoesNotEchoTraceIdentifiers(t *testing.T) {
	const sentinel = "secret-value-must-not-appear"
	left := baselineTrace(1, 101, 202, 7)
	right := baselineTrace(1, 101, 202, 7)
	right[1].CaseID = sentinel
	right[1].Method = "etcdserverpb.KV/" + sentinel
	err := Compare(left, right)
	if err == nil {
		t.Fatal("trace mismatch was accepted")
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatalf("comparison error leaked an untrusted identifier: %v", err)
	}
}

func TestCompareTracesRejectsMalformedJSONL(t *testing.T) {
	if err := CompareTraces(strings.NewReader("{}\n"), strings.NewReader("{}\n")); err == nil {
		t.Fatal("invalid JSONL trace was accepted")
	}
}

func TestNormalizeTraceRetainsInspectableRevisionAndIDMappings(t *testing.T) {
	left := encodeTrace(t, baselineTrace(11, 101, 202, 7))
	right := encodeTrace(t, baselineTrace(41, 909, 808, 99))
	leftNormalized := new(bytes.Buffer)
	rightNormalized := new(bytes.Buffer)
	if err := NormalizeTrace(bytes.NewReader(left), leftNormalized); err != nil {
		t.Fatal(err)
	}
	if err := NormalizeTrace(bytes.NewReader(right), rightNormalized); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(leftNormalized.Bytes(), rightNormalized.Bytes()) {
		t.Fatal("normalized traces differ")
	}
	if bytes.Contains(leftNormalized.Bytes(), []byte(`"elapsed_ns"`)) || !bytes.Contains(leftNormalized.Bytes(), []byte(`"memberId":"member:1"`)) {
		t.Fatalf("normalized trace does not expose only the approved mapping: %s", leftNormalized.Bytes())
	}
}

func baselineTrace(base, clusterID, memberID, leaseID int64) []trace.Record {
	status := trace.Record{
		Schema: trace.SchemaVersion, CaseID: "native-baseline", Ordinal: 1,
		Method: "etcdserverpb.Maintenance/Status", Request: raw(`{"fields":{},"oneofs":{}}`),
		Response:      raw(`{"fields":{"header":{"fields":{"clusterId":"` + i64(clusterID) + `","memberId":"` + i64(memberID) + `","revision":"` + i64(base) + `"},"oneofs":{}}},"oneofs":{}}`),
		EvidenceScope: "native-etcd", ElapsedNS: "100",
	}
	rangeRecord := trace.Record{
		Schema: trace.SchemaVersion, CaseID: "C05", Ordinal: 1,
		Method: "etcdserverpb.KV/Range", Request: raw(`{"fields":{"rangeEnd":"cA==","countOnly":true,"limit":"1"},"oneofs":{}}`),
		Response:      raw(`{"fields":{"count":"2","kvs":[],"more":false,"header":{"fields":{"clusterId":"` + i64(clusterID) + `","memberId":"` + i64(memberID) + `","revision":"` + i64(base+1) + `"},"oneofs":{}}},"oneofs":{}}`),
		EvidenceScope: "native-etcd", ElapsedNS: "101",
	}
	deleteRecord := trace.Record{
		Schema: trace.SchemaVersion, CaseID: "delete-prevkv", Ordinal: 1,
		Method: "etcdserverpb.KV/DeleteRange", Request: raw(`{"fields":{"key":"YQ=="},"oneofs":{}}`),
		Response:      raw(`{"fields":{"deleted":"1","prevKvs":[{"fields":{"key":"YQ==","value":"YQ==","createRevision":"` + i64(base+1) + `","modRevision":"` + i64(base+1) + `"},"oneofs":{}}],"header":{"fields":{"clusterId":"` + i64(clusterID) + `","memberId":"` + i64(memberID) + `","revision":"` + i64(base+2) + `"},"oneofs":{}}},"oneofs":{}}`),
		EvidenceScope: "native-etcd", ElapsedNS: "102",
	}
	watchRecord := trace.Record{
		Schema: trace.SchemaVersion, CaseID: "watch-r-plus-1", Ordinal: 1,
		Method: "etcdserverpb.Watch/Watch", Request: raw(`{"fields":{"createRequest":{"fields":{"key":"dw==","startRevision":"` + i64(base+2) + `"},"oneofs":{}}},"oneofs":{"request_union":"create_request"}}`),
		Response:      raw(`{"fields":{"events":[{"fields":{"type":0,"kv":{"fields":{"key":"dw==","value":"YQ==","modRevision":"` + i64(base+2) + `"},"oneofs":{}}},"oneofs":{}}],"header":{"fields":{"clusterId":"` + i64(clusterID) + `","memberId":"` + i64(memberID) + `","revision":"` + i64(base+2) + `"},"oneofs":{}}},"oneofs":{}}`),
		EvidenceScope: "native-etcd", ElapsedNS: "103",
	}
	txnRecord := trace.Record{
		Schema: trace.SchemaVersion, CaseID: "txn-branch", Ordinal: 1,
		Method: "etcdserverpb.KV/Txn", Request: raw(`{"fields":{},"oneofs":{}}`),
		Response:      raw(`{"fields":{"succeeded":true,"responses":[],"header":{"fields":{"clusterId":"` + i64(clusterID) + `","memberId":"` + i64(memberID) + `","revision":"` + i64(base+3) + `"},"oneofs":{}}},"oneofs":{}}`),
		EvidenceScope: "native-etcd", ElapsedNS: "104",
	}
	leaseRecord := trace.Record{
		Schema: trace.SchemaVersion, CaseID: "lease-keepalive", Ordinal: 1,
		Method: "etcdserverpb.Lease/LeaseKeepAlive", Request: raw(`{"fields":{"ID":"` + i64(leaseID) + `"},"oneofs":{}}`),
		Response:      raw(`{"fields":{"ID":"` + i64(leaseID) + `","TTL":"5"},"oneofs":{}}`),
		EvidenceScope: "native-etcd", ElapsedNS: "105",
	}
	terminal := trace.Record{
		Schema: trace.SchemaVersion, CaseID: "watch-r-plus-1", Ordinal: 2,
		Method: "etcdserverpb.Watch/Watch", Request: watchRecord.Request, Response: raw("null"),
		StreamTerminal: &trace.StreamTerminal{Kind: "client-close"}, EvidenceScope: "native-etcd", ElapsedNS: "106",
	}
	return []trace.Record{status, rangeRecord, deleteRecord, watchRecord, txnRecord, leaseRecord, terminal}
}

func raw(value string) json.RawMessage { return json.RawMessage(value) }

func replace(rawMessage json.RawMessage, old, replacement string) json.RawMessage {
	return json.RawMessage(strings.Replace(string(rawMessage), old, replacement, 1))
}

func i64(value int64) string { return strconv.FormatInt(value, 10) }

func encodeTrace(t *testing.T, records []trace.Record) []byte {
	t.Helper()
	var buffer bytes.Buffer
	for _, record := range records {
		if err := trace.WriteJSONL(&buffer, record); err != nil {
			t.Fatal(err)
		}
	}
	return buffer.Bytes()
}
