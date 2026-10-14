package trace

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
)

func TestSnapshotProtoPreservesBinaryAndInt64Values(t *testing.T) {
	request := &etcdserverpb.RangeRequest{
		Key:      []byte{0x61, 0x00, 0xff},
		RangeEnd: []byte{},
		Revision: 1 << 54,
	}
	snapshot, err := SnapshotProto(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Fields map[string]any `json:"fields"`
	}
	if err := json.Unmarshal(snapshot, &decoded); err != nil {
		t.Fatal(err)
	}
	if got, want := decoded.Fields["key"], base64.StdEncoding.EncodeToString(request.Key); got != want {
		t.Fatalf("key snapshot = %v, want base64 %q", got, want)
	}
	if got, want := decoded.Fields["rangeEnd"], ""; got != want {
		t.Fatalf("empty range_end snapshot = %v, want empty base64 string", got)
	}
	if got, want := decoded.Fields["revision"], "18014398509481984"; got != want {
		t.Fatalf("revision snapshot = %v, want %q", got, want)
	}
}

func TestSnapshotProtoPreservesTxnOneofAndPrevKV(t *testing.T) {
	request := &etcdserverpb.RequestOp{
		Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte{0x00, 0xff}},
		},
	}
	snapshot, err := SnapshotProto(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Oneofs map[string]string `json:"oneofs"`
	}
	if err := json.Unmarshal(snapshot, &decoded); err != nil {
		t.Fatal(err)
	}
	if got, want := decoded.Oneofs["request"], "request_range"; got != want {
		t.Fatalf("selected Txn oneof = %q, want %q", got, want)
	}

	response := &mvccpb.Event{
		Type: mvccpb.DELETE,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("current"),
			Value:          []byte{},
			CreateRevision: 1 << 54,
			ModRevision:    1 << 55,
			Version:        2,
		},
		PrevKv: &mvccpb.KeyValue{Key: []byte("current"), Value: []byte{0, 0xff}, ModRevision: 9},
	}
	snapshot, err = SnapshotProto(response)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(snapshot, []byte(`"prevKv"`)) || !bytes.Contains(snapshot, []byte(`"18014398509481984"`)) || !bytes.Contains(snapshot, []byte(`"36028797018963968"`)) {
		t.Fatalf("event snapshot omitted PrevKV or large revisions: %s", snapshot)
	}
	if !bytes.Contains(snapshot, []byte(`"value":""`)) {
		t.Fatalf("event snapshot omitted empty value: %s", snapshot)
	}
}

func TestSnapshotProtoPreservesWatchAndLeaseStreamValues(t *testing.T) {
	watchRequest := &etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte{0x00, 0xff}, StartRevision: 1 << 55, PrevKv: true},
		},
	}
	requestSnapshot, err := SnapshotProto(watchRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(requestSnapshot, []byte(`"request_union":"create_request"`)) || !bytes.Contains(requestSnapshot, []byte(`"startRevision":"36028797018963968"`)) {
		t.Fatalf("watch request snapshot lost oneof or revision: %s", requestSnapshot)
	}
	watchResponse := &etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{Revision: 1 << 55},
		WatchId: 1 << 54,
		Events: []*mvccpb.Event{
			{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("first"), Value: []byte{0, 0xff}}},
			{Type: mvccpb.DELETE, PrevKv: &mvccpb.KeyValue{Key: []byte("second"), ModRevision: 7}},
		},
	}
	responseSnapshot, err := SnapshotProto(watchResponse)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(responseSnapshot, []byte(`"watchId":"18014398509481984"`)) || !bytes.Contains(responseSnapshot, []byte(`"events"`)) || !bytes.Contains(responseSnapshot, []byte(`"36028797018963968"`)) {
		t.Fatalf("watch response snapshot lost stream events or 64-bit values: %s", responseSnapshot)
	}

	leaseSnapshot, err := SnapshotProto(&etcdserverpb.LeaseKeepAliveResponse{ID: 1 << 54, TTL: 1 << 53})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(leaseSnapshot, []byte(`"ID":"18014398509481984"`)) || !bytes.Contains(leaseSnapshot, []byte(`"TTL":"9007199254740992"`)) {
		t.Fatalf("lease stream snapshot lost 64-bit fields: %s", leaseSnapshot)
	}
}

func TestAuthenticateSnapshotsOnlyRecordPresence(t *testing.T) {
	request, err := SnapshotProto(&etcdserverpb.AuthenticateRequest{Name: "user-secret", Password: "password-secret"})
	if err != nil {
		t.Fatal(err)
	}
	response, err := SnapshotProto(&etcdserverpb.AuthenticateResponse{Token: "token-secret"})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"user-secret", "password-secret", "token-secret"} {
		if bytes.Contains(request, []byte(secret)) || bytes.Contains(response, []byte(secret)) {
			t.Fatalf("authentication trace contains secret %q", secret)
		}
	}
	if !bytes.Contains(request, []byte(`"name_present":true`)) || !bytes.Contains(request, []byte(`"password_present":true`)) {
		t.Fatalf("authentication request lost field-presence evidence: %s", request)
	}
	if !bytes.Contains(response, []byte(`"token_present":true`)) {
		t.Fatalf("authentication response lost token-presence evidence: %s", response)
	}
	authRecord := testRecord("authenticate", 1)
	authRecord.Method = "etcdserverpb.Auth/Authenticate"
	authRecord.Request = request
	authRecord.Response = response
	if err := WriteJSONL(&bytes.Buffer{}, authRecord); err != nil {
		t.Fatalf("sanitized authentication record was rejected: %v", err)
	}
}

func TestAuthAdministrationSnapshotsDoNotExposeIdentityOrPassword(t *testing.T) {
	request, err := SnapshotProto(&etcdserverpb.AuthUserAddRequest{Name: "user-secret", Password: "password-secret", HashedPassword: "hash-secret"})
	if err != nil {
		t.Fatal(err)
	}
	users, err := SnapshotProto(&etcdserverpb.AuthUserListResponse{Users: []string{"alice-secret", "bob-secret"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"user-secret", "password-secret", "hash-secret", "alice-secret", "bob-secret"} {
		if bytes.Contains(request, []byte(secret)) || bytes.Contains(users, []byte(secret)) {
			t.Fatalf("auth administration trace contains identity or password %q", secret)
		}
	}
	if !bytes.Contains(request, []byte(`"name_present":true`)) || !bytes.Contains(request, []byte(`"password_present":true`)) || !bytes.Contains(users, []byte(`"users_count":2`)) {
		t.Fatalf("auth administration snapshot lost presence/count evidence: request=%s users=%s", request, users)
	}
}

func TestSnapshotProtoRejectsUnknownFields(t *testing.T) {
	request := &etcdserverpb.RangeRequest{}
	unknown := reflect.ValueOf(request).Elem().FieldByName("XXX_unrecognized")
	unknown.SetBytes([]byte{0x98, 0x06, 0x01})
	if _, err := SnapshotProto(request); err == nil {
		t.Fatal("unknown protobuf fields should fail rather than disappear")
	}
}

func TestJSONLRoundTripPreservesOrderAndRejectsTruncatedRecords(t *testing.T) {
	first := testRecord("bytes", 1)
	first.Request = json.RawMessage(`{"fields":{"key":"YQD/"},"oneofs":{}}`)
	first.Response = json.RawMessage(`{"fields":{"count":"9007199254740993"},"oneofs":{}}`)
	second := testRecord("bytes", 2)
	var output bytes.Buffer
	if err := WriteJSONL(&output, first); err != nil {
		t.Fatal(err)
	}
	if err := WriteJSONL(&output, second); err != nil {
		t.Fatal(err)
	}
	got, err := ReadJSONL(&output)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Ordinal != 1 || got[1].Ordinal != 2 {
		t.Fatalf("records were not preserved in order: %#v", got)
	}
	if got[0].Response == nil || !bytes.Contains(got[0].Response, []byte(`"9007199254740993"`)) {
		t.Fatalf("large decimal value was not preserved: %s", got[0].Response)
	}
	if _, err := ReadJSONL(strings.NewReader("{\"schema\":")); err == nil {
		t.Fatal("truncated JSONL record should fail")
	}
}

func TestRecordRejectsMalformedProtobufSnapshotEnvelopes(t *testing.T) {
	malformed := []struct {
		name     string
		snapshot json.RawMessage
	}{
		{name: "missing fields", snapshot: json.RawMessage(`{"oneofs":{}}`)},
		{name: "missing oneofs", snapshot: json.RawMessage(`{"fields":{}}`)},
		{name: "fields are not an object", snapshot: json.RawMessage(`{"fields":[],"oneofs":{}}`)},
		{name: "oneofs are not an object", snapshot: json.RawMessage(`{"fields":{},"oneofs":[]}`)},
		{name: "unexpected envelope field", snapshot: json.RawMessage(`{"fields":{},"oneofs":{},"extra":true}`)},
		{name: "oneof selection is not a string", snapshot: json.RawMessage(`{"fields":{},"oneofs":{"request":1}}`)},
	}

	for _, test := range malformed {
		t.Run(test.name, func(t *testing.T) {
			record := testRecord("malformed-snapshot", 1)
			record.Request = test.snapshot
			if err := record.Validate(); err == nil {
				t.Fatal("record accepted a malformed request snapshot envelope")
			}
			encoded, err := json.Marshal(record)
			if err != nil {
				t.Fatalf("marshal malformed record: %v", err)
			}
			if _, err := ReadJSONL(bytes.NewReader(append(encoded, '\n'))); err == nil {
				t.Fatal("trace reader accepted a malformed request snapshot envelope")
			}

			record = testRecord("malformed-snapshot", 1)
			record.Response = test.snapshot
			if err := record.Validate(); err == nil {
				t.Fatal("record accepted a malformed response snapshot envelope")
			}
		})
	}
}

func TestWriteJSONLRejectsRecordLargerThanReaderLineLimit(t *testing.T) {
	record := testRecord("bounded-line", 1)
	var output bytes.Buffer
	if err := writeJSONL(&output, record, 100); err == nil {
		t.Fatal("writer accepted a record larger than the configured line limit")
	}
	if output.Len() != 0 {
		t.Fatalf("writer emitted a partial record before rejecting it: %d bytes", output.Len())
	}
}

func TestReadJSONLBoundsTotalBytesAndRecordCount(t *testing.T) {
	var output bytes.Buffer
	for ordinal := uint64(1); ordinal <= 2; ordinal++ {
		if err := WriteJSONL(&output, testRecord("bounded-trace", ordinal)); err != nil {
			t.Fatal(err)
		}
	}
	encoded := output.String()

	if _, err := readJSONL(strings.NewReader(encoded), int64(output.Len()), 2); err != nil {
		t.Fatalf("reader rejected a trace exactly at both limits: %v", err)
	}
	if _, err := readJSONL(strings.NewReader(encoded), int64(output.Len()-1), 2); err == nil || !strings.Contains(err.Error(), "maximum total size") {
		t.Fatalf("reader did not reject a trace above the total byte limit: %v", err)
	}
	if _, err := readJSONL(strings.NewReader(encoded), int64(output.Len()), 1); err == nil || !strings.Contains(err.Error(), "maximum record count") {
		t.Fatalf("reader did not reject a trace above the record count limit: %v", err)
	}
}

func TestTraceValidationErrorsDoNotEchoUntrustedIdentifiersOrFieldNames(t *testing.T) {
	const sentinel = "secret-value-must-not-appear"

	invalidCase := testRecord(sentinel+"!", 1)
	if err := invalidCase.Validate(); err == nil || strings.Contains(err.Error(), sentinel) {
		t.Fatalf("case_id validation leaked an untrusted value: %v", err)
	}
	invalidMethod := testRecord("safe-case", 1)
	invalidMethod.Method = sentinel
	if err := invalidMethod.Validate(); err == nil || strings.Contains(err.Error(), sentinel) {
		t.Fatalf("method validation leaked an untrusted value: %v", err)
	}

	secretField := testRecord("safe-case", 1)
	secretField.Request = json.RawMessage(`{"fields":{"password-` + sentinel + `":"x"},"oneofs":{}}`)
	if err := secretField.Validate(); err == nil || strings.Contains(err.Error(), sentinel) {
		t.Fatalf("secret-field validation leaked an untrusted field name: %v", err)
	}

	duplicateKey := testRecord("safe-case", 1)
	duplicateKey.Request = json.RawMessage(`{"fields":{"` + sentinel + `":1,"` + sentinel + `":2},"oneofs":{}}`)
	if err := duplicateKey.Validate(); err == nil || strings.Contains(err.Error(), sentinel) {
		t.Fatalf("duplicate-key validation leaked an untrusted field name: %v", err)
	}
}

func TestReadJSONLRejectsInvalidOrderingAndSecrets(t *testing.T) {
	var output bytes.Buffer
	if err := WriteJSONL(&output, testRecord("ordered", 2)); err != nil {
		t.Fatal(err)
	}
	if err := WriteJSONL(&output, testRecord("ordered", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadJSONL(&output); err == nil {
		t.Fatal("decreasing ordinals should fail")
	}

	secret := testRecord("secret", 1)
	secret.Request = json.RawMessage(`{"fields":{"password":"do-not-write"},"oneofs":{}}`)
	if err := WriteJSONL(&bytes.Buffer{}, secret); err == nil {
		t.Fatal("trace writer accepted a password field")
	}
	authName := testRecord("auth", 1)
	authName.Method = "etcdserverpb.Auth/Authenticate"
	authName.Request = json.RawMessage(`{"fields":{"name":"do-not-write"},"oneofs":{}}`)
	if err := WriteJSONL(&bytes.Buffer{}, authName); err == nil {
		t.Fatal("trace writer accepted an authentication name")
	}
}

func TestTraceRejectsDuplicateJSONKeys(t *testing.T) {
	secret := testRecord("duplicate-secret", 1)
	secret.Request = json.RawMessage(`{"fields":{"password":"do-not-write"},"fields":{},"oneofs":{}}`)
	if err := WriteJSONL(&bytes.Buffer{}, secret); err == nil {
		t.Fatal("trace writer accepted a duplicate key that hides a password")
	}

	duplicateEnvelope := `{"schema":"etcd-hbase-trace/v1","case_id":"duplicate","case_id":"duplicate","ordinal":1,"method":"etcdserverpb.KV/Range","request":{"fields":{},"oneofs":{}},"response":null,"error":null,"stream_terminal":null,"evidence_scope":"native-etcd","elapsed_ns":"0"}`
	if _, err := ReadJSONL(strings.NewReader(duplicateEnvelope)); err == nil {
		t.Fatal("trace reader accepted duplicate envelope keys")
	}

	duplicateSnapshot := `{"schema":"etcd-hbase-trace/v1","case_id":"duplicate-snapshot","ordinal":1,"method":"etcdserverpb.KV/Range","request":{"fields":{"password":"do-not-write"},"fields":{},"oneofs":{}},"response":null,"error":null,"stream_terminal":null,"evidence_scope":"native-etcd","elapsed_ns":"0"}`
	if _, err := ReadJSONL(strings.NewReader(duplicateSnapshot)); err == nil {
		t.Fatal("trace reader accepted a snapshot with a hidden password")
	}
}

func TestClassifyErrorDoesNotCopyErrorText(t *testing.T) {
	classified := ClassifyError(context.DeadlineExceeded)
	if classified.GRPCCode != "DeadlineExceeded" || classified.ConsumerKind != "context-deadline" {
		t.Fatalf("unexpected error classification: %#v", classified)
	}
	if ClassifyError(errors.New("password-secret")).ConsumerKind != "go-error" {
		t.Fatal("plain consumer error was not classified")
	}
}

func TestClassifyEtcdClientErrorPreservesConsumerAndGRPCViews(t *testing.T) {
	classified := ClassifyError(rpctypes.Error(rpctypes.ErrCompacted))
	if classified == nil || classified.GRPCCode != codes.OutOfRange.String() || classified.ConsumerKind != "etcd-client-error" {
		t.Fatalf("unexpected etcd error classification: %#v", classified)
	}
}

func testRecord(caseID string, ordinal uint64) Record {
	return Record{
		Schema:        SchemaVersion,
		CaseID:        caseID,
		Ordinal:       ordinal,
		Method:        "etcdserverpb.KV/Range",
		Request:       json.RawMessage(`{"fields":{},"oneofs":{}}`),
		Response:      json.RawMessage("null"),
		EvidenceScope: "transport",
		ElapsedNS:     "0",
	}
}
