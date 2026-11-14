package compare

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"google.golang.org/grpc/codes"
)

const m4MaximumLeaseTTL = int64(9_000_000_000)

var m4LeaseCases = []struct {
	id     string
	method string
}{
	{"native-baseline", "etcdserverpb.Lease/LeaseLeases"},
	{"auto-grant", "etcdserverpb.Lease/LeaseGrant"},
	{"auto-ttl", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"explicit-grant", "etcdserverpb.Lease/LeaseGrant"},
	{"explicit-ttl", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"duplicate-id", "etcdserverpb.Lease/LeaseGrant"},
	{"missing-ttl", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"negative-id", "etcdserverpb.Lease/LeaseGrant"},
	{"leases-list", "etcdserverpb.Lease/LeaseLeases"},
	{"oversized-ttl", "etcdserverpb.Lease/LeaseGrant"},
	{"zero-id-ttl", "etcdserverpb.Lease/LeaseTimeToLive"},
}

func validateM4LeaseTrace(records []trace.Record) error {
	if len(records) != len(m4LeaseCases) {
		return fmt.Errorf("M4 trace has %d records, expected %d", len(records), len(m4LeaseCases))
	}
	requests := make([]map[string]any, len(records))
	responses := make([]map[string]any, len(records))
	for index, expected := range m4LeaseCases {
		record := records[index]
		if record.CaseID != expected.id || record.Method != expected.method || record.Error != nil &&
			record.Error.GRPCCode == "Unknown" {
			return fmt.Errorf("M4 trace record %d is not the pinned %s step", index+1, expected.id)
		}
		request, err := m4SnapshotFields(record.Request)
		if err != nil {
			return fmt.Errorf("decode %s request: %w", expected.id, err)
		}
		response, err := m4SnapshotFields(record.Response)
		if err != nil && record.Error == nil {
			return fmt.Errorf("decode %s response: %w", expected.id, err)
		}
		requests[index] = request
		responses[index] = response
	}

	if records[0].Error != nil || responseInt64(responses[0], "header.revision") < 1 ||
		len(responseList(responses[0], "leases")) != 0 {
		return errors.New("native baseline is not a successful empty Lease listing")
	}
	autoID, autoTTL, err := grantObservation(requests[1], responses[1], records[1].Error, true)
	if err != nil {
		return fmt.Errorf("automatic Grant observation is invalid: %w", err)
	}
	if autoID <= 0 || autoTTL <= 0 {
		return fmt.Errorf("automatic Grant returned ID=%d TTL=%d; both must be positive", autoID, autoTTL)
	}
	autoRequestID, err := requestInt64(requests[2], "id")
	if err != nil || autoRequestID != autoID || records[2].Error != nil ||
		responseInt64(responses[2], "id") != autoID || responseInt64(responses[2], "ttl") <= 0 ||
		responseInt64(responses[2], "ttl") > responseInt64(responses[2], "grantedTTL") ||
		responseInt64(responses[2], "grantedTTL") != autoTTL || len(responseList(responses[2], "keys")) != 0 {
		return errors.New("automatic TTL response differs from its active empty Lease")
	}
	explicitID, explicitTTL, err := grantObservation(requests[3], responses[3], records[3].Error, false)
	if err != nil || explicitID == 0 || explicitTTL != 30 {
		return errors.New("explicit Grant changed its signed Lease ID or requested TTL")
	}
	if requestInt64Unchecked(requests[4], "id") != explicitID || records[4].Error != nil ||
		responseInt64(responses[4], "id") != explicitID || responseInt64(responses[4], "ttl") <= 0 ||
		responseInt64(responses[4], "ttl") > explicitTTL || responseInt64(responses[4], "grantedTTL") != explicitTTL ||
		len(responseList(responses[4], "keys")) != 0 {
		return errors.New("explicit TTL response differs from its active empty Lease")
	}
	if requestInt64Unchecked(requests[5], "id") != explicitID || records[5].Error == nil ||
		records[5].Error.GRPCCode != codes.FailedPrecondition.String() || responses[5] != nil {
		return errors.New("duplicate ID did not retain the native FailedPrecondition result")
	}
	missingID, err := requestInt64(requests[6], "id")
	if err != nil || missingID == 0 || missingID == explicitID || missingID == autoID || records[6].Error != nil ||
		responseInt64(responses[6], "id") != missingID || responseInt64(responses[6], "ttl") != -1 ||
		responseInt64(responses[6], "grantedTTL") != 0 || len(responseList(responses[6], "keys")) != 0 {
		return errors.New("missing ID did not return TTL=-1 without a partial key list")
	}
	negativeID, negativeTTL, err := grantObservation(requests[7], responses[7], records[7].Error, false)
	if err != nil || negativeID >= 0 || negativeTTL != 30 {
		return errors.New("negative explicit Lease ID was not preserved as signed int64")
	}
	if records[8].Error != nil || len(responseList(responses[8], "leases")) != 3 ||
		!containsResponseID(responses[8], autoID) || !containsResponseID(responses[8], explicitID) ||
		!containsResponseID(responses[8], negativeID) {
		return errors.New("LeaseLeases omitted an active Lease")
	}
	if requestInt64Unchecked(requests[9], "ttl") != m4MaximumLeaseTTL+1 || records[9].Error == nil ||
		records[9].Error.GRPCCode != codes.OutOfRange.String() || responses[9] != nil {
		return errors.New("oversized TTL did not preserve the native OutOfRange result")
	}
	if requestInt64Unchecked(requests[10], "id") != 0 || records[10].Error != nil ||
		responseInt64(responses[10], "id") != 0 || responseInt64(responses[10], "ttl") != -1 ||
		responseInt64(responses[10], "grantedTTL") != 0 || len(responseList(responses[10], "keys")) != 0 {
		return errors.New("zero-ID TTL did not preserve the native missing-lease projection")
	}
	return nil
}

func grantObservation(request, response map[string]any, callError *trace.Error,
	automatic bool) (int64, int64, error) {
	id, err := requestInt64(request, "id")
	if err != nil {
		return 0, 0, err
	}
	requestedTTL, err := requestInt64(request, "ttl")
	if err != nil {
		return 0, 0, err
	}
	if callError != nil || response == nil {
		return 0, 0, errors.New("LeaseGrant did not succeed")
	}
	responseID := responseInt64(response, "id")
	grantedTTL := responseInt64(response, "ttl")
	if responseID == 0 || grantedTTL <= 0 || automatic && (id != 0 || requestedTTL > 0) ||
		!automatic && (id == 0 || responseID != id || grantedTTL != requestedTTL) {
		return 0, 0, errors.New("LeaseGrant request and response do not match")
	}
	return responseID, grantedTTL, nil
}

func projectM4LeaseTerm(left, right map[string]any, leftScope string) error {
	method, ok := left["method"].(string)
	if !ok || right["method"] != method || method != "etcdserverpb.Lease/LeaseGrant" &&
		method != "etcdserverpb.Lease/LeaseTimeToLive" && method != "etcdserverpb.Lease/LeaseLeases" {
		return errors.New("method is not registered for the M4 Lease policy")
	}
	native, target := left, right
	if leftScope == "target-hbase" {
		native, target = right, left
	}
	nativeHeader, err := normalizedResponseHeader(native)
	if err != nil {
		return err
	}
	targetHeader, err := normalizedResponseHeader(target)
	if err != nil {
		return err
	}
	nativeTermValue, err := lookupM4Field(nativeHeader, "raftTerm")
	if err != nil {
		return err
	}
	nativeTerm, ok := nativeTermValue.(string)
	parsed, parseErr := strconv.ParseUint(nativeTerm, 10, 64)
	targetTerm, err := lookupM4Field(targetHeader, "raftTerm")
	if err != nil {
		return err
	}
	if !ok || parseErr != nil || parsed == 0 || targetTerm != "0" {
		return errors.New("invalid native or target Raft term")
	}
	targetKey, _, exists, err := locateM4Field(targetHeader, "raftTerm")
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("target response header has no Raft term")
	}
	targetHeader[targetKey] = nativeTerm
	return nil
}

func projectM4LeaseResponseWindow(left, right map[string]any, leftScope, rightScope string) error {
	if err := sortM4LeaseList(left); err != nil {
		return err
	}
	if err := sortM4LeaseList(right); err != nil {
		return err
	}
	caseID, ok := left["case_id"].(string)
	if !ok || right["case_id"] != caseID {
		return errors.New("M4 case identity differs")
	}
	if caseID == "auto-grant" {
		if err := replaceResponseField(left, "ttl", "configured-minimum"); err != nil {
			return err
		}
		return replaceResponseField(right, "ttl", "configured-minimum")
	}
	if caseID != "auto-ttl" && caseID != "explicit-ttl" {
		return nil
	}
	leftTTL, err := normalizedResponseInt64(left, "ttl")
	if err != nil {
		return err
	}
	rightTTL, err := normalizedResponseInt64(right, "ttl")
	if err != nil {
		return err
	}
	if difference := leftTTL - rightTTL; difference < -1 || difference > 1 {
		return fmt.Errorf("remaining TTL differs by %d seconds, maximum allowed skew is one second", difference)
	}
	if err := replaceResponseField(right, "ttl", strconv.FormatInt(leftTTL, 10)); err != nil {
		return err
	}
	if caseID == "auto-ttl" {
		if err := replaceResponseField(left, "grantedTTL", "configured-minimum"); err != nil {
			return err
		}
		return replaceResponseField(right, "grantedTTL", "configured-minimum")
	}
	return nil
}

func normalizedResponseInt64(record map[string]any, field string) (int64, error) {
	response, err := normalizedFields(record, "response")
	if err != nil {
		return 0, err
	}
	return m4Int64Field(response, field)
}

func sortM4LeaseList(record map[string]any) error {
	if record["case_id"] != "native-baseline" && record["case_id"] != "leases-list" {
		return nil
	}
	response, err := normalizedFields(record, "response")
	if err != nil {
		return err
	}
	leases := responseList(response, "leases")
	if leases == nil {
		return errors.New("LeaseLeases response has no complete leases list")
	}
	type leaseStatus struct {
		id      string
		encoded any
	}
	ordered := make([]leaseStatus, 0, len(leases))
	ids := make(map[string]struct{}, len(leases))
	for _, item := range leases {
		entry, ok := item.(map[string]any)
		if !ok {
			return errors.New("LeaseLeases contains a malformed status")
		}
		fields, err := normalizedFields(entry, "")
		if err != nil {
			return err
		}
		idValue, err := lookupM4Field(fields, "id")
		if err != nil {
			return err
		}
		id, ok := idValue.(string)
		if !ok || id == "" {
			return errors.New("LeaseLeases status has no normalized ID")
		}
		if _, exists := ids[id]; exists {
			return errors.New("LeaseLeases contains a duplicate ID")
		}
		ids[id] = struct{}{}
		ordered = append(ordered, leaseStatus{id: id, encoded: item})
	}
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left].id < ordered[right].id
	})
	for index := range ordered {
		leases[index] = ordered[index].encoded
	}
	return nil
}

func replaceResponseField(record map[string]any, field, replacement string) error {
	fields, err := normalizedFields(record, "response")
	if err != nil {
		return err
	}
	key, _, exists, err := locateM4Field(fields, field)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("M4 response is missing %s", field)
	}
	fields[key] = replacement
	return nil
}

func normalizedResponseHeader(record map[string]any) (map[string]any, error) {
	response, err := normalizedFields(record, "response")
	if err != nil {
		return nil, err
	}
	header, err := normalizedFields(response, "header")
	if err != nil {
		return nil, err
	}
	return header, nil
}

func normalizedFields(record map[string]any, field string) (map[string]any, error) {
	current := record
	if field != "" {
		value, err := lookupM4Field(record, field)
		if err != nil {
			return nil, err
		}
		object, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("missing normalized %s object", field)
		}
		current = object
	}
	value, err := lookupM4Field(current, "fields")
	if err != nil {
		return nil, err
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("missing normalized protobuf fields")
	}
	return fields, nil
}

func m4SnapshotFields(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var snapshot struct {
		Fields map[string]any `json:"fields"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&snapshot); err != nil || snapshot.Fields == nil {
		return nil, errors.New("invalid protobuf snapshot")
	}
	return snapshot.Fields, nil
}

func requestInt64(snapshot map[string]any, field string) (int64, error) {
	if snapshot == nil {
		return 0, errors.New("request snapshot is missing")
	}
	return m4Int64Field(snapshot, field)
}

func m4Int64Field(fields map[string]any, field string) (int64, error) {
	fieldValue, err := lookupM4Field(fields, field)
	if err != nil {
		return 0, err
	}
	value, ok := fieldValue.(string)
	if !ok {
		return 0, fmt.Errorf("field %s is not a decimal string", field)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || strconv.FormatInt(parsed, 10) != value {
		return 0, fmt.Errorf("field %s is not a canonical int64: %q", field, value)
	}
	return parsed, nil
}

func requestInt64Unchecked(snapshot map[string]any, field string) int64 {
	parsed, _ := requestInt64(snapshot, field)
	return parsed
}

func responseInt64(snapshot map[string]any, field string) int64 {
	if snapshot == nil {
		return math.MinInt64
	}
	var value any
	var err error
	if field == "header.revision" {
		header, nestedErr := m4NestedFields(snapshot, "header", "fields")
		if nestedErr != nil {
			return math.MinInt64
		}
		value, err = lookupM4Field(header, "revision")
	} else {
		value, err = lookupM4Field(snapshot, field)
	}
	if err != nil {
		return math.MinInt64
	}
	text, ok := value.(string)
	if !ok {
		return math.MinInt64
	}
	parsed, parseErr := strconv.ParseInt(text, 10, 64)
	if parseErr != nil || strconv.FormatInt(parsed, 10) != text {
		return math.MinInt64
	}
	return parsed
}

func responseList(snapshot map[string]any, field string) []any {
	if snapshot == nil {
		return nil
	}
	value, err := lookupM4Field(snapshot, field)
	if err != nil {
		return nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	return items
}

func containsResponseID(snapshot map[string]any, expected int64) bool {
	for _, item := range responseList(snapshot, "leases") {
		status, ok := item.(map[string]any)
		if !ok {
			continue
		}
		fields, err := m4NestedFields(status, "fields")
		if err != nil {
			continue
		}
		if responseInt64(fields, "id") == expected {
			return true
		}
	}
	return false
}

func m4NestedFields(snapshot map[string]any, keys ...string) (map[string]any, error) {
	current := snapshot
	for _, key := range keys {
		value, err := lookupM4Field(current, key)
		if err != nil {
			return nil, err
		}
		next, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("snapshot field %s is missing", key)
		}
		current = next
	}
	return current, nil
}

func lookupM4Field(fields map[string]any, name string) (any, error) {
	_, value, exists, err := locateM4Field(fields, name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return value, nil
}

func locateM4Field(fields map[string]any, name string) (string, any, bool, error) {
	var matchedKey string
	var matchedValue any
	for key, value := range fields {
		if !strings.EqualFold(key, name) {
			continue
		}
		if matchedKey != "" {
			return "", nil, false, fmt.Errorf("M4 snapshot has ambiguous %s field", name)
		}
		matchedKey = key
		matchedValue = value
	}
	return matchedKey, matchedValue, matchedKey != "", nil
}

func m4ResponseHeaderFields(response map[string]any) (map[string]any, error) {
	return m4NestedFields(response, "fields", "header", "fields")
}
