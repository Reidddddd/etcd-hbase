package compare

import (
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

var m4LeaseBindingCases = []struct {
	id     string
	method string
}{
	{"binding-baseline", "etcdserverpb.Lease/LeaseLeases"},
	{"binding-grant-a", "etcdserverpb.Lease/LeaseGrant"},
	{"binding-grant-b", "etcdserverpb.Lease/LeaseGrant"},
	{"binding-register-then", "etcdserverpb.KV/Txn"},
	{"binding-ttl-a-initial", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-ttl-b-initial", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-ignore-flags", "etcdserverpb.KV/Put"},
	{"binding-ttl-a-preserved", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-rebind-a-to-b", "etcdserverpb.KV/Put"},
	{"binding-ttl-a-after-rebind", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-ttl-b-after-rebind", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-register-else", "etcdserverpb.KV/Txn"},
	{"binding-ttl-b-after-else", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-register-nested", "etcdserverpb.KV/Txn"},
	{"binding-ttl-a-after-nested", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-ttl-b-after-nested", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-unbind", "etcdserverpb.KV/Put"},
	{"binding-ttl-a-after-unbind", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-delete", "etcdserverpb.KV/DeleteRange"},
	{"binding-ttl-b-after-delete", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-recreate-unleased", "etcdserverpb.KV/Put"},
	{"binding-ttl-b-after-recreate", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-delete-range", "etcdserverpb.KV/DeleteRange"},
	{"binding-ttl-a-after-delete-range", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-ttl-b-after-delete-range", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-role-prefix-after-delete-range", "etcdserverpb.KV/Range"},
	{"binding-version-after-delete-range", "etcdserverpb.KV/Range"},
	{"binding-survivor-put", "etcdserverpb.KV/Put"},
	{"binding-ttl-b-after-survivor-put", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-invalid-lease-atomicity", "etcdserverpb.KV/Txn"},
	{"binding-atomicity-range-first", "etcdserverpb.KV/Range"},
	{"binding-atomicity-range-second", "etcdserverpb.KV/Range"},
	{"binding-ttl-a-after-failed-txn", "etcdserverpb.Lease/LeaseTimeToLive"},
	{"binding-invalid-lease-put", "etcdserverpb.KV/Put"},
	{"binding-atomicity-range-invalid-put", "etcdserverpb.KV/Range"},
	{"binding-invalid-lease-unselected-branch", "etcdserverpb.KV/Txn"},
	{"binding-unselected-then-absent", "etcdserverpb.KV/Range"},
	{"binding-unselected-else-visible", "etcdserverpb.KV/Range"},
	{"binding-ttl-b-after-unselected-branch", "etcdserverpb.Lease/LeaseTimeToLive"},
}

// CompareM4LeaseBindingTraces compares target behavior with the pinned native etcd trace.
func CompareM4LeaseBindingTraces(left, right io.Reader) error {
	leftRecords, err := trace.ReadJSONL(left)
	if err != nil {
		return fmt.Errorf("read native M4 binding trace: %w", err)
	}
	rightRecords, err := trace.ReadJSONL(right)
	if err != nil {
		return fmt.Errorf("read target M4 binding trace: %w", err)
	}
	return CompareM4LeaseBindingRecords(leftRecords, rightRecords)
}

// CompareM4LeaseBindingRecords checks requests, errors, responses, and KV revisions.
func CompareM4LeaseBindingRecords(left, right []trace.Record) error {
	if len(left) != len(m4LeaseBindingCases) || len(right) != len(m4LeaseBindingCases) {
		return fmt.Errorf("M4 binding trace has left=%d right=%d records, expected %d",
			len(left), len(right), len(m4LeaseBindingCases))
	}
	nativeMaximumTTL := make(map[string]int64)
	targetMaximumTTL := make(map[string]int64)
	nativeGrantedTTL := make(map[string]int64)
	targetGrantedTTL := make(map[string]int64)
	for index, expected := range m4LeaseBindingCases {
		leftRecord, rightRecord := left[index], right[index]
		if leftRecord.CaseID != expected.id || rightRecord.CaseID != expected.id ||
			leftRecord.Method != expected.method || rightRecord.Method != expected.method {
			return fmt.Errorf("M4 binding trace record %d is not the pinned %s step", index+1, expected.id)
		}
		if leftRecord.StreamTerminal != nil || rightRecord.StreamTerminal != nil {
			return fmt.Errorf("unary M4 binding step %s has a stream terminal", expected.id)
		}
		leftRequest, err := m4SnapshotFields(leftRecord.Request)
		if err != nil {
			return fmt.Errorf("decode native %s request: %w", expected.id, err)
		}
		rightRequest, err := m4SnapshotFields(rightRecord.Request)
		if err != nil {
			return fmt.Errorf("decode target %s request: %w", expected.id, err)
		}
		if !reflect.DeepEqual(leftRequest, rightRequest) {
			return fmt.Errorf("native and target %s requests differ", expected.id)
		}
		if !sameM4BindingError(leftRecord.Error, rightRecord.Error) {
			return fmt.Errorf("native and target %s gRPC errors differ", expected.id)
		}
		if isM4InvalidLeaseCase(expected.id) {
			if leftRecord.Error == nil || leftRecord.Error.GRPCCode != "NotFound" {
				return fmt.Errorf("native unknown-Lease operation %s did not return NotFound", expected.id)
			}
		} else if leftRecord.Error != nil {
			return fmt.Errorf("native %s unexpectedly failed with %s", expected.id, leftRecord.Error.GRPCCode)
		}
		leftResponse, err := m4SnapshotFields(leftRecord.Response)
		if err != nil {
			return fmt.Errorf("decode native %s response: %w", expected.id, err)
		}
		rightResponse, err := m4SnapshotFields(rightRecord.Response)
		if err != nil {
			return fmt.Errorf("decode target %s response: %w", expected.id, err)
		}
		if leftRecord.Error != nil {
			if leftResponse != nil || rightResponse != nil {
				return fmt.Errorf("failed %s returned a partial response", expected.id)
			}
			continue
		}
		if leftResponse == nil || rightResponse == nil {
			return fmt.Errorf("successful %s is missing a response", expected.id)
		}
		stripM4TransportFields(leftResponse)
		stripM4TransportFields(rightResponse)
		if isM4BindingTtlCase(expected.id) {
			// Independent scenarios have different HBase/native execution times; keep raw TTL
			// evidence, but compare its bounds and within-run progress instead of wall seconds.
			if err := validateM4BindingRemainingTTL(expected.id, "native", leftResponse,
				nativeMaximumTTL, nativeGrantedTTL); err != nil {
				return err
			}
			if err := validateM4BindingRemainingTTL(expected.id, "target", rightResponse,
				targetMaximumTTL, targetGrantedTTL); err != nil {
				return err
			}
			if err := normalizeM4LeaseBindingKeys(leftResponse, expected.id); err != nil {
				return fmt.Errorf("native %s response: %w", expected.id, err)
			}
			if err := normalizeM4LeaseBindingKeys(rightResponse, expected.id); err != nil {
				return fmt.Errorf("target %s response: %w", expected.id, err)
			}
			if err := normalizeM4BindingRemainingTTL(leftResponse); err != nil {
				return fmt.Errorf("native %s response: %w", expected.id, err)
			}
			if err := normalizeM4BindingRemainingTTL(rightResponse); err != nil {
				return fmt.Errorf("target %s response: %w", expected.id, err)
			}
		}
		if !reflect.DeepEqual(leftResponse, rightResponse) {
			return fmt.Errorf("native and target %s responses differ", expected.id)
		}
	}
	return nil
}

func validateM4BindingRemainingTTL(caseID, side string, response map[string]any,
	maximumTTL, grantedTTLs map[string]int64) error {
	leaseName := m4BindingLeaseName(caseID)
	ttl := responseInt64(response, "ttl")
	grantedTTL := responseInt64(response, "grantedTTL")
	if leaseName == "" || ttl == math.MinInt64 || grantedTTL == math.MinInt64 ||
		ttl <= 0 || grantedTTL <= 0 || ttl > grantedTTL {
		return fmt.Errorf("%s %s returned an invalid remaining/granted TTL pair", side, caseID)
	}
	if previousGrant, exists := grantedTTLs[leaseName]; exists && previousGrant != grantedTTL {
		return fmt.Errorf("%s %s changed the Lease granted TTL", side, caseID)
	}
	if maximumObserved, exists := maximumTTL[leaseName]; exists &&
		ttl > maximumObserved && ttl-maximumObserved > 1 {
		return fmt.Errorf("%s %s increased remaining TTL without a KeepAlive", side, caseID)
	}
	if ttl > maximumTTL[leaseName] {
		maximumTTL[leaseName] = ttl
	}
	grantedTTLs[leaseName] = grantedTTL
	return nil
}

func m4BindingLeaseName(caseID string) string {
	switch {
	case strings.HasPrefix(caseID, "binding-ttl-a-"):
		return "lease-a"
	case strings.HasPrefix(caseID, "binding-ttl-b-"):
		return "lease-b"
	default:
		return ""
	}
}

func normalizeM4BindingRemainingTTL(response map[string]any) error {
	fieldName := ""
	for name := range response {
		if !strings.EqualFold(name, "ttl") {
			continue
		}
		if fieldName != "" {
			return errors.New("LeaseTimeToLive response has duplicate TTL fields")
		}
		fieldName = name
	}
	if fieldName == "" {
		return errors.New("LeaseTimeToLive response is missing its TTL field")
	}
	response[fieldName] = "remaining-ttl"
	return nil
}

func normalizeM4LeaseBindingKeys(response map[string]any, caseID string) error {
	value, exists := response["keys"]
	if !exists {
		return errors.New("LeaseTimeToLive response is missing its keys field")
	}
	keys, ok := value.([]any)
	if !ok {
		return errors.New("LeaseTimeToLive keys field is not an array")
	}
	seen := make(map[string]struct{}, len(keys))
	normalized := make([]string, len(keys))
	for index, value := range keys {
		key, ok := value.(string)
		if !ok {
			return errors.New("LeaseTimeToLive keys field contains a non-string key")
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("LeaseTimeToLive response contains a duplicate key in %s", caseID)
		}
		seen[key] = struct{}{}
		normalized[index] = key
	}
	sort.Strings(normalized)
	response["keys"] = normalized
	return nil
}

func sameM4BindingError(left, right *trace.Error) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.GRPCCode == right.GRPCCode
}

func isM4BindingTtlCase(caseID string) bool {
	switch caseID {
	case "binding-ttl-a-initial", "binding-ttl-b-initial", "binding-ttl-a-preserved", "binding-ttl-a-after-rebind",
		"binding-ttl-b-after-rebind", "binding-ttl-b-after-else", "binding-ttl-a-after-unbind",
		"binding-ttl-a-after-nested", "binding-ttl-b-after-nested", "binding-ttl-b-after-delete",
		"binding-ttl-b-after-recreate", "binding-ttl-a-after-delete-range", "binding-ttl-b-after-delete-range",
		"binding-ttl-b-after-survivor-put", "binding-ttl-a-after-failed-txn",
		"binding-ttl-b-after-unselected-branch":
		return true
	default:
		return false
	}
}

func isM4InvalidLeaseCase(caseID string) bool {
	switch caseID {
	case "binding-invalid-lease-atomicity", "binding-invalid-lease-put":
		return true
	default:
		return false
	}
}

func stripM4TransportFields(value any) {
	switch current := value.(type) {
	case map[string]any:
		for key, nested := range current {
			if key == "header" {
				if header, ok := nested.(map[string]any); ok {
					if fields, ok := header["fields"].(map[string]any); ok {
						delete(fields, "clusterId")
						delete(fields, "memberId")
						delete(fields, "raftTerm")
					}
				}
				continue
			}
			stripM4TransportFields(nested)
		}
	case []any:
		for _, nested := range current {
			stripM4TransportFields(nested)
		}
	}
}
