package compare

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"io"
	"reflect"
)

// CompareTraces compares two complete JSONL traces after applying only the
// protocol normalizations approved for M0: elapsed time, stable entity ID
// correspondence, and one scenario-relative revision offset.
func CompareTraces(left, right io.Reader) error {
	return compareTraces(left, right, standardPolicy)
}

// CompareM3PutTraces also applies the explicitly disclosed non-Raft header term projection.
// It is restricted to the J30 Put suite; other M3 methods need their own registered policy.
func CompareM3PutTraces(left, right io.Reader) error {
	return compareTraces(left, right, m3PutPolicy)
}

// CompareM3TxnTraces allows the top-level non-Raft term projection for the
// registered Put baseline and Txn cases. Child responses stay strict.
func CompareM3TxnTraces(left, right io.Reader) error {
	return compareTraces(left, right, m3TxnPolicy)
}

// CompareM3DeleteTraces adds the registered standalone DeleteRange method to
// the M3 Txn policy without weakening its child-response checks.
func CompareM3DeleteTraces(left, right io.Reader) error {
	return compareTraces(left, right, m3DeletePolicy)
}

// CompareM3RangeTraces adds standalone Range to the registered M3 methods.
func CompareM3RangeTraces(left, right io.Reader) error {
	return compareTraces(left, right, m3RangePolicy)
}

// CompareM4LeaseTraces compares the J40 Lease surface with only the configured
// minimum-TTL and whole-second remaining-TTL differences permitted.
func CompareM4LeaseTraces(left, right io.Reader) error {
	leftRecords, err := trace.ReadJSONL(left)
	if err != nil {
		return fmt.Errorf("read native M4 trace: %w", err)
	}
	rightRecords, err := trace.ReadJSONL(right)
	if err != nil {
		return fmt.Errorf("read target M4 trace: %w", err)
	}
	return CompareM4LeaseRecords(leftRecords, rightRecords)
}

// CompareM4LeaseRecords exposes the same strict M4 policy to native regressions.
func CompareM4LeaseRecords(left, right []trace.Record) error {
	return compare(left, right, m4LeasePolicy)
}

type tracePolicy int

const (
	standardPolicy tracePolicy = iota
	m3PutPolicy
	m3TxnPolicy
	m3DeletePolicy
	m3RangePolicy
	m4LeasePolicy
)

func compareTraces(left, right io.Reader, policy tracePolicy) error {
	leftRecords, err := trace.ReadJSONL(left)
	if err != nil {
		return fmt.Errorf("read left trace: %w", err)
	}
	rightRecords, err := trace.ReadJSONL(right)
	if err != nil {
		return fmt.Errorf("read right trace: %w", err)
	}
	return compare(leftRecords, rightRecords, policy)
}

// NormalizeTrace writes the permitted comparison representation for one
// complete trace so both normalized sides remain inspectable as evidence.
func NormalizeTrace(input io.Reader, output io.Writer) error {
	if output == nil {
		return errors.New("normalized trace writer is required")
	}
	records, err := trace.ReadJSONL(input)
	if err != nil {
		return fmt.Errorf("read trace: %w", err)
	}
	normalized, err := normalize(records)
	if err != nil {
		return fmt.Errorf("normalize trace: %w", err)
	}
	encoder := json.NewEncoder(output)
	for _, record := range normalized {
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("write normalized trace: %w", err)
		}
	}
	return nil
}

// Compare rejects missing, reordered, or semantically different records.
func Compare(left, right []trace.Record) error {
	return compare(left, right, standardPolicy)
}

func compare(left, right []trace.Record, policy tracePolicy) error {
	leftScope, rightScope, err := comparisonScopes(left, right)
	if err != nil {
		return err
	}
	if policy != standardPolicy && policy != m4LeasePolicy && (leftScope == rightScope ||
		(leftScope != "native-etcd" && rightScope != "native-etcd") ||
		(leftScope != "target-hbase" && rightScope != "target-hbase")) {
		return errors.New("M3 comparison requires native and target traces")
	}
	if policy == m4LeasePolicy {
		if (leftScope != "native-etcd" && leftScope != "target-hbase") ||
			(leftScope != rightScope && leftScope != "native-etcd" && rightScope != "native-etcd") ||
			(leftScope != rightScope && leftScope != "target-hbase" && rightScope != "target-hbase") {
			return errors.New("M4 comparison requires native or target Lease traces")
		}
		if err := validateM4LeaseTrace(left); err != nil {
			return fmt.Errorf("validate left M4 trace: %w", err)
		}
		if err := validateM4LeaseTrace(right); err != nil {
			return fmt.Errorf("validate right M4 trace: %w", err)
		}
	}
	leftNormalized, err := normalize(left)
	if err != nil {
		return fmt.Errorf("normalize left trace: %w", err)
	}
	rightNormalized, err := normalize(right)
	if err != nil {
		return fmt.Errorf("normalize right trace: %w", err)
	}
	if len(leftNormalized) != len(rightNormalized) {
		return fmt.Errorf("record count differs: left=%d right=%d", len(leftNormalized), len(rightNormalized))
	}
	for index := range leftNormalized {
		// Evidence provenance is checked above; it is not a wire field.
		rightNormalized[index]["evidence_scope"] = leftNormalized[index]["evidence_scope"]
		bothSucceeded := left[index].Error == nil && right[index].Error == nil &&
			left[index].StreamTerminal == nil && right[index].StreamTerminal == nil
		if policy == m4LeasePolicy && leftScope != rightScope && bothSucceeded {
			if err := projectM4LeaseTerm(leftNormalized[index], rightNormalized[index], leftScope); err != nil {
				return fmt.Errorf("trace record %d has invalid M4 response header: %w", index+1, err)
			}
		} else if policy != standardPolicy && policy != m4LeasePolicy {
			if err := projectM3Term(leftNormalized[index], rightNormalized[index], leftScope, bothSucceeded, policy); err != nil {
				return fmt.Errorf("trace record %d has invalid M3 header: %w", index+1, err)
			}
		}
		if policy == m4LeasePolicy {
			if err := projectM4LeaseResponseWindow(leftNormalized[index], rightNormalized[index], leftScope, rightScope); err != nil {
				return fmt.Errorf("trace record %d violates the M4 Lease timing or ordering contract: %w", index+1, err)
			}
		}
		if (policy == m3TxnPolicy || policy == m3DeletePolicy || policy == m3RangePolicy) && leftScope != rightScope && bothSucceeded {
			if applied, equal := equalTxnWithCountAllowance(leftNormalized[index], rightNormalized[index], leftScope); applied {
				if !equal {
					return fmt.Errorf("trace record %d violates the selected Txn Range Count policy", index+1)
				}
				continue
			}
		}
		count := strictCount
		if leftScope != rightScope && bothSucceeded {
			count = countPolicyForRequest(leftNormalized[index])
		}
		// A successful target Count must satisfy its contract even when both
		// observed Counts happen to be equal. Failed observations stay strict.
		if count != strictCount {
			if !equalWithCountAllowance(leftNormalized[index], rightNormalized[index], leftScope, count) {
				return fmt.Errorf("trace record %d violates the %s Count policy", index+1, count)
			}
			continue
		}
		if !reflect.DeepEqual(leftNormalized[index], rightNormalized[index]) {
			return fmt.Errorf("trace record %d differs", index+1)
		}
	}
	return nil
}

func comparisonScopes(left, right []trace.Record) (string, string, error) {
	scopes := make([]string, 0, 2)
	for _, records := range [][]trace.Record{left, right} {
		if len(records) == 0 {
			return "", "", errors.New("trace is empty")
		}
		scope := records[0].EvidenceScope
		if scope != "native-etcd" && scope != "target-hbase" {
			return "", "", errors.New("oracle requires native-etcd or target-hbase evidence")
		}
		for _, record := range records {
			if record.EvidenceScope != scope {
				return "", "", errors.New("oracle cannot mix evidence scopes within one trace")
			}
			if record.Error != nil && record.Error.SemanticKind == "unclassified" {
				return "", "", errors.New("unclassified errors cannot establish equivalence")
			}
		}
		scopes = append(scopes, scope)
	}
	return scopes[0], scopes[1], nil
}
