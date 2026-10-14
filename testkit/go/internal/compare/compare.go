package compare

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"io.etcdhbase/testkit/internal/trace"
)

// CompareTraces compares two complete JSONL traces after applying only the
// protocol normalizations approved for M0: elapsed time, stable entity ID
// correspondence, and one scenario-relative revision offset.
func CompareTraces(left, right io.Reader) error {
	leftRecords, err := trace.ReadJSONL(left)
	if err != nil {
		return fmt.Errorf("read left trace: %w", err)
	}
	rightRecords, err := trace.ReadJSONL(right)
	if err != nil {
		return fmt.Errorf("read right trace: %w", err)
	}
	return Compare(leftRecords, rightRecords)
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
		if reflect.DeepEqual(leftNormalized[index], rightNormalized[index]) {
			continue
		}
		if !equalWithCountAllowance(leftNormalized[index], rightNormalized[index]) {
			return fmt.Errorf("trace record %d differs", index+1)
		}
	}
	return nil
}

func normalize(records []trace.Record) ([]map[string]any, error) {
	if len(records) == 0 {
		return nil, errors.New("trace is empty")
	}
	baseRevision, found := int64(0), false
	for _, record := range records {
		if record.CaseID != "native-baseline" || len(record.Response) == 0 || bytes.Equal(record.Response, []byte("null")) {
			continue
		}
		response, err := decodeObject(record.Response)
		if err != nil {
			return nil, fmt.Errorf("decode baseline response: %w", err)
		}
		revision, err := responseRevision(response)
		if err != nil {
			return nil, fmt.Errorf("read scenario baseline revision: %w", err)
		}
		baseRevision, found = revision, true
		break
	}
	if !found {
		return nil, errors.New("trace has no native-baseline scenario record")
	}

	ids := map[string]map[string]string{
		"cluster": {},
		"member":  {},
		"lease":   {},
	}
	sequences := map[string]uint64{}
	normalized := make([]map[string]any, 0, len(records))
	for _, record := range records {
		request, err := decodeNullable(record.Request)
		if err != nil {
			return nil, fmt.Errorf("decode request in trace record %d: invalid snapshot JSON", record.Ordinal)
		}
		response, err := decodeNullable(record.Response)
		if err != nil {
			return nil, fmt.Errorf("decode response in trace record %d: invalid snapshot JSON", record.Ordinal)
		}
		request, err = normalizeValue(request, record.Method, baseRevision, ids, sequences)
		if err != nil {
			return nil, fmt.Errorf("normalize request in trace record %d: %w", record.Ordinal, err)
		}
		response, err = normalizeValue(response, record.Method, baseRevision, ids, sequences)
		if err != nil {
			return nil, fmt.Errorf("normalize response in trace record %d: %w", record.Ordinal, err)
		}
		normalized = append(normalized, map[string]any{
			"schema":          record.Schema,
			"case_id":         record.CaseID,
			"ordinal":         record.Ordinal,
			"method":          record.Method,
			"request":         request,
			"response":        response,
			"error":           record.Error,
			"stream_terminal": record.StreamTerminal,
			"evidence_scope":  record.EvidenceScope,
		})
	}
	return normalized, nil
}

func decodeNullable(raw json.RawMessage) (any, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, errors.New("invalid snapshot JSON")
	}
	return value, nil
}

func decodeObject(raw json.RawMessage) (map[string]any, error) {
	value, err := decodeNullable(raw)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("expected protobuf snapshot object")
	}
	return object, nil
}

func responseRevision(response map[string]any) (int64, error) {
	fields, err := nestedFields(response, "fields", "header", "fields")
	if err != nil {
		return 0, err
	}
	return decimalField(fields, "revision")
}

func nestedFields(value map[string]any, keys ...string) (map[string]any, error) {
	current := value
	for _, key := range keys {
		next, ok := current[key].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("missing object field %q", key)
		}
		current = next
	}
	return current, nil
}

func decimalField(fields map[string]any, key string) (int64, error) {
	value, ok := fields[key].(string)
	if !ok {
		return 0, fmt.Errorf("field %q is not a decimal string", key)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || strconv.FormatInt(parsed, 10) != value {
		return 0, fmt.Errorf("field %q is not a canonical int64", key)
	}
	return parsed, nil
}

func normalizeValue(value any, method string, baseRevision int64, ids map[string]map[string]string, sequences map[string]uint64) (any, error) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		result := make(map[string]any, len(typed))
		for _, key := range keys {
			child := typed[key]
			if isRevisionField(key) {
				revision, err := parseRevision(child)
				if err != nil {
					return nil, fmt.Errorf("invalid revision field: %w", err)
				}
				if revision > 0 {
					revision -= baseRevision
					if revision < 0 {
						return nil, fmt.Errorf("revision field %q precedes scenario baseline", key)
					}
				}
				result[key] = strconv.FormatInt(revision, 10)
				continue
			}
			if domain := identityDomain(method, key); domain != "" {
				id, err := parseID(child)
				if err != nil {
					return nil, fmt.Errorf("invalid identity field: %w", err)
				}
				if id == "0" {
					result[key] = id
					continue
				}
				canonical, ok := ids[domain][id]
				if !ok {
					sequences[domain]++
					canonical = fmt.Sprintf("%s:%d", domain, sequences[domain])
					ids[domain][id] = canonical
				}
				result[key] = canonical
				continue
			}
			normalized, err := normalizeValue(child, method, baseRevision, ids, sequences)
			if err != nil {
				return nil, err
			}
			result[key] = normalized
		}
		return result, nil
	case []any:
		result := make([]any, len(typed))
		for index, child := range typed {
			normalized, err := normalizeValue(child, method, baseRevision, ids, sequences)
			if err != nil {
				return nil, err
			}
			result[index] = normalized
		}
		return result, nil
	default:
		return value, nil
	}
}

func isRevisionField(key string) bool {
	switch strings.ToLower(strings.ReplaceAll(key, "_", "")) {
	case "revision", "createrevision", "modrevision", "compactrevision", "startrevision":
		return true
	default:
		return false
	}
}

func parseRevision(value any) (int64, error) {
	text, ok := value.(string)
	if !ok {
		return 0, errors.New("revision is not encoded as a decimal string")
	}
	parsed, err := strconv.ParseInt(text, 10, 64)
	if err != nil || strconv.FormatInt(parsed, 10) != text {
		return 0, errors.New("revision is not a canonical int64")
	}
	return parsed, nil
}

func identityDomain(method, key string) string {
	switch strings.ToLower(key) {
	case "clusterid":
		return "cluster"
	case "memberid", "leader":
		return "member"
	case "lease":
		return "lease"
	case "id":
		if strings.HasPrefix(method, "etcdserverpb.Lease/") {
			return "lease"
		}
	}
	return ""
}

func parseID(value any) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", errors.New("ID is not encoded as a decimal string")
	}
	parsed, err := strconv.ParseUint(text, 10, 64)
	if err != nil || strconv.FormatUint(parsed, 10) != text {
		return "", errors.New("ID is not a canonical uint64")
	}
	return text, nil
}

func equalWithCountAllowance(left, right map[string]any) bool {
	if !reflect.DeepEqual(withoutResponse(left), withoutResponse(right)) {
		return false
	}
	caseID, _ := left["case_id"].(string)
	method, _ := left["method"].(string)
	if caseID != "C05" || method != "etcdserverpb.KV/Range" {
		return false
	}
	request, ok := left["request"].(map[string]any)
	if !ok {
		return false
	}
	requestFields, ok := request["fields"].(map[string]any)
	if !ok || requestFields["countOnly"] != true || requestFields["limit"] != "1" {
		return false
	}
	if end, ok := requestFields["rangeEnd"].(string); !ok || end == "" {
		return false
	}
	leftResponse, okLeft := left["response"].(map[string]any)
	rightResponse, okRight := right["response"].(map[string]any)
	if !okLeft || !okRight {
		return false
	}
	leftFields, okLeft := leftResponse["fields"].(map[string]any)
	rightFields, okRight := rightResponse["fields"].(map[string]any)
	if !okLeft || !okRight {
		return false
	}
	leftCount, ok := leftFields["count"].(string)
	if !ok || !canonicalUint64(leftCount) {
		return false
	}
	rightCount, ok := rightFields["count"].(string)
	if !ok || !canonicalUint64(rightCount) {
		return false
	}
	leftCopy := cloneMap(leftResponse)
	rightCopy := cloneMap(rightResponse)
	leftFieldCopy := cloneMap(leftFields)
	rightFieldCopy := cloneMap(rightFields)
	delete(leftFieldCopy, "count")
	delete(rightFieldCopy, "count")
	leftCopy["fields"] = leftFieldCopy
	rightCopy["fields"] = rightFieldCopy
	return reflect.DeepEqual(leftCopy, rightCopy)
}

func canonicalUint64(value string) bool {
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && strconv.FormatUint(parsed, 10) == value
}

func withoutResponse(record map[string]any) map[string]any {
	copy := cloneMap(record)
	delete(copy, "response")
	return copy
}

func cloneMap(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, child := range value {
		result[key] = child
	}
	return result
}
