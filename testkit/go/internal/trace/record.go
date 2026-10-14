package trace

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const SchemaVersion = "etcd-hbase-trace/v1"

const (
	maxRecordBytes  = 64 << 20
	maxTraceBytes   = 256 << 20
	maxTraceRecords = 10_000
)

type legacyProtoMessage interface {
	Reset()
	String() string
	ProtoMessage()
}

// Record is one request/response observation. Protobuf snapshots are built
// from generated field tags before JSON encoding, preserving bytes, 64-bit
// values, nested messages, and selected oneof branches.
type Record struct {
	Schema         string          `json:"schema"`
	CaseID         string          `json:"case_id"`
	Ordinal        uint64          `json:"ordinal"`
	Method         string          `json:"method"`
	Request        json.RawMessage `json:"request"`
	Response       json.RawMessage `json:"response"`
	Error          *Error          `json:"error"`
	StreamTerminal *StreamTerminal `json:"stream_terminal"`
	EvidenceScope  string          `json:"evidence_scope"`
	ElapsedNS      string          `json:"elapsed_ns"`
}

// Error stores stable classifications only. Error descriptions may contain
// endpoints, credentials, or request data, so they are never copied to traces.
type Error struct {
	GRPCCode     string `json:"grpc_code"`
	ConsumerKind string `json:"consumer_kind"`
}

type StreamTerminal struct {
	Kind     string `json:"kind"`
	GRPCCode string `json:"grpc_code,omitempty"`
}

type messageSnapshot struct {
	Fields map[string]any    `json:"fields"`
	Oneofs map[string]string `json:"oneofs"`
}

// SnapshotProto serializes known etcd protobuf fields. Authentication names,
// passwords, and tokens are recorded only as presence flags. Unknown wire
// fields fail closed rather than disappearing from a supposedly complete trace.
func SnapshotProto(message legacyProtoMessage) (json.RawMessage, error) {
	if message == nil {
		return nil, errors.New("nil protobuf message")
	}
	value := reflect.ValueOf(message)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, errors.New("protobuf message must be a non-nil pointer")
	}
	typeOf := value.Type().Elem()
	if typeOf.Kind() != reflect.Struct {
		return nil, errors.New("protobuf message must point to a generated struct")
	}
	if typeOf.PkgPath() == "" || !strings.HasPrefix(typeOf.PkgPath(), "go.etcd.io/etcd/api/v3/") {
		return nil, fmt.Errorf("unsupported protobuf package %q", typeOf.PkgPath())
	}
	if err := rejectUnknownFields(value); err != nil {
		return nil, err
	}
	authContext := strings.HasPrefix(typeOf.Name(), "Auth")
	snapshot, err := snapshotMessage(value, authContext)
	if err != nil {
		return nil, err
	}
	return json.Marshal(snapshot)
}

func ClassifyError(err error) *Error {
	if err == nil {
		return nil
	}
	kind := "go-error"
	grpcCode := status.Code(err)
	switch {
	case errors.Is(err, context.Canceled):
		kind = "context-canceled"
		grpcCode = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		kind = "context-deadline"
		grpcCode = codes.DeadlineExceeded
	default:
		var codeProvider interface{ Code() codes.Code }
		if errors.As(err, &codeProvider) {
			kind = "etcd-client-error"
			grpcCode = codeProvider.Code()
		} else if _, ok := status.FromError(err); ok {
			kind = "grpc-status"
		}
	}
	return &Error{GRPCCode: grpcCode.String(), ConsumerKind: kind}
}

func WriteJSONL(writer io.Writer, record Record) error {
	return writeJSONL(writer, record, maxRecordBytes)
}

func writeJSONL(writer io.Writer, record Record, maxBytes int) error {
	if writer == nil {
		return errors.New("trace writer is required")
	}
	if err := record.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode trace record: %w", err)
	}
	if len(encoded) > maxBytes {
		return fmt.Errorf("encoded trace record exceeds maximum %d-byte line", maxBytes)
	}
	line := append(encoded, '\n')
	written, err := writer.Write(line)
	if err != nil {
		return fmt.Errorf("write trace record: %w", err)
	}
	if written != len(line) {
		return io.ErrShortWrite
	}
	return nil
}

func ReadJSONL(reader io.Reader) ([]Record, error) {
	return readJSONL(reader, maxTraceBytes, maxTraceRecords)
}

func readJSONL(reader io.Reader, maxBytes int64, maxRecords int) ([]Record, error) {
	limitedReader := &io.LimitedReader{R: reader, N: maxBytes + 1}
	scanner := bufio.NewScanner(limitedReader)
	scanner.Buffer(make([]byte, 64*1024), maxRecordBytes)
	var records []Record
	lastOrdinal := make(map[string]uint64)
	line := 0
	for scanner.Scan() {
		if limitedReader.N == 0 {
			return nil, errors.New("trace exceeds maximum total size")
		}
		if len(records) >= maxRecords {
			return nil, errors.New("trace exceeds maximum record count")
		}
		line++
		payload := bytes.TrimSpace(scanner.Bytes())
		if len(payload) == 0 {
			return nil, fmt.Errorf("trace line %d is empty", line)
		}
		if err := rejectDuplicateJSONKeys(payload); err != nil {
			return nil, fmt.Errorf("trace line %d has invalid JSON: %w", line, err)
		}
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		var record Record
		if err := decoder.Decode(&record); err != nil {
			return nil, fmt.Errorf("decode trace line %d: invalid record JSON", line)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			if err == nil {
				return nil, fmt.Errorf("trace line %d contains multiple JSON values", line)
			}
			return nil, fmt.Errorf("trace line %d has trailing data", line)
		}
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("validate trace line %d: %w", line, err)
		}
		if previous, ok := lastOrdinal[record.CaseID]; ok && record.Ordinal <= previous {
			return nil, fmt.Errorf("trace line %d ordinal %d is not after previous ordinal %d", line, record.Ordinal, previous)
		}
		lastOrdinal[record.CaseID] = record.Ordinal
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read trace line %d: %w", line+1, err)
	}
	if limitedReader.N == 0 {
		return nil, errors.New("trace exceeds maximum total size")
	}
	return records, nil
}

func (record Record) Validate() error {
	if record.Schema != SchemaVersion {
		return errors.New("unsupported trace schema")
	}
	if record.CaseID == "" || !safeIdentifier(record.CaseID) {
		return errors.New("invalid case_id")
	}
	if record.Ordinal == 0 || record.Ordinal > 1<<53-1 {
		return errors.New("ordinal must be a positive safe JSON integer")
	}
	service, method, found := strings.Cut(record.Method, "/")
	if !found || service == "" || method == "" || strings.Contains(method, "/") || strings.ContainsAny(record.Method, " \t\r\n") {
		return errors.New("invalid method path")
	}
	switch record.EvidenceScope {
	case "transport", "transport-fixture", "native-etcd", "target-hbase":
	default:
		return errors.New("invalid evidence_scope")
	}
	elapsed, err := strconv.ParseUint(record.ElapsedNS, 10, 64)
	if err != nil || strconv.FormatUint(elapsed, 10) != record.ElapsedNS {
		return errors.New("elapsed_ns must be a canonical non-negative decimal string")
	}
	authMethod := strings.HasPrefix(record.Method, "etcdserverpb.Auth/")
	if err := validateObject(record.Request, "request", false, authMethod); err != nil {
		return err
	}
	if err := validateObject(record.Response, "response", true, authMethod); err != nil {
		return err
	}
	if record.Error != nil {
		if !validGRPCCode(record.Error.GRPCCode) {
			return errors.New("invalid grpc_code")
		}
		switch record.Error.ConsumerKind {
		case "grpc-status", "etcd-client-error", "context-canceled", "context-deadline", "go-error":
		default:
			return errors.New("invalid consumer_kind")
		}
	}
	if record.StreamTerminal != nil {
		switch record.StreamTerminal.Kind {
		case "eof", "grpc-status", "context-canceled", "context-deadline", "client-close":
		default:
			return errors.New("invalid stream terminal kind")
		}
		if record.StreamTerminal.Kind == "grpc-status" && !validGRPCCode(record.StreamTerminal.GRPCCode) {
			return errors.New("invalid stream terminal grpc_code")
		}
	}
	return nil
}

func snapshotMessage(value reflect.Value, authContext bool) (messageSnapshot, error) {
	value = indirect(value)
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return messageSnapshot{}, errors.New("invalid generated protobuf struct")
	}
	snapshot := messageSnapshot{Fields: make(map[string]any), Oneofs: make(map[string]string)}
	typeOf := value.Type()
	for index := 0; index < value.NumField(); index++ {
		field := typeOf.Field(index)
		fieldValue := value.Field(index)
		if field.Name == "XXX_unrecognized" {
			if fieldValue.Len() != 0 {
				return messageSnapshot{}, fmt.Errorf("unknown protobuf fields in %s cannot be represented losslessly", typeOf.Name())
			}
			continue
		}
		oneofName := field.Tag.Get("protobuf_oneof")
		if oneofName != "" {
			if fieldValue.IsNil() {
				continue
			}
			wrapper := indirect(fieldValue)
			if !wrapper.IsValid() || wrapper.Kind() != reflect.Struct {
				return messageSnapshot{}, fmt.Errorf("invalid oneof wrapper %s.%s", typeOf.Name(), field.Name)
			}
			for childIndex := 0; childIndex < wrapper.NumField(); childIndex++ {
				child := wrapper.Type().Field(childIndex)
				wireKind, protoName, jsonName := parseProtoTag(child.Tag.Get("protobuf"))
				if protoName == "" {
					continue
				}
				if isSensitiveName(protoName) || (authContext && containsName(protoName)) {
					snapshot.Fields[protoName+"_present"] = fieldPresent(wrapper.Field(childIndex))
					continue
				}
				converted, err := snapshotValue(wrapper.Field(childIndex), wireKind, authContext)
				if err != nil {
					return messageSnapshot{}, fmt.Errorf("snapshot %s.%s: %w", typeOf.Name(), protoName, err)
				}
				snapshot.Fields[jsonName] = converted
				snapshot.Oneofs[oneofName] = protoName
			}
			continue
		}
		wireKind, protoName, jsonName := parseProtoTag(field.Tag.Get("protobuf"))
		if protoName == "" || !fieldValue.CanInterface() {
			continue
		}
		if authContext && containsName(protoName) {
			if fieldValue.Kind() == reflect.Slice {
				snapshot.Fields[jsonName+"_count"] = fieldValue.Len()
			} else {
				snapshot.Fields[jsonName+"_present"] = fieldPresent(fieldValue)
			}
			continue
		}
		if isSensitiveName(protoName) {
			if authContext {
				snapshot.Fields[jsonName+"_present"] = fieldPresent(fieldValue)
			}
			continue
		}
		converted, err := snapshotValue(fieldValue, wireKind, authContext)
		if err != nil {
			return messageSnapshot{}, fmt.Errorf("snapshot %s.%s: %w", typeOf.Name(), protoName, err)
		}
		snapshot.Fields[jsonName] = converted
	}
	return snapshot, nil
}

func snapshotValue(value reflect.Value, wireKind string, authContext bool) (any, error) {
	if !value.IsValid() {
		return nil, nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil, nil
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, nil
		}
		if value.CanInterface() {
			if _, ok := value.Interface().(legacyProtoMessage); ok {
				return snapshotMessage(value, authContext)
			}
		}
		value = value.Elem()
	}
	if value.CanInterface() {
		if _, ok := value.Interface().(legacyProtoMessage); ok {
			return snapshotMessage(value, authContext)
		}
	}
	switch value.Kind() {
	case reflect.Bool:
		return value.Bool(), nil
	case reflect.Int64:
		return strconv.FormatInt(value.Int(), 10), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return value.Int(), nil
	case reflect.Uint64:
		return strconv.FormatUint(value.Uint(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
		if wireKind == "bytes" && value.Kind() == reflect.Uint8 {
			return base64.StdEncoding.EncodeToString([]byte{byte(value.Uint())}), nil
		}
		return value.Uint(), nil
	case reflect.Float32, reflect.Float64:
		floatValue := value.Float()
		if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
			return strconv.FormatFloat(floatValue, 'g', -1, value.Type().Bits()), nil
		}
		return floatValue, nil
	case reflect.String:
		return value.String(), nil
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return base64.StdEncoding.EncodeToString(value.Bytes()), nil
		}
		items := make([]any, value.Len())
		for index := 0; index < value.Len(); index++ {
			converted, err := snapshotValue(value.Index(index), wireKind, authContext)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", index, err)
			}
			items[index] = converted
		}
		return items, nil
	case reflect.Array:
		items := make([]any, value.Len())
		for index := 0; index < value.Len(); index++ {
			converted, err := snapshotValue(value.Index(index), wireKind, authContext)
			if err != nil {
				return nil, err
			}
			items[index] = converted
		}
		return items, nil
	case reflect.Map:
		result := make(map[string]any, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			key := fmt.Sprint(iterator.Key().Interface())
			converted, err := snapshotValue(iterator.Value(), wireKind, authContext)
			if err != nil {
				return nil, fmt.Errorf("map value %q: %w", key, err)
			}
			result[key] = converted
		}
		return result, nil
	case reflect.Struct:
		return snapshotMessage(value, authContext)
	default:
		return nil, fmt.Errorf("unsupported generated field kind %s", value.Kind())
	}
}

func rejectUnknownFields(value reflect.Value) error {
	value = indirect(value)
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return nil
	}
	for index := 0; index < value.NumField(); index++ {
		field := value.Type().Field(index)
		fieldValue := value.Field(index)
		if field.Name == "XXX_unrecognized" && fieldValue.Len() != 0 {
			return fmt.Errorf("unknown protobuf fields in %s cannot be represented losslessly", value.Type().Name())
		}
		if field.Tag.Get("protobuf_oneof") != "" {
			if !fieldValue.IsNil() {
				if err := rejectUnknownFields(fieldValue.Elem()); err != nil {
					return err
				}
			}
			continue
		}
		if !fieldValue.CanInterface() {
			continue
		}
		switch fieldValue.Kind() {
		case reflect.Pointer:
			if !fieldValue.IsNil() {
				if _, ok := fieldValue.Interface().(legacyProtoMessage); ok {
					if err := rejectUnknownFields(fieldValue); err != nil {
						return err
					}
				}
			}
		case reflect.Slice:
			if fieldValue.Type().Elem().Kind() == reflect.Pointer {
				for element := 0; element < fieldValue.Len(); element++ {
					if err := rejectUnknownFields(fieldValue.Index(element)); err != nil {
						return err
					}
				}
			}
		case reflect.Map:
			iterator := fieldValue.MapRange()
			for iterator.Next() {
				if iterator.Value().Kind() == reflect.Pointer {
					if err := rejectUnknownFields(iterator.Value()); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func parseProtoTag(tag string) (wireKind, protoName, jsonName string) {
	if tag == "" || tag == "-" {
		return "", "", ""
	}
	parts := strings.Split(tag, ",")
	wireKind = parts[0]
	for _, part := range parts[1:] {
		if strings.HasPrefix(part, "name=") {
			protoName = strings.TrimPrefix(part, "name=")
		}
		if strings.HasPrefix(part, "json=") {
			jsonName = strings.TrimPrefix(part, "json=")
		}
	}
	if jsonName == "" {
		jsonName = lowerCamel(protoName)
	}
	return wireKind, protoName, jsonName
}

func lowerCamel(value string) string {
	if value == "" || !strings.Contains(value, "_") {
		return value
	}
	parts := strings.Split(value, "_")
	for index := 1; index < len(parts); index++ {
		if parts[index] != "" {
			parts[index] = strings.ToUpper(parts[index][:1]) + parts[index][1:]
		}
	}
	return strings.Join(parts, "")
}

func indirect(value reflect.Value) reflect.Value {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	return value
}

func fieldPresent(value reflect.Value) bool {
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer || value.Kind() == reflect.Map || value.Kind() == reflect.Slice {
		return !value.IsNil()
	}
	return !value.IsZero()
}

func isSensitiveName(name string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(name, "_", ""), "-", ""))
	for _, marker := range []string{"password", "token", "authorization", "credential", "secret", "privatekey", "username"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func containsName(name string) bool {
	normalized := strings.ToLower(name)
	switch normalized {
	case "name", "names", "user", "users", "role", "roles":
		return true
	default:
		return strings.HasSuffix(normalized, "_name") || strings.HasSuffix(normalized, "_names")
	}
}

func validateObject(raw json.RawMessage, label string, allowNull, authMethod bool) error {
	if len(raw) == 0 {
		return fmt.Errorf("%s is required", label)
	}
	if len(raw) > maxRecordBytes {
		return fmt.Errorf("%s exceeds maximum trace snapshot size", label)
	}
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return fmt.Errorf("%s contains invalid JSON: %w", label, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("%s is invalid JSON: %w", label, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%s contains trailing JSON", label)
	}
	if decoded == nil && allowNull {
		return nil
	}
	snapshot, ok := decoded.(map[string]any)
	if !ok {
		return fmt.Errorf("%s must be a JSON object", label)
	}
	if len(snapshot) != 2 {
		return fmt.Errorf("%s must contain a protobuf snapshot envelope", label)
	}
	_, hasFields := snapshot["fields"].(map[string]any)
	oneofs, hasOneofs := snapshot["oneofs"].(map[string]any)
	if !hasFields || !hasOneofs {
		return fmt.Errorf("%s must contain object fields and oneofs", label)
	}
	for _, selected := range oneofs {
		if _, ok := selected.(string); !ok {
			return fmt.Errorf("%s oneof selections must be strings", label)
		}
	}
	if key := findSecretKey(decoded, authMethod); key != "" {
		return fmt.Errorf("%s contains a forbidden secret field", label)
	}
	return nil
}

func rejectDuplicateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("contains multiple JSON values")
		}
		return fmt.Errorf("has trailing data: %w", err)
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return errors.New("duplicate JSON object key")
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return errors.New("unterminated JSON object")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return errors.New("unterminated JSON array")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
	return nil
}

func findSecretKey(value any, authMethod bool) string {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "_", ""), "-", ""))
			lowerKey := strings.ToLower(key)
			if strings.HasSuffix(lowerKey, "_present") || strings.HasSuffix(key, "Present") {
				if _, ok := child.(bool); !ok {
					return key
				}
			} else if strings.HasSuffix(lowerKey, "_count") || (len(key) > len("count") && strings.HasSuffix(key, "Count")) {
				if number, ok := child.(json.Number); !ok || !isNonNegativeInteger(number) {
					return key
				}
			} else if isSensitiveName(normalized) || (authMethod && normalized == "name") {
				return key
			}
			if nested := findSecretKey(child, authMethod); nested != "" {
				return nested
			}
		}
	case []any:
		for _, child := range typed {
			if nested := findSecretKey(child, authMethod); nested != "" {
				return nested
			}
		}
	}
	return ""
}

func isNonNegativeInteger(number json.Number) bool {
	parsed, err := strconv.ParseUint(number.String(), 10, 64)
	return err == nil && strconv.FormatUint(parsed, 10) == number.String()
}

func validGRPCCode(value string) bool {
	for code := codes.OK; code <= codes.Unauthenticated; code++ {
		if code.String() == value && code != codes.OK {
			return true
		}
	}
	return false
}

func safeIdentifier(value string) bool {
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}
