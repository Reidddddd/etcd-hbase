package scenario

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

type runner struct {
	client        *clientv3.Client
	writer        io.Writer
	ordinals      map[string]uint64
	evidenceScope string
}

func (runner *runner) call(ctx context.Context, caseID, method string, request protoMessage, operation func(context.Context) (protoMessage, error)) (protoMessage, error) {
	response, err := runner.callWithError(ctx, caseID, method, request, operation)
	if err != nil {
		return nil, fmt.Errorf("scenario %s failed with grpc_code=%s: %s", method,
			status.Code(err), status.Convert(err).Message())
	}
	return response, nil
}

func (runner *runner) callWithError(ctx context.Context, caseID, method string, request protoMessage, operation func(context.Context) (protoMessage, error)) (protoMessage, error) {
	requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	started := time.Now()
	response, err := operation(requestCtx)
	if isNilMessage(response) {
		response = nil
	}
	if recordErr := runner.record(caseID, method, request, response, err, nil, time.Since(started)); recordErr != nil {
		return response, fmt.Errorf("record %s result: %w", method, recordErr)
	}
	return response, err
}

func (runner *runner) record(caseID, method string, request, response protoMessage, callErr error, terminal *trace.StreamTerminal, elapsed time.Duration) error {
	requestSnapshot, err := trace.SnapshotProto(request)
	if err != nil {
		return fmt.Errorf("snapshot %s request: %w", method, err)
	}
	responseSnapshot, err := trace.SnapshotNullableProto(response)
	if err != nil {
		return fmt.Errorf("snapshot %s response: %w", method, err)
	}

	runner.ordinals[caseID]++
	record := trace.Record{
		Schema: trace.SchemaVersion, CaseID: caseID, Ordinal: runner.ordinals[caseID], Method: method,
		Request: requestSnapshot, Response: responseSnapshot, Error: trace.ClassifyError(callErr),
		StreamTerminal: terminal, EvidenceScope: runner.evidenceScope, ElapsedNS: strconv.FormatInt(elapsed.Nanoseconds(), 10),
	}
	if err := trace.WriteJSONL(runner.writer, record); err != nil {
		return fmt.Errorf("write %s trace: %w", method, err)
	}
	return nil
}

func isNilMessage(message protoMessage) bool {
	if message == nil {
		return true
	}
	value := reflect.ValueOf(message)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func prefixRangeEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for index := len(end) - 1; index >= 0; index-- {
		if end[index] < 0xff {
			end[index]++
			return end[:index+1]
		}
	}
	return []byte{0}
}

type protoMessage interface {
	Reset()
	String() string
	ProtoMessage()
}
