package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"io.etcdhbase/testkit/internal/client"
	"io.etcdhbase/testkit/internal/trace"
)

const protocolRevision int64 = 1 << 54

var (
	binaryKey = []byte{0x00, 0x00, 0xff}
	probeKeys = map[byte][]byte{
		'a': {0x61, 0x00, 0xff},
		'p': {0x70, 0x00, 0xff},
		'u': {0x75, 0x00, 0xff},
		'r': {0x72, 0x00, 0xff},
		'd': {0x64, 0x00, 0xff},
	}
)

type Options struct {
	SkeletonEndpoint string
	FixtureEndpoint  string
	Timeout          time.Duration
}

func (options Options) Validate() error {
	if options.SkeletonEndpoint == "" {
		return errors.New("skeleton endpoint is required")
	}
	if options.FixtureEndpoint == "" {
		return errors.New("fixture endpoint is required")
	}
	if options.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	return nil
}

// Run exercises the real Java skeleton and the separate test-only failure fixture.
// Every expected failure is recorded before its status and sentinel are checked.
func Run(ctx context.Context, options Options, output io.Writer) error {
	if ctx == nil {
		return errors.New("probe context is required")
	}
	if output == nil {
		return errors.New("trace output is required")
	}
	if err := options.Validate(); err != nil {
		return err
	}

	skeleton, err := client.New(client.Options{Endpoint: options.SkeletonEndpoint, Timeout: options.Timeout})
	if err != nil {
		return errors.New("skeleton client initialization failed")
	}
	defer func() { _ = skeleton.Close() }()
	fixture, err := client.New(client.Options{Endpoint: options.FixtureEndpoint, Timeout: options.Timeout})
	if err != nil {
		return errors.New("failure fixture client initialization failed")
	}
	defer func() { _ = fixture.Close() }()

	if err := probeSkeletonUnary(ctx, options, skeleton, output); err != nil {
		return err
	}
	if err := probeSkeletonStreams(ctx, options, skeleton, fixture, output); err != nil {
		return err
	}
	if err := probeFixtureErrors(ctx, options, fixture, output); err != nil {
		return err
	}
	return nil
}

func probeSkeletonUnary(
	ctx context.Context,
	options Options,
	connection *clientv3.Client,
	output io.Writer,
) error {
	request := &etcdserverpb.RangeRequest{Key: binaryKey, Revision: protocolRevision}

	rpcContext, cancel := context.WithTimeout(ctx, options.Timeout)
	started := time.Now()
	_, callErr := connection.KV.Get(rpcContext, string(binaryKey), clientv3.WithRev(protocolRevision))
	callErr = clientv3.ContextError(rpcContext, callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-skeleton-clientv3", "transport", request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectError(callErr, codes.Unimplemented, nil); err != nil {
		return fmt.Errorf("clientv3 KV.Get did not reach the skeleton Range handler: %w", err)
	}

	rawRequest := &etcdserverpb.RangeRequest{Key: binaryKey, Revision: protocolRevision}
	rpcContext, cancel = context.WithTimeout(ctx, options.Timeout)
	started = time.Now()
	_, callErr = client.RawKV(connection).Range(rpcContext, rawRequest)
	callErr = clientv3.ContextError(rpcContext, callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-skeleton-raw-range", "transport", rawRequest, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectError(callErr, codes.Unimplemented, nil); err != nil {
		return fmt.Errorf("raw apiv3 KV.Range did not reach the skeleton handler: %w", err)
	}
	return nil
}

func probeSkeletonStreams(
	ctx context.Context,
	options Options,
	connection *clientv3.Client,
	fixture *clientv3.Client,
	output io.Writer,
) error {
	watchRequest := &etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: binaryKey, StartRevision: protocolRevision},
		},
	}
	rpcContext, cancel := context.WithTimeout(ctx, options.Timeout)
	started := time.Now()
	watchStream, err := client.RawWatch(connection).Watch(rpcContext)
	if err == nil {
		err = watchStream.Send(watchRequest)
	}
	if err == nil {
		_, err = watchStream.Recv()
	}
	if watchStream != nil {
		_ = watchStream.CloseSend()
	}
	err = clientv3.ContextError(rpcContext, err)
	cancel()
	if recordErr := writeStreamRecord(
		output,
		"wire-skeleton-watch",
		"transport",
		"etcdserverpb.Watch/Watch",
		watchRequest,
		err,
		grpcTerminal(err),
		time.Since(started),
	); recordErr != nil {
		return recordErr
	}
	if err := expectError(err, codes.Unimplemented, nil); err != nil {
		return fmt.Errorf("Watch.Watch did not terminate as UNIMPLEMENTED: %w", err)
	}

	leaseRequest := &etcdserverpb.LeaseKeepAliveRequest{ID: protocolRevision}
	rpcContext, cancel = context.WithTimeout(ctx, options.Timeout)
	started = time.Now()
	leaseStream, err := etcdserverpb.NewLeaseClient(connection.ActiveConnection()).LeaseKeepAlive(rpcContext)
	if err == nil {
		err = leaseStream.Send(leaseRequest)
	}
	if err == nil {
		_, err = leaseStream.Recv()
	}
	if leaseStream != nil {
		_ = leaseStream.CloseSend()
	}
	err = clientv3.ContextError(rpcContext, err)
	cancel()
	if recordErr := writeStreamRecord(
		output,
		"wire-skeleton-lease-keepalive",
		"transport",
		"etcdserverpb.Lease/LeaseKeepAlive",
		leaseRequest,
		err,
		grpcTerminal(err),
		time.Since(started),
	); recordErr != nil {
		return recordErr
	}
	if err := expectError(err, codes.Unimplemented, nil); err != nil {
		return fmt.Errorf("Lease.LeaseKeepAlive did not terminate as UNIMPLEMENTED: %w", err)
	}

	cancelTimeout := min(options.Timeout, time.Second)
	cancelContext, cancel := context.WithTimeout(ctx, cancelTimeout)
	started = time.Now()
	cancelStream, err := client.RawWatch(fixture).Watch(cancelContext)
	if err == nil {
		err = cancelStream.Send(watchRequest)
	}
	if err == nil {
		_, err = cancelStream.Recv()
	}
	if cancelStream != nil {
		_ = cancelStream.CloseSend()
	}
	err = clientv3.ContextError(cancelContext, err)
	cancel()
	if recordErr := writeStreamRecord(
		output,
		"wire-fixture-watch-cancel",
		"transport-fixture",
		"etcdserverpb.Watch/Watch",
		watchRequest,
		err,
		trace.StreamTerminal{Kind: "context-deadline"},
		time.Since(started),
	); recordErr != nil {
		return recordErr
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("cancelled Watch did not return context.DeadlineExceeded: %w", err)
	}
	return nil
}

func probeFixtureErrors(
	ctx context.Context,
	options Options,
	connection *clientv3.Client,
	output io.Writer,
) error {
	requestFor := func(marker byte) *etcdserverpb.RangeRequest {
		return &etcdserverpb.RangeRequest{Key: probeKeys[marker], Revision: protocolRevision}
	}

	request := requestFor('a')
	rpcContext, cancel := context.WithTimeout(ctx, options.Timeout)
	started := time.Now()
	_, callErr := connection.KV.Get(rpcContext, string(request.Key), clientv3.WithRev(protocolRevision))
	callErr = clientv3.ContextError(rpcContext, callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-fixture-clientv3-auth", "transport-fixture", request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectError(callErr, codes.InvalidArgument, rpctypes.ErrAuthFailed); err != nil {
		return fmt.Errorf("clientv3 authentication sentinel mismatch: %w", err)
	}

	request = requestFor('p')
	rpcContext, cancel = context.WithTimeout(ctx, options.Timeout)
	started = time.Now()
	_, callErr = client.RawKV(connection).Range(rpcContext, request)
	callErr = rpctypes.Error(callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-fixture-raw-permission", "transport-fixture", request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectError(callErr, codes.PermissionDenied, rpctypes.ErrPermissionDenied); err != nil {
		return fmt.Errorf("raw apiv3 permission sentinel mismatch: %w", err)
	}

	request = requestFor('u')
	rpcContext, cancel = context.WithTimeout(ctx, options.Timeout)
	started = time.Now()
	_, callErr = client.RawKV(connection).Range(rpcContext, request)
	callErr = rpctypes.Error(callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-fixture-raw-unavailable", "transport-fixture", request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectError(callErr, codes.Unavailable, nil); err != nil {
		return fmt.Errorf("raw apiv3 unavailable mapping mismatch: %w", err)
	}

	request = requestFor('r')
	boundedRetryTimeout := min(options.Timeout, 300*time.Millisecond)
	rpcContext, cancel = context.WithTimeout(ctx, boundedRetryTimeout)
	started = time.Now()
	_, callErr = connection.KV.Get(rpcContext, string(request.Key), clientv3.WithRev(protocolRevision))
	callErr = clientv3.ContextError(rpcContext, callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-fixture-clientv3-retry-deadline", "transport-fixture", request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectDeadline(callErr); err != nil {
		return fmt.Errorf("clientv3 retry did not stop at its deadline: %w", err)
	}

	request = requestFor('d')
	requestTimeout := min(options.Timeout, 300*time.Millisecond)
	rpcContext, cancel = context.WithTimeout(ctx, requestTimeout)
	started = time.Now()
	_, callErr = client.RawKV(connection).Range(rpcContext, request)
	callErr = clientv3.ContextError(rpcContext, callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-fixture-raw-deadline-cancel", "transport-fixture", request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectDeadline(callErr); err != nil {
		return fmt.Errorf("raw apiv3 deadline did not cancel the request: %w", err)
	}
	return nil
}

func expectDeadline(err error) error {
	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("got %v, want context.DeadlineExceeded", err)
	}
	classified := trace.ClassifyError(err)
	if classified == nil || classified.GRPCCode != codes.DeadlineExceeded.String() {
		return fmt.Errorf("got trace error %v, want gRPC code %s", classified, codes.DeadlineExceeded)
	}
	return nil
}

func expectError(err error, expectedCode codes.Code, sentinel error) error {
	if err == nil {
		return fmt.Errorf("expected gRPC code %s, got success", expectedCode)
	}
	if code := errorCode(err); code != expectedCode {
		return fmt.Errorf("got gRPC code %s, want %s", code, expectedCode)
	}
	if sentinel != nil && !errors.Is(err, sentinel) {
		return fmt.Errorf("error %T does not match client sentinel %v", err, sentinel)
	}
	return nil
}

func errorCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	var codeProvider interface{ Code() codes.Code }
	if errors.As(err, &codeProvider) {
		return codeProvider.Code()
	}
	return status.Code(err)
}

func grpcTerminal(err error) trace.StreamTerminal {
	return trace.StreamTerminal{Kind: "grpc-status", GRPCCode: errorCode(err).String()}
}

type legacyProtoMessage interface {
	Reset()
	String() string
	ProtoMessage()
}

func writeUnaryRecord(
	output io.Writer,
	caseID string,
	scope string,
	request legacyProtoMessage,
	err error,
	elapsed time.Duration,
) error {
	return writeRecord(output, caseID, scope, "etcdserverpb.KV/Range", request, err, nil, elapsed)
}

func writeStreamRecord(
	output io.Writer,
	caseID string,
	scope string,
	method string,
	request legacyProtoMessage,
	err error,
	terminal trace.StreamTerminal,
	elapsed time.Duration,
) error {
	return writeRecord(output, caseID, scope, method, request, err, &terminal, elapsed)
}

func writeRecord(
	output io.Writer,
	caseID string,
	scope string,
	method string,
	request legacyProtoMessage,
	err error,
	terminal *trace.StreamTerminal,
	elapsed time.Duration,
) error {
	requestJSON, snapshotErr := trace.SnapshotProto(request)
	if snapshotErr != nil {
		return errors.New("request snapshot failed")
	}
	record := trace.Record{
		Schema:         trace.SchemaVersion,
		CaseID:         caseID,
		Ordinal:        1,
		Method:         method,
		Request:        requestJSON,
		Response:       json.RawMessage("null"),
		Error:          trace.ClassifyError(err),
		StreamTerminal: terminal,
		EvidenceScope:  scope,
		ElapsedNS:      strconv.FormatInt(max(0, elapsed.Nanoseconds()), 10),
	}
	if err := trace.WriteJSONL(output, record); err != nil {
		return errors.New("wire trace record could not be written")
	}
	return nil
}
