package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io.etcdhbase/testkit/internal/client"
	"io.etcdhbase/testkit/internal/trace"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("m0driver", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "127.0.0.1:2379", "etcd-compatible gRPC endpoint")
	caseID := flags.String("case", "range-bytes", "trace case identifier")
	tracePath := flags.String("trace", "-", "JSONL output path, or - for stdout")
	mode := flags.String("mode", "clientv3", "clientv3 or raw apiv3 stub")
	scope := flags.String("scope", "transport", "transport, transport-fixture, native-etcd, or target-hbase")
	timeout := flags.Duration("timeout", 5*time.Second, "request deadline")
	keyB64 := flags.String("key-b64", "YQD/", "request key as base64")
	rangeEndB64 := flags.String("range-end-b64", "", "optional range end as base64")
	revisionText := flags.String("revision", "0", "non-negative revision as decimal int64")
	countOnly := flags.Bool("count-only", false, "send a CountOnly Range request")
	limit := flags.Int64("limit", 0, "optional positive Range limit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *mode != "clientv3" && *mode != "raw" {
		return errors.New("mode must be clientv3 or raw")
	}
	switch *scope {
	case "transport", "transport-fixture":
	default:
		return errors.New("scope must be transport or transport-fixture for the Go driver")
	}
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if *limit < 0 {
		return errors.New("limit cannot be negative")
	}
	key, err := base64.StdEncoding.DecodeString(*keyB64)
	if err != nil {
		return errors.New("key-b64 is invalid base64")
	}
	rangeEnd, err := base64.StdEncoding.DecodeString(*rangeEndB64)
	if err != nil {
		return errors.New("range-end-b64 is invalid base64")
	}
	revision, err := strconv.ParseInt(*revisionText, 10, 64)
	if err != nil || revision < 0 {
		return errors.New("revision must be a non-negative decimal int64")
	}

	traceWriter, closeTrace, err := openTrace(*tracePath, stdout)
	if err != nil {
		return err
	}
	defer closeTrace()

	clientOptions := client.Options{Endpoint: *endpoint, Timeout: *timeout}
	clientConnection, err := client.New(clientOptions)
	if err != nil {
		return errors.New("client initialization failed")
	}
	defer func() { _ = clientConnection.Close() }()

	request := &etcdserverpb.RangeRequest{
		Key:       key,
		RangeEnd:  rangeEnd,
		Revision:  revision,
		CountOnly: *countOnly,
		Limit:     *limit,
	}
	requestSnapshot, err := trace.SnapshotProto(request)
	if err != nil {
		return errors.New("request snapshot failed")
	}
	preflight := trace.Record{
		Schema:        trace.SchemaVersion,
		CaseID:        *caseID,
		Ordinal:       1,
		Method:        "etcdserverpb.KV/Range",
		Request:       requestSnapshot,
		Response:      json.RawMessage("null"),
		EvidenceScope: *scope,
		ElapsedNS:     "0",
	}
	if err := preflight.Validate(); err != nil {
		return errors.New("case metadata is invalid")
	}
	ctx, cancel, err := client.WithDeadline(context.Background(), *timeout)
	if err != nil {
		return err
	}
	defer cancel()

	started := time.Now()
	var response protoMessage
	switch *mode {
	case "raw":
		response, err = client.RawKV(clientConnection).Range(ctx, request)
	case "clientv3":
		options := make([]clientv3.OpOption, 0, 4)
		if len(rangeEnd) == 1 && rangeEnd[0] == 0 {
			options = append(options, clientv3.WithFromKey())
		} else if len(rangeEnd) != 0 {
			options = append(options, clientv3.WithRange(string(rangeEnd)))
		}
		if revision != 0 {
			options = append(options, clientv3.WithRev(revision))
		}
		if *countOnly {
			options = append(options, clientv3.WithCountOnly())
		}
		if *limit != 0 {
			options = append(options, clientv3.WithLimit(*limit))
		}
		result, requestErr := clientConnection.KV.Get(ctx, string(key), options...)
		if result != nil {
			response = (*etcdserverpb.RangeResponse)(result)
		}
		err = requestErr
	}
	elapsed := time.Since(started)
	responseSnapshot, snapshotErr := snapshotResponse(response)
	if snapshotErr != nil {
		return errors.New("response snapshot failed")
	}
	record := trace.Record{
		Schema:        trace.SchemaVersion,
		CaseID:        *caseID,
		Ordinal:       1,
		Method:        "etcdserverpb.KV/Range",
		Request:       requestSnapshot,
		Response:      responseSnapshot,
		Error:         trace.ClassifyError(err),
		EvidenceScope: *scope,
		ElapsedNS:     strconv.FormatInt(elapsed.Nanoseconds(), 10),
	}
	if writeErr := trace.WriteJSONL(traceWriter, record); writeErr != nil {
		return errors.New("trace write failed")
	}
	if err != nil {
		return fmt.Errorf("request failed: grpc_code=%s", record.Error.GRPCCode)
	}
	return nil
}

type protoMessage interface {
	Reset()
	String() string
	ProtoMessage()
}

func snapshotResponse(message protoMessage) (json.RawMessage, error) {
	if message == nil {
		return json.RawMessage("null"), nil
	}
	value := reflect.ValueOf(message)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return json.RawMessage("null"), nil
	}
	return trace.SnapshotProto(message)
}

func openTrace(path string, stdout io.Writer) (io.Writer, func(), error) {
	if path == "-" {
		return stdout, func() {}, nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, func() {}, errors.New("trace output could not be opened")
	}
	return file, func() { _ = file.Close() }, nil
}
