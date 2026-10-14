package scenario

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io.etcdhbase/testkit/internal/trace"
	"sigs.k8s.io/yaml"
)

const (
	protocolVersion = "etcd-v3.5.23"
	sourceRevision  = "1cf4bc40111fd95d1e14a90a09a146357b15eb13"
	requestTimeout  = 8 * time.Second
	readyTimeout    = 30 * time.Second
)

var expectedSteps = []Step{
	{CaseID: "C01", ID: "binary-put-get"},
	{CaseID: "C03", ID: "prefix-limit-more"},
	{CaseID: "C05", ID: "prefix-count-only-limit-one"},
	{CaseID: "C11", ID: "txn-compare-success-branch"},
	{CaseID: "C12", ID: "txn-compare-failure-branch"},
	{CaseID: "C09", ID: "delete-prev-kv"},
	{CaseID: "C15", ID: "lease-grant"},
	{CaseID: "C16", ID: "put-with-lease"},
	{CaseID: "C17", ID: "lease-keepalive-stream"},
	{CaseID: "C18", ID: "lease-revoke-and-key-removal"},
	{CaseID: "C19", ID: "list-revision-then-watch-r-plus-one"},
	{CaseID: "C14", ID: "compact"},
	{CaseID: "C06", ID: "old-revision-after-compact"},
}

type Suite struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	Protocol      string `json:"protocol"`
	Steps         []Step `json:"steps"`
}

type Step struct {
	CaseID string `json:"case_id"`
	ID     string `json:"id"`
}

type Module struct {
	Path    string `json:"path"`
	Version string `json:"version"`
}

type Metadata struct {
	ScenarioID     string   `json:"scenario_id"`
	Protocol       string   `json:"protocol"`
	SourceRevision string   `json:"source_revision"`
	GoVersion      string   `json:"go_version"`
	Distribution   string   `json:"distribution"`
	Modules        []Module `json:"modules"`
}

func LoadSuite(path string) (Suite, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Suite{}, fmt.Errorf("read native scenario: %w", err)
	}
	var suite Suite
	if err := yaml.UnmarshalStrict(data, &suite); err != nil {
		return Suite{}, fmt.Errorf("parse native scenario: %w", err)
	}
	if err := validateSuite(suite); err != nil {
		return Suite{}, err
	}
	return suite, nil
}

func validateSuite(suite Suite) error {
	if suite.SchemaVersion != 1 || suite.ID != "m0-native-v1" || suite.Protocol != protocolVersion {
		return errors.New("native scenario identity or protocol version is not pinned")
	}
	if len(suite.Steps) != len(expectedSteps) {
		return fmt.Errorf("native scenario has %d steps, expected %d", len(suite.Steps), len(expectedSteps))
	}
	for index, expected := range expectedSteps {
		if suite.Steps[index] != expected {
			return fmt.Errorf("native scenario step %d is %q/%s, expected %q/%s", index+1, suite.Steps[index].CaseID, suite.Steps[index].ID, expected.CaseID, expected.ID)
		}
	}
	return nil
}

func BuildMetadata(suite Suite) (Metadata, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return Metadata{}, errors.New("Go build information is unavailable")
	}
	modules := make([]Module, 0, len(info.Deps))
	for _, dependency := range info.Deps {
		if dependency.Path == "go.etcd.io/etcd/server/v3" ||
			dependency.Path == "go.etcd.io/etcd/api/v3" ||
			dependency.Path == "go.etcd.io/etcd/client/v3" {
			modules = append(modules, Module{Path: dependency.Path, Version: dependency.Version})
		}
	}
	sort.Slice(modules, func(left, right int) bool { return modules[left].Path < modules[right].Path })
	if len(modules) != 3 {
		return Metadata{}, fmt.Errorf("expected build metadata for server, api, and client modules; got %d", len(modules))
	}
	for _, module := range modules {
		if module.Version != "v3.5.23" {
			return Metadata{}, fmt.Errorf("module %s is %s, expected v3.5.23", module.Path, module.Version)
		}
	}
	return Metadata{
		ScenarioID: suite.ID, Protocol: suite.Protocol, SourceRevision: sourceRevision,
		GoVersion: runtime.Version(), Distribution: "embedded Go library; no upstream etcd binary or container image validated",
		Modules: modules,
	}, nil
}

// Run starts a private embedded etcd node, executes the pinned native suite,
// and writes lossless request/response observations as JSONL.
func Run(ctx context.Context, suite Suite, dataDir string, output io.Writer) (Metadata, error) {
	if ctx == nil {
		return Metadata{}, errors.New("scenario context is required")
	}
	if err := validateSuite(suite); err != nil {
		return Metadata{}, fmt.Errorf("native scenario was not loaded from the pinned manifest: %w", err)
	}
	if output == nil {
		return Metadata{}, errors.New("trace output writer is required")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return Metadata{}, fmt.Errorf("create isolated etcd data directory: %w", err)
	}
	server, err := startEmbedded(ctx, dataDir)
	if err != nil {
		return Metadata{}, err
	}
	defer server.close()

	metadata, err := BuildMetadata(suite)
	if err != nil {
		return Metadata{}, err
	}
	runner := &runner{client: server.client, writer: output, ordinals: make(map[string]uint64)}
	if err := runner.run(ctx); err != nil {
		return metadata, err
	}
	return metadata, nil
}

type embedded struct {
	server *embed.Etcd
	client *clientv3.Client
}

func startEmbedded(parent context.Context, dataDir string) (*embedded, error) {
	peerURL, err := loopbackURL()
	if err != nil {
		return nil, fmt.Errorf("allocate peer listener: %w", err)
	}
	clientURL, err := loopbackURL()
	if err != nil {
		return nil, fmt.Errorf("allocate client listener: %w", err)
	}
	cfg := embed.NewConfig()
	cfg.Name = "m0-native"
	cfg.Dir = filepath.Join(dataDir, "data")
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	cfg.ClusterState = embed.ClusterStateFlagNew
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{"stderr"}
	server, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, fmt.Errorf("start embedded etcd: %w", err)
	}
	startCtx, cancel := context.WithTimeout(parent, readyTimeout)
	defer cancel()
	select {
	case <-server.Server.ReadyNotify():
	case err := <-server.Err():
		server.Close()
		return nil, fmt.Errorf("embedded etcd exited before ready: %w", err)
	case <-startCtx.Done():
		server.Server.Stop()
		server.Close()
		return nil, fmt.Errorf("embedded etcd ready timeout: %w", startCtx.Err())
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{clientURL.Host}, DialTimeout: requestTimeout,
	})
	if err != nil {
		server.Close()
		return nil, fmt.Errorf("connect to embedded etcd: %w", err)
	}
	return &embedded{server: server, client: client}, nil
}

func (server *embedded) close() {
	if server.client != nil {
		_ = server.client.Close()
	}
	if server.server != nil {
		server.server.Close()
	}
}

func loopbackURL() (*url.URL, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return nil, err
	}
	return &url.URL{Scheme: "http", Host: address}, nil
}

type runner struct {
	client   *clientv3.Client
	writer   io.Writer
	ordinals map[string]uint64
}

func (runner *runner) run(ctx context.Context) error {
	kv := etcdserverpb.NewKVClient(runner.client.ActiveConnection())
	lease := etcdserverpb.NewLeaseClient(runner.client.ActiveConnection())
	watch := etcdserverpb.NewWatchClient(runner.client.ActiveConnection())

	baselineReq := &etcdserverpb.RangeRequest{Key: []byte("m0/scenario-baseline")}
	baselineResult, err := runner.call(ctx, "native-baseline", "etcdserverpb.KV/Range", baselineReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, baselineReq)
	})
	if err != nil {
		return err
	}
	baseline, ok := baselineResult.(*etcdserverpb.RangeResponse)
	if !ok || baseline.Header == nil || baseline.Header.Revision < 1 {
		return errors.New("native baseline has no valid revision header")
	}

	binaryKey := []byte{0x00, 0xff, 'm', '0'}
	binaryValue := []byte{0x00, 0x80, 0xff, 'v'}
	putReq := &etcdserverpb.PutRequest{Key: binaryKey, Value: binaryValue}
	putResp, err := runner.call(ctx, "C01", "etcdserverpb.KV/Put", putReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Put(requestCtx, putReq)
	})
	if err != nil {
		return err
	}
	if putResp == nil {
		return errors.New("native binary Put returned no response")
	}
	rangeReq := &etcdserverpb.RangeRequest{Key: binaryKey}
	rangeResp, err := runner.call(ctx, "C01", "etcdserverpb.KV/Range", rangeReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, rangeReq)
	})
	if err != nil {
		return err
	}
	binaryRange, ok := rangeResp.(*etcdserverpb.RangeResponse)
	if !ok || len(binaryRange.Kvs) != 1 || string(binaryRange.Kvs[0].Key) != string(binaryKey) || string(binaryRange.Kvs[0].Value) != string(binaryValue) {
		return errors.New("native binary Range did not return the original key and value")
	}

	for index, value := range [][]byte{[]byte("one"), []byte("two"), []byte("three")} {
		key := []byte(fmt.Sprintf("m0/prefix/%d", index+1))
		request := &etcdserverpb.PutRequest{Key: key, Value: value}
		if _, err := runner.call(ctx, "C01", "etcdserverpb.KV/Put", request, func(requestCtx context.Context) (protoMessage, error) {
			return kv.Put(requestCtx, request)
		}); err != nil {
			return err
		}
	}
	prefix := []byte("m0/prefix/")
	prefixEnd := prefixRangeEnd(prefix)
	limited := &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: prefixEnd, Limit: 2}
	limitedResp, err := runner.call(ctx, "C03", "etcdserverpb.KV/Range", limited, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, limited)
	})
	if err != nil {
		return err
	}
	limitedRange, ok := limitedResp.(*etcdserverpb.RangeResponse)
	if !ok || len(limitedRange.Kvs) != 2 || !limitedRange.More {
		return errors.New("native prefix Range did not preserve Limit and More")
	}
	countOnly := &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: prefixEnd, Limit: 1, CountOnly: true}
	countResp, err := runner.call(ctx, "C05", "etcdserverpb.KV/Range", countOnly, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, countOnly)
	})
	if err != nil {
		return err
	}
	countRange, ok := countResp.(*etcdserverpb.RangeResponse)
	if !ok || len(countRange.Kvs) != 0 || countRange.Count != 3 || countRange.More {
		return errors.New("native CountOnly Range did not return the expected count, empty KVs, and More=false")
	}

	compareVersion := &etcdserverpb.Compare{
		Result: etcdserverpb.Compare_EQUAL, Target: etcdserverpb.Compare_VERSION,
		Key: binaryKey, TargetUnion: &etcdserverpb.Compare_Version{Version: 1},
	}
	txnSuccess := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{compareVersion},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("m0/txn/success"), Value: []byte("then")}}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("m0/txn/wrong-success-failure"), Value: []byte("else")}}}},
	}
	result, err := runner.call(ctx, "C11", "etcdserverpb.KV/Txn", txnSuccess, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Txn(requestCtx, txnSuccess)
	})
	if err != nil {
		return err
	}
	if response, ok := result.(*etcdserverpb.TxnResponse); !ok || !response.Succeeded || len(response.Responses) != 1 {
		return errors.New("native Txn did not execute its matching success branch")
	}
	txnFailure := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Result: etcdserverpb.Compare_EQUAL, Target: etcdserverpb.Compare_VALUE,
			Key: binaryKey, TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("not-the-value")},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("m0/txn/wrong-failure-success"), Value: []byte("then")}}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("m0/txn/failure"), Value: []byte("else")}}}},
	}
	result, err = runner.call(ctx, "C12", "etcdserverpb.KV/Txn", txnFailure, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Txn(requestCtx, txnFailure)
	})
	if err != nil {
		return err
	}
	if response, ok := result.(*etcdserverpb.TxnResponse); !ok || response.Succeeded || len(response.Responses) != 1 {
		return errors.New("native Txn did not execute its matching failure branch")
	}

	deleteKey := []byte("m0/delete/prev-kv")
	deletePut := &etcdserverpb.PutRequest{Key: deleteKey, Value: []byte("before-delete")}
	if _, err := runner.call(ctx, "C01", "etcdserverpb.KV/Put", deletePut, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Put(requestCtx, deletePut)
	}); err != nil {
		return err
	}
	deleteReq := &etcdserverpb.DeleteRangeRequest{Key: deleteKey, PrevKv: true}
	deleteResp, err := runner.call(ctx, "C09", "etcdserverpb.KV/DeleteRange", deleteReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.DeleteRange(requestCtx, deleteReq)
	})
	if err != nil {
		return err
	}
	deleted, ok := deleteResp.(*etcdserverpb.DeleteRangeResponse)
	if !ok || deleted.Deleted != 1 || len(deleted.PrevKvs) != 1 || string(deleted.PrevKvs[0].Value) != "before-delete" {
		return errors.New("native DeleteRange did not return the previous key-value")
	}

	leaseGrantReq := &etcdserverpb.LeaseGrantRequest{TTL: 30}
	leaseGrantResp, err := runner.call(ctx, "C15", "etcdserverpb.Lease/LeaseGrant", leaseGrantReq, func(requestCtx context.Context) (protoMessage, error) {
		return lease.LeaseGrant(requestCtx, leaseGrantReq)
	})
	if err != nil {
		return err
	}
	grant, ok := leaseGrantResp.(*etcdserverpb.LeaseGrantResponse)
	if !ok || grant.ID == 0 || grant.TTL != leaseGrantReq.TTL {
		return errors.New("native LeaseGrant returned an invalid ID or TTL")
	}
	leasedKey := []byte("m0/lease/value")
	leasedPut := &etcdserverpb.PutRequest{Key: leasedKey, Value: []byte("leased"), Lease: grant.ID}
	if _, err := runner.call(ctx, "C16", "etcdserverpb.KV/Put", leasedPut, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Put(requestCtx, leasedPut)
	}); err != nil {
		return err
	}
	if err := runner.keepAlive(ctx, lease, grant.ID); err != nil {
		return err
	}
	revokeReq := &etcdserverpb.LeaseRevokeRequest{ID: grant.ID}
	if _, err := runner.call(ctx, "C18", "etcdserverpb.Lease/LeaseRevoke", revokeReq, func(requestCtx context.Context) (protoMessage, error) {
		return lease.LeaseRevoke(requestCtx, revokeReq)
	}); err != nil {
		return err
	}
	leasedRange := &etcdserverpb.RangeRequest{Key: leasedKey}
	leasedResp, err := runner.call(ctx, "C18", "etcdserverpb.KV/Range", leasedRange, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, leasedRange)
	})
	if err != nil {
		return err
	}
	if response, ok := leasedResp.(*etcdserverpb.RangeResponse); !ok || len(response.Kvs) != 0 {
		return errors.New("native LeaseRevoke did not remove the attached key")
	}

	watchKey := []byte("m0/watch/r-plus-one")
	watchBaseReq := &etcdserverpb.RangeRequest{Key: watchKey}
	watchBaseResp, err := runner.call(ctx, "C19", "etcdserverpb.KV/Range", watchBaseReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, watchBaseReq)
	})
	if err != nil {
		return err
	}
	watchBase, ok := watchBaseResp.(*etcdserverpb.RangeResponse)
	if !ok || watchBase.Header == nil {
		return errors.New("native watch baseline has no revision header")
	}
	watchCtx, cancelWatch := context.WithTimeout(ctx, requestTimeout)
	watchStream, err := watch.Watch(watchCtx)
	if err != nil {
		cancelWatch()
		return fmt.Errorf("open native Watch stream: %w", err)
	}
	watchReq := &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
		Key: watchKey, StartRevision: watchBase.Header.Revision + 1,
	}}}
	watchCreateStarted := time.Now()
	if err := watchStream.Send(watchReq); err != nil {
		cancelWatch()
		return fmt.Errorf("send native Watch create: %w", err)
	}
	created, err := watchStream.Recv()
	if err != nil {
		cancelWatch()
		return fmt.Errorf("receive native Watch create response: %w", err)
	}
	if !created.Created || created.Canceled || created.WatchId < 0 || created.Header == nil {
		cancelWatch()
		return errors.New("native Watch did not acknowledge creation with a watch ID and header")
	}
	if err := runner.record("C19", "etcdserverpb.Watch/Watch", watchReq, created, nil, nil, time.Since(watchCreateStarted)); err != nil {
		cancelWatch()
		return err
	}
	watchPut := &etcdserverpb.PutRequest{Key: watchKey, Value: []byte("after-watch-revision")}
	if _, err := runner.call(ctx, "C01", "etcdserverpb.KV/Put", watchPut, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Put(requestCtx, watchPut)
	}); err != nil {
		cancelWatch()
		return err
	}
	var eventResponse *etcdserverpb.WatchResponse
	for eventResponse == nil {
		receiveStarted := time.Now()
		response, recvErr := watchStream.Recv()
		if recvErr != nil {
			cancelWatch()
			return fmt.Errorf("receive native Watch event: %w", recvErr)
		}
		if err := runner.record("C19", "etcdserverpb.Watch/Watch", watchReq, response, nil, nil, time.Since(receiveStarted)); err != nil {
			cancelWatch()
			return err
		}
		if len(response.Events) > 0 {
			eventResponse = response
		}
	}
	if len(eventResponse.Events) != 1 || eventResponse.Events[0].Type != mvccpb.PUT ||
		string(eventResponse.Events[0].Kv.Key) != string(watchKey) ||
		string(eventResponse.Events[0].Kv.Value) != "after-watch-revision" ||
		eventResponse.Events[0].Kv.ModRevision != watchBase.Header.Revision+1 ||
		eventResponse.Header == nil || eventResponse.Header.Revision != eventResponse.Events[0].Kv.ModRevision {
		cancelWatch()
		return errors.New("native Watch did not return the expected R+1 Put event and revision header")
	}
	_ = watchStream.CloseSend()
	cancelWatch()
	terminalStarted := time.Now()
	_, terminalErr := watchStream.Recv()
	terminal := &trace.StreamTerminal{Kind: "context-canceled", GRPCCode: codes.Canceled.String()}
	if terminalErr == nil || (status.Code(terminalErr) != codes.Canceled && !errors.Is(terminalErr, context.Canceled)) {
		return fmt.Errorf("native Watch did not terminate after cancellation: %s", status.Code(terminalErr))
	}
	if err := runner.record("C19", "etcdserverpb.Watch/Watch", watchReq, nil, terminalErr, terminal, time.Since(terminalStarted)); err != nil {
		return err
	}

	latestReq := &etcdserverpb.RangeRequest{Key: binaryKey}
	latestResult, err := runner.call(ctx, "C06", "etcdserverpb.KV/Range", latestReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, latestReq)
	})
	if err != nil {
		return err
	}
	latest, ok := latestResult.(*etcdserverpb.RangeResponse)
	if !ok || latest.Header == nil {
		return errors.New("native pre-compaction Range has no revision header")
	}
	compactReq := &etcdserverpb.CompactionRequest{Revision: latest.Header.Revision}
	if _, err := runner.call(ctx, "C14", "etcdserverpb.KV/Compact", compactReq, func(requestCtx context.Context) (protoMessage, error) {
		return etcdserverpb.NewKVClient(runner.client.ActiveConnection()).Compact(requestCtx, compactReq)
	}); err != nil {
		return err
	}
	oldRevisionReq := &etcdserverpb.RangeRequest{Key: binaryKey, Revision: baseline.Header.Revision + 1}
	oldRevisionResp, oldRevisionErr := runner.callWithError(ctx, "C06", "etcdserverpb.KV/Range", oldRevisionReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, oldRevisionReq)
	})
	classified := trace.ClassifyError(oldRevisionErr)
	if oldRevisionResp != nil || classified == nil || classified.GRPCCode != codes.OutOfRange.String() {
		code := "none"
		if classified != nil {
			code = classified.GRPCCode
		}
		return fmt.Errorf("native compacted historical Range returned grpc_code=%s, expected OUT_OF_RANGE", code)
	}
	return nil
}

func (runner *runner) keepAlive(ctx context.Context, lease etcdserverpb.LeaseClient, id int64) error {
	streamCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	stream, err := lease.LeaseKeepAlive(streamCtx)
	if err != nil {
		return fmt.Errorf("open native LeaseKeepAlive stream: %w", err)
	}
	request := &etcdserverpb.LeaseKeepAliveRequest{ID: id}
	keepAliveStarted := time.Now()
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("send native LeaseKeepAlive request: %w", err)
	}
	response, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("receive native LeaseKeepAlive response: %w", err)
	}
	if response.ID != id || response.TTL <= 0 {
		return errors.New("native LeaseKeepAlive returned an invalid ID or TTL")
	}
	if err := runner.record("C17", "etcdserverpb.Lease/LeaseKeepAlive", request, response, nil, nil, time.Since(keepAliveStarted)); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("close native LeaseKeepAlive request stream: %w", err)
	}
	terminalStarted := time.Now()
	terminalErr := error(nil)
	_, terminalErr = stream.Recv()
	if !errors.Is(terminalErr, io.EOF) {
		return fmt.Errorf("native LeaseKeepAlive stream did not end cleanly: %s", status.Code(terminalErr))
	}
	terminal := &trace.StreamTerminal{Kind: "eof"}
	return runner.record("C17", "etcdserverpb.Lease/LeaseKeepAlive", request, nil, nil, terminal, time.Since(terminalStarted))
}

func (runner *runner) call(ctx context.Context, caseID, method string, request protoMessage, operation func(context.Context) (protoMessage, error)) (protoMessage, error) {
	response, err := runner.callWithError(ctx, caseID, method, request, operation)
	if err != nil {
		return nil, fmt.Errorf("native %s failed with grpc_code=%s", method, status.Code(err))
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
	responseSnapshot := []byte("null")
	if !isNilMessage(response) {
		responseSnapshot, err = trace.SnapshotProto(response)
		if err != nil {
			return fmt.Errorf("snapshot %s response: %w", method, err)
		}
	}
	runner.ordinals[caseID]++
	record := trace.Record{
		Schema: trace.SchemaVersion, CaseID: caseID, Ordinal: runner.ordinals[caseID], Method: method,
		Request: requestSnapshot, Response: responseSnapshot, Error: trace.ClassifyError(callErr),
		StreamTerminal: terminal, EvidenceScope: "native-etcd", ElapsedNS: strconv.FormatInt(elapsed.Nanoseconds(), 10),
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
