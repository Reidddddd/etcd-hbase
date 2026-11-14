package scenario

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const m4MaximumLeaseTTL = int64(9_000_000_000)

// etcd derives its minimum Lease TTL from 1.5 times the election timeout.
// Match the target fixture's five-second minimum instead of comparing two
// different TTL policies as if they were clock skew.
const m4NativeElectionTimeoutMs = uint(3_000)

// RunM4NativeLease runs the pinned J40 scenario against a fresh embedded etcd.
func RunM4NativeLease(ctx context.Context, dataDir, prefix string, output io.Writer) error {
	if err := ValidateM4Lease("native-etcd", prefix); err != nil {
		return err
	}
	if ctx == nil || output == nil || dataDir == "" {
		return errors.New("native M4 lease run requires context, data directory, and trace output")
	}
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		return fmt.Errorf("create isolated native data directory: %w", err)
	}
	server, err := startEmbeddedWithElectionTimeout(ctx, dataDir, m4NativeElectionTimeoutMs)
	if err != nil {
		return err
	}
	defer server.close()
	return RunM4Lease(ctx, server.client, "native-etcd", prefix, output)
}

// RunM4Lease exercises only the implemented Lease grant and management-query surface.
func RunM4Lease(ctx context.Context, client *clientv3.Client, evidenceScope, prefix string,
	output io.Writer) error {
	if err := ValidateM4Lease(evidenceScope, prefix); err != nil {
		return err
	}
	if ctx == nil || client == nil || output == nil {
		return errors.New("M4 lease scenario requires context, client, and trace output")
	}
	lease := etcdserverpb.NewLeaseClient(client.ActiveConnection())
	runner := &runner{client: client, writer: output, ordinals: make(map[string]uint64),
		evidenceScope: evidenceScope}
	baselineRequest := &etcdserverpb.LeaseLeasesRequest{}
	baselineResult, err := runner.call(ctx, "native-baseline", "etcdserverpb.Lease/LeaseLeases",
		baselineRequest, func(requestContext context.Context) (protoMessage, error) {
			return lease.LeaseLeases(requestContext, baselineRequest)
		})
	if err != nil {
		return err
	}
	baseline, ok := baselineResult.(*etcdserverpb.LeaseLeasesResponse)
	if !ok || baseline.Header == nil || baseline.Header.Revision < 1 || len(baseline.Leases) != 0 {
		return errors.New("M4 lease scenario requires a fresh empty table group")
	}

	autoGrant, err := grantLease(ctx, runner, lease, "auto-grant", 0, 0)
	if err != nil {
		return err
	}
	if autoGrant.ID <= 0 || autoGrant.TTL <= 0 {
		return errors.New("automatic LeaseGrant did not return a positive ID and minimum TTL")
	}
	autoTTL, err := readLeaseTTL(ctx, runner, lease, "auto-ttl", autoGrant.ID, true)
	if err != nil {
		return err
	}
	if autoTTL.ID != autoGrant.ID || autoTTL.TTL <= 0 || autoTTL.GrantedTTL != autoGrant.TTL ||
		len(autoTTL.Keys) != 0 {
		return errors.New("automatic LeaseTimeToLive did not preserve the active empty-lease state")
	}

	explicitID := leaseIDForPrefix(prefix, "explicit")
	explicitGrant, err := grantLease(ctx, runner, lease, "explicit-grant", explicitID, 30)
	if err != nil {
		return err
	}
	if explicitGrant.ID != explicitID || explicitGrant.TTL != 30 {
		return errors.New("explicit LeaseGrant changed its ID or TTL")
	}
	explicitTTL, err := readLeaseTTL(ctx, runner, lease, "explicit-ttl", explicitID, true)
	if err != nil {
		return err
	}
	if explicitTTL.ID != explicitID || explicitTTL.TTL <= 0 || explicitTTL.GrantedTTL != 30 ||
		len(explicitTTL.Keys) != 0 {
		return errors.New("explicit LeaseTimeToLive did not preserve the active empty-lease state")
	}

	duplicate := &etcdserverpb.LeaseGrantRequest{ID: explicitID, TTL: 30}
	_, err = runner.callWithError(ctx, "duplicate-id", "etcdserverpb.Lease/LeaseGrant", duplicate,
		func(requestContext context.Context) (protoMessage, error) {
			return lease.LeaseGrant(requestContext, duplicate)
		})
	if status.Code(err) != codes.FailedPrecondition {
		return fmt.Errorf("duplicate explicit LeaseGrant returned grpc_code=%s, expected FailedPrecondition",
			status.Code(err))
	}

	missingID := leaseIDForPrefix(prefix, "missing")
	missing, err := readLeaseTTL(ctx, runner, lease, "missing-ttl", missingID, true)
	if err != nil {
		return err
	}
	if missing.ID != missingID || missing.TTL != -1 || missing.GrantedTTL != 0 || len(missing.Keys) != 0 {
		return errors.New("missing LeaseTimeToLive did not return the native missing-lease projection")
	}

	negativeID := -leaseIDForPrefix(prefix, "negative")
	negativeGrant, err := grantLease(ctx, runner, lease, "negative-id", negativeID, 30)
	if err != nil {
		return err
	}
	if negativeGrant.ID != negativeID || negativeGrant.TTL != 30 {
		return errors.New("explicit negative Lease ID did not preserve its signed wire bits")
	}

	listRequest := &etcdserverpb.LeaseLeasesRequest{}
	listResult, err := runner.call(ctx, "leases-list", "etcdserverpb.Lease/LeaseLeases", listRequest,
		func(requestContext context.Context) (protoMessage, error) {
			return lease.LeaseLeases(requestContext, listRequest)
		})
	if err != nil {
		return err
	}
	list, ok := listResult.(*etcdserverpb.LeaseLeasesResponse)
	if !ok || list.Header == nil || len(list.Leases) != 3 ||
		!containsLease(list.Leases, autoGrant.ID) || !containsLease(list.Leases, explicitID) ||
		!containsLease(list.Leases, negativeID) {
		return errors.New("LeaseLeases did not return the complete active Lease set")
	}

	oversized := &etcdserverpb.LeaseGrantRequest{TTL: m4MaximumLeaseTTL + 1}
	_, err = runner.callWithError(ctx, "oversized-ttl", "etcdserverpb.Lease/LeaseGrant", oversized,
		func(requestContext context.Context) (protoMessage, error) {
			return lease.LeaseGrant(requestContext, oversized)
		})
	if status.Code(err) != codes.OutOfRange {
		return fmt.Errorf("oversized LeaseGrant returned grpc_code=%s, expected OutOfRange", status.Code(err))
	}

	zeroID, err := readLeaseTTL(ctx, runner, lease, "zero-id-ttl", 0, true)
	if err != nil || zeroID.ID != 0 || zeroID.TTL != -1 || zeroID.GrantedTTL != 0 || len(zeroID.Keys) != 0 {
		return errors.New("zero Lease ID did not return the native missing-lease response")
	}
	return nil
}

// ValidateM4Lease prevents a test scenario from mutating an unscoped endpoint or keyspace.
func ValidateM4Lease(evidenceScope, prefix string) error {
	if evidenceScope != "native-etcd" && evidenceScope != "target-hbase" {
		return errors.New("M4 lease scope must be native-etcd or target-hbase")
	}
	if len(prefix) < len("etcd-hbase-m4-test/run/") || len(prefix) > 256 ||
		!strings.HasPrefix(prefix, "etcd-hbase-m4-test/") || !strings.HasSuffix(prefix, "/") ||
		strings.Contains(prefix, "..") || strings.Contains(prefix, "//") {
		return errors.New("M4 lease prefix must be an isolated test namespace")
	}
	for _, character := range prefix {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' ||
			character == '.' || character == '/' {
			continue
		}
		return errors.New("M4 lease prefix contains unsupported characters")
	}
	return nil
}

func grantLease(ctx context.Context, runner *runner, lease etcdserverpb.LeaseClient,
	caseID string, id, ttl int64) (*etcdserverpb.LeaseGrantResponse, error) {
	request := &etcdserverpb.LeaseGrantRequest{ID: id, TTL: ttl}
	result, err := runner.call(ctx, caseID, "etcdserverpb.Lease/LeaseGrant", request,
		func(requestContext context.Context) (protoMessage, error) {
			return lease.LeaseGrant(requestContext, request)
		})
	if err != nil {
		return nil, err
	}
	response, ok := result.(*etcdserverpb.LeaseGrantResponse)
	if !ok || response.Header == nil {
		return nil, errors.New("LeaseGrant response is missing its response header")
	}
	return response, nil
}

func readLeaseTTL(ctx context.Context, runner *runner, lease etcdserverpb.LeaseClient,
	caseID string, id int64, includeKeys bool) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	request := &etcdserverpb.LeaseTimeToLiveRequest{ID: id, Keys: includeKeys}
	result, err := runner.call(ctx, caseID, "etcdserverpb.Lease/LeaseTimeToLive", request,
		func(requestContext context.Context) (protoMessage, error) {
			return lease.LeaseTimeToLive(requestContext, request)
		})
	if err != nil {
		return nil, err
	}
	response, ok := result.(*etcdserverpb.LeaseTimeToLiveResponse)
	if !ok || response.Header == nil {
		return nil, errors.New("LeaseTimeToLive response is missing its response header")
	}
	return response, nil
}

func leaseIDForPrefix(prefix, purpose string) int64 {
	digest := sha256.Sum256([]byte(prefix + ":" + purpose))
	id := int64(binary.BigEndian.Uint64(digest[:8]) & (1<<63 - 1))
	if id == 0 {
		return 1
	}
	return id
}

func containsLease(leases []*etcdserverpb.LeaseStatus, expected int64) bool {
	for _, lease := range leases {
		if lease != nil && lease.ID == expected {
			return true
		}
	}
	return false
}
