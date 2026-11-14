package scenario

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const m4BindingLeaseTTL = int64(60)

// RunM4NativeLeaseBindings runs the pinned J41 contract against a fresh embedded etcd.
func RunM4NativeLeaseBindings(ctx context.Context, dataDir, prefix string, output io.Writer) error {
	if err := ValidateM4Lease("native-etcd", prefix); err != nil {
		return err
	}
	if ctx == nil || output == nil || dataDir == "" {
		return errors.New("native M4 binding run requires context, data directory, and trace output")
	}
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		return fmt.Errorf("create isolated native data directory: %w", err)
	}
	server, err := startEmbeddedWithElectionTimeout(ctx, dataDir, m4NativeElectionTimeoutMs)
	if err != nil {
		return err
	}
	defer server.close()
	return RunM4LeaseBindings(ctx, server.client, "native-etcd", prefix, output)
}

// RunM4LeaseBindings checks atomic registration, binding changes, and key-index projections.
func RunM4LeaseBindings(ctx context.Context, client *clientv3.Client, evidenceScope, prefix string,
	output io.Writer) error {
	if err := ValidateM4Lease(evidenceScope, prefix); err != nil {
		return err
	}
	if ctx == nil || client == nil || output == nil {
		return errors.New("M4 lease binding scenario requires context, client, and trace output")
	}
	runner := &runner{client: client, writer: output, ordinals: make(map[string]uint64),
		evidenceScope: evidenceScope}
	lease := etcdserverpb.NewLeaseClient(client.ActiveConnection())
	kv := etcdserverpb.NewKVClient(client.ActiveConnection())
	key := func(name string) []byte { return []byte(prefix + name) }

	baselineRequest := &etcdserverpb.LeaseLeasesRequest{}
	baselineResult, err := runner.call(ctx, "binding-baseline", "etcdserverpb.Lease/LeaseLeases",
		baselineRequest, func(requestContext context.Context) (protoMessage, error) {
			return lease.LeaseLeases(requestContext, baselineRequest)
		})
	if err != nil {
		return err
	}
	baseline, ok := baselineResult.(*etcdserverpb.LeaseLeasesResponse)
	if !ok || baseline.Header == nil || baseline.Header.Revision < 1 || len(baseline.Leases) != 0 {
		return errors.New("M4 binding scenario requires a fresh empty table group")
	}

	leaseAID := leaseIDForPrefix(prefix, "binding-a")
	leaseBID := leaseIDForPrefix(prefix, "binding-b")
	if leaseAID == leaseBID {
		return errors.New("deterministic test Lease IDs collided")
	}
	if err := grantBindingLease(ctx, runner, lease, "binding-grant-a", leaseAID); err != nil {
		return err
	}
	if err := grantBindingLease(ctx, runner, lease, "binding-grant-b", leaseBID); err != nil {
		return err
	}

	roleA, roleB, roleCoordinator, version :=
		key("role/member"), key("role/query"), key("role/coordinator"), key("version")
	registerThen := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{Key: roleA, Target: etcdserverpb.Compare_VERSION,
			Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Version{Version: 0}}},
		Success: []*etcdserverpb.RequestOp{
			bindingPutOp(roleA, []byte("member-v1"), leaseAID),
			bindingPutOp(roleB, []byte("query-v1"), leaseAID),
			bindingPutOp(roleCoordinator, []byte("coordinator-v1"), leaseBID),
			bindingPutOp(version, []byte("v1"), 0),
		},
		Failure: []*etcdserverpb.RequestOp{bindingPutOp(key("unexpected-then"), []byte("bad"), leaseBID)},
	}
	registered, err := runBindingTxn(ctx, runner, kv, "binding-register-then", registerThen)
	if err != nil {
		return err
	}
	if !registered.Succeeded || len(registered.Responses) != 4 {
		return errors.New("multi-role Then registration did not commit all role and version keys")
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-a-initial", leaseAID,
		[][]byte{roleA, roleB}); err != nil {
		return err
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-b-initial", leaseBID,
		[][]byte{roleCoordinator}); err != nil {
		return err
	}

	ignored := &etcdserverpb.PutRequest{Key: roleA, IgnoreValue: true, IgnoreLease: true, PrevKv: true}
	ignoredResult, err := runner.call(ctx, "binding-ignore-flags", "etcdserverpb.KV/Put", ignored,
		func(requestContext context.Context) (protoMessage, error) { return kv.Put(requestContext, ignored) })
	if err != nil {
		return err
	}
	ignoredResponse, ok := ignoredResult.(*etcdserverpb.PutResponse)
	if !ok || ignoredResponse.PrevKv == nil || ignoredResponse.PrevKv.Lease != leaseAID ||
		string(ignoredResponse.PrevKv.Value) != "member-v1" {
		return errors.New("ignore flags did not preserve the prior value and Lease association")
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-a-preserved", leaseAID,
		[][]byte{roleA, roleB}); err != nil {
		return err
	}

	rebind := &etcdserverpb.PutRequest{Key: roleA, Value: []byte("member-v2"), Lease: leaseBID}
	if _, err := runner.call(ctx, "binding-rebind-a-to-b", "etcdserverpb.KV/Put", rebind,
		func(requestContext context.Context) (protoMessage, error) { return kv.Put(requestContext, rebind) }); err != nil {
		return err
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-a-after-rebind", leaseAID,
		[][]byte{roleB}); err != nil {
		return err
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-b-after-rebind", leaseBID,
		[][]byte{roleA, roleCoordinator}); err != nil {
		return err
	}

	roleElse := key("role/fallback")
	registerElse := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{Key: roleB, Target: etcdserverpb.Compare_LEASE,
			Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Lease{Lease: leaseBID}}},
		Success: []*etcdserverpb.RequestOp{bindingPutOp(key("unexpected-else"), []byte("bad"), leaseAID)},
		Failure: []*etcdserverpb.RequestOp{bindingPutOp(roleElse, []byte("fallback-v1"), leaseBID)},
	}
	registeredElse, err := runBindingTxn(ctx, runner, kv, "binding-register-else", registerElse)
	if err != nil {
		return err
	}
	if registeredElse.Succeeded || len(registeredElse.Responses) != 1 {
		return errors.New("LEASE Compare did not select the expected Else registration branch")
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-b-after-else", leaseBID,
		[][]byte{roleElse, roleA, roleCoordinator}); err != nil {
		return err
	}

	nestedMember, nestedQuery, nestedVersion :=
		key("role/nested/member"), key("role/nested/query"), key("version/nested")
	nested := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{Key: roleA, Target: etcdserverpb.Compare_LEASE,
			Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Lease{Lease: leaseAID}}},
		Success: []*etcdserverpb.RequestOp{bindingPutOp(key("role/unexpected-nested"), []byte("bad"), leaseAID)},
		Failure: []*etcdserverpb.RequestOp{
			bindingPutOp(nestedMember, []byte("nested-member"), leaseAID),
			bindingPutOp(nestedQuery, []byte("nested-query"), leaseBID),
			bindingPutOp(nestedVersion, []byte("nested-v1"), 0),
		},
	}
	nestedResponse, err := runBindingTxn(ctx, runner, kv, "binding-register-nested",
		&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{bindingTxnOp(nested)}})
	if err != nil {
		return err
	}
	if !nestedResponse.Succeeded || len(nestedResponse.Responses) != 1 {
		return errors.New("outer transaction did not execute its nested registration")
	}
	nestedResult := nestedResponse.Responses[0].GetResponseTxn()
	if nestedResult == nil || nestedResult.Succeeded || len(nestedResult.Responses) != 3 {
		return errors.New("nested Lease Compare did not select the expected mixed-Lease Else branch")
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-a-after-nested", leaseAID,
		[][]byte{roleB, nestedMember}); err != nil {
		return err
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-b-after-nested", leaseBID,
		[][]byte{roleElse, roleA, roleCoordinator, nestedQuery}); err != nil {
		return err
	}

	unbound := &etcdserverpb.PutRequest{Key: roleB, Value: []byte("query-v2")}
	if _, err := runner.call(ctx, "binding-unbind", "etcdserverpb.KV/Put", unbound,
		func(requestContext context.Context) (protoMessage, error) { return kv.Put(requestContext, unbound) }); err != nil {
		return err
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-a-after-unbind", leaseAID,
		[][]byte{nestedMember}); err != nil {
		return err
	}

	delete := &etcdserverpb.DeleteRangeRequest{Key: roleA, PrevKv: true}
	deletedResult, err := runner.call(ctx, "binding-delete", "etcdserverpb.KV/DeleteRange", delete,
		func(requestContext context.Context) (protoMessage, error) {
			return kv.DeleteRange(requestContext, delete)
		})
	if err != nil {
		return err
	}
	deleted, ok := deletedResult.(*etcdserverpb.DeleteRangeResponse)
	if !ok || deleted.Deleted != 1 || len(deleted.PrevKvs) != 1 || deleted.PrevKvs[0].Lease != leaseBID {
		return errors.New("DeleteRange did not return the actual previous role key")
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-b-after-delete", leaseBID,
		[][]byte{roleElse, roleCoordinator, nestedQuery}); err != nil {
		return err
	}

	recreated := &etcdserverpb.PutRequest{Key: roleA, Value: []byte("unleased-recreated")}
	if _, err := runner.call(ctx, "binding-recreate-unleased", "etcdserverpb.KV/Put", recreated,
		func(requestContext context.Context) (protoMessage, error) { return kv.Put(requestContext, recreated) }); err != nil {
		return err
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-b-after-recreate", leaseBID,
		[][]byte{roleElse, roleCoordinator, nestedQuery}); err != nil {
		return err
	}

	deleteRange := &etcdserverpb.DeleteRangeRequest{
		Key: key("role/"), RangeEnd: key("role0"), PrevKv: true,
	}
	deleteRangeResult, err := runner.call(ctx, "binding-delete-range", "etcdserverpb.KV/DeleteRange",
		deleteRange, func(requestContext context.Context) (protoMessage, error) {
			return kv.DeleteRange(requestContext, deleteRange)
		})
	if err != nil {
		return err
	}
	deletedRoles, ok := deleteRangeResult.(*etcdserverpb.DeleteRangeResponse)
	if !ok || deletedRoles.Deleted != 6 || len(deletedRoles.PrevKvs) != 6 {
		return errors.New("multi-key DeleteRange did not return every role key and PrevKV")
	}
	deletedRoleKeys := make(map[string]int64, len(deletedRoles.PrevKvs))
	for _, previous := range deletedRoles.PrevKvs {
		deletedRoleKeys[string(previous.Key)] = previous.Lease
	}
	expectedRoleLeases := map[string]int64{
		string(roleA): 0, string(roleB): 0, string(roleCoordinator): leaseBID,
		string(roleElse): leaseBID, string(nestedMember): leaseAID, string(nestedQuery): leaseBID,
	}
	if len(deletedRoleKeys) != len(expectedRoleLeases) {
		return errors.New("multi-key DeleteRange returned an incorrect number of role keys")
	}
	for roleKey, leaseID := range expectedRoleLeases {
		if actualLease, exists := deletedRoleKeys[roleKey]; !exists || actualLease != leaseID {
			return errors.New("multi-key DeleteRange returned an incorrect role binding set")
		}
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-a-after-delete-range", leaseAID, nil); err != nil {
		return err
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-b-after-delete-range", leaseBID, nil); err != nil {
		return err
	}

	rolesAfterDelete := &etcdserverpb.RangeRequest{Key: key("role/"), RangeEnd: key("role0")}
	rolesAfterDeleteResult, err := runner.call(ctx, "binding-role-prefix-after-delete-range", "etcdserverpb.KV/Range",
		rolesAfterDelete, func(requestContext context.Context) (protoMessage, error) {
			return kv.Range(requestContext, rolesAfterDelete)
		})
	if err != nil {
		return err
	}
	remainingRoles, ok := rolesAfterDeleteResult.(*etcdserverpb.RangeResponse)
	if !ok || len(remainingRoles.Kvs) != 0 {
		return errors.New("role prefix retained keys after its DeleteRange")
	}

	versionRange := &etcdserverpb.RangeRequest{Key: version}
	versionResult, err := runner.call(ctx, "binding-version-after-delete-range", "etcdserverpb.KV/Range",
		versionRange, func(requestContext context.Context) (protoMessage, error) {
			return kv.Range(requestContext, versionRange)
		})
	if err != nil {
		return err
	}
	retainedVersion, ok := versionResult.(*etcdserverpb.RangeResponse)
	if !ok || len(retainedVersion.Kvs) != 1 || string(retainedVersion.Kvs[0].Value) != "v1" ||
		retainedVersion.Kvs[0].Lease != 0 {
		return errors.New("role DeleteRange removed or changed the unleased version key")
	}

	survivor := &etcdserverpb.PutRequest{
		Key: key("survivor/registered"), Value: []byte("still-live"), Lease: leaseBID,
	}
	if _, err := runner.call(ctx, "binding-survivor-put", "etcdserverpb.KV/Put", survivor,
		func(requestContext context.Context) (protoMessage, error) {
			return kv.Put(requestContext, survivor)
		}); err != nil {
		return err
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-b-after-survivor-put", leaseBID,
		[][]byte{survivor.Key}); err != nil {
		return err
	}

	firstAtomic, secondAtomic := key("atomic/first"), key("atomic/second")
	invalidLeaseID := leaseIDForPrefix(prefix, "unknown")
	invalidTxn := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{Key: firstAtomic, Target: etcdserverpb.Compare_VERSION,
			Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Version{Version: 0}}},
		Success: []*etcdserverpb.RequestOp{
			bindingPutOp(firstAtomic, []byte("must-not-appear"), leaseAID),
			bindingPutOp(secondAtomic, []byte("invalid-lease"), invalidLeaseID),
		},
	}
	_, err = runner.callWithError(ctx, "binding-invalid-lease-atomicity", "etcdserverpb.KV/Txn", invalidTxn,
		func(requestContext context.Context) (protoMessage, error) { return kv.Txn(requestContext, invalidTxn) })
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("transaction with an unknown Lease returned grpc_code=%s", status.Code(err))
	}
	if err := requireAbsentBindingKey(ctx, runner, kv, "binding-atomicity-range-first", firstAtomic); err != nil {
		return err
	}
	if err := requireAbsentBindingKey(ctx, runner, kv, "binding-atomicity-range-second", secondAtomic); err != nil {
		return err
	}
	if err := requireBindingKeys(ctx, runner, lease, "binding-ttl-a-after-failed-txn", leaseAID, nil); err != nil {
		return err
	}

	invalidPutKey := key("atomic/invalid-put")
	invalidPut := &etcdserverpb.PutRequest{
		Key: invalidPutKey, Value: []byte("must-not-appear"), Lease: invalidLeaseID,
	}
	_, err = runner.callWithError(ctx, "binding-invalid-lease-put", "etcdserverpb.KV/Put", invalidPut,
		func(requestContext context.Context) (protoMessage, error) { return kv.Put(requestContext, invalidPut) })
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("Put with an unknown Lease returned grpc_code=%s", status.Code(err))
	}
	if err := requireAbsentBindingKey(ctx, runner, kv, "binding-atomicity-range-invalid-put", invalidPutKey); err != nil {
		return err
	}

	unselectedThen, unselectedElse := key("atomic/unselected-then"), key("atomic/unselected-else")
	unselectedCompare := key("atomic/unselected-compare")
	unselectedTxn := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{Key: unselectedCompare,
			Target: etcdserverpb.Compare_VERSION, Result: etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 1}}},
		Success: []*etcdserverpb.RequestOp{
			bindingPutOp(unselectedThen, []byte("must-not-validate"), invalidLeaseID),
		},
		Failure: []*etcdserverpb.RequestOp{
			bindingPutOp(unselectedElse, []byte("selected-else"), leaseBID),
		},
	}
	unselectedResponse, err := runBindingTxn(ctx, runner,
		kv, "binding-invalid-lease-unselected-branch", unselectedTxn)
	if err != nil {
		return err
	}
	if unselectedResponse.Succeeded || len(unselectedResponse.Responses) != 1 {
		return errors.New("unselected invalid-Lease Then branch prevented the valid Else branch")
	}
	if err := requireAbsentBindingKey(ctx, runner, kv,
		"binding-unselected-then-absent", unselectedThen); err != nil {
		return err
	}
	visibleRequest := &etcdserverpb.RangeRequest{Key: unselectedElse}
	visibleResult, err := runner.call(ctx, "binding-unselected-else-visible",
		"etcdserverpb.KV/Range", visibleRequest,
		func(requestContext context.Context) (protoMessage, error) {
			return kv.Range(requestContext, visibleRequest)
		})
	if err != nil {
		return err
	}
	visible, ok := visibleResult.(*etcdserverpb.RangeResponse)
	if !ok || len(visible.Kvs) != 1 || string(visible.Kvs[0].Value) != "selected-else" ||
		visible.Kvs[0].Lease != leaseBID {
		return errors.New("valid Else branch was not committed after an invalid unselected Then")
	}
	return requireBindingKeys(ctx, runner, lease,
		"binding-ttl-b-after-unselected-branch", leaseBID,
		[][]byte{key("survivor/registered"), unselectedElse})
}

func grantBindingLease(ctx context.Context, runner *runner, client etcdserverpb.LeaseClient,
	caseID string, id int64) error {
	request := &etcdserverpb.LeaseGrantRequest{ID: id, TTL: m4BindingLeaseTTL}
	result, err := runner.call(ctx, caseID, "etcdserverpb.Lease/LeaseGrant", request,
		func(requestContext context.Context) (protoMessage, error) {
			return client.LeaseGrant(requestContext, request)
		})
	if err != nil {
		return err
	}
	response, ok := result.(*etcdserverpb.LeaseGrantResponse)
	if !ok || response.Header == nil || response.ID != id || response.TTL != m4BindingLeaseTTL {
		return errors.New("LeaseGrant did not preserve its requested binding fixture")
	}
	return nil
}

func runBindingTxn(ctx context.Context, runner *runner, client etcdserverpb.KVClient,
	caseID string, request *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	result, err := runner.call(ctx, caseID, "etcdserverpb.KV/Txn", request,
		func(requestContext context.Context) (protoMessage, error) { return client.Txn(requestContext, request) })
	if err != nil {
		return nil, err
	}
	response, ok := result.(*etcdserverpb.TxnResponse)
	if !ok || response.Header == nil {
		return nil, errors.New("transaction response is missing its header")
	}
	return response, nil
}

func requireBindingKeys(ctx context.Context, runner *runner, client etcdserverpb.LeaseClient,
	caseID string, id int64, expected [][]byte) error {
	request := &etcdserverpb.LeaseTimeToLiveRequest{ID: id, Keys: true}
	result, err := runner.call(ctx, caseID, "etcdserverpb.Lease/LeaseTimeToLive", request,
		func(requestContext context.Context) (protoMessage, error) {
			return client.LeaseTimeToLive(requestContext, request)
		})
	if err != nil {
		return err
	}
	response, ok := result.(*etcdserverpb.LeaseTimeToLiveResponse)
	if !ok || response.Header == nil || response.ID != id || response.TTL <= 0 ||
		response.GrantedTTL != m4BindingLeaseTTL || len(response.Keys) != len(expected) {
		return fmt.Errorf("%s LeaseTimeToLive returned %d keys for Lease %d, expected %d",
			caseID,
			len(response.GetKeys()), id, len(expected))
	}
	expectedKeys := make(map[string]struct{}, len(expected))
	for _, key := range expected {
		if _, duplicate := expectedKeys[string(key)]; duplicate {
			return fmt.Errorf("%s expected binding set contains a duplicate key", caseID)
		}
		expectedKeys[string(key)] = struct{}{}
	}
	for _, key := range response.Keys {
		if _, exists := expectedKeys[string(key)]; !exists {
			return fmt.Errorf("%s LeaseTimeToLive returned a key outside the committed binding set", caseID)
		}
		delete(expectedKeys, string(key))
	}
	if len(expectedKeys) != 0 {
		return fmt.Errorf("%s LeaseTimeToLive omitted a committed binding key", caseID)
	}
	return nil
}

func requireAbsentBindingKey(ctx context.Context, runner *runner, client etcdserverpb.KVClient,
	caseID string, key []byte) error {
	request := &etcdserverpb.RangeRequest{Key: key}
	result, err := runner.call(ctx, caseID, "etcdserverpb.KV/Range", request,
		func(requestContext context.Context) (protoMessage, error) {
			return client.Range(requestContext, request)
		})
	if err != nil {
		return err
	}
	response, ok := result.(*etcdserverpb.RangeResponse)
	if !ok || response.Header == nil || response.Count != 0 || len(response.Kvs) != 0 {
		return errors.New("failed transaction exposed a partially registered key")
	}
	return nil
}

func bindingPutOp(key, value []byte, lease int64) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{Key: key, Value: value, Lease: lease},
	}}
}

func bindingTxnOp(request *etcdserverpb.TxnRequest) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: request}}
}
