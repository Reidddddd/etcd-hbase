# M0 gRPC error mapping

This baseline fixes the upstream behavior to etcd v3.5.23. It separates observed native results from source-derived entries so a status documented from `rpctypes` is not mistaken for a completed native integration case.

## Native etcd baseline

| etcd error | gRPC status | Canonical message | Go consumer behavior | Evidence |
| --- | --- | --- | --- | --- |
| `ErrGRPCKeyNotFound` | `INVALID_ARGUMENT` | `etcdserver: key not found` | `rpctypes.Error` converts the matching status description to client `ErrKeyNotFound` (`EtcdError`); the client error exposes the same code and message. | v3.5.23 source |
| `ErrGRPCAuthFailed` | `INVALID_ARGUMENT` | `etcdserver: authentication failed, invalid user ID or password` | `rpctypes.Error` converts the matching description to client `ErrAuthFailed`. | v3.5.23 source |
| `ErrGRPCPermissionDenied` | `PERMISSION_DENIED` | `etcdserver: permission denied` | `rpctypes.Error` converts the matching description to client `ErrPermissionDenied`. This remains distinct from authentication failure. | v3.5.23 source |
| `ErrGRPCInvalidAuthToken` | `UNAUTHENTICATED` | `etcdserver: invalid auth token` | Client conversion is description-based; token handling is not represented by the current `BackendFailure` categories and remains outside the M0 mapper. | v3.5.23 source |
| `ErrGRPCCompacted` | `OUT_OF_RANGE` | `etcdserver: mvcc: required revision has been compacted` | The native testkit records the returned gRPC status code. | Native C06 observed; message from source |
| `ErrGRPCFutureRev` | `OUT_OF_RANGE` | `etcdserver: mvcc: required revision is a future revision` | Client conversion maps a matching description to `ErrFutureRev`. | v3.5.23 source |
| `ErrGRPCNoSpace` | `RESOURCE_EXHAUSTED` | `etcdserver: mvcc: database space exceeded` | Client conversion maps a matching description to `ErrNoSpace`. | v3.5.23 source |
| `ErrGRPCRequestTooManyRequests` | `RESOURCE_EXHAUSTED` | `etcdserver: too many requests` | Client conversion maps a matching description to `ErrTooManyRequests`. | v3.5.23 source |
| `ErrGRPCRequestTooLarge` | `INVALID_ARGUMENT` | `etcdserver: request is too large` | Client conversion maps a matching description to `ErrRequestTooLarge`. | v3.5.23 source |
| `ErrGRPCNoLeader` | `UNAVAILABLE` | `etcdserver: no leader` | `UNAVAILABLE` is the transient status; the retry client still applies method-specific idempotency rules. | v3.5.23 source |
| `ErrGRPCTimeout` | `UNAVAILABLE` | `etcdserver: request timed out` | This server-side timeout is distinct from an expired caller deadline. | v3.5.23 source |
| `ErrGRPCCanceled` | `CANCELLED` | `etcdserver: request canceled` | If the caller context is canceled, `clientv3.ContextError` returns `context.Canceled`. | v3.5.23 source |
| `ErrGRPCDeadlineExceeded` | `DEADLINE_EXCEEDED` | `etcdserver: context deadline exceeded` | If the caller deadline has expired, `clientv3.ContextError` returns `context.DeadlineExceeded`. | v3.5.23 source |

The checked-in native scenario `C06` requests a compacted revision against etcd v3.5.23. Its trace records `grpc_code=OutOfRange` and `consumer_kind=grpc-status`; the trace schema does not capture the canonical message. Other rows above are fixed-source baselines and are not claims of live native authentication, quota, or timeout tests.

### Go sentinel and consumption rules

The pinned `rpctypes.Error` implementation extracts the gRPC status message and converts recognized descriptions to a client `EtcdError` with the canonical code and description. Unknown descriptions pass through unchanged. The resulting client error is the corresponding client-side sentinel such as `ErrKeyNotFound`; the server-side `ErrGRPC*` status value is not the returned client error. `clientv3.ContextError` performs this conversion first, then substitutes `context.Canceled` or `context.DeadlineExceeded` when the supplied context has ended. The retry client retries immutable RPCs for `UNAVAILABLE`; mutable RPCs follow separate write-at-most-once rules, and an ended caller context stops retry processing.

The Go sentinel conversion and context substitution are covered by `testkit/go/internal/rpctypes/error_test.go`.

Sources:

- [etcd v3.5.23 `rpctypes/error.go`](https://github.com/etcd-io/etcd/blob/v3.5.23/api/v3rpc/rpctypes/error.go)
- [etcd v3.5.23 `client/v3/client.go`](https://github.com/etcd-io/etcd/blob/v3.5.23/client/v3/client.go)
- [etcd v3.5.23 `client/v3/retry.go`](https://github.com/etcd-io/etcd/blob/v3.5.23/client/v3/retry.go)
- [etcd v3.5.23 `rpctypes/error_test.go`](https://github.com/etcd-io/etcd/blob/v3.5.23/api/v3rpc/rpctypes/error_test.go)

## M0 backend-failure projection

`ErrorProjection` accepts the transport-independent `BackendFailure` type. It unwraps `CompletionException` and `ExecutionException`, then maps the bounded category without inspecting or returning a backend exception message.

| Internal category | gRPC status | Description |
| --- | --- | --- |
| `AUTHENTICATION` | `INVALID_ARGUMENT` | `etcdserver: authentication failed, invalid user ID or password` |
| `PERMISSION` | `PERMISSION_DENIED` | `etcdserver: permission denied` |
| `UNAVAILABLE` | `UNAVAILABLE` | `backend is unavailable` |
| `TIMEOUT` | `DEADLINE_EXCEEDED` | `backend operation timed out` |
| `CANCELLED` | `CANCELLED` | `backend operation was cancelled` |
| `RESOURCE_LIMIT` | `RESOURCE_EXHAUSTED` | `backend resource limit was reached` |
| `UNKNOWN` | `INTERNAL` | `backend operation failed` |

The `TIMEOUT` mapping represents an HBase operation deadline. It does not copy etcd's consensus `ErrGRPCTimeout`, which is `UNAVAILABLE`. Standard Java timeout, cancellation, and rejected-execution exceptions follow the same distinct status classes. Unknown exception messages and causes are not attached to the returned gRPC status.

Methods without an M0 handler return `UNIMPLEMENTED` with `etcd-hbase M0: <method> is not implemented`; they do not construct success responses or headers.

## Response headers and authentication input

`ResponseHeaderFactory` requires explicit cluster ID, member ID, revision, and Raft term values from one caller-owned state snapshot. The three `uint64` protocol fields retain the raw Java `long` bits, including values whose high bit is set. The factory does not substitute an initial revision or Raft state.

`AuthenticateCredentialMapper` copies only the request name and password into the SPI credential object. Its `AuthenticationScope` argument comes from the bound listener and is not derived from the request. The mapper creates no token and does not include the password in its result's string representation; the returned credential owner must be closed after use.
