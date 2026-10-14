package rpctypes_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStatusConversionUsesClientSentinels(t *testing.T) {
	tests := []struct {
		name           string
		message        string
		code           codes.Code
		clientSentinel error
		serverStatus   error
	}{
		{
			name:           "key not found",
			message:        "etcdserver: key not found",
			code:           codes.InvalidArgument,
			clientSentinel: rpctypes.ErrKeyNotFound,
			serverStatus:   rpctypes.ErrGRPCKeyNotFound,
		},
		{
			name:           "authentication failed",
			message:        "etcdserver: authentication failed, invalid user ID or password",
			code:           codes.InvalidArgument,
			clientSentinel: rpctypes.ErrAuthFailed,
			serverStatus:   rpctypes.ErrGRPCAuthFailed,
		},
		{
			name:           "permission denied",
			message:        "etcdserver: permission denied",
			code:           codes.PermissionDenied,
			clientSentinel: rpctypes.ErrPermissionDenied,
			serverStatus:   rpctypes.ErrGRPCPermissionDenied,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			serverError := status.Error(test.code, test.message)
			converted := rpctypes.Error(serverError)
			etcdError, ok := converted.(rpctypes.EtcdError)
			if !ok {
				t.Fatalf("expected rpctypes.EtcdError, got %T", converted)
			}
			if etcdError.Code() != test.code {
				t.Fatalf("expected code %s, got %s", test.code, etcdError.Code())
			}
			if converted.Error() != test.message {
				t.Fatalf("expected message %q, got %q", test.message, converted.Error())
			}
			if !errors.Is(converted, test.clientSentinel) {
				t.Fatalf("converted error does not match the client sentinel %v", test.clientSentinel)
			}
			if errors.Is(converted, test.serverStatus) {
				t.Fatal("converted client error unexpectedly matches the server status error")
			}
		})
	}
}

func TestUnknownStatusPassesThroughUnchanged(t *testing.T) {
	original := status.Error(codes.Internal, "unrecognized server message")

	if converted := rpctypes.Error(original); converted != original {
		t.Fatalf("unknown status changed during conversion: %v", converted)
	}
}

func TestContextErrorReturnsCanceledCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	serverError := status.Error(codes.Canceled, "etcdserver: request canceled")

	if converted := clientv3.ContextError(ctx, serverError); converted != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", converted)
	}
}

func TestContextErrorReturnsExpiredCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	serverError := status.Error(codes.DeadlineExceeded, "etcdserver: context deadline exceeded")

	if converted := clientv3.ContextError(ctx, serverError); converted != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", converted)
	}
}
