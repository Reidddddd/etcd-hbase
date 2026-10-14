package wire

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOptionsValidateRequiresBothEndpointsAndPositiveTimeout(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		wantErr bool
	}{
		{
			name: "valid",
			options: Options{
				SkeletonEndpoint: "127.0.0.1:12345",
				FixtureEndpoint:  "127.0.0.1:12346",
				Timeout:          time.Second,
			},
		},
		{name: "missing skeleton", options: Options{FixtureEndpoint: "127.0.0.1:12346", Timeout: time.Second}, wantErr: true},
		{name: "missing fixture", options: Options{SkeletonEndpoint: "127.0.0.1:12345", Timeout: time.Second}, wantErr: true},
		{name: "zero timeout", options: Options{SkeletonEndpoint: "127.0.0.1:12345", FixtureEndpoint: "127.0.0.1:12346"}, wantErr: true},
		{name: "negative timeout", options: Options{SkeletonEndpoint: "127.0.0.1:12345", FixtureEndpoint: "127.0.0.1:12346", Timeout: -time.Second}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.options.Validate(); (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestExpectErrorChecksStatusAndClientSentinel(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		code     codes.Code
		sentinel error
		wantErr  bool
	}{
		{
			name: "authentication sentinel",
			err:  rpctypes.Error(status.Error(codes.InvalidArgument, "etcdserver: authentication failed, invalid user ID or password")),
			code: codes.InvalidArgument, sentinel: rpctypes.ErrAuthFailed,
		},
		{
			name: "permission sentinel",
			err:  rpctypes.Error(status.Error(codes.PermissionDenied, "etcdserver: permission denied")),
			code: codes.PermissionDenied, sentinel: rpctypes.ErrPermissionDenied,
		},
		{name: "wrong status", err: status.Error(codes.Unavailable, "temporary"), code: codes.PermissionDenied, wantErr: true},
		{name: "missing error", err: nil, code: codes.Unimplemented, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := expectError(test.err, test.code, test.sentinel)
			if (err != nil) != test.wantErr {
				t.Fatalf("expectError() = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestExpectDeadlineRequiresExpiredCallerContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	<-ctx.Done()
	if err := expectDeadline(ctx.Err()); err != nil {
		t.Fatalf("expired deadline rejected: %v", err)
	}
	if err := expectDeadline(status.Error(codes.DeadlineExceeded, "remote deadline")); err == nil {
		t.Fatal("remote DEADLINE_EXCEEDED without caller deadline was accepted")
	}
}
