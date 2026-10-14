package client

import (
	"context"
	"testing"
	"time"
)

func TestOptionsValidation(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		wantErr bool
	}{
		{name: "valid", options: Options{Endpoint: "127.0.0.1:2379", Timeout: time.Second}},
		{name: "missing endpoint", options: Options{Timeout: time.Second}, wantErr: true},
		{name: "zero timeout", options: Options{Endpoint: "127.0.0.1:2379"}, wantErr: true},
		{name: "negative timeout", options: Options{Endpoint: "127.0.0.1:2379", Timeout: -time.Second}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.options.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestWithDeadlineCancelsAtDeadline(t *testing.T) {
	ctx, cancel, err := WithDeadline(context.Background(), 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	select {
	case <-ctx.Done():
		if ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("context error = %v, want deadline exceeded", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("deadline did not cancel context")
	}
}

func TestWithDeadlineRejectsInvalidInputs(t *testing.T) {
	if _, _, err := WithDeadline(nil, time.Second); err == nil {
		t.Fatal("nil parent context should fail")
	}
	if _, _, err := WithDeadline(context.Background(), 0); err == nil {
		t.Fatal("zero timeout should fail")
	}
}
