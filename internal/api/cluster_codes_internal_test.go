package api

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
)

// Whether a cancel the cluster did not answer was the caller giving up is
// read from the caller's own context, never from the error: a silent
// frontend ends the SDK's per-call deadline while the caller still waits, and
// that is unavailable, worth asking again.
func TestCancelErrorCode_TheCallersOwnContextDecidesWhetherItGaveUp(t *testing.T) {
	live := context.Background()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	deadline := fmt.Errorf("cancel run r: %w", serviceerror.NewDeadlineExceeded("timed out"))
	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want connect.Code
	}{
		{"cluster deadline, caller waiting", live, deadline, connect.CodeUnavailable},
		{"cluster cancellation, caller waiting", live, serviceerror.NewCanceled("x"), connect.CodeUnavailable},
		{"engine bound, caller waiting", live, context.DeadlineExceeded, connect.CodeUnavailable},
		{"rate-limited, caller waiting", live, serviceerror.NewResourceExhausted(0, "busy"), connect.CodeResourceExhausted},
		{"cluster deadline, caller's deadline passed", expired, deadline, connect.CodeDeadlineExceeded},
		{"cluster deadline, caller canceled", canceled, deadline, connect.CodeCanceled},
		{"unavailable, caller canceled", canceled, serviceerror.NewUnavailable("x"), connect.CodeCanceled},
		{"anything else, caller waiting", live, errors.New("boom"), connect.CodeInternal},
		{"anything else, caller canceled", canceled, errors.New("boom"), connect.CodeInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cancelErrorCode(tt.ctx, tt.err); got != tt.want {
				t.Fatalf("cancelErrorCode = %s, want %s", got, tt.want)
			}
		})
	}
}
