package api

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect"
)

// TestRPCCode_IsWhatTheClientReceives: a bare context error is recorded as
// the code Connect sends for it, not unknown.
func TestRPCCode_IsWhatTheClientReceives(t *testing.T) {
	for err, want := range map[error]string{
		nil: "ok",
		connect.NewError(connect.CodeNotFound, errors.New("x")): "not_found",
		context.Canceled: "canceled",
		fmt.Errorf("read: %w", context.DeadlineExceeded): "deadline_exceeded",
		errors.New("plain"): "unknown",
	} {
		if got := rpcCode(err); got != want {
			t.Errorf("rpcCode(%v) = %q; want %q", err, got, want)
		}
	}
}
