package api

import (
	"errors"
	"net/http"
	"testing"

	"connectrpc.com/connect"
)

// A trigger invoke whose Idempotency-Key was used for another submission is
// a conflict: a retry of it can never succeed, so it must not read as a 500
// a client or gateway would retry.
func TestTriggerSubmitStatus_AKeyUsedElsewhereIsAConflict(t *testing.T) {
	status, code := triggerSubmitStatus(errKeyUsedElsewhere("deploy-1"))
	if status != http.StatusConflict || code != connect.CodeAlreadyExists.String() {
		t.Fatalf("triggerSubmitStatus = %d %q, want 409 already_exists", status, code)
	}
	if status, _ := triggerSubmitStatus(connect.NewError(connect.CodeInternal, errors.New("x"))); status != http.StatusInternalServerError {
		t.Fatalf("an internal error answered %d", status)
	}
}

// A submit the cluster refused for its rate limit is, on the trigger's plain
// HTTP route, the same 503 `unavailable` an unreachable cluster is: both are
// worth sending again (docs/ORKFLOW_TRIGGERS.md).
func TestTriggerSubmitStatus_ARateLimitedClusterIsUnavailable(t *testing.T) {
	for _, code := range []connect.Code{connect.CodeUnavailable, connect.CodeResourceExhausted} {
		status, name := triggerSubmitStatus(connect.NewError(code, errors.New("x")))
		if status != http.StatusServiceUnavailable || name != connect.CodeUnavailable.String() {
			t.Errorf("triggerSubmitStatus(%s) = %d %q, want 503 unavailable", code, status, name)
		}
	}
}

// A submit the caller gave up on is, on the trigger's plain HTTP route, the
// same 503 `unavailable`: the engine finished on its own clock, and sending
// again under the same key reads what it did (docs/ORKFLOW_TRIGGERS.md).
func TestTriggerSubmitStatus_ACallerThatGaveUpIsUnavailable(t *testing.T) {
	for _, code := range []connect.Code{connect.CodeCanceled, connect.CodeDeadlineExceeded} {
		status, name := triggerSubmitStatus(connect.NewError(code, errors.New("x")))
		if status != http.StatusServiceUnavailable || name != connect.CodeUnavailable.String() {
			t.Errorf("triggerSubmitStatus(%s) = %d %q, want 503 unavailable", code, status, name)
		}
	}
}
