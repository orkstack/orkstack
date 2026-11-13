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
