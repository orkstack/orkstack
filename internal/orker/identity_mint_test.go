package orker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect"
)

// refusal is the error the engine's API returns for a mint it refused:
// the reason first in the message, as an Orker reads it.
func refusal(code connect.Code, reason, engineBuild string) error {
	err := connect.NewError(code, errors.New(reason+": the mint issued nothing"))
	if engineBuild != "" {
		err.Meta().Set("Ork-Engine-Build", engineBuild)
	}
	return err
}

// TestMapMintAnswer is the Orker's table of a mint's answers, one case a row
// (identity_mint.go): a
// ContractViolation is only a refusal that would be given again identically
// and concerns this run; a fault of the deployment is an InfrastructureError;
// anything unknown is IDENTITY_UNAVAILABLE.
func TestMapMintAnswer(t *testing.T) {
	const build = "b-orker"
	for _, tc := range []struct {
		name   string
		err    error
		reason string
		cat    string
		then   mintThen
		count  string // the ork.identity.engine_refused counting, "" for none
	}{
		{"a transport error", errors.New("dial tcp: connection refused"), "IDENTITY_UNAVAILABLE", CategoryInfrastructureError, mintAskAgain, ""},
		{"a followed result not recorded", refusal(connect.CodeFailedPrecondition, "REQUIRED_RESULT_NOT_RECORDED", ""), "REQUIRED_RESULT_NOT_RECORDED", CategoryInfrastructureError, mintEnd, ""},
		{"a timeout", context.DeadlineExceeded, "IDENTITY_UNAVAILABLE", CategoryInfrastructureError, mintAskAgain, ""},
		{"unavailable", refusal(connect.CodeUnavailable, "IDENTITY_UNAVAILABLE", ""), "IDENTITY_UNAVAILABLE", CategoryInfrastructureError, mintAskAgain, ""},
		{"resource_exhausted", refusal(connect.CodeResourceExhausted, "IDENTITY_UNAVAILABLE", ""), "IDENTITY_UNAVAILABLE", CategoryInfrastructureError, mintAskAgain, ""},
		{"aborted", connect.NewError(connect.CodeAborted, errors.New("x")), "IDENTITY_UNAVAILABLE", CategoryInfrastructureError, mintAskAgain, ""},
		{"internal", connect.NewError(connect.CodeInternal, errors.New("the mint failed")), "IDENTITY_UNAVAILABLE", CategoryInfrastructureError, mintAskAgain, ""},
		{"unknown", connect.NewError(connect.CodeUnknown, errors.New("?")), "IDENTITY_UNAVAILABLE", CategoryInfrastructureError, mintAskAgain, ""},
		{"an unmapped code", connect.NewError(connect.CodeNotFound, errors.New("?")), "IDENTITY_UNAVAILABLE", CategoryInfrastructureError, mintAskAgain, ""},
		{"an unknown reason", refusal(connect.CodeFailedPrecondition, "IDENTITY_SOMETHING_NEW", ""), "IDENTITY_UNAVAILABLE", CategoryInfrastructureError, mintAskAgain, ""},
		{"unauthenticated", connect.NewError(connect.CodeUnauthenticated, errors.New("x")), "IDENTITY_ENGINE_REFUSED", CategoryInfrastructureError, mintAskAgain, "refused"},
		{"permission_denied", connect.NewError(connect.CodePermissionDenied, errors.New("the caller may not mint for this attempt")), "IDENTITY_ENGINE_REFUSED", CategoryInfrastructureError, mintAskAgain, "refused"},
		{"unimplemented", connect.NewError(connect.CodeUnimplemented, errors.New("x")), "IDENTITY_ENGINE_REFUSED", CategoryInfrastructureError, mintAskAgain, "refused"},
		{"engine refused", refusal(connect.CodeFailedPrecondition, "IDENTITY_ENGINE_REFUSED", ""), "IDENTITY_ENGINE_REFUSED", CategoryInfrastructureError, mintAskAgain, "refused"},
		{"invalid_argument", connect.NewError(connect.CodeInvalidArgument, errors.New("x")), "IDENTITY_ENGINE_REFUSED", CategoryInfrastructureError, mintAskAgain, "invalid"},
		{"attempt not held", refusal(connect.CodeFailedPrecondition, "ATTEMPT_NOT_HELD", ""), "ATTEMPT_NOT_HELD", CategoryInfrastructureError, mintAskAgain, ""},
		{"not admitted", refusal(connect.CodeFailedPrecondition, "IDENTITY_NOT_ADMITTED", ""), "IDENTITY_NOT_ADMITTED", CategoryContractViolation, mintAskAgain, ""},
		{"not as admitted, on the admitting build", refusal(connect.CodeFailedPrecondition, "TASK_NOT_AS_ADMITTED", build), "TASK_NOT_AS_ADMITTED", CategoryInfrastructureError, mintEnd, ""},
		{"not as admitted, another build", refusal(connect.CodeFailedPrecondition, "TASK_NOT_AS_ADMITTED", "b-engine"), "IDENTITY_SPEC_SKEW", CategoryInfrastructureError, mintEnd, ""},
		{"attempts exhausted", refusal(connect.CodeFailedPrecondition, "IDENTITY_ATTEMPTS_EXHAUSTED", ""), "IDENTITY_ATTEMPTS_EXHAUSTED", CategoryInfrastructureError, mintEnd, ""},
		{"attempt expired", refusal(connect.CodeFailedPrecondition, "IDENTITY_ATTEMPT_EXPIRED", ""), "IDENTITY_ATTEMPT_EXPIRED", CategoryInfrastructureError, mintEnd, ""},
		{"revoked", refusal(connect.CodeFailedPrecondition, "IDENTITY_REVOKED", ""), "IDENTITY_REVOKED", CategoryContractViolation, mintStop, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := mapMintAnswer(tc.err, build)
			if got.Reason != tc.reason || got.Category != tc.cat || got.Then != tc.then || got.Count != tc.count {
				t.Fatalf("= %+v; want %s %s %v count %q", got, tc.reason, tc.cat, tc.then, tc.count)
			}
			if got.Message == "" {
				t.Fatal("an answer with no message")
			}
		})
	}
	// A wrapped refusal is read through its wrapping.
	if got := mapMintAnswer(fmt.Errorf("mint: %w", refusal(connect.CodeFailedPrecondition, "IDENTITY_REVOKED", "")), build); got.Reason != "IDENTITY_REVOKED" {
		t.Fatalf("a wrapped refusal: %+v", got)
	}
}

// TestMapMintAnswer_ReasonsAreClassified: every reason a mint's answer can
// end an attempt with is in the Orker's category table, with the retry the
// mint table gives it.
func TestMapMintAnswer_ReasonsAreClassified(t *testing.T) {
	for reason, retryable := range map[string]bool{
		"IDENTITY_UNAVAILABLE": true, "IDENTITY_ENGINE_REFUSED": true, "ATTEMPT_NOT_HELD": true,
		"IDENTITY_NOT_ADMITTED": false, "TASK_NOT_AS_ADMITTED": false, "IDENTITY_SPEC_SKEW": true,
		"IDENTITY_ATTEMPTS_EXHAUSTED": false, "IDENTITY_ATTEMPT_EXPIRED": false, "IDENTITY_REVOKED": false, "REQUIRED_RESULT_NOT_RECORDED": false,
	} {
		if _, ok := reasonCategory[reason]; !ok {
			t.Errorf("%s has no category", reason)
		}
		if got := RetryableReason(reason); got != retryable {
			t.Errorf("%s retryable = %v, want %v", reason, got, retryable)
		}
	}
}
