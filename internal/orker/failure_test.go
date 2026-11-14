package orker

import (
	"errors"
	"fmt"
	"testing"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// allReasonCodes is every code the §8.2 table defines. It is written out
// rather than derived from the map under test, so a code that lost its row
// fails here instead of silently agreeing with itself.
var allReasonCodes = []string{
	ReasonContainerExitNonzero, ReasonContainerTimeout, ReasonRuntimeRefused, ReasonContainerStoppedByRuntime,
	ReasonWorkspaceNotBindable, ReasonWorkdirNotBindable, ReasonImagePullFailed,
	ReasonImageDigestMismatch, ReasonImageNotPinned, ReasonValuesMissing,
	ReasonValuesMalformed, ReasonValuesTooLarge, ReasonArtifactNotAFile,
	ReasonArtifactLimit, ReasonArtifactNameNotUTF8, ReasonArtifactUploadFailed,
	ReasonInputUnavailable, ReasonInputCorrupt, ReasonInputTooLarge,
	ReasonRenderFailed, ReasonWorkdirFailed, ReasonSecretUnresolved,
	ReasonSecretSchemeForbid, ReasonSecretNotPermitted, ReasonCommitReadFailed, ReasonCommitWriteFailed,
	ReasonCommitSchema, ReasonCommitIdentity, ReasonCommitUndecodable,
	ReasonCommitTooLarge, ReasonCommitNotConditional, ReasonCommitStoreUnavailable, ReasonOutputNotReadable,
	ReasonValuesCorrupt, ReasonValuesRefetchFailed, ReasonResourceRefused, ReasonResourceAdmissionMismatch,
	ReasonNetworkModeRefused, ReasonEachItemUnavailable, ReasonOrkerStopped, ReasonReportUndelivered, ReasonHeartbeatLost, ReasonAttemptStoreUnavailable,
	ReasonImageNotPermitted, ReasonImageNotSigned, ReasonImageSignatureInvalid,
	ReasonImageSignatureUnavailable, ReasonBindProbeImageUnavailable,
	ReasonBindProbeTimedOut, ReasonRuntimePostureRefused, ReasonRunAsOrkerUnavailable,
}

// TestReasonCodes_EveryCodeIsClassified is the guard against the one mistake
// this taxonomy makes easy: adding a reason code and forgetting to classify
// it. An unclassified code would silently become InfrastructureError and be
// retried forever against something deterministic.
func TestReasonCodes_EveryCodeIsClassified(t *testing.T) {
	for _, reason := range allReasonCodes {
		if _, ok := reasonCategory[reason]; !ok {
			t.Errorf("reason code %q has no category in the §8.2 table", reason)
		}
	}
	if len(reasonCategory) != len(allReasonCodes) {
		t.Errorf("the §8.2 table has %d rows but %d codes are declared; one side gained a code alone",
			len(reasonCategory), len(allReasonCodes))
	}
}

// TestReasonCodes_RetryabilityFollowsTheTable pins the rows where the
// classification is load-bearing, spelled out as the table rather than
// recomputed from the code under test.
func TestReasonCodes_RetryabilityFollowsTheTable(t *testing.T) {
	tests := []struct {
		reason        string
		wantCategory  string
		wantRetryable bool
		why           string
	}{
		{ReasonContainerExitNonzero, CategoryActivityFailed, true,
			"every non-zero status Wait returns is the workload's own, whatever the number"},
		{ReasonContainerTimeout, CategoryTimedOut, false,
			"a workload that timed out once would time out again on the same wall clock (§8.1)"},
		{ReasonRuntimeRefused, CategoryInfrastructureError, true,
			"the container never ran; the host may be fine next attempt"},
		{ReasonContainerStoppedByRuntime, CategoryInfrastructureError, true,
			"its daemon or someone using it stopped the container; the workload did not end on its own"},
		{ReasonWorkspaceNotBindable, CategoryInfrastructureError, false,
			"a host misconfiguration retrying cannot fix"},
		{ReasonCommitNotConditional, CategoryInfrastructureError, false,
			"the object store does not implement conditional create; retrying proves nothing"},
		{ReasonCommitStoreUnavailable, CategoryInfrastructureError, false,
			"a boot refusal: the Orker exits, so there is no attempt to retry"},
		{ReasonOutputNotReadable, CategoryContractViolation, false,
			"the image writes the same mode every attempt, so retrying loops forever"},
		{ReasonValuesMissing, CategoryContractViolation, false,
			"the image is wrong, not the world"},
		{ReasonSecretNotPermitted, CategoryContractViolation, false,
			"a policy refusal refuses identically forever"},
		{ReasonCommitReadFailed, CategoryInfrastructureError, true,
			"never proceed to execution blind, but do try again"},
		{ReasonValuesRefetchFailed, CategoryInfrastructureError, true,
			"the store blinked while re-reading an adopted commit's values"},
		{ReasonValuesCorrupt, CategoryContractViolation, false,
			"the store returned bytes that contradict the commit; no retry makes them match"},
		{ReasonArtifactUploadFailed, CategoryInfrastructureError, true,
			"storage failures are usually transient"},
		{ReasonArtifactNameNotUTF8, CategoryContractViolation, false,
			"no protobuf encoding can carry the name (§14.16)"},
		{ReasonImageNotPermitted, CategoryContractViolation, false,
			"an image outside the pinned policy refuses identically on every attempt"},
		{ReasonImageNotSigned, CategoryContractViolation, false,
			"a digest with no signature object is the same digest next attempt"},
		{ReasonImageSignatureInvalid, CategoryContractViolation, false,
			"no signature verifies with the pinned key, and none will"},
		{ReasonImageSignatureUnavailable, CategoryInfrastructureError, true,
			"the registry could not be asked; that is IMAGE_PULL_FAILED's situation, never a verdict"},
		{ReasonBindProbeTimedOut, CategoryInfrastructureError, true,
			"a slow daemon says nothing about the work root; the next boot may find it fast"},
		{ReasonWorkdirNotBindable, CategoryInfrastructureError, false,
			"the daemon refused the bind, in its own words: a host fact a restart does not change"},
		{ReasonRunAsOrkerUnavailable, CategoryInfrastructureError, false,
			"the boot probe found the host maps the Orker's uid elsewhere; the next attempt on this Orker meets the same host"},
		{ReasonHeartbeatLost, CategoryInfrastructureError, true,
			"the cluster did not answer in time; nothing about the workload, and the next attempt may find it answering"},
		{ReasonOrkerStopped, CategoryInfrastructureError, true,
			"the host went away, not the workload: what the heartbeat timeout it stands beside says"},
		{ReasonReportUndelivered, CategoryInfrastructureError, true,
			"recorded only: the cluster, which never took the report, times the attempt out and retries it"},
	}
	for _, tc := range tests {
		t.Run(tc.reason, func(t *testing.T) {
			if got := CategoryFor(tc.reason); got != tc.wantCategory {
				t.Errorf("CategoryFor(%q) = %q, want %q — %s", tc.reason, got, tc.wantCategory, tc.why)
			}
			if got := RetryableReason(tc.reason); got != tc.wantRetryable {
				t.Errorf("RetryableReason(%q) = %t, want %t — %s",
					tc.reason, got, tc.wantRetryable, tc.why)
			}
		})
	}
}

// TestCategories_AreTheTemporalErrorTypeStrings is the test that would have
// caught a rename.
//
// RetryPolicy.NonRetryableErrorTypes matches an ApplicationError's type string
// LITERALLY, not with errors.Is. If the constant and the string ever drift,
// nothing fails to compile and contract violations quietly start retrying.
func TestCategories_AreTheTemporalErrorTypeStrings(t *testing.T) {
	spellings := map[string]string{
		CategoryActivityFailed:      "ActivityFailed",
		CategoryInfrastructureError: "InfrastructureError",
		CategoryContractViolation:   "ContractViolation",
		CategoryTimedOut:            "TimedOut",
		CategoryCancelled:           "Cancelled",
	}
	for constant, literal := range spellings {
		if constant != literal {
			t.Errorf("category constant is %q but the wire spelling is %q", constant, literal)
		}
	}
	// The engine and the Orker must agree, or the list the engine hands
	// Temporal names categories the Orker never constructs.
	if CategoryContractViolation != task.CategoryContractViolation {
		t.Error("internal/orker and internal/task disagree on ContractViolation")
	}
	nonRetryable := task.NonRetryableCategories()
	if len(nonRetryable) != 2 ||
		nonRetryable[0] != CategoryContractViolation || nonRetryable[1] != CategoryTimedOut {
		t.Errorf("NonRetryableCategories = %v, want [ContractViolation TimedOut]", nonRetryable)
	}
}

// TestFailure_TemporalErrorCarriesTheCategoryAsItsType proves the transport:
// the ApplicationError's TYPE is the category (what the retry policy matches)
// and the reason code travels in the detail (too granular for a policy to
// enumerate).
func TestFailure_TemporalErrorCarriesTheCategoryAsItsType(t *testing.T) {
	failure := Fail(ReasonValuesMissing, "no values.json").WithDetail(
		&orkstackv1.ActivityFailureDetail{ExitCode: 0})

	err := failure.TemporalError()

	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) {
		t.Fatalf("TemporalError() = %T, want *temporal.ApplicationError", err)
	}
	if applicationErr.Type() != CategoryContractViolation {
		t.Errorf("error type = %q, want the category %q", applicationErr.Type(), CategoryContractViolation)
	}
	if !applicationErr.NonRetryable() {
		t.Error("a ContractViolation is retryable; the constructor's flag must be set so a " +
			"task whose retry policy forgot the category still cannot loop")
	}
	// Details takes a pointer to the value it assigns, so the target is a
	// **ActivityFailureDetail: in-process the SDK assigns the stored pointer
	// directly, and across the wire the proto converter decodes into it.
	var detail *orkstackv1.ActivityFailureDetail
	must.OKf(t, applicationErr.Details(&detail), "decode failure detail")
	if detail.GetReasonCode() != ReasonValuesMissing {
		t.Errorf("detail reason = %q, want %q", detail.GetReasonCode(), ReasonValuesMissing)
	}
	if detail.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION {
		t.Errorf("detail category = %v, want CONTRACT_VIOLATION", detail.GetCategory())
	}
}

// TestFailure_TemporalErrorSaysItsSentenceOnce: what the engine receives is the
// failure's own sentence under its category, retried or not, with its detail.
// The error the sentence came from stays on the Failure for this Orker; sent
// along, it comes back as a second error named after its Go type, marked
// retryable whatever the failure's own answer, saying the sentence again.
func TestFailure_TemporalErrorSaysItsSentenceOnce(t *testing.T) {
	cause := errors.New("the store said no")
	wire := temporal.GetDefaultFailureConverter()
	for _, tc := range []struct {
		reason, category string
		retryable        bool
	}{
		{ReasonSecretNotPermitted, CategoryContractViolation, false},
		{ReasonRuntimeRefused, CategoryInfrastructureError, true},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			failure := FailCause(tc.reason, cause, "read it: %v", cause).
				WithDetail(&orkstackv1.ActivityFailureDetail{ExitCode: 3})
			if !errors.Is(failure, cause) {
				t.Errorf("the Failure lost its cause: %v", failure)
			}

			sent := wire.ErrorToFailure(failure.TemporalError())
			if sent.GetCause() != nil {
				t.Errorf("a second error crossed beneath the failure: %v", sent.GetCause())
			}
			got := wire.FailureToError(sent)
			want := fmt.Sprintf("%s: read it: the store said no (type: %s, retryable: %t)", tc.reason, tc.category, tc.retryable)
			if got.Error() != want {
				t.Errorf("the engine reads %q, want %q", got.Error(), want)
			}

			var applicationErr *temporal.ApplicationError
			if !errors.As(got, &applicationErr) {
				t.Fatalf("the engine reads a %T, want *temporal.ApplicationError", got)
			}
			var detail *orkstackv1.ActivityFailureDetail
			must.OKf(t, applicationErr.Details(&detail), "decode failure detail")
			if detail.GetReasonCode() != tc.reason || detail.GetExitCode() != 3 {
				t.Errorf("detail = %v, want reason %s and exit code 3", detail, tc.reason)
			}
		})
	}
}

// TestFailure_RetryableCategoryIsNotConstructedNonRetryable is the other half:
// an ActivityFailed must stay retryable so a task's own retry policy governs
// it.
func TestFailure_RetryableCategoryIsNotConstructedNonRetryable(t *testing.T) {
	err := Fail(ReasonContainerExitNonzero, "exited 1").TemporalError()

	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) {
		t.Fatalf("TemporalError() = %T, want *temporal.ApplicationError", err)
	}
	if applicationErr.NonRetryable() {
		t.Error("a container exiting non-zero was constructed non-retryable; retry behavior " +
			"belongs to the task's Temporal policy, not to the Orker (AGENTS.md §3.2)")
	}
}
