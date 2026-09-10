package engine

import (
	"errors"
	"fmt"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestRetryStateEnumFor_MapsEveryTemporalValue walks the SDK's enum member by
// member.
//
// The two enums are NOT numerically compatible — temporal's 1 is IN_PROGRESS
// where OrkStack's 1 is NON_RETRYABLE_FAILURE — so every value from 1 up would
// be reported as the wrong one under a cast. This table is what stops anybody
// from replacing the switch with one.
func TestRetryStateEnumFor_MapsEveryTemporalValue(t *testing.T) {
	cases := []struct {
		from enumspb.RetryState
		want orkstackv1.RetryState
	}{
		{enumspb.RETRY_STATE_UNSPECIFIED, orkstackv1.RetryState_RETRY_STATE_UNSPECIFIED},
		// IN_PROGRESS names a retry that has not finished. This field is only
		// ever read from the error of a task that already reached a terminal
		// outcome, so "OrkStack does not know" is the honest answer rather than
		// a member of its own.
		{enumspb.RETRY_STATE_IN_PROGRESS, orkstackv1.RetryState_RETRY_STATE_UNSPECIFIED},
		{enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, orkstackv1.RetryState_RETRY_STATE_NON_RETRYABLE_FAILURE},
		{enumspb.RETRY_STATE_TIMEOUT, orkstackv1.RetryState_RETRY_STATE_TIMEOUT},
		{enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, orkstackv1.RetryState_RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED},
		{enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET, orkstackv1.RetryState_RETRY_STATE_RETRY_POLICY_NOT_SET},
		{enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR, orkstackv1.RetryState_RETRY_STATE_INTERNAL_SERVER_ERROR},
		{enumspb.RETRY_STATE_CANCEL_REQUESTED, orkstackv1.RetryState_RETRY_STATE_CANCEL_REQUESTED},
	}
	for _, tc := range cases {
		if got := retryStateEnumFor(tc.from); got != tc.want {
			t.Errorf("retryStateEnumFor(%v) = %v, want %v", tc.from, got, tc.want)
		}
	}

	// Every member of the SDK's enum is covered, so a future SDK value shows up
	// here as a failing count rather than as a silently UNSPECIFIED field on
	// somebody's run page.
	if len(cases) != len(enumspb.RetryState_name) {
		t.Fatalf("the table covers %d of the SDK's %d RetryState values; a new one needs a decision",
			len(cases), len(enumspb.RetryState_name))
	}

	// A value from a future SDK degrades to UNSPECIFIED, which is exactly what
	// a snapshot written before this field existed already says.
	if got := retryStateEnumFor(enumspb.RetryState(9999)); got != orkstackv1.RetryState_RETRY_STATE_UNSPECIFIED {
		t.Errorf("retryStateEnumFor(unknown) = %v, want UNSPECIFIED", got)
	}
}

// activityErrorWithRetryState builds the error a workflow ACTUALLY receives
// when an activity fails: the *ActivityError the SDK reconstructs from the
// server's Failure proto, carrying the retry state the server computed.
//
// It is built through the public failure converter rather than by hand because
// temporal exports the ActivityError TYPE but no constructor for it — and going
// through the converter is what proves the field survives the wire form the
// workflow really decodes, rather than a struct a test filled in.
//
// This is also why recordFailure's own retry_state is not asserted end to end
// in workflow_test.go: the SDK's TestWorkflowEnvironment does not simulate the
// server's retry-state computation at all. It hardcodes
// RETRY_STATE_UNSPECIFIED for every ordinary activity failure
// (internal/internal_workflow_testsuite.go, wrapActivityError call sites), so
// a test-environment run can only ever observe UNSPECIFIED — an assertion
// about the test double, not about OrkStack.
func activityErrorWithRetryState(state enumspb.RetryState, cause string) error {
	return temporal.GetDefaultFailureConverter().FailureToError(&failurepb.Failure{
		Message: "activity error",
		FailureInfo: &failurepb.Failure_ActivityFailureInfo{
			ActivityFailureInfo: &failurepb.ActivityFailureInfo{
				ActivityId:   "1",
				ActivityType: &commonpb.ActivityType{Name: "ExecuteTask"},
				RetryState:   state,
			},
		},
		Cause: &failurepb.Failure{
			Message: cause,
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "ActivityFailed"},
			},
		},
	})
}

// TestRetryStateOf_ReadsTheStateOffARealActivityError is the read path
// recordFailure uses, on the error shape a workflow really gets.
func TestRetryStateOf_ReadsTheStateOffARealActivityError(t *testing.T) {
	for state, want := range map[enumspb.RetryState]orkstackv1.RetryState{
		enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED: orkstackv1.RetryState_RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
		enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE:    orkstackv1.RetryState_RETRY_STATE_NON_RETRYABLE_FAILURE,
		enumspb.RETRY_STATE_TIMEOUT:                  orkstackv1.RetryState_RETRY_STATE_TIMEOUT,
		enumspb.RETRY_STATE_CANCEL_REQUESTED:         orkstackv1.RetryState_RETRY_STATE_CANCEL_REQUESTED,
		enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET:     orkstackv1.RetryState_RETRY_STATE_RETRY_POLICY_NOT_SET,
		enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR:    orkstackv1.RetryState_RETRY_STATE_INTERNAL_SERVER_ERROR,
	} {
		cause := activityErrorWithRetryState(state, "workload exited 1")
		var activityErr *temporal.ActivityError
		if !errors.As(cause, &activityErr) {
			t.Fatalf("the converter did not produce an *ActivityError for %v", state)
		}
		if got := retryStateOf(cause); got != want {
			t.Errorf("retryStateOf(%v) = %v, want %v", state, got, want)
		}
		// Wrapped once more on the way to recordFailure — errors.As, never a
		// type assertion, is what keeps that from silently reporting nothing.
		if got := retryStateOf(fmt.Errorf("collect task %q: %w", "flaky", cause)); got != want {
			t.Errorf("retryStateOf(wrapped %v) = %v, want %v", state, got, want)
		}
	}
}

// An error that never reached an activity has no retry state to report:
// UNSPECIFIED, which is also what a snapshot written before this field existed
// says. Nothing else may be invented for it.
func TestRetryStateOf_IsUnspecifiedForANonActivityError(t *testing.T) {
	for _, cause := range []error{
		errors.New("orkflow has no tasks"),
		temporal.NewApplicationError("task output invalid", "ContractViolation"),
		fmt.Errorf("wrapped: %w", errors.New("object storage is down")),
	} {
		if got := retryStateOf(cause); got != orkstackv1.RetryState_RETRY_STATE_UNSPECIFIED {
			t.Errorf("retryStateOf(%v) = %v, want UNSPECIFIED", cause, got)
		}
	}
}

// Every member of OrkStack's own enum is reachable from some Temporal value.
// An enum member nothing can produce is a promise to a client that never comes
// true.
func TestRetryStateEnum_EveryMemberIsReachable(t *testing.T) {
	reached := map[orkstackv1.RetryState]bool{}
	for value := range enumspb.RetryState_name {
		reached[retryStateEnumFor(enumspb.RetryState(value))] = true
	}
	for value, name := range orkstackv1.RetryState_name {
		if !reached[orkstackv1.RetryState(value)] {
			t.Errorf("%s is unreachable: no temporal RetryState maps onto it", name)
		}
	}
}
