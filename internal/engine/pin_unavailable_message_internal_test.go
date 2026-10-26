package engine

import (
	"errors"
	"fmt"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/task"
)

// TestPinUnavailableMessage_LegacyWordingIsByteIdentical: a run on the
// DefaultVersion branch — no decision, or the DefaultVersion one — reports
// every cause with the sentence every build before the gate wrote, naming the
// wait the run took. The string is spelled out here, not built from the
// code's own, so rewording it fails this test.
func TestPinUnavailableMessage_LegacyWordingIsByteIdentical(t *testing.T) {
	p := &pinState{name: "build", creationTimeout: legacyPinCreationTimeout}
	causes := map[string]error{
		"plain":   errors.New("too many outstanding sessions"),
		"cordon":  temporal.NewNonRetryableApplicationError("orker o is cordoned (r)", task.ReasonOrkerCordoned, nil),
		"timeout": temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, nil),
	}
	for name, cause := range causes {
		for label, decisions := range map[string]map[string]workflow.Version{
			"no decision":     nil,
			"DefaultVersion":  {PinCreationTimeoutChange: workflow.DefaultVersion},
			"another change":  {RefusedRouteChange: 1},
			"gated elsewhere": {PinCreationTimeoutChange: workflow.DefaultVersion, RefusedRouteChange: 1},
		} {
			t.Run(fmt.Sprintf("%s/%s", name, label), func(t *testing.T) {
				s := &runState{versions: decisions}
				want := `pin "build" could not be opened on pool "main" within 5m0s: no Orker of the pool had a free ` +
					`pin slot, so this task never reached one: ` + cause.Error()
				if got := s.pinUnavailableMessage(p, "main", cause); got != want {
					t.Errorf("message = %q\nwant      %q", got, want)
				}
			})
		}
	}
}

// TestPinUnavailableMessage_GatedNamesTheCause: from version 1 the message
// says what ended the wait, through the layers of Temporal's error wrapping
// the cause arrives in.
func TestPinUnavailableMessage_GatedNamesTheCause(t *testing.T) {
	s := &runState{versions: map[string]workflow.Version{PinCreationTimeoutChange: 1}}
	p := &pinState{name: "build", creationTimeout: 30 * time.Second}
	// An ActivityError, as the cluster delivers a failed activity: built
	// through the failure converter, the SDK exports no constructor for it.
	wrap := func(cause error) error {
		fc := temporal.GetDefaultFailureConverter()
		return fc.FailureToError(&failurepb.Failure{
			Message: "activity error",
			Cause:   fc.ErrorToFailure(cause),
			FailureInfo: &failurepb.Failure_ActivityFailureInfo{ActivityFailureInfo: &failurepb.ActivityFailureInfo{
				ActivityType: &commonpb.ActivityType{Name: "creation"}, ActivityId: "id",
				RetryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
			}},
		})
	}
	const (
		cordoned = `pin "build" could not be opened on pool "main": every pin host of the pool refused it because it is cordoned, so this task never reached one: `
		unstart  = `pin "build" could not be opened on pool "main" within 30s: no Orker of the pool had a free pin slot, so this task never reached one: `
		other    = `pin "build" could not be opened on pool "main": no pin host of the pool opened it, so this task never reached one: `
	)
	cases := []struct {
		name  string
		cause error
		want  string
	}{
		{"cordon refusal", wrap(temporal.NewNonRetryableApplicationError("every host said no", task.ReasonOrkerCordoned, nil)), cordoned},
		{"schedule to start", wrap(temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, nil)), unstart},
		{"another timeout claims no wait", wrap(temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, nil)), other},
		{"another application error", wrap(temporal.NewApplicationError("boom", "OTHER")), other},
		{"a plain error", errors.New("boom"), other},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := s.pinUnavailableMessage(p, "main", tc.cause), tc.want+tc.cause.Error(); got != want {
				t.Errorf("message = %q\nwant      %q", got, want)
			}
		})
	}
}
