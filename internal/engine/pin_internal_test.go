package engine

import (
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestPinFailureError_SeparatesALostHostFromAnExpiredLifetime is the
// PIN_HOST_LOST / PIN_TIMEOUT classification (docs/DSL.md "Pins"), held on
// its own.
//
// Both failures reach the workflow as the SAME event — the pin's session was
// marked failed, because its creation activity stopped — and the elapsed time
// is the ONLY thing that tells them apart. That makes this a table: a decision
// on two numbers, with a boundary, and it is worth stating exactly rather than
// inferring from whichever of the two a workflow test happens to reach.
//
// It is tested here rather than only through TestWorkflowEnvironment because
// nothing outside the SDK can put a session into SessionStateFailed at a
// chosen workflow time: the environment's clock advances through timers, so a
// test can reach each branch but cannot sweep the boundary.
func TestPinFailureError_SeparatesALostHostFromAnExpiredLifetime(t *testing.T) {
	cases := []struct {
		name     string
		elapsed  time.Duration
		limit    time.Duration
		wantCode string
		wantCat  orkstackv1.FailureCategory
	}{{
		name:     "host lost early in the pin's life",
		elapsed:  2 * time.Second,
		limit:    time.Hour,
		wantCode: "PIN_HOST_LOST",
		wantCat:  orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
	}, {
		name:     "host lost one tick short of the lifetime",
		elapsed:  time.Hour - time.Nanosecond,
		limit:    time.Hour,
		wantCode: "PIN_HOST_LOST",
		wantCat:  orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
	}, {
		// The boundary belongs to the timeout: at exactly the lifetime the
		// pin HAS reached its bound, and the creation activity's
		// StartToCloseTimeout IS that lifetime, so this is the moment
		// Temporal itself ends it.
		name:     "exactly at the lifetime",
		elapsed:  time.Hour,
		limit:    time.Hour,
		wantCode: "PIN_TIMEOUT",
		wantCat:  orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT,
	}, {
		name:     "past the lifetime",
		elapsed:  2 * time.Hour,
		limit:    time.Hour,
		wantCode: "PIN_TIMEOUT",
		wantCat:  orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT,
	}, {
		// A pin with no bound at all cannot have exceeded one. It is not
		// reachable through Plan — a pin always gets a lifetime, declared or
		// derived — and a zero here must not read as "every failure is a
		// timeout", which is what a bare >= comparison would have said.
		name:     "no lifetime recorded",
		elapsed:  time.Hour,
		limit:    0,
		wantCode: "PIN_HOST_LOST",
		wantCat:  orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := pinFailureError("build", tc.elapsed, tc.limit)
			if got.GetCode() != tc.wantCode {
				t.Errorf("code = %q, want %q", got.GetCode(), tc.wantCode)
			}
			if got.GetCategory() != tc.wantCat {
				t.Errorf("category = %v, want %v", got.GetCategory(), tc.wantCat)
			}
			// Neither is the workload's fault and neither can be retried: a
			// pin never moves, so a reader told "retryable" would retry a task
			// onto an Orker with an empty /orkstack/shared.
			if got.GetRetryable() {
				t.Error("retryable = true; a lost pin is never retryable")
			}
			if got.GetMessage() == "" {
				t.Error("message is empty; it is what a client is shown")
			}
		})
	}
}

// TestRunState_PinLifetimeIsTheAuthorsOrTheSequentialWorstCase pins the pin
// lifetime rule: the author's timeout_seconds, or the derived sum.
//
// The declared number wins outright, because a lifetime is a property of the
// group and its author said so. With none declared the engine cannot know the
// pin's internal parallelism more cheaply than by assuming there is none, so
// it sums every member's own wall clock, its cancellation grace and this run's
// transfer budget — the same three a member's own StartToCloseTimeout is built
// from — and adds the release budget, because cleanup has to fit inside the
// session too. An over-estimate costs nothing (a pin closes on its last
// member, never on this bound); an under-estimate kills a healthy pin.
func TestRunState_PinLifetimeIsTheAuthorsOrTheSequentialWorstCase(t *testing.T) {
	state := &runState{
		plan: RunPlan{TransferBudgetSeconds: 15},
		tasks: map[string]*orkstackv1.Task{
			// Defaults: 300s budget, 30s grace.
			"prep": {Id: "prep", Pin: "build"},
			"compile": {
				Id:                             "compile",
				Pin:                            "build",
				TimeoutSeconds:                 600,
				CancellationGracePeriodSeconds: 45,
			},
		},
	}
	members := []string{"prep", "compile"}

	derived := state.pinLifetime(&pinState{
		name:  "build",
		pin:   &orkstackv1.Pin{Name: "build"},
		tasks: members,
	})
	want := time.Duration((300+30+15)+(600+45+15))*time.Second + pinReleaseBudget
	if derived != want {
		t.Errorf("derived lifetime = %s, want %s", derived, want)
	}

	declared := state.pinLifetime(&pinState{
		name:  "build",
		pin:   &orkstackv1.Pin{Name: "build", TimeoutSeconds: 900},
		tasks: members,
	})
	if declared != 900*time.Second {
		t.Errorf("declared lifetime = %s, want the author's 900s and no release budget added to it",
			declared)
	}
}
