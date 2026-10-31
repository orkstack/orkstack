package engine

import (
	"math"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The spacing between a task's attempts (docs/DSL.md "Tasks"). A retry: that
// sets no maximum_interval_seconds waits at most the number its run's plan
// carries (RunPlan.RetryMaxIntervalSeconds, DefaultRetryMaxInterval at
// submit), so a release that changes the default moves only new runs. Every
// build before the field used 100 seconds, and a run whose history was
// recorded before the retry-max-interval gate, or whose plan carries no
// number, still does.

// RetryMaxIntervalChange is the workflow.GetVersion change id of the maximum
// retry interval a run's plan carries. Asked lazily, where the run first
// schedules a task whose retry: sets no maximum interval or whose backoff
// coefficient is a NaN (retryPolicyFor), and only by a run whose plan carries
// a number: a history recorded before the gate, or by a build that ignored
// the plan field, replays its DefaultVersion branch, which waits
// legacyRetryMaxInterval and schedules a NaN coefficient as it is.
const RetryMaxIntervalChange = "retry-max-interval"

// legacyRetryMaxInterval is what every build before the gate hardcoded, and
// what a DefaultVersion branch keeps; it is frozen
// (TestLegacyRetryMaxInterval_IsFrozen), and a change to the default moves
// DefaultRetryMaxInterval instead.
const legacyRetryMaxInterval = 100 * time.Second

// retryMaxIntervalGateDisabledForTest is a TEST-ONLY switch, the precedent of
// fairnessKeysGateDisabledForTest: when set, retryPolicyFor takes the
// DefaultVersion branch WITHOUT asking its version, so a recorder can write a
// history in the shape of the builds before the gate. Never set in
// production code.
var retryMaxIntervalGateDisabledForTest atomic.Bool

// retryPolicyFor converts the DSL retry policy to Temporal's. A missing policy
// means a single attempt: leaving it nil would inherit Temporal's default of
// unlimited retries, which contradicts the proto contract ("0 or 1 = no
// retries").
func (s *runState) retryPolicyFor(ctx workflow.Context, t *orkstackv1.Task) *temporal.RetryPolicy {
	retry := t.GetRetry()
	if retry == nil || retry.GetMaximumAttempts() <= 1 {
		return &temporal.RetryPolicy{MaximumAttempts: 1}
	}
	policy := &temporal.RetryPolicy{
		MaximumAttempts:    retry.GetMaximumAttempts(),
		InitialInterval:    time.Duration(retry.GetInitialIntervalSeconds()) * time.Second,
		BackoffCoefficient: retry.GetBackoffCoefficient(),
		MaximumInterval:    time.Duration(retry.GetMaximumIntervalSeconds()) * time.Second,
	}
	if policy.InitialInterval <= 0 {
		policy.InitialInterval = time.Second
	}
	if policy.BackoffCoefficient <= 0 {
		policy.BackoffCoefficient = 2.0
	}
	// A NaN, which a version applied before docs/DSL.md rule 47 may carry,
	// multiplies no wait into another, and no comparison with 1 below could
	// raise it. The cluster accepts it, so a run may already have scheduled
	// it: it reads as the default only where the run decided the
	// retry-max-interval path, never partway through a run that did not.
	if math.IsNaN(policy.BackoffCoefficient) && s.retryMaxIntervalPath(ctx) {
		policy.BackoffCoefficient = 2.0
	}
	// Such a version may also carry a coefficient below 1 or a maximum below
	// the initial interval. The cluster refuses either by failing the
	// workflow task, which it then retries forever
	// (TestIntegration_AVersionStoredBeforeTheRetryRuleIsScheduled), so no
	// recorded history holds such a command and raising each to the nearest
	// value the cluster accepts needs no gate.
	policy.BackoffCoefficient = max(policy.BackoffCoefficient, 1)
	if policy.MaximumInterval <= 0 {
		policy.MaximumInterval = s.unsetRetryMaxInterval(ctx, policy.InitialInterval)
	} else {
		policy.MaximumInterval = max(policy.MaximumInterval, policy.InitialInterval)
	}
	return policy
}

// retryMaxIntervalPath says whether the run takes the retry-max-interval
// path: its plan carries a number (every plan submitted from v0.1.1 on) and
// the gate says so. A plan without the number asks nothing, so a run
// submitted before the field never meets the gate, whatever release carries
// it on; a history recorded before the gate, or by a build that ignored the
// field, replays its DefaultVersion branch.
func (s *runState) retryMaxIntervalPath(ctx workflow.Context) bool {
	return s.plan.RetryMaxIntervalSeconds > 0 && !retryMaxIntervalGateDisabledForTest.Load() &&
		s.version(ctx, RetryMaxIntervalChange, 1) >= 1
}

// unsetRetryMaxInterval is the maximum interval of a retry: that sets none:
// the plan's number once the gate says so, and the legacy 100 seconds for a
// run whose plan carries no number or whose history predates the gate —
// either way never below the policy's own initial interval. The cluster
// refuses a maximum below the initial by failing the workflow task, which it
// then retries forever (TestIntegration_ARetryWithALongInitialIntervalIsScheduled),
// so no recorded history holds such a command, and raising the maximum to the
// initial interval in the DefaultVersion branch too changes no command a
// history recorded: it only schedules what every earlier build left stuck.
func (s *runState) unsetRetryMaxInterval(ctx workflow.Context, initial time.Duration) time.Duration {
	if !s.retryMaxIntervalPath(ctx) {
		return max(legacyRetryMaxInterval, initial)
	}
	return max(time.Duration(s.plan.RetryMaxIntervalSeconds)*time.Second, initial)
}
