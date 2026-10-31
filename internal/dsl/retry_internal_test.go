package dsl

import (
	"math"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestValidate_HandBuiltRetryRules: rule 47 is checked on the compiled
// message, so a request that never met the YAML front end meets it at the
// same path.
func TestValidate_HandBuiltRetryRules(t *testing.T) {
	withRetry := func(retry *orkstackv1.RetryPolicy) *orkstackv1.Orkflow {
		return &orkstackv1.Orkflow{Name: "hand", Tasks: []*orkstackv1.Task{{
			Id: "t", Uses: "shell", TimeoutSeconds: 60, Retry: retry,
		}}}
	}
	for name, tc := range map[string]struct {
		retry *orkstackv1.RetryPolicy
		field string
	}{
		"maximum below initial": {&orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 30, MaximumIntervalSeconds: 10},
			"tasks[0].retry.maximum_interval_seconds"},
		"coefficient below 1": {&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 0.25},
			"tasks[0].retry.backoff_coefficient"},
		"coefficient not a number": {&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: math.NaN()},
			"tasks[0].retry.backoff_coefficient"},
		"coefficient infinite": {&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: math.Inf(1), MaximumIntervalSeconds: 60},
			"tasks[0].retry.backoff_coefficient"},
		"coefficient negative infinite": {&orkstackv1.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: math.Inf(-1)},
			"tasks[0].retry.backoff_coefficient"},
		"initial negative": {&orkstackv1.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: -1},
			"tasks[0].retry.initial_interval_seconds"},
		"maximum negative": {&orkstackv1.RetryPolicy{MaximumAttempts: 3, MaximumIntervalSeconds: math.MinInt32},
			"tasks[0].retry.maximum_interval_seconds"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validate(withRetry(tc.retry), Options{}); err == nil || cacheFieldOf(err) != tc.field {
				t.Fatalf("validate = %v (field %q), want a refusal at %s", err, cacheFieldOf(err), tc.field)
			}
		})
	}
	for name, retry := range map[string]*orkstackv1.RetryPolicy{
		"no retry":            nil,
		"defaults":            {MaximumAttempts: 3},
		"maximum equal":       {MaximumAttempts: 3, InitialIntervalSeconds: 30, MaximumIntervalSeconds: 30},
		"no maximum":          {MaximumAttempts: 3, InitialIntervalSeconds: 120},
		"constant wait":       {MaximumAttempts: 3, BackoffCoefficient: 1},
		"largest coefficient": {MaximumAttempts: 3, BackoffCoefficient: math.MaxFloat64, MaximumIntervalSeconds: 60},
	} {
		if err := validate(withRetry(retry), Options{}); err != nil {
			t.Errorf("%s: validate refused %v: %v", name, retry, err)
		}
	}
}
