package dsl_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

func retryDoc(retry string) []byte {
	return []byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: retried
tasks:
  - id: fetch
    uses: shell
    with: {op: exec, command: ["true"]}
    retry: ` + retry + "\n")
}

// TestCompile_RetryRules is rule 47: a retry: the cluster would refuse — a
// maximum interval below the initial one, or a backoff coefficient below 1 —
// is refused at compile, naming the path and both values, instead of leaving
// every run that reaches the task stuck on a workflow task the cluster fails
// forever.
func TestCompile_RetryRules(t *testing.T) {
	const maxBelow = `task "fetch": retry.maximum_interval_seconds %d is below retry.initial_interval_seconds %d — ` +
		`the longest wait between attempts cannot be shorter than the first; set it to at least %d, ` +
		`or omit it for the default, which is never shorter than the first`
	const coefficientBelow = `task "fetch": retry.backoff_coefficient %s is below 1 — it multiplies each wait ` +
		`to give the next, so it must be at least 1; write 1 for a constant wait, or omit it (2: each wait doubles)`
	const coefficientNotFinite = `task "fetch": retry.backoff_coefficient %s is not a finite number — it multiplies ` +
		`each wait to give the next, so it must be a number of at least 1; write 1 for a constant wait, or omit it ` +
		`(2: each wait doubles)`
	const initialNegative = `task "fetch": retry.initial_interval_seconds %d is negative — it is the wait before ` +
		`the second attempt, in seconds; omit it for the default of 1`
	const maxNegative = `task "fetch": retry.maximum_interval_seconds %d is negative — it is the longest wait ` +
		`between attempts, in seconds; omit it for the default, which is never shorter than the first`
	for _, tc := range []struct {
		name, retry, field, message string
	}{
		{
			name:    "a negative initial interval",
			retry:   "{maximum_attempts: 3, initial_interval_seconds: -5}",
			field:   "tasks[0].retry.initial_interval_seconds",
			message: fmt.Sprintf(initialNegative, -5),
		},
		{
			name:    "a negative maximum interval",
			retry:   "{maximum_attempts: 3, maximum_interval_seconds: -1}",
			field:   "tasks[0].retry.maximum_interval_seconds",
			message: fmt.Sprintf(maxNegative, -1),
		},
		{
			name:    "a negative maximum interval after a set initial one",
			retry:   "{maximum_attempts: 3, initial_interval_seconds: 30, maximum_interval_seconds: -60}",
			field:   "tasks[0].retry.maximum_interval_seconds",
			message: fmt.Sprintf(maxNegative, -60),
		},
		{
			name:    "a coefficient just below 1",
			retry:   "{maximum_attempts: 3, backoff_coefficient: 0.999}",
			field:   "tasks[0].retry.backoff_coefficient",
			message: fmt.Sprintf(coefficientBelow, "0.999"),
		},
		{
			name:    "an infinite coefficient",
			retry:   "{maximum_attempts: 3, backoff_coefficient: .inf, maximum_interval_seconds: 60}",
			field:   "tasks[0].retry.backoff_coefficient",
			message: fmt.Sprintf(coefficientNotFinite, "+Inf"),
		},
		{
			name:    "a maximum below the initial interval",
			retry:   "{maximum_attempts: 3, initial_interval_seconds: 30, maximum_interval_seconds: 10}",
			field:   "tasks[0].retry.maximum_interval_seconds",
			message: fmt.Sprintf(maxBelow, 10, 30, 30),
		},
		{
			name:    "a maximum below the initial interval with one attempt",
			retry:   "{maximum_attempts: 1, initial_interval_seconds: 5, maximum_interval_seconds: 4}",
			field:   "tasks[0].retry.maximum_interval_seconds",
			message: fmt.Sprintf(maxBelow, 4, 5, 5),
		},
		{
			name:    "a coefficient between 0 and 1",
			retry:   "{maximum_attempts: 3, backoff_coefficient: 0.5}",
			field:   "tasks[0].retry.backoff_coefficient",
			message: fmt.Sprintf(coefficientBelow, "0.5"),
		},
		{
			name:    "a negative coefficient",
			retry:   "{maximum_attempts: 3, backoff_coefficient: -2}",
			field:   "tasks[0].retry.backoff_coefficient",
			message: fmt.Sprintf(coefficientBelow, "-2"),
		},
		{
			name:    "a coefficient that is not a number",
			retry:   "{maximum_attempts: 3, backoff_coefficient: .nan}",
			field:   "tasks[0].retry.backoff_coefficient",
			message: fmt.Sprintf(coefficientNotFinite, "NaN"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile(retryDoc(tc.retry))
			if err == nil || fieldOf(err) != tc.field || err.Error() != tc.message {
				t.Fatalf("Compile(retry: %s) = %v (field %q)\nwant %q at %q", tc.retry, err, fieldOf(err), tc.message, tc.field)
			}
		})
	}
	// An interval is an integer field, so a non-finite one never reaches the
	// rule: the document does not decode.
	for _, notFinite := range []string{
		"{maximum_attempts: 3, initial_interval_seconds: .inf}",
		"{maximum_attempts: 3, maximum_interval_seconds: -.inf}",
		"{maximum_attempts: 3, maximum_interval_seconds: .nan}",
	} {
		if _, err := dsl.Compile(retryDoc(notFinite)); err == nil || !strings.Contains(err.Error(), "into int32") {
			t.Errorf("Compile(retry: %s) = %v, want the document refused as not an integer", notFinite, err)
		}
	}
	for _, ok := range []string{
		"{maximum_attempts: 3}",
		"{maximum_attempts: 3, initial_interval_seconds: 30, maximum_interval_seconds: 30}",
		"{maximum_attempts: 3, initial_interval_seconds: 120}",
		"{maximum_attempts: 3, maximum_interval_seconds: 1}",
		"{maximum_attempts: 3, backoff_coefficient: 1}",
		"{maximum_attempts: 3, backoff_coefficient: 0}",
		"{maximum_attempts: 3, initial_interval_seconds: 2, backoff_coefficient: 1.5, maximum_interval_seconds: 10}",
	} {
		if _, err := dsl.Compile(retryDoc(ok)); err != nil {
			t.Errorf("Compile(retry: %s) refused: %v", ok, err)
		}
	}
}
