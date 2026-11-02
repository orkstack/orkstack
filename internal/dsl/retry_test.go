package dsl_test

import (
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
// is refused at compile, naming the path and the values, instead of leaving
// every run that reaches the task stuck on a workflow task the cluster fails
// forever. Each refusal is placed at the field, names the task, pairs each
// path it speaks of with the value written there (so a swapped pair is
// caught), and says which rule it is in one word; the sentence around them
// is the author's to read, not this test's to copy.
func TestCompile_RetryRules(t *testing.T) {
	for _, tc := range []struct {
		name, retry, field string
		says               []string
	}{
		{
			name:  "a negative initial interval",
			retry: "{maximum_attempts: 3, initial_interval_seconds: -5}",
			field: "tasks[0].retry.initial_interval_seconds",
			says:  []string{`task "fetch"`, "retry.initial_interval_seconds -5", "negative"},
		},
		{
			name:  "a negative maximum interval",
			retry: "{maximum_attempts: 3, maximum_interval_seconds: -1}",
			field: "tasks[0].retry.maximum_interval_seconds",
			says:  []string{`task "fetch"`, "retry.maximum_interval_seconds -1", "negative"},
		},
		{
			name:  "a negative maximum interval after a set initial one",
			retry: "{maximum_attempts: 3, initial_interval_seconds: 30, maximum_interval_seconds: -60}",
			field: "tasks[0].retry.maximum_interval_seconds",
			says:  []string{`task "fetch"`, "retry.maximum_interval_seconds -60", "negative"},
		},
		{
			name:  "a coefficient just below 1",
			retry: "{maximum_attempts: 3, backoff_coefficient: 0.999}",
			field: "tasks[0].retry.backoff_coefficient",
			says:  []string{`task "fetch"`, "retry.backoff_coefficient 0.999", "below 1"},
		},
		{
			name:  "an infinite coefficient",
			retry: "{maximum_attempts: 3, backoff_coefficient: .inf, maximum_interval_seconds: 60}",
			field: "tasks[0].retry.backoff_coefficient",
			says:  []string{`task "fetch"`, "retry.backoff_coefficient +Inf", "finite"},
		},
		{
			name:  "a maximum below the initial interval",
			retry: "{maximum_attempts: 3, initial_interval_seconds: 30, maximum_interval_seconds: 10}",
			field: "tasks[0].retry.maximum_interval_seconds",
			says: []string{`task "fetch"`, "retry.maximum_interval_seconds 10 is below retry.initial_interval_seconds 30",
				"at least 30"},
		},
		{
			name:  "a maximum below the initial interval with one attempt",
			retry: "{maximum_attempts: 1, initial_interval_seconds: 5, maximum_interval_seconds: 4}",
			field: "tasks[0].retry.maximum_interval_seconds",
			says: []string{`task "fetch"`, "retry.maximum_interval_seconds 4 is below retry.initial_interval_seconds 5",
				"at least 5"},
		},
		{
			name:  "a coefficient between 0 and 1",
			retry: "{maximum_attempts: 3, backoff_coefficient: 0.5}",
			field: "tasks[0].retry.backoff_coefficient",
			says:  []string{`task "fetch"`, "retry.backoff_coefficient 0.5", "below 1"},
		},
		{
			name:  "a negative coefficient",
			retry: "{maximum_attempts: 3, backoff_coefficient: -2}",
			field: "tasks[0].retry.backoff_coefficient",
			says:  []string{`task "fetch"`, "retry.backoff_coefficient -2", "below 1"},
		},
		{
			name:  "a coefficient that is not a number",
			retry: "{maximum_attempts: 3, backoff_coefficient: .nan}",
			field: "tasks[0].retry.backoff_coefficient",
			says:  []string{`task "fetch"`, "retry.backoff_coefficient NaN", "finite"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile(retryDoc(tc.retry))
			if err == nil {
				t.Fatalf("Compile(retry: %s) accepted a retry the cluster would refuse", tc.retry)
			}
			if got := fieldOf(err); got != tc.field {
				t.Errorf("Compile(retry: %s) refused at field %q, want %q: %v", tc.retry, got, tc.field, err)
			}
			for _, fragment := range tc.says {
				if !strings.Contains(err.Error(), fragment) {
					t.Errorf("Compile(retry: %s) = %q, which does not say %q", tc.retry, err, fragment)
				}
			}
		})
	}
	// An interval is an integer field, so a non-finite one never reaches the
	// rule: the document is refused at the line that writes it, naming the
	// value. (retryDoc opens with a blank line, so its retry: is line 9.)
	for _, notFinite := range []struct{ retry, value string }{
		{"{maximum_attempts: 3, initial_interval_seconds: .inf}", ".inf"},
		{"{maximum_attempts: 3, maximum_interval_seconds: -.inf}", "-.inf"},
		{"{maximum_attempts: 3, maximum_interval_seconds: .nan}", ".nan"},
	} {
		_, err := dsl.Compile(retryDoc(notFinite.retry))
		if err == nil || !strings.Contains(err.Error(), "line 9") || !strings.Contains(err.Error(), notFinite.value) {
			t.Errorf("Compile(retry: %s) = %v, want the document refused at line 9 naming %s",
				notFinite.retry, err, notFinite.value)
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
