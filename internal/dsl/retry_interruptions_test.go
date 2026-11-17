package dsl_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

// TestCompile_MaxInterruptions is E92's opt-out key: retry.max_interruptions
// caps how many times an interruption (a lost Orker, a hand-back, a task
// that never started) re-runs a task without spending its own attempts.
// Written, it compiles to RetryPolicy.max_interruptions, 0 included, which
// is the opt-out; absent, it stays absent, which is the deployment's
// budgets.
func TestCompile_MaxInterruptions(t *testing.T) {
	for _, tc := range []struct {
		name, retry string
		want        *int32
	}{
		{"absent", "{maximum_attempts: 3}", nil},
		{"the opt-out", "{max_interruptions: 0}", ptr(int32(0))},
		{"beside attempts", "{maximum_attempts: 3, max_interruptions: 2}", ptr(int32(2))},
		{"at its ceiling", "{max_interruptions: 100}", ptr(int32(100))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf, err := dsl.Compile(retryDoc(tc.retry))
			if err != nil {
				t.Fatalf("Compile(retry: %s): %v", tc.retry, err)
			}
			retry := wf.GetTasks()[0].GetRetry()
			switch {
			case tc.want == nil && retry.MaxInterruptions != nil:
				t.Fatalf("max_interruptions = %d, want it absent", retry.GetMaxInterruptions())
			case tc.want != nil && (retry.MaxInterruptions == nil || *retry.MaxInterruptions != *tc.want):
				t.Fatalf("max_interruptions = %v, want %d", retry.MaxInterruptions, *tc.want)
			}
		})
	}
}

// TestCompile_MaxInterruptionsRules: a negative value, or one above the
// ceiling of 100, is refused at the field, naming the task and the value.
func TestCompile_MaxInterruptionsRules(t *testing.T) {
	for _, tc := range []struct{ name, retry, value string }{
		{"negative", "{max_interruptions: -1}", "-1"},
		{"above the ceiling", "{maximum_attempts: 3, max_interruptions: 101}", "101"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile(retryDoc(tc.retry))
			if err == nil {
				t.Fatalf("Compile(retry: %s) accepted it", tc.retry)
			}
			if got := fieldOf(err); got != "tasks[0].retry.max_interruptions" {
				t.Errorf("refused at field %q, want tasks[0].retry.max_interruptions: %v", got, err)
			}
			for _, fragment := range []string{`task "fetch"`, "retry.max_interruptions " + tc.value, "0 to 100"} {
				if !strings.Contains(err.Error(), fragment) {
					t.Errorf("Compile(retry: %s) = %q, which does not say %q", tc.retry, err, fragment)
				}
			}
		})
	}
}

// TestDefinitionDigest_MaxInterruptionsOnlyWhenWritten: a document that
// does not write the key keeps the digest it had before the key existed,
// while writing it, 0 included, is a different definition.
func TestDefinitionDigest_MaxInterruptionsOnlyWhenWritten(t *testing.T) {
	digest := func(retry string) string {
		t.Helper()
		wf, err := dsl.Compile(retryDoc(retry))
		if err != nil {
			t.Fatal(err)
		}
		d, err := dsl.DefinitionDigest(wf)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	without, zero, two := digest("{maximum_attempts: 3}"), digest("{maximum_attempts: 3, max_interruptions: 0}"),
		digest("{maximum_attempts: 3, max_interruptions: 2}")
	if without == zero || zero == two || without == two {
		t.Fatalf("digests without, with 0 and with 2 = %s, %s, %s; want three different definitions", without, zero, two)
	}
	wf, err := dsl.Compile(retryDoc("{maximum_attempts: 3}"))
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := dsl.CanonicalOrkflow(wf)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(canonical), "maxInterruptions") {
		t.Fatalf("the canonical form names maxInterruptions when the document does not: %s", canonical)
	}
}

func ptr[T any](v T) *T { return &v }
