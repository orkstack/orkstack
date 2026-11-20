package task

import "testing"

// A task a resume runs again is a new logical activity (E85 §4.1): its key
// carries the resume's scope, so an external system sees it as one, and
// stays the same across that rerun's own retries. Scope 0 is the key the run
// had before any resume.
func TestScopedIdempotencyKeys(t *testing.T) {
	for _, tc := range []struct {
		name  string
		index int32 // -1: the task's key
		scope uint64
		want  string
	}{
		{"task, scope 0", -1, 0, "payments/ork-1/convert"},
		{"task, scoped", -1, 41, "payments/ork-1/convert@r41"},
		{"item, scope 0", 3, 0, "payments/ork-1/convert/3"},
		{"item, scoped", 3, 41, "payments/ork-1/convert@r41/3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			if tc.index < 0 {
				got = ScopedIdempotencyKeyFor("payments", "ork-1", "convert", tc.scope)
			} else {
				got = ScopedItemIdempotencyKeyFor("payments", "ork-1", "convert", tc.index, tc.scope)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
	if got := ScopedIdempotencyKeyFor("", "ork-1", "convert", 2); got != "default/ork-1/convert@r2" {
		t.Fatalf("an empty namespace keys as default: got %q", got)
	}
}
