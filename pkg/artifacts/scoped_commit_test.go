package artifacts

import (
	"errors"
	"testing"
)

// A resumed run's reruns commit under the resume's scope (E85 §4.1): a
// segment r{scope:020} after the task, before any item. Scope 0 is the run
// before any resume and renders CommitKey's and ItemCommitKey's keys.
func TestScopedCommitKey_RendersTheDocumentedLayout(t *testing.T) {
	const ns, run = "payments", "ork-1700000000-1"
	for _, tc := range []struct {
		name  string
		index int32 // -1: the task's own commit
		scope uint64
		want  string
	}{
		{"task, scope 0", -1, 0, "metadata/commits/payments/ork-1700000000-1/convert/commit.json"},
		{"task, scoped", -1, 41, "metadata/commits/payments/ork-1700000000-1/convert/r00000000000000000041/commit.json"},
		{"item, scope 0", 97, 0, "metadata/commits/payments/ork-1700000000-1/convert/00097/commit.json"},
		{"item, scoped", 97, 41, "metadata/commits/payments/ork-1700000000-1/convert/r00000000000000000041/00097/commit.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			var err error
			if tc.index < 0 {
				got, err = ScopedCommitKey(ns, run, "convert", tc.scope)
			} else {
				got, err = ScopedItemCommitKey(ns, run, "convert", tc.index, tc.scope)
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
			// Every scoped key has an attested twin: the one mapping the
			// identity records already accept.
			if _, err := AttestedCommitKey(got); err != nil {
				t.Fatalf("AttestedCommitKey(%q): %v", got, err)
			}
		})
	}
}

func TestScopedCommitKey_RefusesWhatCommitKeyRefuses(t *testing.T) {
	for _, tc := range []struct {
		name, ns, run, task string
		index               int32
	}{
		{"bad namespace", "Bad_NS", "ork-1", "convert", 0},
		{"empty run", "payments", "", "convert", 0},
		{"bad task", "payments", "ork-1", "../x", 0},
		{"negative index", "payments", "ork-1", "convert", -1},
		{"index too large", "payments", "ork-1", "convert", MaxItemIndex + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ScopedItemCommitKey(tc.ns, tc.run, tc.task, tc.index, 7); !errors.Is(err, ErrInvalidCommitKey) {
				t.Fatalf("ScopedItemCommitKey error = %v, want ErrInvalidCommitKey", err)
			}
			if tc.index == 0 {
				if _, err := ScopedCommitKey(tc.ns, tc.run, tc.task, 7); !errors.Is(err, ErrInvalidCommitKey) {
					t.Fatalf("ScopedCommitKey error = %v, want ErrInvalidCommitKey", err)
				}
			}
		})
	}
}
