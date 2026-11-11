package must_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// recorder is a testing.TB that records what OK asked of it instead of
// stopping anything.
type recorder struct {
	testing.TB
	failed  bool
	message string
}

func (r *recorder) Helper() {}

func (r *recorder) Fatal(args ...any) {
	r.failed, r.message = true, fmt.Sprint(args...)
}

func (r *recorder) Fatalf(format string, args ...any) {
	r.failed, r.message = true, fmt.Sprintf(format, args...)
}

func TestOK(t *testing.T) {
	r := &recorder{TB: t}
	must.OK(r, nil)
	if r.failed {
		t.Errorf("a nil error failed the test: %q", r.message)
	}
	must.OK(r, errors.New("boom"))
	if !r.failed || r.message != "boom" {
		t.Errorf("failed=%v message=%q, want the error itself, as t.Fatal(err) reads", r.failed, r.message)
	}
}

func TestOKf(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name   string
		err    error
		format string
		args   []any
		want   string // empty: not failed
	}{
		{name: "nil error passes", format: "put %d", args: []any{1}},
		{name: "reads as t.Fatalf with \": %v\" appended", err: boom, format: "put attempt %d of %q", args: []any{2, "build"}, want: `put attempt 2 of "build": boom`},
		{name: "a plain name needs no arguments", err: boom, format: "GetRun", want: "GetRun: boom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &recorder{TB: t}
			must.OKf(r, tc.err, tc.format, tc.args...)
			if r.failed != (tc.want != "") || r.message != tc.want {
				t.Errorf("failed=%v message=%q, want failed=%v message=%q", r.failed, r.message, tc.want != "", tc.want)
			}
		})
	}
}
