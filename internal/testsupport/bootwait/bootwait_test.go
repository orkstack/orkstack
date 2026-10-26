package bootwait_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/bootwait"
)

// logRecorder is a Wait.Logf that keeps what was logged.
type logRecorder struct {
	mu    sync.Mutex
	lines []string
}

func (l *logRecorder) Logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logRecorder) joined() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

// returnsWithin runs fn and fails the test when it does not return within
// limit. A wait that must end at once and instead blocks on a one-hour timer is
// reported by this, not by the package's go test timeout.
func returnsWithin(t *testing.T, limit time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		t.Fatalf("the wait did not return within %s", limit)
	}
}

// TestDefaults_TolerateTheStallClassAndStillWarnOnIt pins the numbers a Wait
// with no durations uses. An engine boot has been measured stalling 50 s on
// its fresh bucket's first read while the object store grew and removed
// per-bucket volumes: the deadline must outlast that with room to spare, and
// the warning threshold must sit under it, so the stall is recorded rather
// than absorbed.
func TestDefaults_TolerateTheStallClassAndStillWarnOnIt(t *testing.T) {
	const measuredStall = 50 * time.Second
	if bootwait.Deadline != 180*time.Second {
		t.Fatalf("Deadline = %s, want 180s", bootwait.Deadline)
	}
	if bootwait.Deadline < 3*measuredStall {
		t.Fatalf("Deadline %s does not tolerate three times the measured %s stall", bootwait.Deadline, measuredStall)
	}
	if bootwait.WarnAfter != 20*time.Second {
		t.Fatalf("WarnAfter = %s, want 20s", bootwait.WarnAfter)
	}
	if bootwait.WarnAfter >= measuredStall {
		t.Fatalf("WarnAfter %s would not record the measured %s stall", bootwait.WarnAfter, measuredStall)
	}
	if bootwait.Interval != 250*time.Millisecond {
		t.Fatalf("Interval = %s, want 250ms", bootwait.Interval)
	}

	// A zero Wait is bounded by Deadline: the probe sees its context.
	var remaining time.Duration
	if _, err := (bootwait.Wait{What: "ork"}).Until(context.Background(), func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			return errors.New("the probe's context has no deadline")
		}
		remaining = time.Until(deadline)
		return nil
	}); err != nil {
		t.Fatalf("Until: %v", err)
	}
	if remaining > bootwait.Deadline || remaining < bootwait.Deadline-10*time.Second {
		t.Fatalf("a zero Wait bounds its probe by %s, want about %s", remaining, bootwait.Deadline)
	}
}

func TestUntil_ReturnsOnceTheProbeAnswersAndLogsTheDuration(t *testing.T) {
	var log logRecorder
	var probes int
	w := bootwait.Wait{What: "ork at 127.0.0.1:9", Logf: log.Logf, Interval: time.Millisecond}

	took, err := w.Until(context.Background(), func(context.Context) error {
		probes++
		if probes < 3 {
			return errors.New("connection refused")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Until: %v", err)
	}
	if probes != 3 {
		t.Fatalf("probes = %d, want 3: it probes until the first answer and stops", probes)
	}
	// Two failed probes were each followed by one Interval.
	if took < 2*time.Millisecond {
		t.Fatalf("took = %s, want at least the two intervals it waited", took)
	}
	out := log.joined()
	if !strings.Contains(out, "ork at 127.0.0.1:9 became ready after ") {
		t.Fatalf("log does not report the boot's duration:\n%s", out)
	}
	if strings.Contains(out, "WARN") {
		t.Fatalf("a boot well under the warning threshold logged a warning:\n%s", out)
	}
}

func TestUntil_ProbesAtOnceBeforeAnyInterval(t *testing.T) {
	w := bootwait.Wait{What: "ork", Interval: time.Hour, Deadline: time.Hour}
	returnsWithin(t, 10*time.Second, func() {
		if _, err := w.Until(context.Background(), func(context.Context) error { return nil }); err != nil {
			t.Errorf("Until: %v", err)
		}
	})
}

func TestUntil_ASlowBootPassesWithAWarnLine(t *testing.T) {
	var log logRecorder
	var probes int
	// Twenty failed probes are twenty intervals of at least 2ms: past the
	// 10ms warning threshold on any host, with nothing slept to get there.
	w := bootwait.Wait{
		What:      "orker",
		Logf:      log.Logf,
		WarnAfter: 10 * time.Millisecond,
		Interval:  2 * time.Millisecond,
	}
	took, err := w.Until(context.Background(), func(context.Context) error {
		probes++
		if probes <= 20 {
			return errors.New("not yet")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("a slow boot under the deadline must pass: %v", err)
	}
	if took < 40*time.Millisecond {
		t.Fatalf("took = %s, want at least twenty 2ms intervals", took)
	}
	out := log.joined()
	for _, want := range []string{
		"orker became ready after ",
		"WARN: orker took ",
		"to become ready (over the 10ms warning threshold)",
		"a slow boot is recorded here, not absorbed",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log is missing %q:\n%s", want, out)
		}
	}
}

func TestUntil_TheDeadlineFailsWithTheLastProbeError(t *testing.T) {
	cases := []struct {
		name  string
		probe func(ctx context.Context) error
		want  []string
	}{
		{
			name:  "a probe that answers with an error",
			probe: func(context.Context) error { return errors.New("exit 7: no such orkflow store") },
			want:  []string{"ork did not become ready within 30ms", "last probe: exit 7: no such orkflow store"},
		},
		{
			name: "a probe that hangs until its context ends",
			probe: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			want: []string{"ork did not become ready within 30ms", "last probe: context deadline exceeded"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var log logRecorder
			w := bootwait.Wait{What: "ork", Logf: log.Logf, Deadline: 30 * time.Millisecond, Interval: time.Millisecond}
			var (
				took time.Duration
				err  error
			)
			returnsWithin(t, 10*time.Second, func() { took, err = w.Until(context.Background(), tc.probe) })
			if err == nil {
				t.Fatal("Until succeeded against a probe that never answers")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not contain %q", err, want)
				}
			}
			if took < 30*time.Millisecond {
				t.Fatalf("gave up after %s, before the 30ms deadline", took)
			}
			if strings.Contains(log.joined(), "became ready") {
				t.Fatalf("a failed wait logged a ready line:\n%s", log.joined())
			}
		})
	}
}

func TestUntil_AnExitedProcessEndsTheWaitAtOnce(t *testing.T) {
	t.Run("already exited", func(t *testing.T) {
		exited := make(chan struct{})
		close(exited)
		// An hour of interval and deadline: only the exit can end this wait.
		w := bootwait.Wait{What: "ork", Exited: exited, Interval: time.Hour, Deadline: time.Hour}
		var (
			err    error
			probes int
		)
		returnsWithin(t, 10*time.Second, func() {
			_, err = w.Until(context.Background(), func(context.Context) error {
				probes++
				return errors.New("connection refused")
			})
		})
		if err == nil {
			t.Fatal("Until succeeded for a process that exited")
		}
		for _, want := range []string{"ork exited before it became ready", "last probe: connection refused"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not contain %q", err, want)
			}
		}
		if probes != 1 {
			t.Fatalf("probes = %d, want 1: a wait whose process is gone probes once for its error and stops", probes)
		}
	})

	t.Run("exits while the probe runs", func(t *testing.T) {
		exited := make(chan struct{})
		var probes atomic.Int32
		w := bootwait.Wait{What: "ork", Exited: exited, Interval: time.Millisecond, Deadline: time.Hour}
		var err error
		returnsWithin(t, 10*time.Second, func() {
			_, err = w.Until(context.Background(), func(context.Context) error {
				// The process dies during the first probe and never answers
				// again: a second probe would only wait out an interval for
				// nothing.
				if probes.Add(1) == 1 {
					close(exited)
				}
				return errors.New("connection reset")
			})
		})
		if err == nil || !strings.Contains(err.Error(), "ork exited before it became ready") {
			t.Fatalf("Until = %v, want the exit reported", err)
		}
		if got := probes.Load(); got != 1 {
			t.Fatalf("probes = %d, want 1: a closed Exited is not probed again", got)
		}
	})

	t.Run("an answer wins over an exit seen in the same instant", func(t *testing.T) {
		exited := make(chan struct{})
		close(exited)
		w := bootwait.Wait{What: "ork", Exited: exited}
		if _, err := w.Until(context.Background(), func(context.Context) error { return nil }); err != nil {
			t.Fatalf("a probe that answered must pass: %v", err)
		}
	})
}

func TestUntil_ACancelledContextEndsTheWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := bootwait.Wait{What: "ork", Interval: time.Hour, Deadline: time.Hour}
	var err error
	returnsWithin(t, 10*time.Second, func() {
		_, err = w.Until(ctx, func(context.Context) error {
			cancel()
			return errors.New("connection refused")
		})
	})
	if err == nil {
		t.Fatal("Until succeeded after its context was cancelled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %q does not wrap context.Canceled", err)
	}
	if !strings.Contains(err.Error(), "ork") {
		t.Fatalf("error %q does not name what it waited for", err)
	}
	if strings.Contains(err.Error(), "did not become ready within") {
		t.Fatalf("a cancelled wait was reported as a deadline: %q", err)
	}
}

func TestUntil_ANilLogfIsSilent(t *testing.T) {
	w := bootwait.Wait{What: "ork", WarnAfter: time.Nanosecond}
	if _, err := w.Until(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Until: %v", err)
	}
}
