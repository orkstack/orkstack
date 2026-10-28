package orker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// probeBudget is the per-attempt budget a scenario gives a bind probe that is
// meant to overrun it. It is long enough that a stand-in `docker run` which
// exits at once finishes inside it on a host with four times as many busy
// processes as CPUs, and short enough that a scenario with two overruns costs
// about two seconds.
const probeBudget = time.Second

// runs reports how many `docker run` invocations the stand-in has counted.
func (c *countingDocker) runs() int {
	c.t.Helper()
	raw, err := os.ReadFile(filepath.Join(c.dir, "runs"))
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		c.t.Fatal(err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		c.t.Fatalf("the stand-in's run counter %q: %v", raw, err)
	}
	return n
}

// waitForFile returns once path exists, and fails the test at a deadline.
func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not appear within 30 s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// probeContainerNames returns the --name of every `docker run` in calls, in
// order, and the container every `rm --force` removed.
func probeContainerNames(t *testing.T, calls []string) (ran, removed []string) {
	t.Helper()
	for _, call := range calls {
		fields := strings.Fields(call)
		switch {
		case len(fields) > 0 && fields[0] == "run":
			for i, field := range fields {
				if field == "--name" && i+1 < len(fields) {
					ran = append(ran, fields[i+1])
				}
			}
		case strings.HasPrefix(call, "rm --force"):
			removed = append(removed, fields[len(fields)-1])
		}
	}
	return ran, removed
}

// TestDockerRuntime_ProbeBindableClassifiesTheRun is the classification of the
// boot bind probe, one row per way the probe's `docker run` can end:
//
//   - the container ran and exited non-zero: the work root cannot be bound,
//     a fact a second attempt cannot change, so it is ErrNotBindable and the
//     probe is not run again;
//   - the daemon did not start the container within the probe's own budget: a
//     slow daemon says nothing about the work root, so the probe is tried once
//     more, and an Orker that still cannot start it is told the daemon was
//     slow (ErrProbeTimedOut), never that the path is unbindable.
func TestDockerRuntime_ProbeBindableClassifiesTheRun(t *testing.T) {
	tests := []struct {
		name     string
		flags    map[string]string
		budget   time.Duration
		wantErr  error // nil: the probe passes
		wantRuns int
		// wantRemoved is how many probe containers were force-removed: one
		// per attempt that overran, so an attempt the daemon is still
		// starting cannot run beside the retry.
		wantRemoved int
	}{
		{name: "the container starts and exits zero", budget: time.Minute, wantRuns: 1},
		{
			name: "the container ran and exited non-zero", flags: map[string]string{"runfail": ""},
			budget: time.Minute, wantErr: ErrNotBindable, wantRuns: 1,
		},
		{
			name: "one overrun and the retry passes", flags: map[string]string{"slowruns": "1"},
			budget: probeBudget, wantRuns: 2, wantRemoved: 1,
		},
		{
			name: "two overruns, and never a third attempt", flags: map[string]string{"slowruns": "9"},
			budget: probeBudget, wantErr: ErrProbeTimedOut, wantRuns: 2, wantRemoved: 2,
		},
	}
	sentinels := map[string]error{
		"ErrNotBindable":           ErrNotBindable,
		"ErrProbeImageUnavailable": ErrProbeImageUnavailable,
		"ErrProbeTimedOut":         ErrProbeTimedOut,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set("present", "")
			for flag, content := range tt.flags {
				fake.set(flag, content)
			}
			runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: tt.budget}

			err := runtime.ProbeBindable(context.Background(), t.TempDir())

			if tt.wantErr == nil && err != nil {
				t.Fatalf("ProbeBindable = %v; want nil", err)
			}
			if tt.wantErr != nil && err == nil {
				t.Fatalf("ProbeBindable = nil; want %v", tt.wantErr)
			}
			for name, sentinel := range sentinels {
				if got, want := errors.Is(err, sentinel), sentinel == tt.wantErr; got != want {
					t.Errorf("errors.Is(%v, %s) = %t; want %t", err, name, got, want)
				}
			}
			if got := fake.runs(); got != tt.wantRuns {
				t.Errorf("docker run was invoked %d times; want %d", got, tt.wantRuns)
			}
			ran, removed := probeContainerNames(t, fake.calls())
			if len(removed) != tt.wantRemoved {
				t.Errorf("removed %q after the run(s) of %q; want %d removals", removed, ran, tt.wantRemoved)
			}
			for i, name := range removed {
				if name != ran[i] {
					t.Errorf("removal #%d was of %q; want the container the overrun attempt named, %q", i, name, ran[i])
				}
			}
			if len(ran) == 2 && ran[0] == ran[1] {
				t.Errorf("both attempts named their container %q; a retry beside a still-starting first attempt would collide", ran[0])
			}
		})
	}
}

// TestDockerRuntime_ProbeBindableTimeoutNamesTheDaemonAndTheSetting is what an
// operator reads when the daemon stays slow: it says the daemon did not start
// the probe container within the budget, names the setting that raises it, and
// does not call the work root unbindable.
func TestDockerRuntime_ProbeBindableTimeoutNamesTheDaemonAndTheSetting(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("slowruns", "9")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: probeBudget}
	root := t.TempDir()

	err := runtime.ProbeBindable(context.Background(), root)
	if !errors.Is(err, ErrProbeTimedOut) {
		t.Fatalf("ProbeBindable = %v; want ErrProbeTimedOut", err)
	}
	msg := err.Error()
	for _, want := range []string{probeBudget.String(), BindProbeTimeoutVar, "daemon", "probe container", "2 attempts"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal %q does not contain %q", msg, want)
		}
	}
	if strings.Contains(msg, ErrNotBindable.Error()) || strings.Contains(msg, "bind-mountable") {
		t.Errorf("the refusal %q calls the work root unbindable; the daemon was slow", msg)
	}
}

// TestDockerRuntime_ProbeBindableStopsWhenTheContextEnds: an Orker told to
// stop while its probe runs is neither slow-daemon nor unbindable. It returns
// the context's own error at once, runs the probe no second time, and a
// stopped Orker is not left waiting out the probe's budget.
func TestDockerRuntime_ProbeBindableStopsWhenTheContextEnds(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("slowruns", "9")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: time.Hour}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.ProbeBindable(ctx, t.TempDir()) }()
	waitForFile(t, filepath.Join(fake.dir, "runstarted"))
	cancel()

	var err error
	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("ProbeBindable did not return within 30 s of its context being canceled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ProbeBindable = %v; want the context's own error", err)
	}
	for name, sentinel := range map[string]error{
		"ErrNotBindable": ErrNotBindable, "ErrProbeImageUnavailable": ErrProbeImageUnavailable, "ErrProbeTimedOut": ErrProbeTimedOut,
	} {
		if errors.Is(err, sentinel) {
			t.Errorf("a canceled probe is classified %s: %v", name, err)
		}
	}
	if got := fake.runs(); got != 1 {
		t.Errorf("docker run was invoked %d times; a canceled probe is not retried", got)
	}
}

// TestBoot_NamesTheBindProbeOutcome: Boot turns the probe's classification
// into the boot reason an operator reads in `orker boot checks failed:`.
func TestBoot_NamesTheBindProbeOutcome(t *testing.T) {
	tests := []struct {
		name       string
		flags      map[string]string
		budget     time.Duration
		wantReason string // "" boots
		notReason  string
	}{
		{name: "a slow start that the retry gets through boots", flags: map[string]string{"slowruns": "1"}, budget: probeBudget},
		{
			name: "a container that ran and failed", flags: map[string]string{"runfail": ""}, budget: time.Minute,
			wantReason: ReasonWorkdirNotBindable, notReason: ReasonBindProbeTimedOut,
		},
		{
			name: "a daemon that stays slow", flags: map[string]string{"slowruns": "9"}, budget: probeBudget,
			wantReason: ReasonBindProbeTimedOut, notReason: ReasonWorkdirNotBindable,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set("present", "")
			for flag, content := range tt.flags {
				fake.set(flag, content)
			}
			activities := NewActivitiesWithOptions(nil, Options{
				OrkerID:  fmt.Sprintf("orker-probe-boot-%d-%d", os.Getpid(), i),
				WorkRoot: t.TempDir(),
				Runtime:  &DockerRuntime{Bin: fake.bin, BindProbeTimeout: tt.budget},
			})
			t.Cleanup(activities.Close)

			err := activities.Boot(context.Background())

			if tt.wantReason == "" {
				if err != nil {
					t.Fatalf("Boot = %v; want it to boot", err)
				}
				return
			}
			if err == nil || !strings.HasPrefix(err.Error(), tt.wantReason+":") {
				t.Fatalf("Boot = %v; want a refusal starting %s:", err, tt.wantReason)
			}
			if strings.Contains(err.Error(), tt.notReason) {
				t.Errorf("Boot = %v; it must not name %s", err, tt.notReason)
			}
		})
	}
}

// TestBoot_ACanceledBindProbeNamesNeitherBindReason: an Orker stopped during
// its boot probe says it was stopped, not that its work root is unbindable.
func TestBoot_ACanceledBindProbeNamesNeitherBindReason(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("slowruns", "9")
	activities := NewActivitiesWithOptions(nil, Options{
		OrkerID:  fmt.Sprintf("orker-probe-cancel-%d", os.Getpid()),
		WorkRoot: t.TempDir(),
		Runtime:  &DockerRuntime{Bin: fake.bin, BindProbeTimeout: time.Hour},
	})
	t.Cleanup(activities.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- activities.Boot(ctx) }()
	waitForFile(t, filepath.Join(fake.dir, "runstarted"))
	cancel()

	var err error
	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Boot did not return within 30 s of its context being canceled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Boot = %v; want the context's own error", err)
	}
	for _, reason := range []string{ReasonWorkdirNotBindable, ReasonBindProbeTimedOut, ReasonBindProbeImageUnavailable} {
		if strings.Contains(err.Error(), reason) {
			t.Errorf("Boot = %v; a stop is not %s", err, reason)
		}
	}
}

// TestDockerRuntime_BindProbeTimeoutPrecedence: the probe's budget is the
// field, then ORK_DOCKER_BIND_PROBE_TIMEOUT, then the default, and a value
// that is not a positive duration is ignored rather than read as no budget.
func TestDockerRuntime_BindProbeTimeoutPrecedence(t *testing.T) {
	if DefaultBindProbeTimeout <= controlTimeout {
		t.Fatalf("DefaultBindProbeTimeout = %s must exceed the %s control timeout the probe used to run under, "+
			"or the probe still misnames the daemon the control timeout misnamed", DefaultBindProbeTimeout, controlTimeout)
	}
	tests := []struct {
		name  string
		field time.Duration
		env   string
		want  time.Duration
	}{
		{"nothing set", 0, "", DefaultBindProbeTimeout},
		{"the environment", 0, "45s", 45 * time.Second},
		{"the field over the environment", 3 * time.Second, "45s", 3 * time.Second},
		{"not a duration", 0, "soon", DefaultBindProbeTimeout},
		{"zero", 0, "0s", DefaultBindProbeTimeout},
		{"negative", 0, "-5s", DefaultBindProbeTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(BindProbeTimeoutVar, tt.env)
			if got := (&DockerRuntime{BindProbeTimeout: tt.field}).bindProbeTimeout(); got != tt.want {
				t.Fatalf("bindProbeTimeout = %s; want %s", got, tt.want)
			}
		})
	}
}
