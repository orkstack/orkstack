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
//   - it ends within the budget with a failure that is not about time (the
//     container exited non-zero, or the daemon refused the bind in its own
//     words, or refused it for another reason): that is the daemon's answer
//     about the work root, a fact a second attempt cannot change, so it is
//     ErrNotBindable and the probe is not run again;
//   - it does not end within the probe's own budget, or ends sooner with the
//     daemon's own deadline error: a slow daemon says nothing about the work
//     root, so the probe is tried once more, and an Orker that still cannot
//     start it is told the daemon was slow (ErrProbeTimedOut), never that the
//     path is unbindable. The words of a deadline are looked for in what the
//     daemon said, never in the path it quoted.
func TestDockerRuntime_ProbeBindableClassifiesTheRun(t *testing.T) {
	tests := []struct {
		name     string
		flags    map[string]string
		budget   time.Duration
		rootName string // the work root's directory name; "" is a plain temporary one
		wantErr  error  // nil: the probe passes
		wantRuns int
		// wantRemoved is how many probe containers were force-removed: one
		// per attempt that overran, so an attempt the daemon is still
		// starting cannot run beside the retry.
		wantRemoved int
	}{
		{name: "the container starts and exits zero", budget: time.Minute, wantRuns: 1},
		{
			name: "the container ran and exited non-zero", flags: map[string]string{"runexit": ""},
			budget: time.Minute, wantErr: ErrNotBindable, wantRuns: 1,
		},
		{
			name: "the daemon refuses the bind in its own words", flags: map[string]string{"runfail": ""},
			budget: time.Minute, wantErr: ErrNotBindable, wantRuns: 1,
		},
		{
			name: "the daemon refuses the run for another reason", flags: map[string]string{"runother": ""},
			budget: time.Minute, wantErr: ErrNotBindable, wantRuns: 1,
		},
		{
			name:  "a refusal that quotes a path with timeout in its name is still the work root's",
			flags: map[string]string{"runfailsrc": ""}, budget: time.Minute, rootName: "timeout-work-root",
			wantErr: ErrNotBindable, wantRuns: 1,
		},
		{
			name:  "the daemon answers with its own deadline error once and the retry passes",
			flags: map[string]string{"timeoutruns": "1"}, budget: time.Minute, wantRuns: 2, wantRemoved: 1,
		},
		{
			name:  "the daemon answers with its own deadline error twice, and never a third attempt",
			flags: map[string]string{"timeoutruns": "9"}, budget: time.Minute,
			wantErr: ErrProbeTimedOut, wantRuns: 2, wantRemoved: 2,
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

			root := t.TempDir()
			if tt.rootName != "" {
				root = filepath.Join(root, tt.rootName)
				if err := os.Mkdir(root, 0o755); err != nil {
					t.Fatal(err)
				}
			}

			err := runtime.ProbeBindable(context.Background(), root)

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

// TestDockerRuntime_ProbeBindableReportsAContainerItCouldNotRemove: the
// force-removal of an attempt that overran is best effort, but a removal the
// daemon refuses is a fact an operator wants beside the refusal, not one
// thrown away: the leftover container is the thing to look for.
func TestDockerRuntime_ProbeBindableReportsAContainerItCouldNotRemove(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("slowruns", "9")
	fake.set("rmfailall", "")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: probeBudget}

	err := runtime.ProbeBindable(context.Background(), t.TempDir())

	if !errors.Is(err, ErrProbeTimedOut) {
		t.Fatalf("ProbeBindable = %v; want ErrProbeTimedOut", err)
	}
	ran, _ := probeContainerNames(t, fake.calls())
	for _, want := range append([]string{"could not be removed", "already in progress"}, ran...) {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not contain %q", err, want)
		}
	}
	if got := fake.runs(); got != 2 {
		t.Errorf("docker run was invoked %d times; a removal that failed does not end the probe's retry", got)
	}
}

// TestDockerRuntime_ProbeBindableTimeoutQuotesTheDaemonsDeadlineError: an
// attempt the daemon ended with its own deadline error did not overrun the
// budget, so the refusal quotes what the daemon said as well as the budget.
func TestDockerRuntime_ProbeBindableTimeoutQuotesTheDaemonsDeadlineError(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("timeoutruns", "9")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: time.Minute}

	err := runtime.ProbeBindable(context.Background(), t.TempDir())

	if !errors.Is(err, ErrProbeTimedOut) {
		t.Fatalf("ProbeBindable = %v; want ErrProbeTimedOut", err)
	}
	for _, want := range []string{"context deadline exceeded", BindProbeTimeoutVar, "2 attempts"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not contain %q", err, want)
		}
	}
}

// TestDockerRuntime_ProbeBindableSlowImageInspectIsTheDaemonsNotTheImages: the
// probe asks the daemon whether its image is local before it runs it. A daemon
// that does not answer that within the probe's budget is slow; reading the
// silence as "the image is absent" would send a daemon that is merely slow to
// a registry, and on a host with no registry to a refusal that blames the image.
func TestDockerRuntime_ProbeBindableSlowImageInspectIsTheDaemonsNotTheImages(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("inspecthang", "")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: probeBudget}

	err := runtime.ProbeBindable(context.Background(), t.TempDir())

	if !errors.Is(err, ErrProbeTimedOut) {
		t.Fatalf("ProbeBindable = %v; want ErrProbeTimedOut", err)
	}
	for name, sentinel := range map[string]error{"ErrNotBindable": ErrNotBindable, "ErrProbeImageUnavailable": ErrProbeImageUnavailable} {
		if errors.Is(err, sentinel) {
			t.Errorf("a daemon that did not answer image inspect is classified %s: %v", name, err)
		}
	}
	for _, want := range []string{"image inspect", probeBudget.String(), BindProbeTimeoutVar, "daemon"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not contain %q", err, want)
		}
	}
	calls := fake.calls()
	if n := countPrefix(calls, "pull") + countPrefix(calls, "run"); n != 0 {
		t.Errorf("calls = %q; a daemon that did not answer image inspect is neither asked to pull nor to run", calls)
	}
}

// TestDockerRuntime_ProbeBindableStopsWhenTheContextEndsDuringTheInspect: an
// Orker told to stop while the daemon is silent on image inspect returns the
// context's own error, not an image or a slow-daemon finding.
func TestDockerRuntime_ProbeBindableStopsWhenTheContextEndsDuringTheInspect(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("inspecthang", "")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: time.Hour}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.ProbeBindable(ctx, t.TempDir()) }()
	waitForFile(t, filepath.Join(fake.dir, "calls"))
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
			name: "a daemon that refuses the bind", flags: map[string]string{"runfail": ""}, budget: time.Minute,
			wantReason: ReasonWorkdirNotBindable, notReason: ReasonBindProbeTimedOut,
		},
		{
			name: "a daemon whose own deadline error repeats", flags: map[string]string{"timeoutruns": "9"}, budget: time.Minute,
			wantReason: ReasonBindProbeTimedOut, notReason: ReasonWorkdirNotBindable,
		},
		{
			name: "a daemon that does not answer the image inspect", flags: map[string]string{"inspecthang": ""}, budget: probeBudget,
			wantReason: ReasonBindProbeTimedOut, notReason: ReasonBindProbeImageUnavailable,
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
