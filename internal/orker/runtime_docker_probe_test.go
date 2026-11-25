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

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// probeBudget is the per-attempt budget a scenario states for a bind probe that
// overruns it. Where it runs under countingDocker.slow it is only what the
// refusal quotes: an attempt ends when the stand-in holds it, and one the
// stand-in answers has no budget to beat, however slow the host.
const probeBudget = time.Second

// runs reports how many `docker run` invocations the stand-in has counted.
func (c *countingDocker) runs() int {
	c.t.Helper()
	raw, err := os.ReadFile(filepath.Join(c.dir, "runs"))
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	must.OK(c.t, err)
	n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	must.OKf(c.t, err, "the stand-in's run counter %q", raw)
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

// probeSpecUnder is the boot probe's own container spec under root, built by
// the dispatcher exactly as Boot builds it, with the Orker's nonce written
// into its input as Boot writes it.
func probeSpecUnder(t *testing.T, root string) ContainerSpec {
	t.Helper()
	ws, err := NewWorkspace(filepath.Join(root, "ork-bindprobe-test"))
	must.OK(t, err)
	mustWrite(t, filepath.Join(ws.Input, bootProbeNonceFile), "host-nonce")
	d := &OCIDispatcher{Policy: DefaultSecurityPolicy()}
	spec, err := d.bootProbeSpec(ws, "container-nonce")
	must.OKf(t, err, "bootProbeSpec")
	return spec
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

// TestDockerRuntime_RunBootProbeClassifiesTheRun is the classification of the
// boot probe's run, one row per way the probe's `docker run` can end:
//
//   - it ends within the budget with a failure that is not about time: that
//     is the daemon's answer, a fact a second attempt cannot change, so the
//     probe is not run again. A refusal of the bind in the daemon's own words
//     is ErrNotBindable; any other refusal, and a container that exited
//     non-zero (the probe's script always exits 0), is the runtime refusing a
//     task's container, ErrPostureRefused;
//   - it does not end within the probe's own budget, or ends sooner with the
//     daemon's own deadline error: a slow daemon says nothing about the work
//     root, so the probe is tried once more, and an Orker that still cannot
//     start it is told the daemon was slow (ErrProbeTimedOut), never that the
//     path is unbindable. The words of a deadline are looked for in what the
//     daemon said, never in the path it quoted.
func TestDockerRuntime_RunBootProbeClassifiesTheRun(t *testing.T) {
	tests := []struct {
		name     string
		flags    map[string]string
		budget   time.Duration
		rootName string // the work root's directory name; "" is a plain temporary one
		wantErr  error  // nil: the probe passes
		wantRuns int
		// wantRemoved is how many probe containers the runtime
		// force-removed: one per attempt that did not hand its container
		// over, so an attempt the daemon is still starting cannot run
		// beside the retry, and one the daemon refused or cut off is not
		// left created and never started. A container that ran to its end
		// is handed to the caller, who removes it.
		wantRemoved int
		// wantLeftovers is how many of those removals the daemon refused on
		// a run that still passed: each is handed back with the run, naming
		// the attempt's container, for the caller to report.
		wantLeftovers int
	}{
		{name: "the container starts and exits zero", budget: time.Minute, wantRuns: 1},
		{
			name: "the container ran and exited non-zero", flags: map[string]string{"runexit": ""},
			budget: time.Minute, wantErr: ErrPostureRefused, wantRuns: 1, wantRemoved: 1,
		},
		{
			name: "the daemon refuses the bind in its own words", flags: map[string]string{"runfail": ""},
			budget: time.Minute, wantErr: ErrNotBindable, wantRuns: 1, wantRemoved: 1,
		},
		{
			name: "the daemon refuses the run for another reason", flags: map[string]string{"runother": ""},
			budget: time.Minute, wantErr: ErrPostureRefused, wantRuns: 1, wantRemoved: 1,
		},
		{
			name:  "a refusal that quotes a path with timeout in its name is still the work root's",
			flags: map[string]string{"runfailsrc": ""}, budget: time.Minute, rootName: "timeout-work-root",
			wantErr: ErrNotBindable, wantRuns: 1, wantRemoved: 1,
		},
		{
			name:  "the daemon answers with its own deadline error once and the retry passes",
			flags: map[string]string{"timeoutruns": "1"}, budget: time.Minute, wantRuns: 2, wantRemoved: 1,
		},
		{
			name:  "the daemon answers with its own deadline error twice and never a third attempt",
			flags: map[string]string{"timeoutruns": "9"}, budget: time.Minute,
			wantErr: ErrProbeTimedOut, wantRuns: 2, wantRemoved: 2,
		},
		{
			name: "one overrun and the retry passes", flags: map[string]string{"slowruns": "1"},
			budget: probeBudget, wantRuns: 2, wantRemoved: 1,
		},
		{
			name:  "one overrun whose container the daemon will not remove, and the retry passes",
			flags: map[string]string{"slowruns": "1", "rmfailall": ""}, budget: probeBudget,
			wantRuns: 2, wantRemoved: 1, wantLeftovers: 1,
		},
		{
			name: "two overruns and never a third attempt", flags: map[string]string{"slowruns": "9"},
			budget: probeBudget, wantErr: ErrProbeTimedOut, wantRuns: 2, wantRemoved: 2,
		},
	}
	sentinels := map[string]error{
		"ErrNotBindable":           ErrNotBindable,
		"ErrProbeImageUnavailable": ErrProbeImageUnavailable,
		"ErrProbeTimedOut":         ErrProbeTimedOut,
		"ErrPostureRefused":        ErrPostureRefused,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set("present", "")
			for flag, content := range tt.flags {
				fake.set(flag, content)
			}
			runtime := fake.slow(&DockerRuntime{Bin: fake.bin, BindProbeTimeout: tt.budget})

			root := t.TempDir()
			if tt.rootName != "" {
				root = filepath.Join(root, tt.rootName)
				must.OK(t, os.Mkdir(root, 0o755))
			}

			run, err := runtime.RunBootProbe(context.Background(), probeSpecUnder(t, root))

			if tt.wantErr == nil && err != nil {
				t.Fatalf("RunBootProbe = %v; want nil", err)
			}
			if tt.wantErr != nil && err == nil {
				t.Fatalf("RunBootProbe = nil; want %v", tt.wantErr)
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
					t.Errorf("removal #%d was of %q; want the container that attempt named, %q", i, name, ran[i])
				}
			}
			if len(run.Leftovers) != tt.wantLeftovers {
				t.Errorf("the run handed back leftovers %+v; want %d", run.Leftovers, tt.wantLeftovers)
			}
			for i, left := range run.Leftovers {
				if left.Container != ContainerID(ran[i]) || left.Err == nil {
					t.Errorf("leftover #%d is %+v; want attempt %d's container %q with the daemon's refusal", i, left, i+1, ran[i])
				}
			}
			if tt.wantErr == nil && (len(ran) == 0 || run.Container != ContainerID(ran[len(ran)-1])) {
				t.Errorf("the run handed back %q; want the passing attempt's container, the last of %q", run.Container, ran)
			}
			if len(ran) == 2 && ran[0] == ran[1] {
				t.Errorf("both attempts named their container %q; a retry beside a still-starting first attempt would collide", ran[0])
			}
		})
	}
}

// TestDockerRuntime_RunBootProbeTimeoutNamesTheDaemonAndTheSetting is what an
// operator reads when the daemon stays slow: it says the daemon did not start
// the probe container within the budget, names the setting that raises it, and
// does not call the work root unbindable.
func TestDockerRuntime_RunBootProbeTimeoutNamesTheDaemonAndTheSetting(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("slowruns", "9")
	runtime := fake.slow(&DockerRuntime{Bin: fake.bin, BindProbeTimeout: probeBudget})
	root := t.TempDir()

	_, err := runtime.RunBootProbe(context.Background(), probeSpecUnder(t, root))
	if !errors.Is(err, ErrProbeTimedOut) {
		t.Fatalf("RunBootProbe = %v; want ErrProbeTimedOut", err)
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

// TestDockerRuntime_RunBootProbeReportsAContainerItCouldNotRemove: the
// force-removal of an attempt that overran is best effort, but a removal the
// daemon refuses is a fact an operator wants beside the refusal, not one
// thrown away: the leftover container is the thing to look for.
func TestDockerRuntime_RunBootProbeReportsAContainerItCouldNotRemove(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("slowruns", "9")
	fake.set("rmfailall", "")
	runtime := fake.slow(&DockerRuntime{Bin: fake.bin, BindProbeTimeout: probeBudget})

	_, err := runtime.RunBootProbe(context.Background(), probeSpecUnder(t, t.TempDir()))

	if !errors.Is(err, ErrProbeTimedOut) {
		t.Fatalf("RunBootProbe = %v; want ErrProbeTimedOut", err)
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

// TestDockerRuntime_RunBootProbeTimeoutQuotesTheDaemonsDeadlineError: an
// attempt the daemon ended with its own deadline error did not overrun the
// budget, so the refusal quotes what the daemon said as well as the budget.
func TestDockerRuntime_RunBootProbeTimeoutQuotesTheDaemonsDeadlineError(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("timeoutruns", "9")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: time.Minute}

	_, err := runtime.RunBootProbe(context.Background(), probeSpecUnder(t, t.TempDir()))

	if !errors.Is(err, ErrProbeTimedOut) {
		t.Fatalf("RunBootProbe = %v; want ErrProbeTimedOut", err)
	}
	for _, want := range []string{"context deadline exceeded", BindProbeTimeoutVar, "2 attempts"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not contain %q", err, want)
		}
	}
}

// TestDockerRuntime_RunBootProbeSlowImageInspectIsTheDaemonsNotTheImages: the
// probe asks the daemon whether its image is local before it runs it. A daemon
// that does not answer that within the probe's budget is slow; reading the
// silence as "the image is absent" would send a daemon that is merely slow to
// a registry, and on a host with no registry to a refusal that blames the image.
func TestDockerRuntime_RunBootProbeSlowImageInspectIsTheDaemonsNotTheImages(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("inspecthang", "")
	runtime := fake.slow(&DockerRuntime{Bin: fake.bin, BindProbeTimeout: probeBudget})

	_, err := runtime.RunBootProbe(context.Background(), probeSpecUnder(t, t.TempDir()))

	if !errors.Is(err, ErrProbeTimedOut) {
		t.Fatalf("RunBootProbe = %v; want ErrProbeTimedOut", err)
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

// TestDockerRuntime_RunBootProbeSlowEngineIsTheDaemonsNotThePostures: the
// probe first asks the engine which it is and whether it is rootless, inside
// its own budget. An engine that does not answer within it is a slow daemon,
// refused as one, never as a runtime that weakens a task's posture nor as a
// work root it cannot bind, and nothing is pulled or run.
func TestDockerRuntime_RunBootProbeSlowEngineIsTheDaemonsNotThePostures(t *testing.T) {
	for _, tt := range []struct{ name, cli, flag string }{
		{name: "docker version", cli: CLIDocker, flag: "versionhang"},
		{name: "docker info", cli: CLIDocker, flag: "infohang"},
		{name: "podman info", cli: CLIPodman, flag: "infohang"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set("present", "")
			fake.set(tt.flag, "")
			runtime := fake.slow(&DockerRuntime{Bin: fake.bin, CLI: tt.cli, BindProbeTimeout: probeBudget})

			_, err := runtime.RunBootProbe(context.Background(), probeSpecUnder(t, t.TempDir()))

			if !errors.Is(err, ErrProbeTimedOut) {
				t.Fatalf("RunBootProbe = %v; want ErrProbeTimedOut", err)
			}
			for name, sentinel := range map[string]error{"ErrPostureRefused": ErrPostureRefused, "ErrNotBindable": ErrNotBindable} {
				if errors.Is(err, sentinel) {
					t.Errorf("an engine that did not answer in time is classified %s: %v", name, err)
				}
			}
			for _, want := range []string{"which engine", probeBudget.String(), BindProbeTimeoutVar, "daemon"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal %q does not contain %q", err, want)
				}
			}
			calls := fake.calls()
			if n := countPrefix(calls, "pull") + countPrefix(calls, "run"); n != 0 {
				t.Errorf("calls = %q; an engine that did not say what it is is neither asked to pull nor to run", calls)
			}
		})
	}
}

// TestDockerRuntime_RunBootProbeStopsWhenTheContextEndsDuringTheInspect: an
// Orker told to stop while the daemon is silent on image inspect returns the
// context's own error, not an image or a slow-daemon finding.
func TestDockerRuntime_RunBootProbeStopsWhenTheContextEndsDuringTheInspect(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("inspecthang", "")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: time.Hour}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	spec := probeSpecUnder(t, t.TempDir())
	go func() { _, err := runtime.RunBootProbe(ctx, spec); done <- err }()
	waitForFile(t, filepath.Join(fake.dir, "calls"))
	cancel()

	var err error
	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("RunBootProbe did not return within 30 s of its context being canceled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBootProbe = %v; want the context's own error", err)
	}
	for name, sentinel := range map[string]error{
		"ErrNotBindable": ErrNotBindable, "ErrProbeImageUnavailable": ErrProbeImageUnavailable, "ErrProbeTimedOut": ErrProbeTimedOut,
	} {
		if errors.Is(err, sentinel) {
			t.Errorf("a canceled probe is classified %s: %v", name, err)
		}
	}
}

// TestDockerRuntime_RunBootProbeStopsWhenTheContextEnds: an Orker told to
// stop while its probe runs is neither slow-daemon nor unbindable. It returns
// the context's own error at once, runs the probe no second time, and a
// stopped Orker is not left waiting out the probe's budget.
func TestDockerRuntime_RunBootProbeStopsWhenTheContextEnds(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("slowruns", "9")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: time.Hour}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	spec := probeSpecUnder(t, t.TempDir())
	go func() { _, err := runtime.RunBootProbe(ctx, spec); done <- err }()
	waitForFile(t, filepath.Join(fake.dir, "runstarted"))
	cancel()

	var err error
	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("RunBootProbe did not return within 30 s of its context being canceled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBootProbe = %v; want the context's own error", err)
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
		{
			name: "a daemon that refuses a task's container for another reason", flags: map[string]string{"runother": ""},
			budget: time.Minute, wantReason: ReasonRuntimePostureRefused, notReason: ReasonWorkdirNotBindable,
		},
		{
			name:   "a container that runs with a weaker posture than a task is owed",
			flags:  map[string]string{"facts": strings.Replace(GoodBootProbeFacts(SecurityPolicyFromEnv()), "Seccomp:\t2", "Seccomp:\t0", 1)},
			budget: time.Minute, wantReason: ReasonRuntimePostureRefused, notReason: ReasonWorkdirNotBindable,
		},
		{
			// G24: Podman's Docker-compatible socket creates a missing bind
			// source instead of refusing it, so a container can start on a
			// directory the Orker never sees.
			name: "a container that sees another directory than the Orker's", flags: map[string]string{"wrongdir": ""},
			budget: time.Minute, wantReason: ReasonWorkdirNotBindable, notReason: ReasonRuntimePostureRefused,
		},
		{
			name: "a container whose writes never reach the Orker", flags: map[string]string{"nowrite": ""},
			budget: time.Minute, wantReason: ReasonWorkdirNotBindable, notReason: ReasonRuntimePostureRefused,
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
				Runtime:  fake.slow(&DockerRuntime{Bin: fake.bin, BindProbeTimeout: tt.budget}),
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

// TestDockerRuntime_RunBootProbeFailedSaysWhatTheContainerSaid: a run that
// ended without success hands back what its container wrote and what could
// not be removed, so a caller can tell one that started from one that never
// did.
func TestDockerRuntime_RunBootProbeFailedSaysWhatTheContainerSaid(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("runexit", "")
	fake.set("runsays", "orker probe unit: started")
	fake.set("rmfailall", "")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: time.Minute}
	run, err := runtime.RunBootProbe(t.Context(), probeSpecUnder(t, t.TempDir()))
	if !errors.Is(err, ErrPostureRefused) || errors.Is(err, errEngineUnreachable) {
		t.Fatalf("RunBootProbe = %v, want ErrPostureRefused from an engine that answered", err)
	}
	if !strings.HasPrefix(run.Stdout, unitProbeStarted) || len(run.Leftovers) != 1 {
		t.Fatalf("run = %+v; want the container's words and the one it could not remove", run)
	}
}

// TestDockerRuntime_RunBootProbeUnreachableEngine: the CLI's own word that no
// engine answered is told from a refusal the engine gave.
func TestDockerRuntime_RunBootProbeUnreachableEngine(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("rununreachable", "")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: time.Minute}
	if _, err := runtime.RunBootProbe(t.Context(), probeSpecUnder(t, t.TempDir())); !errors.Is(err, errEngineUnreachable) || !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("RunBootProbe = %v, want ErrPostureRefused and errEngineUnreachable", err)
	}
}

// TestDockerRuntime_AnImageNobodyAnsweredForIsNotAbsent: a pull or a label
// read the engine gave no answer to is told from an image it says it has not
// got, by its own word or by its silence past the bound; what the caller sees
// of a pull that failed is otherwise what it was.
func TestDockerRuntime_AnImageNobodyAnsweredForIsNotAbsent(t *testing.T) {
	const image = "ghcr.io/orkstack/orker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for name, tc := range map[string]struct {
		flag        string
		unreachable bool
	}{"no daemon": {"imagesunreachable", true}, "the registry has none": {"pullfail", false}} {
		t.Run(name, func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set(tc.flag, "")
			runtime := &DockerRuntime{Bin: fake.bin}
			if _, err := runtime.Pull(t.Context(), image); err == nil || errors.Is(err, errEngineUnreachable) != tc.unreachable {
				t.Fatalf("Pull = %v; want an error, the engine's silence %v", err, tc.unreachable)
			}
			if _, _, err := runtime.ImageLabel(t.Context(), image, LabelBuild); err == nil || errors.Is(err, errEngineUnreachable) != tc.unreachable {
				t.Fatalf("ImageLabel = %v; want an error, the engine's silence %v", err, tc.unreachable)
			}
		})
	}
	fake := newCountingDocker(t)
	fake.set("inspecthang", "")
	runtime := &DockerRuntime{Bin: fake.bin}
	if _, present, silent, err := runtime.lookupImage(t.Context(), 50*time.Millisecond, image); present || !silent || err != nil {
		t.Fatalf("lookupImage of a daemon that hangs = present %v, silent %v, %v; want only silent", present, silent, err)
	}
}

// TestDialect_Unreachable: each CLI's words that no engine answered it, and
// words that are an engine's answer.
func TestDialect_Unreachable(t *testing.T) {
	for said, want := range map[string]bool{
		"Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?":        true,
		"docker: Cannot connect to the Docker daemon at tcp://x:2375.":                                             true,
		"Got permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock": true,
		"permission denied while trying to connect to the docker API at unix:///var/run/docker.sock":               true,
		"error during connect: Get \"http://x/v1.51/info\": EOF":                                                   true,
		"Cannot connect to Podman. Please verify your connection":                                                  true,
		"unable to connect to Podman socket: dial unix /run/podman/podman.sock: connect: connection refused":       true,
		"docker: Error response from daemon: CDI device injection failed: unresolvable CDI devices amd.com/gpu=0":  false,
		"docker: Error response from daemon: no space left on device":                                              false,
		"Error response from daemon: Get \"https://ghcr.io/v2/\": dial tcp: connect: connection refused":           false,
		"Error: No such image": false,
	} {
		for _, cli := range []string{CLIDocker, CLIPodman} {
			if got := (dialect{cli: cli}).unreachable(said); got != want {
				t.Errorf("unreachable(%q) under %s = %v, want %v", said, cli, got, want)
			}
		}
	}
}

// TestDockerRuntime_APullThatAnswersIsNotSilence: an inspect nobody answered
// followed by a pull that got its answer is the pull's answer.
func TestDockerRuntime_APullThatAnswersIsNotSilence(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("inspectunreachable", "")
	fake.set("pullfail", "")
	runtime := &DockerRuntime{Bin: fake.bin}
	_, err := runtime.Pull(t.Context(), "ghcr.io/orkstack/orker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err == nil || errors.Is(err, errEngineUnreachable) || !strings.Contains(err.Error(), "pull access denied") {
		t.Fatalf("Pull = %v; want the registry's own answer", err)
	}
}

// TestDockerRuntime_RunBootProbeUnidentifiedEngine: an engine that cannot be
// reached to say what it is, is unreachable and not a refusal.
func TestDockerRuntime_RunBootProbeUnidentifiedEngine(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("versionfail", "")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: time.Minute}
	if _, err := runtime.RunBootProbe(t.Context(), probeSpecUnder(t, t.TempDir())); !errors.Is(err, errEngineUnreachable) || !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("RunBootProbe = %v, want ErrPostureRefused and errEngineUnreachable", err)
	}
}

// TestDockerRuntime_RunBootProbeNeverRetriesAUnit: a probe given a CDI device
// is run once: a second run would share the unit with a first that could not
// be removed.
func TestDockerRuntime_RunBootProbeNeverRetriesAUnit(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("slowruns", "9")
	runtime := fake.slow(&DockerRuntime{Bin: fake.bin, BindProbeTimeout: probeBudget})
	spec := probeSpecUnder(t, t.TempDir())
	spec.CDIDevices = []string{"amd.com/gpu=0"}
	if _, err := runtime.RunBootProbe(t.Context(), spec); !errors.Is(err, ErrProbeTimedOut) || fake.runs() != 1 {
		t.Fatalf("RunBootProbe = %v after %d runs; want ErrProbeTimedOut after one", err, fake.runs())
	}
}

// TestDockerRuntime_ACallEndsAtItsBudget: outside callBound a call and the
// engine question end at the budget they are given, as an overrun. Nothing here
// has to be quick: the stand-in holds each call far past its budget.
func TestDockerRuntime_ACallEndsAtItsBudget(t *testing.T) {
	const budget = 50 * time.Millisecond
	fake := newCountingDocker(t)
	fake.set("inspecthang", "")
	if _, overran, _ := (&DockerRuntime{Bin: fake.bin}).runWithin(t.Context(), budget, "image", "inspect"); !overran {
		t.Error("a held call did not end as an overrun of its budget")
	}
	fake.set("versionhang", "")
	if _, overran, _ := (&DockerRuntime{Bin: fake.bin}).dialectWithin(t.Context(), budget); !overran {
		t.Error("an engine that answers nothing did not end as an overrun of its budget")
	}
}

// TestDockerRuntime_TheProbesCallsAreBoundedByItsBudget: BindProbeTimeout is the
// bound of every step of a boot probe that waits on the daemon: which engine it
// is, its image, and each container's run, attached and detached.
func TestDockerRuntime_TheProbesCallsAreBoundedByItsBudget(t *testing.T) {
	const budget = 73 * time.Minute // no other bound's value
	for name, probe := range map[string]struct {
		run   func(*DockerRuntime, ContainerSpec) error
		bound []string
	}{
		"attached": {func(d *DockerRuntime, spec ContainerSpec) error {
			_, err := d.RunBootProbe(t.Context(), spec)
			return err
		}, []string{"version ", "info ", "image inspect ", "run "}},
		"detached": {func(d *DockerRuntime, spec ContainerSpec) error {
			_, err := d.StartBootProbe(t.Context(), spec)
			return err
		}, []string{"version ", "info ", "run --detach "}},
	} {
		t.Run(name, func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set("present", "")
			var withins []time.Duration
			runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: budget}
			runtime.callBound = func(ctx context.Context, within time.Duration) (context.Context, context.CancelFunc) {
				withins = append(withins, within)
				return context.WithCancel(ctx)
			}
			must.OK(t, probe.run(runtime, probeSpecUnder(t, t.TempDir())))
			// A run's script spans lines, so only the calls' first lines are told apart.
			calls := fake.calls()
			if len(calls) < len(probe.bound) {
				t.Fatalf("calls = %q; want %q", calls, probe.bound)
			}
			for i, want := range probe.bound {
				if !strings.HasPrefix(calls[i], want) {
					t.Errorf("call %d = %q; want %q", i, calls[i], want)
				}
			}
			// One bound for the engine question, then one a call.
			if len(withins) != len(probe.bound)+1 {
				t.Fatalf("%d bounds for %d calls", len(withins), len(probe.bound))
			}
			for i, within := range withins {
				if within != budget {
					t.Errorf("bound %d is %s; want the probe's budget, %s", i, within, budget)
				}
			}
		})
	}
}
