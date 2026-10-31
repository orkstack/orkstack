package orker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// waitForCall returns once the stand-in has been invoked with an argv holding
// every one of words, without forgetting any call, and fails the test at a
// deadline.
func waitForCall(t *testing.T, fake *countingDocker, words ...string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		raw, _ := os.ReadFile(filepath.Join(fake.dir, "calls"))
		for _, line := range strings.Split(string(raw), "\n") {
			found := line != ""
			for _, word := range words {
				found = found && strings.Contains(line, word)
			}
			if found {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no call holding %q within 30 s:\n%s", words, raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestBoot_ProbeContainersCarryTheOrkersIdentity: both of the boot probe's
// containers are labelled with the Orker's identity, as a task's are, so one
// an Orker stopped mid-probe leaves behind is its next boot's to reap
// (Reap keys on ork.orker) and `proofrun clean` lists it. Neither is left to
// the daemon's --rm, which fires only when a container exits and never for one
// created and not started: the Orker removes each itself before Boot returns.
func TestBoot_ProbeContainersCarryTheOrkersIdentity(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	id := fmt.Sprintf("orker-probe-label-%d", os.Getpid())
	activities := NewActivitiesWithOptions(nil, Options{
		OrkerID:  id,
		WorkRoot: t.TempDir(),
		Runtime:  &DockerRuntime{Bin: fake.bin},
	})
	t.Cleanup(activities.Close)
	activities.dispatcher.signalProbeGrace = 300 * time.Millisecond

	if err := activities.Boot(context.Background()); err != nil {
		t.Fatalf("Boot = %v", err)
	}

	calls := fake.calls()
	probes := map[string]bool{}
	for _, call := range calls {
		fields := strings.Fields(call)
		if len(fields) == 0 || fields[0] != "run" {
			continue
		}
		for _, task := range []string{bootProbeTaskID, signalProbeTaskID} {
			if strings.Contains(call, "--label ork.task="+task) {
				probes[task] = true
				if !strings.Contains(call, "--label ork.orker="+id+" ") {
					t.Errorf("the %s container is not labelled ork.orker=%s: %s", task, id, call)
				}
				if strings.Contains(call, " --rm ") {
					t.Errorf("the %s container is left to the daemon's --rm: %s", task, call)
				}
			}
		}
	}
	if !probes[bootProbeTaskID] || !probes[signalProbeTaskID] {
		t.Fatalf("Boot ran probe containers %v; want both: %q", probes, calls)
	}
	ran, removed := probeContainerNames(t, calls)
	for _, name := range ran {
		if !slices.Contains(removed, name) {
			t.Errorf("the probe's container %s was not removed before Boot returned; removed %q", name, removed)
		}
	}
}

// TestBoot_EveryWayTheProbeEndsRemovesItsContainer: whichever way Boot leaves
// the probe — it passes, the daemon refuses or fails a container, the
// workload never hears SIGTERM, or the Orker is stopped while a container's
// `run` is still in flight — every container a probe's `run` named is
// removed before Boot returns. A `run` cut off between the daemon's create
// and its start leaves a container created and never started, which the
// daemon's --rm never removes: four such ork-sigprobe-* containers were found
// on a proof host, unlabelled, so neither a reaper nor `proofrun clean` could
// tell whose they were.
func TestBoot_EveryWayTheProbeEndsRemovesItsContainer(t *testing.T) {
	tests := []struct {
		name  string
		flags map[string]string
		// stopAt, when set, stops the Orker once a call holding every one of
		// these words has been made.
		stopAt []string
		boots  bool
	}{
		{name: "the probe passes", boots: true},
		{name: "the first container exits non-zero", flags: map[string]string{"runexit": ""}},
		{name: "the daemon refuses the first container's bind", flags: map[string]string{"runfail": ""}},
		{name: "the daemon refuses the first container for another reason", flags: map[string]string{"runother": ""}},
		{name: "the daemon refuses the SIGTERM container", flags: map[string]string{"sigrunother": ""}},
		{name: "the SIGTERM container's workload never hears it", flags: map[string]string{"deaf": ""}},
		{
			name:   "the Orker is stopped while the first container's run is in flight",
			flags:  map[string]string{"slowruns": "9"},
			stopAt: []string{"run ", "ork.task=" + bootProbeTaskID},
		},
		{
			name:   "the Orker is stopped while the SIGTERM container's run is in flight",
			flags:  map[string]string{"sigrunslow": ""},
			stopAt: []string{"run ", "ork.task=" + signalProbeTaskID},
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
				OrkerID:  fmt.Sprintf("orker-probe-ends-%d-%d", os.Getpid(), i),
				WorkRoot: t.TempDir(),
				Runtime:  &DockerRuntime{Bin: fake.bin, BindProbeTimeout: time.Minute},
			})
			t.Cleanup(activities.Close)
			activities.dispatcher.signalProbeGrace = 300 * time.Millisecond

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- activities.Boot(ctx) }()
			if tt.stopAt != nil {
				waitForCall(t, fake, tt.stopAt...)
				cancel()
			}
			var err error
			select {
			case err = <-done:
			case <-time.After(time.Minute):
				t.Fatal("Boot did not return within a minute")
			}

			switch {
			case tt.boots && err != nil:
				t.Fatalf("Boot = %v; want it to boot", err)
			case !tt.boots && err == nil:
				t.Fatal("Boot = nil; want a refusal")
			case tt.stopAt != nil && !errors.Is(err, context.Canceled):
				t.Fatalf("Boot = %v; want the context's own error", err)
			}
			calls := fake.calls()
			ran, removed := probeContainerNames(t, calls)
			if len(ran) == 0 {
				t.Fatalf("no probe container was run: %q", calls)
			}
			for _, name := range ran {
				if !slices.Contains(removed, name) {
					t.Errorf("the probe's container %s was not removed before Boot returned (%v); removed %q", name, err, removed)
				}
			}
		})
	}
}

// TestDockerRuntime_ARefusalNamesTheProbeContainerItCouldNotRemove: a probe
// container the daemon refused, and then would not remove either, is named in
// the refusal beside the daemon's answer, as an overrun's is: it is the
// container an operator looks for, until the Orker's next boot reaps it.
func TestDockerRuntime_ARefusalNamesTheProbeContainerItCouldNotRemove(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("runother", "")
	fake.set("rmfailall", "")
	runtime := &DockerRuntime{Bin: fake.bin, BindProbeTimeout: time.Minute}

	_, err := runtime.RunBootProbe(context.Background(), probeSpecUnder(t, t.TempDir()))

	if !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("RunBootProbe = %v; want ErrPostureRefused", err)
	}
	ran, _ := probeContainerNames(t, fake.calls())
	if len(ran) != 1 {
		t.Fatalf("runs %q; want one", ran)
	}
	for _, want := range []string{"no space left on device", "could not be removed and may remain", ran[0]} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not contain %q", err, want)
		}
	}
}

// TestBoot_ARetryThatPassesLogsTheContainerItCouldNotRemove: an attempt that
// overran, and whose container the daemon then would not remove, is not
// forgotten because the retry passed. Boot logs it by name, as it logs a
// container of a probe that ended any other way, so an operator knows what
// may remain until the next boot under this identity reaps it.
func TestBoot_ARetryThatPassesLogsTheContainerItCouldNotRemove(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("slowruns", "1")
	fake.set("rmfailall", "")
	var logs bytes.Buffer
	activities := NewActivitiesWithOptions(nil, Options{
		OrkerID:  fmt.Sprintf("orker-probe-leftover-%d", os.Getpid()),
		WorkRoot: t.TempDir(),
		Runtime:  &DockerRuntime{Bin: fake.bin, BindProbeTimeout: probeBudget},
		Logger:   slog.New(slog.NewTextHandler(&logs, nil)),
	})
	t.Cleanup(activities.Close)
	activities.dispatcher.signalProbeGrace = 300 * time.Millisecond

	if err := activities.Boot(context.Background()); err != nil {
		t.Fatalf("Boot = %v; want it to boot on the retry", err)
	}

	ran, _ := probeContainerNames(t, fake.calls())
	if len(ran) < 2 || !strings.HasPrefix(ran[0], "ork-"+bootProbeTaskID+"-") {
		t.Fatalf("probe containers run %q; want the first container's overrun attempt first", ran)
	}
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "level=WARN") && strings.Contains(line, "could not be removed") &&
			strings.Contains(line, "container="+ran[0]) && strings.Contains(line, "already in progress") {
			return
		}
	}
	t.Errorf("Boot did not warn, by name, of the overrun attempt's container %s it could not remove:\n%s", ran[0], logs.String())
}
