package orker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestBoot_RefusesARuntimeThatDoesNotDeliverSIGTERM is the boot probe's
// SIGTERM check: a container built as a task's is, whose workload traps
// SIGTERM, is cancelled as a task is, and an Orker whose workload never hears
// it refuses to start as RUNTIME_POSTURE_REFUSED, naming what the workload
// says it ran under. Rootful Podman under AppArmor 5 was such a runtime:
// every cancelled task was killed at the end of its grace without its
// cleanup, and the Orker booted. A daemon too slow to start that container,
// or to let its workload say it is ready, is the slow daemon it is.
func TestBoot_RefusesARuntimeThatDoesNotDeliverSIGTERM(t *testing.T) {
	const stacked = "containers-default-0.66.0//&crun (enforce)"
	tests := []struct {
		name       string
		flags      map[string]string
		budget     time.Duration
		wantReason string   // "" boots
		want       []string // in the refusal
		// wantRemoved: the Orker removes the container itself, because it
		// may still be running; otherwise the daemon removes it on its exit.
		wantRemoved bool
	}{
		{name: "a workload that hears SIGTERM boots"},
		{
			name:       "a workload under a stacked AppArmor label that never hears it",
			flags:      map[string]string{"deaf": "", "label": stacked},
			wantReason: ReasonRuntimePostureRefused,
			want: []string{
				"never heard SIGTERM", "its trap had not run at the end of the grace",
				"without its cleanup", "stacked AppArmor label", stacked, `operation="signal"`,
				`"Rootful Podman under AppArmor"`, "rootless",
			},
		},
		{
			name: "a workload that started with SIGTERM ignored",
			flags: map[string]string{"deaf": "", "signals": "SigBlk:\t0000000000000000\n" +
				"SigIgn:\t0000000000004004\nSigCgt:\t0000000000000000\n"},
			wantReason: ReasonRuntimePostureRefused,
			want:       []string{"never heard SIGTERM", "SIGTERM ignored (SigIgn 0000000000004004)"},
		},
		{
			name: "a workload with SIGTERM blocked",
			flags: map[string]string{"deaf": "", "signals": "SigBlk:\t0000000000004000\n" +
				"SigIgn:\t0000000000000000\nSigCgt:\t0000000000004000\n"},
			wantReason: ReasonRuntimePostureRefused,
			want:       []string{"SIGTERM blocked (SigBlk 0000000000004000)"},
		},
		{
			name:       "an unconfined workload that never hears it, for no reason it can see",
			flags:      map[string]string{"deaf": ""},
			wantReason: ReasonRuntimePostureRefused,
			want:       []string{"never heard SIGTERM", "nothing inside the container says why"},
		},
		{
			name:       "a runtime that refuses the SIGTERM itself",
			flags:      map[string]string{"termrefused": ""},
			wantReason: ReasonRuntimePostureRefused,
			want:       []string{"never heard SIGTERM", "the runtime refused the SIGTERM itself", "permission denied"},
		},
		{
			name:        "a workload that is not ready within the probe's budget",
			flags:       map[string]string{"neverready": ""},
			budget:      time.Second,
			wantReason:  ReasonBindProbeTimedOut,
			want:        []string{"let the SIGTERM probe's workload say it is ready within 1s", BindProbeTimeoutVar},
			wantRemoved: true,
		},
		{
			name:        "a daemon that does not start the SIGTERM container is slow, not a weaker posture",
			flags:       map[string]string{"sigrunslow": ""},
			budget:      time.Second,
			wantReason:  ReasonBindProbeTimedOut,
			want:        []string{"on each of 2 attempts", BindProbeTimeoutVar},
			wantRemoved: true, // each overrun attempt's container
		},
		{
			name:        "a daemon whose own deadline error repeats at the SIGTERM container is slow",
			flags:       map[string]string{"sigruntimeout": ""},
			wantReason:  ReasonBindProbeTimedOut,
			want:        []string{"context deadline exceeded", BindProbeTimeoutVar},
			wantRemoved: true,
		},
		{
			name:       "a daemon that refuses the SIGTERM container for another reason",
			flags:      map[string]string{"sigrunother": ""},
			wantReason: ReasonRuntimePostureRefused,
			want:       []string{"SIGTERM container", "no space left on device"},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set("present", "")
			for flag, content := range tt.flags {
				fake.set(flag, content)
			}
			budget := tt.budget
			if budget == 0 {
				budget = time.Minute
			}
			activities := NewActivitiesWithOptions(nil, Options{
				OrkerID:  fmt.Sprintf("orker-sigprobe-%d-%d", os.Getpid(), i),
				WorkRoot: t.TempDir(),
				Runtime:  &DockerRuntime{Bin: fake.bin, BindProbeTimeout: budget},
			})
			t.Cleanup(activities.Close)
			activities.dispatcher.signalProbeGrace = 300 * time.Millisecond

			err := activities.Boot(context.Background())

			calls := fake.calls()
			if tt.wantReason == "" {
				if err != nil {
					t.Fatalf("Boot = %v; want it to boot", err)
				}
			} else {
				if err == nil || !strings.HasPrefix(err.Error(), tt.wantReason+":") {
					t.Fatalf("Boot = %v; want a refusal starting %s:", err, tt.wantReason)
				}
				for _, want := range tt.want {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("Boot = %v; want it to say %q", err, want)
					}
				}
			}
			// The probe's container is started detached and removed by the
			// daemon when it exits, and by the Orker only when it may still
			// be running.
			var started, removed bool
			for _, call := range calls {
				if strings.HasPrefix(call, "run --detach --rm ") && strings.Contains(call, "ork.task="+signalProbeTaskID) {
					started = true
				}
				if strings.HasPrefix(call, "rm ") && strings.Contains(call, "ork-"+signalProbeTaskID+"-") {
					removed = true
				}
			}
			if !started {
				t.Errorf("the SIGTERM probe's container was never started detached with --rm: %q", calls)
			}
			if removed != tt.wantRemoved {
				t.Errorf("the Orker removed the SIGTERM probe's container: %t; want %t: %q", removed, tt.wantRemoved, calls)
			}
		})
	}
}

// TestBoot_SIGTERMReachesTheProbeAsACancelledTasksDoes: the signal is sent
// the way a cancelled task's is (TerminateContainer), once, after the
// workload said it was ready, and a workload that hears it is never killed.
func TestBoot_SIGTERMReachesTheProbeAsACancelledTasksDoes(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	activities := NewActivitiesWithOptions(nil, Options{
		OrkerID:  fmt.Sprintf("orker-sigprobe-path-%d", os.Getpid()),
		WorkRoot: t.TempDir(),
		Runtime:  &DockerRuntime{Bin: fake.bin},
	})
	t.Cleanup(activities.Close)
	if err := activities.Boot(context.Background()); err != nil {
		t.Fatalf("Boot = %v", err)
	}
	var signals []string
	for _, call := range fake.calls() {
		if strings.HasPrefix(call, "kill ") && strings.Contains(call, "ork-"+signalProbeTaskID+"-") {
			signals = append(signals, call)
		}
	}
	if len(signals) != 1 || !strings.HasPrefix(signals[0], "kill --signal TERM ork-"+signalProbeTaskID+"-") {
		t.Fatalf("signals = %q; want one SIGTERM to the probe's container, as a cancelled task's container gets it", signals)
	}
}

// TestBoot_AnOrkerStoppedDuringTheSIGTERMProbeSaysSo: an Orker told to stop
// while it waits out the probe's grace stops at once, and says it was
// stopped, not that its runtime is refused.
func TestBoot_AnOrkerStoppedDuringTheSIGTERMProbeSaysSo(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("present", "")
	fake.set("deaf", "")
	activities := NewActivitiesWithOptions(nil, Options{
		OrkerID:  fmt.Sprintf("orker-sigprobe-stop-%d", os.Getpid()),
		WorkRoot: t.TempDir(),
		Runtime:  &DockerRuntime{Bin: fake.bin},
	})
	t.Cleanup(activities.Close)
	activities.dispatcher.signalProbeGrace = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- activities.Boot(ctx) }()
	waitForFile(t, filepath.Join(fake.dir, "termsent"))
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
	if strings.Contains(err.Error(), ReasonRuntimePostureRefused) {
		t.Errorf("Boot = %v; a stop is not %s", err, ReasonRuntimePostureRefused)
	}
	removed := false
	for _, call := range fake.calls() {
		removed = removed || strings.HasPrefix(call, "rm ") && strings.Contains(call, "ork-"+signalProbeTaskID+"-")
	}
	if !removed {
		t.Error("the SIGTERM probe's container, still running when the Orker stopped, was not removed")
	}
}
