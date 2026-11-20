package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
)

// readyVerdict is what `service test` makes of a status at one instant: it
// waits while a service starts, and fails, naming why, a service that says
// it cannot work, never reports, or is still not healthy at the end of its
// startup bound, so a hostile image fails a check instead of hanging it.
func TestReadyVerdict(t *testing.T) {
	const startup = 10 * time.Second
	tests := []struct {
		name    string
		h       servicehost.Health
		st      servicehost.Status
		elapsed time.Duration
		done    bool
		wantErr string
	}{
		{name: "ready", h: servicehost.HealthReady, done: true},
		{name: "starting", h: servicehost.HealthStarting, elapsed: time.Second},
		{name: "not ready yet", h: servicehost.HealthNotReady, elapsed: time.Second},
		{name: "unhealthy within the bound waits", h: servicehost.HealthUnhealthy, st: servicehost.Status{Reason: "OBJECT_INVALID"}, elapsed: time.Second},
		{name: "unhealthy past the bound fails with its reason", h: servicehost.HealthUnhealthy,
			st: servicehost.Status{Reason: "OBJECT_INVALID", Field: "broker.maxDeliveries"}, elapsed: 11 * time.Second,
			done: true, wantErr: "OBJECT_INVALID broker.maxDeliveries"},
		{name: "not ready past the bound fails", h: servicehost.HealthNotReady, elapsed: 11 * time.Second, done: true, wantErr: "not ready"},
		{name: "stale past the bound fails", h: servicehost.HealthNoVerdict, elapsed: 11 * time.Second, done: true, wantErr: "no fresh status"},
		{name: "fatal", h: servicehost.HealthFatal, st: servicehost.Status{Reason: "CONFIG_INVALID", Field: "subject"},
			done: true, wantErr: "CONFIG_INVALID subject"},
		{name: "never reported", h: servicehost.HealthNeverReported, elapsed: 11 * time.Second, done: true, wantErr: "no status within 10s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done, err := readyVerdict(tt.h, tt.st, tt.elapsed, startup)
			if done != tt.done {
				t.Fatalf("done = %v; want %v (err %v)", done, tt.done, err)
			}
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("err = %v; want none", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("err = %v; want one containing %q", err, tt.wantErr)
			}
		})
	}
}

// A container that is gone before its process could be found is reported
// as an exit, with the last of what it wrote, never as the runtime's own
// complaint about a process it could not inspect.
func TestStartFailure(t *testing.T) {
	exited := make(chan struct{})
	close(exited)
	logs := strings.Repeat("x", 5000) + "hostile: it failed, but not for space"
	err := startFailure(errors.New("inspect container: no running process"), exited, func() error { return nil }, []byte(logs))
	if err == nil || !strings.Contains(err.Error(), "exited before it could be watched") ||
		!strings.Contains(err.Error(), "not for space") || len(err.Error()) > 2200 {
		t.Fatalf("an early exit: %v", err)
	}
	running := make(chan struct{})
	inner := errors.New("the runtime erred")
	if err := startFailure(inner, running, func() error { return nil }, nil); !errors.Is(err, inner) {
		t.Fatalf("a container still running: %v", err)
	}
}

// A wait the runtime could not answer is no exit: the start's failure says
// so instead of reporting what the container wrote as its last words.
func TestStartFailure_AWaitThatFailedIsNoExit(t *testing.T) {
	exited := make(chan struct{})
	close(exited)
	err := startFailure(errors.New("inspect failed"), exited, func() error { return errors.New("docker wait: daemon gone") }, nil)
	if err == nil || !strings.Contains(err.Error(), "daemon gone") || strings.Contains(err.Error(), "exited before") {
		t.Fatalf("a failed wait: %v", err)
	}
}
