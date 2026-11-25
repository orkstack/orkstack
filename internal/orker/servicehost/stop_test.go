package servicehost

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"
)

// fakeContainer exits on the signals it is told to honour and records
// every call. It exists for the fault cases a real container cannot be
// made to produce on demand: one the kernel will not kill, one the runtime
// cannot remove.
type fakeContainer struct {
	mu        sync.Mutex
	signals   []syscall.Signal
	exitOn    map[syscall.Signal]bool
	exited    chan struct{}
	once      sync.Once
	removeErr error
	removed   bool
}

func newFake(exitOn ...syscall.Signal) *fakeContainer {
	f := &fakeContainer{exitOn: map[syscall.Signal]bool{}, exited: make(chan struct{})}
	for _, s := range exitOn {
		f.exitOn[s] = true
	}
	return f
}

func (f *fakeContainer) Signal(_ context.Context, sig syscall.Signal) error {
	f.mu.Lock()
	f.signals = append(f.signals, sig)
	exit := f.exitOn[sig]
	f.mu.Unlock()
	if exit {
		f.once.Do(func() { close(f.exited) })
	}
	return nil
}

func (f *fakeContainer) Wait(ctx context.Context) error {
	select {
	case <-f.exited:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fakeContainer) Remove(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = f.removeErr == nil
	return f.removeErr
}

// A bound the container is meant to overrun is short; one it must beat is only
// there to end a hang.
const (
	overrun = 50 * time.Millisecond
	hang    = 30 * time.Second
)

func TestStop_NoticeThenSequence(t *testing.T) {
	polite := newFake(syscall.SIGTERM)
	res, err := Stop(context.Background(), polite, StopTimes{Grace: hang, Stuck: hang})
	if err != nil || res.Killed || !polite.removed || len(polite.signals) != 1 {
		t.Fatalf("a container that exits on SIGTERM: %+v, %v, signals %v", res, err, polite.signals)
	}
	stubborn := newFake(syscall.SIGKILL)
	res, err = Stop(context.Background(), stubborn, StopTimes{Grace: overrun, Stuck: hang})
	if err != nil || !res.Killed || !stubborn.removed || len(stubborn.signals) != 2 || stubborn.signals[1] != syscall.SIGKILL {
		t.Fatalf("a container that ignores SIGTERM: %+v, %v, signals %v", res, err, stubborn.signals)
	}
}

func TestStop_StuckIsReported(t *testing.T) {
	unkillable := newFake()
	if _, err := Stop(context.Background(), unkillable, StopTimes{Grace: overrun, Stuck: overrun}); !errors.Is(err, ErrStopStuck) {
		t.Fatalf("a container that never exits: %v", err)
	}
	unremovable := newFake(syscall.SIGTERM)
	unremovable.removeErr = errors.New("device busy")
	if _, err := Stop(context.Background(), unremovable, StopTimes{Grace: hang, Stuck: hang}); !errors.Is(err, ErrStopStuck) {
		t.Fatalf("a container that cannot be removed: %v", err)
	}
}

func TestStop_FitsTheCancellationGrace(t *testing.T) {
	d := DefaultStopTimes(20)
	if d.Grace != 20*time.Second || d.Stuck != time.Minute || d.Grace >= 30*time.Second {
		t.Fatalf("stop times %+v: SIGTERM to SIGKILL fits the 30 s cancellation grace", d)
	}
}

// TestStop_ACancelledStopStillKills: a caller that gives up cuts the grace
// short, never the kill and the removal.
func TestStop_ACancelledStopStillKills(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := newFake(syscall.SIGKILL)
	res, err := Stop(ctx, f, StopTimes{Grace: hang, Stuck: hang})
	if err != nil || !res.Killed || !res.CallerGaveUp || !f.removed {
		t.Fatalf("a cancelled stop: %+v, %v, removed %v", res, err, f.removed)
	}
}

// failingSignal is a container whose signals fail: it already exited.
type failingSignal struct{ *fakeContainer }

func (f failingSignal) Signal(context.Context, syscall.Signal) error {
	return errors.New("no such process")
}

func TestStop_AFailedSIGTERMStillRemoves(t *testing.T) {
	f := newFake()
	f.once.Do(func() { close(f.exited) })
	if res, err := Stop(context.Background(), failingSignal{f}, StopTimes{Grace: hang, Stuck: hang}); err != nil || !f.removed || res.TermErr == nil {
		t.Fatalf("a container that already exited: %v, removed %v", err, f.removed)
	}
}

func TestExit_TableOfMeanings(t *testing.T) {
	for _, tc := range []struct {
		name    string
		exit    Exit
		reason  string
		retried bool
	}{
		{"after SIGTERM, any code", Exit{AskedToStop: true, Code: 137}, "", false},
		{"0 unasked", Exit{Code: 0}, "SERVICE_EXITED", true},
		{"non-zero unasked", Exit{Code: 3}, "SERVICE_FAILED", true},
		{"OOM", Exit{Code: 137, OOMKilled: true}, "SERVICE_OOM_KILLED", true},
		{"stopped by its runtime", Exit{Code: 0, StoppedByRuntime: true}, "CONTAINER_STOPPED_BY_RUNTIME", true},
		{"needs root", Exit{Code: 1, ImageUserRoot: true, BeforeFirstStatus: true}, "SERVICE_RUNS_AS_ROOT", true},
		{"root image that ran", Exit{Code: 1, ImageUserRoot: true}, "SERVICE_FAILED", true},
		{"asked, though root", Exit{AskedToStop: true, ImageUserRoot: true, BeforeFirstStatus: true}, "", false},
	} {
		got := Meaning(tc.exit)
		if got.Reason != tc.reason || got.Retried != tc.retried || got.Detail == "" || got.Stopped() != (tc.reason == "") {
			t.Errorf("%s: %+v; want %q retried %v", tc.name, got, tc.reason, tc.retried)
		}
	}
}

func TestBeat_Rises(t *testing.T) {
	var b Beat
	first, second := b.Next(), b.Next()
	if first["beat"].(uint64) != 1 || second["beat"].(uint64) != 2 || first["schemaVersion"] != SchemaVersion {
		t.Fatalf("beats %v %v", first, second)
	}
}
