//go:build linux

package orker

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// unitDispatcher is a dispatcher offering the gpu profile "amd" of units
// u0..u(n-1) under a held lock, and its ledger.
func unitDispatcher(t *testing.T, n int) (*OCIDispatcher, *Capacity, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "gpu-orker")
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	lock := openTestGPULock(t, dir)
	if ok, err := lock.tryLock(); !ok || err != nil {
		t.Fatalf("tryLock = %v, %v", ok, err)
	}
	c, _ := gpuLedger(t, 4, n, n)
	units := make([]string, n)
	for i := range units {
		units[i] = "u" + string(rune('0'+i))
	}
	d := &OCIDispatcher{profiles: newProfileState(Profiles{}, "")}
	d.gpu.Store(&gpuOffer{profile: "amd", cdiKind: "amd.com/gpu", units: units, lock: lock})
	return d, c, dir
}

func gpuTask(n int32) *orkstackv1.ExecuteTaskInput {
	return &orkstackv1.ExecuteTaskInput{
		RunId: "ork-r", TaskId: "train",
		Devices:   []*orkstackv1.ProfileRef{{Name: "amd", IdentityDigest: "x"}},
		Resources: &orkstackv1.ResourceRequest{Gpu: n},
	}
}

// TestBindUnits_NamesTheBoundUnitsAndLabelsThem: step 6b gives a task its
// units as CDI names and labels the container with the lock and the indices,
// so the lock-scoped reap can find and seed it.
func TestBindUnits_NamesTheBoundUnitsAndLabelsThem(t *testing.T) {
	d, c, _ := unitDispatcher(t, 3)
	a, done := pollTask(t, c, gpuReq(2))
	defer done()
	b, failure := d.bindUnits(context.Background(), gpuTask(2), a)
	if failure != nil {
		t.Fatalf("bindUnits = %v", failure)
	}
	if !slices.Equal(b.cdi, []string{"amd.com/gpu=u0", "amd.com/gpu=u1"}) {
		t.Fatalf("cdi = %v", b.cdi)
	}
	if b.labels[labelUnits] != "amd:0,1" || b.labels[labelUnitsLock] != d.gpu.Load().lock.identity() {
		t.Fatalf("labels = %v", b.labels)
	}
	a.Done()
}

// TestBindUnits_NoneAskedNothingBound: a task with no gpu count binds nothing.
func TestBindUnits_NoneAskedNothingBound(t *testing.T) {
	d, c, _ := unitDispatcher(t, 1)
	a, done := pollTask(t, c, amount{milli: 100, bytes: 1 << 20})
	defer done()
	b, failure := d.bindUnits(context.Background(), &orkstackv1.ExecuteTaskInput{TaskId: "plain"}, a)
	if failure != nil || len(b.cdi) != 0 || len(b.labels) != 0 {
		t.Fatalf("bindUnits = %+v, %v; want nothing", b, failure)
	}
	a.Done()
}

// TestBindUnits_NeverFewerThanAsked: an admission that reserved another count
// than the task asks for (an input its reader could not read) fails the attempt
// before any container, never running it with no unit or with another task's.
func TestBindUnits_NeverFewerThanAsked(t *testing.T) {
	for name, reserved := range map[string]amount{
		"none":  {milli: 100, bytes: 1 << 20},
		"fewer": gpuReq(1),
		"more":  gpuReq(3),
	} {
		t.Run(name, func(t *testing.T) {
			d, c, _ := unitDispatcher(t, 3)
			a, done := pollTask(t, c, reserved)
			defer done()
			b, failure := d.bindUnits(context.Background(), gpuTask(2), a)
			if failure == nil || failure.Reason != ReasonGPUUnitsUnavailable || len(b.cdi) != 0 {
				t.Fatalf("bindUnits = %+v, %v; want %s and nothing bound", b, failure, ReasonGPUUnitsUnavailable)
			}
			a.Done()
			c.mu.Lock()
			defer c.mu.Unlock()
			for i, owner := range c.units.owner {
				if owner != nil {
					t.Errorf("unit %d is still held after the refused attempt ended", i)
				}
			}
		})
	}
}

// TestBindUnits_RefusedWithoutAGPUProfileOrLedger: an Orker that offers no
// gpu profile, and a local run with no ledger, bind nothing.
func TestBindUnits_RefusedWithoutAGPUProfileOrLedger(t *testing.T) {
	plain := &OCIDispatcher{}
	if _, failure := plain.bindUnits(context.Background(), gpuTask(1), nil); failure == nil || failure.Reason != ReasonGPUUnitsUnavailable {
		t.Fatalf("no gpu profile: %v", failure)
	}
	d, _, _ := unitDispatcher(t, 1)
	if _, failure := d.bindUnits(context.Background(), gpuTask(1), nil); failure == nil || failure.Reason != ReasonGPUUnitsUnavailable {
		t.Fatalf("no ledger: %v", failure)
	}
	task := gpuTask(1)
	task.Devices = []*orkstackv1.ProfileRef{{Name: "other"}}
	_, c, _ := unitDispatcher(t, 1)
	a, done := pollTask(t, c, gpuReq(1))
	defer done()
	if _, failure := d.bindUnits(context.Background(), task, a); failure == nil || failure.Reason != ReasonProfileNotOffered {
		t.Fatalf("a task not naming the gpu profile: %v", failure)
	}
	a.Done()
}

// TestBindUnits_LockMismatchIsDrift: a lock directory recreated under the
// held one stops binding, latched as HOST_LOCK_MISMATCH drift, and the
// attempt is a never-started PROFILE_DRIFTED.
func TestBindUnits_LockMismatchIsDrift(t *testing.T) {
	d, c, dir := unitDispatcher(t, 1)
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	a, done := pollTask(t, c, gpuReq(1))
	defer done()
	_, failure := d.bindUnits(context.Background(), gpuTask(1), a)
	if failure == nil || failure.Reason != ReasonProfileDrifted || !failure.NeverStarted {
		t.Fatalf("bindUnits = %v, want a never-started PROFILE_DRIFTED", failure)
	}
	if !d.profiles.drifted("amd") {
		t.Fatal("the mismatch did not latch drift")
	}
	if got := a.Units(); len(got) != 0 {
		t.Fatalf("units %v bound under a mismatched lock", got)
	}
	a.Done()
}
