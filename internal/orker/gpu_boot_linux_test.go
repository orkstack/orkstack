//go:build linux

package orker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// unitListRuntime is a runtime that lists unit containers and removes them,
// recording what it removed.
type unitListRuntime struct {
	unitProbeRuntime
	listed  []UnitContainer
	removed []ContainerID
}

func (r *unitListRuntime) UnitContainers(context.Context) ([]UnitContainer, error) {
	return r.listed, nil
}
func (r *unitListRuntime) Remove(_ context.Context, id ContainerID) error {
	r.removed = append(r.removed, id)
	return nil
}

// gpuActivities is an Activities offering the gpu profile "amd" (one unit,
// audited as amdNodes) over a lock directory of its own, and that directory.
func gpuActivities(t *testing.T, rt *unitListRuntime) (*Activities, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "gpu-orker")
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	old := gpuLockDirPath
	gpuLockDirPath = dir
	t.Cleanup(func() { gpuLockDirPath = old })
	declared := Profiles{List: []Profile{{Name: "amd", Kind: orkstackv1.ProfileKind_PROFILE_KIND_GPU,
		GPU: &GPUProfile{CDIKind: "amd.com/gpu", Units: []string{"0"}}}}}
	s := newProfileState(declared, "c0ffee")
	rt.label = "c0ffee"
	s.setAudited(map[string]profileAudit{"amd": {identity: "x", gpu: &gpuSpecAudit{}, gpuNodes: amdNodes, unitNodes: [][]auditedDevice{amdNodes}}})
	c := newCapacity(amount{}, amount{milli: 100, bytes: 1 << 20}, 2)
	c.useUnits("amd", 1, 1, unitHealthReporter(s, "amd", time.Now))
	a := &Activities{dispatcher: &OCIDispatcher{Runtime: rt, Policy: DefaultSecurityPolicy(), profiles: s}, profiles: s, capacity: c}
	return a, dir
}

// TestBootGPU_TakesTheLockReapsAndOffers: with the lock free, boot takes it,
// removes every container bound under it, and offers the units to dispatch.
func TestBootGPU_TakesTheLockReapsAndOffers(t *testing.T) {
	rt := &unitListRuntime{}
	a, dir := gpuActivities(t, rt)
	probe, err := openGPULock(dir)
	if err != nil {
		t.Fatal(err)
	}
	identity := probe.identity()
	probe.close()
	rt.listed = []UnitContainer{{Lock: identity, Units: "amd:0", OwnContainer: OwnContainer{ID: "left"}}}
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatalf("bootGPU = %v", err)
	}
	if len(rt.removed) != 1 || rt.removed[0] != "left" {
		t.Fatalf("removed %v, want [left]", rt.removed)
	}
	if a.dispatcher.gpu.Load() == nil || a.profiles.unavailable("amd") {
		t.Fatal("the units are not offered after the lock was taken")
	}
	t.Cleanup(func() { a.dispatcher.gpu.Load().lock.(*gpuHostLock).close() })
}

// TestBootGPU_HeldLockWaitsThenOffers: a lock another holds keeps the profile
// unavailable (GPU_HOST_LOCK_HELD), naming the holder, until it is released;
// then the reap runs and the units are offered.
func TestBootGPU_HeldLockWaitsThenOffers(t *testing.T) {
	rt := &unitListRuntime{}
	a, dir := gpuActivities(t, rt)
	other, err := openGPULock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := other.tryLock(); !ok {
		t.Fatal("the other holder did not take the lock")
	}
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatalf("bootGPU = %v", err)
	}
	held := orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GPU_HOST_LOCK_HELD
	if !a.profiles.unavailableFor("amd", held) || a.dispatcher.gpu.Load() != nil {
		t.Fatal("a held lock did not hold the profile unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { a.awaitGPULock(ctx, 5*time.Millisecond); close(done) }()
	other.close()
	<-done
	if a.profiles.unavailableFor("amd", held) || a.dispatcher.gpu.Load() == nil {
		t.Fatal("releasing the lock did not bring the units into service")
	}
	a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
}

// TestProbeUnitsOnce_FlipsHealthUnderTheLedger: one pass probes every free
// unit; a unit whose probe fails leaves the usable set.
func TestProbeUnitsOnce_FlipsHealthUnderTheLedger(t *testing.T) {
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	rt.view = append(answering(amdNodes), heldLockEntry(a))
	a.probeUnitsOnce(context.Background(), t.TempDir(), viewProbeImage)
	if a.capacity.limit.gpu != 1 {
		t.Fatalf("a healthy unit left the usable set: limit %d", a.capacity.limit.gpu)
	}
	rt.view = nil
	rt.runErr = os.ErrDeadlineExceeded
	a.probeUnitsOnce(context.Background(), t.TempDir(), viewProbeImage)
	if a.capacity.limit.gpu != 0 || !a.profiles.unavailableFor("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_ALL_UNITS_UNHEALTHY) {
		t.Fatalf("a failing unit stayed usable: limit %d", a.capacity.limit.gpu)
	}
}

// TestProbeUnitsOnce_RefusesAProbeImageOfAnotherBuild: a probe image whose
// build is not this Orker's judges no unit; the gpu profile is unavailable
// (PROBE_IMAGE_MISMATCH) until an image of its own build is there.
func TestProbeUnitsOnce_RefusesAProbeImageOfAnotherBuild(t *testing.T) {
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	rt.label = "another"
	a.probeUnitsOnce(context.Background(), t.TempDir(), viewProbeImage)
	mismatch := orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH
	if len(rt.specs) != 0 || !a.profiles.unavailableFor("amd", mismatch) {
		t.Fatalf("probes run %d, mismatch held %v; want none run and PROBE_IMAGE_MISMATCH", len(rt.specs), a.profiles.unavailableFor("amd", mismatch))
	}
	rt.label, rt.view = "c0ffee", append(answering(amdNodes), heldLockEntry(a))
	a.probeUnitsOnce(context.Background(), t.TempDir(), viewProbeImage)
	if a.profiles.unavailable("amd") || len(rt.specs) != 1 {
		t.Fatalf("an image of its own build: unavailable %v, probes %d", a.profiles.unavailable("amd"), len(rt.specs))
	}
}

// TestProbeUnits_RecoveryProbesOnlyFailedUnits: the recovery pass leaves a
// usable unit alone and brings a failed one back after recoveryPasses good
// probes.
func TestProbeUnits_RecoveryProbesOnlyFailedUnits(t *testing.T) {
	rt := &unitListRuntime{}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	rt.view = append(answering(amdNodes), heldLockEntry(a))
	a.probeUnits(context.Background(), t.TempDir(), viewProbeImage, true)
	if len(rt.specs) != 0 {
		t.Fatalf("the recovery pass probed a usable unit %d times", len(rt.specs))
	}
	rt.runErr = os.ErrDeadlineExceeded
	a.probeUnitsOnce(context.Background(), t.TempDir(), viewProbeImage)
	rt.runErr = nil
	for range recoveryPasses {
		a.probeUnits(context.Background(), t.TempDir(), viewProbeImage, true)
	}
	if !a.capacity.unitUsable(0) {
		t.Fatal("the failed unit did not recover after three good recovery probes")
	}
}

// heldLockEntry is the held lock directory as a probe container sees it.
func heldLockEntry(a *Activities) viewEntry {
	_, dev, ino := a.dispatcher.gpu.Load().lock.view()
	return lockEntry(dev, ino)
}

// TestBootGPU_ForeignUnitsHeldUntilGone: a container still running under
// another lock holds the profile FOREIGN_UNITS_HELD, naming it, and the next
// probe pass that finds it gone ends the hold.
func TestBootGPU_ForeignUnitsHeldUntilGone(t *testing.T) {
	rt := &unitListRuntime{listed: []UnitContainer{{Lock: "OLD", Units: "amd:0", State: "running", OwnContainer: OwnContainer{ID: "still"}}}}
	a, _ := gpuActivities(t, rt)
	if err := a.bootGPU(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.dispatcher.gpu.Load().lock.(*gpuHostLock).close()
	held := orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_FOREIGN_UNITS_HELD
	if !a.profiles.unavailableFor("amd", held) || len(rt.removed) != 0 {
		t.Fatalf("a foreign running container: held %v, removed %v", a.profiles.unavailableFor("amd", held), rt.removed)
	}
	rt.listed = nil
	rt.view = append(answering(amdNodes), heldLockEntry(a))
	a.probeUnitsOnce(context.Background(), t.TempDir(), viewProbeImage)
	if a.profiles.unavailableFor("amd", held) {
		t.Fatal("the hold did not end once the foreign container was gone")
	}
}
