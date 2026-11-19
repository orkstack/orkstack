package orker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// gpuLockDirPath is the GPU host lock's directory (design §4.7.5): fixed in
// production, so two Orkers can never be given different locks over one
// host's GPUs, and made by the tmpfiles drop-in scripts/gpu-host-setup.sh
// installs. Only a test assigns it.
var gpuLockDirPath = "/run/orkstack/gpu-orker"

// gpuLockRetry is how often an Orker waiting on a held GPU host lock tries it.
const gpuLockRetry = time.Second

// gpuProfileOf is the one gpu profile an Orker may offer, if it offers one.
func gpuProfileOf(declared Profiles) (Profile, bool) {
	for _, p := range declared.List {
		if p.Kind == orkstackv1.ProfileKind_PROFILE_KIND_GPU && p.GPU != nil {
			return p, true
		}
	}
	return Profile{}, false
}

// gpuWaiting is a GPU host lock this Orker opened and another process holds.
type gpuWaiting struct {
	mu   sync.Mutex
	lock *gpuHostLock
}

// bootGPU takes the GPU host lock for the gpu profile, once the by-id reap
// has run. Free, it runs the lock-scoped reap and offers the units to
// dispatch. Held by another process (an old Orker in its stop grace), the
// profile is unavailable (GPU_HOST_LOCK_HELD) naming the holder, its pool
// stays live, and awaitGPULock takes it later. A lock directory that is not
// there, or cannot be locked, refuses the boot.
func (a *Activities) bootGPU(ctx context.Context) error {
	p, ok := gpuProfileOf(a.profiles.declared)
	if !ok {
		return nil
	}
	lock, err := openGPULock(gpuLockDirPath)
	if err != nil {
		return fmt.Errorf("%s: profile %s: %w; run scripts/gpu-host-setup.sh on this host", ReasonProfileInvalid, p.Name, err)
	}
	held, err := lock.tryLock()
	if err != nil {
		lock.close()
		return fmt.Errorf("%s: profile %s: %w", ReasonProfileInvalid, p.Name, err)
	}
	if held {
		return a.offerGPU(ctx, p, lock)
	}
	a.profiles.setUnavailable(p.Name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GPU_HOST_LOCK_HELD,
		"the GPU host lock "+gpuLockDirPath+" is "+lock.holder().String(), time.Now())
	a.gpuWait = &gpuWaiting{lock: lock}
	return nil
}

// awaitGPULock waits for the lock bootGPU found held, then offers the units.
// It returns when ctx ends, leaving the profile unavailable.
func (a *Activities) awaitGPULock(ctx context.Context, every time.Duration) {
	w := a.gpuWait
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lock == nil {
		return
	}
	if err := w.lock.wait(ctx, every, func(lockHolder) {}); err != nil {
		return
	}
	p, _ := gpuProfileOf(a.profiles.declared)
	lock := w.lock
	w.lock = nil
	if err := a.offerGPU(ctx, p, lock); err != nil {
		a.dispatcher.logger().Error("the GPU host lock was taken, but its containers could not be listed; "+
			"the gpu profile stays out of service", "profile", p.Name, "error", err)
	}
}

// offerGPU runs the lock-scoped reap under the held lock and, once it has
// listed every unit container, offers the units to dispatch.
func (a *Activities) offerGPU(ctx context.Context, p Profile, lock *gpuHostLock) error {
	lister, ok := a.dispatcher.Runtime.(unitContainerLister)
	if !ok {
		lock.close()
		return fmt.Errorf("%s: profile %s: this container runtime cannot list unit containers", ReasonProfileInvalid, p.Name)
	}
	release := func(ctx context.Context, ref artifacts.AttemptRef) error {
		if a.dispatcher.Claims == nil {
			return nil
		}
		call, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimTimeout)
		defer cancel()
		return a.dispatcher.Claims.ReleaseAttempt(call, ref, a.dispatcher.OrkerID)
	}
	foreign, err := reapUnitsUnderLock(ctx, lock.identity(), p.Name, len(p.GPU.Units), unitReapDeps{
		list:     lister.UnitContainers,
		remove:   a.dispatcher.Runtime.Remove,
		release:  release,
		log:      a.dispatcher.logger().Warn,
		capacity: a.capacity,
	})
	if err != nil {
		lock.close()
		return fmt.Errorf("%s: profile %s: %w", ReasonProfileInvalid, p.Name, err)
	}
	a.dispatcher.gpu.Store(&gpuOffer{profile: p.Name, cdiKind: p.GPU.CDIKind, units: p.GPU.Units, lock: lock})
	a.holdForeignUnits(p.Name, foreign)
	a.profiles.clearUnavailable(p.Name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GPU_HOST_LOCK_HELD)
	return nil
}

// holdForeignUnits holds the profile unavailable (FOREIGN_UNITS_HELD) while a
// container bound under another lock may still hold one of its units, and
// ends that once none does.
func (a *Activities) holdForeignUnits(profile string, foreign []UnitContainer) {
	const held = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_FOREIGN_UNITS_HELD
	if len(foreign) == 0 {
		a.profiles.clearUnavailable(profile, held)
		return
	}
	a.profiles.setUnavailable(profile, held, foreignDetail(foreign), time.Now())
}

// recheckForeignUnits lists the unit containers again while the profile
// holds FOREIGN_UNITS_HELD, and ends the hold once none of another lock
// still runs. A list that fails keeps the hold.
func (a *Activities) recheckForeignUnits(ctx context.Context, offer *gpuOffer) {
	const held = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_FOREIGN_UNITS_HELD
	if !a.profiles.unavailableFor(offer.profile, held) {
		return
	}
	lister, ok := a.dispatcher.Runtime.(unitContainerLister)
	if !ok {
		return
	}
	listed, err := lister.UnitContainers(ctx)
	if err != nil {
		return
	}
	var foreign []UnitContainer
	for _, c := range listed {
		if c.Lock != offer.lock.identity() && foreignUnits(c, offer.profile) {
			foreign = append(foreign, c)
		}
	}
	a.holdForeignUnits(offer.profile, foreign)
}

// probeUnitsOnce is one pass of the unit probes (design §4.7.7): every unit
// free of a binding is probed, concurrently, and its health flipped under the
// ledger's mutex. It probes nothing before the lock is held and its reap has
// run, nothing under drift, and nothing once the Orker is stopping; a probe
// the stop cuts off judges nothing.
func (a *Activities) probeUnitsOnce(ctx context.Context, root string) {
	a.probeUnits(ctx, root, false)
}

// runUnitRecovery re-probes each unit outside the usable set every
// recovery_interval (design §4.7.7), so a unit that recovers is back after
// recoveryPasses good probes rather than as many audit intervals.
func (a *Activities) runUnitRecovery(ctx context.Context, root string, every time.Duration) {
	if every <= 0 {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.probeUnits(ctx, root, true)
		}
	}
}

// probeImageReasons are the two reasons the probe image's check sets.
var probeImageReasons = []orkstackv1.ProfileCordonReason{
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE,
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH,
}

// probeUnits is probeUnitsOnce, or, recovering, only for the units outside
// the usable set.
func (a *Activities) probeUnits(ctx context.Context, root string, recovering bool) {
	offer := a.dispatcher.gpu.Load()
	if offer == nil || a.capacity == nil || a.profiles.drifted(offer.profile) {
		return
	}
	a.recheckForeignUnits(ctx, offer)
	if a.cordon != nil {
		if _, stopping := a.cordon.Stopping(); stopping {
			return
		}
	}
	rt, ok := a.dispatcher.Runtime.(hostViewRuntime)
	if !ok {
		return
	}
	// A recovery pass with every unit usable, and the image not what holds
	// the profile, has nothing to probe, so it asks no registry or daemon.
	if recovering && !a.profiles.unavailableFor(offer.profile, probeImageReasons...) {
		needed := false
		for i := range offer.units {
			needed = needed || !a.capacity.unitUsable(i)
		}
		if !needed {
			return
		}
	}
	// The probe image first, as every probe's is: pulled, and its build held
	// to this Orker's own, so a probe of another build never judges a unit.
	// Its two reasons are this check's alone, set and cleared here.
	imageReasons := probeImageReasons
	image, reason, detail, err := checkProbeImage(ctx, rt, a.profiles.probeSource(), a.profiles.build)
	if err != nil {
		return
	}
	if reason != 0 {
		a.profiles.clearUnavailable(offer.profile, imageReasons...)
		a.profiles.setUnavailable(offer.profile, reason, detail, time.Now())
		return
	}
	a.profiles.clearUnavailable(offer.profile, imageReasons...)
	audit, _ := a.profiles.audit(offer.profile)
	var wg sync.WaitGroup
	for i := range offer.units {
		if i >= len(audit.unitNodes) {
			break
		}
		if recovering && a.capacity.unitUsable(i) {
			continue
		}
		wg.Go(func() {
			probe, err := a.capacity.probeUnit(ctx, i)
			if err != nil {
				return
			}
			healthy, detail, err := a.dispatcher.probeGPUUnit(ctx, rt, root, image, offer, i, audit.unitNodes[i])
			if errors.Is(err, errHostLockMismatch) {
				a.capacity.abandonProbe(probe)
				a.profiles.setDrift(offer.profile, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOST_LOCK_MISMATCH,
					detail, time.Now())
				a.dispatcher.logger().Error("a unit probe sees another GPU host lock directory than this Orker holds; "+
					"the gpu profile is out of service until the Orker restarts", "profile", offer.profile, "detail", detail)
				return
			}
			if err != nil {
				a.capacity.abandonProbe(probe)
				return
			}
			if !healthy {
				a.dispatcher.logger().Warn("a gpu unit failed its probe", "profile", offer.profile, "unit", i, "detail", detail)
			}
			a.capacity.endProbe(probe, healthy)
		})
	}
	wg.Wait()
}
