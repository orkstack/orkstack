package orker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
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

// holdUntilRemoved keeps a probe's unit with it until every container the
// probe left is confirmed gone: an engine that could not remove one may still
// start it, or it may still run, and a unit is never held by two. The removal
// is asked again every recovery_interval and logged every minute it is
// refused. The Orker's stop ends the asking and keeps the unit held: a task
// its open polls still deliver must not be bound it either.
func (a *Activities) holdUntilRemoved(ctx context.Context, profile string, probe *unitProbe, leftovers []ProbeLeftover) {
	log := a.dispatcher.logger().With("profile", profile, "unit", probe.index)
	log.Warn("a gpu unit is held until its probe's container is gone", "containers", leftoverTexts(leftovers))
	every := a.profiles.declared.RecoveryInterval
	if every <= 0 {
		every = DefaultProfileRecoveryInterval
	}
	a.unitEngine.hold(probe.index, &heldUnit{probe: probe, left: strings.Join(leftoverTexts(leftovers), "; ")})
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		said := time.Now()
		for len(leftovers) > 0 {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			leftovers = slices.DeleteFunc(leftovers, func(l ProbeLeftover) bool {
				err := a.dispatcher.Runtime.Remove(ctx, l.Container)
				if err != nil && time.Since(said) >= time.Minute {
					said = time.Now()
					log.Warn("a gpu unit is still held: its probe's container could not be removed", "container", l.Container, "error", err)
				}
				return err == nil
			})
		}
		a.unitEngine.hold(probe.index, nil)
		a.capacity.abandonProbe(probe)
	}()
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
	// One pass at a time, or the audit's and the recovery's would both count one
	// silence. A recovery pass that finds one running has its next tick; the
	// audit's waits, because slow recovery passes follow each other without a
	// gap and probe only the units that are out.
	if recovering {
		if !a.unitEngine.passing.TryLock() {
			return
		}
	} else {
		a.unitEngine.passing.Lock()
	}
	defer a.unitEngine.passing.Unlock()
	if ctx.Err() != nil {
		return
	}
	offer := a.dispatcher.gpu.Load()
	if offer == nil || a.capacity == nil || a.profiles.drifted(offer.profile) {
		return
	}
	a.recheckForeignUnits(ctx, offer)
	if a.stopping() {
		return
	}
	rt, ok := a.dispatcher.Runtime.(hostViewRuntime)
	if !ok {
		return
	}
	// A recovery pass with every unit usable, and neither the image nor the
	// engine what holds the profile, has nothing to probe, so it asks no
	// registry or daemon.
	engineHeld := a.profiles.unavailableFor(offer.profile, probeNotStarted)
	if recovering && !engineHeld && !a.profiles.unavailableFor(offer.profile, probeImageReasons...) {
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
	asked := &silenceRecorder{hostViewRuntime: rt}
	image, reason, detail, err := checkProbeImage(ctx, asked, a.profiles.probeSource(), a.profiles.build)
	if err != nil {
		return
	}
	if asked.silent && reason == orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE {
		// The engine's silence, which is not the image's absence: the pass
		// started no probe, and the image's reasons stay what they were.
		var pass probePass
		pass.notStarted(detail)
		a.notePass(offer.profile, &pass)
		return
	}
	if reason != 0 {
		a.unitEngine.answered()
		a.profiles.clearUnavailable(offer.profile, imageReasons...)
		a.profiles.setUnavailable(offer.profile, reason, detail, time.Now())
		return
	}
	a.profiles.clearUnavailable(offer.profile, imageReasons...)
	audit, _ := a.profiles.audit(offer.profile)
	var pass probePass
	var wg sync.WaitGroup
	for i := range offer.units {
		if i >= len(audit.unitNodes) {
			break
		}
		if recovering && !engineHeld && a.capacity.unitUsable(i) {
			continue
		}
		wg.Go(func() {
			probe, err := a.capacity.probeUnit(ctx, i)
			if err != nil {
				if h := a.unitEngine.holds(i); h != nil && a.capacity.unitUsable(i) {
					pass.notStarted(fmt.Sprintf("unit %d is held until its probe's container is gone: %s", i, h.left))
					pass.silent(h.probe, "its probe's container cannot be removed: "+h.left, nil)
				}
				return
			}
			// Asked again here: the image check and the wait for a count can each
			// take long enough for a stop to begin.
			if a.stopping() {
				a.capacity.abandonProbe(probe)
				return
			}
			out, err := a.dispatcher.probeGPUUnit(ctx, rt, root, image, offer, i, audit.unitNodes[i])
			if errors.Is(err, errHostLockMismatch) {
				pass.started(i)
				if len(out.leftovers) > 0 {
					a.holdUntilRemoved(ctx, offer.profile, probe, out.leftovers)
				} else {
					a.capacity.abandonProbe(probe)
				}
				a.profiles.setDrift(offer.profile, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOST_LOCK_MISMATCH,
					out.detail, time.Now())
				a.dispatcher.logger().Error("a unit probe sees another GPU host lock directory than this Orker holds; "+
					"the gpu profile is out of service until the Orker restarts", "profile", offer.profile, "detail", out.detail)
				return
			}
			if errors.Is(err, errProbeImageGone) {
				a.profiles.setUnavailable(offer.profile, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE,
					out.detail, time.Now())
			}
			if err != nil {
				a.capacity.abandonProbe(probe)
				return
			}
			switch {
			case out.verdict != unitNoVerdict:
				pass.started(i)
			case a.capacity.unitUsable(i):
				// A unit already out of service says nothing more of the engine:
				// the recovery passes probe it alone, and would charge it.
				pass.notStarted(out.detail)
			}
			release := func() {
				if len(out.leftovers) == 0 {
					a.capacity.abandonProbe(probe)
					return
				}
				a.holdUntilRemoved(ctx, offer.profile, probe, out.leftovers)
			}
			switch out.verdict {
			case unitNoVerdict:
				// Kept with its probe until the pass ends and knows whether
				// another unit's probe started.
				a.dispatcher.logger().Warn("a gpu unit's probe was never started, which by itself says nothing of the unit",
					"profile", offer.profile, "unit", i, "detail", out.detail)
				pass.silent(probe, out.detail, release)
				return
			case unitIsUnhealthy:
				a.dispatcher.logger().Warn("a gpu unit failed its probe", "profile", offer.profile, "unit", i, "detail", out.detail)
				a.capacity.settleProbe(probe, false)
			case unitIsHealthy:
				a.capacity.settleProbe(probe, true)
			}
			release()
		})
	}
	wg.Wait()
	if ctx.Err() == nil {
		a.notePass(offer.profile, &pass)
		for _, u := range a.unitEngine.alone(&pass, a.capacity.unitUsable) {
			a.dispatcher.logger().Warn("a gpu unit is out of service: the container engine started other units' probes in the same passes and not its own",
				"profile", offer.profile, "unit", u.probe.index, "passes", notStartedPasses, "detail", u.why)
			a.capacity.settleProbe(u.probe, false)
		}
	}
	for _, u := range pass.silents {
		if u.release != nil {
			u.release()
		}
	}
}

// silenceRecorder is a runtime that notes whether the engine left an image
// question unanswered.
type silenceRecorder struct {
	hostViewRuntime
	silent bool
}

func (r *silenceRecorder) Pull(ctx context.Context, pinned string) (PullInfo, error) {
	info, err := r.hostViewRuntime.Pull(ctx, pinned)
	r.silent = r.silent || errors.Is(err, errEngineUnreachable)
	return info, err
}

func (r *silenceRecorder) ImageLabel(ctx context.Context, ref, key string) (string, bool, error) {
	value, ok, err := r.hostViewRuntime.ImageLabel(ctx, ref, key)
	r.silent = r.silent || errors.Is(err, errEngineUnreachable)
	return value, ok, err
}

// probeNotStarted is the reason the passes of unit probes set and clear.
const probeNotStarted = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_NOT_STARTED

// notStartedPasses is how many passes in a row the engine starts no probe
// before the profile is unavailable: a daemon that starts no probe starts no
// task either, and one slow create is not that.
const notStartedPasses = 3

// probePass is what one pass of the unit probes found of the engine.
type probePass struct {
	mu          sync.Mutex
	anyStarted  bool
	unstartedBy string // why a probe was not started, the last one's; "" when none
	silents     []silentUnit
	// startedUnits are the units whose probe the engine started.
	startedUnits []int
}

// silentUnit is a unit whose probe did not start in a pass, or that was held
// throughout it. The probe keeps the unit until release, which is nil for one
// held beyond the pass.
type silentUnit struct {
	probe   *unitProbe
	why     string
	release func()
}

func (p *probePass) silent(probe *unitProbe, why string, release func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.silents = append(p.silents, silentUnit{probe: probe, why: why, release: release})
}

func (p *probePass) started(unit int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.anyStarted = true
	p.startedUnits = append(p.startedUnits, unit)
}

func (p *probePass) notStarted(why string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unstartedBy = why
}

// heldUnit is a unit its probe keeps until the probe's containers are gone.
type heldUnit struct {
	probe *unitProbe
	left  string // the containers, and why each could not be removed
}

// unitEngine is what the unit probes know of the engine across passes: how
// many in a row started nothing, and the units held for a container it has
// not been seen to remove.
type unitEngine struct {
	passing sync.Mutex // held by the one pass that runs

	mu     sync.Mutex
	passes int
	held   map[int]*heldUnit
	// aloneFor counts, by unit, the passes in a row in which its probe did not
	// start and another unit's did.
	aloneFor map[int]int
}

// alone counts pass and is the units in service that the passes in a row put
// at fault: each was silent in notStartedPasses of those that probed it while
// another unit's probe started, so the engine answers. A pass in which a
// unit's probe started, or which probed it and in which no probe started,
// begins its count again; one that did not probe it leaves its count alone.
func (e *unitEngine) alone(p *probePass, usable func(unit int) bool) []silentUnit {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.aloneFor == nil {
		e.aloneFor = map[int]int{}
	}
	for _, unit := range p.startedUnits {
		delete(e.aloneFor, unit)
	}
	var fault []silentUnit
	for _, u := range p.silents {
		unit := u.probe.index
		if !p.anyStarted || !usable(unit) {
			delete(e.aloneFor, unit)
			continue
		}
		e.aloneFor[unit]++
		if e.aloneFor[unit] >= notStartedPasses {
			fault = append(fault, u)
		}
	}
	return fault
}

// hold records what unit is held for, nil for nothing.
func (e *unitEngine) hold(unit int, h *heldUnit) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.held == nil {
		e.held = map[int]*heldUnit{}
	}
	e.held[unit] = h
}

// holds is what unit is held for, or nil.
func (e *unitEngine) holds(unit int) *heldUnit {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.held[unit]
}

// answered ends the run of passes the engine started nothing in.
func (e *unitEngine) answered() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.passes = 0
}

// stopping says whether the Orker is stopping.
func (a *Activities) stopping() bool {
	if a.cordon == nil {
		return false
	}
	_, stopping := a.cordon.Stopping()
	return stopping
}

// notePass counts a pass: one probe the engine started ends the run of passes
// and the reason; a pass that started none counts; one that probed nothing,
// every unit being bound, counts neither way.
func (a *Activities) notePass(profile string, p *probePass) {
	e := &a.unitEngine
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case p.anyStarted:
		e.passes = 0
		a.profiles.clearUnavailable(profile, probeNotStarted)
	case p.unstartedBy != "":
		e.passes++
		if e.passes >= notStartedPasses {
			a.profiles.setUnavailable(profile, probeNotStarted, fmt.Sprintf(
				"the container engine started no unit probe in %d passes in a row: %s", e.passes, p.unstartedBy), time.Now())
		}
	}
}
