package orker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"time"
)

// The unit probe (design §4.7.7): `orker probe unit NODE...` runs inside a
// probe container that holds one gpu unit, and reports each of the unit's
// nodes as the container sees it, with what its driver answers to a version
// ioctl (DRM_IOCTL_VERSION for a render node, AMDKFD_IOC_GET_VERSION for
// kfd). It runs no compute and never wakes a runtime-suspended GPU (design
// Q7). A unit is healthy only when every node is the one the Orker audited
// and every driver answered.

var renderNodeName = regexp.MustCompile(`^renderD[0-9]+$`)

// gpuLockViewPath is where a unit probe container sees the GPU host lock
// directory, bound read-only: its dev:inode must be the held lock's (design
// §4.7.5, the host-native rule for gpu).
const gpuLockViewPath = "/orkstack/gpu-lock"

// probeUnitNodes is `orker probe unit`.
func probeUnitNodes(paths []string, stdout, stderr io.Writer) int {
	if len(paths) == 0 || len(paths) > maxProbePaths {
		fmt.Fprintf(stderr, "usage: orker probe unit NODE... (1 to %d render nodes or /dev/kfd)\n", maxProbePaths)
		return 2
	}
	for _, p := range paths {
		if !path.IsAbs(p) || path.Clean(p) != p ||
			(p != "/dev/kfd" && p != gpuLockViewPath && !renderNodeName.MatchString(path.Base(p))) {
			fmt.Fprintf(stderr, "orker probe unit: %q is not a render node, /dev/kfd or %s\n", p, gpuLockViewPath)
			return 2
		}
	}
	// Before any device is opened: an Orker that gets this line and no report
	// knows the engine did start the probe, and that the silence is the unit's.
	if _, err := io.WriteString(stdout, unitProbeStarted); err != nil {
		fmt.Fprintf(stderr, "orker probe unit: %v\n", err)
		return 1
	}
	view := unitViewsWithin(paths, unitDriverTimeout, func(p string) viewEntry {
		if p == gpuLockViewPath {
			return statView(p)
		}
		return unitView(p, path.Base(p))
	})
	if err := writeProbeView(stdout, view); err != nil {
		fmt.Fprintf(stderr, "orker probe unit: %v\n", err)
		return 1
	}
	return 0
}

// unitProbeStarted is the first line `orker probe unit` writes; its report,
// one JSON object as every probe's is, follows it.
const unitProbeStarted = "orker probe unit: started\n"

// unitDriverTimeout bounds a unit's driver answer (design §4.7.7): a driver
// answers a version ioctl in well under a millisecond (design Q7). It is the
// probe process's own bound, so that the container engine's create and start
// are no part of it.
const unitDriverTimeout = 5 * time.Second

// unitViewsWithin is ask of every path, at the same time, each answer bounded
// by within: one that does not come is reported as its node's error. Its ask
// is left behind, since an ioctl the kernel does not return from cannot be
// given up; the process's end is what ends it.
func unitViewsWithin(paths []string, within time.Duration, ask func(string) viewEntry) []viewEntry {
	answers := make([]chan viewEntry, len(paths))
	for i, p := range paths {
		answers[i] = make(chan viewEntry, 1)
		go func() { answers[i] <- ask(p) }()
	}
	bound := time.NewTimer(within)
	defer bound.Stop()
	spent := false
	view := make([]viewEntry, 0, len(paths))
	for i, p := range paths {
		if !spent {
			select {
			case v := <-answers[i]:
				view = append(view, v)
				continue
			case <-bound.C:
				spent = true
			}
		}
		select {
		case v := <-answers[i]:
			view = append(view, v)
		default:
			view = append(view, viewEntry{Path: p, Error: "the driver did not answer within " + within.String()})
		}
	}
	return view
}

// unitView is one node's view, with its driver's answer, or an error.
func unitView(p, base string) viewEntry {
	v := statView(p)
	if v.Error != "" {
		return v
	}
	driver, err := askDriver(p, base == "kfd")
	if err != nil {
		return viewEntry{Path: p, Error: "the driver did not answer its version ioctl: " + err.Error()}
	}
	v.Driver = driver
	return v
}

// unitHealthy says whether a unit probe's view is healthy: every node the one
// the Orker audited (judgeHostView), and every driver answered.
func unitHealthy(want []viewWant, probed []viewEntry) error {
	if err := judgeHostView(want, probed); err != nil {
		return err
	}
	var silent []string
	for _, p := range probed {
		if p.Kind == viewChar && p.Driver == "" {
			silent = append(silent, p.Path)
		}
	}
	if len(silent) > 0 {
		return errors.New("no driver answered for " + strings.Join(silent, ", "))
	}
	return nil
}

// unitProbeRunTimeout bounds a unit probe's container, its create, start and
// end: what the engine takes. Removing one that overran takes its own time
// beyond it. The driver's own answer has unitDriverTimeout, inside it.
const unitProbeRunTimeout = 60 * time.Second

// unitVerdict is what one probe says of its unit.
type unitVerdict int

const (
	// unitNoVerdict: the engine never started the probe, so it says nothing of
	// the unit, whose state stays what it was.
	unitNoVerdict unitVerdict = iota
	unitIsHealthy
	unitIsUnhealthy
)

// unitProbeOutcome is one probe's verdict, why it is not healthy, and the
// containers it left that could not be removed: each may still hold the unit.
type unitProbeOutcome struct {
	verdict   unitVerdict
	detail    string
	leftovers []ProbeLeftover
}

// probeGPUUnit runs one unit's probe container: the unit's CDI device, the
// labels the lock-scoped reap finds it by, `orker probe unit` over its nodes.
// Only the context's own end, a lock that is not the held one, and an image
// that is gone are errors.
// A probe that reported judges the unit, whether or not its container ended;
// one that started and reported nothing is an unhealthy unit, as is one the
// engine answered and refused (a device that is gone); one the engine never
// started gives no verdict. The caller holds the unit (Capacity.probeUnit) and
// checked the image's build.
func (d *OCIDispatcher) probeGPUUnit(ctx context.Context, rt bootProber, root, image string, offer *gpuOffer,
	index int, nodes []auditedDevice,
) (unitProbeOutcome, error) {
	var leftovers []ProbeLeftover
	unhealthy := func(format string, args ...any) (unitProbeOutcome, error) {
		return unitProbeOutcome{verdict: unitIsUnhealthy, detail: fmt.Sprintf(format, args...), leftovers: leftovers}, nil
	}
	dir, err := os.MkdirTemp(root, "ork-unitprobe-*")
	if err != nil {
		return unhealthy("the unit probe's directory could not be created under %s: %v", root, err)
	}
	defer func() {
		if err := removeSealedTree(dir); err != nil {
			d.logger().Warn("unit probe: its directory could not be removed and is left on disk", "path", dir, "error", err)
		}
	}()
	lockPath, lockDev, lockIno := offer.lock.view()
	args := []string{"probe", "unit"}
	want := make([]viewWant, 0, len(nodes))
	for _, n := range nodes {
		args = append(args, n.ContainerPath)
		want = append(want, viewWant{Check: checkCDINode, Own: viewEntry{
			Path: n.ContainerPath, Kind: viewChar, Major: n.Host.Major, Minor: n.Host.Minor,
		}})
	}
	args = append(args, gpuLockViewPath)
	spec, err := d.probeSpec(dir, image, args, nil)
	if err != nil {
		return unhealthy("the unit probe could not be set up: %v", err)
	}
	spec.Mounts = append(spec.Mounts, Mount{Source: lockPath, Destination: gpuLockViewPath, ReadOnly: true})
	spec.CDIDevices = []string{offer.cdiKind + "=" + offer.units[index]}
	spec.Labels[labelUnitsLock] = offer.lock.identity()
	spec.Labels[labelUnits] = unitsLabel(offer.profile, []int{index})
	if d.OrkerID != "" {
		spec.Labels["ork.probe"] = d.OrkerID
	}
	call, cancel := context.WithTimeout(ctx, unitProbeRunTimeout)
	defer cancel()
	run, runErr := rt.RunBootProbe(call, spec)
	if ctx.Err() != nil {
		return unitProbeOutcome{}, fmt.Errorf("the unit probe was interrupted: %w", ctx.Err())
	}
	leftovers = run.Leftovers
	d.warnProbeLeftovers(leftovers)
	report, started := strings.CutPrefix(run.Stdout, unitProbeStarted)
	probed, reportErr := parseProbeView([]byte(report))
	switch {
	case runErr == nil:
		d.removeProbeContainer(ctx, run.Container)
		switch {
		case !started:
			return unhealthy("unit %d's probe did not begin as `orker probe unit` does", index)
		case reportErr != nil:
			return unhealthy("unit %d's probe printed no report the Orker can read: %v", index, reportErr)
		}
	case started && reportErr == nil:
		// It reported; only its container's end never came, or failed.
	case started && call.Err() != nil:
		return unhealthy("unit %d's probe started and gave no report within %s", index, unitProbeRunTimeout)
	case started:
		return unhealthy("unit %d's probe started and gave no report: %v", index, runErr)
	case call.Err() != nil:
		return unitProbeOutcome{verdict: unitNoVerdict, leftovers: leftovers, detail: fmt.Sprintf(
			"the container engine did not start unit %d's probe within %s", index, unitProbeRunTimeout)}, nil
	case errors.Is(runErr, ErrProbeImageUnavailable) && !errors.Is(runErr, errEngineUnreachable):
		return unitProbeOutcome{detail: runErr.Error()}, fmt.Errorf("%w: %w", errProbeImageGone, runErr)
	case engineGaveNoAnswer(runErr):
		return unitProbeOutcome{verdict: unitNoVerdict, leftovers: leftovers, detail: fmt.Sprintf(
			"the container engine did not start unit %d's probe: %v", index, runErr)}, nil
	default:
		return unhealthy("unit %d's probe did not run: %v", index, runErr)
	}
	// The lock as the container sees it, judged apart from the unit: a
	// directory that is not the held one is drift, never an unhealthy unit.
	var nodesSeen []viewEntry
	lockSeen := false
	for _, p := range probed {
		if p.Path != gpuLockViewPath {
			nodesSeen = append(nodesSeen, p)
			continue
		}
		lockSeen = true
		if msg := viewDiff(viewWant{Check: checkBind, Own: viewEntry{Path: gpuLockViewPath, Kind: viewDir, Dev: lockDev, Ino: lockIno}}, p); msg != "" {
			return unitProbeOutcome{verdict: unitIsUnhealthy, detail: msg, leftovers: leftovers}, fmt.Errorf("%w: %s", errHostLockMismatch, msg)
		}
	}
	if !lockSeen {
		return unhealthy("unit %d's probe did not report the GPU host lock directory", index)
	}
	if err := unitHealthy(want, nodesSeen); err != nil {
		return unhealthy("unit %d: %v", index, err)
	}
	return unitProbeOutcome{verdict: unitIsHealthy, leftovers: leftovers}, nil
}

// errProbeImageGone is the engine's definite answer that it has not got the
// probe's image: the image's fault, which the profile says, and no unit's.
var errProbeImageGone = errors.New("the probe image is not there")

// engineGaveNoAnswer says whether a probe's error is the engine's silence
// rather than its answer.
func engineGaveNoAnswer(err error) bool {
	return errors.Is(err, ErrProbeTimedOut) || errors.Is(err, errEngineUnreachable)
}
