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
	view := make([]viewEntry, 0, len(paths))
	for _, p := range paths {
		if p == gpuLockViewPath {
			view = append(view, statView(p))
			continue
		}
		view = append(view, unitView(p, path.Base(p)))
	}
	if err := writeProbeView(stdout, view); err != nil {
		fmt.Fprintf(stderr, "orker probe unit: %v\n", err)
		return 1
	}
	return 0
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

// unitProbeTimeout is a unit probe's budget (design §4.7.7): its driver
// answers a version ioctl in well under a millisecond (design Q7).
const unitProbeTimeout = 5 * time.Second

// probeGPUUnit runs one unit's probe container: the unit's CDI device, the
// labels the lock-scoped reap finds it by, `orker probe unit` over its nodes.
// It reports whether the unit is healthy and, when not, why. Only the
// context's own end is an error; a probe that cannot run, times out, or
// reports what the audit does not hold is an unhealthy unit. The caller holds
// the unit (Capacity.probeUnit) and checked the image's build.
func (d *OCIDispatcher) probeGPUUnit(ctx context.Context, rt bootProber, root, image string, offer *gpuOffer,
	index int, nodes []auditedDevice,
) (bool, string, error) {
	dir, err := os.MkdirTemp(root, "ork-unitprobe-*")
	if err != nil {
		return false, fmt.Sprintf("the unit probe's directory could not be created under %s: %v", root, err), nil
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
		return false, fmt.Sprintf("the unit probe could not be set up: %v", err), nil
	}
	spec.Mounts = append(spec.Mounts, Mount{Source: lockPath, Destination: gpuLockViewPath, ReadOnly: true})
	spec.CDIDevices = []string{offer.cdiKind + "=" + offer.units[index]}
	spec.Labels[labelUnitsLock] = offer.lock.identity()
	spec.Labels[labelUnits] = unitsLabel(offer.profile, []int{index})
	if d.OrkerID != "" {
		spec.Labels["ork.probe"] = d.OrkerID
	}
	call, cancel := context.WithTimeout(ctx, unitProbeTimeout)
	defer cancel()
	run, err := rt.RunBootProbe(call, spec)
	switch {
	case ctx.Err() != nil:
		return false, "", fmt.Errorf("the unit probe was interrupted: %w", ctx.Err())
	case err != nil:
		return false, fmt.Sprintf("unit %d's probe did not run: %v", index, err), nil
	}
	d.warnProbeLeftovers(run.Leftovers)
	d.removeProbeContainer(ctx, run.Container)
	probed, err := parseProbeView([]byte(run.Stdout))
	if err != nil {
		return false, fmt.Sprintf("unit %d's probe printed no report the Orker can read: %v", index, err), nil
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
			return false, msg, fmt.Errorf("%w: %s", errHostLockMismatch, msg)
		}
	}
	if !lockSeen {
		return false, fmt.Sprintf("unit %d's probe did not report the GPU host lock directory", index), nil
	}
	if err := unitHealthy(want, nodesSeen); err != nil {
		return false, fmt.Sprintf("unit %d: %v", index, err), nil
	}
	return true, "", nil
}
