package orker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/buildinfo"
)

// The host-view probe (design §4.12): the probe image, which is the orker
// image, run with the devices the Orker passes and its hidden `orker probe
// view`, so the Orker can compare what a container is given with what it
// audited (host_view.go judges the two).

// LabelBuild is the label the orker image carries: the commit its binary was
// built from (deploy/Dockerfile). The probe image must carry this Orker's own,
// so the `orker probe view` it runs is the one this Orker reads.
const LabelBuild = "org.orkstack.build"

// DetailHostViewMismatch is the ORKER_PROFILE_INVALID detail of a boot whose
// probe container did not see the devices the Orker audited, or whose
// container CLI is not pointed at a daemon on this host.
const DetailHostViewMismatch = "HOST_VIEW_MISMATCH"

// hostViewProbeTaskID names the probe's container (ork-viewprobe-…).
const hostViewProbeTaskID = "viewprobe"

// profileRecoveryChecks is how many good checks in a row, recovery_interval
// apart, end an unavailable cause.
const profileRecoveryChecks = 3

// hostViewRuntime is what the host-view probe needs of a runtime: the boot
// probe's run, a label of a local image, and whether its CLI reaches a daemon
// on this host.
type hostViewRuntime interface {
	bootProber
	// Pull makes the image local, so its label is read before it runs.
	Pull(ctx context.Context, pinned string) (PullInfo, error)
	// ImageLabel reads one label of a local image; ok is false when the
	// image carries none of that name.
	ImageLabel(ctx context.Context, ref, key string) (value string, ok bool, err error)
	// LocalDaemon is nil when nothing in the CLI's environment points it at
	// a daemon other than this host's own unix socket.
	LocalDaemon() error
}

// hostViewFinding is what one probe found. Unavailable is set, with its
// Detail, when the probe could not judge the view; otherwise Differs names
// each profile whose devices the container saw otherwise.
type hostViewFinding struct {
	Unavailable orkstackv1.ProfileCordonReason
	Detail      string
	Differs     map[string]string
}

// probeHostView runs one probe container from image with every device of
// audits and judges what it reports against the audit. build is this
// Orker's own commit, which the image's LabelBuild must equal. Only the
// context's own end is an error: a probe that cannot run, an image that is
// not this build's, and a report that cannot be read are findings that hold
// the profiles unavailable until a later probe judges them.
func (d *OCIDispatcher) probeHostView(ctx context.Context, rt hostViewRuntime, root, image, build string,
	audits map[string]profileAudit,
) (hostViewFinding, error) {
	names := slices.Sorted(maps.Keys(audits))
	var paths []string
	var devices []DeviceMapping
	for _, name := range names {
		for _, dev := range audits[name].devices {
			paths = append(paths, dev.ContainerPath)
			devices = append(devices, DeviceMapping{HostPath: dev.Host.Resolved, ContainerPath: dev.ContainerPath, Permissions: dev.Permissions})
		}
	}
	if len(paths) == 0 {
		return hostViewFinding{}, nil
	}
	unavailable := func(reason orkstackv1.ProfileCordonReason, format string, args ...any) (hostViewFinding, error) {
		return hostViewFinding{Unavailable: reason, Detail: fmt.Sprintf(format, args...)}, nil
	}
	const imageUnavailable = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE

	dir, err := os.MkdirTemp(root, "ork-viewprobe-*")
	if err != nil {
		return unavailable(imageUnavailable, "the probe could not be set up: its directory could not be created under "+
			"the work root %s: %v", root, err)
	}
	defer func() {
		if err := removeSealedTree(dir); err != nil {
			d.logger().Warn("host-view probe: its directory could not be removed and is left on disk", "path", dir, "error", err)
		}
	}()
	spec, err := d.hostViewProbeSpec(dir, image, paths, devices)
	if err != nil {
		return unavailable(imageUnavailable, "the probe could not be set up under the work root %s: %v", root, err)
	}
	// The image is judged before it runs: pulled, and its build label read,
	// so an image of another build never starts with the devices attached.
	if reason, detail, err := checkProbeImage(ctx, rt, image, build); err != nil {
		return hostViewFinding{}, fmt.Errorf("the host-view probe was interrupted: %w", err)
	} else if reason != 0 {
		return hostViewFinding{Unavailable: reason, Detail: detail}, nil
	}

	run, err := rt.RunBootProbe(ctx, spec)
	switch {
	case ctx.Err() != nil:
		return hostViewFinding{}, fmt.Errorf("the host-view probe was interrupted: %w", ctx.Err())
	case err != nil:
		// Every failure to run it is the same finding, named by what was
		// said: nothing was judged, so the profiles wait for a probe that is.
		return unavailable(imageUnavailable, "the probe image %s did not run: %v", image, err)
	}
	d.warnProbeLeftovers(run.Leftovers)
	d.removeProbeContainer(ctx, run.Container)

	probed, err := parseProbeView([]byte(run.Stdout))
	if err != nil {
		return unavailable(imageUnavailable, "the probe image %s printed no report the Orker can read: %v", image, err)
	}
	byPath := make(map[string]viewEntry, len(probed))
	for _, p := range probed {
		if _, dup := byPath[p.Path]; dup {
			return unavailable(imageUnavailable, "the probe image %s reported %s twice", image, p.Path)
		}
		byPath[p.Path] = p
	}
	finding := hostViewFinding{}
	for _, name := range names {
		var want []viewWant
		var mine []viewEntry
		for _, dev := range audits[name].devices {
			want = append(want, viewWant{Check: checkDevice, Own: viewEntry{
				Path: dev.ContainerPath, Kind: viewChar, Major: dev.Host.Major, Minor: dev.Host.Minor, GID: dev.Host.GID,
			}})
			if p, seen := byPath[dev.ContainerPath]; seen {
				mine = append(mine, p)
				delete(byPath, dev.ContainerPath)
			}
		}
		if err := judgeHostView(want, mine); err != nil {
			if finding.Differs == nil {
				finding.Differs = map[string]string{}
			}
			finding.Differs[name] = err.Error()
		}
	}
	if len(byPath) > 0 {
		// A report naming a path that was not asked is not this build's
		// probe: nothing it said is judged.
		return unavailable(imageUnavailable, "the probe image %s reported paths it was not asked about: %s",
			image, strings.Join(slices.Sorted(maps.Keys(byPath)), ", "))
	}
	return finding, nil
}

// hostViewProbeSpec is the probe's container: built by containerSpec as a
// task's is, for a task that requests nothing, runs as the Orker's user on
// no network, and is given exactly the devices the profiles pass. Its
// command is the image's own entrypoint, the orker binary, with `probe view`.
func (d *OCIDispatcher) hostViewProbeSpec(dir, image string, paths []string, devices []DeviceMapping) (ContainerSpec, error) {
	return d.probeSpec(dir, image, append([]string{"probe", "view"}, paths...), devices)
}

// probeSpec is a probe container's spec, for `orker probe` with args, given
// devices.
func (d *OCIDispatcher) probeSpec(dir, image string, args []string, devices []DeviceMapping) (ContainerSpec, error) {
	ws, err := NewWorkspace(dir)
	if err != nil {
		return ContainerSpec{}, fmt.Errorf("the probe's workspace: %w", err)
	}
	if err := ws.Seal(); err != nil {
		return ContainerSpec{}, fmt.Errorf("seal the probe's input: %w", err)
	}
	if err := ws.SealSecrets(); err != nil {
		return ContainerSpec{}, fmt.Errorf("seal the probe's secrets: %w", err)
	}
	cpu, memory, failure := d.Policy.ResolveResources(nil)
	if failure != nil {
		return ContainerSpec{}, fmt.Errorf("the probe's resources: %s", failure.Message)
	}
	in := &orkstackv1.ExecuteTaskInput{TaskId: hostViewProbeTaskID, Command: args}
	spec, failure := d.containerSpec(in, ws, &orkstackv1.ImageRef{Pinned: image}, 1, NetworkNone, cpu, memory, true)
	if failure != nil {
		return ContainerSpec{}, errors.New(failure.Message)
	}
	spec.Devices = devices
	// Labelled as the boot probe's are, so a container this Orker could not
	// remove is reaped by the next boot under its identity.
	spec.Labels = map[string]string{"ork.task": hostViewProbeTaskID}
	if d.OrkerID != "" {
		spec.Labels["ork.orker"] = d.OrkerID
	}
	return spec, nil
}

// probeRoot is where the Orker's probes make their directories: its work
// root, as Boot resolves it.
func (a *Activities) probeRoot() string {
	if a.dispatcher.WorkRoot != "" {
		return a.dispatcher.WorkRoot
	}
	return os.TempDir()
}

// bootHostView is the host-native rule at boot: the CLI must reach this
// host's own daemon, and a container must see the audited devices. A view
// that differs refuses the boot; a probe that could not judge it holds every
// profile unavailable, rechecked every recovery_interval (RunProfileAudit).
func (a *Activities) bootHostView(ctx context.Context, root string) error {
	s := a.profiles
	audits := s.hostViewAudits()
	if len(audits) == 0 {
		return nil
	}
	rt, ok := a.dispatcher.Runtime.(hostViewRuntime)
	if !ok {
		return fmt.Errorf("%s (%s): this Orker's container runtime cannot run the host-view probe, "+
			"so no device it passes can be shown to be the one it audited", ReasonProfileInvalid, DetailHostViewMismatch)
	}
	if err := rt.LocalDaemon(); err != nil {
		return fmt.Errorf("%s (%s): device profiles need an Orker on its container daemon's own host: %w",
			ReasonProfileInvalid, DetailHostViewMismatch, err)
	}
	finding, err := a.dispatcher.probeHostView(ctx, rt, root, s.declared.ProbeImage, s.build, audits)
	if err != nil {
		return err
	}
	if finding.Unavailable != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_UNSPECIFIED {
		a.holdUnavailable(audits, finding, time.Now())
		return nil
	}
	if names := slices.Sorted(maps.Keys(finding.Differs)); len(names) > 0 {
		return fmt.Errorf("%s: profile %s (%s): %s; device profiles need an Orker running natively on its "+
			"container daemon's host (docs/DEPLOY.md, \"Running a device Orker\")",
			ReasonProfileInvalid, names[0], DetailHostViewMismatch, finding.Differs[names[0]])
	}
	return nil
}

// hostViewOnce is the host-view probe of one periodic audit or recovery
// check, over present: the profiles whose devices this check found as
// audited. A view that differs is a drift (HOST_VIEW_CHANGED); a probe that
// could not judge it holds them unavailable. good counts, per profile held
// unavailable, the good checks in a row before this one: a profile whose
// devices and view pass profileRecoveryChecks of them is available again. A
// profile not in present starts its count again.
func (a *Activities) hostViewOnce(ctx context.Context, root string, now time.Time, present []string, good map[string]int) {
	s := a.profiles
	all := s.hostViewAudits()
	for name := range good {
		if !slices.Contains(present, name) {
			delete(good, name)
		}
	}
	audits := map[string]profileAudit{}
	for _, name := range present {
		if audit, ok := all[name]; ok {
			audits[name] = audit
		}
	}
	rt, ok := a.dispatcher.Runtime.(hostViewRuntime)
	if len(audits) == 0 || !ok {
		return
	}
	finding, err := a.dispatcher.probeHostView(ctx, rt, root, s.declared.ProbeImage, s.build, audits)
	if err != nil {
		return
	}
	if finding.Unavailable != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_UNSPECIFIED {
		a.holdUnavailable(audits, finding, now)
		clear(good)
		return
	}
	for _, name := range slices.Sorted(maps.Keys(finding.Differs)) {
		s.setDrift(name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOST_VIEW_CHANGED, finding.Differs[name], now)
		delete(good, name)
		a.dispatcher.logger().Error("a device profile drifted: a container no longer sees the devices this Orker audited; "+
			"it is out of service on this Orker until it restarts",
			"profile", name, "reason", ReasonProfileDrifted, "detail", finding.Differs[name])
	}
	for _, name := range slices.Sorted(maps.Keys(audits)) {
		if !s.unavailableFor(name, deviceRecoveryReasons...) {
			delete(good, name)
			continue
		}
		good[name]++
		if good[name] >= profileRecoveryChecks {
			s.clearUnavailable(name, deviceRecoveryReasons...)
			delete(good, name)
			a.dispatcher.logger().Info("a device profile is available again", "profile", name)
		}
	}
}

// deviceRecoveryReasons are the unavailable reasons this check owns and
// clears: a device that was not there and a probe that could not judge the
// view. A reason another check sets (the GPU host lock, a foreign unit) is
// that check's to clear.
var deviceRecoveryReasons = []orkstackv1.ProfileCordonReason{
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_ABSENT,
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE,
	orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH,
}

// holdUnavailable marks every profile of audits unavailable with finding's
// reason, logging the ones that were not already.
func (a *Activities) holdUnavailable(audits map[string]profileAudit, finding hostViewFinding, now time.Time) {
	s := a.profiles
	for _, name := range slices.Sorted(maps.Keys(audits)) {
		if !s.unavailableFor(name, finding.Unavailable) && !s.drifted(name) {
			a.dispatcher.logger().Warn("a device profile is unavailable: the host-view probe could not judge it; "+
				"it is checked again every recovery_interval",
				"profile", name, "reason", finding.Unavailable.String(), "detail", finding.Detail)
		}
		s.setUnavailable(name, finding.Unavailable, finding.Detail, now)
	}
}

// checkProbeImage pulls the probe image and holds its build label to this
// Orker's own commit, for every probe that carries devices or a gpu unit. A
// reason other than 0 is the finding that holds a profile unavailable
// (PROBE_IMAGE_UNAVAILABLE or PROBE_IMAGE_MISMATCH), with its detail; only
// the context's own end is an error.
func checkProbeImage(ctx context.Context, rt hostViewRuntime, image, build string) (orkstackv1.ProfileCordonReason, string, error) {
	const imageUnavailable = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE
	const imageMismatch = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH
	if _, err := rt.Pull(ctx, image); err != nil {
		if ctx.Err() != nil {
			return 0, "", ctx.Err()
		}
		return imageUnavailable, fmt.Sprintf("the probe image %s could not be pulled: %v", image, err), nil
	}
	label, ok, err := rt.ImageLabel(ctx, image, LabelBuild)
	switch {
	case ctx.Err() != nil:
		return 0, "", ctx.Err()
	case err != nil:
		return imageUnavailable, fmt.Sprintf("the probe image %s could not be inspected: %v", image, err), nil
	case build == "" || build == buildinfo.Unknown:
		return imageMismatch, "this Orker does not know which commit built it, so no probe image " +
			"can be shown to be its own; run an Orker built from a commit (`orker --version` names it)", nil
	case !ok:
		return imageMismatch, fmt.Sprintf("the probe image %s has no %s label; profiles.probe_image must be "+
			"the orker image of this Orker's own build (%s)", image, LabelBuild, build), nil
	case label != build:
		return imageMismatch, fmt.Sprintf("the probe image %s was built from commit %q and this Orker from %q; "+
			"profiles.probe_image must be the orker image of this Orker's own build", image, label, build), nil
	}
	return 0, "", nil
}
