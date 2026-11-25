package orker

import (
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"

	cdispec "tags.cncf.io/container-device-interface/specs-go"
)

// The env and mount edits an NVIDIA CDI spec may carry (design E91 §4.10
// items 5 and 7, with decisions Q4d to Q4f), and the audit of its hooks'
// binary. Like the hook grammar (cdi_hooks.go) it is a closed set compiled
// into the Orker: it admits what nvidia-ctk writes for a compute host and
// nothing else.

const (
	// nvidiaVoidSource and nvidiaVoidTarget are NVIDIA's placeholder mount:
	// /dev/null over the legacy device list, so the image's own
	// NVIDIA_VISIBLE_DEVICES selects nothing (decision Q4d). It is the one
	// device node a mount may have as its source, and it is exact.
	nvidiaVoidSource = "/dev/null"
	nvidiaVoidTarget = "/var/run/nvidia-container-devices/void"
)

// nvidiaHostRoots are the directories a mount's source, after every link, may
// lie under: the design's compiled roots with /usr/sbin and /sbin (Q4f,
// regular files only, as every source is, for the driver's own tools).
var nvidiaHostRoots = []string{
	"/usr/lib", "/usr/lib64", "/usr/bin", "/usr/sbin", "/sbin", "/lib/firmware",
	"/usr/share/vulkan", "/usr/share/glvnd", "/usr/share/egl", "/usr/share/nvidia", "/etc/vulkan",
}

// nvidiaContainerRoots are where a mount may land in the container: the host
// roots, and /lib, /lib64 and /bin, which the generator writes where the host
// is not merged-/usr.
var nvidiaContainerRoots = append(slices.Clone(nvidiaHostRoots), "/lib", "/lib64", "/bin")

// mountOptions: a mount is read-only, nosuid and nodev, and may say how it is
// bound. Nothing else.
var (
	mountRequired = []string{"ro", "nosuid", "nodev"}
	mountAllowed  = []string{"ro", "nosuid", "nodev", "bind", "rbind", "rprivate"}
)

var pathSegment = regexp.MustCompile(`^[A-Za-z0-9._+-]+$`)

// normalPath reports whether p is a clean absolute path of plain names: no
// "..", no empty or trailing segment, and no character a runtime's mount
// argument or a shell reads specially (":" and "," included).
func normalPath(p string) bool {
	if !path.IsAbs(p) || path.Clean(p) != p {
		return false
	}
	if p == "/" {
		return true
	}
	for _, seg := range strings.Split(p[1:], "/") {
		if !pathSegment.MatchString(seg) {
			return false
		}
	}
	return true
}

// checkNVIDIAEnv holds an env edit to the two entries nvidia-ctk writes at
// spec level: the placeholder for the legacy device list, and the library
// directories its hooks' links are made in. Every other name is refused,
// LD_*, PATH, ORK_* and OTEL_* among them, and so is a repeat.
func checkNVIDIAEnv(env []string) error {
	seen := map[string]bool{}
	for _, entry := range env {
		name, value, ok := strings.Cut(entry, "=")
		if !ok {
			return refuse(DetailCDIEditRefused, "env %q is not NAME=VALUE", entry)
		}
		if seen[name] {
			return refuse(DetailCDIEditRefused, "env %s is set twice", name)
		}
		seen[name] = true
		switch name {
		case "NVIDIA_VISIBLE_DEVICES":
			if value != "void" {
				return refuse(DetailCDIEditRefused, "NVIDIA_VISIBLE_DEVICES must be void, not %q: a spec may not select devices through the legacy list", value)
			}
		case "NVIDIA_CTK_LIBCUDA_DIR":
			for _, dir := range strings.Split(value, ":") {
				if !normalPath(dir) || !underAny(dir, cdiLibRoots) {
					return refuse(DetailCDIEditRefused, "NVIDIA_CTK_LIBCUDA_DIR names %q, which is not a library directory (%s)", dir, strings.Join(cdiLibRoots, ", "))
				}
			}
		default:
			return refuse(DetailCDIEditRefused, "env %s is not one an NVIDIA spec may set (NVIDIA_VISIBLE_DEVICES=void and NVIDIA_CTK_LIBCUDA_DIR only)", name)
		}
	}
	return nil
}

// mountShape holds a mount's options, type and paths to the grammar and says
// whether it is the void placeholder. Nothing here looks at the host.
func mountShape(m *cdispec.Mount) (void bool, err error) {
	if m == nil {
		return false, refuse(DetailCDIEditRefused, "an empty mount")
	}
	if m.Type != "" && m.Type != "bind" {
		return false, refuse(DetailCDIEditRefused, "mount %s: type %q is refused; a mount is a bind mount", m.ContainerPath, m.Type)
	}
	for _, want := range mountRequired {
		if !slices.Contains(m.Options, want) {
			return false, refuse(DetailCDIEditRefused, "mount %s: option %s is required (read-only, nosuid and nodev)", m.ContainerPath, want)
		}
	}
	for _, o := range m.Options {
		if !slices.Contains(mountAllowed, o) {
			return false, refuse(DetailCDIEditRefused, "mount %s: option %q is not admitted (%s)", m.ContainerPath, o, strings.Join(mountAllowed, ", "))
		}
	}
	if m.HostPath == nvidiaVoidSource {
		if m.ContainerPath != nvidiaVoidTarget {
			return false, refuse(DetailCDIEditRefused, "mount %s: %s, a device, is a mount source only at %s", m.ContainerPath, nvidiaVoidSource, nvidiaVoidTarget)
		}
		return true, nil
	}
	if m.ContainerPath == nvidiaVoidTarget {
		return false, refuse(DetailCDIEditRefused, "mount source %s at %s: %s is the placeholder for %s alone", m.HostPath, m.ContainerPath, nvidiaVoidTarget, nvidiaVoidSource)
	}
	if !normalPath(m.HostPath) {
		return false, refuse(DetailCDIEditRefused, "mount source %q is not a normal absolute path (no .., no empty segment, plain names only)", m.HostPath)
	}
	if !normalPath(m.ContainerPath) {
		return false, refuse(DetailCDIEditRefused, "mount container path %q is not a normal absolute path (no .., no empty segment, plain names only)", m.ContainerPath)
	}
	return false, nil
}

// auditMount holds one mount to the rules and returns its source as the host
// resolves it: a regular file under the compiled roots, never a socket, a
// device (but the void placeholder), a directory or a trusted spec directory,
// landing under the roots in the container.
func (a *specAudit) auditMount(m *cdispec.Mount) (src auditedMountSource, err error) {
	void, err := mountShape(m)
	if err != nil {
		return auditedMountSource{}, err
	}
	if void {
		return auditedMountSource{Resolved: nvidiaVoidSource, Kind: mountDevice}, nil
	}
	host, err := a.h.inspectMount(m.HostPath)
	if err != nil {
		return auditedMountSource{}, refuse(DetailCDIEditRefused, "mount source %s: %v", m.HostPath, err)
	}
	return a.holdMountSource(m, host)
}

// holdMountSource is auditMount's judgment of a source the host described.
func (a *specAudit) holdMountSource(m *cdispec.Mount, src hostMount) (auditedMountSource, error) {
	none := auditedMountSource{}
	switch src.Kind {
	case mountFile:
	case mountSocket:
		return none, refuse(DetailCDIEditRefused, "mount source %s is a socket, a channel to a host daemon, and is refused", m.HostPath)
	case mountDevice:
		return none, refuse(DetailCDIEditRefused, "mount source %s is a device, and is refused", m.HostPath)
	case mountDir:
		return none, refuse(DetailCDIEditRefused, "mount source %s is a directory; this release mounts regular files only", m.HostPath)
	default:
		return none, refuse(DetailCDIEditRefused, "mount source %s is not a regular file", m.HostPath)
	}
	if !normalPath(src.Resolved) || !underAny(src.Resolved, nvidiaHostRoots) {
		return none, refuse(DetailCDIEditRefused, "mount source %s resolves to %s, outside the compiled roots (%s)",
			m.HostPath, src.Resolved, strings.Join(nvidiaHostRoots, ", "))
	}
	if src.Untrusted != "" {
		return none, refuse(DetailCDIEditRefused, "mount source %s is not root's: %s; a file or a directory above it that another user can change would change what every task loads", m.HostPath, src.Untrusted)
	}
	for _, dir := range a.dirs {
		if underAny(src.Resolved, []string{path.Clean(dir)}) {
			return none, refuse(DetailCDIEditRefused, "mount source %s is in the CDI spec directory %s, which is never a mount source", m.HostPath, dir)
		}
	}
	if !underAny(m.ContainerPath, nvidiaContainerRoots) {
		return none, refuse(DetailCDIEditRefused, "mount container path %s is outside the library, binary and share directories (%s)",
			m.ContainerPath, strings.Join(nvidiaContainerRoots, ", "))
	}
	return auditedMountSource{Resolved: src.Resolved, Kind: src.Kind}, nil
}

// auditNVIDIAEdits holds an NVIDIA unit's hooks, env and mounts, with its
// file's spec-level ones, to the rules, and records the hook binary and the
// mount sources in the audit. Device nodes and groups are auditUnit's.
func (a *specAudit) auditNVIDIAEdits(at string, spec, dev cdispec.ContainerEdits) error {
	wrap := func(err error) error {
		var r *cdiRefusal
		if errors.As(err, &r) {
			return fmt.Errorf("%s: %s: %s", r.detail, at, r.why)
		}
		return err
	}
	if err := checkNVIDIAEnv(slices.Concat(spec.Env, dev.Env)); err != nil {
		return wrap(err)
	}
	hooks := slices.Concat(spec.Hooks, dev.Hooks)
	for _, h := range hooks {
		if err := checkHook(h); err != nil {
			return wrap(err)
		}
	}
	if len(hooks) > 0 {
		if err := a.auditHookBinary(at); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, m := range slices.Concat(spec.Mounts, dev.Mounts) {
		source, err := a.auditMount(m)
		if err != nil {
			return wrap(err)
		}
		if seen[m.ContainerPath] {
			return fmt.Errorf("%s: %s: mount container path %s is mounted twice", DetailCDIEditRefused, at, m.ContainerPath)
		}
		seen[m.ContainerPath] = true
		a.out.mounts[m.HostPath] = source
	}
	return nil
}

// auditHookBinary hashes the one binary every hook runs (once per audit) and
// holds it to the table: the Orker never executes it, it knows which build it
// is. A binary that is not root's, or not there, is CDI_HOOK_PATH; a build the
// table lacks is errHookBuildUnknown, which disables the profile.
func (a *specAudit) auditHookBinary(at string) error {
	if _, done := a.out.hooks[cdiHookBinary]; done || a.unknown != nil {
		return nil
	}
	if a.h.hookBinary == nil {
		return fmt.Errorf("%s: %s: this host cannot audit a hook binary", DetailCDIHookPath, at)
	}
	bin, err := a.h.hookBinary(cdiHookBinary)
	if err != nil {
		return fmt.Errorf("%s: %s: %s: %w: %w", DetailCDIHookPath, at, cdiHookBinary, errHookBinaryUnreadable, err)
	}
	if _, ok := knownHookBuild(bin.SHA256, a.h.arch); !ok {
		// Not returned yet: the whole audit finishes first, so that a hard
		// refusal anywhere in the specs wins over an unlisted build, which
		// only disables the profile (auditGPUSpecs returns it last).
		a.unknown = fmt.Errorf("%s: %w: %s is sha256:%s on %s; this release lists %s", DetailHookBuildUnknown, errHookBuildUnknown,
			cdiHookBinary, bin.SHA256, a.h.arch, listedHookVersions(a.h.arch))
		return nil
	}
	a.out.hooks[cdiHookBinary] = bin.SHA256
	return nil
}

// listedHookVersions names the toolkit releases the table holds for arch, for
// a refusal to say what would be accepted.
func listedHookVersions(arch string) string {
	var versions []string
	for _, b := range cdiHookTable {
		if b.Arch == arch && !slices.Contains(versions, b.Version) {
			versions = append(versions, b.Version)
		}
	}
	if len(versions) == 0 {
		return "no release for " + arch
	}
	return "toolkit " + strings.Join(versions, ", ")
}
