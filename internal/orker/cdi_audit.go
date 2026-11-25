package orker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	cdi "tags.cncf.io/container-device-interface/pkg/cdi"
	cdispec "tags.cncf.io/container-device-interface/specs-go"
)

// The gpu profile's CDI audit (design §4.10). Every spec file is read with
// tags.cncf.io/container-device-interface's ReadSpec, pinned to the version
// moby v29.9.2 vendors: it is dockerd's own decoder and validator (a strict
// decode refusing unknown fields, the version and kind grammar, device names),
// so the audit refuses exactly what dockerd would, and reads what it resolves.
// The library's Cache is not used: the audit lists and reads the directories
// itself, so nothing watches or refreshes behind it. OrkStack's own rules run
// on top of the library's.
//
// AMD and Intel specs are hookless: a hook, an env or a mount edit is refused
// under any kind but nvidia.com's, and so are spec-level edits. An NVIDIA
// spec (experimental, E91 P4b) may carry what nvidia-ctk writes and nothing
// else: the hook grammar (cdi_hooks.go) and the env, mount and node rules of
// cdi_nvidia_edits.go, its spec-level edits applying to every unit of its
// file.

// The ORKER_PROFILE_INVALID details of a gpu profile's audit.
const (
	DetailCDISpecInvalid      = "CDI_SPEC_INVALID"
	DetailCDISpecDirUntrusted = "CDI_SPEC_DIR_UNTRUSTED"
	DetailCDIEditRefused      = "CDI_EDIT_REFUSED"
	DetailCDIGroupRefused     = "CDI_GROUP_REFUSED"
	DetailCDIUnitUnresolved   = "CDI_UNIT_UNRESOLVED"
	DetailUnitsOverlap        = "UNITS_OVERLAP"
)

// errCDISpecAbsent is the audit's answer when no spec declares the profile's
// kind: the profile is unavailable (SPEC_ABSENT), not refused.
var errCDISpecAbsent = errors.New("no CDI spec in the spec directories declares the profile's kind")

// refusedGroupNames are groups no gpu unit may add, whatever node they own.
var refusedGroupNames = []string{"docker", "disk", "kmem", "shadow", "sudo", "wheel"}

// cdiHost is what the audit asks of the host.
type cdiHost struct {
	inspect   func(path string) (hostDevice, error) // inspectHostDevice
	drivers   map[uint32]string                     // /proc/devices' character drivers
	groupName func(gid uint32) string               // "" when the gid has no name
	// forbiddenFor names a node or socket the gid owns that no unit's group
	// may: the runtime socket, a block device, /dev/mem, /dev/kmem, a tty or a
	// console.
	forbiddenFor func(gid uint32) (string, bool)
	// trustedDir refuses a spec directory, or an ancestor, that is not
	// root-owned, is group- or world-writable, or is a symlink.
	trustedDir func(dir string) error
	// arch is the Orker's arch in the table's spelling (runtime.GOARCH).
	arch string
	// hookBinary resolves the hook binary at path, holds it and its ancestors
	// to root's, and hashes it. Only an NVIDIA spec with a hook asks.
	hookBinary func(path string) (hostHook, error)
	// inspectMount is what the host says a mount's source is: resolved, and
	// of what kind.
	inspectMount func(path string) (hostMount, error)
}

// hostHook is a hook binary as the host has it: the file after links, and
// the SHA-256 of its bytes.
type hostHook struct {
	Resolved string
	SHA256   string
}

// mountKind is what a mount source is on the host.
type mountKind string

const (
	mountFile   mountKind = "file"
	mountDir    mountKind = "directory"
	mountSocket mountKind = "socket"
	mountDevice mountKind = "device"
	mountOther  mountKind = "other"
)

// hostMount is a mount source as the host has it: resolved through every
// link, and of one kind. Untrusted says why the file, or a directory above it
// as written or as resolved, is not root's and unwritable by others ("" when
// it is): a source another user can change is not one the audit may admit.
type hostMount struct {
	Resolved  string
	Kind      mountKind
	Untrusted string
}

// auditedMountSource is a mount source as the audit recorded it.
type auditedMountSource struct {
	Resolved string
	Kind     mountKind
}

// gpuSpecAudit is what one audit found: the audit set's bytes, by digest, and
// each unit's nodes and groups, in the profile's unit order.
type gpuSpecAudit struct {
	files map[string]string // path → SHA-256 of its bytes
	units []gpuUnitAudit
	// hooks is each hook binary the specs run, by path, with its SHA-256: a
	// row of the compiled table. Empty for a hookless spec.
	hooks map[string]string
	// mounts is each mount source the specs name, as written, with what the
	// host resolved it to and of what kind (rechecked before create).
	mounts map[string]auditedMountSource
}

type gpuUnitAudit struct {
	name  string
	nodes []auditedNode
	gids  []uint32
}

type auditedNode struct {
	path, hostPath, permissions string
	host                        hostDevice
	class                       string
	// shared says the node is a spec-level edit, which every unit of its
	// file passes: no unit owns it, so two units naming it are no overlap.
	shared bool
}

// isNVIDIAKind says a CDI kind is NVIDIA's, whose specs may carry the edits
// cdi_nvidia_edits.go and cdi_hooks.go admit.
func isNVIDIAKind(kind string) bool { return strings.HasPrefix(kind, "nvidia.com/") }

// specAudit is one audit's context: the kind, what the host says, the trusted
// directories (a mount may not come from one) and the audit it fills.
type specAudit struct {
	kind   string
	nvidia bool
	dirs   []string
	h      cdiHost
	out    *gpuSpecAudit
	// unknown is the HOOK_BUILD_UNKNOWN finding, held until the audit is done.
	unknown error
}

// auditGPUSpecs audits the specs in dirs for kind and the profile's units. A
// refusal names its detail; no spec of the kind is errCDISpecAbsent, and a
// hook binary of a build the table lacks is errHookBuildUnknown.
func auditGPUSpecs(dirs []string, kind string, units []string, h cdiHost) (gpuSpecAudit, error) {
	audit := gpuSpecAudit{files: map[string]string{}, hooks: map[string]string{}, mounts: map[string]auditedMountSource{}}
	ctx := &specAudit{kind: kind, nvidia: isNVIDIAKind(kind), dirs: dirs, h: h, out: &audit}
	found := map[string]cdispec.Device{}
	foundIn := map[string]string{}
	specEdits := map[string]cdispec.ContainerEdits{}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return gpuSpecAudit{}, fmt.Errorf("%s: read %s: %w", DetailCDISpecDirUntrusted, dir, err)
		}
		if err := h.trustedDir(dir); err != nil {
			return gpuSpecAudit{}, fmt.Errorf("%s: %w", DetailCDISpecDirUntrusted, err)
		}
		for _, e := range entries {
			ext := filepath.Ext(e.Name())
			if e.IsDir() || (ext != ".yaml" && ext != ".json") {
				continue
			}
			path := filepath.Join(dir, e.Name())
			spec, err := cdi.ReadSpec(path, 0)
			if err != nil {
				return gpuSpecAudit{}, fmt.Errorf("%s: %s: %w", DetailCDISpecInvalid, path, err)
			}
			if spec.Kind != kind {
				continue
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return gpuSpecAudit{}, fmt.Errorf("%s: read %s: %w", DetailCDISpecInvalid, path, err)
			}
			sum := sha256.Sum256(raw)
			audit.files[path] = hex.EncodeToString(sum[:])
			if !ctx.nvidia && !editsEmpty(spec.ContainerEdits) {
				return gpuSpecAudit{}, fmt.Errorf("%s: %s: spec-level containerEdits apply to every device of the kind; "+
					"write each unit's edits on its device", DetailCDIEditRefused, path)
			}
			for _, d := range spec.Devices {
				if other, dup := foundIn[d.Name]; dup {
					return gpuSpecAudit{}, fmt.Errorf("%s: device %s=%s is declared in %s and in %s",
						DetailCDIUnitUnresolved, kind, d.Name, other, path)
				}
				found[d.Name], foundIn[d.Name], specEdits[d.Name] = d, path, spec.ContainerEdits
			}
		}
	}
	if len(audit.files) == 0 {
		return gpuSpecAudit{}, fmt.Errorf("%w (%s)", errCDISpecAbsent, kind)
	}
	owner := map[string]string{}
	for _, name := range units {
		d, ok := found[name]
		if !ok {
			return gpuSpecAudit{}, fmt.Errorf("%s: no spec declares %s=%s", DetailCDIUnitUnresolved, kind, name)
		}
		unit, err := ctx.auditUnit(d, specEdits[name])
		if err != nil {
			return gpuSpecAudit{}, err
		}
		for _, n := range unit.nodes {
			if n.shared {
				continue
			}
			if other, shared := owner[n.host.Resolved]; shared {
				return gpuSpecAudit{}, fmt.Errorf("%s: units %s and %s share %s; a node is one unit's",
					DetailUnitsOverlap, other, name, n.host.Resolved)
			}
			owner[n.host.Resolved] = name
		}
		audit.units = append(audit.units, unit)
	}
	// An unlisted hook build disables the profile; it is reported last, so
	// that any hard refusal above, in any unit, wins over it.
	if ctx.unknown != nil {
		return gpuSpecAudit{}, ctx.unknown
	}
	return audit, nil
}

func editsEmpty(e cdispec.ContainerEdits) bool {
	return len(e.Env) == 0 && len(e.DeviceNodes) == 0 && len(e.NetDevices) == 0 && len(e.Hooks) == 0 &&
		len(e.Mounts) == 0 && e.IntelRdt == nil && len(e.AdditionalGIDs) == 0
}

// auditUnit holds one unit's edits, and the spec-level edits of its file
// (NVIDIA only: any other kind's are refused before), to the rules.
func (a *specAudit) auditUnit(d cdispec.Device, spec cdispec.ContainerEdits) (gpuUnitAudit, error) {
	at := a.kind + "=" + d.Name
	e := d.ContainerEdits
	refused := func(why string) error { return fmt.Errorf("%s: %s: %s", DetailCDIEditRefused, at, why) }
	switch {
	case !a.nvidia && len(e.Hooks) > 0:
		return gpuUnitAudit{}, refused("hooks are refused; this release runs hookless specs only")
	case !a.nvidia && len(e.Env) > 0:
		return gpuUnitAudit{}, refused("env edits are refused")
	case !a.nvidia && len(e.Mounts) > 0:
		return gpuUnitAudit{}, refused("mounts are refused")
	case len(e.NetDevices) > 0 || e.IntelRdt != nil || len(spec.NetDevices) > 0 || spec.IntelRdt != nil:
		return gpuUnitAudit{}, refused("only deviceNodes and additionalGids are edits a unit may make")
	case len(e.DeviceNodes)+len(spec.DeviceNodes) == 0:
		return gpuUnitAudit{}, refused("a unit passes at least one device node")
	}
	if a.nvidia {
		if err := a.auditNVIDIAEdits(at, spec, e); err != nil {
			return gpuUnitAudit{}, err
		}
	}
	unit := gpuUnitAudit{name: d.Name}
	for _, group := range []struct {
		nodes  []*cdispec.DeviceNode
		shared bool
	}{{spec.DeviceNodes, true}, {e.DeviceNodes, false}} {
		for _, n := range group.nodes {
			node, err := a.auditNode(at, n)
			if err != nil {
				return gpuUnitAudit{}, err
			}
			node.shared = group.shared
			unit.nodes = append(unit.nodes, node)
		}
	}
	for _, gid := range append(slices.Clone(spec.AdditionalGIDs), e.AdditionalGIDs...) {
		if err := auditGroup(at, gid, unit.nodes, a.h); err != nil {
			return gpuUnitAudit{}, err
		}
		unit.gids = append(unit.gids, gid)
	}
	return unit, nil
}

func (a *specAudit) auditNode(at string, n *cdispec.DeviceNode) (auditedNode, error) {
	h := a.h
	refused := func(format string, args ...any) error {
		return fmt.Errorf("%s: %s: %s", DetailCDIEditRefused, at, fmt.Sprintf(format, args...))
	}
	// NVIDIA's generator writes rwm, no type, and the node's own fileMode and
	// gid (decision Q4c): rwm because mknod grants nothing under the
	// mandatory profile's --cap-drop=ALL, fileMode and gid because, equal to
	// the host node's, they change nothing. Every other kind's rules stay.
	perms := []string{"r", "w", "rw"}
	if a.nvidia {
		perms = append(perms, "rwm")
	}
	switch {
	case n == nil:
		return auditedNode{}, refused("an empty device node")
	case n.Path == "" || n.HostPath == "":
		return auditedNode{}, refused("device node %q: write both path and hostPath", n.Path)
	case !containerPathPattern.MatchString(n.Path):
		return auditedNode{}, refused("device node path %q is not a path under /dev", n.Path)
	case n.Type != "c" && !(a.nvidia && n.Type == ""):
		return auditedNode{}, refused("device node %q: type must be c", n.Path)
	case !slices.Contains(perms, n.Permissions):
		return auditedNode{}, refused("device node %q: permissions must be %s, not %q", n.Path, strings.Join(perms, ", "), n.Permissions)
	case n.UID != nil:
		return auditedNode{}, refused("device node %q: uid is refused", n.Path)
	case !a.nvidia && (n.GID != nil || n.FileMode != nil):
		return auditedNode{}, refused("device node %q: uid, gid and fileMode are refused", n.Path)
	}
	host, err := h.inspect(n.HostPath)
	if err != nil {
		return auditedNode{}, fmt.Errorf("%s: %s: %w", DetailDeviceClassRefused, at, err)
	}
	// The runtime Lstat()s hostPath when it starts the container and refuses
	// anything but a device node there ("not a device node"), so a link, such
	// as one under /dev/dri/by-path, would pass this audit and fail every
	// task's start. It is refused here, naming the node to write instead.
	if host.Resolved != n.HostPath {
		return auditedNode{}, refused("device node %q: hostPath %s is a link to %s; the runtime refuses a link "+
			"when it starts the container, so write %s", n.Path, n.HostPath, host.Resolved, host.Resolved)
	}
	class, err := classifyGPUNode(host, h.drivers)
	if err != nil {
		return auditedNode{}, fmt.Errorf("%s: %s: %w", DetailDeviceClassRefused, at, err)
	}
	if a.nvidia && class != "nvidia" && class != "nvidia-uvm" {
		return auditedNode{}, fmt.Errorf("%s: %s: %s is of class %s, not an nvidia class", DetailDeviceClassRefused, at, host.Resolved, class)
	}
	if (n.Major != 0 && n.Major != int64(host.Major)) || (n.Minor != 0 && n.Minor != int64(host.Minor)) {
		return auditedNode{}, refused("device node %q says %d:%d, the host's %s is %d:%d",
			n.Path, n.Major, n.Minor, host.Resolved, host.Major, host.Minor)
	}
	if n.FileMode != nil && *n.FileMode != host.Mode {
		return auditedNode{}, refused("device node %q: fileMode %#o is not the host's %s, %#o", n.Path, *n.FileMode, host.Resolved, host.Mode)
	}
	if n.GID != nil && *n.GID != host.GID {
		return auditedNode{}, refused("device node %q: gid %d is not the host's %s, %d", n.Path, *n.GID, host.Resolved, host.GID)
	}
	return auditedNode{path: n.Path, hostPath: n.HostPath, permissions: n.Permissions, host: host, class: class}, nil
}

func auditGroup(at string, gid uint32, nodes []auditedNode, h cdiHost) error {
	refused := func(why string) error {
		return fmt.Errorf("%s: %s: additionalGids %d: %s", DetailCDIGroupRefused, at, gid, why)
	}
	if gid == 0 {
		return refused("gid 0 is refused")
	}
	if name := h.groupName(gid); slices.Contains(refusedGroupNames, name) {
		return refused("the " + name + " group is refused")
	}
	if !slices.ContainsFunc(nodes, func(n auditedNode) bool { return n.host.GID == gid }) {
		return refused("it owns none of the unit's nodes")
	}
	if what, owns := h.forbiddenFor(gid); owns {
		return refused("it owns " + what)
	}
	return nil
}

// inspectHostMount is what the host says a mount source is: resolved through
// every link, and stat'ed itself (never through it, and never opened: a
// socket or a device is not read).
func inspectHostMount(p string) (hostMount, error) {
	resolved, err := filepath.EvalSymlinks(p)
	if err != nil {
		return hostMount{}, fmt.Errorf("resolve %s: %w", p, err)
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		return hostMount{}, err
	}
	kind := mountOther
	switch mode := info.Mode(); {
	case mode.IsRegular():
		kind = mountFile
	case mode.IsDir():
		kind = mountDir
	case mode&os.ModeSocket != 0:
		kind = mountSocket
	case mode&os.ModeDevice != 0:
		kind = mountDevice
	}
	return hostMount{Resolved: filepath.Clean(resolved), Kind: kind, Untrusted: mountTrust(p, filepath.Clean(resolved))}, nil
}

// specFilesOfKind lists the spec files of dirs that declare kind, read as the
// audit reads them: a file that no longer parses is an error, as it refuses a
// boot. A directory that is not there holds none.
func specFilesOfKind(dirs []string, kind string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			ext := filepath.Ext(e.Name())
			if e.IsDir() || (ext != ".yaml" && ext != ".json") {
				continue
			}
			path := filepath.Join(dir, e.Name())
			spec, err := cdi.ReadSpec(path, 0)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", path, err)
			}
			if spec.Kind == kind {
				out[path] = true
			}
		}
	}
	return out, nil
}
