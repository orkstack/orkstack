package orker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

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
// This release audits hookless specs (AMD, Intel): a hook, an env or a mount
// edit is refused, because no compiled allow-list for them exists yet (the
// NVIDIA toolkit's hooks, env and mount roots come with P4b).

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
}

// gpuSpecAudit is what one audit found: the audit set's bytes, by digest, and
// each unit's nodes and groups, in the profile's unit order.
type gpuSpecAudit struct {
	files map[string]string // path → SHA-256 of its bytes
	units []gpuUnitAudit
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
}

// auditGPUSpecs audits the specs in dirs for kind and the profile's units. A
// refusal names its detail; no spec of the kind is errCDISpecAbsent.
func auditGPUSpecs(dirs []string, kind string, units []string, h cdiHost) (gpuSpecAudit, error) {
	audit := gpuSpecAudit{files: map[string]string{}}
	found := map[string]cdispec.Device{}
	foundIn := map[string]string{}
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
				return gpuSpecAudit{}, fmt.Errorf("%s: %w", DetailCDISpecInvalid, err)
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
			if !editsEmpty(spec.ContainerEdits) {
				return gpuSpecAudit{}, fmt.Errorf("%s: %s: spec-level containerEdits apply to every device of the kind; "+
					"write each unit's edits on its device", DetailCDIEditRefused, path)
			}
			for _, d := range spec.Devices {
				if other, dup := foundIn[d.Name]; dup {
					return gpuSpecAudit{}, fmt.Errorf("%s: device %s=%s is declared in %s and in %s",
						DetailCDIUnitUnresolved, kind, d.Name, other, path)
				}
				found[d.Name], foundIn[d.Name] = d, path
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
		unit, err := auditUnit(kind, d, h)
		if err != nil {
			return gpuSpecAudit{}, err
		}
		for _, n := range unit.nodes {
			if other, shared := owner[n.host.Resolved]; shared {
				return gpuSpecAudit{}, fmt.Errorf("%s: units %s and %s share %s; a node is one unit's",
					DetailUnitsOverlap, other, name, n.host.Resolved)
			}
			owner[n.host.Resolved] = name
		}
		audit.units = append(audit.units, unit)
	}
	return audit, nil
}

func editsEmpty(e cdispec.ContainerEdits) bool {
	return len(e.Env) == 0 && len(e.DeviceNodes) == 0 && len(e.NetDevices) == 0 && len(e.Hooks) == 0 &&
		len(e.Mounts) == 0 && e.IntelRdt == nil && len(e.AdditionalGIDs) == 0
}

// auditUnit holds one unit's edits to the rules: device nodes and groups only.
func auditUnit(kind string, d cdispec.Device, h cdiHost) (gpuUnitAudit, error) {
	at := kind + "=" + d.Name
	e := d.ContainerEdits
	refused := func(why string) error { return fmt.Errorf("%s: %s: %s", DetailCDIEditRefused, at, why) }
	switch {
	case len(e.Hooks) > 0:
		return gpuUnitAudit{}, refused("hooks are refused; this release runs hookless specs only")
	case len(e.Env) > 0:
		return gpuUnitAudit{}, refused("env edits are refused")
	case len(e.Mounts) > 0:
		return gpuUnitAudit{}, refused("mounts are refused")
	case len(e.NetDevices) > 0 || e.IntelRdt != nil:
		return gpuUnitAudit{}, refused("only deviceNodes and additionalGids are edits a unit may make")
	case len(e.DeviceNodes) == 0:
		return gpuUnitAudit{}, refused("a unit passes at least one device node")
	}
	unit := gpuUnitAudit{name: d.Name}
	for _, n := range e.DeviceNodes {
		node, err := auditNode(at, n, h)
		if err != nil {
			return gpuUnitAudit{}, err
		}
		unit.nodes = append(unit.nodes, node)
	}
	for _, gid := range e.AdditionalGIDs {
		if err := auditGroup(at, gid, unit.nodes, h); err != nil {
			return gpuUnitAudit{}, err
		}
		unit.gids = append(unit.gids, gid)
	}
	return unit, nil
}

func auditNode(at string, n *cdispec.DeviceNode, h cdiHost) (auditedNode, error) {
	refused := func(format string, args ...any) error {
		return fmt.Errorf("%s: %s: %s", DetailCDIEditRefused, at, fmt.Sprintf(format, args...))
	}
	switch {
	case n == nil:
		return auditedNode{}, refused("an empty device node")
	case n.Path == "" || n.HostPath == "":
		return auditedNode{}, refused("device node %q: write both path and hostPath", n.Path)
	case !containerPathPattern.MatchString(n.Path):
		return auditedNode{}, refused("device node path %q is not a path under /dev", n.Path)
	case n.Type != "c":
		return auditedNode{}, refused("device node %q: type must be c", n.Path)
	case !slices.Contains([]string{"r", "w", "rw"}, n.Permissions):
		return auditedNode{}, refused("device node %q: permissions must be r, w or rw, not %q", n.Path, n.Permissions)
	case n.UID != nil || n.GID != nil || n.FileMode != nil:
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
	if (n.Major != 0 && n.Major != int64(host.Major)) || (n.Minor != 0 && n.Minor != int64(host.Minor)) {
		return auditedNode{}, refused("device node %q says %d:%d, the host's %s is %d:%d",
			n.Path, n.Major, n.Minor, host.Resolved, host.Major, host.Minor)
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
