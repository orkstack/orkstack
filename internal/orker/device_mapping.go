package orker

import (
	"fmt"
	"path"
	"strings"
)

// DeviceMapping is one host device a container is given, as a device
// profile passes it (design §4.5): the host node the boot audit resolved, the
// path the task sees it at, and its permissions. It is runtime-neutral; each
// runtime spells it in its own argv.
type DeviceMapping struct {
	// HostPath is the resolved host node, never a link: what was audited is
	// what is passed.
	HostPath string
	// ContainerPath is where the task sees it, under /dev.
	ContainerPath string
	// Permissions is r, w or rw, always given: never the runtime's default,
	// which includes mknod.
	Permissions string
}

// checkCDIDevice refuses a CDI device name that is not kind=name with a kind
// and a unit name a gpu profile could declare (profiles.go): never a path, a
// flag or `all`, whatever reached the spec.
func checkCDIDevice(name string) error {
	kind, unit, ok := strings.Cut(name, "=")
	if !ok || !cdiKindPattern.MatchString(kind) || !cdiUnitPattern.MatchString(unit) ||
		unit == "all" {
		return fmt.Errorf("CDI device %q is not a qualified name of a gpu profile's unit", name)
	}
	return nil
}

// checkDeviceMapping refuses a mapping that is not a host node under /dev
// to a container path a profile may name, with r, w or rw. It is the argv
// boundary's own check: a mapping reaches it only from an audited profile,
// and it does not rely on that.
func checkDeviceMapping(d DeviceMapping) error {
	if !plainArgument(d.HostPath) || strings.Contains(d.HostPath, ":") ||
		!strings.HasPrefix(d.HostPath, "/dev/") || path.Clean(d.HostPath) != d.HostPath {
		return fmt.Errorf("create container: device %q is not a plain host node under /dev", d.HostPath)
	}
	if err := hostDevicePath(d.HostPath); err != nil {
		return fmt.Errorf("create container: device %q: %w", d.HostPath, err)
	}
	if strings.Contains(d.ContainerPath, ":") {
		return fmt.Errorf("create container: device container path %q holds a colon", d.ContainerPath)
	}
	if err := checkContainerPath(d.ContainerPath); err != nil {
		return fmt.Errorf("create container: device %s: %w", d.HostPath, err)
	}
	switch d.Permissions {
	case "r", "w", "rw":
	default:
		return fmt.Errorf("create container: device %s: permissions %q are not r, w or rw", d.HostPath, d.Permissions)
	}
	return nil
}
