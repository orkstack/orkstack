package orker

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// Device classes (design §4.9): the closed set of host devices a device
// profile may pass to a task. A device is of a class when it is a character
// node, its driver (by its major, in /proc/devices) is one of the class's,
// and its node name is the class's. Nothing else is passed, whatever the
// operator wrote.

// deviceClass is one class of the table.
type deviceClass struct {
	name    string
	drivers []string
	node    *regexp.Regexp
	// gpu says the class is the gpu kind's (a unit's nodes, through CDI),
	// never the devices kind's.
	gpu bool
	// minMinor, when set, is the least minor a node of the class has: a DRM
	// render node is minor 128 and up, and a card node below it is refused.
	minMinor uint32
}

// deviceClasses is the table both kinds use, each only its own classes.
var deviceClasses = []deviceClass{
	{name: "usb-serial", drivers: []string{"ttyUSB", "ttyACM"}, node: regexp.MustCompile(`^tty(USB|ACM)[0-9]+$`)},
	{name: "drm-render", drivers: []string{"drm"}, node: regexp.MustCompile(`^renderD[0-9]+$`), gpu: true, minMinor: 128},
	{name: "kfd", drivers: []string{"kfd"}, node: regexp.MustCompile(`^kfd$`), gpu: true},
	// NVIDIA (experimental): the GPU nodes and the control node under the
	// nvidia driver (nvidia-frontend before driver 550.40), and the UVM
	// nodes under nvidia-uvm. nvidia-modeset, the same major's display node,
	// is not passed: compute does not need it.
	{name: "nvidia", drivers: []string{"nvidia", "nvidia-frontend"}, node: regexp.MustCompile(`^nvidia([0-9]+|ctl)$`), gpu: true},
	{name: "nvidia-uvm", drivers: []string{"nvidia-uvm"}, node: regexp.MustCompile(`^nvidia-uvm(-tools)?$`), gpu: true},
}

// hostDevice is what the host says one device path is.
type hostDevice struct {
	// Resolved is the path with every link resolved.
	Resolved     string
	Char         bool
	Major, Minor uint32
	GID          uint32
}

// parseProcDevices reads /proc/devices' character section: driver by major.
func parseProcDevices(raw string) map[uint32]string {
	out := map[uint32]string{}
	inChar := false
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "Character devices:":
			inChar = true
			continue
		case strings.HasSuffix(line, "devices:"):
			inChar = false
			continue
		case !inChar || line == "":
			continue
		}
		major, driver, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		n, err := strconv.ParseUint(major, 10, 32)
		if err != nil {
			continue
		}
		out[uint32(n)] = strings.TrimSpace(driver)
	}
	return out
}

// classifyDevice is the class of dev, or why it has none.
func classifyDevice(dev hostDevice, drivers map[uint32]string) (string, error) {
	return classifyNode(dev, drivers, false)
}

// classifyGPUNode is classifyDevice for a gpu unit's node: only the gpu
// kind's classes.
func classifyGPUNode(dev hostDevice, drivers map[uint32]string) (string, error) {
	return classifyNode(dev, drivers, true)
}

func classifyNode(dev hostDevice, drivers map[uint32]string, gpu bool) (string, error) {
	if !dev.Char {
		return "", fmt.Errorf("%s is not a character device", dev.Resolved)
	}
	driver, ok := drivers[dev.Major]
	if !ok {
		return "", fmt.Errorf("%s: major %d has no driver in /proc/devices", dev.Resolved, dev.Major)
	}
	name := path.Base(dev.Resolved)
	for _, c := range deviceClasses {
		if c.gpu != gpu {
			continue
		}
		for _, d := range c.drivers {
			if d != driver {
				continue
			}
			if !c.node.MatchString(name) || dev.Minor < c.minMinor {
				return "", fmt.Errorf("%s: driver %s, but %s is not a %s node", dev.Resolved, driver, name, c.name)
			}
			return c.name, nil
		}
	}
	return "", fmt.Errorf("%s: driver %s is not of a device class this Orker passes", dev.Resolved, driver)
}

// resolveDevicePath resolves p on the host with eval and holds the result to
// /dev: a link that leads out of it, or to a path no profile may name, is
// refused.
func resolveDevicePath(p string, eval func(string) (string, error)) (string, error) {
	resolved, err := eval(p)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", p, err)
	}
	resolved = filepath.Clean(resolved)
	if !strings.HasPrefix(resolved, "/dev/") {
		return "", fmt.Errorf("%s resolves to %s, outside /dev", p, resolved)
	}
	if err := hostDevicePath(resolved); err != nil {
		return "", fmt.Errorf("%s resolves to %s: %w", p, resolved, err)
	}
	return resolved, nil
}

// inspectHostDevice is what the host says the device at p is, resolved and
// stat'ed. The stat is of the resolved node itself, never through it, and it
// opens nothing: opening a serial device can toggle its control lines.
func inspectHostDevice(p string) (hostDevice, error) {
	resolved, err := resolveDevicePath(p, filepath.EvalSymlinks)
	if err != nil {
		return hostDevice{}, err
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		return hostDevice{}, fmt.Errorf("stat %s: %w", resolved, err)
	}
	st, ok := nodeStatOf(info)
	if !ok {
		return hostDevice{}, fmt.Errorf("stat %s: no device numbers on this platform", resolved)
	}
	dev := hostDevice{Resolved: resolved, Char: info.Mode()&os.ModeCharDevice != 0, GID: st.GID}
	dev.Major, dev.Minor = deviceNumbers(st.Rdev)
	return dev, nil
}

// nodeStat is what nodeStatOf reads of a stat'ed file.
type nodeStat struct {
	Rdev     uint64
	GID      uint32
	Dev, Ino uint64
}

// deviceNumbers splits a Linux dev_t into its major and minor, as glibc's
// major() and minor() do.
func deviceNumbers(rdev uint64) (major, minor uint32) {
	major = uint32((rdev>>8)&0xfff) | uint32((rdev>>32)&^0xfff)
	minor = uint32(rdev&0xff) | uint32((rdev>>12)&^0xff)
	return major, minor
}

// hostDeviceClassifier is the DeviceClassifier the boot audit hands
// StructuralIdentity: each path inspected on this host and classified
// against this host's /proc/devices.
func hostDeviceClassifier(procDevices string) DeviceClassifier {
	drivers := parseProcDevices(procDevices)
	return func(p string) (string, error) {
		dev, err := inspectHostDevice(p)
		if err != nil {
			return "", err
		}
		return classifyDevice(dev, drivers)
	}
}
