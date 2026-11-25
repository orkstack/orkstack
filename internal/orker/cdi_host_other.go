//go:build !linux

package orker

import (
	"fmt"
	"runtime"
)

// hostCDI off Linux: a gpu profile needs a Linux host, so every spec
// directory is refused and no gpu profile passes its audit.
func hostCDI(drivers map[uint32]string, _ string) cdiHost {
	return cdiHost{
		inspect:      inspectHostDevice,
		arch:         runtime.GOARCH,
		inspectMount: inspectHostMount,
		hookBinary:   hashHostHook,
		drivers:      drivers,
		groupName:    func(uint32) string { return "" },
		forbiddenFor: func(uint32) (string, bool) { return "", false },
		trustedDir: func(dir string) error {
			return fmt.Errorf("%s: a gpu profile needs a Linux host", dir)
		},
	}
}

func runtimeSocketPath() string { return "" }

// hashHostHook off Linux: a gpu profile needs a Linux host.
func hashHostHook(p string) (hostHook, error) {
	return hostHook{}, fmt.Errorf("%s: a gpu profile needs a Linux host", p)
}

// mountTrust off Linux: no owner can be read, so no source is root's.
func mountTrust(_, _ string) string { return "no owner on this platform" }
