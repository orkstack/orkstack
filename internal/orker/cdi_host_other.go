//go:build !linux

package orker

import "fmt"

// hostCDI off Linux: a gpu profile needs a Linux host, so every spec
// directory is refused and no gpu profile passes its audit.
func hostCDI(drivers map[uint32]string, _ string) cdiHost {
	return cdiHost{
		inspect:      inspectHostDevice,
		drivers:      drivers,
		groupName:    func(uint32) string { return "" },
		forbiddenFor: func(uint32) (string, bool) { return "", false },
		trustedDir: func(dir string) error {
			return fmt.Errorf("%s: a gpu profile needs a Linux host", dir)
		},
	}
}

func runtimeSocketPath() string { return "" }
