//go:build linux

package procguard

import (
	"fmt"
	"syscall"
)

// supported says Harden does something on this platform.
const supported = true

// prSetDumpable is prctl(2)'s PR_SET_DUMPABLE.
const prSetDumpable = 4

func setDumpable(on bool) error {
	var v uintptr
	if on {
		v = 1
	}
	if _, _, errno := syscall.RawSyscall6(syscall.SYS_PRCTL, prSetDumpable, v, 0, 0, 0, 0); errno != 0 {
		return fmt.Errorf("prctl(PR_SET_DUMPABLE, %d): %w", v, errno)
	}
	return nil
}
