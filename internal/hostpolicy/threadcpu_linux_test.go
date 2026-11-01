//go:build linux

package hostpolicy

import (
	"syscall"
	"time"
	"unsafe"
)

// clockThreadCPUTimeID is Linux's CLOCK_THREAD_CPUTIME_ID.
const clockThreadCPUTimeID = 3

// threadCPU is the CPU time the calling OS thread has consumed, its system
// calls included. Unlike the wall clock it does not advance while the thread
// waits for a CPU, so a host loaded by other processes cannot move it. A
// caller measuring a goroutine locks it to its thread first
// (runtime.LockOSThread), or a migration would read another thread's clock.
func threadCPU() (time.Duration, bool) {
	var ts syscall.Timespec
	if _, _, errno := syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, clockThreadCPUTimeID,
		uintptr(unsafe.Pointer(&ts)), 0); errno != 0 {
		return 0, false
	}
	return time.Duration(ts.Nano()), true
}
