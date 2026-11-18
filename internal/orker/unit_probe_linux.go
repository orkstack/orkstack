//go:build linux

package orker

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
	"unsafe"
)

// The two version ioctls, from the kernel's uapi headers: drm.h's
// DRM_IOCTL_VERSION, _IOWR('d', 0x00, struct drm_version), and kfd_ioctl.h's
// AMDKFD_IOC_GET_VERSION, _IOR('K', 0x01, struct kfd_ioctl_get_version_args).
// No Go package wraps them (golang.org/x/sys/unix has neither), so the
// structs are declared here exactly as the headers lay them out.
const (
	drmIoctlVersion     = 0xC0406400
	kfdIoctlGetVersion  = 0x80084B01
	maxDRMDriverNameLen = 64
)

type drmVersion struct {
	major, minor, patch int32
	_                   int32
	nameLen             uintptr
	name                *byte
	dateLen             uintptr
	date                *byte
	descLen             uintptr
	desc                *byte
}

type kfdVersion struct{ major, minor uint32 }

// askDriver asks the node's driver for its version: a DRM driver's name and
// version, or kfd's version.
func askDriver(p string, kfd bool) (string, error) {
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	if kfd {
		var v kfdVersion
		if err := ioctl(f.Fd(), kfdIoctlGetVersion, unsafe.Pointer(&v)); err != nil {
			return "", fmt.Errorf("AMDKFD_IOC_GET_VERSION: %w", err)
		}
		return fmt.Sprintf("kfd %d.%d", v.major, v.minor), nil
	}
	var v drmVersion
	if err := ioctl(f.Fd(), drmIoctlVersion, unsafe.Pointer(&v)); err != nil {
		return "", fmt.Errorf("DRM_IOCTL_VERSION: %w", err)
	}
	if v.nameLen == 0 || v.nameLen > maxDRMDriverNameLen {
		return "", fmt.Errorf("DRM_IOCTL_VERSION: a driver name of %d bytes", v.nameLen)
	}
	name := make([]byte, v.nameLen)
	v = drmVersion{nameLen: uintptr(len(name)), name: &name[0]}
	err = ioctl(f.Fd(), drmIoctlVersion, unsafe.Pointer(&v))
	// The kernel wrote into name through a pointer the struct holds, which
	// the garbage collector does not see as a use.
	runtime.KeepAlive(name)
	if err != nil {
		return "", fmt.Errorf("DRM_IOCTL_VERSION: %w", err)
	}
	return fmt.Sprintf("%s %d.%d", strings.TrimRight(string(name), "\x00"), v.major, v.minor), nil
}

func ioctl(fd uintptr, req uintptr, arg unsafe.Pointer) error {
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, req, uintptr(arg)); errno != 0 {
		return errno
	}
	return nil
}
