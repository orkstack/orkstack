//go:build linux

package orker

import (
	"fmt"
	"os"
	"syscall"
)

// cgroup2SuperMagic is CGROUP2_SUPER_MAGIC (linux/magic.h).
const cgroup2SuperMagic = 0x63677270

// cgroup2Mounted reports whether mount is the unified (v2) hierarchy.
func cgroup2Mounted(mount string) (bool, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(mount, &st); err != nil {
		return false, err
	}
	return st.Type == cgroup2SuperMagic, nil
}

// oNoFollow refuses a link where a cgroup file should be.
const oNoFollow = syscall.O_NOFOLLOW

// cgroupSuperMagic is CGROUP_SUPER_MAGIC (linux/magic.h): a cgroup v1
// hierarchy.
const cgroupSuperMagic = 0x27e0eb

// verifyCgroupFS accepts an opened directory only on a cgroup filesystem
// (v2, or a v1 hierarchy): kernfs, whose node ids are not reused within a
// boot. Anything else is errCgroupUnverified, explicitly.
func verifyCgroupFS(dir *os.File) error {
	var st syscall.Statfs_t
	if err := syscall.Fstatfs(int(dir.Fd()), &st); err != nil {
		return fmt.Errorf("%w: statfs: %v", errCgroupUnverified, err)
	}
	if st.Type != cgroup2SuperMagic && st.Type != cgroupSuperMagic {
		return fmt.Errorf("%w: not on a cgroup filesystem (magic %#x)", errCgroupUnverified, st.Type)
	}
	return nil
}

// oDirectory refuses anything but a directory where a cgroup should be.
const oDirectory = syscall.O_DIRECTORY
