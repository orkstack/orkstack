//go:build linux

package orker

import "syscall"

// TMPFS_MAGIC and RAMFS_MAGIC (linux/magic.h).
const (
	tmpfsMagic = 0x01021994
	ramfsMagic = 0x858458f6
)

// identityMemoryBacked reports whether the filesystem under path is tmpfs
// or ramfs.
func identityMemoryBacked(path string) (bool, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return false, err
	}
	return uint32(st.Type) == tmpfsMagic || uint32(st.Type) == ramfsMagic, nil
}
