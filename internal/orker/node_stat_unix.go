//go:build unix

package orker

import (
	"os"
	"syscall"
)

// nodeStatOf is what a device profile reads of a file the operating system
// stat'ed: its device numbers, group and identity. ok is false where the
// platform reports none.
func nodeStatOf(info os.FileInfo) (nodeStat, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nodeStat{}, false
	}
	return nodeStat{Rdev: uint64(st.Rdev), GID: st.Gid, Dev: uint64(st.Dev), Ino: st.Ino}, true
}
