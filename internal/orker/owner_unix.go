//go:build unix

package orker

import (
	"fmt"
	"os"
	"syscall"
)

// ownerOf renders " owner=UID:GID" for a file the operating system reports an
// owner of, and "" for one it does not.
func ownerOf(info os.FileInfo) string {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return fmt.Sprintf(" owner=%d:%d", stat.Uid, stat.Gid)
	}
	return ""
}
