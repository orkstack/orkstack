//go:build unix

package temporalconn

import (
	"os"
	"syscall"
)

// openForRead opens path without blocking: a FIFO swapped in after the stat
// opens at once, and the check of the opened file then refuses it.
func openForRead(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}
