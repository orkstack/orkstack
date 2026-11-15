//go:build !unix

package temporalconn

import "os"

// openForRead opens path; off Unix there is no FIFO to swap in.
func openForRead(path string) (*os.File, error) {
	return os.Open(path)
}
