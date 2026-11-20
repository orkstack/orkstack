//go:build linux

package orker

import (
	"os"
	"testing"
)

// TestIdentityMemoryBacked: /dev/shm is tmpfs on a Linux host, and the
// test's own temporary directory is told apart from it by statfs alone.
func TestIdentityMemoryBacked(t *testing.T) {
	if _, err := os.Stat("/dev/shm"); err != nil {
		t.Fatalf("a Linux host has /dev/shm: %v", err)
	}
	if got, err := identityMemoryBacked("/dev/shm"); err != nil || !got {
		t.Fatalf("/dev/shm: %v, %v", got, err)
	}
	if _, err := identityMemoryBacked("/no/such/path"); err == nil {
		t.Fatal("a path that does not exist was answered")
	}
}
