//go:build unix

package orker

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLockIdentity_HeldIsOnlyALiveHolder: a second holder of the lock is
// errIdentityHeld, and any other failure of the lock says what failed rather
// than sending the operator to look for a second Orker that does not exist.
func TestLockIdentity_HeldIsOnlyALiveHolder(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "identity.lock")
	open := func() *os.File {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		return file
	}

	first := open()
	if err := lockIdentity(first); err != nil {
		t.Fatalf("first lock: %v", err)
	}
	// flock belongs to the open file description, so a second open in this
	// process contends exactly as a second process would.
	if err := lockIdentity(open()); !errors.Is(err, errIdentityHeld) {
		t.Fatalf("second holder: %v, want errIdentityHeld", err)
	}

	closed := open()
	_ = closed.Close()
	err := lockIdentity(closed)
	if err == nil || errors.Is(err, errIdentityHeld) {
		t.Fatalf("a lock that cannot be taken at all: %v, want an error other than errIdentityHeld", err)
	}
	if !strings.HasPrefix(err.Error(), ReasonWorkdirFailed+":") {
		t.Fatalf("error %q does not carry %s", err, ReasonWorkdirFailed)
	}
}
