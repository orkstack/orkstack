//go:build unix

package provider_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

// TestDir_ModesDoNotDependOnTheUmask: under a restrictive umask (a hardened
// service unit's 0077), a bundle's files are still 0444, so the container's
// own uid can read them.
func TestDir_ModesDoNotDependOnTheUmask(t *testing.T) {
	old := syscall.Umask(0o077)
	defer syscall.Umask(old)
	d, err := provider.NewDir(tempRoot(t), "db")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put(provider.Bundle{Files: map[string][]byte{"password": []byte("p")}}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"password", "expiration"} {
		info, err := os.Stat(filepath.Join(d.Path(), name))
		if err != nil || info.Mode().Perm() != 0o444 {
			t.Errorf("%s mode = %v, %v; want 0444 whatever the umask", name, info.Mode().Perm(), err)
		}
	}
}
