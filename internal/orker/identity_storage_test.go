//go:build unix

package orker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// storageProbe is an identityStorageProbe over real directories, where
// memory says which paths a statfs reports as memory-backed and unreadable
// which ones a container cannot read.
func storageProbe(t *testing.T, runtimeDir, shmDir string, memory, unreadable map[string]bool) identityStorageProbe {
	t.Helper()
	return identityStorageProbe{
		uid:        os.Getuid(),
		runtimeDir: runtimeDir,
		shmDir:     shmDir,
		memoryBacked: func(path string) (bool, error) {
			for p := range memory {
				if path == p || strings.HasPrefix(path, p+"/") {
					return true, nil
				}
			}
			return false, nil
		},
		readable: func(_ context.Context, dir string) error {
			if unreadable[dir] {
				return errors.New("the probe container could not read the file")
			}
			return nil
		},
	}
}

// TestChooseIdentityStorage_Auto: auto takes the first candidate that is
// memory-backed, the Orker's own and readable from a container, creating it
// 0700 when absent; with none it falls back to the work root and says why.
func TestChooseIdentityStorage_Auto(t *testing.T) {
	run, shm := t.TempDir(), t.TempDir()
	runCand := filepath.Join(run, "orkstack")
	shmCand := filepath.Join(shm, "orkstack-"+strconv.Itoa(os.Getuid()))

	got, err := chooseIdentityStorage(context.Background(), "auto", storageProbe(t, run, shm, map[string]bool{run: true, shm: true}, nil))
	if err != nil || got.Dir != runCand || got.Kind != identityStorageMemory || got.Warning != "" {
		t.Fatalf("both memory-backed: %+v, %v", got, err)
	}
	assertMode(t, runCand, 0o700)

	got, err = chooseIdentityStorage(context.Background(), "auto", storageProbe(t, run, shm, map[string]bool{shm: true}, nil))
	if err != nil || got.Dir != shmCand || got.Kind != identityStorageMemory {
		t.Fatalf("only /dev/shm memory-backed: %+v, %v", got, err)
	}

	got, err = chooseIdentityStorage(context.Background(), "auto", storageProbe(t, run, shm, map[string]bool{run: true, shm: true}, map[string]bool{runCand: true, shmCand: true}))
	if err != nil || got.Dir != "" || got.Kind != identityStorageDisk || !strings.Contains(got.Warning, "could not read") {
		t.Fatalf("no candidate a container can read: %+v, %v", got, err)
	}

	got, err = chooseIdentityStorage(context.Background(), "auto", storageProbe(t, "", shm, nil, nil))
	if err != nil || got.Dir != "" || got.Kind != identityStorageDisk || !strings.Contains(got.Warning, "memory-backed") {
		t.Fatalf("nothing memory-backed, no XDG_RUNTIME_DIR: %+v, %v", got, err)
	}
}

// TestChooseIdentityStorage_CandidateRefusals: a candidate that is a link,
// not a directory, or open to anyone else is passed over, never used.
func TestChooseIdentityStorage_CandidateRefusals(t *testing.T) {
	for name, setup := range map[string]func(t *testing.T, cand string){
		"a link": func(t *testing.T, cand string) {
			if err := os.Symlink(t.TempDir(), cand); err != nil {
				t.Fatal(err)
			}
		},
		"a file": func(t *testing.T, cand string) {
			if err := os.WriteFile(cand, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"mode 0755": func(t *testing.T, cand string) {
			if err := os.Mkdir(cand, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(cand, 0o755); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			run := t.TempDir()
			cand := filepath.Join(run, "orkstack")
			setup(t, cand)
			got, err := chooseIdentityStorage(context.Background(), "auto", storageProbe(t, run, "", map[string]bool{run: true}, nil))
			if err != nil || got.Kind != identityStorageDisk || got.Dir != "" || got.Warning == "" {
				t.Fatalf("%+v, %v", got, err)
			}
		})
	}
}

// TestChooseIdentityStorage_Explicit: workroot is always the work root; an
// absolute path must exist and pass the ownership checks, and one that is
// not memory-backed is used and warned of; anything else is refused.
func TestChooseIdentityStorage_Explicit(t *testing.T) {
	got, err := chooseIdentityStorage(context.Background(), "workroot", storageProbe(t, t.TempDir(), "", nil, nil))
	if err != nil || got.Dir != "" || got.Kind != identityStorageDisk || got.Warning != "" {
		t.Fatalf("workroot: %+v, %v", got, err)
	}

	mem := filepath.Join(t.TempDir(), "ids")
	if err := os.Mkdir(mem, 0o700); err != nil {
		t.Fatal(err)
	}
	got, err = chooseIdentityStorage(context.Background(), mem, storageProbe(t, "", "", map[string]bool{mem: true}, nil))
	if err != nil || got.Dir != mem || got.Kind != identityStorageMemory || got.Warning != "" {
		t.Fatalf("a memory-backed path: %+v, %v", got, err)
	}
	got, err = chooseIdentityStorage(context.Background(), mem, storageProbe(t, "", "", nil, nil))
	if err != nil || got.Dir != mem || got.Kind != identityStorageDisk || !strings.Contains(got.Warning, "not memory-backed") {
		t.Fatalf("a disk path: %+v, %v", got, err)
	}

	open := filepath.Join(t.TempDir(), "open")
	if err := os.Mkdir(open, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o750); err != nil {
		t.Fatal(err)
	}
	for setting, want := range map[string]string{
		"relative/dir":                       "absolute",
		"":                                   "absolute",
		filepath.Join(t.TempDir(), "absent"): "must exist",
		open:                                 "0700",
	} {
		if _, err := chooseIdentityStorage(context.Background(), setting, storageProbe(t, "", "", nil, nil)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: %v, want %q", setting, err, want)
		}
	}
	if _, err := chooseIdentityStorage(context.Background(), mem, storageProbe(t, "", "", map[string]bool{mem: true}, map[string]bool{mem: true})); err == nil {
		t.Error("an explicit path no container can read was accepted")
	}
}

// TestExplicitIdentityDirSafe: an explicit directory under a parent other
// users may write, without the sticky bit, is refused; under a sticky one it
// is not.
func TestExplicitIdentityDirSafe(t *testing.T) {
	open := filepath.Join(t.TempDir(), "open")
	if err := os.Mkdir(open, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := explicitIdentityDirSafe(filepath.Join(open, "ids"), os.Getuid()); err == nil || !strings.Contains(err.Error(), "writable by other users") {
		t.Fatalf("a world-writable parent: %v", err)
	}
	if err := os.Chmod(open, 0o777|os.ModeSticky); err != nil {
		t.Fatal(err)
	}
	if err := explicitIdentityDirSafe(filepath.Join(open, "ids"), os.Getuid()); err != nil {
		t.Fatalf("a sticky parent: %v", err)
	}
}
