package orker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// What an Orker reports its identity storage as (DescribeOrker).
const (
	identityStorageMemory = "memory"
	identityStorageDisk   = "disk"
)

// identityStorageChoice is where the directories of identity/, credentials/
// and a provider image's state/ are made. Dir "" is the attempt's own root
// in the work root.
type identityStorageChoice struct {
	Dir  string
	Kind string
	// Warning, when set, is logged at warning at boot: a fallback under auto
	// says why, and an explicit path that is not memory-backed says so at
	// every boot.
	Warning string
}

// identityStorageProbe is what the choice reads from the host.
type identityStorageProbe struct {
	uid int
	// runtimeDir is $XDG_RUNTIME_DIR; "" when unset.
	runtimeDir string
	// shmDir is /dev/shm; "" where there is none.
	shmDir string
	// memoryBacked reports whether the filesystem under path is tmpfs or
	// ramfs.
	memoryBacked func(path string) (bool, error)
	// readable shows a container can read a file the Orker put in dir, by
	// binding one into the boot's probe container.
	readable func(ctx context.Context, dir string) error
}

// chooseIdentityStorage decides orker.identity.dir at boot
// (orker.identity.dir): auto takes the first of
// $XDG_RUNTIME_DIR/orkstack and /dev/shm/orkstack-<uid> that passes the
// checks, else the work root; workroot is the work root; an absolute path is
// that directory, which must exist. An error refuses the boot.
func chooseIdentityStorage(ctx context.Context, setting string, p identityStorageProbe) (identityStorageChoice, error) {
	switch setting {
	case "workroot":
		return identityStorageChoice{Kind: identityStorageDisk}, nil
	case "auto":
		return chooseIdentityStorageAuto(ctx, p)
	}
	if !filepath.IsAbs(setting) {
		return identityStorageChoice{}, fmt.Errorf("orker.identity.dir %q is neither auto, workroot nor an absolute path", setting)
	}
	dir := filepath.Clean(setting)
	if _, err := os.Lstat(dir); errors.Is(err, os.ErrNotExist) {
		return identityStorageChoice{}, fmt.Errorf("orker.identity.dir %s must exist", dir)
	}
	if err := ownIdentityDir(dir, p.uid, false); err != nil {
		return identityStorageChoice{}, fmt.Errorf("orker.identity.dir %s: %w", dir, err)
	}
	if err := explicitIdentityDirSafe(dir, p.uid); err != nil {
		return identityStorageChoice{}, fmt.Errorf("orker.identity.dir %s: %w", dir, err)
	}
	if err := p.readable(ctx, dir); err != nil {
		return identityStorageChoice{}, fmt.Errorf("orker.identity.dir %s: a container could not read a file put there: %w", dir, err)
	}
	memory, err := p.memoryBacked(dir)
	if err != nil {
		return identityStorageChoice{}, fmt.Errorf("orker.identity.dir %s: read its filesystem: %w", dir, err)
	}
	if !memory {
		return identityStorageChoice{Dir: dir, Kind: identityStorageDisk,
			Warning: fmt.Sprintf("orker.identity.dir %s is not memory-backed: identity tokens and credentials are written to disk there", dir)}, nil
	}
	return identityStorageChoice{Dir: dir, Kind: identityStorageMemory}, nil
}

func chooseIdentityStorageAuto(ctx context.Context, p identityStorageProbe) (identityStorageChoice, error) {
	var candidates []string
	if p.runtimeDir != "" && filepath.IsAbs(p.runtimeDir) {
		candidates = append(candidates, filepath.Join(p.runtimeDir, "orkstack"))
	}
	if p.shmDir != "" {
		candidates = append(candidates, filepath.Join(p.shmDir, "orkstack-"+strconv.Itoa(p.uid)))
	}
	var why []string
	for _, dir := range candidates {
		err := identityCandidate(ctx, dir, p)
		if err == nil {
			return identityStorageChoice{Dir: dir, Kind: identityStorageMemory}, nil
		}
		if errors.Is(err, errIdentityProbeNotRun) {
			return identityStorageChoice{}, fmt.Errorf("orker.identity.dir auto, %s: %w", dir, err)
		}
		why = append(why, fmt.Sprintf("%s: %v", dir, err))
	}
	if len(why) == 0 {
		why = append(why, "no XDG_RUNTIME_DIR and no /dev/shm")
	}
	return identityStorageChoice{Kind: identityStorageDisk,
		Warning: "no memory-backed directory passed the checks, so identity tokens and credentials are written under the work root (" + strings.Join(why, "; ") + ")"}, nil
}

// identityCandidate checks one auto candidate: memory-backed, the Orker's
// own at 0700 (made when absent), and readable from a container.
func identityCandidate(ctx context.Context, dir string, p identityStorageProbe) error {
	// The parent first, so nothing is made on a disk; then the directory
	// itself, which a mount of its own could put elsewhere.
	if memory, err := p.memoryBacked(filepath.Dir(dir)); err != nil || !memory {
		return notMemoryBacked(err)
	}
	if err := ownIdentityDir(dir, p.uid, true); err != nil {
		return err
	}
	if memory, err := p.memoryBacked(dir); err != nil || !memory {
		return notMemoryBacked(err)
	}
	if err := p.readable(ctx, dir); err != nil {
		return fmt.Errorf("a container could not read a file put there: %w", err)
	}
	return nil
}

// ownIdentityDir holds dir to what holds tokens: a directory, not a link,
// owned by uid, mode 0700. create makes it when absent.
func ownIdentityDir(dir string, uid int, create bool) error {
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) && create {
		if err := os.Mkdir(dir, 0o700); err != nil {
			return fmt.Errorf("make it: %w", err)
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return fmt.Errorf("set its mode: %w", err)
		}
		info, err = os.Lstat(dir)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("is a link, which is never followed")
	}
	if !info.IsDir() {
		return errors.New("is not a directory")
	}
	owner, ok := ownerUID(info)
	if !ok {
		return errors.New("its owner cannot be read on this system")
	}
	if int(owner) != uid {
		return fmt.Errorf("belongs to uid %d, not the Orker's %d", owner, uid)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		return fmt.Errorf("has mode %04o, not 0700", perm)
	}
	return nil
}

func notMemoryBacked(err error) error {
	if err != nil {
		return fmt.Errorf("read its filesystem: %w", err)
	}
	return errors.New("not memory-backed")
}

// explicitIdentityDirSafe refuses an explicit orker.identity.dir with an
// ancestor that another user can write without the sticky bit, where the
// directory could be swapped after the boot checked it.
func explicitIdentityDirSafe(dir string, uid int) error {
	resolved, err := filepath.EvalSymlinks(filepath.Dir(dir))
	if err != nil {
		return fmt.Errorf("resolve %s: %w", filepath.Dir(dir), err)
	}
	for p := resolved; ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil {
			return fmt.Errorf("read %s: %w", p, err)
		}
		if mode := info.Mode(); mode.Perm()&0o022 != 0 && mode&os.ModeSticky == 0 {
			return fmt.Errorf("its ancestor %s is writable by other users (%04o) and not sticky, so the directory could be replaced", p, mode.Perm())
		}
		if owner, ok := ownerUID(info); !ok || (owner != 0 && int(owner) != uid) {
			return fmt.Errorf("its ancestor %s belongs to neither root nor this Orker's user, so its owner could replace the directory", p)
		}
		if p == filepath.Dir(p) {
			return nil
		}
	}
}
