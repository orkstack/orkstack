package provider

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ContainerDir is where a container sees its credentials.
const ContainerDir = "/orkstack/credentials"

// Dir is one credential's directory on the Orker's side,
// <attempt root>/credentials/<name>/, which the container sees read-only at
// /orkstack/credentials/<name>/:
//
//	<name>/                       0555
//	├── <file> -> current/<file>  one link per file of the bundle
//	├── expiration -> current/expiration   RFC 3339; absent when the bundle does not expire
//	├── current -> .v3            replaced by one rename(2)
//	└── .v3/                      0555, files 0444
//
// Put writes a whole bundle into a new version directory and moves `current`
// with one rename(2), so on disk a bundle is always a whole set, the old or
// the new, a crash between any two steps included. The previous version
// stays until the Put after, because a process may have opened a file of it.
// A Dir is not safe for concurrent use: one refresh loop owns it.
type Dir struct {
	path string
	n    int
	// afterWrite is a test hook: it runs once a version directory is whole
	// and its links are made, just before `current` moves; an error it
	// returns is a write fault there.
	afterWrite func() error
}

// NewDir creates the directory of one credential under root
// (<attempt root>/credentials). name must be a valid credential name.
func NewDir(root, name string) (*Dir, error) {
	if err := ValidCredentialName(name); err != nil {
		return nil, err
	}
	path := filepath.Join(root, name)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	if err := os.Mkdir(path, 0o555); err != nil {
		return nil, err
	}
	return &Dir{path: path}, nil
}

// Path is the directory's path on the Orker's side.
func (d *Dir) Path() string { return d.path }

// Put writes b into a new version and makes it current. expires is the wall
// time written to `expiration`; the zero time writes none, for a bundle that
// does not expire. On an error `current` still names the version it named
// before, and the partial version is removed.
func (d *Dir) Put(b Bundle, expires time.Time) (err error) {
	if err := ValidBundle(b); err != nil {
		return err
	}
	next := d.n + 1
	if err := os.Chmod(d.path, 0o755); err != nil {
		return err
	}
	defer func() {
		if cerr := os.Chmod(d.path, 0o555); cerr != nil && err == nil {
			// After the swap the set is in place whatever this says.
			if d.n == next {
				err = &CleanupError{Err: cerr}
			} else {
				err = cerr
			}
		}
	}()
	version := ".v" + strconv.Itoa(next)
	vpath := filepath.Join(d.path, version)
	_ = removeTree(vpath) // a version a crashed Put left behind
	if err := os.Mkdir(vpath, 0o755); err != nil {
		return err
	}
	written := false
	defer func() {
		if !written {
			_ = removeTree(vpath)
		}
	}()
	names := sortedNames(b.Files)
	for _, name := range names {
		if err := writeFile(filepath.Join(vpath, name), b.Files[name]); err != nil {
			return err
		}
	}
	if !expires.IsZero() {
		if err := writeFile(filepath.Join(vpath, "expiration"), []byte(expires.UTC().Format(time.RFC3339)+"\n")); err != nil {
			return err
		}
		names = append(names, "expiration")
	}
	if err := os.Chmod(vpath, 0o555); err != nil {
		return err
	}
	// The links come before the swap, so the moment `current` moves every
	// name of the new set already resolves through it. A link to a name the
	// old set lacks dangles until then, which a reader sees as the old set
	// (that name absent), never as a mixed one.
	want := map[string]bool{}
	for _, name := range names {
		want[name] = true
		link := filepath.Join(d.path, name)
		if target, err := os.Readlink(link); err == nil && target == filepath.Join("current", name) {
			continue
		}
		_ = os.Remove(link)
		if err := os.Symlink(filepath.Join("current", name), link); err != nil {
			return err
		}
	}
	if d.afterWrite != nil {
		if err := d.afterWrite(); err != nil {
			return err
		}
	}
	tmp := filepath.Join(d.path, ".current-"+strconv.Itoa(next))
	_ = os.Remove(tmp)
	if err := os.Symlink(version, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(d.path, "current")); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	// From here the new set is in place: whatever fails is clean-up, never a
	// failed refresh, so the caller records the bundle as the one on disk.
	written = true
	previous := d.n
	d.n = next
	entries, err := os.ReadDir(d.path)
	if err != nil {
		return &CleanupError{Err: err}
	}
	var errs []error
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case name == "current", want[name]:
		case strings.HasPrefix(name, ".v"):
			// Keep the new version and the one before it.
			n, convErr := strconv.Atoi(strings.TrimPrefix(name, ".v"))
			if convErr == nil && (n == next || n == previous) {
				continue
			}
			errs = append(errs, removeTree(filepath.Join(d.path, name)))
		case entry.Type()&os.ModeSymlink != 0:
			errs = append(errs, os.Remove(filepath.Join(d.path, name)))
		}
	}
	if cleanup := errors.Join(errs...); cleanup != nil {
		return &CleanupError{Err: cleanup}
	}
	return nil
}

// CleanupError is a Put that put the bundle in place and could not remove
// an older version. It is logged, never counted as a failed refresh.
type CleanupError struct{ Err error }

func (e *CleanupError) Error() string {
	return "credential directory: removing an old version: " + e.Err.Error()
}
func (e *CleanupError) Unwrap() error { return e.Err }

// Remove removes the whole directory. The version directories are 0555 with
// 0444 files, so each is made writable first, as the sealed input tree is.
func (d *Dir) Remove() error { return removeTree(d.path) }

func writeFile(path string, content []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o444)
	if err != nil {
		return err
	}
	// The mode is set, not left to the umask: under a restrictive umask
	// (0077) the container's own uid could not read a 0400 file.
	if err := f.Chmod(0o444); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(content); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func sortedNames(files map[string][]byte) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// removeTree removes path whatever its modes: each directory is made
// writable before its entries are removed. Symlinks are removed, never
// followed.
func removeTree(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir() {
		if err := os.Chmod(path, 0o755); err != nil {
			return err
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := removeTree(filepath.Join(path, entry.Name())); err != nil {
				return err
			}
		}
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", path, err)
	}
	return nil
}
