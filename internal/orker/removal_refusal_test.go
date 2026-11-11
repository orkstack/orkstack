//go:build unix

package orker

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// statInfo is an os.FileInfo whose owner is whatever the case says, which no
// unprivileged test could make a real file have.
type statInfo struct {
	mode fs.FileMode
	stat *syscall.Stat_t
}

func (s statInfo) Name() string       { return "x" }
func (s statInfo) Size() int64        { return 0 }
func (s statInfo) Mode() fs.FileMode  { return s.mode }
func (s statInfo) ModTime() time.Time { return time.Time{} }
func (s statInfo) IsDir() bool        { return s.mode.IsDir() }
func (s statInfo) Sys() any {
	if s.stat == nil {
		return nil
	}
	return s.stat
}

// TestDescribeRemovalRefusal: the file a refused unlink names is not what
// refused it; its directory is, so the message names that directory, its mode
// and its owner. When another user owns it, the message says so and says what
// fixes it, because that is the one case where the Orker can neither write nor
// chmod the directory and nothing it does will ever remove it.
func TestDescribeRemovalRefusal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		info     os.FileInfo
		orkerUID int
		want     []string
		absent   []string
	}{
		{
			name:     "a directory a workload made as another user",
			info:     statInfo{mode: fs.ModeDir | 0o755, stat: &syscall.Stat_t{Uid: 65532, Gid: 65532}},
			orkerUID: 1000,
			want: []string{
				"/w/ork-run-t-1-x/tmp/repo/.git/objects (mode=755 owner=65532:65532) refused it",
				"owned by uid 65532, not the Orker's uid 1000",
				"can neither write nor chmod",
				"remove its own scratch",
				"§10.1, rule 11",
			},
		},
		{
			name:     "a directory the Orker owns",
			info:     statInfo{mode: fs.ModeDir | 0o555, stat: &syscall.Stat_t{Uid: 1000, Gid: 1000}},
			orkerUID: 1000,
			want:     []string{"/w/ork-run-t-1-x/tmp/repo/.git/objects (mode=555 owner=1000:1000) refused it"},
			absent:   []string{"not the Orker's uid", "rule 11"},
		},
		{
			name:     "no owner reported",
			info:     statInfo{mode: fs.ModeDir | 0o555},
			orkerUID: 1000,
			want:     []string{"/w/ork-run-t-1-x/tmp/repo/.git/objects (mode=555) refused it"},
			absent:   []string{"owner=", "not the Orker's uid"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := describeRemovalRefusal("/w/ork-run-t-1-x/tmp/repo/.git/objects", tc.info, tc.orkerUID)
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("describeRemovalRefusal = %q; want it to say %q", got, want)
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(got, absent) {
					t.Errorf("describeRemovalRefusal = %q; it must not say %q", got, absent)
				}
			}
		})
	}
}

// TestRemovalRefusal_NamesTheDirectoryOfTheRefusedPath: removalRefusal finds
// the directory from the error os.RemoveAll returns, and says nothing for an
// error that is not a permission refusal on a path whose directory exists.
func TestRemovalRefusal_NamesTheDirectoryOfTheRefusedPath(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	refused := &fs.PathError{Op: "unlinkat", Path: filepath.Join(dir, "f"), Err: syscall.EACCES}

	got := removalRefusal(fmt.Errorf("remove: %w", refused))
	if want := dir + " (mode=555"; !strings.Contains(got, want) {
		t.Errorf("removalRefusal = %q; want it to start from %q", got, want)
	}

	// A directory that cannot be opened is itself what refused the removal,
	// not the directory above it.
	sealed := filepath.Join(t.TempDir(), "sealed")
	if err := os.Mkdir(sealed, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sealed, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sealed, 0o700) })
	for _, op := range []string{"openfdat", "open", "readdirent"} {
		got := removalRefusal(&fs.PathError{Op: op, Path: sealed, Err: syscall.EACCES})
		if want := sealed + " (mode=300"; !strings.Contains(got, want) {
			t.Errorf("removalRefusal(%s %s) = %q; want it to name the directory itself, %q", op, sealed, got, want)
		}
	}
	for _, err := range []error{
		nil,
		errors.New("disk on fire"),
		&fs.PathError{Op: "unlinkat", Path: filepath.Join(dir, "f"), Err: syscall.EBUSY},
		&fs.PathError{Op: "unlinkat", Path: filepath.Join(dir, "gone", "f"), Err: syscall.EACCES},
	} {
		if got := removalRefusal(err); got != "" {
			t.Errorf("removalRefusal(%v) = %q; want nothing", err, got)
		}
	}
}

// TestReleasePinDirectory_NamesTheDirectoryThatRefusedIt: a pin directory the
// Orker cannot remove is reported with the directory that refused the unlink,
// which for a pinned checkout a workload made as another user is a directory
// inside shared/, not the pin directory the error starts from.
func TestReleasePinDirectory_NamesTheDirectoryThatRefusedIt(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root, which can remove a directory from one it cannot write; the check is about a non-root Orker")
	}
	root := t.TempDir()
	pin, err := AcquirePin(root, "ork-one", "build", testInstanceID, 3600, "orker-a")
	if err != nil {
		t.Fatal(err)
	}
	// The work root itself is what the Orker cannot write here: the one
	// directory removeSealedTree, which opens only the tree, never changes.
	if err := os.Chmod(root, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o755) })

	_, err = ReleasePinDirectory(root, "ork-one", "build", testInstanceID)
	if err == nil {
		t.Fatalf("ReleasePinDirectory = nil under a read-only work root; want a refusal (pin %s)", pin.Shared)
	}
	if want := root + " (mode=555"; !strings.Contains(err.Error(), want) {
		t.Errorf("ReleasePinDirectory = %v; want it to name %q", err, want)
	}
}
