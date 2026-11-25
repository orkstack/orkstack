package provider_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

// tempRoot is a temporary directory that is removed whatever the modes the
// credential directories under it leave.
func tempRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("", "credentials-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := provider.RemoveTree(root); err != nil {
			t.Errorf("remove %s: %v", root, err)
		}
	})
	return root
}

func readCredential(t *testing.T, dir, file string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, file))
	if err != nil {
		t.Fatalf("read %s through its link: %v", file, err)
	}
	return string(b)
}

func entries(t *testing.T, dir string) []string {
	t.Helper()
	list, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range list {
		names = append(names, e.Name())
	}
	slices.Sort(names)
	return names
}

func TestDir_PutReplacesTheWholeSetAndKeepsThePreviousVersion(t *testing.T) {
	root := filepath.Join(tempRoot(t), "credentials")
	d, err := provider.NewDir(root, "db")
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	put := func(password string, files ...string) {
		t.Helper()
		b := provider.Bundle{Files: map[string][]byte{"password": []byte(password)}}
		for _, f := range files {
			b.Files[f] = []byte(f + "-" + password)
		}
		if err := d.Put(b, expires); err != nil {
			t.Fatal(err)
		}
	}
	put("one", "username")
	if got := readCredential(t, d.Path(), "password"); got != "one" {
		t.Fatalf("password = %q", got)
	}
	if got := readCredential(t, d.Path(), "expiration"); got != "2026-11-17T12:00:00Z\n" {
		t.Fatalf("expiration = %q", got)
	}
	if target, _ := os.Readlink(filepath.Join(d.Path(), "current")); target != ".v1" {
		t.Fatalf("current -> %q", target)
	}
	put("two")
	if got := readCredential(t, d.Path(), "password"); got != "two" {
		t.Fatalf("password after the second Put = %q", got)
	}
	if _, err := os.Lstat(filepath.Join(d.Path(), "username")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a file the new bundle lacks kept its link: %v", err)
	}
	put("three", "username")
	if got := strings.Join(entries(t, d.Path()), ","); got != ".v2,.v3,current,expiration,password,username" {
		t.Errorf("entries = %s; want the new version, the one before, current and the links", got)
	}
	// Modes: the container must not be able to change anything.
	for _, path := range []string{d.Path(), filepath.Join(d.Path(), ".v3")} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o555 {
			t.Errorf("%s mode = %v, %v; want 0555", path, info.Mode().Perm(), err)
		}
	}
	info, err := os.Stat(filepath.Join(d.Path(), ".v3", "password"))
	if err != nil || info.Mode().Perm() != 0o444 {
		t.Errorf("a file's mode = %v, %v; want 0444", info.Mode().Perm(), err)
	}
	if err := d.Remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(d.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Remove left %s: %v", d.Path(), err)
	}
}

func TestDir_NoExpirationForABundleThatDoesNotExpire(t *testing.T) {
	d, err := provider.NewDir(tempRoot(t), "vault")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put(provider.Bundle{Files: map[string][]byte{"token": []byte("t")}}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := d.Put(provider.Bundle{Files: map[string][]byte{"token": []byte("t2")}}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(d.Path(), "expiration")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expiration of a bundle that does not expire: %v; want none", err)
	}
}

// TestDir_AFaultBeforeTheSwapLeavesTheOldSetWhole: a write fault between a
// version being written and `current` moving (a crash there leaves the same
// tree) leaves every file the old one's, and the next Put starts clean.
func TestDir_AFaultBeforeTheSwapLeavesTheOldSetWhole(t *testing.T) {
	d, err := provider.NewDir(tempRoot(t), "db")
	if err != nil {
		t.Fatal(err)
	}
	old := provider.Bundle{Files: map[string][]byte{"username": []byte("u1"), "password": []byte("p1")}}
	if err := d.Put(old, time.Time{}); err != nil {
		t.Fatal(err)
	}
	provider.SetDirAfterWrite(d, func() error { return errors.New("disk full") })
	next := provider.Bundle{Files: map[string][]byte{"username": []byte("u2"), "password": []byte("p2"), "extra": []byte("e2")}}
	if err := d.Put(next, time.Time{}); err == nil {
		t.Fatal("Put succeeded through a write fault")
	}
	if u, p := readCredential(t, d.Path(), "username"), readCredential(t, d.Path(), "password"); u != "u1" || p != "p1" {
		t.Fatalf("after the fault the set is %q/%q; want the old one whole", u, p)
	}
	if _, err := os.Stat(filepath.Join(d.Path(), "extra")); err == nil {
		t.Error("a name only the new set holds is readable before the swap")
	}
	provider.SetDirAfterWrite(d, nil)
	if err := d.Put(next, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if u, p, e := readCredential(t, d.Path(), "username"), readCredential(t, d.Path(), "password"), readCredential(t, d.Path(), "extra"); u != "u2" || p != "p2" || e != "e2" {
		t.Fatalf("after the retry the set is %q/%q/%q; want the new one whole", u, p, e)
	}
}

func TestDir_RefusesABundleItCannotPutInPlace(t *testing.T) {
	d, err := provider.NewDir(tempRoot(t), "db")
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []provider.Bundle{
		{},
		{Files: map[string][]byte{"../escape": []byte("x")}},
		{Files: map[string][]byte{"current": []byte("x")}},
	} {
		if err := d.Put(b, time.Time{}); err == nil {
			t.Errorf("Put(%v) succeeded", b.Files)
		}
	}
	if _, err := provider.NewDir(tempRoot(t), "../up"); err == nil {
		t.Error("NewDir accepted the name ../up")
	}
}
