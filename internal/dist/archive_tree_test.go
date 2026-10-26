package dist_test

import (
	"archive/tar"
	"bytes"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/dist"
)

func treeEntries() []dist.Entry {
	return []dist.Entry{
		{Name: "examples/b.ork.yaml", Data: []byte("b\n"), Mode: 0o644},
		{Name: "compose.yaml", Data: []byte("services: {}\n"), Mode: 0o644},
		{Name: ".env", Data: []byte("A=1\n"), Mode: 0o644},
		{Name: "examples/a.ork.yaml", Data: []byte("a\n"), Mode: 0o644},
		{Name: "LICENSE", Data: fakeLicense, Mode: 0o644},
	}
}

// TestTarGzTree_IsADeterministicRootOwnedTree: directories come before what
// is in them, every entry is sorted, owned by 0:0 with no names, stamped with
// the commit time, and the bytes do not depend on the order entries are given.
func TestTarGzTree_IsADeterministicRootOwnedTree(t *testing.T) {
	t.Parallel()
	data, err := dist.TarGzTree("bundle", treeEntries(), commitTime)
	if err != nil {
		t.Fatal(err)
	}
	headers, contents := readTarGz(t, data)
	var names []string
	for _, h := range headers {
		names = append(names, h.Name)
		if h.Uid != 0 || h.Gid != 0 || h.Uname != "" || h.Gname != "" {
			t.Errorf("%s is owned by %d:%d %q:%q, want root-owned and nameless", h.Name, h.Uid, h.Gid, h.Uname, h.Gname)
		}
		if !h.ModTime.Equal(commitTime) {
			t.Errorf("%s has mtime %v, want the commit time %v", h.Name, h.ModTime, commitTime)
		}
		if h.Format != tar.FormatUSTAR {
			t.Errorf("%s is written as format %v, want plain USTAR", h.Name, h.Format)
		}
		switch {
		case strings.HasSuffix(h.Name, "/"):
			if h.Typeflag != tar.TypeDir || h.Mode != 0o755 || h.Size != 0 {
				t.Errorf("directory %s: type %q mode %o size %d, want a 0755 directory", h.Name, h.Typeflag, h.Mode, h.Size)
			}
		default:
			if h.Typeflag != tar.TypeReg || h.Mode != 0o644 {
				t.Errorf("file %s: type %q mode %o, want a regular 0644 file", h.Name, h.Typeflag, h.Mode)
			}
		}
	}
	want := []string{
		"bundle/", "bundle/.env", "bundle/LICENSE", "bundle/compose.yaml",
		"bundle/examples/", "bundle/examples/a.ork.yaml", "bundle/examples/b.ork.yaml",
	}
	if !slices.Equal(names, want) {
		t.Errorf("entries %q, want %q", names, want)
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("entries are not sorted: %q", names)
	}
	if string(contents["bundle/examples/a.ork.yaml"]) != "a\n" || string(contents["bundle/.env"]) != "A=1\n" {
		t.Errorf("contents are wrong: %q", contents)
	}

	reversed := treeEntries()
	slices.Reverse(reversed)
	for i, entries := range [][]dist.Entry{treeEntries(), reversed} {
		again, err := dist.TarGzTree("bundle", entries, commitTime)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(again, data) {
			t.Errorf("run %d gave different bytes", i)
		}
	}
	other, err := dist.TarGzTree("bundle", treeEntries(), commitTime.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(other, data) {
		t.Error("a different commit time gave the same bytes")
	}
}

func TestTarGzTree_RefusesWhatIsNotATree(t *testing.T) {
	t.Parallel()
	file := func(name string) dist.Entry { return dist.Entry{Name: name, Data: []byte("x"), Mode: 0o644} }
	for _, tc := range []struct {
		name    string
		root    string
		entries []dist.Entry
		mtime   time.Time
	}{
		{"no commit time", "bundle", treeEntries(), time.Time{}},
		{"no root", "", treeEntries(), commitTime},
		{"a nested root", "a/b", treeEntries(), commitTime},
		{"a root that climbs", "..", treeEntries(), commitTime},
		{"a dot root", ".", treeEntries(), commitTime},
		{"a root with a backslash", `a\b`, treeEntries(), commitTime},
		{"an empty name", "bundle", []dist.Entry{file("")}, commitTime},
		{"an absolute name", "bundle", []dist.Entry{file("/etc/passwd")}, commitTime},
		{"a name that climbs", "bundle", []dist.Entry{file("../escape")}, commitTime},
		{"a name that climbs in the middle", "bundle", []dist.Entry{file("a/../../escape")}, commitTime},
		{"an empty segment", "bundle", []dist.Entry{file("a//b")}, commitTime},
		{"a dot segment", "bundle", []dist.Entry{file("./a")}, commitTime},
		{"a trailing slash", "bundle", []dist.Entry{file("a/")}, commitTime},
		{"a backslash", "bundle", []dist.Entry{file(`a\b`)}, commitTime},
		{"a NUL", "bundle", []dist.Entry{file("a\x00b")}, commitTime},
		{"a duplicate", "bundle", []dist.Entry{file("a"), file("a")}, commitTime},
		{"a file that is also a directory", "bundle", []dist.Entry{file("a"), file("a/b")}, commitTime},
		{"no entries", "bundle", nil, commitTime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if data, err := dist.TarGzTree(tc.root, tc.entries, tc.mtime); err == nil {
				t.Errorf("accepted it, wrote %d bytes", len(data))
			}
		})
	}
}
