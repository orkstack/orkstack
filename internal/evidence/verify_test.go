package evidence

import (
	"archive/tar"
	"bytes"
	"io/fs"
	"os"
	"strings"
	"testing"
)

// E5
func TestVerify_AcceptsWhatWriteProduced(t *testing.T) {
	f := newFixture()
	report, err := Verify(bytes.NewReader(writeFixture(t, f)))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(report.Failures) != 0 {
		t.Fatalf("failures: %+v", report.Failures)
	}
	if !report.OK() {
		t.Fatal("OK() = false on a clean bundle")
	}
	if len(report.Verified) != f.manifest.Counts.Present {
		t.Fatalf("verified %d, want %d", len(report.Verified), f.manifest.Counts.Present)
	}
	if len(report.Reclaimed) != 1 || report.Reclaimed[0].Path != "tasks/reconcile/attempts/1/stderr.log" {
		t.Fatalf("reclaimed = %+v", report.Reclaimed)
	}
	if len(report.Unrecorded) != 1 || report.Unrecorded[0].Path != "tasks/extract/attempts/2.json" {
		t.Fatalf("unrecorded = %+v", report.Unrecorded)
	}
}

// E6
func TestVerify_FailsByName(t *testing.T) {
	f := newFixture()
	artifact := "tasks/reconcile/artifacts/rates.parquet"
	indexOf := func(entries []tarEntry, name string) int {
		for i, e := range entries {
			if e.name == name {
				return i
			}
		}
		t.Fatalf("no entry %s", name)
		return -1
	}
	type check struct {
		path, reason string
	}
	cases := []struct {
		name   string
		mutate func([]tarEntry) []tarEntry
		want   []check
	}{
		{"flipped byte", func(e []tarEntry) []tarEntry {
			e[indexOf(e, artifact)].body[7] ^= 0xff
			return e
		}, []check{{artifact, FailureDigestMismatch}}},
		{"truncated", func(e []tarEntry) []tarEntry {
			i := indexOf(e, artifact)
			e[i].body = e[i].body[:100]
			return e
		}, []check{{artifact, FailureSizeMismatch}}},
		{"removed", func(e []tarEntry) []tarEntry {
			i := indexOf(e, artifact)
			return append(e[:i], e[i+1:]...)
		}, []check{{artifact, FailureMissing}}},
		{"extra", func(e []tarEntry) []tarEntry {
			return append(e, tarEntry{name: "tasks/reconcile/artifacts/smuggled.txt", body: []byte("hi")})
		}, []check{{"tasks/reconcile/artifacts/smuggled.txt", FailureNotInManifest}}},
		{"reclaimed member present", func(e []tarEntry) []tarEntry {
			return append(e, tarEntry{name: "tasks/reconcile/attempts/1/stderr.log", body: []byte("gone forever")})
		}, []check{{"tasks/reconcile/attempts/1/stderr.log", FailureNotInManifest}}},
		{"swapped", func(e []tarEntry) []tarEntry {
			i, j := indexOf(e, "tasks/reconcile/values.json"), indexOf(e, artifact)
			e[i], e[j] = e[j], e[i]
			return e
		}, []check{{"tasks/reconcile/values.json", FailureOutOfOrder}}},
		{"manifest second", func(e []tarEntry) []tarEntry {
			e[0], e[1] = e[1], e[0]
			return e
		}, []check{{"run.jsonl", FailureManifestNotFirst}}},
		{"unsafe and symlink", func(e []tarEntry) []tarEntry {
			return append(e,
				tarEntry{name: "../x", body: []byte("escape")},
				tarEntry{name: "tasks/reconcile/link", typeflag: tar.TypeSymlink})
		}, []check{{"../x", FailureUnsafePath}, {"tasks/reconcile/link", FailureNotRegular}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			report, err := Verify(bytes.NewReader(buildTar(t, tc.mutate(fixtureEntries(t, f)))))
			if err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if len(report.Failures) != len(tc.want) {
				t.Fatalf("failures = %+v, want %+v", report.Failures, tc.want)
			}
			for i, want := range tc.want {
				got := report.Failures[i]
				if got.Path != want.path || got.Reason != want.reason {
					t.Errorf("failure %d = %s %q, want %s %q", i, got.Path, got.Reason, want.path, want.reason)
				}
			}
			t.Logf("%s: %+v", tc.name, report.Failures)
			if tc.name == "flipped byte" {
				got := report.Failures[0]
				if got.Want != f.manifest.Members[4].SHA256 || got.Got == "" || got.Got == got.Want {
					t.Errorf("digest mismatch want/got = %q/%q", got.Want, got.Got)
				}
			}
			if tc.name == "manifest second" && report.Manifest != nil {
				t.Error("nothing is checked when the manifest is not first")
			}
			if tc.name == "swapped" && len(report.Verified) != f.manifest.Counts.Present-1 {
				t.Errorf("everything else is still hashed: verified %d", len(report.Verified))
			}
		})
	}

	t.Run("schema version 2", func(t *testing.T) {
		entries := fixtureEntries(t, f)
		entries[0].body = bytes.Replace(entries[0].body, []byte(`"schema_version": 1`), []byte(`"schema_version": 2`), 1)
		_, err := Verify(bytes.NewReader(buildTar(t, entries)))
		if err == nil || !strings.Contains(err.Error(), "schema_version 2") {
			t.Fatalf("Verify = %v, want an error naming schema_version 2", err)
		}
	})
}

// E7
func TestVerify_NeverWritesToDisk(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	f := newFixture()
	entries := append(fixtureEntries(t, f),
		tarEntry{name: "../x", body: []byte("escape")},
		tarEntry{name: "tasks/reconcile/link", typeflag: tar.TypeSymlink})
	if _, err := Verify(bytes.NewReader(buildTar(t, entries))); err != nil {
		t.Fatal(err)
	}
	var found []string
	if err := fs.WalkDir(os.DirFS(dir), ".", func(path string, _ fs.DirEntry, err error) error {
		if path != "." {
			found = append(found, path)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Fatalf("Verify wrote %v", found)
	}
	if _, err := os.Stat("../x"); err == nil {
		t.Fatal("Verify followed ../x")
	}
}
