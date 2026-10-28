package dist_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dist"
	"github.com/orkstack/orkstack/internal/dsl"
)

// fakeDigest is a well-formed digest that differs for every name.
func fakeDigest(name string) string {
	sum := sha256.Sum256([]byte(name))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// indexOf is an OCI layout's index.json naming the given manifest digests.
func indexOf(digests ...string) string {
	var entries []string
	for _, digest := range digests {
		entries = append(entries, fmt.Sprintf(
			`{"mediaType":"application/vnd.oci.image.index.v1+json","digest":%q,"size":1234}`, digest))
	}
	return `{"schemaVersion":2,"manifests":[` + strings.Join(entries, ",") + `]}`
}

func writeLayout(t *testing.T, dir, name, index string) {
	t.Helper()
	layout := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Join(layout, "blobs", "sha256"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(layout, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(layout, "index.json"), []byte(index), 0o644); err != nil {
		t.Fatal(err)
	}
}

// layoutsOf writes a layout for each name, each with its fakeDigest.
func layoutsOf(t *testing.T, names []string) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range names {
		writeLayout(t, dir, name, indexOf(fakeDigest(name)))
	}
	return dir
}

func TestImages_AreTheTenTheReleasePublishes(t *testing.T) {
	t.Parallel()
	want := []string{
		"activity-file", "activity-git", "activity-http", "activity-json", "activity-shell", "activity-template",
		"ork", "orkctl", "orker", "ui",
	}
	if got := dist.Images(); !slices.Equal(got, want) {
		t.Errorf("Images() = %q, want %q (sorted)", got, want)
	}
	if got := sorted(dist.ServiceImages()); !slices.Equal(got, []string{"ork", "orkctl", "orker", "ui"}) {
		t.Errorf("ServiceImages() = %q", got)
	}
	if got := sorted(dist.ActivityNames()); !slices.Equal(got, []string{"file", "git", "http", "json", "shell", "template"}) {
		t.Errorf("ActivityNames() = %q", got)
	}
}

func TestImages_ReturnedSlicesBelongToTheCaller(t *testing.T) {
	t.Parallel()
	for name, list := range map[string]func() []string{
		"ServiceImages": dist.ServiceImages, "ActivityNames": dist.ActivityNames, "Images": dist.Images,
	} {
		first := list()
		if len(first) == 0 {
			t.Fatalf("%s() is empty", name)
		}
		want := slices.Clone(first)
		first[0] = "tampered"
		if second := list(); !slices.Equal(second, want) {
			t.Errorf("%s() changed after its caller edited the slice it got: %q", name, second)
		}
	}
}

// TestImages_ActivitiesAreTheBuiltinCatalog: the activity images a release
// publishes are exactly the short names the engine's built-in catalog offers,
// so a new built-in cannot ship without an image, nor an image without a name.
func TestImages_ActivitiesAreTheBuiltinCatalog(t *testing.T) {
	t.Parallel()
	if got, want := sorted(dist.ActivityNames()), dsl.BuiltinActivityCatalog().Names(); !slices.Equal(got, want) {
		t.Errorf("the release publishes activities %q, the built-in catalog names %q", got, want)
	}
	for name, reference := range dsl.BuiltinActivityCatalog() {
		ref, err := dsl.ParseReference(reference)
		if err != nil {
			t.Fatal(err)
		}
		if want := dist.ImageNamespace + "/" + dist.ActivityPrefix + name; ref.Registry != "ghcr.io" || ref.Repository != want {
			t.Errorf("the catalog resolves %s to %s/%s, but the release publishes ghcr.io/%s", name, ref.Registry, ref.Repository, want)
		}
	}
}

// TestImages_AgreeWithTheMakefiles: the two Makefiles that build the images
// name the same lists, so a release cannot build ten images and publish nine.
func TestImages_AgreeWithTheMakefiles(t *testing.T) {
	t.Parallel()
	list := func(file, variable string) []string {
		m := regexp.MustCompile(`(?m)^` + variable + `\s*:=\s*(.+?)\s*$`).FindSubmatch(readRepoFile(t, file))
		if m == nil {
			t.Fatalf("%s does not define %s", file, variable)
		}
		return strings.Fields(string(m[1]))
	}
	if got := sorted(list("Makefile", "RELEASE_IMAGES")); !slices.Equal(got, sorted(dist.ServiceImages())) {
		t.Errorf("Makefile RELEASE_IMAGES = %q, want %q", got, sorted(dist.ServiceImages()))
	}
	if got := sorted(list("images/Makefile", "ACTIVITIES")); !slices.Equal(got, sorted(dist.ActivityNames())) {
		t.Errorf("images/Makefile ACTIVITIES = %q, want %q", got, sorted(dist.ActivityNames()))
	}
}

func TestDigests_ReadEachLayoutsSingleIndexEntry(t *testing.T) {
	t.Parallel()
	dir := layoutsOf(t, dist.Images())
	got, err := dist.ReadLayouts(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("read %d images, want 10: %v", len(got), got)
	}
	for _, image := range got {
		if want := fakeDigest(image.Name); image.Digest != want {
			t.Errorf("%s: digest %s, want %s", image.Name, image.Digest, want)
		}
	}
}

func TestDigests_AreSortedByName(t *testing.T) {
	t.Parallel()
	// Written in the reverse of the order they are read back in.
	names := dist.Images()
	slices.Reverse(names)
	got, err := dist.ReadLayouts(layoutsOf(t, names))
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, image := range got {
		order = append(order, image.Name)
	}
	if want := dist.Images(); !slices.Equal(order, want) {
		t.Errorf("order %q, want %q", order, want)
	}
	text := dist.FormatImages(got)
	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	if len(lines) != 10 || !strings.HasSuffix(text, "\n") {
		t.Fatalf("FormatImages wrote %d lines, final newline %v:\n%s", len(lines), strings.HasSuffix(text, "\n"), text)
	}
	if lines[0] != "activity-file\t"+fakeDigest("activity-file") || lines[9] != "ui\t"+fakeDigest("ui") {
		t.Errorf("lines are not name<TAB>digest: first %q, last %q", lines[0], lines[9])
	}
}

func TestDigests_RefuseAnIndexWithoutExactlyOneManifest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, index string }{
		{"no manifests", indexOf()},
		{"an empty document", `{}`},
		{"manifests that is not a list", `{"manifests":{}}`},
		{"two manifests", indexOf(fakeDigest("a"), fakeDigest("b"))},
		{"the same manifest twice", indexOf(fakeDigest("a"), fakeDigest("a"))},
		{"not JSON", `not json`},
		{"empty file", ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := layoutsOf(t, dist.Images())
			writeLayout(t, dir, "ork", tc.index)
			_, err := dist.ReadLayouts(dir)
			if err == nil {
				t.Fatal("want a refusal")
			}
			if !strings.Contains(err.Error(), "ork") || !strings.Contains(err.Error(), "index.json") {
				t.Errorf("error %q does not name the layout and its index.json", err)
			}
		})
	}
}

func TestDigests_RefuseAMalformedDigest(t *testing.T) {
	t.Parallel()
	good := fakeDigest("ork")
	for _, tc := range []struct{ name, digest string }{
		{"uppercase hex", "sha256:" + strings.ToUpper(strings.TrimPrefix(good, "sha256:"))},
		{"another algorithm", "sha512:" + strings.Repeat("a", 128)},
		{"too short", good[:len(good)-1]},
		{"too long", good + "0"},
		{"no algorithm", strings.TrimPrefix(good, "sha256:")},
		{"not hex", "sha256:" + strings.Repeat("g", 64)},
		{"empty", ""},
		{"a trailing space", good + " "},
		{"a trailing newline", good + "\n"},
		{"a tag, not a digest", "latest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := layoutsOf(t, dist.Images())
			writeLayout(t, dir, "ork", indexOf(tc.digest))
			_, err := dist.ReadLayouts(dir)
			if err == nil {
				t.Fatalf("accepted the digest %q", tc.digest)
			}
			if !strings.Contains(err.Error(), "ork") || !strings.Contains(err.Error(), "sha256:") {
				t.Errorf("error %q does not name the layout and the form a digest takes", err)
			}
		})
	}
}

func TestDigests_RefuseUnexpectedAndMissingNames(t *testing.T) {
	t.Parallel()
	t.Run("an image the release does not publish", func(t *testing.T) {
		t.Parallel()
		for _, extra := range []string{"activity-curl", "docker-cli", "ORK", "activity-", "ork-2"} {
			dir := layoutsOf(t, dist.Images())
			writeLayout(t, dir, extra, indexOf(fakeDigest(extra)))
			_, err := dist.ReadLayouts(dir)
			if err == nil || !strings.Contains(err.Error(), extra) {
				t.Errorf("an extra layout %q: error %v; want a refusal naming it", extra, err)
			}
		}
	})
	t.Run("an image that is missing", func(t *testing.T) {
		t.Parallel()
		for _, missing := range dist.Images() {
			var have []string
			for _, name := range dist.Images() {
				if name != missing {
					have = append(have, name)
				}
			}
			_, err := dist.ReadLayouts(layoutsOf(t, have))
			if err == nil || !strings.Contains(err.Error(), missing) {
				t.Errorf("without %q: error %v; want a refusal naming it", missing, err)
			}
		}
	})
	t.Run("an empty directory", func(t *testing.T) {
		t.Parallel()
		_, err := dist.ReadLayouts(t.TempDir())
		if err == nil || !strings.Contains(err.Error(), "ork") || !strings.Contains(err.Error(), "activity-shell") {
			t.Errorf("error %v; want a refusal listing the images it needs", err)
		}
	})
}

func TestDigests_RefuseABrokenDirectory(t *testing.T) {
	t.Parallel()
	t.Run("a layout without an index", func(t *testing.T) {
		t.Parallel()
		dir := layoutsOf(t, dist.Images())
		if err := os.Remove(filepath.Join(dir, "ui", "index.json")); err != nil {
			t.Fatal(err)
		}
		if _, err := dist.ReadLayouts(dir); err == nil || !strings.Contains(err.Error(), "ui") {
			t.Errorf("error %v; want a refusal naming ui", err)
		}
	})
	t.Run("a stray file beside the layouts", func(t *testing.T) {
		t.Parallel()
		dir := layoutsOf(t, dist.Images())
		if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := dist.ReadLayouts(dir); err == nil || !strings.Contains(err.Error(), "notes.txt") {
			t.Errorf("error %v; want a refusal naming notes.txt", err)
		}
	})
	t.Run("a layout that is a symbolic link", func(t *testing.T) {
		t.Parallel()
		dir := layoutsOf(t, dist.Images())
		elsewhere := layoutsOf(t, []string{"ork"})
		if err := os.RemoveAll(filepath.Join(dir, "ork")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(elsewhere, "ork"), filepath.Join(dir, "ork")); err != nil {
			t.Skipf("no symbolic links here: %v", err)
		}
		if _, err := dist.ReadLayouts(dir); err == nil || !strings.Contains(err.Error(), "ork") {
			t.Errorf("error %v; want a refusal naming ork", err)
		}
	})
	t.Run("no such directory", func(t *testing.T) {
		t.Parallel()
		if _, err := dist.ReadLayouts(filepath.Join(t.TempDir(), "absent")); err == nil {
			t.Error("want an error")
		}
	})
	t.Run("a file, not a directory", func(t *testing.T) {
		t.Parallel()
		file := filepath.Join(t.TempDir(), "layouts")
		if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := dist.ReadLayouts(file); err == nil {
			t.Error("want an error")
		}
	})
}
