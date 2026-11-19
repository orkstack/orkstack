package dist_test

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/buildinfo"
	"github.com/orkstack/orkstack/internal/dist"
)

// TestDist_StampedReleaseKeyIsDeployCosignPub: the key a release stamps into
// the orker is deploy/cosign.pub as committed, the public half release.yml
// signs the images with.
func TestDist_StampedReleaseKeyIsDeployCosignPub(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "deploy", "cosign.pub"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := dist.ReleaseKey(root)
	if err != nil {
		t.Fatalf("ReleaseKey: %v", err)
	}
	if want := base64.StdEncoding.EncodeToString(raw); got != want {
		t.Fatalf("ReleaseKey = %q, want deploy/cosign.pub base64-encoded", got)
	}
	for _, target := range dist.Targets() {
		if target.Binary != dist.Orker {
			continue
		}
		args := strings.Join(target.BuildArgs("v1.2.3", "abc", got, "out"), " ")
		if !strings.Contains(args, "-X main.releaseKey="+got) {
			t.Fatalf("%s: build arguments %q do not stamp the release key", target, args)
		}
	}
}

// TestDist_ReleaseKeyRefusesAKeyThatDoesNotRead: a missing or unreadable key
// stops a release build rather than stamping nothing.
func TestDist_ReleaseKeyRefusesAKeyThatDoesNotRead(t *testing.T) {
	root := t.TempDir()
	if _, err := dist.ReleaseKey(root); err == nil {
		t.Fatal("a tree with no deploy/cosign.pub gave a key")
	}
	if err := os.MkdirAll(filepath.Join(root, "deploy"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "deploy", "cosign.pub"), []byte("not a key"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := dist.ReleaseKey(root); err == nil {
		t.Fatal("an unreadable key was stamped")
	}
}

// TestDist_VersionPatternIsBuildinfos: what dist parses as a release version
// and what a binary takes for one are the same strings.
func TestDist_VersionPatternIsBuildinfos(t *testing.T) {
	for _, v := range []string{"v0.4.0", "v10.2.3", "v0.4", "0.4.0", "v0.4.0-rc1", "v01.2.3", " v1.2.3", "dev", ""} {
		_, err := dist.ParseVersion(v)
		if parsed, release := err == nil, buildinfo.ReleaseVersionPattern.MatchString(v); parsed != release {
			t.Errorf("%q: dist parses it %v, buildinfo takes it for a release %v", v, parsed, release)
		}
	}
}

// TestReleaseImage_MatchesWhatAReleasePublishes: the image a release build
// defaults its probe to is the orker image a release publishes, under the
// namespace and the plain-version tag release.yml pushes.
func TestReleaseImage_MatchesWhatAReleasePublishes(t *testing.T) {
	version, err := dist.ParseVersion("v1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	got, ok := buildinfo.ReleaseImage(dist.Orker, version.String(), "abc")
	if want := "ghcr.io/" + dist.ImageNamespace + "/" + dist.Orker + ":" + version.Plain(); !ok || got != want {
		t.Fatalf("ReleaseImage = %q, %v; want %q", got, ok, want)
	}
}
