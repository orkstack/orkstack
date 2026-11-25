package vaulttest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func pin(t *testing.T, text string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "deploy"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "deploy", "vault-image.txt"), []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

// TestImage_IsPinnedByDigest: the repository's own file names one image by
// version and digest, and no other shape is taken.
func TestImage_IsPinnedByDigest(t *testing.T) {
	if image, err := Image("../../.."); err != nil || !strings.HasPrefix(image, "hashicorp/vault:") {
		t.Fatalf("deploy/vault-image.txt = %q, %v", image, err)
	}
	for _, bad := range []string{"", "# a comment\n", "hashicorp/vault:latest\n", "hashicorp/vault:2.2.2\n", "example.com/vault:2.2.2@sha256:" + strings.Repeat("a", 64) + "\n"} {
		if image, err := Image(pin(t, bad)); err == nil {
			t.Errorf("%q was taken as %q", bad, image)
		}
	}
}

// TestStart_AMissingImageIsNamed: a host with no docker, and one whose docker
// neither holds the image nor can pull it, is told so as a missing
// prerequisite, and no server is started.
func TestStart_AMissingImageIsNamed(t *testing.T) {
	root := pin(t, "hashicorp/vault:2.2.2@sha256:"+strings.Repeat("a", 64)+"\n")
	for name, script := range map[string]string{"no docker": "", "no image": "#!/bin/sh\necho \"$1\" >>\"$0.calls\"\nexit 1\n"} {
		bin := t.TempDir()
		docker := filepath.Join(bin, "docker")
		if script != "" {
			if err := os.WriteFile(docker, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		t.Setenv("PATH", bin)
		if _, err := Start(context.Background(), Options{Root: root, Dir: t.TempDir()}); !errors.Is(err, ErrNoImage) {
			t.Errorf("%s: Start = %v, want ErrNoImage", name, err)
		}
		if calls, _ := os.ReadFile(docker + ".calls"); strings.Contains(string(calls), "run") {
			t.Errorf("%s: docker was asked to run one", name)
		}
	}
}
