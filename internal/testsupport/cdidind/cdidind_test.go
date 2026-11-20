package cdidind

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// TestDindImage_IsTheManifestsPin: the dind the tests run is the one the
// release's privileged Orker manifest pins, so the CDI the tests prove is
// the CDI that dockerd resolves.
func TestDindImage_IsTheManifestsPin(t *testing.T) {
	raw, err := os.ReadFile("../../../deploy/kubernetes/orker-privileged/orker.yaml")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s+image: (docker:\S+-dind@sha256:[0-9a-f]{64})$`).FindStringSubmatch(string(raw))
	if m == nil {
		t.Fatal("the manifest pins no docker:*-dind image by digest")
	}
	if m[1] != DindImage {
		t.Fatalf("DindImage = %s, the manifest pins %s", DindImage, m[1])
	}
	for _, image := range []string{DindImage, GlibcImage, WorkloadImage} {
		if !regexp.MustCompile(`^\S+:\S+@sha256:[0-9a-f]{64}$`).MatchString(image) {
			t.Errorf("%s is not pinned by digest with its tag", image)
		}
	}
}

// TestGlibcFiles_TheLoaderWhereTheHookNamesIt: the loader lands at the
// interpreter path NVIDIA's hook names, and the four libraries where a Debian
// loader looks, for each arch the Orker ships.
func TestGlibcFiles_TheLoaderWhereTheHookNamesIt(t *testing.T) {
	for arch, interp := range map[string]string{"amd64": "/lib64/ld-linux-x86-64.so.2", "arm64": "/lib/ld-linux-aarch64.so.1"} {
		files, err := glibcFiles(arch)
		if err != nil {
			t.Fatalf("%s: %v", arch, err)
		}
		if len(files) != 5 || files[0].to != interp {
			t.Fatalf("%s: files %+v; want the loader at %s and four libraries", arch, files, interp)
		}
		for _, f := range files[1:] {
			if f.from != f.to || !strings.HasPrefix(f.to, "/usr/lib/") {
				t.Errorf("%s: %+v is not at a Debian loader's own path", arch, f)
			}
		}
	}
	if _, err := glibcFiles("riscv64"); err == nil {
		t.Fatal("an arch with no layout was given one")
	}
}

// TestRunArgs_NoMountNoSocketNoPort: the dind is privileged and pulls
// nothing, gets no mount, no published port, no network (its dockerd's
// unauthenticated TCP port is then unreachable) and never the host's Docker
// socket, and carries its proof's label, and its owner's when known.
func TestRunArgs_NoMountNoSocketNoPort(t *testing.T) {
	args := runArgs("pr-1", "host:42:7")
	joined := strings.Join(args, " ")
	for _, forbidden := range []string{"-v", "--volume", "--mount", "-p", "--publish", "--network=host", "--pid=host"} {
		if slices.Contains(args, forbidden) {
			t.Fatalf("run args %q carry %s", joined, forbidden)
		}
	}
	if strings.Contains(joined, "docker.sock") {
		t.Fatalf("run args %q reach the host's Docker socket", joined)
	}
	for _, want := range []string{"--privileged", "--pull never", "--network none", "orkstack.proof=pr-1", "orkstack.proof.owner=host:42:7"} {
		if !strings.Contains(joined, want) {
			t.Errorf("run args %q lack %s", joined, want)
		}
	}
	if args[len(args)-1] != DindImage {
		t.Fatalf("the image is %s, want %s", args[len(args)-1], DindImage)
	}
	if unknown := strings.Join(runArgs("pr-1", ""), " "); strings.Contains(unknown, "orkstack.proof.owner") || !strings.Contains(unknown, "orkstack.proof=pr-1") {
		t.Fatalf("with no owner known, run args %q; want the proof labelled and no owner", unknown)
	}
}
