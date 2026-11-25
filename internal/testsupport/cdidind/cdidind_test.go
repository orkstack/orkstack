package cdidind

import (
	"os"
	"os/exec"
	"path/filepath"
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

// TestExecRunArgs: an image a test made in the dind is run from the dind's own
// store, never pulled, each device named once.
func TestExecRunArgs(t *testing.T) {
	got := strings.Join(execRunArgs("c1", "img:1", []string{"a/b=c", "a/b=d"}, "true"), " ")
	want := "exec c1 docker run --rm --pull never --network none --device a/b=c --device a/b=d img:1 sh -c true"
	if got != want {
		t.Fatalf("args = %q, want %q", got, want)
	}
}

// TestImagesScript_ListsEveryPinnedImage: scripts/cdi-dind-images.sh, which a
// proof's setup pulls by, names exactly the images Start looks for, and fails
// on a source whose pins it cannot read rather than pull fewer.
func TestImagesScript_ListsEveryPinnedImage(t *testing.T) {
	script := "../../../scripts/cdi-dind-images.sh"
	out, err := exec.Command("bash", script).Output()
	if err != nil {
		t.Fatalf("%s: %v", script, err)
	}
	if want := strings.Join(Images(), "\n") + "\n"; string(out) != want {
		t.Fatalf("%s lists\n%s\nwant\n%s", script, out, want)
	}
	source := filepath.Join(t.TempDir(), "cdidind.go")
	for name, body := range map[string]string{
		"a pin it cannot read": "package x\n\nconst DindImage = \"docker:29@sha256:" + strings.Repeat("a", 64) + "\"\nconst GlibcImage = DindImage\n",
		"a tag":                "package x\n\nconst DindImage = \"docker:29\"\n",
		"none":                 "package x\n",
	} {
		if err := os.WriteFile(source, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("bash", script)
		cmd.Env = append(os.Environ(), "ORK_CDI_DIND_SOURCE="+source)
		if out, err := cmd.Output(); err == nil || len(out) != 0 {
			t.Errorf("%s: listed %q, %v; want nothing and a failure", name, out, err)
		}
	}
}

// TestImagesScript_PullsOnlyWhatIsMissing: with --pull, the images the host
// has are not pulled; one it lacks is, by its digest. A registry or engine that
// could not be reached is exit 77, a missing prerequisite; an answer that the
// digest is not there, or any other failure, is exit 1, which fails a proof. The
// docker is a fake, because neither an unreachable registry nor one that lacks
// a digest can be produced on demand; its words are Docker 29.9's.
func TestImagesScript_PullsOnlyWhatIsMissing(t *testing.T) {
	for name, tc := range map[string]struct {
		has    string // what `image inspect` finds
		says   string // what a pull fails with; "" succeeds
		want   int
		pulled []string
	}{
		"all present":         {has: "all", want: 0},
		"none, and they pull": {want: 0, pulled: []string{DindImage, GlibcImage, WorkloadImage}},
		"one missing":         {has: DindImage + " " + WorkloadImage, want: 0, pulled: []string{GlibcImage}},
		"no such host":        {says: `failed to resolve reference "x": failed to do request: Head "https://x/": dial tcp: lookup x: no such host`, want: 77, pulled: []string{DindImage}},
		"refused":             {says: `failed to do request: Head "https://x/": dial tcp [::1]:1: connect: connection refused`, want: 77, pulled: []string{DindImage}},
		"rate limited":        {says: "toomanyrequests: You have reached your unauthenticated pull rate limit", want: 77, pulled: []string{DindImage}},
		"a 503":               {says: "unexpected status from HEAD request to https://x/: 503 Service Unavailable", want: 77, pulled: []string{DindImage}},
		"no daemon":           {says: "Cannot connect to the Docker daemon at unix:///var/run/docker.sock", want: 77, pulled: []string{DindImage}},
		"an EOF":              {says: `Get "https://x/v2/": EOF`, want: 77, pulled: []string{DindImage}},
		"a client timeout":    {says: "net/http: request canceled (Client.Timeout exceeded while awaiting headers)", want: 77, pulled: []string{DindImage}},
		"no route":            {says: "dial: connect: no route to host", want: 77, pulled: []string{DindImage}},
		"the newer no daemon": {says: "failed to connect to the docker API at unix:///var/run/docker.sock", want: 77, pulled: []string{DindImage}},
		"another registry":    {says: `failed to do request: Head "https://x/": tls: failed to verify certificate: x509: certificate signed by unknown authority`, want: 1, pulled: []string{DindImage}},
		"a digest not there":  {says: `failed to resolve reference "x": docker.io/library/x: not found`, want: 1, pulled: []string{DindImage}},
		"a digest that lies":  {says: "failed commit on ref: unexpected commit digest sha256:bb, expected sha256:aa: failed precondition", want: 1, pulled: []string{DindImage}},
		"denied":              {says: "pull access denied for x", want: 1, pulled: []string{DindImage}},
		"unknown words":       {says: "something else", want: 1, pulled: []string{DindImage}},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			fake := `#!/bin/sh
for ref in "$@"; do :; done
case "$1" in
image) case " $HAS " in *" all "*|*" $ref "*) exit 0 ;; esac; exit 1 ;;
pull) echo "$ref" >>"$CALLS"; if [ -n "$SAYS" ]; then echo "Error response from daemon: $SAYS" >&2; exit 1; fi ;;
esac
`
			if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(fake), 0o755); err != nil {
				t.Fatal(err)
			}
			calls := filepath.Join(dir, "calls")
			cmd := exec.Command("bash", "../../../scripts/cdi-dind-images.sh", "--pull")
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "HAS="+tc.has, "SAYS="+tc.says, "CALLS="+calls)
			var stderr strings.Builder
			cmd.Stderr = &stderr
			_ = cmd.Run()
			if got := cmd.ProcessState.ExitCode(); got != tc.want {
				t.Fatalf("exit %d, want %d; said %q", got, tc.want, stderr.String())
			}
			raw, _ := os.ReadFile(calls)
			if got := strings.Fields(string(raw)); !slices.Equal(got, tc.pulled) {
				t.Fatalf("pulled %q, want %q", got, tc.pulled)
			}
			if tc.want != 0 && (!strings.Contains(stderr.String(), DindImage) || !strings.Contains(stderr.String(), tc.says)) {
				t.Fatalf("said %q; want the image and docker's words", stderr.String())
			}
		})
	}
}

// TestImagesScript_NoDockerIsAMissingPrerequisite: with no docker CLI, --pull
// is exit 77 and says so.
func TestImagesScript_NoDockerIsAMissingPrerequisite(t *testing.T) {
	dir := t.TempDir()
	for _, tool := range []string{"bash", "dirname", "grep", "sed", "cat"} {
		path, err := exec.LookPath(tool)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(path, filepath.Join(dir, tool)); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(filepath.Join(dir, "bash"), "../../../scripts/cdi-dind-images.sh", "--pull")
	cmd.Env = []string{"PATH=" + dir}
	out, _ := cmd.CombinedOutput()
	if code := cmd.ProcessState.ExitCode(); code != 77 || !strings.Contains(string(out), "docker") {
		t.Fatalf("exit %d: %s; want 77, naming docker", code, out)
	}
}

// TestLoadedRef: an image saved by its digest has no name in the dind that
// loads it, so what it is called there is what `docker load` reports.
func TestLoadedRef(t *testing.T) {
	id := "sha256:" + strings.Repeat("a", 64)
	for out, want := range map[string]string{
		"Loaded image ID: " + id + "\n": id,
		"Loaded image: alpine:3.24\n":   "alpine:3.24",
		"":                              "",
		"Loaded image ID: " + id + "\nLoaded image: alpine:3.24\n": "",
		"Loaded image ID: " + id + "; rm -rf /\n":                  "",
		"error\n": "",
	} {
		got, err := loadedRef(out)
		if got != want || (err == nil) != (want != "") {
			t.Errorf("loadedRef(%q) = %q, %v; want %q", out, got, err, want)
		}
	}
}
