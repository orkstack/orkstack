package main

import (
	"bytes"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ---- the release smoke's paths ---------------------------------------------------
//
// release.yml runs `scripts/quickstart-smoke.sh --release vX.Y.Z --layouts
// dist/layouts`, and docker reads the source of a relative `-v` as a named
// volume: the first dry run failed there, exit 125. The script runs here with
// docker, go and the other tools it calls stubbed, up to the first push, which
// the docker stub answers with the wrong digest; the stubs record what the
// script mounted and what it handed the dist tool.

const smokeDockerStub = `#!/usr/bin/env bash
printf 'docker' >>"$STUB_LOG"
printf ' %q' "$@" >>"$STUB_LOG"
printf '\n' >>"$STUB_LOG"
case "$1" in
context) echo "unix://$STUB_SOCKET" ;;
run) case " $* " in *" inspect --raw "*) echo "not the layout's manifest" ;; esac ;;
esac
exit 0
`

const smokeGoStub = `#!/usr/bin/env bash
printf 'go' >>"$STUB_LOG"
printf ' %q' "$@" >>"$STUB_LOG"
printf '\n' >>"$STUB_LOG"
case "$1 $2" in
"env GOARCH") echo amd64 ;;
"run ./internal/dist/cmd/dist") printf 'ork\tsha256:%s\n' "$(printf 'a%.0s' {1..64})" ;;
esac
exit 0
`

type smokeSandbox struct {
	t    *testing.T
	root string // holds the copied repository, the stubs and the callers' directories
	repo string
	bin  string
	log  string
	sock string
}

func newSmokeSandbox(t *testing.T) *smokeSandbox {
	t.Helper()
	for _, tool := range []string{"bash", "sha256sum", "od", "mktemp"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatalf("%s is not installed; the script needs it", tool)
		}
	}
	root := t.TempDir()
	s := &smokeSandbox{t: t, root: root, repo: filepath.Join(root, "repo"), bin: filepath.Join(root, "bin"), log: filepath.Join(root, "calls.log")}
	for _, f := range []string{"scripts/quickstart-smoke.sh", "deploy/skopeo-image.txt", "deploy/cosign-image.txt", "compose.yaml"} {
		b, err := os.ReadFile(gateRepoPath(t, f))
		if err != nil {
			t.Fatal(err)
		}
		dst := filepath.Join(s.repo, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, b, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(s.bin, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"docker":  smokeDockerStub,
		"go":      smokeGoStub,
		"python3": "#!/usr/bin/env bash\necho $((20000 + RANDOM % 10000))\n",
		"curl":    "#!/usr/bin/env bash\nexit 0\n",
		"openssl": "#!/usr/bin/env bash\nexit 0\n",
	} {
		if err := os.WriteFile(filepath.Join(s.bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// The script requires the daemon's endpoint to be a unix socket; a unix
	// socket's path is short, so it lives outside t.TempDir().
	dir, err := os.MkdirTemp("", "smoke")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	s.sock = filepath.Join(dir, "d.sock")
	l, err := net.Listen("unix", s.sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return s
}

func (s *smokeSandbox) mkdir(rel string) string {
	s.t.Helper()
	p := filepath.Join(s.root, filepath.FromSlash(rel))
	if err := os.MkdirAll(p, 0o755); err != nil {
		s.t.Fatal(err)
	}
	return p
}

// run calls the script from cwd, as release.yml does from the repository
// root, with tmpdir as TMPDIR; it returns the output and the exit status.
func (s *smokeSandbox) run(cwd, tmpdir string, args ...string) (string, int) {
	s.t.Helper()
	cmd := exec.Command("bash", append([]string{filepath.Join(s.repo, "scripts", "quickstart-smoke.sh")}, args...)...)
	cmd.Dir = cwd
	var env []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		switch {
		case k == "PATH" || k == "TMPDIR" || k == "SOURCE_DATE_EPOCH" || strings.HasPrefix(k, "COSIGN_") ||
			strings.HasPrefix(k, "ORKSTACK_") || strings.HasPrefix(k, "SMOKE_") || strings.HasPrefix(k, "DOCKER_"):
			continue
		}
		env = append(env, kv)
	}
	cmd.Env = append(env, "PATH="+s.bin+":"+os.Getenv("PATH"), "TMPDIR="+tmpdir, "SOURCE_DATE_EPOCH=1",
		"STUB_LOG="+s.log, "STUB_SOCKET="+s.sock)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return out.String(), 0
	case errors.As(err, &exit):
		return out.String(), exit.ExitCode()
	}
	s.t.Fatalf("running the smoke: %v\n%s", err, out.String())
	return "", 0
}

// mounts is every `-v SOURCE:TARGET[:MODE]` the docker stub was handed, by target.
func (s *smokeSandbox) mounts() map[string][]string {
	s.t.Helper()
	got := map[string][]string{}
	for _, line := range gateLogLines(s.t, s.log) {
		f := strings.Fields(line)
		for i := 0; i+1 < len(f); i++ {
			if f[0] == "docker" && f[i] == "-v" {
				spec := strings.Split(f[i+1], ":")
				if len(spec) >= 2 {
					got[spec[1]] = append(got[spec[1]], spec[0])
				}
			}
		}
	}
	return got
}

// distLayouts is the -layouts argument the dist tool was run with.
func (s *smokeSandbox) distLayouts() string {
	for _, line := range gateLogLines(s.t, s.log) {
		f := strings.Fields(line)
		for i := 0; i+1 < len(f); i++ {
			if f[0] == "go" && f[i] == "-layouts" {
				return f[i+1]
			}
		}
	}
	return ""
}

func TestReleaseSmoke_MountsTheLayoutsByAnAbsolutePath(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cwd, layouts  string // relative to the sandbox's root; layouts relative to cwd
		tmpdir        string // relative to the repository, where the script runs; "" is an absolute one
		wantedLayouts string // relative to the sandbox's root
	}{
		// release.yml's own call, from the repository's root; a relative
		// TMPDIR makes the work directory relative too.
		{name: "release.yml's call", cwd: "repo", layouts: "dist/layouts", tmpdir: "tmp", wantedLayouts: "repo/dist/layouts"},
		// From another directory, a relative path is the caller's.
		{name: "a caller elsewhere", cwd: "elsewhere", layouts: "../built/layouts", wantedLayouts: "built/layouts"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSmokeSandbox(t)
			cwd := s.mkdir(tc.cwd)
			want := s.mkdir(tc.wantedLayouts)
			tmpdir := s.mkdir("abs-tmp")
			if tc.tmpdir != "" {
				s.mkdir("repo/" + tc.tmpdir)
				tmpdir = tc.tmpdir
			}
			out, code := s.run(cwd, tmpdir, "--release", "v0.1.0", "--layouts", tc.layouts)
			// The stubbed registry answers the push with another digest.
			if code != 1 || !strings.Contains(out, "FAIL: ork: pushed") {
				t.Fatalf("exit %d; want 1 at the first push:\n%s", code, out)
			}
			want, err := filepath.EvalSymlinks(want)
			if err != nil {
				t.Fatal(err)
			}
			if got := s.distLayouts(); got != want {
				t.Errorf("the dist tool read -layouts %q, want %q", got, want)
			}
			m := s.mounts()
			if got := m["/layouts"]; len(got) == 0 || got[0] != want {
				t.Errorf("docker mounted %q at /layouts, want %q", got, want)
			}
			if len(m["/w"]) == 0 {
				t.Errorf("the cleanup never mounted the work directory; mounts: %v", m)
			}
			for target, sources := range m {
				for _, src := range sources {
					if !filepath.IsAbs(src) {
						t.Errorf("docker run -v %s:%s: a relative source is a named volume", src, target)
					}
				}
			}
		})
	}
}

func TestReleaseSmoke_RefusesAnEmptyLayoutsArgument(t *testing.T) {
	s := newSmokeSandbox(t)
	if out, code := s.run(s.repo, s.mkdir("abs-tmp"), "--release", "v0.1.0", "--layouts", ""); code != 2 {
		t.Fatalf("exit %d; want 2 for an empty --layouts:\n%s", code, out)
	}
	if calls := gateLogLines(t, s.log); len(calls) != 0 {
		t.Errorf("an invalid invocation ran %v", calls)
	}
}
