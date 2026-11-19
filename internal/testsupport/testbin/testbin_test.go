package testbin

import (
	"debug/elf"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestPathPrefersTheProofsBinaries(t *testing.T) {
	shared := t.TempDir()
	want := filepath.Join(shared, "orkctl")
	must.OK(t, os.WriteFile(want, []byte("#!/bin/sh\n"), 0o755))
	t.Setenv(EnvDir, shared)
	if got := Path(t, "./cmd/orkctl"); got != want {
		t.Errorf("Path = %s, want the proof's %s", got, want)
	}
}

func TestPathRefusesAnythingButCommandsAndTools(t *testing.T) {
	for _, pkg := range []string{"./internal/engine", "cmd/ork", "../cmd/ork"} {
		if _, err := resolve(pkg); err == nil {
			t.Errorf("resolve(%q) accepted it", pkg)
		}
	}
}

func TestPathBuildsOncePerProcessWithoutTheProofsBinaries(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a binary")
	}
	t.Setenv(EnvDir, "")
	first, err := resolve("./test/tools/deaf-poller")
	must.OK(t, err)
	second, err := resolve("./test/tools/deaf-poller")
	if err != nil || second != first {
		t.Fatalf("second build: %s, %v; want the first, %s", second, err, first)
	}
	if root, _ := repoRoot(); filepath.Dir(first) == filepath.Join(root, "bin") {
		t.Fatalf("built into the repository's bin/: %s", first)
	}
	Cleanup()
	if _, err := os.Stat(first); !os.IsNotExist(err) {
		t.Errorf("Cleanup left %s", first)
	}
}

// TestStaticPath_BuildsWithNoInterpreter: StaticPath's binary asks for no
// dynamic loader, so it runs in an image with no C library, and it is this
// process's own build even where a proof exported its binaries.
func TestStaticPath_BuildsWithNoInterpreter(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a binary")
	}
	shared := t.TempDir()
	must.OK(t, os.WriteFile(filepath.Join(shared, "deaf-poller"), []byte("#!/bin/sh\n"), 0o755))
	t.Setenv(EnvDir, shared)
	t.Cleanup(Cleanup)
	got := StaticPath(t, "./test/tools/deaf-poller")
	if filepath.Dir(got) == shared {
		t.Fatalf("StaticPath took the proof's binary %s", got)
	}
	f, err := elf.Open(got)
	must.OK(t, err)
	defer f.Close()
	for _, p := range f.Progs {
		if p.Type == elf.PT_INTERP {
			t.Fatalf("%s asks for a dynamic loader", got)
		}
	}
}

// TestOrkerEnv_GivesAnOrkerNoStopGraceUnlessTheTestNamesOne: an Orker
// defaults to a day's stop grace, so a test that stops one while it runs a
// task would wait it out; OrkerEnv adds a zero grace, and a grace the test
// (or the proof) already named wins.
func TestOrkerEnv_GivesAnOrkerNoStopGraceUnlessTheTestNamesOne(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
		want []string
	}{
		{"none named", []string{"A=1"}, []string{"A=1", "ORK_ORKER_STOP_GRACE=0s"}},
		{"empty", nil, []string{"ORK_ORKER_STOP_GRACE=0s"}},
		{"the test names one", []string{"ORK_ORKER_STOP_GRACE=10m", "A=1"}, []string{"ORK_ORKER_STOP_GRACE=10m", "A=1"}},
		{"an empty one is still named", []string{"ORK_ORKER_STOP_GRACE="}, []string{"ORK_ORKER_STOP_GRACE="}},
	} {
		in := append([]string(nil), tc.env...)
		got := OrkerEnv(in)
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("%s: OrkerEnv(%q) = %q, want %q", tc.name, tc.env, got, tc.want)
		}
		if strings.Join(in, "|") != strings.Join(tc.env, "|") {
			t.Errorf("%s: OrkerEnv changed its argument to %q", tc.name, in)
		}
	}
}

// TestGoBuildEnv_DropsWhatChangesTheBinary: a shell's target, flags and
// experiments never reach the build of a binary under test; everything else
// does, a near-miss name and an entry with no value included.
func TestGoBuildEnv_DropsWhatChangesTheBinary(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		want []string
	}{
		{"empty", nil, []string{}},
		{"nothing to drop", []string{"PATH=/bin", "HOME=/h"}, []string{"PATH=/bin", "HOME=/h"}},
		{"a polluted shell", []string{"PATH=/bin", "GOFLAGS=-tags=x", "GOOS=plan9", "GOARCH=386",
			"GOEXPERIMENT=nocoverageredesign", "CGO_ENABLED=0", "GOAMD64=v4", "GOARM64=v9.0", "HOME=/h", "GOCACHE=/c"},
			[]string{"PATH=/bin", "HOME=/h", "GOCACHE=/c"}},
		{"near-miss names stay", []string{"GOFLAGSX=1", "XGOOS=1", "GOOS"}, []string{"GOFLAGSX=1", "XGOOS=1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := slices.Clone(tt.env)
			if got := GoBuildEnv(in); !slices.Equal(got, tt.want) {
				t.Errorf("GoBuildEnv(%q) = %q, want %q", tt.env, got, tt.want)
			}
			if !slices.Equal(in, tt.env) {
				t.Errorf("GoBuildEnv changed its argument to %q", in)
			}
		})
	}
}

// TestStackEnv_DropsTheShellsOwnDeployment: a configuration file, an admin
// listener, pprof and a TLS or API-key Temporal connection that a developer's
// shell holds never reach a process under test; what the proof and CI hand a
// stack passes through, and a value the test appends after it wins.
func TestStackEnv_DropsTheShellsOwnDeployment(t *testing.T) {
	handed := []string{"ORK_TEMPORAL_ADDRESS=127.0.0.1:7233", "ORK_TEMPORAL_NAMESPACE=ork-p1", "ORK_S3_ENDPOINT=http://s3",
		"ORK_DOCKER_CLI=podman", "ORK_DOCKER_BIN=/usr/bin/podman", "ORK_ORKER_WORKDIR=/w", "PATH=/bin"}
	shells := []string{"ORK_CONFIG_FILE=/etc/ork.yaml", "ORK_ADMIN_ADDR=127.0.0.1:9464", "ORK_PPROF_ENABLED=true",
		"ORK_TEMPORAL_TLS_ENABLED=true", "ORK_TEMPORAL_TLS_CA_FILE=/ca", "ORK_TEMPORAL_TLS_CERT_FILE=/c",
		"ORK_TEMPORAL_TLS_KEY_FILE=/k", "ORK_TEMPORAL_TLS_SERVER_NAME=t", "ORK_TEMPORAL_API_KEY_FILE=/key"}
	tests := []struct {
		name string
		env  []string
		want []string
	}{
		{"empty", nil, []string{}},
		{"what the proof and CI hand a stack", handed, handed},
		{"the shell's own deployment", append(slices.Clone(shells), handed...), handed},
		{"near-miss names stay", []string{"ORK_CONFIG_FILEX=1", "ORK_TEMPORAL_TLS=1"}, []string{"ORK_CONFIG_FILEX=1", "ORK_TEMPORAL_TLS=1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StackEnv(tt.env); !slices.Equal(got, tt.want) {
				t.Errorf("StackEnv =\n%q\nwant\n%q", got, tt.want)
			}
		})
	}
	// A test's own setting, appended after StackEnv, is the one the child
	// reads: exec keeps the last of duplicate names.
	env := append(StackEnv([]string{"ORK_TEMPORAL_TLS_CA_FILE=/shell"}), "ORK_TEMPORAL_TLS_CA_FILE=/test")
	if !slices.Equal(env, []string{"ORK_TEMPORAL_TLS_CA_FILE=/test"}) {
		t.Errorf("StackEnv then the test's own = %q, want only the test's", env)
	}
}

// TestCompile_IgnoresTheShellsGoTarget: with GOOS, GOARCH and GOFLAGS set for
// another platform in the environment, as a cross-compiling shell has them,
// the binary a test runs is still this host's.
func TestCompile_IgnoresTheShellsGoTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a binary")
	}
	t.Cleanup(Cleanup)
	t.Setenv("GOOS", "plan9")
	t.Setenv("GOARCH", "386")
	t.Setenv("GOFLAGS", "-tags=testbin_polluted")
	got, err := compile("./test/tools/deaf-poller", "deaf-poller-polluted", false)
	must.OK(t, err)
	f, err := elf.Open(got)
	if err != nil {
		t.Fatalf("%s is not this host's executable: %v", got, err)
	}
	defer f.Close()
	if want := hostMachine(); f.Machine != want {
		t.Fatalf("%s is built for %v, want this host's %v", got, f.Machine, want)
	}
}

func hostMachine() elf.Machine {
	switch runtime.GOARCH {
	case "arm64":
		return elf.EM_AARCH64
	default:
		return elf.EM_X86_64
	}
}
