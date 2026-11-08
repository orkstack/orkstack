package dist_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/dist"
)

const (
	testCommit  = "af4986a1c0ffee00c0ffee00c0ffee00c0ffee00"
	otherCommit = "0123456789abcdef0123456789abcdef01234567"
)

// TestParseCommit: a release is stamped only with a whole revision, SHA-1 or
// SHA-256, so what it reports always names one commit and nothing but hex
// reaches the linker's flags.
func TestParseCommit(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		in string
		ok bool
	}{
		{testCommit, true},
		{strings.Repeat("a", 64), true},
		{"", false},
		{"unknown", false},
		{"af4986a1", false},
		{strings.ToUpper(testCommit), false},
		{testCommit + "-dirty", false},
		{testCommit + " -X main.version=v9.9.9", false},
		{strings.Repeat("a", 39) + "g", false},
		{strings.Repeat("a", 41), false},
	} {
		got, err := dist.ParseCommit(c.in)
		if (err == nil) != c.ok || (c.ok && got != c.in) {
			t.Errorf("ParseCommit(%q) = %q, %v; want ok=%v", c.in, got, err, c.ok)
		}
	}
}

// TestRelease_BuildsReportTheirCommit is the failure a published v0.2.0 showed:
// `commit: unknown`. Each binary, compiled exactly as a release compiles it for
// this machine, is asked what it is. A build is slow, so -short skips it; the
// proof's unit tier does not run -short.
func TestRelease_BuildsReportTheirCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles the three release binaries; -short skips it, the proof runs it")
	}
	t.Parallel()
	for _, target := range dist.Targets() {
		if target.GOOS != runtime.GOOS || target.GOARCH != runtime.GOARCH {
			continue
		}
		t.Run(target.Binary, func(t *testing.T) {
			t.Parallel()
			binary := releaseBuild(t, target, testCommit)
			args := []string{"--version"}
			if target.Binary == dist.Orkctl {
				args = []string{"version"}
			}
			got := runBinary(t, binary, args...)
			for _, line := range []string{
				`(?m)^` + target.Binary + `:\s+v1\.2\.3$`,
				`(?m)^commit:\s+` + testCommit + `$`,
			} {
				if !regexp.MustCompile(line).MatchString(got) {
					t.Errorf("%s %v prints\n%s\nwhich has no line matching %s", target.Binary, args, got, line)
				}
			}
			if target.Binary != dist.Orkctl {
				return
			}
			var info struct {
				Version string `json:"version"`
				Commit  string `json:"commit"`
			}
			out := runBinary(t, binary, "version", "--json")
			if err := json.Unmarshal([]byte(out), &info); err != nil {
				t.Fatalf("orkctl version --json printed %q: %v", out, err)
			}
			if info.Version != "v1.2.3" || info.Commit != testCommit {
				t.Errorf("orkctl version --json = %+v, want v1.2.3 at %s", info, testCommit)
			}
		})
	}
}

// TestRelease_OneCommitBuildsOneBinary: the stamp is a function of the commit,
// so `the same commit always produces the same bytes` still holds, and two
// commits never claim one binary.
func TestRelease_OneCommitBuildsOneBinary(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles orkctl three times; -short skips it, the proof runs it")
	}
	t.Parallel()
	target := hostTarget(t, dist.Orkctl)
	read := func(commit string) []byte {
		b, err := os.ReadFile(releaseBuild(t, target, commit))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	first := read(testCommit)
	if !bytes.Equal(first, read(testCommit)) {
		t.Error("two release builds of one commit differ")
	}
	if bytes.Equal(first, read(otherCommit)) {
		t.Error("release builds of two commits are the same bytes: the stamp did not reach the binary")
	}
}

// TestUnstampedBuild_ReportsUnknownAndStillRuns: a build nobody stamped, with
// no version control information either (the quickstart's source build, a
// source archive), says so rather than failing or guessing.
func TestUnstampedBuild_ReportsUnknownAndStillRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles orkctl; -short skips it, the proof runs it")
	}
	t.Parallel()
	out := filepath.Join(t.TempDir(), "orkctl")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, goTool(t), "build", "-trimpath", "-buildvcs=false", "-ldflags", "-X main.commit=", "-o", out, "./cmd/orkctl")
	cmd.Dir = repoRoot(t)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, output)
	}
	if got := runBinary(t, out, "version"); !regexp.MustCompile(`(?m)^commit:\s+unknown$`).MatchString(got) {
		t.Errorf("orkctl version prints\n%s\nwhich does not say commit: unknown", got)
	}
}

// TestMakeDist_StampsTheCheckoutsCommit: `make dist` hands the tool the commit
// the tree is checked out at, an override wins for a tree with no git, and with
// neither it stops rather than build a release that cannot say what it is.
func TestMakeDist_StampsTheCheckoutsCommit(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	dry := func(args ...string) (string, error) {
		cmd := exec.Command("make", append([]string{"-n", "dist", "VERSION=v1.2.3", "SOURCE_DATE_EPOCH=1700000000"}, args...)...)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	got, err := dry()
	if err != nil {
		t.Fatalf("make -n dist: %v\n%s", err, got)
	}
	if !regexp.MustCompile(`-commit [0-9a-f]{40,64} `).MatchString(got) {
		t.Errorf("make -n dist prints\n%s\nwhich names no whole commit", got)
	}
	got, err = dry("SOURCE_COMMIT=" + otherCommit)
	if err != nil || !strings.Contains(got, "-commit "+otherCommit) {
		t.Errorf("make -n dist SOURCE_COMMIT=%s: %v\n%s", otherCommit, err, got)
	}
	got, err = dry("SOURCE_COMMIT=")
	if err == nil || !strings.Contains(got, "SOURCE_COMMIT") {
		t.Errorf("make -n dist with no commit: %v\n%s\nwant a refusal naming SOURCE_COMMIT", err, got)
	}
}

func hostTarget(t *testing.T, binary string) dist.Target {
	t.Helper()
	for _, target := range dist.Targets() {
		if target.Binary == binary && target.GOOS == runtime.GOOS && target.GOARCH == runtime.GOARCH {
			return target
		}
	}
	t.Fatalf("this machine, %s/%s, has no release target for %s", runtime.GOOS, runtime.GOARCH, binary)
	return dist.Target{}
}

// releaseBuild compiles target as a release does, with GoBuilder.
func releaseBuild(t *testing.T, target dist.Target, commit string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	out := filepath.Join(t.TempDir(), target.Executable())
	if err := dist.GoBuilder(ctx, repoRoot(t), target, "v1.2.3", commit, out); err != nil {
		t.Fatal(err)
	}
	return out
}

// runBinary runs a built binary with nothing of this process's environment, so
// no ORK_ setting can change what it answers.
func runBinary(t *testing.T, binary string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = []string{"HOME=" + t.TempDir()}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s %v: %v\nstdout: %s\nstderr: %s", filepath.Base(binary), args, err, stdout.String(), stderr.String())
	}
	return stdout.String()
}
