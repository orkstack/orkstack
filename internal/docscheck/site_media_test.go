package docscheck

import (
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestSiteMedia_RefusesADevelopmentBuild: the UI's sidebar shows the engine's
// version on every frame the site records, and make up builds with git describe
// unless told otherwise, so the site's demo once showed v0.1.0-276-g9970b2c. A
// capture takes a release-shaped version or none, and says how to get one.
func TestSiteMedia_RefusesADevelopmentBuild(t *testing.T) {
	root := repoRoot(t)
	script := filepath.Join(root, "site", "media", "check-engine-version.sh")
	tests := []struct {
		version string
		pass    bool
	}{
		{"v0.2.0", true},
		{"v12.34.56", true},
		{"", false},
		{"dev", false},
		{"e1d32bd8", false},
		{"v0.1.0-276-g9970b2c", false},
		{"v0.2.0-dirty", false},
		{"v0.2.0-rc.1", false},
		{"0.2.0", false},
	}
	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			t.Parallel()
			out, err := exec.Command("bash", script, tt.version).CombinedOutput()
			if tt.pass {
				if err != nil {
					t.Fatalf("%q refused: %v\n%s", tt.version, err, out)
				}
				return
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 77 {
				t.Fatalf("%q: err = %v, want exit 77, the missing-prerequisite status\n%s", tt.version, err, out)
			}
			if !strings.Contains(string(out), "VERSION=vX.Y.Z") {
				t.Errorf("%q: the refusal does not say how to get a release's version:\n%s", tt.version, out)
			}
		})
	}
	// The check only helps a capture that runs it: for every run with a UI
	// capture, and before anything is recorded.
	run := readFile(t, filepath.Join(root, "site", "media", "run.sh"))
	call := strings.Index(run, `check-engine-version.sh"`)
	work := strings.Index(run, `mkdir -p "$WORK"`)
	if call < 0 {
		t.Fatal("site/media/run.sh does not run check-engine-version.sh")
	}
	if work < 0 || call > work {
		t.Errorf("site/media/run.sh runs the check at %d, after it starts work at %d", call, work)
	}
	if line := run[strings.LastIndex(run[:call], "\n")+1 : call]; !strings.Contains(line, `[ "$ONLY" = terminal ] ||`) {
		t.Errorf("the check is not skipped only for a terminal-only run: %q", line)
	}
}
