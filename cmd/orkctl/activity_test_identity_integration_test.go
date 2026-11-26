//go:build integration

package main

import (
	"context"
	"encoding/json/v2"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// TestIntegration_ActivityTestWithIdentity: `orkctl activity test
// --with-identity`, run as built against this tree's shell image on a real
// container runtime, adds one check, which passes: the workload reads its
// token and expiration through current and by name, cannot write there, and
// sees the Orker's refresh put another token in place.
func TestIntegration_ActivityTestWithIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	requireLabelledImages(t)
	if out, err := exec.CommandContext(ctx, localContainerCLI(), "info").CombinedOutput(); err != nil {
		infraAbsent(t, "no container runtime (%v): %s", err, out)
	}
	reference := localcatalog.Reference("shell")
	run := func(args ...string) (map[string]string, int) {
		t.Helper()
		stdout, stderr, code := runBinaryWithEnv(ctx, t, testbin.Path(t, "./cmd/orkctl"), repoRoot(t),
			[]string{orker.WorkRootVar + "=" + bindableOrkerWorkRoot(t)},
			append([]string{"activity", "test", reference, "--json", "--grace-period", "2s"}, args...)...)
		var report struct {
			Assertions []activityTestOutcome `json:"assertions"`
		}
		if err := json.Unmarshal([]byte(stdout), &report); err != nil {
			t.Fatalf("activity test exited %d with no JSON report: %v\nstdout:\n%s\nstderr:\n%s", code, err, stdout, stderr)
		}
		outcomes := map[string]string{}
		for _, a := range report.Assertions {
			outcomes[a.Name] = strings.TrimSpace(a.Status + " " + a.Error)
		}
		return outcomes, code
	}
	without, _ := run()
	with, code := run("--with-identity")
	const name = "an identity's token and expiration are read through current, and a refresh swaps them"
	if with[name] != "PASS" || code != 0 {
		t.Fatalf("with --with-identity: exit %d, %q = %q\n%v", code, name, with[name], with)
	}
	if _, there := without[name]; there || len(with) != len(without)+1 {
		t.Fatalf("without the flag %d checks, with it %d; want one more: %v, %v", len(without), len(with), without, with)
	}
	t.Logf("EVIDENCE activity test --with-identity: %s", with[name])
}
