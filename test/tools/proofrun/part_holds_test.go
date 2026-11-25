package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestPartHoldsTest_ReadsThePlan: scripts/proof-part.sh fetches the NVIDIA
// emulation test's prerequisites only in the part whose plan gives it that
// test. The plan is this package's own type, so a field renamed here fails the
// script's reading of it; a plan that cannot be read says yes, since a
// prerequisite fetched for nothing is better than one missing.
func TestPartHoldsTest_ReadsThePlan(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq, which a proof's host has, is not here to read the plan with")
	}
	script, err := os.ReadFile("../../../scripts/proof-part.sh")
	must.OK(t, err)
	function := regexp.MustCompile(`(?ms)^part_holds_test\(\) \{$.*?^\}$`).Find(script)
	if function == nil {
		t.Fatal("scripts/proof-part.sh has no part_holds_test")
	}
	plan := filepath.Join(t.TempDir(), "plan.json")
	must.OK(t, writeJSON(plan, integrationPlan{Jobs: []job{
		{ID: "a", Tests: []string{"TestIntegration_Other"}, Shard: 1},
		{ID: "b", Tests: []string{"TestIntegration_Before", "TestIntegration_Wanted"}, Shard: 2},
		{ID: "c", Tests: []string{"TestIntegration_WantedToo"}, Shard: 3},
	}}))
	unreadable := filepath.Join(t.TempDir(), "unreadable.json")
	must.OK(t, os.WriteFile(unreadable, []byte("{"), 0o644))
	for name, tc := range map[string]struct {
		plan, part string
		holds      bool
	}{
		"the part that holds it":      {plan, "integration:2", true},
		"a part that does not":        {plan, "integration:1", false},
		"a test whose name begins so": {plan, "integration:3", false},
		"no plan":                     {plan + ".none", "integration:1", true},
		"a plan that does not read":   {unreadable, "integration:1", true},
	} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command("bash", "-c", string(function)+"\npart_holds_test TestIntegration_Wanted")
			cmd.Env = append(os.Environ(), "PLAN="+tc.plan, "PART="+tc.part)
			out, err := cmd.CombinedOutput()
			if code := cmd.ProcessState.ExitCode(); (code == 0) != tc.holds || code > 1 {
				t.Fatalf("part_holds_test = exit %d (%v): %s; want holds %v", code, err, out, tc.holds)
			}
		})
	}
}

// TestPartRequire_TheNVIDIAEmulation: an integration part makes that test's
// prerequisites ready only when its plan holds it and nothing else is missing,
// and fails when they are not what they should be.
func TestPartRequire_TheNVIDIAEmulation(t *testing.T) {
	script, err := os.ReadFile("../../../scripts/proof-part.sh")
	must.OK(t, err)
	function := regexp.MustCompile(`(?ms)^part_require\(\) \{$.*?^\}$`).Find(script)
	if function == nil {
		t.Fatal("scripts/proof-part.sh has no part_require")
	}
	for name, tc := range map[string]struct {
		holds, ready int
		missing      string
		want         int
		asked        bool
	}{
		"holds it":                 {holds: 0, ready: 0, want: 0, asked: true},
		"does not hold it":         {holds: 1, ready: 1, want: 0},
		"not what it should be":    {holds: 0, ready: 1, want: 1, asked: true},
		"something else is absent": {holds: 0, ready: 1, missing: "a tool", want: 77},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			asked := filepath.Join(dir, "asked")
			cmd := exec.Command("bash", "-c", `
part_need() { [ -z "$ABSENT" ] || echo "  - $ABSENT" >>"$PART_MISSING"; }
part_say() { :; }
part_holds_test() { return "$HOLDS"; }
part_nvidia_emulation() { touch "$ASKED"; return "$READY"; }
`+string(function)+"\npart_require")
			cmd.Env = append(os.Environ(), "PART=integration:1", "PART_MISSING="+filepath.Join(dir, "missing"), "ASKED="+asked,
				"HOLDS="+strconv.Itoa(tc.holds), "READY="+strconv.Itoa(tc.ready), "ABSENT="+tc.missing, "REGISTRY=r", "LAYOUTS=l", "PLAN=p")
			out, _ := cmd.CombinedOutput()
			_, err := os.Stat(asked)
			if code := cmd.ProcessState.ExitCode(); code != tc.want || (err == nil) != tc.asked {
				t.Fatalf("part_require = exit %d, asked %v: %s; want exit %d, asked %v", code, err == nil, out, tc.want, tc.asked)
			}
		})
	}
}

// TestIntegrationScript_TheNVIDIAEmulation: scripts/integration.sh makes that
// test's prerequisites ready when the packages and pattern it is given hold
// the test, and gives it their directory; it ends with their own status when
// they are not ready.
func TestIntegrationScript_TheNVIDIAEmulation(t *testing.T) {
	bin := t.TempDir()
	must.OK(t, os.WriteFile(filepath.Join(bin, "go"), []byte("#!/bin/sh\ncase \"$*\" in *\" integration \"*) echo \"hooks=$ORK_TEST_NVIDIA_HOOKS\" ;; esac\n"), 0o755))
	ready := filepath.Join(bin, "ready")
	must.OK(t, os.WriteFile(ready, []byte("#!/bin/sh\necho asked >&2\n[ \"$RC\" = 0 ] && echo /the/hooks\nexit \"$RC\"\n"), 0o755))
	for name, tc := range map[string]struct {
		args  []string
		rc    string
		want  int
		asked bool
		hooks string
	}{
		"every package":        {nil, "0", 0, true, "/the/hooks"},
		"its package":          {[]string{"./internal/orker"}, "0", 0, true, "/the/hooks"},
		"its package, its own": {[]string{"./internal/orker", "-run", "^TestIntegration_NVIDIAEmulation$/one"}, "0", 0, true, "/the/hooks"},
		"another of its tests": {[]string{"./internal/orker", "-run", "^TestIntegration_BootProbe"}, "0", 0, false, ""},
		"another package":      {[]string{"./internal/engine"}, "0", 0, false, ""},
		"unreachable":          {[]string{"./internal/orker"}, "77", 77, true, ""},
		"not the table's":      {[]string{"./internal/orker"}, "1", 1, true, ""},
	} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command("bash", append([]string{"../../../scripts/integration.sh"}, tc.args...)...)
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "NVIDIA_EMULATION="+ready, "RC="+tc.rc, "ORK_TEST_NVIDIA_HOOKS=", "ORK_REQUIRE_INFRA=")
			out, _ := cmd.CombinedOutput()
			if m := regexp.MustCompile(`per-job logs in (\S+)`).FindSubmatch(out); m != nil {
				must.OK(t, os.RemoveAll(string(m[1])))
			}
			if code := cmd.ProcessState.ExitCode(); code != tc.want || strings.Contains(string(out), "asked") != tc.asked {
				t.Fatalf("integration.sh %v = exit %d: %s; want exit %d, asked %v", tc.args, code, out, tc.want, tc.asked)
			}
			if tc.want == 0 && !strings.Contains(string(out), "hooks="+tc.hooks+"\n") {
				t.Fatalf("integration.sh %v gave the tests %s; want hooks=%q", tc.args, out, tc.hooks)
			}
		})
	}
}
