package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const fakeSuite = modulePath + "/test/tools/proofrun/testdata/fakesuite"

// TestIntegrationRunner drives the real runner over testdata/fakesuite: it
// compiles the package once, lists it, runs it as jobs on two slots, and the
// ledger tells every outcome apart — including the test that was running when
// its binary died (failed, never re-run) and the test behind it (re-run in a
// fresh process, and passed).
func TestIntegrationRunner(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles a test binary")
	}
	quiet(t)
	out := t.TempDir()
	costsFile := filepath.Join(out, "costs.tsv")
	var b strings.Builder
	for _, n := range []string{"TestPass", "TestFail", "TestSkip", "TestAllowedSkip", "TestSubSkip", "TestCwd", "TestNamespace", "TestCrash", "TestAfterCrash"} {
		b.WriteString(fakeSuite + "\t" + n + "\t1\n")
	}
	must.OK(t, os.WriteFile(costsFile, []byte(b.String()), 0o644))
	allowFile := filepath.Join(out, "allow.txt")
	must.OK(t, os.WriteFile(allowFile, []byte("integration\t"+fakeSuite+"\tTestAllowedSkip\tan opt-in recorder\n"), 0o644))
	t.Setenv("FAKESUITE_CRASH", "1")

	code := cmdIntegration([]string{"-out", out, "-jobs", "2", "-namespace-prefix", "fake",
		"-costs", costsFile, "-allow", allowFile, "./testdata/fakesuite"})
	if code != 1 {
		t.Fatalf("exit %d, want 1 (the suite has failures)", code)
	}
	var l ledger
	must.OK(t, readJSON(filepath.Join(out, "integration-tests.json"), &l))
	got := map[string]entry{}
	for _, e := range l.Tests {
		got[e.Test] = e
	}
	want := map[string]string{
		"TestPass": outcomePass, "TestFail": outcomeFail, "TestSkip": outcomeSkip, "TestAllowedSkip": outcomeSkip,
		"TestSubSkip": outcomePass, "TestCwd": outcomePass, "TestNamespace": outcomePass,
		"TestCrash": outcomeFail, "TestAfterCrash": outcomePass,
	}
	for name, outcome := range want {
		if got[name].Outcome != outcome {
			t.Errorf("%s = %+v, want %s", name, got[name], outcome)
		}
	}
	if _, ok := got["TestUnitOnly"]; !ok {
		t.Error("the integration build also holds the package's untagged tests; TestUnitOnly is missing")
	}
	if !strings.Contains(got["TestCrash"].Reason, "ended while it ran") {
		t.Errorf("TestCrash reason = %q", got["TestCrash"].Reason)
	}
	if !strings.HasSuffix(got["TestAfterCrash"].Job, ".2") {
		t.Errorf("TestAfterCrash ran in job %q, want a second process", got["TestAfterCrash"].Job)
	}
	if got["TestAllowedSkip"].AllowedSkip == "" {
		t.Error("TestAllowedSkip is not marked allowed")
	}
	problems := strings.Join(l.Problems, "\n")
	for _, f := range []string{"TestFail failed", "TestSkip skipped", "TestSubSkip/inner skipped", "TestCrash its test binary ended while it ran"} {
		if !strings.Contains(problems, f) {
			t.Errorf("problems lack %q:\n%s", f, problems)
		}
	}
	if strings.Contains(problems, "TestAllowedSkip") {
		t.Errorf("an allowed skip is a problem:\n%s", problems)
	}
	for _, f := range []string{"integration-plan.json", "integration.events.jsonl"} {
		if _, err := os.Stat(filepath.Join(out, f)); err != nil {
			t.Error(err)
		}
	}
}

func TestUnitRunner(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test")
	}
	quiet(t)
	out := t.TempDir()
	if code := cmdUnit([]string{"-out", out, "-allow", "", "./testdata/fakesuite"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	var l ledger
	must.OK(t, readJSON(filepath.Join(out, "unit-tests.json"), &l))
	if l.Summary.Expected != 1 || l.Summary.Pass != 1 || l.Tests[0].Test != "TestUnitOnly" {
		t.Errorf("ledger = %+v", l)
	}
}

func TestIntegrationPackagesOnlyListsPackagesTheTagChanges(t *testing.T) {
	pkgs, err := integrationPackages([]string{"./testdata/fakesuite", "."})
	must.OK(t, err)
	if len(pkgs) != 1 || pkgs[0].ImportPath != fakeSuite {
		t.Errorf("got %+v, want only %s (proofrun itself has no integration files)", pkgs, fakeSuite)
	}
}

// quiet sends the runners' report to the test log for one test.
func quiet(t *testing.T) {
	t.Helper()
	prev := stdout
	stdout = logWriter{t}
	t.Cleanup(func() { stdout = prev })
}

type logWriter struct{ t *testing.T }

func (w logWriter) Write(b []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(b), "\n"))
	return len(b), nil
}
