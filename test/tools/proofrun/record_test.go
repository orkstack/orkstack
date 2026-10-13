package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestScanBinaries(t *testing.T) {
	fs := []srcFile{
		{Name: "a_integration_test.go", Content: []byte(`//go:build integration

package main

import (
	"os/exec"
	"testing"

	tb "github.com/orkstack/orkstack/internal/testsupport/testbin"
)

func TestA(t *testing.T) {
	_ = tb.Path(t, "./cmd/ork")
	_ = tb.Path(t, "./test/tools/cosign-sign")
	name := "orker"
	_ = tb.Path(t, "./cmd/"+name)
	_ = exec.Command("go", "build", "-o", "x", "./cmd/orkctl")
	_ = exec.CommandContext(nil, "go", "run", "./test/tools/bucket-ensure")
	_ = exec.Command("docker", "build", ".")
	_ = exec.Command("go", "version")
}
`)},
		{Name: "b.go", Content: []byte("package main\n\nimport \"os/exec\"\n\nvar _ = exec.Command(\"go\", \"build\")\n")},
	}
	uses, bad := scanBinaries("cmd/orkctl", fs)
	var got []string
	for _, u := range uses {
		got = append(got, relDir(u.Package))
	}
	if !reflect.DeepEqual(got, []string{"cmd/ork", "test/tools/cosign-sign"}) {
		t.Errorf("uses = %v", got)
	}
	if len(bad) != 3 {
		t.Fatalf("unresolved = %v, want the computed testbin path and the two exec builds", bad)
	}
	joined := strings.Join(bad, "\n")
	for _, f := range []string{"a_integration_test.go:16", "a_integration_test.go:17", "a_integration_test.go:18", "use testbin.Path"} {
		if !strings.Contains(joined, f) {
			t.Errorf("unresolved lacks %q:\n%s", f, joined)
		}
	}
}

func TestDependsOn(t *testing.T) {
	d := fakeDeps()
	cases := []struct {
		pkg, target string
		want        bool
		how         string
	}{
		{pEngine, pEngine, true, "is the package"},
		{pEngine, pDSL, true, "imports it"},
		{pOrkctl, pEngine, true, "runs cmd/ork, which links it"},
		{pOrkctl, pOrker, true, "runs cmd/orker, which links it"},
		{pEngine, pOrker, false, ""},
	}
	for _, c := range cases {
		got, how := d.dependsOn(cfgIntegration, c.pkg, c.target)
		if got != c.want || how != c.how {
			t.Errorf("%s -> %s = %v %q, want %v %q", c.pkg, c.target, got, how, c.want, c.how)
		}
	}
}

func TestComposeTemporal(t *testing.T) {
	doc := `{"name":"orkstack","services":{"temporal":{"image":"temporalio/temporal:1.8.2","command":["server","start-dev","--ip","0.0.0.0","--ui-port","8233","--log-level","warn"],"ports":[{"target":7233}]},"minio":{"image":"minio/minio"}}}`
	image, args, err := composeTemporal([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if image != "temporalio/temporal:1.8.2" || strings.Join(args, " ") != "server start-dev --ip 0.0.0.0 --ui-port 8233 --log-level warn" {
		t.Errorf("image %q args %v", image, args)
	}
	if _, args, _ := composeTemporal([]byte(`{"services":{"temporal":{"image":"t","command":"server start-dev"}}}`)); strings.Join(args, " ") != "server start-dev" {
		t.Errorf("string command: %v", args)
	}
	if _, _, err := composeTemporal([]byte(`{"services":{"minio":{"image":"m"}}}`)); err == nil {
		t.Error("a compose file without temporal was accepted")
	}
}

func passingFull() proofRecord {
	r := proofRecord{Tier: "full", Tree: "t1", TreeEnd: "t1", CleanStart: true, CleanEnd: true, ImageTag: "pr-abc", ImageTagOwn: true, BaseRef: "main", BaseCommit: "m1"}
	r.Temporal.Mode = "own"
	for _, g := range fullGates {
		r.Gates = append(r.Gates, gateRecord{Gate: g, Result: "pass"})
	}
	return r
}

func TestVerdict(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*proofRecord)
		status   int
		obs      bool
		ledgers  map[string]bool
		result   string
		eligible bool
		reason   string
	}{
		{name: "a clean full proof", result: "pass", eligible: true},
		{name: "a failed gate", mutate: func(r *proofRecord) { r.Gates[5].Result = "fail" }, result: "fail", reason: "the proof did not pass"},
		{name: "the script's own failure", status: 1, result: "fail", reason: "the proof did not pass"},
		{name: "a missing gate", mutate: func(r *proofRecord) { r.Gates = r.Gates[:len(r.Gates)-1] }, result: "pass", reason: "gate ui-e2e did not run"},
		{name: "observability required", obs: true, result: "pass", reason: "gate observability did not run"},
		{name: "PROOF_GATES", mutate: func(r *proofRecord) { r.GatesOverride = "unit" }, result: "pass", reason: "PROOF_GATES=unit chose the gates"},
		{name: "a dirty start", mutate: func(r *proofRecord) { r.CleanStart = false }, result: "pass", reason: "not clean when the proof started"},
		{name: "a tree that moved", mutate: func(r *proofRecord) { r.TreeEnd = "t2" }, result: "pass", reason: "the tree changed while the proof ran"},
		{name: "shared Temporal", mutate: func(r *proofRecord) { r.Temporal.Mode = "shared" }, result: "pass", reason: "PROOF_TEMPORAL=shared"},
		{name: "an outside image tag", mutate: func(r *proofRecord) { r.ImageTagOwn = false }, result: "pass", reason: "ORK_TEST_IMAGE_TAG was supplied from outside"},
		{name: "network churn", mutate: func(r *proofRecord) { n, c := 4, 3; r.GuardRetries, r.GuardCeiling = &n, &c }, result: "pass", reason: "network-change retries above the ceiling"},
		{name: "a base other than main", mutate: func(r *proofRecord) { r.BaseRef = "HEAD" }, result: "pass", reason: "the full proof diffed against HEAD, not main"},
		{name: "no base recorded", mutate: func(r *proofRecord) { r.BaseRef = "" }, result: "pass", reason: "the full proof diffed against -, not main"},
		{name: "an incomplete ledger", ledgers: map[string]bool{"integration": false}, result: "fail", reason: "the integration ledger is incomplete"},
		{name: "a feature proof", mutate: func(r *proofRecord) { r.Tier = "feature" }, result: "pass", reason: "a feature proof is never merge evidence"},
		{name: "failed evidence", mutate: func(r *proofRecord) { r.Tier = "feature"; r.EvidenceFailed = []string{"x"} }, result: "fail", reason: "the proof did not pass"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := passingFull()
			if c.mutate != nil {
				c.mutate(&r)
			}
			verdict(&r, c.status, c.obs, c.ledgers)
			if r.Result != c.result || r.MergeEligible != c.eligible {
				t.Errorf("result %s eligible %v, want %s %v (%v)", r.Result, r.MergeEligible, c.result, c.eligible, r.Ineligible)
			}
			if c.reason != "" && !strings.Contains(strings.Join(r.Ineligible, "\n"), c.reason) {
				t.Errorf("reasons %v lack %q", r.Ineligible, c.reason)
			}
		})
	}
}

func TestWriteVerdictKeepsTheFullTierResultLines(t *testing.T) {
	for _, c := range []struct {
		tier, result, want string
	}{
		{"full", "pass", "Result: **PASS**\n"},
		{"full", "fail", "Result: **FAIL**\n"},
		{"feature", "pass", "Result: **FEATURE PASS — not merge evidence**\n"},
		{"feature", "fail", "Result: **FEATURE FAIL — not merge evidence**\n"},
	} {
		var b strings.Builder
		writeVerdict(&b, &proofRecord{Tier: c.tier, Result: c.result})
		if !strings.HasSuffix(b.String(), c.want) {
			t.Errorf("%s/%s ends %q", c.tier, c.result, b.String())
		}
		if c.tier == "feature" && strings.Contains(b.String(), "**PASS**") {
			t.Errorf("a feature verdict contains the full tier's **PASS**")
		}
	}
}

func withBase(r proofRecord, commit string) proofRecord {
	r.BaseCommit = commit
	return r
}

func TestVerifyProblems(t *testing.T) {
	good := passingFull()
	verdict(&good, 0, false, nil)
	if p := verifyProblems(good, "t1", "m1", "m1", "", "main"); len(p) != 0 {
		t.Fatalf("a good candidate was refused: %v", p)
	}
	feature := good
	feature.Tier = "feature"
	cases := []struct {
		name                         string
		r                            proofRecord
		tree, parent, mainTip, dirty string
		want                         string
	}{
		{"another tree", good, "t2", "m1", "m1", "", "it proved tree t1, and HEAD's tree is t2"},
		{"main moved", good, "t1", "m1", "m2", "", "is not main's current tip"},
		{"no parent", good, "t1", "", "m1", "", "is not main's current tip"},
		{"dirty", good, "t1", "m1", "m1", "?? x\n", "the worktree is not clean"},
		{"a feature proof", feature, "t1", "m1", "m1", "", "only a full proof gates a merge"},
		// --base HEAD made the observability gate "not due": the record's base
		// is the candidate itself, never main's tip.
		{"another base", withBase(good, "c1"), "t1", "m1", "m1", "", "it diffed against c1, not main's current tip (m1)"},
		{"no base", withBase(good, ""), "t1", "m1", "m1", "", "it diffed against -, not main's current tip"},
	}
	for _, c := range cases {
		p := strings.Join(verifyProblems(c.r, c.tree, c.parent, c.mainTip, c.dirty, "main"), "\n")
		if !strings.Contains(p, c.want) {
			t.Errorf("%s: problems %q lack %q", c.name, p, c.want)
		}
	}
}

func TestEvidenceFailures(t *testing.T) {
	sel := &selection{
		UnitEvidence: []selectedTest{{Package: pDSL, Test: "TestParse"}},
		Integration:  map[string]*pkgSelection{pEngine: {Evidence: map[string]string{"TestIntegration_Uses": "named", "TestIntegration_Recorder": "named"}}},
		Specs:        []selectedSpec{{File: "ui/e2e/admin.spec.ts"}},
	}
	o := outcomes{
		{pDSL, "TestParse"}:                   {Outcome: outcomePass},
		{pEngine, "TestIntegration_Uses"}:     {Outcome: outcomePass},
		{pEngine, "TestIntegration_Recorder"}: {Outcome: outcomeSkip, AllowedSkip: "recorder"},
	}
	got := evidenceFailures(sel, o, map[string]string{"ui/e2e/admin.spec.ts": outcomePass})
	if !reflect.DeepEqual(got, []string{"internal/engine.TestIntegration_Recorder: skip"}) {
		t.Errorf("got %v: an allowed skip is still not evidence", got)
	}
}

func TestUILedger(t *testing.T) {
	spec := func(title string, tests ...pwTest) pwSpec { return pwSpec{Title: title, Tests: tests} }
	listed := pwReport{Suites: []pwSuite{{Title: "a.spec.ts", File: "a.spec.ts", Specs: []pwSpec{
		spec("passes", pwTest{ProjectName: "chromium"}),
		spec("skips", pwTest{ProjectName: "chromium"}),
		spec("flakes", pwTest{ProjectName: "chromium"}),
		spec("never ran", pwTest{ProjectName: "chromium"}),
	}, Suites: []pwSuite{{Title: "group", Specs: []pwSpec{spec("nested", pwTest{ProjectName: "chromium"})}}}}}}
	report := pwReport{Suites: []pwSuite{{Title: "a.spec.ts", File: "a.spec.ts", Specs: []pwSpec{
		spec("passes", pwTest{ProjectName: "chromium", Status: "expected", Results: []pwResult{{Status: "passed", Duration: 1500}}}),
		spec("skips", pwTest{ProjectName: "chromium", Status: "skipped", Results: []pwResult{{Status: "skipped"}}}),
		spec("flakes", pwTest{ProjectName: "chromium", Status: "flaky", Results: []pwResult{{Status: "failed"}, {Status: "passed"}}}),
	}, Suites: []pwSuite{{Title: "group", Specs: []pwSpec{spec("nested", pwTest{ProjectName: "chromium", Status: "expected", Results: []pwResult{{Status: "passed"}}})}}}}}}

	l := buildUILedger(listed, report, 2, 3)
	if l.Listed != 5 || l.Pass != 2 {
		t.Errorf("listed %d pass %d", l.Listed, l.Pass)
	}
	p := strings.Join(l.Problems, "\n")
	for _, f := range []string{"a.spec.ts › skips [chromium] skipped", "a.spec.ts › flakes [chromium] failed (flaky", "a.spec.ts › never ran [chromium] has no outcome"} {
		if !strings.Contains(p, f) {
			t.Errorf("problems lack %q:\n%s", f, p)
		}
	}
	if strings.Contains(p, "nested") || strings.Contains(p, "churn") {
		t.Errorf("unexpected problem:\n%s", p)
	}
	if l := buildUILedger(listed, report, 4, 3); !strings.Contains(strings.Join(l.Problems, "\n"), "network churn above ceiling; evidence taken on a noisy host") {
		t.Error("retries above the ceiling did not fail the ledger")
	}
}

func TestRetryCeilingIsDeclaredInThePlaywrightConfig(t *testing.T) {
	n, err := retryCeiling("../../../ui/playwright.config.ts")
	if err != nil {
		t.Fatal(err)
	}
	if n < 0 || n > 3 {
		t.Errorf("ceiling %d: 0 once the browser is isolated, at most 3 while it shares the host's network", n)
	}
}

// A gate that ran without leaving its ledger fails the proof.
func TestRecordRequiresTheLedgerOfEveryGateThatRan(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "gates.tsv"), []byte("unit\tpass\t3\tunit.log\tproofrun unit\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	prev := os.Stdout
	devnull, _ := os.Open(os.DevNull)
	os.Stdout = devnull
	code := cmdRecord([]string{"-dir", dir, "-tier", "feature", "-tree-start", "t", "-tree-end", "t", "-clean-start", "-clean-end"})
	os.Stdout = prev
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	var r proofRecord
	if err := readJSON(filepath.Join(dir, "proof.json"), &r); err != nil {
		t.Fatal(err)
	}
	if r.Result != "fail" || !strings.Contains(strings.Join(r.Ineligible, "\n"), "the unit ledger is incomplete") {
		t.Errorf("record = %+v", r)
	}
}
