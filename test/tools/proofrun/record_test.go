package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestComposeTemporal(t *testing.T) {
	doc := `{"name":"orkstack","services":{"temporal":{"image":"temporalio/temporal:1.8.2","command":["server","start-dev","--ip","0.0.0.0","--ui-port","8233","--log-level","warn"],"ports":[{"target":7233}]},"minio":{"image":"minio/minio"}}}`
	image, args, err := composeTemporal([]byte(doc))
	must.OK(t, err)
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
	r := proofRecord{Tier: "full", Tree: "t1", TreeEnd: "t1", CleanStart: true, CleanEnd: true, ImageTag: "pr-abc", ImageTagOwn: true, BaseRef: "main", BaseCommit: "m1",
		Quarantine: &quarantineRecord{CheckedOn: todayUTC().Format("2006-01-02")}}
	r.Temporal.Mode = "own"
	for _, g := range fullGates {
		r.Gates = append(r.Gates, gateRecord{Gate: g, Result: "pass"})
	}
	return r
}

// withoutGate is gates less every record of gate.
func withoutGate(gates []gateRecord, gate string) []gateRecord {
	var out []gateRecord
	for _, g := range gates {
		if g.Gate != gate {
			out = append(out, g)
		}
	}
	return out
}

// setGate gives the record of gate a result, in place.
func setGate(gates []gateRecord, gate, result string) {
	for i := range gates {
		if gates[i].Gate == gate {
			gates[i].Result = result
		}
	}
}

// The merge gate's gate list names the site, after the UI gates and before
// Playwright: a full proof without a passing site gate is not merge-eligible.
func TestFullGates_NameTheSiteGateAmongTheFastGates(t *testing.T) {
	idx := map[string]int{}
	for i, g := range fullGates {
		if _, dup := idx[g]; dup {
			t.Errorf("fullGates names %s twice", g)
		}
		idx[g] = i
	}
	site, ok := idx["site"]
	if !ok {
		t.Fatalf("fullGates = %v: no site gate", fullGates)
	}
	if site != idx["ui-unit"]+1 || idx["ui-e2e"] != len(fullGates)-2 || idx["observability"] != len(fullGates)-1 {
		t.Errorf("fullGates = %v: the site gate runs right after ui-unit, then ui-e2e, and observability last", fullGates)
	}
	// The nightly tier runs the deferred tests and nothing else.
	r := passingFull()
	r.Tier = "nightly"
	r.Gates = []gateRecord{{Gate: "integration", Result: "pass"}}
	verdict(&r, 0, map[string]bool{"integration": true})
	if strings.Contains(strings.Join(r.Ineligible, "\n"), "gate site") {
		t.Errorf("a nightly proof is held to the site gate: %v", r.Ineligible)
	}
}

func TestVerdict(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*proofRecord)
		status   int
		ledgers  map[string]bool
		result   string
		eligible bool
		reason   string
	}{
		{name: "a clean full proof", result: "pass", eligible: true},
		{name: "a failed gate", mutate: func(r *proofRecord) { r.Gates[5].Result = "fail" }, result: "fail", reason: "the proof did not pass"},
		{name: "the script's own failure", status: 1, result: "fail", reason: "the proof did not pass"},
		{name: "a missing gate", mutate: func(r *proofRecord) { r.Gates = withoutGate(r.Gates, "ui-e2e") }, result: "pass", reason: "gate ui-e2e did not run"},
		{name: "the site gate missing", mutate: func(r *proofRecord) { r.Gates = withoutGate(r.Gates, "site") }, result: "pass", reason: "gate site did not run"},
		{name: "the site gate failed", mutate: func(r *proofRecord) { setGate(r.Gates, "site", "fail") }, result: "fail", reason: "the proof did not pass"},
		{name: "the observability gate missing", mutate: func(r *proofRecord) { r.Gates = withoutGate(r.Gates, "observability") }, result: "pass", reason: "gate observability did not run"},
		{name: "PROOF_GATES", mutate: func(r *proofRecord) { r.GatesOverride = "unit" }, result: "pass", reason: "PROOF_GATES=unit chose the gates"},
		{name: "a dirty start", mutate: func(r *proofRecord) { r.CleanStart = false }, result: "pass", reason: "not clean when the proof started"},
		{name: "a tree that moved", mutate: func(r *proofRecord) { r.TreeEnd = "t2" }, result: "pass", reason: "the tree changed while the proof ran"},
		{name: "shared Temporal", mutate: func(r *proofRecord) { r.Temporal.Mode = "shared" }, result: "pass", reason: "PROOF_TEMPORAL=shared"},
		{name: "an outside image tag", mutate: func(r *proofRecord) { r.ImageTagOwn = false }, result: "pass", reason: "ORK_TEST_IMAGE_TAG was supplied from outside"},
		{name: "network churn", mutate: func(r *proofRecord) { n, c := 4, 3; r.GuardRetries, r.GuardCeiling = &n, &c }, result: "pass", reason: "network-change retries above the ceiling"},
		{name: "a base other than main", mutate: func(r *proofRecord) { r.BaseRef = "HEAD" }, result: "pass", reason: "the full proof diffed against HEAD, not main"},
		{name: "no base recorded", mutate: func(r *proofRecord) { r.BaseRef = "" }, result: "pass", reason: "the full proof diffed against -, not main"},
		{name: "an incomplete ledger", ledgers: map[string]bool{"integration": false}, result: "fail", reason: "the integration ledger is incomplete"},
		{name: "a nightly proof", mutate: func(r *proofRecord) { r.Tier = tierNightly }, result: "pass", reason: "only a full proof is merge evidence"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := passingFull()
			if c.mutate != nil {
				c.mutate(&r)
			}
			verdict(&r, c.status, c.ledgers)
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
	} {
		var b strings.Builder
		writeVerdict(&b, &proofRecord{Tier: c.tier, Result: c.result})
		if !strings.HasSuffix(b.String(), c.want) {
			t.Errorf("%s/%s ends %q", c.tier, c.result, b.String())
		}
	}
}

func withBase(r proofRecord, commit string) proofRecord {
	r.BaseCommit = commit
	return r
}

func TestVerifyProblems(t *testing.T) {
	good := passingFull()
	verdict(&good, 0, nil)
	if p := verifyProblems(good, "t1", "m1", "m1", "", "main"); len(p) != 0 {
		t.Fatalf("a good candidate was refused: %v", p)
	}
	nightly := good
	nightly.Tier = tierNightly
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
		{"a nightly proof", nightly, "t1", "m1", "m1", "", "only a full proof gates a merge"},
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
	must.OK(t, err)
	if n < 0 || n > 3 {
		t.Errorf("ceiling %d: 0 once the browser is isolated, at most 3 while it shares the host's network", n)
	}
}

func TestRecord_TheFullTiersWriteOnlyAPart(t *testing.T) {
	for _, tier := range []string{tierFull, tierRelease, tierNightly} {
		t.Run(tier, func(t *testing.T) {
			dir := t.TempDir()
			out := filepath.Join(dir, "parts", "local-a1")
			must.OK(t, os.WriteFile(filepath.Join(dir, "gates.tsv"), []byte("unit\tpass\t3\tunit.log\tproofrun unit\n"), 0o644))
			prev := os.Stdout
			devnull, _ := os.Open(os.DevNull)
			os.Stdout = devnull
			code := cmdRecord([]string{"-dir", dir, "-tier", tier, "-attempt", "1", "-tree-start", "t", "-tree-end", "t", "-clean-start", "-clean-end", "-out-dir", out})
			os.Stdout = prev
			if code != 0 {
				t.Fatalf("exit %d, want 0", code)
			}
			var rec partRecord
			must.OKf(t, readJSON(filepath.Join(out, "part.json"), &rec), "no part.json")
			if rec.Part != "local" || rec.Tier != tier {
				t.Errorf("part %+v, want part local of tier %s", rec, tier)
			}
			if _, err := os.Stat(filepath.Join(out, "proof.json")); !os.IsNotExist(err) {
				t.Errorf("a %s record wrote proof.json; the merger decides the verdict", tier)
			}
		})
	}
}

// A part names its runner's setup log and the record carries every line of
// it, so a fetch that needed its second attempt reaches the merger; a log the
// part names and cannot read is an error, never a part that reads as if its
// runner's setup went unrecorded.
func TestRecord_CarriesTheSetupLog(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "parts", "integration-1-a1")
	must.OK(t, os.WriteFile(filepath.Join(dir, "gates.tsv"), []byte("integration\tpass\t3\tintegration.log\tproofrun integration\n"), 0o644))
	log := filepath.Join(dir, "setup.log")
	must.OK(t, os.WriteFile(log, []byte("fetch artifact plan-a1: attempt 1 timed out after 180s (limit 180s)\nfetch artifact plan-a1: attempt 2 ok in 14s (limit 360s)\n\n"), 0o644))
	quiet(t)
	prev := os.Stdout
	devnull, _ := os.Open(os.DevNull)
	os.Stdout = devnull
	defer func() { os.Stdout = prev }()
	base := []string{"-dir", dir, "-tier", tierFull, "-part", "integration:1", "-attempt", "1", "-tree-start", "t", "-tree-end", "t", "-clean-start", "-clean-end", "-out-dir", out}
	if code := cmdRecord(append(base, "-setup-log", log)); code != 0 {
		t.Fatalf("exit %d, want 0", code)
	}
	var rec partRecord
	must.OKf(t, readJSON(filepath.Join(out, "part.json"), &rec), "no part.json")
	if want := []string{
		"fetch artifact plan-a1: attempt 1 timed out after 180s (limit 180s)",
		"fetch artifact plan-a1: attempt 2 ok in 14s (limit 360s)",
	}; strings.Join(rec.Setup, "\n") != strings.Join(want, "\n") {
		t.Errorf("setup = %q, want %q", rec.Setup, want)
	}
	if code := cmdRecord(append(base, "-setup-log", filepath.Join(dir, "absent.log"))); code != 2 {
		t.Errorf("an unreadable setup log exited %d, want 2", code)
	}
}
