package main

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// fakeDir is the fake suite's directory, relative to the repository root, as
// a plan records it.
const fakeDir = "test/tools/proofrun/testdata/fakesuite"

// fakeListing is what the fake suite's integration binary lists.
var fakeListing = []string{"TestPass", "TestFail", "TestSkip", "TestAllowedSkip", "TestSubSkip", "TestCwd", "TestNamespace", "TestCrash", "TestAfterCrash", "TestUnitOnly"}

// stubTree makes every shard in this test see tree as its checkout's.
func stubTree(t *testing.T, tree string) {
	t.Helper()
	prev := checkoutTree
	checkoutTree = func(string) (string, error) { return tree, nil }
	t.Cleanup(func() { checkoutTree = prev })
}

// twoShardPlan is a plan of the fake suite: shard 1 runs TestPass and
// TestNamespace, shard 2 runs TestCwd.
func twoShardPlan(pkg planPackage) *integrationPlan {
	return &integrationPlan{
		Format: planFormat, Tree: "plan-tree", Tier: tierFull, CheckedOn: "2026-10-21",
		Packages: []planPackage{pkg},
		Expected: []testRef{{fakeSuite, "TestCwd"}, {fakeSuite, "TestNamespace"}, {fakeSuite, "TestPass"}},
		Jobs: []job{
			{ID: "fakesuite-001", Package: fakeSuite, Tests: []string{"TestPass", "TestNamespace"}, Cost: 1, Known: true, Shard: 1, TimeoutSeconds: 600},
			{ID: "fakesuite-002", Package: fakeSuite, Tests: []string{"TestCwd"}, Cost: 1, Known: true, Shard: 2, TimeoutSeconds: 600},
		},
		Shards: []planShard{{Index: 1, Jobs: []string{"fakesuite-001"}}, {Index: 2, Jobs: []string{"fakesuite-002"}}},
	}
}

// runShardOf writes p and runs shard k of it, returning the exit and the
// shard's ledger (if any).
func runShardOf(t *testing.T, p *integrationPlan, k int, extra ...string) (int, ledger) {
	t.Helper()
	code, l, _ := runShardOfStderr(t, p, k, extra...)
	return code, l
}

// captureStderr runs f and returns what it wrote to os.Stderr.
func captureStderr(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	prev := os.Stderr
	os.Stderr = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	func() {
		defer func() { os.Stderr = prev; _ = w.Close() }()
		f()
	}()
	return <-done
}

// runShardOfStderr is runShardOf, with what the shard wrote to stderr.
func runShardOfStderr(t *testing.T, p *integrationPlan, k int, extra ...string) (int, ledger, string) {
	t.Helper()
	quiet(t)
	dir := t.TempDir()
	planFile := filepath.Join(dir, "integration-plan.json")
	if err := writeJSON(planFile, p); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out")
	args := append([]string{"-plan", planFile, "-shard", itoa(k), "-out", out, "-jobs", "1", "-allow", "", "-quarantine", ""}, extra...)
	var code int
	stderr := captureStderr(t, func() { code = cmdIntegration(args) })
	var l ledger
	_ = readJSON(filepath.Join(out, "integration-tests.json"), &l)
	return code, l, stderr
}

// shippedScript writes an executable "binary" that lists names, as a
// -test.list run prints them, into dir under name.
func shippedScript(t *testing.T, dir, name string, names ...string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, name)
	script := "#!/bin/sh\nprintf '%s\\n' " + strings.Join(names, " ") + " 'ok  \tfake\t0.001s'\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

func TestIntegrationShard_RefusesAnotherTree(t *testing.T) {
	pkg := planPackage{ImportPath: fakeSuite, Dir: fakeDir, Binary: "fake.test", Listed: fakeListing}
	stubTree(t, "this-checkout")
	code, _, stderr := runShardOfStderr(t, twoShardPlan(pkg), 1)
	if code != 2 || !strings.Contains(stderr, "this plan was made for tree plan-tree, and this checkout is this-checkout") {
		t.Errorf("a plan of another tree: exit %d, stderr %q", code, stderr)
	}
	// A plan that names no tree is refused too: it fails closed.
	p := twoShardPlan(pkg)
	p.Tree = ""
	code, _, stderr = runShardOfStderr(t, p, 1)
	if code != 2 || !strings.Contains(stderr, "this plan was made for tree , and this checkout is this-checkout") {
		t.Errorf("a plan with no tree: exit %d, stderr %q", code, stderr)
	}
	// The positive control: the same plan on its own tree is not refused.
	stubTree(t, "plan-tree")
	bins := t.TempDir()
	bin := shippedScript(t, bins, "fake.test", fakeListing...)
	sha, _ := fileSHA256(bin)
	pkg.BinarySHA256 = sha
	code, _, stderr = runShardOfStderr(t, twoShardPlan(pkg), 1, "-bin", bins)
	if code == 2 || strings.Contains(stderr, "this plan was made for tree") {
		t.Errorf("the plan on its own tree was refused: exit %d, stderr %q", code, stderr)
	}
}

func TestIntegrationShard_RefusesABinaryWhoseListingDiffers(t *testing.T) {
	stubTree(t, "plan-tree")
	bins := t.TempDir()
	bin := shippedScript(t, bins, "fake.test", append(append([]string{}, fakeListing...), "TestAddedSincePlan")...)
	sha, _ := fileSHA256(bin)
	pkg := planPackage{ImportPath: fakeSuite, Dir: fakeDir, Binary: "fake.test", BinarySHA256: sha, Listed: fakeListing}
	code, _, stderr := runShardOfStderr(t, twoShardPlan(pkg), 1, "-bin", bins)
	if code != 2 || !strings.Contains(stderr, "TestAddedSincePlan") {
		t.Errorf("a binary that lists a test the plan did not: exit %d, stderr %q", code, stderr)
	}
	// The positive control: a shipped script listing exactly the plan's tests.
	good := t.TempDir()
	gbin := shippedScript(t, good, "fake.test", fakeListing...)
	gsha, _ := fileSHA256(gbin)
	pkg.BinarySHA256 = gsha
	code, _, stderr = runShardOfStderr(t, twoShardPlan(pkg), 1, "-bin", good)
	if code == 2 || strings.Contains(stderr, "TestAddedSincePlan") {
		t.Errorf("a shipped script that lists the plan's tests was refused: exit %d, stderr %q", code, stderr)
	}
}

func TestIntegrationShard_RefusesAShippedBinaryWhoseDigestDiffers(t *testing.T) {
	stubTree(t, "plan-tree")
	bins := t.TempDir()
	bin := shippedScript(t, bins, "fake.test", fakeListing...)
	pkg := planPackage{ImportPath: fakeSuite, Dir: fakeDir, Binary: "fake.test", BinarySHA256: bytesSHA256([]byte("the plan's compile")), Listed: fakeListing}
	code, _, stderr := runShardOfStderr(t, twoShardPlan(pkg), 1, "-bin", bins)
	if code != 2 || !strings.Contains(stderr, "shipped binary") {
		t.Errorf("a shipped binary of another digest: exit %d, stderr %q", code, stderr)
	}
	// The positive control: the same script with the digest the plan hashed.
	sha, _ := fileSHA256(bin)
	pkg.BinarySHA256 = sha
	code, _, stderr = runShardOfStderr(t, twoShardPlan(pkg), 1, "-bin", bins)
	if code == 2 || strings.Contains(stderr, "shipped binary") {
		t.Errorf("a shipped script of the plan's digest was refused: exit %d, stderr %q", code, stderr)
	}
}

// compiledFakePlan plans the fake suite for real (compile and list), and
// returns its package entry and the directory its binaries were compiled to.
func compiledFakePlan(t *testing.T) (planPackage, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("compiles a test binary")
	}
	quiet(t)
	out := t.TempDir()
	if code := cmdIntegration([]string{"-out", out, "-plan-only", "-jobs", "1", "-costs", "", "-allow", "", "-quarantine", "",
		"-nightly", "", "-host-speed", "", "./testdata/fakesuite"}); code != 0 {
		t.Fatalf("plan-only exit %d", code)
	}
	p, err := readPlan(filepath.Join(out, "integration-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Packages) != 1 {
		t.Fatalf("packages = %+v", p.Packages)
	}
	return p.Packages[0], filepath.Join(out, "bin", "test")
}

func ledgerTests(l ledger) []string {
	var names []string
	for _, e := range l.Tests {
		names = append(names, e.Test+"="+e.Outcome)
	}
	sort.Strings(names)
	return names
}

func TestIntegrationShard_RunsOnlyItsJobs(t *testing.T) {
	pkg, bins := compiledFakePlan(t)
	stubTree(t, "plan-tree")
	code, l := runShardOf(t, twoShardPlan(pkg), 1, "-bin", bins, "-namespace-prefix", "fake")
	if code != 0 {
		t.Fatalf("exit %d: %v", code, l.Problems)
	}
	if got := strings.Join(ledgerTests(l), " "); got != "TestNamespace=pass TestPass=pass" || l.Shard != 1 {
		t.Errorf("shard %d ran %s, want shard 1's TestNamespace and TestPass only", l.Shard, got)
	}
}

func TestIntegrationShard_RecordsACompiledBinarysDigest(t *testing.T) {
	pkg, _ := compiledFakePlan(t)
	stubTree(t, "plan-tree")
	quiet(t)
	dir := t.TempDir()
	planFile := filepath.Join(dir, "integration-plan.json")
	if err := writeJSON(planFile, twoShardPlan(pkg)); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out")
	if code := cmdIntegration([]string{"-plan", planFile, "-shard", "2", "-out", out, "-jobs", "1", "-allow", "", "-quarantine", ""}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	var l ledger
	if err := readJSON(filepath.Join(out, "integration-tests.json"), &l); err != nil {
		t.Fatal(err)
	}
	compiled, _ := fileSHA256(filepath.Join(out, "bin", "test", pkg.Binary))
	got := l.Binaries[fakeSuite]
	if got.SHA256 == "" || got.SHA256 != compiled || got.ListingSHA256 != pkg.ListingSHA256 {
		t.Errorf("binaries = %+v, want sha256 %s and the plan's listing %s", l.Binaries, compiled, pkg.ListingSHA256)
	}
}

func TestIntegrationShard_UsesItsOwnNamespacePrefix(t *testing.T) {
	pkg, bins := compiledFakePlan(t)
	stubTree(t, "plan-tree")
	// The fake suite's TestNamespace requires ORK_TEMPORAL_NAMESPACE to start
	// with "fake-w": the shard's prefix, then its slot.
	if code, l := runShardOf(t, twoShardPlan(pkg), 1, "-bin", bins, "-namespace-prefix", "fake"); code != 0 {
		t.Errorf("prefix fake: exit %d, %v", code, l.Problems)
	}
	code, l := runShardOf(t, twoShardPlan(pkg), 1, "-bin", bins, "-namespace-prefix", "another")
	if code != 1 || !strings.Contains(strings.Join(l.Problems, "\n"), "TestNamespace failed") {
		t.Errorf("prefix another: exit %d, %v", code, l.Problems)
	}
}

func TestIntegration_DeferWithoutANightlyBaseIsAnInvalidInvocation(t *testing.T) {
	quiet(t)
	if code := cmdIntegration([]string{"-out", t.TempDir(), "-plan-only", "-nightly-mode", "defer"}); code != 2 {
		t.Errorf("exit %d, want 2", code)
	}
}

func TestPlan_APullRequestThatDoesNotContainItsBaseFailsFast(t *testing.T) {
	quiet(t)
	out := t.TempDir()
	code := cmdIntegration([]string{"-out", out, "-plan-only", "-pr-number", "12", "-pr-head-sha", "h1", "-pr-base-sha", "b1", "-pr-contains-base", "false"})
	if code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if _, err := os.Stat(filepath.Join(out, "integration-plan.json")); !os.IsNotExist(err) {
		t.Error("a refused plan was written")
	}
}

// nightlyRow is a nightly-list row of the fake suite.
func nightlyRow(test, covering string) string {
	return fakeSuite + "\t" + test + "\t" + covering + "\t2026-10-18 Daniel Santos\tsoak\n"
}

// planWithLists plans the fake suite in mode with the given lists.
func planWithLists(t *testing.T, mode, tree, base string) *integrationPlan {
	t.Helper()
	if testing.Short() {
		t.Skip("compiles a test binary")
	}
	quiet(t)
	out := t.TempDir()
	treeFile := filepath.Join(out, "nightly.txt")
	baseFile := filepath.Join(out, "nightly-base.txt")
	for f, text := range map[string]string{treeFile: tree, baseFile: base} {
		if err := os.WriteFile(f, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"-out", out, "-plan-only", "-jobs", "1", "-costs", "", "-allow", "", "-quarantine", "", "-host-speed", "",
		"-nightly", treeFile}
	if mode != "" {
		args = append(args, "-nightly-mode", mode, "-nightly-base", baseFile)
	}
	if code := cmdIntegration(append(args, "./testdata/fakesuite")); code != 0 {
		t.Fatalf("plan-only exit %d", code)
	}
	p, err := readPlan(filepath.Join(out, "integration-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func planRuns(p *integrationPlan, test string) bool {
	for _, j := range p.Jobs {
		for _, n := range j.Tests {
			if n == test {
				return true
			}
		}
	}
	return false
}

func TestPlan_ANightlyEntryTheBaseLacksIsRunNotDeferred(t *testing.T) {
	p := planWithLists(t, nightlyDefer, nightlyRow("TestPass", "TestNamespace")+nightlyRow("TestCwd", "TestNamespace"),
		nightlyRow("TestPass", "TestNamespace"))
	if !planRuns(p, "TestCwd") || isDeferred(p.Deferred, testKey{fakeSuite, "TestCwd"}) {
		t.Errorf("an entry the change adds was deferred: deferred %+v", p.Deferred)
	}
	if planRuns(p, "TestPass") || !isDeferred(p.Deferred, testKey{fakeSuite, "TestPass"}) {
		t.Errorf("an entry both lists carry was run: deferred %+v", p.Deferred)
	}
}

func TestPlan_AnEntryOnlyTheBaseCarriesIsRun(t *testing.T) {
	p := planWithLists(t, nightlyDefer, "", nightlyRow("TestCwd", "TestNamespace"))
	if !planRuns(p, "TestCwd") || len(p.Deferred) != 0 {
		t.Errorf("an entry the change removes was not run: deferred %+v", p.Deferred)
	}
}

func TestPlan_TheBaseListIsReadForKeysOnly(t *testing.T) {
	// The base names a test this tree renamed; it is not a stale-entry
	// failure, because the base's tests are not this tree's.
	p := planWithLists(t, nightlyDefer, "", nightlyRow("TestRenamedAway", "TestGoneToo"))
	for _, problem := range p.Problems {
		if strings.Contains(problem, "TestRenamedAway") {
			t.Errorf("the base's entry was checked against this tree: %q", problem)
		}
	}
	if len(p.Deferred) != 0 {
		t.Errorf("deferred = %+v", p.Deferred)
	}
}

func TestIntegration_TheDefaultModeRunsTheNightlyTests(t *testing.T) {
	// No -nightly-mode: scripts/integration.sh and make test-integration run
	// every test, the nightly list's included.
	p := planWithLists(t, "", nightlyRow("TestPass", "TestNamespace"), "")
	if p.Nightly.Mode != nightlyInclude || len(p.Deferred) != 0 || !planRuns(p, "TestPass") {
		t.Errorf("mode %q deferred %+v: the default mode left a nightly test out", p.Nightly.Mode, p.Deferred)
	}
}

func TestPlanFile_RoundTripsAndPinsItsInputs(t *testing.T) {
	quiet(t)
	dir := t.TempDir()
	files := map[string]string{
		"costs.tsv":           fakeSuite + "\tTestPass\t1\n",
		"nightly.txt":         nightlyRow("TestPass", "TestNamespace"),
		"nightly-base.txt":    nightlyRow("TestPass", "TestNamespace"),
		"quarantine.txt":      "",
		"quarantine-base.txt": "# empty\n",
		"allow.txt":           "integration\t" + fakeSuite + "\tTestAllowedSkip\tan opt-in recorder\n",
		"host-speed.tsv":      "ui\te2e/a.spec.ts\tt\tlessThan 5000\t5\t16\tnote\n",
		"host-speed-base.txt": "",
	}
	for name, text := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	at := func(name string) string { return filepath.Join(dir, name) }
	o := integrationPlanOptions{
		out: dir, nightlyFile: at("nightly.txt"), nightlyBase: at("nightly-base.txt"), nightlyMode: nightlyDefer,
		costsFile: at("costs.tsv"), allowFile: at("allow.txt"), quarantineFile: at("quarantine.txt"), quarantineBase: at("quarantine-base.txt"),
		hostSpeedFile: at("host-speed.tsv"), hostSpeedBase: at("host-speed-base.txt"), tier: tierFull, baseCommit: "",
	}
	nightly, _ := readNightlyList(o.nightlyFile)
	base, _ := readNightlyList(o.nightlyBase)
	in := &integrationPlan{Format: planFormat, Tier: tierFull, ProofID: "pr-x", Packages: []planPackage{{ImportPath: fakeSuite, Dir: fakeDir, Listed: fakeListing}},
		Jobs: []job{{ID: "fakesuite-001", Package: fakeSuite, Tests: []string{"TestCwd"}, Shard: 1, TimeoutSeconds: 600}}}
	if code := writePlan(o, in, costs{}, nightly, base, nil); code != 0 {
		t.Fatalf("writePlan exit %d", code)
	}
	p, err := readPlan(filepath.Join(dir, "integration-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	sha := func(name string) string { s, _ := fileSHA256(at(name)); return s }
	for _, c := range []struct{ what, got, want string }{
		{"costs", p.Costs.SHA256, sha("costs.tsv")},
		{"nightly", p.Nightly.SHA256, sha("nightly.txt")},
		{"nightly base", p.Nightly.BaseSHA256, sha("nightly-base.txt")},
		{"quarantine", p.Quarantine.SHA256, sha("quarantine.txt")},
		{"quarantine base", p.Quarantine.BaseSHA256, sha("quarantine-base.txt")},
		{"allowlist", p.AllowedSkips.SHA256, sha("allow.txt")},
		{"host speed", p.HostSpeed.SHA256, sha("host-speed.tsv")},
		{"host speed base", p.HostSpeed.BaseSHA256, sha("host-speed-base.txt")},
	} {
		if c.got == "" || c.got != c.want {
			t.Errorf("%s sha256 = %q, want %q", c.what, c.got, c.want)
		}
	}
	if p.Tree == "" || p.Commit == "" || p.CheckedOn == "" || p.ProofID != "pr-x" {
		t.Errorf("identity: tree %q commit %q checked_on %q proof id %q", p.Tree, p.Commit, p.CheckedOn, p.ProofID)
	}
	if !isDeferred(p.Deferred, testKey{fakeSuite, "TestPass"}) || len(p.Expected) != 2 {
		t.Errorf("deferred %+v expected %+v", p.Deferred, p.Expected)
	}
	if p.Jobs[0].Timeout.Seconds() != 600 {
		t.Errorf("the job's timeout did not round-trip: %v", p.Jobs[0].Timeout)
	}
}
