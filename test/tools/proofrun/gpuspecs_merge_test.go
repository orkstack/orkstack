package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

var gpuSpecTest = uiPlanTest{File: "orker-gpu.spec.ts", Title: "binds a unit", Project: "chromium"}

// gpuSpecMerge builds a full CI merge whose UI plan read the GPU spec list:
// the tree's list carries the row, the base commit and its copy beside the
// plan carry it when baseHasRow, and the plan host-defers the spec (or, with
// hostDeferIt unset, leaves its test planned with no deferral).
func gpuSpecMerge(t *testing.T, baseHasRow, hostDeferIt bool) *mergeFixture {
	t.Helper()
	list := gpuSpecHeader + gpuSpecRow + "\n"
	baseText := gpuSpecHeader
	if baseHasRow {
		baseText = list
	}
	repo := t.TempDir()
	base := gitRepo(t, repo, map[string]string{gpuSpecFile: baseText})
	must.OK(t, os.WriteFile(filepath.Join(repo, gpuSpecFile), []byte(list), 0o644))
	t.Chdir(repo)
	f := newMergeFixture(t, tierFull, false)
	f.base, f.plan.BaseCommit = base, base
	baseFile := f.writeFile(gpuSpecBaseFileName, baseText)
	sha, _ := fileSHA256(gpuSpecFile)
	baseSHA, _ := fileSHA256(baseFile)
	f.uiPlan.Tests = append(f.uiPlan.Tests, gpuSpecTest)
	f.uiPlan.GPUSpecs = &uiPlanGPUSpecs{File: gpuSpecFile, SHA256: sha, BaseFile: baseFile, BaseSHA256: baseSHA,
		Entries: []string{"ui/e2e/orker-gpu.spec.ts"}, Files: []string{"orker-gpu.spec.ts"}}
	if baseHasRow {
		f.uiPlan.GPUSpecs.BaseEntries = []string{"ui/e2e/orker-gpu.spec.ts"}
	}
	if hostDeferIt {
		f.uiPlan.HostDeferred = []uiDeferredSpec{{File: gpuSpecTest.File, Entry: gpuSpecRow}}
	}
	f.save()
	return f
}

func hostDeferredUILedger() uiLedger {
	l := uiLedgerFixture()
	l.Listed, l.HostDeferred = 2, 1
	l.Tests = append(l.Tests, uiEntryOf(gpuSpecTest, outcomeHostDeferred, gpuSpecRow))
	return l
}

// Without the GPU host, the merger records the spec's tests host-deferred in
// the Playwright ledger, anchored to both lists; proof.json says the spec
// gate did not run.
func TestMerge_AFullProofRecordsGPUSpecsHostDeferred(t *testing.T) {
	f := gpuSpecMerge(t, true, true)
	f.writeLocalUI(hostDeferredUILedger(), fullGateRecords())
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d, result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if r.GPUSpecsGateRan || r.GPUHost {
		t.Errorf("a proof off the GPU host records the spec gate as run: %+v", r)
	}
	var ui uiLedger
	must.OK(t, readJSON(filepath.Join(f.out, "ui-e2e-tests.json"), &ui))
	n := 0
	for _, e := range ui.Tests {
		if e.File == gpuSpecTest.File {
			n++
			if e.Outcome != outcomeHostDeferred || e.Detail != gpuSpecRow {
				t.Errorf("the GPU spec's entry = %+v", e)
			}
		}
	}
	if n != 1 || ui.HostDeferred != 1 || ui.Deferred != 0 || ui.Listed != 2 || len(ui.Problems) != 0 {
		t.Errorf("entries %d, host-deferred %d of %d, problems %v", n, ui.HostDeferred, ui.Listed, ui.Problems)
	}
}

func TestMerge_AGPUSpecHostDeferralIsCheckedAgainstBothLists(t *testing.T) {
	cases := []struct {
		name                  string
		baseHasRow, deferIt   bool
		ui                    uiLedger
		forgeBaseCopy, noPlan bool
		want                  string
	}{
		{name: "a row only the tree carries is not deferred: no outcome", baseHasRow: false, deferIt: false,
			ui: func() uiLedger {
				l := uiLedgerFixture()
				l.Listed = 2
				return l
			}(),
			want: "orker-gpu.spec.ts › binds a unit [chromium] has no outcome"},
		{name: "a row only the tree carries, deferred anyway", baseHasRow: false, deferIt: true, ui: hostDeferredUILedger(),
			want: "orker-gpu.spec.ts was host-deferred, but both GPU spec lists do not carry it"},
		{name: "a row both lists carry, not deferred", baseHasRow: true, deferIt: false, ui: uiLedgerFixture(),
			want: "is in both GPU spec lists and was not host-deferred"},
		{name: "a deferred spec that ran", baseHasRow: true, deferIt: true,
			ui: func() uiLedger {
				l := hostDeferredUILedger()
				l.Tests[1] = uiEntryOf(gpuSpecTest, outcomePass, "")
				return l
			}(),
			want: "which the UI plan defers to the nightly tier or the GPU host"},
		{name: "a base copy that is not the list at the base commit", baseHasRow: true, deferIt: true, ui: hostDeferredUILedger(), forgeBaseCopy: true,
			want: "is not " + gpuSpecFile + " at the base commit"},
		{name: "a plan that recorded no GPU spec list", baseHasRow: true, deferIt: true, ui: hostDeferredUILedger(), noPlan: true,
			want: "recorded no GPU spec list"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := gpuSpecMerge(t, c.baseHasRow, c.deferIt)
			if c.forgeBaseCopy {
				// The plan hashed the forged copy, as a swapped artifact would.
				forged := f.writeFile(gpuSpecBaseFileName, gpuSpecHeader+gpuSpecRow+"\nui/e2e/x.spec.ts\tui/e2e/a.spec.ts\t2026-11-19 Daniel Santos\tr\n")
				f.uiPlan.GPUSpecs.BaseSHA256, _ = fileSHA256(forged)
				f.save()
			}
			if c.noPlan {
				f.uiPlan.GPUSpecs = nil
				f.save()
			}
			f.writeLocalUI(c.ui, fullGateRecords())
			r, _ := f.merge()
			if r.MergeEligible || r.Result == "pass" {
				t.Errorf("the host-deferral was accepted: %v", r.Ineligible)
			}
			var ui uiLedger
			must.OK(t, readJSON(filepath.Join(f.out, "ui-e2e-tests.json"), &ui))
			if got := joined(r) + "\n" + strings.Join(ui.Problems, "\n"); !strings.Contains(got, c.want) {
				t.Errorf("neither the verdict nor the ledger names %q:\n%s", c.want, got)
			}
		})
	}
}

// A proof on the GPU host leaves the spec gate's ledger in its part: the
// merger judges it against the tree's list, and a plan that deferred the spec
// or a ledger that is missing fails it.
func TestMerge_AGPUHostProofJudgesTheSpecGate(t *testing.T) {
	list := gpuSpecHeader + gpuSpecRow + "\n"
	listSHA := bytesSHA256([]byte(list))
	pass := uiEntry{File: gpuSpecTest.File, Title: gpuSpecTest.Title, Project: gpuSpecTest.Project, Outcome: outcomePass, Seconds: 5}
	specLedger := func() uiLedger {
		return uiLedger{Suite: suiteGPUSpecs, Listed: 1, Pass: 1, Problems: []string{}, Tests: []uiEntry{pass}, ListSHA256: listSHA, GPUHost: true, GPUGateRan: true}
	}
	for _, c := range []struct {
		name   string
		ledger *uiLedger
		want   string // "" is a pass
	}{
		{name: "the gate's ledger passes", ledger: ptr(specLedger())},
		{name: "no ledger", ledger: nil, want: "left no gpu-specs ledger"},
		{name: "a failing spec", ledger: func() *uiLedger {
			l := specLedger()
			l.Tests[0].Outcome = outcomeFail
			l.Pass = 0
			l.Problems = []string{"orker-gpu.spec.ts › binds a unit [chromium] failed (boom)"}
			return &l
		}(), want: "failed (boom)"},
		{name: "another list's hash", ledger: func() *uiLedger { l := specLedger(); l.ListSHA256 = "other"; return &l }(), want: "read the GPU spec list"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := gpuSpecMerge(t, true, false)
			// The GPU host runs the spec: the plan defers none, and leaves the
			// spec's tests out of its planned list.
			f.uiPlan.GPUSpecs.Host = true
			f.uiPlan.GPUSpecs.BaseFile, f.uiPlan.GPUSpecs.BaseSHA256, f.uiPlan.GPUSpecs.BaseEntries = "", "", nil
			f.uiPlan.Tests = f.uiPlan.Tests[:1]
			f.save()
			f.writeLocalUI(uiLedgerFixture(), fullGateRecords())
			gateLedger := ledger{Suite: suiteGPU, Tests: []entry{}, Problems: []string{}, GPUHost: true, GPUGateRan: true,
				ListSHA256: emptySHA, BaseSHA256: emptySHA}
			must.OK(t, writeJSON(filepath.Join(filepath.Join(f.parts, "local-a1"), suiteGPU+"-tests.json"), gateLedger))
			if c.ledger != nil {
				must.OK(t, writeJSON(filepath.Join(filepath.Join(f.parts, "local-a1"), suiteGPUSpecs+"-tests.json"), *c.ledger))
			}
			f.plan.GPU = gpuListRef{File: gpuListFile, SHA256: emptySHA, BaseSHA256: emptySHA}
			f.save()
			must.OK(t, os.WriteFile(filepath.Join(f.dir, gpuBaseFileName), nil, 0o644))
			r, code := f.merge()
			if c.want == "" {
				if code != 0 || r.Result != "pass" || !r.GPUSpecsGateRan || !r.GPUHost {
					t.Fatalf("code %d, result %s, spec gate ran %v: %v", code, r.Result, r.GPUSpecsGateRan, r.Ineligible)
				}
				return
			}
			if r.Result == "pass" || !strings.Contains(joined(r), c.want) {
				t.Fatalf("result %s, want a failure naming %q:\n%s", r.Result, c.want, joined(r))
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

// proof.sh writes gpu-specs-tests.json next to gates.tsv; `proofrun record`
// must carry it into the part, or the merger never finds it. Nothing here
// places the file by hand: record, then the GPU-tier merge.
func TestRecord_CarriesTheGPUSpecsLedgerIntoThePartTheMergerReads(t *testing.T) {
	root := t.TempDir()
	base := gitRepo(t, root, nil)
	t.Chdir(root)
	dir := filepath.Join(root, "proof")
	must.OK(t, os.MkdirAll(dir, 0o755))
	emptySHA, _ := fileSHA256("")
	must.OK(t, writeJSON(filepath.Join(dir, suiteGPU+"-tests.json"), ledger{Suite: suiteGPU, Tests: []entry{}, Problems: []string{}, GPUHost: true, GPUGateRan: true, ListSHA256: emptySHA, BaseSHA256: emptySHA}))
	must.OK(t, writeJSON(filepath.Join(dir, suiteGPUSpecs+"-tests.json"), uiLedger{Suite: suiteGPUSpecs, Tests: []uiEntry{}, Problems: []string{}, ListSHA256: emptySHA, GPUHost: true, GPUGateRan: true}))
	must.OK(t, os.WriteFile(filepath.Join(dir, "gates.tsv"), []byte("gpu\tpass\t1\tgpu.log\tgpu_gate\n"), 0o644))
	must.OK(t, os.WriteFile(filepath.Join(dir, gpuBaseFileName), nil, 0o644))
	parts := filepath.Join(dir, "parts")
	local := filepath.Join(parts, "local-a1")
	if code := cmdRecord([]string{"-dir", dir, "-tier", tierGPU, "-attempt", "1", "-tree-start", "aaaa", "-tree-end", "aaaa",
		"-clean-start", "-clean-end", "-base-ref", mainBranch, "-base-commit", base, "-out-dir", local}); code != 0 {
		t.Fatalf("record exited %d", code)
	}
	if _, err := os.Stat(filepath.Join(local, suiteGPUSpecs+"-tests.json")); err != nil {
		t.Fatalf("record left the gpu-specs ledger out of the part: %v", err)
	}
	out := filepath.Join(root, "out")
	must.OK(t, os.MkdirAll(out, 0o755))
	must.OK(t, os.WriteFile(filepath.Join(out, gpuBaseFileName), nil, 0o644))
	quiet(t)
	code := mergeGPUTierProof(parts, out, "g")
	var r proofRecord
	must.OK(t, readJSON(filepath.Join(out, "proof.json"), &r))
	if code != 0 || r.Result != "pass" || !r.GPUSpecsGateRan {
		t.Fatalf("code %d, result %s, spec gate ran %v: %v", code, r.Result, r.GPUSpecsGateRan, r.Ineligible)
	}
}
