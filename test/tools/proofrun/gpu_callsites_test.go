package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The tests in this file guard the places that enforce the GPU-host rule,
// which the rule's own paths: header does not name: cmdVerify's call of
// mergeGPUProblems (merge-check), merger.merge's call of mergeGPU (every
// merged proof), and the Makefile's GPU= pass-through to -gpu. Widening the
// header to all of proofrun and the Makefile would make every harness change
// need a GPU-host proof, which is the user's decision; instead a change that
// removes or weakens any of these call sites fails the merge gate here.

// gpuCandidateList is main's GPU-host list in the call-site tests: one row and
// a header naming one covered file.
const gpuCandidateList = "# paths: internal/orker/gpu_boot.go\n" +
	gpuPkg + "\tTestIntegrationGPU_A\tTestCover\t2026-11-18 Daniel Santos\tbecause\n"

// gpuCandidateRepo is a repository whose main carries gpuCandidateList and
// the covered file, and a candidate branch, one commit on main's tip, that
// changes file. It returns the repository, main's tip and the candidate's tree.
func gpuCandidateRepo(t *testing.T, file string) (dir, mainTip, headTree string) {
	t.Helper()
	r := &squashRepo{t: t, dir: t.TempDir()}
	r.git("init", "-q", "-b", "main")
	must.OK(t, os.MkdirAll(filepath.Join(r.dir, "scripts"), 0o755))
	must.OK(t, os.MkdirAll(filepath.Join(r.dir, "internal/orker"), 0o755))
	must.OK(t, os.WriteFile(filepath.Join(r.dir, gpuListFile), []byte(gpuCandidateList), 0o644))
	r.commit("internal/orker/gpu_boot.go", "x", "chore: the tip of main")
	mainTip = r.git("rev-parse", "main")
	r.git("checkout", "-q", "-b", "candidate")
	must.OK(t, os.MkdirAll(filepath.Dir(filepath.Join(r.dir, file)), 0o755))
	r.commit(file, "changed", "feat(orker): the candidate")
	return r.dir, mainTip, r.git("rev-parse", "HEAD^{tree}")
}

// TestVerify_GPURuleIsEnforcedByMergeCheck drives merge-check's entry point,
// cmdVerify: a candidate that changes a covered path is refused without
// -gpu, and accepted with a passing GPU-host-tier proof of its tree; one that
// changes no covered path needs none.
func TestVerify_GPURuleIsEnforcedByMergeCheck(t *testing.T) {
	dir, mainTip, headTree := gpuCandidateRepo(t, "internal/orker/gpu_boot.go")
	full := ciMerged(t, tierFull, headTree, mainTip)
	gpu := writeGPUProof(t, passingGPUProof(tierGPU, headTree))
	other := writeGPUProof(t, passingGPUProof(tierGPU, "another-tree"))
	// The merges run before either Chdir: a merge reads the list in its
	// working directory, and these proofs are of trees whose lists are not
	// the candidates'.
	untouched, tip2, tree2 := gpuCandidateRepo(t, "README.md")
	full2 := ciMerged(t, tierFull, tree2, tip2)
	t.Chdir(dir)
	quiet(t)
	if code := cmdVerify([]string{"-proof", full}); code != 1 {
		t.Fatalf("merge-check of a change to a covered path without -gpu exited %d, want 1", code)
	}
	if code := cmdVerify([]string{"-proof", full, "-gpu", other}); code != 1 {
		t.Fatalf("merge-check with a GPU-host proof of another tree exited %d, want 1", code)
	}
	if code := cmdVerify([]string{"-proof", full, "-gpu", gpu}); code != 0 {
		t.Fatalf("merge-check with a passing GPU-host-tier proof of the tree exited %d, want 0", code)
	}
	t.Chdir(untouched)
	if code := cmdVerify([]string{"-proof", full2}); code != 0 {
		t.Fatalf("merge-check of a change to no covered path exited %d, want 0", code)
	}
}

// TestMerge_WritesAndJudgesTheGPULedger drives a whole CI merge (cmdMerge,
// merger.merge): with the tree's list and the base's copy anchored, the
// proof carries a gpu ledger of host-deferred entries and passes; without the
// base's copy beside the plan, no entry can be host-deferred and it fails.
func TestMerge_WritesAndJudgesTheGPULedger(t *testing.T) {
	for _, tt := range []struct {
		name     string
		baseCopy bool
		wantPass bool
	}{
		{"anchored lists", true, true},
		{"no base copy beside the plan", false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			repo := t.TempDir()
			base := gitRepo(t, repo, map[string]string{gpuListFile: gpuCandidateList})
			f := newMergeFixture(t, tierFull, true)
			f.base, f.plan.BaseCommit = base, base
			sha := bytesSHA256([]byte(gpuCandidateList))
			f.plan.GPU = gpuListRef{File: gpuListFile, SHA256: sha, BaseSHA256: sha}
			if tt.baseCopy {
				must.OK(t, os.WriteFile(filepath.Join(f.dir, gpuBaseFileName), []byte(gpuCandidateList), 0o644))
			}
			f.save()
			f.writeCI()
			f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
			f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
			t.Chdir(repo)
			r, code := f.merge()
			var l ledger
			must.OK(t, readJSON(filepath.Join(f.out, suiteGPU+"-tests.json"), &l))
			if tt.wantPass {
				if code != 0 || r.Result != "pass" {
					t.Fatalf("merge exited %d, result %s: %v", code, r.Result, r.Ineligible)
				}
				if len(l.Tests) != 1 || l.Tests[0].Outcome != outcomeHostDeferred {
					t.Fatalf("gpu ledger tests = %+v, want TestIntegrationGPU_A host-deferred", l.Tests)
				}
				if ledgers, _ := r.Ledgers.(map[string]any); ledgers[suiteGPU] == nil {
					t.Fatalf("proof.json's ledgers carry no gpu ledger: %v", r.Ledgers)
				}
				return
			}
			if code == 0 || r.Result == "pass" {
				t.Fatalf("merge without the base's GPU list passed (exit %d, result %s)", code, r.Result)
			}
		})
	}
}

// TestMakefile_PassesGPUThrough runs the repository's Makefile: merge-check
// and release-tag hand GPU= to proofrun's -gpu, and release-tag refuses to
// run without GPU=.
func TestMakefile_PassesGPUThrough(t *testing.T) {
	root, err := repoRoot()
	must.OK(t, err)
	if _, err := exec.LookPath("make"); err != nil {
		t.Fatalf("make is not on PATH: %v", err)
	}
	makeN := func(args ...string) (string, error) {
		t.Helper()
		cmd := exec.Command("make", append([]string{"--no-print-directory", "-C", root}, args...)...)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	// -n prints the recipe without running it; -o leaves merge-check's
	// prerequisites (build, lint, test) unmade.
	out, err := makeN("-n", "-o", "build", "-o", "lint", "-o", "test", "merge-check", "PROOF=proof/p", "GPU=proof/g")
	must.OKf(t, err, "make -n merge-check: %s", out)
	if !strings.Contains(out, "verify -proof proof/p") || !strings.Contains(out, "-gpu proof/g") {
		t.Fatalf("merge-check does not hand GPU= to proofrun verify -gpu:\n%s", out)
	}
	out, err = makeN("-n", "release-tag", "VERSION=v9.9.9", "PROOF=proof/p", "GPU=proof/g")
	must.OKf(t, err, "make -n release-tag: %s", out)
	if !strings.Contains(out, "verify -release -tag v9.9.9 -proof proof/p -gpu proof/g") {
		t.Fatalf("release-tag does not hand GPU= to proofrun verify -gpu:\n%s", out)
	}
	// Run for real: the GPU= check is the recipe's third line, before
	// anything is verified or tagged.
	out, err = makeN("release-tag", "VERSION=v9.9.9", "PROOF=proof/p")
	if err == nil || !strings.Contains(out, "release-tag needs GPU=") {
		t.Fatalf("release-tag without GPU= = %v:\n%s", err, out)
	}
}
