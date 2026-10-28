package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ciCandidateRepo is a repository whose main carries a non-empty nightly
// list and whose HEAD is a merge candidate one commit above main's tip, as
// AGENTS.md §11.3(b) builds one. It returns main's tip, its tree and HEAD's
// tree.
func ciCandidateRepo(t *testing.T) (dir, mainTip, mainTree, headTree string) {
	t.Helper()
	dir = t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	write := func(name, text string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git("init", "-q", "-b", "main")
	write("scripts/proof-nightly-tests.txt", mainNightlyList)
	git("add", ".")
	git("commit", "-q", "-m", "main")
	mainTip, mainTree = git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
	git("checkout", "-q", "-b", "candidate")
	write("f", "change")
	git("add", ".")
	git("commit", "-q", "-m", "candidate")
	return dir, mainTip, mainTree, git("rev-parse", "HEAD^{tree}")
}

// ciMerged merges a CI-shaped proof — the parts proof.yml or nightly.yml
// uploads, with the run identity their merge job passes — of tree over base,
// and returns the directory a `gh run download` of its proof-<run>-a<n>
// artifact holds.
func ciMerged(t *testing.T, tier, tree, base string) string {
	t.Helper()
	f := newMergeFixture(t, tier, true)
	f.tree, f.base = tree, base
	f.plan.Tree, f.plan.BaseCommit, f.plan.Commit = tree, base, "c-"+tier
	// The plan job's start date, which the merged quarantine record carries.
	f.plan.CheckedOn = todayUTC().Format("2006-01-02")
	f.save()
	if tier == tierNightly {
		f.writePart(f.basePart("plan"), nil)
		build := f.basePart("build")
		build.Gates = []gateRecord{{Gate: "images", Result: "pass"}}
		build.Images = map[string]string{"activity-shell": "sha256:aaa"}
		f.writePart(build, nil)
	} else {
		f.writeCI()
	}
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	r, code := f.merge("-ci-run-id", "4242", "-ci-attempt", "1", "-ci-event", "push", "-ci-url", "https://github.com/o/r/actions/runs/4242")
	if code != 0 || r.Result != "pass" {
		t.Fatalf("the %s CI proof did not pass: exit %d result %s: %v", tier, code, r.Result, r.Ineligible)
	}
	if r.CI == nil || r.CI.RunID != "4242" || len(r.Parts) < 2 {
		t.Fatalf("the %s proof is not CI-shaped: ci %+v parts %v", tier, r.CI, r.Parts)
	}
	if r.Quarantine == nil || r.Quarantine.CheckedOn == "" {
		t.Fatalf("the %s proof records no quarantine check date: %+v", tier, r.Quarantine)
	}
	return f.out
}

// TestVerify_ADownloadedCIProofIsMergeEvidence proves the local merge path
// takes a CI proof as its evidence (AGENTS.md §11.3(b), docs/PROOFS.md §10):
// a merged CI full proof of HEAD's exact tree over main's tip passes
// `verify -proof`, a merged CI nightly proof of main's tip passes
// `verify -nightly`, and both stop passing once main moves.
func TestVerify_ADownloadedCIProofIsMergeEvidence(t *testing.T) {
	dir, mainTip, mainTree, headTree := ciCandidateRepo(t)
	full := ciMerged(t, tierFull, headTree, mainTip)
	nightly := ciMerged(t, tierNightly, mainTree, mainTip)
	t.Chdir(dir)
	quiet(t)
	if code := cmdVerify([]string{"-proof", full, "-nightly", nightly}); code != 0 {
		t.Fatalf("verify refused a CI full proof of HEAD's tree and a CI nightly proof of main's tip: exit %d", code)
	}
	// Without a nightly proof the merge is not refused, whatever main's
	// lists hold: the nightly of main is GitHub's, and merge-check verifies a
	// nightly proof only when it is given one (AGENTS.md §11).
	if code := cmdVerify([]string{"-proof", full}); code != 0 {
		t.Errorf("verify without NIGHTLY= exited %d, want 0: main's non-empty nightly list asks for no proof now", code)
	}
	// A CI full proof of another tree is no evidence for HEAD.
	other := ciMerged(t, tierFull, mainTree, mainTip)
	if code := cmdVerify([]string{"-proof", other, "-nightly", nightly}); code != 1 {
		t.Errorf("verify of another tree's CI proof exited %d, want 1", code)
	}
	// A merge that moves main invalidates both: the candidate's base and the
	// nightly proof's tree are no longer main's tip.
	moved := exec.Command("git", "-C", dir, "update-ref", "refs/heads/main", "HEAD")
	if out, err := moved.CombinedOutput(); err != nil {
		t.Fatalf("move main: %v: %s", err, out)
	}
	if p := strings.Join(verifyNightlyProblems(nightly, strings.TrimSpace(mustGit("rev-parse", "main"))), "\n"); !strings.Contains(p, "the nightly proof measured tree") {
		t.Errorf("a nightly proof of main's old tip was accepted after main moved: %q", p)
	}
	if code := cmdVerify([]string{"-proof", full, "-nightly", nightly}); code != 1 {
		t.Errorf("verify after main moved exited %d, want 1", code)
	}
}

// A nightly proof asked for by name must exist: `-nightly ""` is a proof
// directory that is not there, never "no proof asked for". merge-check passes
// the flag only when NIGHTLY is set, but a script that passes it from an unset
// variable must not make the check it asked for vanish.
func TestVerify_AnEmptyNightlyValueIsNotAnAbsentFlag(t *testing.T) {
	dir, mainTip, _, headTree := ciCandidateRepo(t)
	full := ciMerged(t, tierFull, headTree, mainTip)
	t.Chdir(dir)
	quiet(t)
	if code := cmdVerify([]string{"-proof", full}); code != 0 {
		t.Fatalf("verify with no -nightly flag exited %d, want 0", code)
	}
	if code := cmdVerify([]string{"-proof", full, "-nightly", ""}); code != 1 {
		t.Errorf("verify with an empty -nightly exited %d, want 1: the check it asked for was skipped", code)
	}
}
