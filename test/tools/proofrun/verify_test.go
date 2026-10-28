package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// passingRelease is a release proof of commit c1 (tree t1) that every rule
// accepts, built through verdict() as the merger builds one.
func passingRelease(mutate func(*proofRecord), extraIneligible ...string) proofRecord {
	r := passingFull()
	r.Tier, r.Commit = tierRelease, "c1"
	if mutate != nil {
		mutate(&r)
	}
	r.Ineligible = append(r.Ineligible, extraIneligible...)
	verdict(&r, 0, true, map[string]bool{"integration": true})
	return r
}

func TestVerify_ReleaseNeedsTheExactCommit(t *testing.T) {
	good := passingRelease(nil)
	if p := releaseProofProblems(good, "c1", "t1"); len(p) != 0 {
		t.Fatalf("a good release proof was refused: %v", p)
	}
	// Another commit with the same tree: a release tags a commit, not a tree.
	if p := strings.Join(releaseProofProblems(good, "c2", "t1"), "\n"); !strings.Contains(p, "it proved commit c1, and the tag is c2") {
		t.Errorf("another commit with the same tree was accepted: %q", p)
	}
	if p := strings.Join(releaseProofProblems(good, "c1", "t2"), "\n"); !strings.Contains(p, "it proved tree t1, and the commit's tree is t2") {
		t.Errorf("another tree was accepted: %q", p)
	}
}

// The identity twins: each rule that makes a full proof ineligible refuses a
// release tag too, though the release tier's own "only a full proof is merge
// evidence" never does.
func TestVerify_ReleaseRefusesEveryIdentityProblem(t *testing.T) {
	cases := []struct {
		name  string
		r     proofRecord
		wants string
	}{
		{"a shared Temporal", passingRelease(func(r *proofRecord) { r.Temporal.Mode = "shared" }), "PROOF_TEMPORAL=shared"},
		{"an outside image tag", passingRelease(func(r *proofRecord) { r.ImageTagOwn = false }), "ORK_TEST_IMAGE_TAG was supplied from outside"},
		{"PROOF_GATES", passingRelease(func(r *proofRecord) { r.GatesOverride = "integration" }), "PROOF_GATES=integration chose the gates"},
		{"a part that ran another image digest", passingRelease(nil,
			"part integration:2 (attempt 1): activity-shell resolved to sha256:bbb, and the build laid out sha256:aaa"), "activity-shell resolved to sha256:bbb"},
		{"network churn", passingRelease(func(r *proofRecord) { n, c := 4, 3; r.GuardRetries, r.GuardCeiling = &n, &c }), "network-change retries above the ceiling"},
		{"a proof.json from before the field", func() proofRecord {
			r := passingRelease(nil)
			r.EligibleButForTier = false
			return r
		}(), "eligible_but_for_tier"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.r.Result != "pass" {
				t.Fatalf("the fixture did not pass: %v", c.r.Ineligible)
			}
			p := strings.Join(releaseProofProblems(c.r, "c1", "t1"), "\n")
			if !strings.Contains(p, "it is not eligible evidence") || !strings.Contains(p, c.wants) {
				t.Errorf("problems %q lack %q", p, c.wants)
			}
			if strings.Contains(p, "only a full proof is merge evidence") {
				t.Errorf("the release tier's own reason was held against it: %q", p)
			}
		})
	}
}

// The merged proof of a release whose parts ran different image digests
// cannot tag: the merger's identity rule reaches verify -release.
func TestVerify_ReleaseRefusesAMergedProofWithADigestMismatch(t *testing.T) {
	f := newMergeFixture(t, tierRelease, true)
	f.plan.PartsExpected = partsExpected(tierRelease, false, 2, 1, false)
	f.save()
	f.writeCI()
	for _, part := range append([]string{"observability"}, func() []string {
		var p []string
		for _, a := range fixtureActivities[1:] {
			p = append(p, "activity-fixture:"+a)
		}
		return p
	}()...) {
		rec := f.basePart(part)
		gate := "observability"
		if part != "observability" {
			gate = "activity-fixture"
		}
		rec.Gates = []gateRecord{{Gate: gate, Result: "pass"}}
		f.writePart(rec, nil)
	}
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	rec := f.basePart("integration:2")
	rec.Gates = []gateRecord{{Gate: "integration", Result: "pass"}}
	rec.Images = map[string]string{"activity-shell": "sha256:bbb"}
	f.writePart(rec, map[string]any{"integration-tests.json": ledger{Suite: "integration", Tests: []entry{passEntry("TestB")}, Binaries: mBinaries()}})
	r, code := f.merge()
	if code != 0 || r.Result != "pass" {
		t.Fatalf("code %d result %s: %v", code, r.Result, r.Ineligible)
	}
	p := strings.Join(releaseProofProblems(r, "commit1", "tree1"), "\n")
	if !strings.Contains(p, "activity-shell resolved to sha256:bbb") {
		t.Errorf("a release proof whose parts ran other images can tag: %q", p)
	}
}

const mainNightlyList = "example.com/m\tTestSoak\tTestA\t2026-10-18 Daniel Santos\tsoak\n"

// nightlyProof writes a nightly proof directory and returns it.
func nightlyProof(t *testing.T, mutate func(*proofRecord)) string {
	t.Helper()
	dir := t.TempDir()
	r := proofRecord{Tier: tierNightly, Tree: "main-tree", TreeEnd: "main-tree", CleanStart: true, CleanEnd: true, BaseRef: mainBranch}
	r.Temporal.Mode = "own"
	r.Gates = []gateRecord{{Gate: "integration", Result: "pass"}}
	if mutate != nil {
		mutate(&r)
	}
	verdict(&r, 0, false, map[string]bool{"integration": true})
	if err := writeJSON(filepath.Join(dir, "proof.json"), r); err != nil {
		t.Fatal(err)
	}
	return dir
}

// The interim rule that a merge needs a nightly proof of main's tip is
// retired (AGENTS.md §11): the nightly of main is GitHub's. merge-check asks
// for none, and verifies the one it is given.
func TestVerify_ANightlyProofIsVerifiedOnlyWhenGiven(t *testing.T) {
	if p := nightlyProofProblems("", "main-tree"); len(p) != 0 {
		t.Errorf("no nightly proof named, and one was asked for: %v", p)
	}
	if p := nightlyProofProblems(nightlyProof(t, nil), "main-tree"); len(p) != 0 {
		t.Errorf("a passing nightly proof of main's tip was refused: %v", p)
	}
}

func TestVerify_ANightlyProofOfAnotherTreeIsRefused(t *testing.T) {
	p := strings.Join(nightlyProofProblems(nightlyProof(t, nil), "other-tree"), "\n")
	if !strings.Contains(p, "the nightly proof measured tree main-tree, and main's tip tree is other-tree") {
		t.Errorf("problems = %q", p)
	}
}

func TestVerify_AFailedOrIneligibleNightlyProofIsRefused(t *testing.T) {
	cases := map[string]struct {
		mutate func(*proofRecord)
		want   string
	}{
		"failed":          {func(r *proofRecord) { r.Gates[0].Result = "fail" }, "the nightly proof did not pass"},
		"shared Temporal": {func(r *proofRecord) { r.Temporal.Mode = "shared" }, "PROOF_TEMPORAL=shared"},
		"PROOF_GATES":     {func(r *proofRecord) { r.GatesOverride = "integration" }, "PROOF_GATES=integration"},
		"a full proof":    {func(r *proofRecord) { r.Tier = tierFull }, "the nightly proof is tier full"},
		"a dirty tree":    {func(r *proofRecord) { r.CleanEnd = false }, "the nightly proof's tree was not clean"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			p := strings.Join(nightlyProofProblems(nightlyProof(t, c.mutate), "main-tree"), "\n")
			if !strings.Contains(p, c.want) {
				t.Errorf("problems %q lack %q", p, c.want)
			}
		})
	}
}

// A proof directory that cannot be read is a refusal, not an absent proof:
// naming a nightly proof asks merge-check to judge it.
func TestVerify_ANightlyProofThatCannotBeReadIsRefused(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-proof")
	if p := nightlyProofProblems(missing, "main-tree"); len(p) != 1 || !strings.Contains(p[0], "is unreadable") {
		t.Errorf("an absent proof.json: %v", p)
	}
}
