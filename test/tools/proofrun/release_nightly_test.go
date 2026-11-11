package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// ---- the release's nightly check ---------------------------------------------------
//
// A release is not tagged while the nightly of its commit is red (AGENTS.md
// §11). release.yml checks it before it asks for the approval: the latest
// completed nightly.yml run on main whose head is the commit, judged by its
// proof.json as `make merge-check NIGHTLY=` judges one, and by the
// conclusions of the jobs its proof does not hold.

func nightlyJobs(over map[string]string) map[string]string {
	jobs := map[string]string{"nightly / proof": "failure", "vuln": "success", "vuln-js": "success", "vuln-images": "success",
		"release-notes": "success", "quickstart": "success", "flakes": "failure"}
	for k, v := range over {
		if v == "" {
			delete(jobs, k)
			continue
		}
		jobs[k] = v
	}
	return jobs
}

func TestVerify_AReleaseNeedsTheNightlyOfItsCommitToHavePassed(t *testing.T) {
	// A red proof job with a passing proof.json, and a red job the release
	// does not check, refuse nothing: the proof's verdict is its proof.json.
	if p := nightlyReleaseProblems(nightlyProof(t, nil), "main-tree", nightlyJobs(nil)); len(p) != 0 {
		t.Errorf("a passing nightly of the release commit was refused: %v", p)
	}
	for _, c := range []struct {
		name string
		dir  func(t *testing.T) string
		tree string
		jobs map[string]string
		want string
	}{
		{"no nightly proof", func(*testing.T) string { return "" }, "main-tree", nightlyJobs(nil), "no nightly proof was named"},
		{"a nightly of another commit", func(t *testing.T) string { return nightlyProof(t, nil) }, "release-tree", nightlyJobs(nil),
			"the nightly proof measured tree main-tree, and the release commit's tree is release-tree"},
		{"a failed nightly proof", func(t *testing.T) string {
			return nightlyProof(t, func(r *proofRecord) { r.Gates[0].Result = "fail" })
		}, "main-tree", nightlyJobs(nil), "the nightly proof did not pass"},
		{"an ineligible proof", func(t *testing.T) string {
			return nightlyProof(t, func(r *proofRecord) { r.Temporal.Mode = "shared" })
		}, "main-tree", nightlyJobs(nil), "not eligible evidence"},
		{"an unreadable proof", func(t *testing.T) string { return filepath.Join(t.TempDir(), "gone") }, "main-tree", nightlyJobs(nil), "is unreadable"},
		{"a vulnerability", func(t *testing.T) string { return nightlyProof(t, nil) }, "main-tree",
			nightlyJobs(map[string]string{"vuln": "failure"}), "the nightly run's vuln job concluded failure"},
		{"a JavaScript advisory", func(t *testing.T) string { return nightlyProof(t, nil) }, "main-tree",
			nightlyJobs(map[string]string{"vuln-js": "failure"}), "the nightly run's vuln-js job concluded failure"},
		{"an image vulnerability", func(t *testing.T) string { return nightlyProof(t, nil) }, "main-tree",
			nightlyJobs(map[string]string{"vuln-images": "failure"}), "the nightly run's vuln-images job concluded failure"},
		{"no image scan job", func(t *testing.T) string { return nightlyProof(t, nil) }, "main-tree",
			nightlyJobs(map[string]string{"vuln-images": ""}), "the nightly run has no vuln-images job"},
		{"release notes out of sync", func(t *testing.T) string { return nightlyProof(t, nil) }, "main-tree",
			nightlyJobs(map[string]string{"release-notes": "failure"}), "the nightly run's release-notes job concluded failure"},
		{"no release-notes job", func(t *testing.T) string { return nightlyProof(t, nil) }, "main-tree",
			nightlyJobs(map[string]string{"release-notes": ""}), "the nightly run has no release-notes job"},
		{"the Quickstart cancelled", func(t *testing.T) string { return nightlyProof(t, nil) }, "main-tree",
			nightlyJobs(map[string]string{"quickstart": "cancelled"}), "the nightly run's quickstart job concluded cancelled"},
		{"no Quickstart job", func(t *testing.T) string { return nightlyProof(t, nil) }, "main-tree",
			nightlyJobs(map[string]string{"quickstart": ""}), "the nightly run has no quickstart job"},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := strings.Join(nightlyReleaseProblems(c.dir(t), c.tree, c.jobs), "\n")
			if !strings.Contains(p, c.want) {
				t.Errorf("problems %q lack %q", p, c.want)
			}
		})
	}
}

// TestVerify_TheReleaseNightlyCheckReadsADownloadedNightly runs the command
// release.yml runs, on a merged CI nightly proof of a real commit.
func TestVerify_TheReleaseNightlyCheckReadsADownloadedNightly(t *testing.T) {
	dir, mainTip, mainTree, _ := ciCandidateRepo(t)
	nightly := ciMerged(t, tierNightly, mainTree, mainTip)
	jobs := filepath.Join(t.TempDir(), "jobs.tsv")
	writeJobs := func(lines string) {
		must.OK(t, os.WriteFile(jobs, []byte(lines), 0o644))
	}
	t.Chdir(dir)
	quiet(t)
	writeJobs("nightly / proof\tsuccess\nvuln\tsuccess\nvuln-js\tsuccess\nvuln-images\tsuccess\nrelease-notes\tsuccess\nquickstart\tsuccess\nflakes\tsuccess\n")
	if code := cmdVerify([]string{"-commit", mainTip, "-nightly", nightly, "-nightly-jobs", jobs}); code != 0 {
		t.Fatalf("a passing nightly of the commit was refused: exit %d", code)
	}
	if code := cmdVerify([]string{"-commit", "HEAD", "-nightly", nightly, "-nightly-jobs", jobs}); code != 1 {
		t.Errorf("the nightly of main's tip passed for another commit: exit %d", code)
	}
	writeJobs("vuln\tsuccess\nvuln-js\tsuccess\nvuln-images\tfailure\nquickstart\tsuccess\n")
	if code := cmdVerify([]string{"-commit", mainTip, "-nightly", nightly, "-nightly-jobs", jobs}); code != 1 {
		t.Errorf("a nightly whose image scan failed passed: exit %d", code)
	}
	writeJobs("vuln success\n")
	if code := cmdVerify([]string{"-commit", mainTip, "-nightly", nightly, "-nightly-jobs", jobs}); code != 1 {
		t.Errorf("an unreadable jobs list passed: exit %d", code)
	}
	for _, args := range [][]string{
		{"-commit", mainTip, "-nightly", nightly},
		{"-commit", mainTip, "-nightly-jobs", jobs},
		{"-commit", mainTip, "-nightly", nightly, "-nightly-jobs", jobs, "-proof", nightly},
		{"-commit", mainTip, "-nightly", nightly, "-nightly-jobs", jobs, "-release"},
		{"-commit", mainTip, "-nightly", nightly, "-nightly-jobs", jobs, "-tag", "v1.0.0"},
		{"-nightly", nightly, "-nightly-jobs", jobs},
	} {
		if code := cmdVerify(args); code != 2 {
			t.Errorf("verify %v: exit %d, want 2", args, code)
		}
	}
}
