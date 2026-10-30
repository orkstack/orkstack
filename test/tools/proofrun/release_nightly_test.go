package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ---- the release's nightly check ---------------------------------------------------
//
// A release is not tagged while the nightly of its commit is red (AGENTS.md
// §11). release.yml checks it before it asks for the approval: the latest
// completed nightly.yml run on main whose head is the commit, judged by its
// proof.json as `make merge-check NIGHTLY=` judges one, and by the
// conclusions of the jobs its proof does not hold.

func nightlyJobs(over map[string]string) map[string]string {
	jobs := map[string]string{"nightly / proof": "failure", "vuln": "success", "quickstart": "success", "flakes": "failure"}
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
	// A red proof job with a passing proof.json, and a red flakes report,
	// refuse nothing: the proof's verdict is its proof.json.
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
		if err := os.WriteFile(jobs, []byte(lines), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(dir)
	quiet(t)
	writeJobs("nightly / proof\tsuccess\nvuln\tsuccess\nquickstart\tsuccess\nflakes\tsuccess\n")
	if code := cmdVerify([]string{"-commit", mainTip, "-nightly", nightly, "-nightly-jobs", jobs}); code != 0 {
		t.Fatalf("a passing nightly of the commit was refused: exit %d", code)
	}
	if code := cmdVerify([]string{"-commit", "HEAD", "-nightly", nightly, "-nightly-jobs", jobs}); code != 1 {
		t.Errorf("the nightly of main's tip passed for another commit: exit %d", code)
	}
	writeJobs("vuln\tfailure\nquickstart\tsuccess\n")
	if code := cmdVerify([]string{"-commit", mainTip, "-nightly", nightly, "-nightly-jobs", jobs}); code != 1 {
		t.Errorf("a nightly whose vulnerability scan failed passed: exit %d", code)
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

// The nightly job's fetch step, run as written in release.yml against a gh
// stub that answers through the real jq, with sleep stubbed: the wait for a
// nightly still running, its bound, and the refusals.
const nightlyGHStub = `#!/usr/bin/env bash
printf 'gh %s\n' "$*" >>"$STUB_LOG"
jqf=""; prev=""
for a in "$@"; do [ "$prev" = --jq ] && jqf="$a"; prev="$a"; done
case "$1 $2" in
"run list")
	n=$(($(cat "$STUB_STATE/polls" 2>/dev/null || echo 0) + 1)); echo "$n" >"$STUB_STATE/polls"
	if [ -n "$STUB_NO_RUN" ]; then echo '[]'
	elif [ "$n" -le "${STUB_RUNNING:-0}" ]; then echo '[{"databaseId":77,"status":"in_progress"}]'
	else echo '[{"databaseId":77,"status":"completed"}]'
	fi | jq -r "$jqf" ;;
"run view") echo '{"jobs":[{"name":"nightly / proof","conclusion":"success"},{"name":"vuln","conclusion":"success"},{"name":"quickstart","conclusion":"success"}]}' | jq -r "$jqf" ;;
"api repos/o/r/actions/runs/77/artifacts?per_page=100")
	if [ -n "$STUB_NO_PROOF" ]; then echo '{"artifacts":[{"name":"nightly-part","expired":false}]}'
	else echo '{"artifacts":[{"name":"proof-77-a1","expired":false},{"name":"proof-77-a2","expired":false},{"name":"proof-77-a3","expired":true}]}'
	fi | jq -r "$jqf" ;;
"run download") shift 2; while [ $# -gt 0 ]; do [ "$1" = -D ] && mkdir -p "$2" && echo '{}' >"$2/proof.json"; shift; done ;;
*) echo "gh stub: unexpected $*" >&2; exit 3 ;;
esac
`

func TestReleaseNightly_WaitsABoundedTimeForTheNightlyOfItsCommit(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Fatal("jq is not installed; the stub answers gh's --jq through it, as the runner's gh does")
	}
	w := loadWorkflow(t, "release.yml")
	job, ok := w.Jobs["nightly"]
	if !ok {
		t.Fatal("release.yml has no nightly job")
	}
	at := stepIndex(job.Steps, func(s wfStep) bool { return strings.Contains(s.Run, nightlyFetch) })
	if at < 0 {
		t.Fatal("release.yml's nightly job has no fetch step")
	}
	script := job.Steps[at].Run
	for _, c := range []struct {
		name   string
		env    []string
		code   int
		sleeps int
		says   string
	}{
		{"a nightly that passed already", nil, 0, 0, "actions/runs/77"},
		{"a nightly still running for three polls", []string{"STUB_RUNNING=3"}, 0, 3, "in_progress; polling again in a minute (3 of 60)"},
		{"a nightly still running after an hour", []string{"STUB_RUNNING=1000"}, 1, 60, "is still in_progress after 60 minutes: re-run this job once it completes"},
		{"no nightly of the commit", []string{"STUB_NO_RUN=1"}, 1, 0, "dispatch one (gh workflow run nightly.yml --ref main), and re-run this job once it passes"},
		{"a nightly that kept no proof", []string{"STUB_NO_PROOF=1"}, 1, 0, "kept no proof"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			bin, state, temp := filepath.Join(dir, "bin"), filepath.Join(dir, "state"), filepath.Join(dir, "temp")
			for _, d := range []string{bin, state, temp} {
				if err := os.MkdirAll(d, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			log := filepath.Join(dir, "calls.log")
			for name, body := range map[string]string{
				"gh":    nightlyGHStub,
				"sleep": "#!/usr/bin/env bash\nprintf 'sleep %s\\n' \"$*\" >>\"$STUB_LOG\"\n",
			} {
				if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("bash", "-e", "-c", script)
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "STUB_LOG="+log, "STUB_STATE="+state,
				"RUNNER_TEMP="+temp, "GITHUB_SERVER_URL=https://github.com", "REPO=o/r", "SHA=abc123", "GH_TOKEN=t")
			cmd.Env = append(cmd.Env, c.env...)
			out, err := cmd.CombinedOutput()
			code := 0
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				code = exit.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			if code != c.code || !strings.Contains(string(out), c.says) {
				t.Fatalf("exit %d, want %d, saying %q:\n%s", code, c.code, c.says, out)
			}
			calls := gateLogLines(t, log)
			sleeps := 0
			for _, l := range calls {
				if l == "sleep 60" {
					sleeps++
				}
			}
			if sleeps != c.sleeps {
				t.Errorf("slept %d times, want %d", sleeps, c.sleeps)
			}
			download := ""
			for _, l := range calls {
				if strings.HasPrefix(l, "gh run download") {
					download = l
				}
			}
			switch {
			case c.code == 0 && !strings.Contains(download, "-n proof-77-a2"):
				t.Errorf("downloaded %q, want the run's newest unexpired proof, proof-77-a2", download)
			case c.code != 0 && download != "":
				t.Errorf("a refused nightly was downloaded: %q", download)
			}
			if c.code == 0 {
				jobs, err := os.ReadFile(filepath.Join(temp, "nightly-jobs.tsv"))
				if err != nil || !strings.Contains(string(jobs), "vuln\tsuccess") {
					t.Errorf("nightly-jobs.tsv = %q (%v), want the run's jobs", jobs, err)
				}
			}
		})
	}
}
