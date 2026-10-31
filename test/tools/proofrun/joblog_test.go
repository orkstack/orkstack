package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The proof job's job records (attempt-N.json) keep a bounded log of every
// failed job: what the merger's classification and the rerun planner read
// from it — every `proofrun: start` and `proofrun: outcome` line, wherever it
// is — and the log's last lines for a reader, its terminal escape sequences
// removed. Before, the records kept no log at all: gh refused to print a log
// that carries escape sequences, and the step swallowed the refusal.

// bound runs boundJobLog over raw.
func bound(t *testing.T, raw string) (string, error) {
	t.Helper()
	var b bytes.Buffer
	err := boundJobLog(strings.NewReader(raw), &b)
	return b.String(), err
}

// ghLine is one line of a GitHub job log: a timestamp, then the text.
func ghLine(i int, text string) string {
	return fmt.Sprintf("2026-10-30T15:%02d:%02d.%07dZ %s\n", (i/60)%60, i%60, i, text)
}

func noise(from, n int) string {
	var b strings.Builder
	for i := from; i < from+n; i++ {
		b.WriteString(ghLine(i, fmt.Sprintf("\x1b[36mstep output %d\x1b[0m: pulling layer sha256:%064d", i, i)))
	}
	return b.String()
}

func TestBoundJobLog_KeepsEveryProofrunLineAndTheLastLines(t *testing.T) {
	early := ghLine(1, "proofrun: start example.com/m TestB") + ghLine(2, "proofrun: outcome fail example.com/m TestB 9.00")
	raw := early + noise(3, 5000) + ghLine(5003, "proofrun: start example.com/m TestC") + noise(5004, 300)
	got, err := bound(t, raw)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(got, "\n"), "\n")
	if want := 2 + 1 + 1 + 1 + jobLogTailLines; len(lines) != want {
		t.Fatalf("%d lines kept, want %d (three proofrun lines, two gaps, the tail)", len(lines), want)
	}
	for i, want := range []string{
		strings.TrimSuffix(ghLine(1, "proofrun: start example.com/m TestB"), "\n"),
		strings.TrimSuffix(ghLine(2, "proofrun: outcome fail example.com/m TestB 9.00"), "\n"),
		"[proofrun job-log: 5000 lines left out]",
		strings.TrimSuffix(ghLine(5003, "proofrun: start example.com/m TestC"), "\n"),
		"[proofrun job-log: 100 lines left out]",
	} {
		if lines[i] != want {
			t.Errorf("line %d = %q, want %q", i, lines[i], want)
		}
	}
	if last := lines[len(lines)-1]; last != strings.TrimSuffix(ghLine(5303, "step output 5303: pulling layer sha256:"+fmt.Sprintf("%064d", 5303)), "\n") {
		t.Errorf("the last line = %q, want the log's own last line without its escape sequences", last)
	}
	if strings.ContainsRune(got, '\x1b') {
		t.Error("the bounded log carries an escape sequence")
	}
	if len(got) > jobLogTailLines*(jobLogLineWidth+8)+4096 {
		t.Errorf("the bounded log is %d bytes", len(got))
	}
}

func TestBoundJobLog_CutsALongLineAndDropsTerminalControls(t *testing.T) {
	long := strings.Repeat("é", 1000)
	got, err := bound(t, "a\x1b]0;title\x07b\x1b[1;31mc\x1b[0m\rd\x00e\tf\n"+long+"\n")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(got, "\n"), "\n")
	if len(lines) != 2 || lines[0] != "abcde\tf" {
		t.Fatalf("lines = %q", lines)
	}
	if !strings.HasSuffix(lines[1], "…") || len(lines[1]) > jobLogLineWidth+len("…") || !strings.HasPrefix(long, strings.TrimSuffix(lines[1], "…")) {
		t.Errorf("a long line is kept as %d bytes %q…", len(lines[1]), lines[1][:20])
	}
	if got, err := bound(t, ""); err != nil || got != "" {
		t.Errorf("an empty log: %q, %v", got, err)
	}
	if got, err := bound(t, "no newline at the end"); err != nil || got != "no newline at the end\n" {
		t.Errorf("a last line without a newline: %q, %v", got, err)
	}
}

// Whatever a log holds within the bound, the merger and the planner read the
// same from its bounded form as from the whole: the tests that failed, the
// tests that started and never finished, the tests it passed, and whether no
// test started at all (the degraded marker's rule).
func TestBoundJobLog_ClassifiesAsTheWholeLogWould(t *testing.T) {
	rng := rand.New(rand.NewSource(41080705584))
	tests := []string{"TestA", "TestB", "TestC", "TestSoak", "TestIntegration_AFanOutAtAReducedItemCeilingMeetsItsThresholds"}
	for round := 0; round < 100; round++ {
		var raw strings.Builder
		n := rng.Intn(3 * jobLogTailLines)
		for i := 0; i < n; i++ {
			test := tests[rng.Intn(len(tests))]
			switch rng.Intn(20) {
			case 0:
				raw.WriteString(ghLine(i, "proofrun: start example.com/m "+test))
			case 1:
				raw.WriteString(ghLine(i, fmt.Sprintf("proofrun: outcome %s example.com/m %s %d.00", []string{"pass", "fail", "skip"}[rng.Intn(3)], test, i)))
			default:
				raw.WriteString(ghLine(i, fmt.Sprintf("noise %d", rng.Int())))
			}
		}
		got, err := bound(t, raw.String())
		if err != nil {
			t.Fatal(err)
		}
		wantFailed, wantUnfinished := logProgress(raw.String())
		gotFailed, gotUnfinished := logProgress(got)
		if !slices.Equal(wantFailed, gotFailed) || !slices.Equal(wantUnfinished, gotUnfinished) {
			t.Fatalf("round %d: logProgress reads failed %v unfinished %v from the whole and %v %v from the bounded log", round, wantFailed, wantUnfinished, gotFailed, gotUnfinished)
		}
		wantPassed, wantOpen := attemptProgress("", raw.String())
		gotPassed, gotOpen := attemptProgress("", got)
		if wantPassed != gotPassed || !slices.Equal(wantOpen, gotOpen) {
			t.Fatalf("round %d: attemptProgress reads %d %v from the whole and %d %v from the bounded log", round, wantPassed, wantOpen, gotPassed, gotOpen)
		}
		job := func(log string) *githubJob {
			return &githubJob{Conclusion: "failure", FailedStep: "integration shard", Log: log}
		}
		if degradedByMarker("integration:1", job(raw.String())) != degradedByMarker("integration:1", job(got)) {
			t.Fatalf("round %d: the degraded marker's rule reads the bounded log differently", round)
		}
	}
}

// A line the merger reads is decided and kept as the merger reads it, byte for
// byte: an escape byte right before `proofrun:`, or a control byte inside the
// line, must not hide it or change the names it carries. A line too long to
// read whole might hide one after its cut, so it is refused.
func TestBoundJobLog_KeepsALineTheMergerReadsByteForByte(t *testing.T) {
	for _, line := range []string{
		"\x1b[proofrun: outcome fail example.com/m TestX 1.00",
		"\x1b[ proofrun: outcome fail example.com/m TestX 1.00",
		"\x1bproofrun: outcome fail example.com/m TestX 1.00",
		"\x1b]0;proofrun: outcome fail example.com/m TestX 1.00\x07",
		"proofrun: outcome fail example.com/m Test\rX 1.00",
		"proofrun: start example.com/m Test\x0bX",
		`{"Action":"run","Package":"example.com/m","Test":"TestX"}`,
	} {
		raw := line + "\n" + noise(1, jobLogTailLines+50)
		got, err := bound(t, raw)
		if err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		wantFailed, wantUnfinished := logProgress(raw)
		gotFailed, gotUnfinished := logProgress(got)
		wantPassed, wantOpen := attemptProgress("", raw)
		gotPassed, gotOpen := attemptProgress("", got)
		if !slices.Equal(wantFailed, gotFailed) || !slices.Equal(wantUnfinished, gotUnfinished) || wantPassed != gotPassed || !slices.Equal(wantOpen, gotOpen) {
			t.Errorf("%q: the whole log reads failed %q unfinished %q open %q; the bounded one %q %q %q", line, wantFailed, wantUnfinished, wantOpen, gotFailed, gotUnfinished, gotOpen)
		}
	}
	long := strings.Repeat("x", jobLogProgressBytes+10) + " proofrun: outcome fail example.com/m TestX 1.00\n"
	if _, err := bound(t, long); err == nil {
		t.Error("a line longer than the bound was cut and kept")
	}
}

// FuzzBoundJobLog: a job log is GitHub's, input OrkStack does not control.
// Whatever it holds, boundJobLog either refuses it, or what the merger and
// the rerun planner read from its bounded form is what they read from the
// whole, and it keeps at most jobLogTailLines lines beside those they read.
func FuzzBoundJobLog(f *testing.F) {
	for _, seed := range []string{
		"",
		"no newline at the end",
		ghLine(1, "proofrun: start example.com/m TestB") + ghLine(2, "proofrun: outcome fail example.com/m TestB 9.00"),
		"a\x1b]0;title\x07b\x1b[1;31mc\x1b[0m\rd\x00e\tf\n",
		"\x1b[proofrun: outcome fail example.com/m TestX 1.00\n",
		"proofrun: start example.com/m Test\x0bX\r\n",
		`{"Action":"run","Package":"example.com/m","Test":"TestX"}` + "\n",
		"x\n\n\ny\n" + noise(1, 3),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		var b bytes.Buffer
		if err := boundJobLog(strings.NewReader(raw), &b); err != nil {
			return
		}
		got := b.String()
		wantFailed, wantUnfinished := logProgress(raw)
		gotFailed, gotUnfinished := logProgress(got)
		if !slices.Equal(wantFailed, gotFailed) || !slices.Equal(wantUnfinished, gotUnfinished) {
			t.Fatalf("logProgress reads failed %q unfinished %q from the whole, %q %q from the bounded log", wantFailed, wantUnfinished, gotFailed, gotUnfinished)
		}
		wantPassed, wantOpen := attemptProgress("", raw)
		gotPassed, gotOpen := attemptProgress("", got)
		if wantPassed != gotPassed || !slices.Equal(wantOpen, gotOpen) {
			t.Fatalf("attemptProgress reads %d %q from the whole, %d %q from the bounded log", wantPassed, wantOpen, gotPassed, gotOpen)
		}
		job := func(log string) *githubJob {
			return &githubJob{Conclusion: "failure", FailedStep: "integration shard", Log: log}
		}
		if degradedByMarker("integration:1", job(raw)) != degradedByMarker("integration:1", job(got)) {
			t.Fatal("the degraded marker's rule reads the bounded log differently")
		}
		other := 0
		for _, line := range strings.Split(strings.TrimSuffix(got, "\n"), "\n") {
			if got != "" && !mergerReads(line) && !strings.HasPrefix(line, "[proofrun job-log: ") {
				other++
			}
		}
		if other > jobLogTailLines {
			t.Fatalf("the bounded log keeps %d lines beside those the merger reads, over %d", other, jobLogTailLines)
		}
	})
}

// A log whose proofrun lines alone pass the bound cannot be kept whole, and
// half of them could read as a lost runner where the whole reads as a failing
// test: it is refused, and the step that keeps it records the job unreadable,
// so the merger fails it closed.
func TestBoundJobLog_RefusesProofrunLinesBeyondItsBound(t *testing.T) {
	line := "proofrun: start example.com/m " + strings.Repeat("T", 200)
	var raw strings.Builder
	for i := 0; raw.Len() <= jobLogProgressBytes; i++ {
		raw.WriteString(ghLine(i, line))
	}
	if got, err := bound(t, raw.String()); err == nil {
		t.Fatalf("a log of %d bytes of proofrun lines was kept as %d bytes", raw.Len(), len(got))
	}
	one := ghLine(0, "proofrun: start example.com/m "+strings.Repeat("T", jobLogProgressBytes))
	if _, err := bound(t, one); err == nil {
		t.Error("a single proofrun line beyond the bound was kept")
	}
}

// The recorded log of nightly 41080705584's degraded `integration (2)`: 81 KB,
// with 361 lines carrying escape sequences, and no test started. Bounded, the
// planner still reads it as a part that stopped before any gate.
func TestBoundJobLog_TheRecordedDegradedLogReadsAsDegraded(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "rerun-nightly-"+nightlyRun, "log-124151401756.txt"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := bound(t, string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "runner-health: fsync_p95_ms 99.43 ms BELOW the floor 30") || strings.ContainsRune(got, '\x1b') {
		t.Errorf("the bounded log lacks the measurement, or keeps an escape sequence:\n%s", got)
	}
	job := &githubJob{Conclusion: "failure", FailedStep: "integration shard", Log: got}
	if !degradedByMarker("integration:2", job) {
		t.Error("the bounded log no longer reads as a part that stopped before any gate")
	}
}

func TestCmdJobLog_BoundsStandardInputOntoStandardOutput(t *testing.T) {
	run := func(in string, args ...string) (string, int) {
		t.Helper()
		prevIn, prevOut := stdin, stdout
		var out bytes.Buffer
		stdin, stdout = strings.NewReader(in), &out
		defer func() { stdin, stdout = prevIn, prevOut }()
		code := cmdJobLog(args)
		return out.String(), code
	}
	quiet(t)
	if out, code := run("2026-10-30T00:00:00Z \x1b[1mhello\x1b[0m\n"); code != 0 || out != "2026-10-30T00:00:00Z hello\n" {
		t.Errorf("code %d output %q", code, out)
	}
	over := strings.Repeat(ghLine(0, "proofrun: start p "+strings.Repeat("T", 300)), jobLogProgressBytes/300+1)
	if out, code := run(over); code != 1 || out != "" {
		t.Errorf("beyond the bound: code %d, %d bytes written, want exit 1 and nothing", code, len(out))
	}
	if _, code := run("x\n", "extra"); code != 2 {
		t.Errorf("an argument: code %d, want 2", code)
	}
}

// The merger reads a failing test from a bounded log however far it is from
// the log's end. Shard 2's first attempt left no artifact, GitHub says it lost
// the runner, and the log shows the test failed long before the end: it is a
// result, so the rerun that passed makes the proof ineligible. A log cut to its
// last lines would have read as a lost runner, and passed.
func TestMerge_AFailingTestFarFromTheEndOfABoundedLogIsStillAResult(t *testing.T) {
	raw := ghLine(1, "proofrun: start example.com/m TestB") + ghLine(2, "proofrun: outcome fail example.com/m TestB 9.00") + noise(3, 20000)
	kept, err := bound(t, raw)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(raw, "\n"), "\n")
	cut := strings.Join(lines[len(lines)-jobLogTailLines:], "\n")
	if class, _ := classify(&githubJob{Conclusion: "failure", Annotations: []string{runnerLost}, Log: cut}, partReading{}); class != attemptLost {
		t.Fatalf("the control: a log cut to its tail reads as %v, want a lost runner", class)
	}
	r, code, _ := rerunShard2(t, &githubJob{Conclusion: "failure", Annotations: []string{runnerLost}, Log: kept})
	if code != 0 || r.Result != "pass" {
		t.Fatalf("code %d result %s: the second attempt passed", code, r.Result)
	}
	if r.MergeEligible || len(r.Reruns) != 0 {
		t.Fatalf("an attempt whose log records a failing test was rerun-eligible: %v %+v", r.Ineligible, r.Reruns)
	}
	if want := "after attempt 1 recorded its result (its log records a failing test: example.com/m.TestB)"; !strings.Contains(strings.Join(r.Ineligible, "\n"), want) {
		t.Errorf("ineligible = %v, want %q", r.Ineligible, want)
	}
}

// fakeGHScript answers the proof job's reads of a recorded run as gh 2.103
// does: --jq through jq; a log only with --allow-escape-sequences when it
// carries one; a check run or a log the recording lacks as gh's HTTP 404; and
// the log of the job FAKE_GH_LOG_ERROR names (<id>:<message>) as a failed read.
// With TOKEN_LOG set, it appends "gh token" to it when GH_TOKEN reached it.
const fakeGHScript = `#!/usr/bin/env bash
set -u
[ -z "${TOKEN_LOG:-}" ] || echo "gh ${GH_TOKEN:+token}" >>"$TOKEN_LOG"
[ "$1" = api ] || { echo "fake gh: only api" >&2; exit 2; }
shift
jqexpr="" allow=no path=""
while [ $# -gt 0 ]; do
	case "$1" in
	--paginate) ;;
	--allow-escape-sequences) allow=yes ;;
	--jq) jqexpr="$2"; shift ;;
	*) path="$1" ;;
	esac
	shift
done
path="${path%%\?*}"
case "$path" in
*/actions/runs/*/attempts/*/jobs) file="$FIXTURE/api-jobs-a${path##*/attempts/}"; file="${file%/jobs}.json" ;;
*/check-runs/*/annotations) id="${path%/annotations}"; file="$FIXTURE/annotations-${id##*/}.json"; [ -f "$file" ] || { echo '[]' | jq -r "${jqexpr:-.}"; exit; } ;;
*/actions/jobs/*/logs)
	id="${path%/logs}"; id="${id##*/}"; file="$FIXTURE/log-$id.txt"
	e="${FAKE_GH_LOG_ERROR:-}"; if [ -n "$e" ] && [ "${e%%:*}" = "$id" ]; then echo "${e#*:}" >&2; exit 1; fi
	;;
*) echo "fake gh: no answer for $path" >&2; exit 1 ;;
esac
[ -f "$file" ] || { echo "gh: Not Found (HTTP 404)" >&2; exit 1; }
if [ -n "$jqexpr" ]; then exec jq -r "$jqexpr" "$file"; fi
if [ "$allow" = no ] && grep -q $'\x1b' "$file"; then
	echo "the response contains terminal escape sequences; pass --allow-escape-sequences to output it anyway" >&2
	exit 1
fi
cat "$file"
`

// runJobRecords runs the proof job's "job records" step itself over the
// recorded run in fixture, with a gh that behaves as the runner's (fakeGHScript,
// told by env which log reads fail) and, when builds, this test binary
// standing in for proofrun; otherwise `go build` fails. With TOKEN_LOG in env,
// gh, go and the stand-in each append to it a line saying whether GH_TOKEN
// reached them. It returns the records of attempt 1 by job name.
func runJobRecords(t *testing.T, fixture string, builds bool, env ...string) map[string]githubJob {
	t.Helper()
	for _, tool := range []string{"bash", "jq"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip(tool + " is not installed")
		}
	}
	var script string
	for _, s := range loadWorkflow(t, "proof.yml").Jobs["proof"].Steps {
		if s.Name == "job records" {
			script = s.Run
		}
	}
	if script == "" {
		t.Fatal("jobs.proof has no job records step")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	bin, temp := t.TempDir(), t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write("gh", fakeGHScript)
	// `go build -o OUT ./test/tools/proofrun` makes OUT this test binary,
	// which runs the subcommand it is given (TestMain).
	// The stand-in's line is single-quoted into it, so it reads GH_TOKEN when
	// it runs, not when the fake go writes it.
	const tokenSeen = `[ -z "${TOKEN_LOG:-}" ] || echo "%s ${GH_TOKEN:+token}" >>"$TOKEN_LOG"`
	goScript := fmt.Sprintf(`#!/usr/bin/env bash
`+fmt.Sprintf(tokenSeen, "go")+`
[ "$1" = build ] && [ "$2" = -o ] && [ "$4" = ./test/tools/proofrun ] || { echo "fake go: $*" >&2; exit 2; }
printf '#!/bin/sh\n%%s\nPROOFRUN_AS_COMMAND=1 exec %%s "$@"\n' '%s' %q >"$3" && chmod +x "$3"
`, fmt.Sprintf(tokenSeen, "proofrun"), self)
	if !builds {
		goScript = "#!/bin/sh\n" + fmt.Sprintf(tokenSeen, "go") + "\necho 'fake go: build failed' >&2\nexit 1\n"
	}
	write("go", goScript)
	cmd := exec.Command("bash", "-c", script)
	cmd.Dir = mustRepoRoot()
	cmd.Env = append(append(os.Environ(),
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "FIXTURE="+fixture, "RUNNER_TEMP="+temp,
		"GITHUB_REPOSITORY=orkstack/orkstack", "GITHUB_RUN_ID="+nightlyRun, "GITHUB_RUN_ATTEMPT=1", "GH_TOKEN=x"), env...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the job records step failed: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(filepath.Join(temp, "ci", "jobs", "attempt-1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var a githubAttempt
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatalf("attempt-1.json does not decode: %v\n%.300s", err, raw)
	}
	if len(a.Jobs) != 16 {
		t.Errorf("attempt-1.json records %d jobs, want the run's 16", len(a.Jobs))
	}
	out := map[string]githubJob{}
	for _, j := range a.Jobs {
		out[j.Name] = j
	}
	return out
}

func recordedNightlyDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("testdata", "rerun-nightly-"+nightlyRun))
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

const degradedJobName = "nightly / integration (2)"

// TestWorkflows_TheJobRecordsKeepABoundedLogOfEachFailedJob: over the recorded
// nightly, the degraded job's record carries its bounded log, and the merger
// classifies the attempt from it as the planner does; the aggregator, whose
// log GitHub does not have (404), keeps no log and stays readable, as a lost
// runner's would.
func TestWorkflows_TheJobRecordsKeepABoundedLogOfEachFailedJob(t *testing.T) {
	for name, j := range runJobRecords(t, recordedNightlyDir(t), true) {
		switch name {
		case degradedJobName:
			if j.ReadError != "" || len(j.Log) > 64<<10 || !strings.Contains(j.Log, "runner-health: fsync_p95_ms 99.43 ms BELOW the floor 30") || strings.ContainsRune(j.Log, '\x1b') {
				t.Errorf("the degraded job's record: read_error %q, %d bytes of log:\n%s", j.ReadError, len(j.Log), j.Log)
			}
			if len(j.Annotations) != 2 || !degradedByMarker(partFromJobName(j.Name), &j) {
				t.Errorf("the degraded job's record does not read as a part that stopped before any gate: %+v", j.Annotations)
			}
		case "nightly / proof":
			if j.Log != "" || j.ReadError != "" {
				t.Errorf("the aggregator's record: log %q read_error %q", j.Log, j.ReadError)
			}
		default:
			if j.Log != "" {
				t.Errorf("%s (%s) kept a log; only a failed job's is read", j.Name, j.Conclusion)
			}
		}
	}
}

// The job records step builds proofrun from the candidate's tree and runs it
// on every failed job's log. Neither the build nor proofrun is given the job's
// token, which only gh's reads take.
func TestWorkflows_TheJobRecordsGiveTheTokenOnlyToGh(t *testing.T) {
	seen := filepath.Join(t.TempDir(), "token-seen")
	runJobRecords(t, recordedNightlyDir(t), true, "TOKEN_LOG="+seen)
	b, err := os.ReadFile(seen)
	if err != nil {
		t.Fatal(err)
	}
	ran := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		tool, token, _ := strings.Cut(line, " ")
		ran[tool]++
		if got, want := token == "token", tool == "gh"; got != want {
			t.Errorf("%s ran with the token: %v, want %v", tool, got, want)
		}
	}
	if ran["gh"] == 0 || ran["go"] != 1 || ran["proofrun"] == 0 {
		t.Errorf("gh, go and proofrun ran %v times; the step reads with gh, builds once and bounds each failed job's log", ran)
	}
}

// Every way the step cannot keep a log it read, or read a log GitHub has,
// fails the job's record closed: a read_error that names why, and no log, so
// the merger classifies the attempt as a result and never runs it again on its
// annotations alone. Only GitHub's own 404 or 410 (a lost runner keeps no log)
// leaves a record readable without a log.
func TestWorkflows_TheJobRecordsFailClosedWhenALogCannotBeKept(t *testing.T) {
	oversized := t.TempDir()
	entries, err := os.ReadDir(recordedNightlyDir(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(recordedNightlyDir(t), e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if e.Name() == "log-124151401756.txt" {
			b = []byte(strings.Repeat(ghLine(7, "proofrun: start example.com/m "+strings.Repeat("T", 200)), jobLogProgressBytes/200+1))
		}
		if err := os.WriteFile(filepath.Join(oversized, e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		name    string
		fixture string
		builds  bool
		env     []string
		want    string
	}{
		{"a log whose proofrun lines pass the bound", oversized, true, nil, "log: proofrun job-log refused it (its proofrun lines, or one line, pass the bound)"},
		{"proofrun that did not build", recordedNightlyDir(t), false, nil, "log: proofrun did not build, so the log could not be kept"},
		{"a server error", recordedNightlyDir(t), true, []string{"FAKE_GH_LOG_ERROR=124151401756:gh: Server Error (HTTP 502)"}, "log: the API call failed"},
		{"a body gh refused", recordedNightlyDir(t), true, []string{"FAKE_GH_LOG_ERROR=124151401756:the response contains terminal escape sequences"}, "log: the API call failed"},
	} {
		t.Run(c.name, func(t *testing.T) {
			jobs := runJobRecords(t, c.fixture, c.builds, c.env...)
			j := jobs[degradedJobName]
			if j.ReadError != c.want || j.Log != "" {
				t.Errorf("the degraded job's record: read_error %q, %d bytes of log; want read_error %q and no log", j.ReadError, len(j.Log), c.want)
			}
			if class, _ := classify(&j, partReading{}); class != attemptResult {
				t.Errorf("the record classifies as %v, want a result (it fails closed)", class)
			}
			if p := jobs["nightly / proof"]; p.ReadError != "" {
				t.Errorf("the aggregator, whose log GitHub does not have, has read_error %q", p.ReadError)
			}
		})
	}
}
