package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The proof job's job records (attempt-N.json) keep a bounded log of every
// failed job: what the merger's classification reads from it — every `proofrun: start` and `proofrun: outcome` line, wherever it
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
	must.OK(t, err)
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
	must.OK(t, err)
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
		must.OK(t, err)
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
		wantClass, _ := classify(job(raw.String()), partReading{Part: "integration:1"})
		gotClass, _ := classify(job(got), partReading{Part: "integration:1"})
		if wantClass != gotClass {
			t.Fatalf("round %d: classify reads the whole log as %v and the bounded log as %v", round, wantClass, gotClass)
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
		must.OKf(t, err, "%q", line)
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
// Whatever it holds, boundJobLog either refuses it, or what the merger reads
// from its bounded form is what it reads from the whole, and it keeps at most
// jobLogTailLines lines beside those it reads.
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
		wantClass, _ := classify(job(raw), partReading{Part: "integration:1"})
		gotClass, _ := classify(job(got), partReading{Part: "integration:1"})
		if wantClass != gotClass {
			t.Fatalf("classify reads the whole log as %v and the bounded log as %v", wantClass, gotClass)
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

// nightlyRun is the recorded nightly whose job records and logs are kept
// under testdata/rerun-nightly-<run>.
const nightlyRun = "41080705584"

// The recorded log of nightly 41080705584's `integration (2)`: 81 KB, with 361
// lines carrying escape sequences, and no test started. Bounded, it keeps its
// last lines without an escape sequence, and the merger still reads it as an
// attempt whose proof step started and recorded no test.
func TestBoundJobLog_TheRecordedNightlyLogKeepsItsEndAndNamesNoTest(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "rerun-nightly-"+nightlyRun, "log-124151401756.txt"))
	must.OK(t, err)
	got, err := bound(t, string(raw))
	must.OK(t, err)
	if !strings.Contains(got, "fsync_p95_ms 99.43 ms") || strings.ContainsRune(got, '\x1b') {
		t.Errorf("the bounded log lost its last lines, or keeps an escape sequence:\n%s", got)
	}
	failed, unfinished := logProgress(got)
	if len(failed) != 0 || len(unfinished) != 0 {
		t.Errorf("the bounded log names tests: failed %v, unfinished %v", failed, unfinished)
	}
	job := &githubJob{Conclusion: "failure", FailedStep: "integration shard", Steps: []githubStep{{Name: "integration shard", Conclusion: "failure"}}, Log: got}
	if class, why := classify(job, partReading{Part: "integration:2"}); class != attemptResult {
		t.Errorf("classify reads the bounded log as %v (%s), want a result: its proof step started", class, why)
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
	must.OK(t, err)
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
	for _, frag := range []string{"attempt 1 recorded its result", "failing test", "example.com/m.TestB"} {
		if !strings.Contains(strings.Join(r.Ineligible, "\n"), frag) {
			t.Errorf("ineligible = %v, want it to say %q", r.Ineligible, frag)
		}
	}
}
