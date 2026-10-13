package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// inspectFrame is what the cluster answers for one poll: the run snapshot
// and the live attempts of that moment.
type inspectFrame struct {
	run  *orkstackv1.Run
	live engine.LiveRun
}

// liveReader is the cluster half of the read model for these tests: a run
// that moves through frames, and the live attempts of each. The frame
// advances once per LiveAttempts call, so a watch sees frame 0 on its first
// poll and the next frame on the one after — the snapshot a poll's GetRun
// returns always belongs with the attempts its InspectRun returns.
type liveReader struct {
	mu     sync.Mutex
	frames []inspectFrame
	at     int
}

func (l *liveReader) frame() inspectFrame {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.at >= len(l.frames) {
		return l.frames[len(l.frames)-1]
	}
	return l.frames[l.at]
}

func (l *liveReader) GetRun(_ context.Context, runID string) (*orkstackv1.Run, error) {
	run := l.frame().run
	if run.GetId() != runID {
		return nil, engine.ErrRunNotFound
	}
	return run, nil
}

func (l *liveReader) ListRuns(context.Context, engine.RunQuery) ([]*orkstackv1.Run, error) {
	return nil, nil
}

func (l *liveReader) StreamEvents(context.Context, string, int64, func(*orkstackv1.Event) error) error {
	return engine.ErrRunNotFound
}

func (l *liveReader) Executions(context.Context, engine.StatsWindow) (engine.Executions, error) {
	return engine.Executions{}, nil
}

func (l *liveReader) RunningCount(context.Context, string, int64) (int64, error) { return 0, nil }

func (l *liveReader) RunStart(ctx context.Context, runID string) (*engine.RunStartRecord, error) {
	run, err := l.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	return startOf(run), nil
}

func (l *liveReader) LiveAttempts(context.Context, string, time.Time) (engine.LiveRun, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	frame := l.frames[min(l.at, len(l.frames)-1)]
	l.at++
	return frame.live, nil
}

// inspectAt is the engine clock every golden is rendered against: the worked
// example's moment.
var inspectAt = time.Date(2026, 9, 23, 9, 51, 7, 0, time.UTC)

func newInspectFixture(t *testing.T, now time.Time, frames ...inspectFrame) *fixture {
	t.Helper()
	reader := &liveReader{frames: frames}
	return newFixtureWith(t, func(cfg *api.Config) {
		cfg.Reader = reader
		cfg.Now = func() time.Time { return now }
	})
}

func liveAt(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(inspectAt.Add(d)) }

// trainRun is the worked example's run: fit running, eval pending, clean skipped.
func trainRun(status orkstackv1.Status) *orkstackv1.Run {
	return &orkstackv1.Run{
		Id:          "ork-train-1",
		OrkflowName: "train-model",
		Status:      status,
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "fit", Status: orkstackv1.Status_STATUS_RUNNING, Pool: "gpu"},
			{TaskId: "eval", Status: orkstackv1.Status_STATUS_PENDING, Pool: "gpu"},
			{TaskId: "clean", Status: orkstackv1.Status_STATUS_SKIPPED},
		},
	}
}

const (
	stStarted   = orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_STARTED
	stScheduled = orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_SCHEDULED
	stCancel    = orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_CANCEL_REQUESTED
)

// fitStarted is L1 as the worked example renders it: attempt 2, 40m2s in, last
// beat 3 s ago, 412.0 MiB logged.
func fitStarted() *orkstackv1.LiveAttempt {
	return &orkstackv1.LiveAttempt{
		TaskId: "fit", Attempt: 2, State: stStarted,
		AttemptScheduledAt:             liveAt(-40*time.Minute - 5*time.Second),
		AttemptStartedAt:               liveAt(-40*time.Minute - 2*time.Second),
		TimeoutSeconds:                 2700,
		CancellationGracePeriodSeconds: 30,
		ExpiresAt:                      timestamppb.New(time.Date(2026, 9, 23, 10, 31, 7, 0, time.UTC)),
		HeartbeatAvailable:             true,
		LastHeartbeatAt:                liveAt(-3 * time.Second),
		HeartbeatDeadlineAt:            liveAt(27 * time.Second),
		Phase:                          "run",
		ContainerId:                    "3f2a9c1d0b4e7f00",
		BytesLogged:                    432013312,
		OrkerId:                        "orker-7",
	}
}

func inspectOne(t *testing.T, golden string, run *orkstackv1.Run, attempts ...*orkstackv1.LiveAttempt) result {
	t.Helper()
	f := newInspectFixture(t, inspectAt, inspectFrame{run: run, live: engine.LiveRun{Attempts: attempts}})
	res := f.run("run", "inspect", run.GetId())
	if res.code != 0 {
		t.Fatalf("run inspect exited %d: %s", res.code, res.stderr)
	}
	if golden != "" {
		requireGolden(t, golden, res.stdout)
	}
	return res
}

func TestRunInspect_GoldenTable(t *testing.T) {
	res := inspectOne(t, "run-inspect.golden", trainRun(orkstackv1.Status_STATUS_RUNNING), fitStarted())
	for _, want := range []string{"412.0 MiB", "40m2s", "3s ago", "3f2a9c1d0b4e ", "at least 4m58s left"} {
		if !strings.Contains(res.stdout, want) {
			t.Fatalf("output lacks %q:\n%s", want, res.stdout)
		}
	}
}

func TestRunInspect_GoldenJSON(t *testing.T) {
	run := trainRun(orkstackv1.Status_STATUS_RUNNING)
	f := newInspectFixture(t, inspectAt, inspectFrame{run: run, live: engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{fitStarted()}}})
	res := f.run("run", "inspect", run.GetId(), "--json")
	if res.code != 0 {
		t.Fatalf("run inspect --json exited %d: %s", res.code, res.stderr)
	}
	requireGolden(t, "run-inspect.json.golden", res.stdout)
	if !strings.Contains(res.stdout, `"state": "STARTED"`) || strings.Contains(res.stdout, "heartbeatUnavailableReason") {
		t.Fatalf("JSON is not in the short-enum dialect, or carries a zero reason:\n%s", res.stdout)
	}
	for _, path := range f.requests() {
		if strings.HasSuffix(path, "/GetRun") {
			t.Fatalf("--json made a GetRun call; requests = %v", f.requests())
		}
	}
}

// One golden per state, reason and the stale sentence.
func TestRunInspect_EveryUnavailableReasonAndStateHasAGolden(t *testing.T) {
	running := trainRun(orkstackv1.Status_STATUS_RUNNING)

	t.Run("waiting", func(t *testing.T) {
		run := trainRun(orkstackv1.Status_STATUS_RUNNING)
		run.TaskRuns[0].Status = orkstackv1.Status_STATUS_SUCCEEDED
		run.TaskRuns[1].Status = orkstackv1.Status_STATUS_RUNNING
		res := inspectOne(t, "run-inspect-waiting.golden", run, &orkstackv1.LiveAttempt{
			TaskId: "eval", Attempt: 1, State: stScheduled,
			AttemptScheduledAt: liveAt(-2*time.Minute - 10*time.Second), TimeoutSeconds: 600, CancellationGracePeriodSeconds: 30,
			HeartbeatUnavailableReason: orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_NOT_STARTED,
		})
		if !strings.Contains(res.stdout, "eval: waiting for an orker on pool gpu for 2m10s") {
			t.Fatalf("output:\n%s", res.stdout)
		}
	})
	t.Run("retry", func(t *testing.T) {
		res := inspectOne(t, "run-inspect-retry.golden", running, &orkstackv1.LiveAttempt{
			TaskId: "fit", Attempt: 3, State: stScheduled,
			AttemptScheduledAt: liveAt(42 * time.Second), NextAttemptAt: liveAt(42 * time.Second),
			TimeoutSeconds: 2700, CancellationGracePeriodSeconds: 30,
			HeartbeatUnavailableReason: orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_NOT_STARTED,
		})
		if !strings.Contains(res.stdout, "fit: attempt 2 ended; attempt 3 is scheduled for 2026-09-23T09:51:49Z (in 42s)") ||
			!strings.Contains(res.stdout, "run attempts ork-train-1 --task fit") {
			t.Fatalf("output:\n%s", res.stdout)
		}
	})
	t.Run("stale", func(t *testing.T) {
		stale := fitStarted()
		stale.LastHeartbeatAt = liveAt(-24 * time.Second)
		stale.HeartbeatDeadlineAt = liveAt(6 * time.Second)
		stale.HeartbeatStale = true
		res := inspectOne(t, "run-inspect-stale.golden", running, stale)
		if !strings.Contains(res.stdout, "24s ago STALE") ||
			!strings.Contains(res.stdout, "Temporal times this attempt out at 2026-09-23T09:51:13Z unless a beat arrives") {
			t.Fatalf("output:\n%s", res.stdout)
		}
	})
	t.Run("nobeat", func(t *testing.T) {
		res := inspectOne(t, "run-inspect-nobeat.golden", running, &orkstackv1.LiveAttempt{
			TaskId: "fit", Attempt: 1, State: stStarted,
			AttemptScheduledAt: liveAt(-3 * time.Second), AttemptStartedAt: liveAt(-time.Second),
			TimeoutSeconds: 2700, CancellationGracePeriodSeconds: 30,
			HeartbeatUnavailableReason: orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_NO_HEARTBEAT_YET,
		})
		if !strings.Contains(res.stdout, "unavailable") || !strings.Contains(res.stdout, "fit: no heartbeat yet") {
			t.Fatalf("output:\n%s", res.stdout)
		}
	})
	t.Run("undecodable", func(t *testing.T) {
		res := inspectOne(t, "run-inspect-undecodable.golden", running, &orkstackv1.LiveAttempt{
			TaskId: "fit", Attempt: 1, State: stStarted,
			AttemptScheduledAt: liveAt(-10 * time.Second), AttemptStartedAt: liveAt(-6 * time.Second),
			TimeoutSeconds: 2700, CancellationGracePeriodSeconds: 30,
			HeartbeatUnavailableReason: orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_UNDECODABLE,
		})
		if !strings.Contains(res.stdout, "fit: heartbeat detail undecodable") {
			t.Fatalf("output:\n%s", res.stdout)
		}
	})
	t.Run("cancelling", func(t *testing.T) {
		cancelling := fitStarted()
		cancelling.State = stCancel
		cancelling.Phase = "cancel"
		res := inspectOne(t, "run-inspect-cancelling.golden", running, cancelling)
		if !strings.Contains(res.stdout, "fit: cancellation requested · phase cancel · grace 30s") {
			t.Fatalf("output:\n%s", res.stdout)
		}
	})
	t.Run("terminal", func(t *testing.T) {
		run := trainRun(orkstackv1.Status_STATUS_SUCCEEDED)
		for _, task := range run.TaskRuns {
			task.Status = orkstackv1.Status_STATUS_SUCCEEDED
		}
		res := inspectOne(t, "run-inspect-terminal.golden", run)
		if !strings.Contains(res.stdout, "run SUCCEEDED; nothing is live") {
			t.Fatalf("output:\n%s", res.stdout)
		}
	})
	t.Run("idle", func(t *testing.T) {
		run := trainRun(orkstackv1.Status_STATUS_RUNNING)
		run.TaskRuns[0].Status = orkstackv1.Status_STATUS_SUCCEEDED
		res := inspectOne(t, "run-inspect-idle.golden", run)
		if !strings.Contains(res.stdout, "nothing is in flight: the run is between tasks") {
			t.Fatalf("output:\n%s", res.stdout)
		}
	})
}

// TestRunInspect_EveryReasonValueIsPrintedByAGolden holds that no reason, and
// not the stale sentence, can exist without a golden that shows it.
func TestRunInspect_EveryReasonValueIsPrintedByAGolden(t *testing.T) {
	var union strings.Builder
	matches, err := filepath.Glob(filepath.Join("testdata", "run-inspect*.golden"))
	if err != nil || len(matches) < 9 {
		t.Fatalf("goldens = %v (%v), want run-inspect.golden and its eight siblings", matches, err)
	}
	for _, path := range matches {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		union.Write(body)
	}
	for number, name := range orkstackv1.HeartbeatUnavailableReason_name {
		reason := orkstackv1.HeartbeatUnavailableReason(number)
		if reason == orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_UNSPECIFIED {
			continue
		}
		if sentence := unavailableReasonSentence(reason); !strings.Contains(union.String(), ": "+sentence+"\n") {
			t.Errorf("%s (%q) is printed by no golden", name, sentence)
		}
	}
	if !strings.Contains(union.String(), "— the orker is gone or partitioned; Temporal times this attempt out at") {
		t.Error("the stale sentence is printed by no golden")
	}
}

func TestRunInspect_OldOrkerHeartbeatPrintsDashForOrker(t *testing.T) {
	old := &orkstackv1.LiveAttempt{
		TaskId: "fit", Attempt: 1, State: stStarted,
		AttemptScheduledAt: liveAt(-10 * time.Second), AttemptStartedAt: liveAt(-6 * time.Second),
		TimeoutSeconds: 2700, CancellationGracePeriodSeconds: 30, ExpiresAt: liveAt(time.Hour),
		HeartbeatAvailable: true, LastHeartbeatAt: liveAt(-time.Second), HeartbeatDeadlineAt: liveAt(29 * time.Second),
		Phase: "image",
	}
	res := inspectOne(t, "", trainRun(orkstackv1.Status_STATUS_RUNNING), old)
	lines := strings.Split(res.stdout, "\n")
	fields := strings.Fields(lines[1])
	// TASK STATUS ATTEMPT STATE PHASE ORKER CONTAINER LOGGED(2) RUNNING HEARTBEAT(2)
	if fields[0] != "fit" || fields[4] != "image" || fields[5] != "-" || fields[6] != "-" || fields[7] != "0" {
		t.Fatalf("fit row = %q, want PHASE image, ORKER -, CONTAINER -, LOGGED 0 B", lines[1])
	}
	if strings.Contains(res.stdout, "unavailable") {
		t.Fatalf("an old orker's beat printed unavailable:\n%s", res.stdout)
	}
}

func TestRunInspect_FormatBytes(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		want string
	}{
		{0, "0 B"}, {1023, "1023 B"}, {1024, "1.0 KiB"}, {432013312, "412.0 MiB"}, {5368709120, "5.0 GiB"},
	} {
		if got := formatBytes(tc.n); got != tc.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestRunInspect_WatchEndsAtTerminalWithTheRunExitCode(t *testing.T) {
	running := trainRun(orkstackv1.Status_STATUS_RUNNING)
	failed := trainRun(orkstackv1.Status_STATUS_FAILED)
	failed.TaskRuns[0].Status = orkstackv1.Status_STATUS_FAILED
	frames := func() []inspectFrame {
		return []inspectFrame{
			{run: running, live: engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{fitStarted()}}},
			{run: failed},
		}
	}

	f := newInspectFixture(t, inspectAt, frames()...)
	res := f.run("run", "inspect", running.GetId(), "--watch", "--interval", "1s")
	if res.code != exitRunFailed {
		t.Fatalf("exit = %d, want %d (failed); stderr %s", res.code, exitRunFailed, res.stderr)
	}
	if got := strings.Count(res.stdout, "--- 2026-09-23T09:51:07Z ---"); got != 2 {
		t.Fatalf("headed blocks = %d, want 2:\n%s", got, res.stdout)
	}
	if !strings.Contains(res.stdout, "run FAILED; nothing is live") {
		t.Fatalf("the last block does not end the watch:\n%s", res.stdout)
	}

	jf := newInspectFixture(t, inspectAt, frames()...)
	jres := jf.run("run", "inspect", running.GetId(), "--watch", "--interval", "1s", "--json")
	if jres.code != exitRunFailed {
		t.Fatalf("--json exit = %d, want %d; stderr %s", jres.code, exitRunFailed, jres.stderr)
	}
	lines := strings.Split(strings.TrimRight(jres.stdout, "\n"), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "{") || !strings.Contains(lines[1], `"runStatus":"FAILED"`) {
		t.Fatalf("--watch --json = %q, want two compact lines ending FAILED", jres.stdout)
	}
}

func TestRunInspect_IntervalIsRefusedBelowOneSecondAndWithoutWatch(t *testing.T) {
	f := newInspectFixture(t, inspectAt, inspectFrame{run: trainRun(orkstackv1.Status_STATUS_RUNNING)})
	res := f.run("run", "inspect", "ork-train-1", "--watch", "--interval", "500ms")
	if res.code == 0 || !strings.Contains(res.stderr, "--interval must be at least 1s") {
		t.Fatalf("below 1s: exit %d, stderr %q", res.code, res.stderr)
	}
	res = f.run("run", "inspect", "ork-train-1", "--interval", "2s")
	if res.code == 0 || !strings.Contains(res.stderr, "--interval needs --watch") {
		t.Fatalf("without --watch: exit %d, stderr %q", res.code, res.stderr)
	}
	if reqs := f.requests(); len(reqs) != 0 {
		t.Fatalf("a refused flag still reached the server: %v", reqs)
	}
}

func TestRunInspect_FailedPreconditionIsWordedForAnOperator(t *testing.T) {
	f := newFixture(t)
	f.runs.Save(trainRun(orkstackv1.Status_STATUS_RUNNING))
	res := f.run("run", "inspect", "ork-train-1")
	if res.code == 0 || !strings.Contains(res.stderr,
		"live inspection is not available: this engine does not read runs from the cluster") {
		t.Fatalf("exit %d, stderr %q", res.code, res.stderr)
	}
}

func TestRunInspect_UnknownTaskAndOtherNamespaceAreNotFound(t *testing.T) {
	f := newInspectFixture(t, inspectAt, inspectFrame{run: trainRun(orkstackv1.Status_STATUS_RUNNING)})
	res := f.run("run", "inspect", "ork-train-1", "--task", "nope")
	if res.code == 0 || !strings.Contains(res.stderr, "not_found") || !strings.Contains(res.stderr, `task "nope" not found`) {
		t.Fatalf("unknown task: exit %d, stderr %q", res.code, res.stderr)
	}

	other := trainRun(orkstackv1.Status_STATUS_RUNNING)
	other.Namespace = "payments"
	g := newInspectFixture(t, inspectAt, inspectFrame{run: other})
	res = g.run("run", "inspect", "ork-train-1")
	if res.code == 0 || !strings.Contains(res.stderr, "not_found") {
		t.Fatalf("other namespace: exit %d, stderr %q", res.code, res.stderr)
	}
}
