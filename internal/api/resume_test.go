package api_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// resumeEpoch is when the histories below begin.
var resumeEpoch = time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)

// resumeHistory writes a base execution's history as the cluster records it.
type resumeHistory struct {
	t      testing.TB
	events []*historypb.HistoryEvent
}

func (b *resumeHistory) add(typ enumspb.EventType, set func(*historypb.HistoryEvent)) int64 {
	ev := &historypb.HistoryEvent{EventId: int64(len(b.events) + 1), EventType: typ,
		EventTime: timestamppb.New(resumeEpoch.Add(time.Duration(len(b.events)) * time.Second))}
	if set != nil {
		set(ev)
	}
	b.events = append(b.events, ev)
	return ev.EventId
}

func (b *resumeHistory) payloads(args ...any) *commonpb.Payloads {
	b.t.Helper()
	p, err := engine.DataConverter().ToPayloads(args...)
	if err != nil {
		b.t.Fatalf("encode: %v", err)
	}
	return p
}

// workflowTask is one completed workflow task; it answers the completion's id.
func (b *resumeHistory) workflowTask() int64 {
	sch := b.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, nil)
	st := b.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, nil)
	return b.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
			WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{ScheduledEventId: sch, StartedEventId: st}}
	})
}

func (b *resumeHistory) schedule(name string, args ...any) int64 {
	in := b.payloads(args...)
	return b.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityType: &commonpb.ActivityType{Name: name}, Input: in, HeartbeatTimeout: durationpb.New(20 * time.Second)}}
	})
}

func (b *resumeHistory) completed(sched int64) {
	b.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{ScheduledEventId: sched}}
	})
}

func (b *resumeHistory) publish(typ orkstackv1.EventType, seq int64, taskID string) {
	b.completed(b.schedule(engine.ActivityPublishEvent, &orkstackv1.Event{Type: typ, Sequence: seq, TaskId: taskID}, "snap"))
}

// failedHistory is a generation 0 that ran tasks and closed FAILED.
func failedHistory(t testing.TB, tasks ...string) []*historypb.HistoryEvent {
	b := &resumeHistory{t: t}
	in := b.payloads("ork-r", &orkstackv1.Orkflow{}, &orkstackv1.Payload{}, engine.RunPlan{})
	b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: in}}
	})
	b.workflowTask()
	b.publish(orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, 2, "")
	b.workflowTask()
	for _, id := range tasks {
		b.completed(b.schedule(task.ActivityExecuteTask, &orkstackv1.ExecuteTaskInput{TaskId: id, CancellationGracePeriodSeconds: 30}))
		b.workflowTask()
	}
	b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, nil)
	return b.events
}

// fakeResumeCluster is the cluster ResumeRun sees.
type fakeResumeCluster struct {
	mu         sync.Mutex
	executions map[string]engine.ResumeExecution // "" is the current one
	histories  map[string][]*historypb.HistoryEvent
	resetKeys  map[string]string
	// children are the current executions of other runs.
	children   map[string]engine.ResumeExecution
	err        error
	historyErr error
	// reads counts history reads, so a test can say that none was made.
	reads  int
	resets []engine.ResumeReset
	// onHistory runs at each history read, and answers it when it says so.
	onHistory func() ([]*historypb.HistoryEvent, bool)
	// onReset answers a reset; nil refuses every one.
	onReset func(engine.ResumeReset) (string, error)
	// payloadsErr refuses the run's payloads; resetKeyErr the reset key's
	// read, which opens them too.
	payloadsErr, resetKeyErr error
}

func (f *fakeResumeCluster) DescribeResumeExecution(_ context.Context, runID, execution string) (engine.ResumeExecution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return engine.ResumeExecution{}, f.err
	}
	if runID != "ork-r" {
		child, ok := f.children[runID]
		if !ok {
			return engine.ResumeExecution{}, engine.ErrResumeExecutionNotFound
		}
		return child, nil
	}
	got, ok := f.executions[execution]
	if !ok {
		return engine.ResumeExecution{}, engine.ErrResumeExecutionNotFound
	}
	return got, nil
}

func (f *fakeResumeCluster) ResumeExecutionHistory(_ context.Context, _, execution string) ([]*historypb.HistoryEvent, error) {
	f.mu.Lock()
	f.reads++
	f.mu.Unlock()
	if f.onHistory != nil {
		if events, answered := f.onHistory(); answered {
			return events, nil
		}
	}
	if f.historyErr != nil {
		return nil, f.historyErr
	}
	events, ok := f.histories[execution]
	if !ok {
		return nil, engine.ErrResumeExecutionNotFound
	}
	return events, nil
}

func (f *fakeResumeCluster) ResetKeyOf(_ context.Context, _, execution string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.resetKeyErr != nil {
		return "", false, f.resetKeyErr
	}
	key, ok := f.resetKeys[execution]
	return key, ok, nil
}

func (f *fakeResumeCluster) ExecutionPayloads(context.Context, string, *historypb.HistoryEvent) (converter.DataConverter, error) {
	if f.payloadsErr != nil {
		return nil, f.payloadsErr
	}
	return engine.DataConverter(), nil
}

func (f *fakeResumeCluster) ResetForResume(_ context.Context, reset engine.ResumeReset) (string, error) {
	f.mu.Lock()
	f.resets = append(f.resets, reset)
	on := f.onReset
	f.mu.Unlock()
	if on == nil {
		return "", errors.New("no reset is expected")
	}
	return on(reset)
}

// resumeClaimer is the claims a replica shares with the others: onClaim runs
// before each claim, and answer is another request's claim.
type resumeClaimer struct {
	*sharedClaimer
	onClaim func()
	answer  *engine.Claim
}

func (c *resumeClaimer) Claim(ctx context.Context, key string, candidate engine.Claim) (engine.Claim, error) {
	if c.onClaim != nil {
		c.onClaim()
	}
	if c.answer != nil {
		return *c.answer, nil
	}
	return c.sharedClaimer.Claim(ctx, key, candidate)
}

// resumeRunReader answers the run and its last sequence from one read.
type resumeRunReader struct {
	*fakeReader
	sequence int64 // under mu, as the run is
	// onStream, when set, is called as each stream of events begins.
	onStream func()
}

func (r *resumeRunReader) StreamEvents(ctx context.Context, runID string, from int64, send func(*orkstackv1.Event) error) error {
	if r.onStream != nil {
		r.onStream()
	}
	return r.fakeReader.StreamEvents(ctx, runID, from, send)
}

func (r *resumeRunReader) set(sequence int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sequence = sequence
}

func (r *resumeRunReader) GetRunAndSequence(_ context.Context, runID string) (*orkstackv1.Run, int64, error) {
	if r.getErr != nil {
		return nil, 0, r.getErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	run, err := r.readRun(runID)
	return run, r.sequence, err
}

// resumeFixture is a run "ork-r" of two tasks, a then b, that failed at b and
// whose last event is 9.
type resumeFixture struct {
	*readerFixture
	run      *orkstackv1.Run
	def      *orkstackv1.Orkflow
	cluster  *fakeResumeCluster
	clock    *stepClock
	settings api.ResumeSettings
	records  api.ResumeRecords
	// recordStore is the identity records an admitted run's resume reads.
	recordStore *artifacts.RecordStore
	claims      *resumeClaimer
	runReader   *resumeRunReader
	attempts    *artifacts.MemoryAttemptStore
	endings     *fakeResumeEndings
	// claimsErr is what listing the run's open claims answers, when set.
	claimsErr error
	fleet     *stubFleet
	commits   *artifacts.MemoryCommitStore
	store     *artifacts.LocalStore
	snaps     *artifacts.LocalRunSnapshotStore
	plan      engine.RunPlan
	pools     livePools
	configure func(*api.Config)
	// stream is the run's events as its reader streams them; nil for none.
	stream []*orkstackv1.Event
	// streamGoesOn keeps the stream open past its events, as a run's current
	// execution does while it runs; streamGone answers it as a run that is
	// not there.
	streamGoesOn, streamGone bool
}

func newResumeFixture(t *testing.T, change func(*resumeFixture)) *resumeFixture {
	t.Helper()
	f := &resumeFixture{
		run: &orkstackv1.Run{Id: "ork-r", OrkflowName: "wf", OrkflowVersion: "1", Status: orkstackv1.Status_STATUS_FAILED,
			Error: &orkstackv1.Error{Code: "RUN_FAILED"},
			TaskRuns: []*orkstackv1.TaskRun{
				{TaskId: "a", Status: orkstackv1.Status_STATUS_SUCCEEDED},
				{TaskId: "b", Status: orkstackv1.Status_STATUS_FAILED},
			}},
		def: &orkstackv1.Orkflow{Name: "wf", Version: "1", Tasks: []*orkstackv1.Task{
			{Id: "a", Uses: "image:busybox"}, {Id: "b", Needs: []string{"a"}, Uses: "image:busybox"}}},
		cluster: &fakeResumeCluster{
			executions: map[string]engine.ResumeExecution{},
			histories:  map[string][]*historypb.HistoryEvent{"exec-1": failedHistory(t, "a", "b")},
		},
		settings: api.DefaultResumeSettings(),
		claims:   &resumeClaimer{sharedClaimer: newSharedClaimer()},
		attempts: artifacts.NewMemoryAttemptStore(),
		fleet:    &stubFleet{},
		endings:  &fakeResumeEndings{archived: map[uint64]bool{0: true}},
		commits:  artifacts.NewMemoryCommitStore(),
		plan:     engine.RunPlan{CommitRoot: resumeCommitRoot},
		pools:    livePools{task.DefaultQueue: 1},
		// Long after everything the run started has stopped.
		clock: &stepClock{now: resumeEpoch.Add(24 * time.Hour)},
	}
	f.endings.body = func() *orkstackv1.Run {
		f.reader.mu.Lock()
		defer f.reader.mu.Unlock()
		return proto.Clone(f.run).(*orkstackv1.Run)
	}
	objects := t.TempDir()
	f.store, f.snaps = openResumeStoresAt(t, objects)
	// One bucket holds the run's objects and its records, as a deployment's does.
	records, err := artifacts.NewRecordStore(context.Background(), artifacts.Config{Backend: "local", LocalDir: objects})
	if err != nil {
		t.Fatal(err)
	}
	f.records, f.recordStore = records, records
	base := engine.ResumeExecution{ExecutionRunID: "exec-1", Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		CloseTime: resumeEpoch.Add(time.Hour)}
	f.cluster.executions[""], f.cluster.executions["exec-1"] = base, base
	if change != nil {
		change(f)
	}
	reader := &fakeReader{runs: map[string]*orkstackv1.Run{"ork-r": f.run}, events: map[string][]*orkstackv1.Event{"ork-r": f.stream},
		starts: map[string]*engine.RunStartRecord{"ork-r": {Orkflow: f.def, Plan: f.plan}}}
	if f.streamGoesOn {
		reader.live = make(chan struct{})
	}
	if f.streamGone {
		delete(reader.events, "ork-r")
	}
	f.runReader = &resumeRunReader{fakeReader: reader, sequence: 9}
	f.readerFixture = newReaderFixtureWith(t, reader, func(cfg *api.Config) {
		cfg.ResumeClaims = f.claims
		cfg.Now = f.clock.Now
		cfg.ResumeSettings = f.settings
		cfg.ResumeRecords = f.records
		cfg.RecordedResults = f.recordStore
		cfg.ResumeEndings, cfg.ResumeArchiver = f.endings, f.endings
		cfg.Attempts = f.attempts
		if f.claimsErr != nil {
			cfg.Attempts = failingClaims{f.attempts, f.claimsErr}
		}
		cfg.Fleet = f.fleet
		cfg.Commits, cfg.ArtifactStore, cfg.ResumeSnapshots = f.commits, f.store, f.snaps
		cfg.Pools = f.pools
		cfg.Reader = f.runReader
		if f.cluster != nil {
			cfg.Resume = f.cluster
		}
		if f.configure != nil {
			f.configure(cfg)
		}
	})
	f.commit(t, "a", nil)
	return f
}

func (f *resumeFixture) resume(req *orkstackv1.ResumeRunRequest) (*orkstackv1.ResumeRunResponse, error) {
	if req.RunId == "" {
		req.RunId = "ork-r"
	}
	resp, err := f.svc.ResumeRun(context.Background(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// refusalOf is the ResumeRefusal a refused ResumeRun carries.
func refusalOf(t *testing.T, err error) *orkstackv1.ResumeRefusal {
	t.Helper()
	var cerr *connect.Error
	if !errors.As(err, &cerr) || cerr.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("= %v, want FailedPrecondition with a refusal", err)
	}
	for _, d := range cerr.Details() {
		v, derr := d.Value()
		if derr != nil {
			t.Fatalf("decode detail: %v", derr)
		}
		if r, ok := v.(*orkstackv1.ResumeRefusal); ok {
			return r
		}
	}
	t.Fatalf("%v carries no ResumeRefusal", err)
	return nil
}

// TestResumeRun_InvalidRequests: a malformed request is InvalidArgument,
// naming the field, and nothing of the run is read.
func TestResumeRun_InvalidRequests(t *testing.T) {
	digest := strings.Repeat("a", 64)
	for name, tc := range map[string]struct {
		req  *orkstackv1.ResumeRunRequest
		want string
	}{
		"a long reason":            {&orkstackv1.ResumeRunRequest{DryRun: true, Reason: strings.Repeat("x", 1025)}, "reason"},
		"a reason that is no text": {&orkstackv1.ResumeRunRequest{DryRun: true, Reason: "\xff"}, "reason"},
		"a confirm of no ending":   {&orkstackv1.ResumeRunRequest{}, "expected_terminal_sequence"},
		"a negative ending":        {&orkstackv1.ResumeRunRequest{DryRun: true, ExpectedTerminalSequence: -1}, "expected_terminal_sequence"},
		"a plan digest that is none": {&orkstackv1.ResumeRunRequest{ExpectedTerminalSequence: 9, ExpectedPlanDigest: "abc"},
			"expected_plan_digest"},
		"attempts with no plan": {&orkstackv1.ResumeRunRequest{ExpectedTerminalSequence: 9, AcknowledgedUnknownAttempts: []string{"o/t//1"}},
			"expected_plan_digest"},
		"too many attempts": {&orkstackv1.ResumeRunRequest{ExpectedTerminalSequence: 9, ExpectedPlanDigest: digest,
			AcknowledgedUnknownAttempts: make([]string, 10_001)}, "acknowledged_unknown_attempts"},
		"a gap with no sequence": {&orkstackv1.ResumeRunRequest{DryRun: true, AcceptSequenceGap: true}, "accepted_highest_seq"},
		"an unseen stop of a run that was not force-canceled": {&orkstackv1.ResumeRunRequest{DryRun: true, AcceptUnverifiedStop: true},
			"accept_unverified_stop"},
		"a task id no task has":  {&orkstackv1.ResumeRunRequest{DryRun: true, FromTaskId: strings.Repeat("a", 201)}, "from_task_id"},
		"a run id no run has":    {&orkstackv1.ResumeRunRequest{DryRun: true, RunId: strings.Repeat("a", 256)}, "run_id"},
		"a sequence with no gap": {&orkstackv1.ResumeRunRequest{DryRun: true, AcceptedHighestSeq: 12}, "accept_sequence_gap"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, nil)
			_, err := f.resume(tc.req)
			if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("= %v, want InvalidArgument naming %s", err, tc.want)
			}
			if f.reader.runReadCount != 0 || f.cluster.reads != 0 {
				t.Fatal("a malformed request read the run")
			}
		})
	}
	f := newResumeFixture(t, nil)
	if _, err := f.svc.ResumeRun(context.Background(), connect.NewRequest(&orkstackv1.ResumeRunRequest{DryRun: true})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("no run id = %v, want InvalidArgument", err)
	}
}

// TestResumeRun_BaseRefusals: what is refused before any saved result is
// read, with the class a client acts on.
func TestResumeRun_BaseRefusals(t *testing.T) {
	permanent, wait, replan := orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT,
		orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT, orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_REPLAN
	current := func(f *resumeFixture, change func(*engine.ResumeExecution)) {
		e := f.cluster.executions[""]
		change(&e)
		f.cluster.executions[""], f.cluster.executions["exec-1"] = e, e
	}
	preview := &orkstackv1.ResumeRunRequest{DryRun: true}
	for name, tc := range map[string]struct {
		change func(*resumeFixture)
		req    *orkstackv1.ResumeRunRequest
		reason string
		class  orkstackv1.ResumeRefusalClass
	}{
		"a running run": {func(f *resumeFixture) { f.run.Status = orkstackv1.Status_STATUS_RUNNING }, preview, "STATE_NOT_RESUMABLE", permanent},
		"a paused run":  {func(f *resumeFixture) { f.run.Status = orkstackv1.Status_STATUS_PAUSED }, preview, "STATE_NOT_RESUMABLE", permanent},
		"a run whose history is gone": {func(f *resumeFixture) { f.cluster.executions = nil }, preview,
			"HISTORY_UNAVAILABLE", permanent},
		"a run whose history went since it was described": {func(f *resumeFixture) { f.cluster.histories = nil }, preview,
			"HISTORY_UNAVAILABLE", permanent},
		"a run still closing": {func(f *resumeFixture) {
			current(f, func(e *engine.ResumeExecution) { e.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING })
		}, preview, "STILL_STOPPING", wait},
		"a run that ended otherwise than it recorded": {func(f *resumeFixture) {
			current(f, func(e *engine.ResumeExecution) { e.Status = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED })
		}, preview, "STATE_NOT_RESUMABLE", permanent},
		"a base that was reset": {func(f *resumeFixture) {
			current(f, func(e *engine.ResumeExecution) { e.ResetRunID = "exec-2" })
		}, preview, "RUN_CHANGED", replan},
		"another ending than the caller saw": {nil, &orkstackv1.ResumeRunRequest{ExpectedTerminalSequence: 7}, "RUN_CHANGED", replan},
		"a task the run does not have":       {nil, &orkstackv1.ResumeRunRequest{DryRun: true, FromTaskId: "z"}, "FROM_TASK_UNKNOWN", permanent},
		"a run that never started": {func(f *resumeFixture) {
			f.cluster.histories["exec-1"] = failedHistory(t)[:4]
			f.cluster.histories["exec-1"] = append(f.cluster.histories["exec-1"], &historypb.HistoryEvent{EventId: 5,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, EventTime: timestamppb.New(resumeEpoch)})
		}, preview, "NEVER_STARTED", permanent},
		"a child run": {func(f *resumeFixture) {
			f.run.Origin = &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: "ork-p", ParentTaskId: "t"}
		}, preview, "CHILD_RUN", permanent},
		"a definition that does not verify": {func(f *resumeFixture) { f.def = nil }, preview, "STATE_UNREADABLE", permanent},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, tc.change)
			if name == "a definition that does not verify" {
				f.reader.definitions = func(context.Context, string, *engine.RunStartRecord) (*orkstackv1.Orkflow, error) {
					return nil, engine.ErrContinuationObjectInvalid
				}
			}
			_, err := f.resume(tc.req)
			r := refusalOf(t, err)
			if r.GetReason() != tc.reason || r.GetClass() != tc.class || r.GetMessage() == "" {
				t.Fatalf("refusal = %v, want %s %s", r, tc.reason, tc.class)
			}
			if tc.class == wait && r.GetRetryAfterSeconds() <= 0 {
				t.Fatalf("a wait says nothing about when: %v", r)
			}
			if len(f.cluster.resets) != 0 {
				t.Fatal("a refused resume reset the run")
			}
		})
	}
}

// TestResumeRun_NotThisNamespacesRun: an unknown run and another namespace's
// read alike, as they do to every other run request.
func TestResumeRun_NotThisNamespacesRun(t *testing.T) {
	f := newResumeFixture(t, func(f *resumeFixture) { f.run.Namespace = "other" })
	_, elsewhere := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	_, unknown := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true, RunId: "ork-none"})
	if connect.CodeOf(elsewhere) != connect.CodeNotFound || connect.CodeOf(unknown) != connect.CodeNotFound {
		t.Fatalf("another namespace's run = %v, an unknown one = %v; want NotFound", elsewhere, unknown)
	}
	if strings.ReplaceAll(elsewhere.Error(), "ork-r", "X") != strings.ReplaceAll(unknown.Error(), "ork-none", "X") {
		t.Fatalf("%v and %v read differently", elsewhere, unknown)
	}
}

// TestResumeRun_ClusterUnavailable: a cluster or store that does not answer
// is a wait, never a permanent refusal.
func TestResumeRun_ClusterUnavailable(t *testing.T) {
	for name, change := range map[string]func(*resumeFixture){
		"describe": func(f *resumeFixture) { f.cluster.err = errors.New("unreachable") },
		"history":  func(f *resumeFixture) { f.cluster.historyErr = errors.New("unreachable") },
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, change)
			if _, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true}); connect.CodeOf(err) != connect.CodeFailedPrecondition || strings.Contains(err.Error(), "unreachable") {
				t.Fatalf("= %v, want a refusal, with the cluster's own words in the log alone", err)
			} else if r := refusalOf(t, err); r.GetReason() != "RESUME_CHECK_UNAVAILABLE" || r.GetRetryAfterSeconds() <= 0 {
				t.Fatalf("refusal = %v, want RESUME_CHECK_UNAVAILABLE with when to retry: no reset was sent", r)
			}
		})
	}
	t.Run("definition", func(t *testing.T) {
		f := newResumeFixture(t, nil)
		f.reader.definitions = func(context.Context, string, *engine.RunStartRecord) (*orkstackv1.Orkflow, error) {
			return nil, errors.New("store unreachable")
		}
		_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
		if r := refusalOf(t, err); r.GetReason() != "RESUME_CHECK_UNAVAILABLE" || r.GetRetryAfterSeconds() <= 0 {
			t.Fatalf("refusal = %v, want RESUME_CHECK_UNAVAILABLE with when to retry", r)
		}
	})
	t.Run("no definition", func(t *testing.T) {
		f := newResumeFixture(t, nil)
		f.reader.definitions = func(context.Context, string, *engine.RunStartRecord) (*orkstackv1.Orkflow, error) { return nil, nil }
		_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
		if r := refusalOf(t, err); r.GetReason() != "STATE_UNREADABLE" {
			t.Fatalf("refusal = %v, want STATE_UNREADABLE", r)
		}
	})
	t.Run("a caller who left", func(t *testing.T) {
		f := newResumeFixture(t, nil)
		ctx, leave := context.WithCancel(context.Background())
		f.cluster.onHistory = func() ([]*historypb.HistoryEvent, bool) { leave(); return nil, false }
		f.cluster.historyErr = context.Canceled
		_, err := f.svc.ResumeRun(ctx, connect.NewRequest(&orkstackv1.ResumeRunRequest{RunId: "ork-r", DryRun: true}))
		if connect.CodeOf(err) != connect.CodeCanceled {
			t.Fatalf("= %v, want Canceled", err)
		}
	})
	t.Run("no seam", func(t *testing.T) {
		f := newResumeFixture(t, func(f *resumeFixture) { f.cluster = nil })
		if _, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true}); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("= %v, want FailedPrecondition", err)
		}
		// With every store there, and no seam: no run says it can be resumed.
		got, err := f.svc.GetRun(context.Background(), connect.NewRequest(&orkstackv1.GetRunRequest{RunId: "ork-r"}))
		if err != nil || got.Msg.GetRun().GetResumeHint() != nil {
			t.Fatalf("GetRun = %v, %v; want no resume hint", got, err)
		}
	})
}

// fakeResumeRecords answers one plan record, as a test sets it, and holds no
// other.
type fakeResumeRecords struct {
	*artifacts.RecordStore
	read   artifacts.RecordRead[*orkstackv1.ResumePlanRecord]
	digest string
	err    error
}

func (f *fakeResumeRecords) ReadResumePlan(context.Context, string, string, uint64, int) (artifacts.RecordRead[*orkstackv1.ResumePlanRecord], string, error) {
	return f.read, f.digest, f.err
}

// TestResumeRun_CountsTheResumesBeforeIt: the run a resume made counts the
// resumes before it in its plan, so the cap holds without a counter stored
// anywhere; a run reset by hand counts none.
func TestResumeRun_CountsTheResumesBeforeIt(t *testing.T) {
	digest := strings.Repeat("ab", 32)
	key := engine.ResumePlanKey{Scope: 12, N: 0, PlanDigest: digest}.String()
	found := func(before int32) artifacts.RecordRead[*orkstackv1.ResumePlanRecord] {
		return artifacts.RecordRead[*orkstackv1.ResumePlanRecord]{Answer: artifacts.RecordFound,
			Value: &orkstackv1.ResumePlanRecord{Block: &orkstackv1.ResumeBlock{ResumesBefore: before}}}
	}
	for name, tc := range map[string]struct {
		key     string
		records *fakeResumeRecords
		refused string
	}{
		"never resumed":                    {"", &fakeResumeRecords{}, ""},
		"reset by hand":                    {"an operator's reset", &fakeResumeRecords{}, ""},
		"resumed eighteen times before":    {key, &fakeResumeRecords{read: found(18), digest: digest}, ""},
		"resumed nineteen times before":    {key, &fakeResumeRecords{read: found(19), digest: digest}, "TOO_MANY_RESUMES"},
		"a record that is not the reset's": {key, &fakeResumeRecords{read: found(19), digest: strings.Repeat("cd", 32)}, ""},
		"a record that is gone":            {key, &fakeResumeRecords{}, ""},
		"a record of a format not known":   {key, &fakeResumeRecords{read: artifacts.RecordRead[*orkstackv1.ResumePlanRecord]{Answer: artifacts.RecordUnsupported, Version: 9}}, "RELEASE_UNSUPPORTED"},
		"a store that does not answer":     {key, &fakeResumeRecords{err: errors.New("unreachable")}, "RESUME_CHECK_UNAVAILABLE"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, func(f *resumeFixture) {
				tc.records.RecordStore = f.records.(*artifacts.RecordStore)
				f.records = tc.records
				if tc.key != "" {
					f.cluster.resetKeys = map[string]string{"exec-1": tc.key}
				}
			})
			_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
			if tc.refused == "" {
				if connect.CodeOf(err) == connect.CodeFailedPrecondition {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if r := refusalOf(t, err); r.GetReason() != tc.refused {
				t.Fatalf("refusal = %v, want %s", r, tc.refused)
			}
		})
	}
}

const resumeCommitRoot = "mem://commits"

func openResumeStoresAt(t *testing.T, dir string) (*artifacts.LocalStore, *artifacts.LocalRunSnapshotStore) {
	t.Helper()
	store, err := artifacts.NewLocalStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	snaps, err := artifacts.NewLocalRunSnapshotStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store, snaps
}

// commit writes task id's commit as its Orker would, as change leaves it.
func (f *resumeFixture) commit(t *testing.T, id string, change func(*orkstackv1.ActivityCommit)) {
	t.Helper()
	for _, def := range f.def.GetTasks() {
		if def.GetId() != id {
			continue
		}
		want, err := engine.ExpectedCommitIdentity(f.def, "ork-r", def, -1, 0)
		if err != nil {
			t.Fatal(err)
		}
		c := &orkstackv1.ActivityCommit{
			SchemaVersion: artifacts.CommitSchemaVersion, Namespace: want.Namespace, RunId: want.RunID, TaskId: want.TaskID,
			OrkflowDefinitionDigest: want.DefinitionDigest, IdempotencyKey: want.IdempotencyKey,
			Image: &orkstackv1.ImageRef{IndexDigest: want.ImageIndexDigest},
		}
		if change != nil {
			change(c)
		}
		if _, err := f.commits.Create(context.Background(), resumeCommitRoot+"/"+want.Key, c); err != nil {
			t.Fatal(err)
		}
		if f.plan.Identity != nil {
			f.recordCommit(t, want.Key, c)
		}
	}
}

// resumeAdmission is the admission digest of a fixture's run that holds an
// identity.
var resumeAdmission = strings.Repeat("a", 64)

// recordCommit records c as the engine records an admitted run's result: in
// the record store, and at the attested key in the commit store the read-back
// reads, as one bucket holds both.
func (f *resumeFixture) recordCommit(t *testing.T, key string, c *orkstackv1.ActivityCommit) {
	t.Helper()
	c = proto.Clone(c).(*orkstackv1.ActivityCommit)
	c.Recorded = &orkstackv1.CommitRecording{AdmissionSha256: f.plan.Identity.AdmissionSHA256}
	if c.GetValues() == nil {
		ref, err := f.store.Put(context.Background(), "values.json", strings.NewReader(`{}`), artifacts.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		c.Values = &orkstackv1.ArtifactRef{Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}
	}
	if _, err := f.recordStore.RecordCommit(context.Background(), key, c); err != nil {
		t.Fatal(err)
	}
	attested, err := artifacts.AttestedCommitKey(key)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := artifacts.MarshalCommit(c)
	if err != nil {
		t.Fatal(err)
	}
	f.commits.PutRaw(resumeCommitRoot+"/"+attested, raw)
}
