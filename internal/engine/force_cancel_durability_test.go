package engine

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestTemporalRunReader_AForceCancelItCannotCheckIsNotPlainlyTerminated: a
// force-canceled run read while the keys its record was signed under cannot be
// read is an error on every read, the archive's above all: an archive body is
// immutable, and one that says RUN_TERMINATED would say it for good
// (AGENTS.md §15). Once the keys are back, every read says RUN_FORCE_CANCELED.
func TestTemporalRunReader_AForceCancelItCannotCheckIsNotPlainlyTerminated(t *testing.T) {
	const runID = "ork-r"
	store, err := artifacts.NewPublishedKeysStore(context.Background(),
		artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must.OK(t, err)
	signer, err := jwt.NewSigner(key)
	must.OK(t, err)
	// One engine's keys, published; another engine that has not read them yet.
	ring := func() *identity.Issuer {
		i, err := identity.New(identity.Config{Keys: []*jwt.Signer{signer}, Algorithm: jwt.ES256, ClockSkew: time.Minute, Engine: "engine-a"}, store, nil, nil)
		must.OK(t, err)
		return i
	}
	keys, unread := ring(), ring()
	must.OK(t, keys.Publish(context.Background()))

	rec := sampleRecord()
	rec.Namespace = "default"
	rec.WorkflowID = ForceCancelWorkflowIDPrefix(rec.Namespace, runID) + rec.RequestID
	details, err := SignForceCancelRecord(keys, rec)
	must.OK(t, err)
	history, snapshots := helloRun(t, runID)
	closeEvent := terminated(t, details)
	closeEvent.EventId, closeEvent.EventType = 9, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
	closeEvent.EventTime = closedEvent(9, closeEvent.EventType, time.Now()).GetEventTime()
	history = append(history[:3], closeEvent)
	read := func(v ForceCancelVerifier) (map[string]string, map[string]error) {
		reader, fake := readerOver(history, runID)
		fake.describes = map[string]*workflowservice.DescribeWorkflowExecutionResponse{runID: {
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
				Execution: &commonpb.WorkflowExecution{WorkflowId: runID, RunId: "exec-1"}}}}
		reader = reader.WithSnapshotStore(snapshots).WithForceCancelVerifier(v, func() string { return "enforce" })
		codes, errs := map[string]string{}, map[string]error{}
		run, err := reader.GetRun(context.Background(), runID)
		codes["GetRun"], errs["GetRun"] = run.GetError().GetCode(), err
		if run.GetForceCancelRecord() != "" {
			t.Error("GetRun answers the record itself, which says more than who")
		}
		archived, _, err := reader.readForArchive(context.Background(), runID, false)
		codes["readForArchive"], errs["readForArchive"] = archived.GetError().GetCode(), err
		events, err := reader.Events(context.Background(), runID, 0)
		codes["Events"], errs["Events"] = lastMessageCode(events), err
		forArchive, err := reader.EventsForArchive(context.Background(), runID)
		codes["EventsForArchive"], errs["EventsForArchive"] = lastMessageCode(forArchive.Events), err
		return codes, errs
	}
	codes, errs := read(unread)
	for surface, err := range errs {
		if err == nil {
			t.Errorf("%s with the keys unread answered %q; want an error, nothing decided", surface, codes[surface])
		}
	}
	// The archive's last chance: it says so and names nobody, and keeps the
	// record for whoever can verify it later; a run that may have been resumed since is still not archived.
	lastResort := func(v ForceCancelVerifier, status enumspb.WorkflowExecutionStatus) (*orkstackv1.Run, bool, error) {
		reader, fake := readerOver(history, runID)
		fake.describes = map[string]*workflowservice.DescribeWorkflowExecutionResponse{runID: {
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status, Execution: &commonpb.WorkflowExecution{WorkflowId: runID, RunId: "exec-1"}}}}
		bodies, err := artifacts.NewRunArchive(artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
		must.OK(t, err)
		archive := NewRunArchiver(bodies)
		unverified, err := NewHistoryArchiver(reader.WithSnapshotStore(snapshots).WithForceCancelVerifier(v, func() string { return "enforce" }), archive).
			ArchiveLastResort(context.Background(), runID)
		if err != nil {
			return nil, unverified, err
		}
		body, err := archive.LoadRunBody(context.Background(), runID)
		must.OK(t, err)
		run, err := UnmarshalRunLine(body)
		must.OK(t, err)
		return run, unverified, nil
	}
	terminatedStatus := enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	run, unverified, err := lastResort(unread, terminatedStatus)
	if err != nil || !unverified || run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != RunForceCancelUnverifiedCode ||
		run.GetCanceledBy() != "" || run.GetCanceledByVerified() || !strings.Contains(run.GetForceCancelRecord(), `"signed":`) || !run.GetFinishedAt().IsValid() {
		t.Errorf("as a last resort: %v, unverified %v, %v", run, unverified, err)
	}
	if run, unverified, err = lastResort(keys, terminatedStatus); err != nil || unverified || run.GetError().GetCode() != RunForceCanceledCode {
		t.Errorf("as a last resort, with the keys read: %v, unverified %v, %v", run, unverified, err)
	}
	if run, _, err = lastResort(unread, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING); err == nil {
		t.Errorf("a run resumed since was archived: %v", run)
	}
	codes, errs = read(keys)
	for surface, err := range errs {
		if err != nil || codes[surface] != RunForceCanceledCode {
			t.Errorf("%s with the keys read: %q, %v; want %s", surface, codes[surface], err, RunForceCanceledCode)
		}
	}
}

// TestTemporalRunReader_AForceCancelOfAResumeThatNeverBegan: a resumed run
// force-canceled before it recorded anything of its own ends at its resume's
// scope, after the ending that resume set aside and with that ending's rows,
// and takes no sequence of it. Where that ending is gone it still reads as
// force-canceled, and says what is missing.
func TestTemporalRunReader_AForceCancelOfAResumeThatNeverBegan(t *testing.T) {
	const runID, scope = "ork-r", int64(9)
	ctx := context.Background()
	keys := recordKeys(t)
	rec := sampleRecord()
	rec.Namespace, rec.WorkflowID = "default", ForceCancelWorkflowIDPrefix("default", runID)+rec.RequestID
	details, err := SignForceCancelRecord(keys, rec)
	must.OK(t, err)
	key := ResumePlanKey{Scope: uint64(scope), PlanDigest: strings.Repeat("a", 64)}.String()
	earlier := ResumePlanKey{Scope: 5, PlanDigest: strings.Repeat("b", 64)}.String()
	reset := func(cause enumspb.WorkflowTaskFailedCause, reason string) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{EventId: 4, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
			WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{Cause: cause, Failure: &failurepb.Failure{Message: reason}}}}
	}
	resets := func(events ...*historypb.HistoryEvent) []*historypb.HistoryEvent { return events }
	// The ending set aside was a force cancel's too: what it says of who ended
	// it is not this ending's.
	base := &orkstackv1.Run{Id: runID, OrkflowName: "wf", Status: orkstackv1.Status_STATUS_CANCELED, Error: &orkstackv1.Error{Code: "EXIT_1"},
		CanceledBy: "someone else", CanceledByVerified: true, ForceCancelRecord: "another record",
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED}}}
	byReset := enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW
	for _, tt := range []struct {
		name   string
		failed []*historypb.HistoryEvent
		lost   bool
		ends   int64
		rows   int
	}{
		{"resumed, and nothing since", resets(reset(byReset, key)), false, scope, 1},
		{"resumed twice", resets(reset(byReset, earlier), reset(byReset, key)), false, scope, 1},
		{"resumed, then reset by hand", resets(reset(byReset, key), reset(byReset, "by hand")), false, scope, 1},
		{"reset by hand, then resumed", resets(reset(byReset, "by hand"), reset(byReset, key)), false, scope, 1},
		{"the ending set aside is gone", resets(reset(byReset, key)), true, scope, 0},
		{"reset by hand", resets(reset(byReset, "by hand")), false, 3, 0},
		{"a task of it failed, saying so", resets(reset(enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE, key)), false, 3, 0},
		{"a scope its own events are past", resets(reset(byReset, ResumePlanKey{Scope: 2, PlanDigest: strings.Repeat("a", 64)}.String())), false, 3, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			history, snapshots := helloRun(t, runID)
			closeEvent := terminated(t, details)
			closeEvent.EventId, closeEvent.EventType = 9, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
			closeEvent.EventTime = closedEvent(9, closeEvent.EventType, time.Now()).GetEventTime()
			history = append(append(history[:3:3], tt.failed...), closeEvent)
			reader, fake := readerOver(history, runID)
			fake.describes = map[string]*workflowservice.DescribeWorkflowExecutionResponse{runID: {
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
					Execution: &commonpb.WorkflowExecution{WorkflowId: runID, RunId: "exec-1"}}}}
			endings := &fakeEndings{run: base, events: setAside(scope - 1)}
			if tt.lost {
				endings = &fakeEndings{err: artifacts.ErrNotFound}
			}
			reader = reader.WithSnapshotStore(snapshots).WithForceCancelVerifier(keys, func() string { return "enforce" }).WithEndings(endings)

			live, err := reader.GetRun(ctx, runID)
			must.OK(t, err)
			archived, read, err := reader.readForArchive(ctx, runID, false)
			must.OK(t, err)
			for where, run := range map[string]*orkstackv1.Run{"live": live, "archived": archived} {
				if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != RunForceCanceledCode || len(run.GetTaskRuns()) != tt.rows ||
					run.GetCanceledBy() != rec.Principal || run.GetForceCancelRecord() == "another record" ||
					strings.Contains(run.GetError().GetDetails(), recordSetAsideSentence) != tt.lost {
					t.Errorf("%s: %v", where, run)
				}
			}
			if last := read.Events[len(read.Events)-1]; last.GetSequence() != tt.ends || last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED || (len(read.Gaps) > 0) != tt.lost {
				t.Errorf("archived, it ends %v with gaps %v", last, read.Gaps)
			}
			if base.GetCanceledBy() != "someone else" {
				t.Error("the ending set aside was changed")
			}

			events, err := reader.Events(ctx, runID, 0)
			if tt.lost {
				if !errors.Is(err, ErrEventSegmentLost) {
					t.Errorf("its stream = %v", err)
				}
			} else if must.OK(t, err); !proto.Equal(events[len(events)-1], read.Events[len(read.Events)-1]) || int64(len(events)) != tt.ends {
				t.Errorf("its stream ends %v after %d events", events[len(events)-1], len(events))
			}
			// A stream that goes on from what the resume set aside gets the end alone.
			since, err := reader.Events(ctx, runID, max(tt.ends-1, 2))
			must.OK(t, err)
			if len(since) != 1 || since[0].GetSequence() != tt.ends {
				t.Errorf("from %d: %v", tt.ends-1, since)
			}
		})
	}
}

// TestHistoryArchiver_TheLastResortOfAResume: the last resort of a resumed run
// is its newest ending's, with the rows of the ending set aside and nobody
// named, and every reader of it shows no more than that.
func TestHistoryArchiver_TheLastResortOfAResume(t *testing.T) {
	const runID, scope = "ork-r", uint64(9)
	ctx := context.Background()
	mode := func() string { return "enforce" }
	keys := recordKeys(t)
	rec := sampleRecord()
	rec.Namespace, rec.WorkflowID = "default", ForceCancelWorkflowIDPrefix("default", runID)+rec.RequestID
	details, err := SignForceCancelRecord(keys, rec)
	must.OK(t, err)
	history, snapshots := helloRun(t, runID)
	closeEvent := terminated(t, details)
	closeEvent.EventId, closeEvent.EventType, closeEvent.EventTime = 9, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, timestamppb.Now()
	history = append(history[:3:3], resetFrom(4, "exec-0", "exec-1", ResumePlanKey{Scope: scope, PlanDigest: strings.Repeat("a", 64)}.String()), closeEvent)
	reader, fake := readerOver(history, runID)
	fake.describes = map[string]*workflowservice.DescribeWorkflowExecutionResponse{runID: {
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			Execution: &commonpb.WorkflowExecution{WorkflowId: runID, RunId: "exec-1"}}}}
	base := &orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", OrkflowVersion: "0.1.0", CreatedAt: timestamppb.Now(), FinishedAt: timestamppb.Now(), Status: orkstackv1.Status_STATUS_CANCELED,
		CanceledBy: "someone else", CanceledByVerified: true, ForceCancelRecord: "another record",
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED}}}
	reader = reader.WithSnapshotStore(snapshots).WithForceCancelVerifier(recordsUnreadable{keys}, mode).
		WithEndings(&fakeEndings{run: base, events: setAside(int64(scope) - 1)})
	bodies, err := artifacts.NewRunArchive(artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)

	unverified, err := NewHistoryArchiver(reader, NewRunArchiver(bodies)).ArchiveLastResort(ctx, runID)
	must.OK(t, err)
	store := bodies.(artifacts.RunEndingStore)
	body, err := store.GetEnding(ctx, runID, scope)
	must.OK(t, err)
	run, err := UnmarshalRunLine(body)
	must.OK(t, err)
	if !unverified || run.GetError().GetCode() != RunForceCancelUnverifiedCode || run.GetCanceledBy() != "" || run.GetCanceledByVerified() ||
		!strings.Contains(run.GetForceCancelRecord(), `"signed":`) || len(run.GetTaskRuns()) != 1 {
		t.Errorf("as a last resort: %v, unverified %v", run, unverified)
	}
	if _, err := store.GetEnding(ctx, runID, 0); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("the first ending was written too: %v", err)
	}

	archive := NewRunArchiver(bodies).WithForceCancelVerifier(keys, mode)
	listed, err := archive.ListRuns(ctx, ArchiveQuery{Namespace: "default", Limit: 10})
	must.OK(t, err)
	shown, err := archive.LoadRun(ctx, runID)
	must.OK(t, err)
	for where, run := range map[string][]*orkstackv1.Run{"listed": listed, "read": {shown}} {
		if len(run) != 1 || run[0].GetError().GetCode() != RunForceCancelUnverifiedCode || run[0].GetCanceledBy() != "" || run[0].GetForceCancelRecord() != "" || len(run[0].GetTaskRuns()) != 1 {
			t.Errorf("%s: %v", where, run)
		}
	}
}

// lastMessageCode reads a force-canceled ending off an event stream, which
// carries its sentence and not its code.
func lastMessageCode(events []*orkstackv1.Event) string {
	if len(events) == 0 {
		return ""
	}
	if last := events[len(events)-1]; last.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED && last.GetRequestId() == "fc-1" {
		return RunForceCanceledCode
	}
	return "RUN_TERMINATED"
}
