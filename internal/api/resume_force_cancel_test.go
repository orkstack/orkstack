package api_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// forceCancelKeys are one engine's signing keys, published: what a force
// cancel's record is signed and verified with.
func forceCancelKeys(t *testing.T) *identity.Issuer {
	t.Helper()
	store, err := artifacts.NewPublishedKeysStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jwt.NewSigner(k)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := identity.New(identity.Config{Keys: []*jwt.Signer{signer}, Algorithm: jwt.ES256, ClockSkew: time.Hour, Engine: "engine-a"}, store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := issuer.Publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	return issuer
}

// forceCanceled makes the fixture's run one a force cancel ended: CANCELED
// RUN_FORCE_CANCELED, its execution terminated with the record that ended it,
// as signed by keys (or by record, when it is given).
func forceCanceled(t *testing.T, keys *identity.Issuer, record func(*engine.ForceCancelRecord)) func(*resumeFixture) {
	return forceCanceledFrom(t, keys, record, failedHistory(t, "a", "b"))
}

// forceCanceledFrom is forceCanceled of a run whose history, up to its close,
// is history's.
func forceCanceledFrom(t *testing.T, keys *identity.Issuer, record func(*engine.ForceCancelRecord), history []*historypb.HistoryEvent) func(*resumeFixture) {
	return func(f *resumeFixture) {
		f.run.Status, f.run.Error = orkstackv1.Status_STATUS_CANCELED, &orkstackv1.Error{Code: engine.RunForceCanceledCode}
		for _, row := range f.run.TaskRuns {
			if row.GetTaskId() == "b" {
				row.Status = orkstackv1.Status_STATUS_CANCELED
			}
		}
		rec := engine.ForceCancelRecord{Namespace: "default", RunID: "ork-r", TerminatedExecutionRunID: "exec-1", RequestID: "fc-1",
			WorkflowID: engine.ForceCancelWorkflowIDPrefix("default", "ork-r") + "fc-1", Principal: "user:corp:alice", PrincipalVerified: true,
			Permission: "run.cancel.force", Reason: "stuck", GraceSeconds: 60, At: time.Now().UTC().Add(-11 * time.Minute)}
		if record != nil {
			record(&rec)
		}
		details, err := engine.SignForceCancelRecord(keys, rec)
		if err != nil {
			t.Fatal(err)
		}
		closing := history[len(history)-1]
		// Closed ten minutes ago, past what its containers may take to stop,
		// under keys published now that allow an hour of clock skew.
		closedAt := time.Now().UTC().Add(-10 * time.Minute)
		f.clock.set(time.Now().UTC())
		closing.EventType, closing.EventTime = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, timestamppb.New(closedAt)
		closing.Attributes = &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
			Reason: engine.ForceCancelTerminateReason, Details: details}}
		f.cluster.histories["exec-1"] = history
		base := f.cluster.executions["exec-1"]
		base.Status, base.CloseTime = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, closedAt
		f.cluster.executions[""], f.cluster.executions["exec-1"] = base, base
		// Its events end with that force cancel's ending, at the run's last
		// sequence, as every reader builds it.
		f.stream = []*orkstackv1.Event{{RunId: "ork-r", Sequence: 2, Type: orkstackv1.EventType_EVENT_TYPE_RUN_STARTED},
			{RunId: "ork-r", Sequence: 9, Type: orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, RequestId: rec.RequestID}}
		f.configure = func(cfg *api.Config) {
			cfg.ResumeForceCancels, cfg.ResumeAuthMode = keys, func() string { return "enforce" }
		}
	}
}

// TestResumeRun_AForceCanceledRun: a force-cancelled run is resumed from a
// task the person chooses (G-a), and only while its close carries a record
// that verifies for this run and this execution (design §28.12 F2); one that
// does not is a termination nothing resumes.
func TestResumeRun_AForceCanceledRun(t *testing.T) {
	keys := forceCancelKeys(t)
	other := forceCancelKeys(t)
	for name, tc := range map[string]struct {
		keys   *identity.Issuer
		record func(*engine.ForceCancelRecord)
		closed enumspb.WorkflowExecutionStatus
		from   string
		reason string
		says   string // what of the refusal tells which check it was
	}{
		"from a task":                       {keys, nil, 0, "b", "", ""},
		"from no task":                      {keys, nil, 0, "", "FROM_TASK_REQUIRED", ""},
		"a record of another run":           {keys, func(r *engine.ForceCancelRecord) { r.RunID = "ork-z" }, 0, "b", "STATE_NOT_RESUMABLE", "no force cancel's record that verifies"},
		"a record of another execution":     {keys, func(r *engine.ForceCancelRecord) { r.TerminatedExecutionRunID = "exec-0" }, 0, "b", "STATE_NOT_RESUMABLE", "no force cancel's record that verifies"},
		"a record under a key not read yet": {other, nil, 0, "b", "RESUME_CHECK_UNAVAILABLE", "can't be checked right now"},
		"events that go on":                 {keys, nil, 0, "b", "RESUME_CHECK_UNAVAILABLE", "events can't be read right now"},
		"events that are gone":              {keys, nil, 0, "b", "HISTORY_UNAVAILABLE", "no longer kept"},
		"an ending not in its events yet":   {keys, nil, 0, "b", "STILL_STOPPING", "has not finished its ending"},
		"an execution that ended otherwise": {keys, nil, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, "b", "STATE_NOT_RESUMABLE", "did not end the way its record says"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, func(f *resumeFixture) {
				forceCanceled(t, tc.keys, tc.record)(f)
				switch name {
				case "an ending not in its events yet":
					f.stream = f.stream[:1]
				case "events that go on":
					// A reset since opened the run again: its stream does not
					// end, and is not waited on past a bound.
					f.stream, f.streamGoesOn = f.stream[:1], true
					quickConfirm(f)
				case "events that are gone":
					f.streamGone = true
				}
				if tc.closed != 0 {
					base := f.cluster.executions["exec-1"]
					base.Status = tc.closed
					f.cluster.executions[""], f.cluster.executions["exec-1"] = base, base
				}
				signed := f.configure
				f.configure = func(cfg *api.Config) { signed(cfg); cfg.ResumeForceCancels = keys }
			})
			got, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true, FromTaskId: tc.from})
			if tc.reason == "" {
				if err != nil {
					t.Fatalf("preview = %v", err)
				}
				if got.GetTerminalSequence() == 0 || len(got.GetWarnings()) == 0 ||
					got.GetWarnings()[0].GetKind() != orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_FORCE_CANCELED_SAME_BUILD {
					t.Fatalf("preview = %v; want it warned that it replays on the same build", got)
				}
				return
			}
			if r := refusalOf(t, err); r.GetReason() != tc.reason || !strings.Contains(r.GetMessage(), tc.says) {
				t.Fatalf("refusal = %v, want %s saying %q", r, tc.reason, tc.says)
			}
		})
	}
}

// TestResumeRun_AForceCanceledRunsOpenChildren: runs a force-cancelled run
// left open below it are ended with a force cancel of its own, which reaches
// every one, and the refusal says so (design §23.4).
func TestResumeRun_AForceCanceledRunsOpenChildren(t *testing.T) {
	keys := forceCancelKeys(t)
	events, _ := childHistory(t, []string{"ork-child"})
	f := newResumeFixture(t, func(f *resumeFixture) {
		forceCanceledFrom(t, keys, nil, events)(f)
		f.cluster.children = map[string]engine.ResumeExecution{"ork-child": {Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, StartTime: time.Now()}}
	})
	_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true, FromTaskId: "b"})
	if r := refusalOf(t, err); r.GetReason() != "STILL_STOPPING" || r.GetCause() != "child_runs_open" ||
		!strings.Contains(r.GetMessage(), "orkctl run cancel --force --cascade ork-r") {
		t.Fatalf("refusal = %v", r)
	}
}

// TestResumeRun_AForceCanceledRunObserved: a request to resume a
// force-cancelled run is counted as one whatever refused it, its base's own
// checks included, and a failed run's is not.
func TestResumeRun_AForceCanceledRunObserved(t *testing.T) {
	keys := forceCancelKeys(t)
	for name, tc := range map[string]struct {
		forced bool
		ending bool // its force cancel's ending is in its events
		reason string
	}{
		"a force-cancelled run whose ending is not in its events": {true, false, "STILL_STOPPING"},
		"a force-cancelled run that changed":                      {true, true, "RUN_CHANGED"},
		"a failed run that changed":                               {false, false, "RUN_CHANGED"},
	} {
		t.Run(name, func(t *testing.T) {
			o := &resumeObserver{}
			f := newResumeFixture(t, func(f *resumeFixture) {
				if tc.forced {
					forceCanceled(t, keys, nil)(f)
					if !tc.ending {
						f.stream = f.stream[:1]
					}
				}
				inner := f.configure
				observedBy(f, o)
				observed := f.configure
				f.configure = func(cfg *api.Config) {
					if inner != nil {
						inner(cfg)
					}
					observed(cfg)
				}
			})
			seen := int64(3) // a run that changed since it was seen
			if tc.reason == "STILL_STOPPING" {
				seen = 9
			}
			_, err := f.confirm(t, func(r *orkstackv1.ResumeRunRequest) { r.FromTaskId, r.ExpectedTerminalSequence = "b", seen })
			if r := refusalOf(t, err); r.GetReason() != tc.reason {
				t.Fatalf("refusal = %v, want %s", r, tc.reason)
			}
			if len(o.requests) != 1 || o.requests[0].ForceCanceled != tc.forced || o.requests[0].Refusal != tc.reason {
				t.Fatalf("requests = %+v, want one %s counted force-cancelled %v", o.requests, tc.reason, tc.forced)
			}
		})
	}
}

// TestResumeRun_AForceCanceledRunNamesWhoEndedIt: a confirmed resume of a
// force-cancelled run says who force-cancelled it, as its verified record
// names them, and which request that was; a failed run's says nothing.
func TestResumeRun_AForceCanceledRunNamesWhoEndedIt(t *testing.T) {
	keys := forceCancelKeys(t)
	for name, forced := range map[string]bool{"force-cancelled": true, "failed": false} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, func(f *resumeFixture) {
				if forced {
					forceCanceled(t, keys, nil)(f)
				}
			})
			f.resets(true)
			req := &orkstackv1.ResumeRunRequest{RunId: "ork-r", FromTaskId: "b", ExpectedTerminalSequence: 9, Reason: "flaky network"}
			got, err := f.resume(req)
			if err != nil {
				t.Fatal(err)
			}
			detail := api.ResumeAuditDetailForTest(req, got)
			if forced != strings.Contains(detail, `from_state=force_canceled force_cancel_request="fc-1"`) {
				t.Fatalf("audit detail = %q; force-cancelled %v", detail, forced)
			}
			by := got.GetResume().GetForceCanceledBy()
			if !forced {
				if by != nil {
					t.Fatalf("a failed run's resume names %v", by)
				}
				return
			}
			if by.GetPrincipal() != "user:corp:alice" || !by.GetPrincipalVerified() || by.GetRequestId() != "fc-1" {
				t.Fatalf("force_canceled_by = %v, want alice, verified, request fc-1", by)
			}
		})
	}
}

// TestResumeRun_AForceCanceledRunsCallerGoesAway: a caller that goes away
// while the run's events end without its force cancel's ending is answered
// with its own end, not told the force cancel has not finished.
func TestResumeRun_AForceCanceledRunsCallerGoesAway(t *testing.T) {
	keys := forceCancelKeys(t)
	f := newResumeFixture(t, func(f *resumeFixture) {
		forceCanceled(t, keys, nil)(f)
		f.stream = f.stream[:1]
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.runReader.onStream = cancel
	_, err := f.svc.ResumeRun(ctx, connect.NewRequest(&orkstackv1.ResumeRunRequest{RunId: "ork-r", DryRun: true, FromTaskId: "b"}))
	if connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("resume = %v, want the caller's own cancel", err)
	}
}
