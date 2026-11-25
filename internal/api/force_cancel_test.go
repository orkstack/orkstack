package api_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// fakeForceCancels answers every request for a force cancel as it is set to.
type fakeForceCancels struct {
	asks   []engine.ForceCancelAsk
	answer engine.ForceCancelAnswer
	err    error
	status engine.ForceCancelStatus
	read   []string
	below  engine.FCSubtree
	// belowErr and stopErr are Below's and Stop's answers.
	belowErr, stopErr error
	stopped           []string
	listed            []string
}

func (f *fakeForceCancels) Stop(_ context.Context, namespace, runID string) (string, error) {
	f.stopped = append(f.stopped, namespace+"/"+runID)
	return "fc-1", f.stopErr
}

func (f *fakeForceCancels) Below(_ context.Context, namespace, runID string) (engine.FCSubtree, error) {
	f.listed = append(f.listed, namespace+"/"+runID)
	return f.below, f.belowErr
}

func (f *fakeForceCancels) Request(_ context.Context, ask engine.ForceCancelAsk) (engine.ForceCancelAnswer, error) {
	f.asks = append(f.asks, ask)
	return f.answer, f.err
}

func (f *fakeForceCancels) Status(_ context.Context, namespace, runID, requestID string) (engine.ForceCancelStatus, error) {
	f.read = append(f.read, namespace+"/"+runID+"/"+requestID)
	return f.status, f.err
}

// forceCancelFixture is a service holding one running run of hello-ork.
func forceCancelFixture(t *testing.T, fc *fakeForceCancels) *fixture {
	t.Helper()
	f := newFixtureWith(t, func(cfg *api.Config) {
		if fc != nil {
			cfg.ForceCancels = fc
		}
	})
	saveForceCancelRun(f, orkstackv1.Status_STATUS_RUNNING, "")
	return f
}

func saveForceCancelRun(f *fixture, status orkstackv1.Status, code string) {
	run := &orkstackv1.Run{Id: "ork-r", Status: status, OrkflowName: "hello-ork"}
	if code != "" {
		run.Error = &orkstackv1.Error{Code: code}
	}
	f.runs.Save(run)
}

// TestForceCancelRun_Asks: the engine is asked for exactly what was requested,
// by whoever the mode says is asking, and what it did is answered.
func TestForceCancelRun_Asks(t *testing.T) {
	verified := &authn.Principal{Kind: authn.KindUser, ID: "user:corp:ada", Method: authn.MethodOIDC, Issuer: "corp"}
	for _, tt := range []struct {
		name, mode, header string
		principal          *authn.Principal
		want               engine.ForceCancelAsk
	}{
		{"asserted", authn.ModeAsserted, "dana@acme", verified, engine.ForceCancelAsk{Principal: "dana@acme"}},
		{"asserted, nobody", authn.ModeAsserted, "", nil, engine.ForceCancelAsk{Principal: "anonymous"}},
		{"enforce", authn.ModeEnforce, "mallory", verified, engine.ForceCancelAsk{Principal: "user:corp:ada", PrincipalVerified: true, AuthMethod: authn.MethodOIDC, PrincipalIssuer: "corp"}},
		{"observe, nobody", authn.ModeObserve, "mallory", nil, engine.ForceCancelAsk{Principal: "anonymous"}},
		{"a principal too long is cut", authn.ModeAsserted, strings.Repeat("x", 400), nil, engine.ForceCancelAsk{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fc := &fakeForceCancels{answer: engine.ForceCancelAnswer{State: engine.FCStateRequested, RequestID: "fc-1",
				Decision: engine.ForceCancelDecision{Principal: "p", PrincipalVerified: true, Reason: "why", GraceSeconds: 90}}}
			f := forceCancelFixture(t, fc)
			ctx := authn.WithMode(context.Background(), tt.mode)
			if tt.principal != nil {
				ctx = authn.WithPrincipal(ctx, tt.principal)
			}
			req := connect.NewRequest(&orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Reason: "stuck", Grace: durationpb.New(2 * time.Minute), ClientRequestId: "mine"})
			req.Header().Set(api.AuditPrincipalHeader, tt.header)
			resp, err := f.svc.ForceCancelRun(ctx, req)
			must.OK(t, err)
			want := tt.want
			want.Namespace, want.RunID, want.Reason, want.Grace, want.ClientRequestID = "default", "ork-r", "stuck", 2*time.Minute, "mine"
			if want.Principal == "" {
				want.Principal = fc.asks[0].Principal
				if len(want.Principal) > 256 {
					t.Errorf("the principal is %d bytes", len(want.Principal))
				}
			}
			if len(fc.asks) != 1 || fc.asks[0] != want {
				t.Fatalf("asked %+v, want %+v", fc.asks, want)
			}
			out := resp.Msg
			if out.GetState() != orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_REQUESTED || out.GetRequestId() != "fc-1" || out.GetPrincipal() != "p" ||
				!out.GetPrincipalVerified() || out.GetReason() != "why" || out.GetGrace().AsDuration() != 90*time.Second {
				t.Errorf("answered %v", out)
			}
		})
	}
}

// TestForceCancelRun_Answers: what the engine did, as the caller reads it.
func TestForceCancelRun_Answers(t *testing.T) {
	requested, waiting := orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_REQUESTED, orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_WAITING
	over := orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_ALREADY_ENDED
	canceled, succeeded := orkstackv1.Status_STATUS_CANCELED, orkstackv1.Status_STATUS_SUCCEEDED
	for _, tt := range []struct {
		name   string
		status orkstackv1.Status
		code   string
		answer engine.ForceCancelAnswer
		err    error
		asked  bool
		want   *orkstackv1.ForceCancelRunResponse
	}{
		{"joined", 0, "", engine.ForceCancelAnswer{State: engine.FCStateWaiting, Attached: true, Squatters: 2, JoinSkipped: true}, nil, true,
			&orkstackv1.ForceCancelRunResponse{State: waiting, Attached: true, AttachUnverified: 2, AttachUnknown: true}},
		{"ended here", 0, "", engine.ForceCancelAnswer{State: engine.FCStateRequested, Direct: true}, nil, true, &orkstackv1.ForceCancelRunResponse{State: requested, Direct: true}},
		{"its own attempt, whose run has ended", 0, "", engine.ForceCancelAnswer{State: engine.FCStateAlreadyEnded, Attached: true}, nil, true, &orkstackv1.ForceCancelRunResponse{State: over, Attached: true}},
		{"a run that succeeded", succeeded, "", engine.ForceCancelAnswer{}, nil, false, &orkstackv1.ForceCancelRunResponse{State: over}},
		{"joined to one that cascades", 0, "", engine.ForceCancelAnswer{State: engine.FCStateWaiting, Attached: true, Decision: engine.ForceCancelDecision{Cascade: true}}, nil, true,
			&orkstackv1.ForceCancelRunResponse{State: waiting, Attached: true, Cascades: true}},
		{"a run that canceled itself", canceled, "RUN_CANCELED", engine.ForceCancelAnswer{}, nil, false, &orkstackv1.ForceCancelRunResponse{State: over}},
		{"a force-canceled run is finished", canceled, "RUN_FORCE_CANCELED", engine.ForceCancelAnswer{State: engine.FCStateRequested}, nil, true, &orkstackv1.ForceCancelRunResponse{State: requested}},
		{"one that could not be verified is finished", canceled, "RUN_FORCE_CANCEL_UNVERIFIED", engine.ForceCancelAnswer{State: engine.FCStateRequested}, nil, true, &orkstackv1.ForceCancelRunResponse{State: requested}},
		{"a run terminated by nobody's force cancel", canceled, "RUN_TERMINATED", engine.ForceCancelAnswer{}, nil, false, &orkstackv1.ForceCancelRunResponse{State: over}},
		{"one gone from the cluster is done", canceled, "RUN_FORCE_CANCELED", engine.ForceCancelAnswer{}, engine.ErrRunNotFound, true, &orkstackv1.ForceCancelRunResponse{State: over}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fc := &fakeForceCancels{answer: tt.answer, err: tt.err}
			f := forceCancelFixture(t, fc)
			if tt.status != 0 {
				saveForceCancelRun(f, tt.status, tt.code)
			}
			resp, err := f.runClient().ForceCancelRun(context.Background(), connect.NewRequest(&orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Reason: "stuck"}))
			must.OK(t, err)
			if resp.Msg.String() != tt.want.String() || (len(fc.asks) == 1) != tt.asked {
				t.Fatalf("answered %v, asked %d; want %v, asked %t", resp.Msg, len(fc.asks), tt.want, tt.asked)
			}
		})
	}
}

// TestForceCancelRun_Refusals: a refusal has its code, and its message begins
// with its reason where it has one.
func TestForceCancelRun_Refusals(t *testing.T) {
	ask := func() *orkstackv1.ForceCancelRunRequest {
		return &orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Reason: "stuck"}
	}
	for _, tt := range []struct {
		name string
		req  *orkstackv1.ForceCancelRunRequest
		err  error
		code connect.Code
		want string
	}{
		{"no run", &orkstackv1.ForceCancelRunRequest{Reason: "stuck"}, nil, connect.CodeInvalidArgument, "run_id is required"},
		{"a run nobody has", &orkstackv1.ForceCancelRunRequest{RunId: "ork-x", Reason: "stuck"}, nil, connect.CodeNotFound, "not found"},
		{"a run of another namespace", &orkstackv1.ForceCancelRunRequest{RunId: "ork-elsewhere", Reason: "stuck"}, nil, connect.CodeNotFound, ""},
		{"a grace of zero", &orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Reason: "stuck", Grace: durationpb.New(0)}, nil, connect.CodeInvalidArgument, "FORCE_CANCEL_GRACE_TOO_SHORT: "},
		{"a grace below zero", &orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Reason: "stuck", Grace: durationpb.New(-time.Second)}, nil, connect.CodeInvalidArgument, "FORCE_CANCEL_GRACE_TOO_SHORT: "},
		{"a grace no one can read", &orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Reason: "stuck", Grace: &durationpb.Duration{Seconds: 1, Nanos: -1}}, nil, connect.CodeInvalidArgument, "grace: "},
		{"too short", ask(), engine.ErrForceCancelGraceTooShort, connect.CodeInvalidArgument, "FORCE_CANCEL_GRACE_TOO_SHORT: "},
		{"too long", ask(), engine.ErrForceCancelGraceTooLong, connect.CodeInvalidArgument, "FORCE_CANCEL_GRACE_TOO_LONG: "},
		{"a record too large", ask(), engine.ErrForceCancelRecordTooLarge, connect.CodeInvalidArgument, "FORCE_CANCEL_REASON_TOO_LONG: "},
		{"text", ask(), fmt.Errorf("%w: reason", engine.ErrForceCancelText), connect.CodeInvalidArgument, ""},
		{"no reason", &orkstackv1.ForceCancelRunRequest{RunId: "ork-over"}, nil, connect.CodeInvalidArgument, "reason"},
		{"every attempt spent", ask(), engine.ErrForceCancelRetriesExhausted, connect.CodeFailedPrecondition, "FORCE_CANCEL_RETRIES_EXHAUSTED: "},
		{"preview and stop", &orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Preview: true, Stop: true}, nil, connect.CodeInvalidArgument, "two requests"},
		{"a cluster that indexes no origins", ask(), engine.ErrForceCancelNoIndex, connect.CodeFailedPrecondition, ""},
		{"gone between the reads", ask(), engine.ErrRunNotFound, connect.CodeNotFound, ""},
		{"resumed since", ask(), engine.ErrForceCancelStale, connect.CodeAborted, "make the request again"},
		{"keys not read", ask(), identity.ErrKeysNotRead, connect.CodeUnavailable, ""},
		{"a close nobody can verify now", ask(), engine.ErrForceCancelUnverifiable, connect.CodeUnavailable, "ask again"},
		{"keys stale", ask(), identity.ErrKeysStale, connect.CodeUnavailable, ""},
		{"a replica with no key", ask(), engine.ErrForceCancelNoKey, connect.CodeUnavailable, "no_signing_key"},
		{"a replica that cannot sign", ask(), identity.ErrCannotSign, connect.CodeUnavailable, ""},
		{"a cluster that does not answer", ask(), serviceerror.NewUnavailable("down"), connect.CodeUnavailable, ""},
		{"anything else", ask(), errors.New("what"), connect.CodeInternal, "what"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := forceCancelFixture(t, &fakeForceCancels{err: tt.err})
			f.runs.Save(&orkstackv1.Run{Id: "ork-over", Status: orkstackv1.Status_STATUS_SUCCEEDED})
			f.runs.Save(&orkstackv1.Run{Id: "ork-elsewhere", Namespace: "other", Status: orkstackv1.Status_STATUS_RUNNING})
			_, err := f.runClient().ForceCancelRun(context.Background(), connect.NewRequest(tt.req))
			if connect.CodeOf(err) != tt.code || !strings.Contains(fmt.Sprint(err), tt.want) {
				t.Fatalf("%v, want %s %q", err, tt.code, tt.want)
			}
		})
	}
	t.Run("not configured", func(t *testing.T) {
		f := forceCancelFixture(t, nil)
		_, err := f.runClient().ForceCancelRun(context.Background(), connect.NewRequest(ask()))
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("%v", err)
		}
	})
}

// TestAudit_AForceCancel: the trail holds who asked and what became of it.
func TestAudit_AForceCancel(t *testing.T) {
	for _, tt := range []struct {
		name   string
		answer engine.ForceCancelAnswer
		err    error
		action string
		ok     bool
	}{
		{"started", engine.ForceCancelAnswer{State: engine.FCStateRequested, RequestID: "fc-1"}, nil, "run.cancel.force", true},
		{"joined", engine.ForceCancelAnswer{State: engine.FCStateWaiting, RequestID: "fc-1", Attached: true}, nil, "run.cancel.force.attached", true},
		{"ended here", engine.ForceCancelAnswer{State: engine.FCStateRequested, RequestID: "fc-1", Direct: true}, nil, "run.cancel.force.direct", true},
		{"every attempt spent", engine.ForceCancelAnswer{}, engine.ErrForceCancelRetriesExhausted, "run.cancel.force", false},
		{"refused", engine.ForceCancelAnswer{}, engine.ErrForceCancelGraceTooShort, "run.cancel.force", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newAuditFixture(t, auditOptions{forceCancels: &fakeForceCancels{answer: tt.answer, err: tt.err}})
			f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_RUNNING, OrkflowName: "hello-ork"})
			runs := orkstackv1connect.NewRunServiceClient(f.server.Client(), f.server.URL)
			_, _ = runs.ForceCancelRun(context.Background(), connect.NewRequest(&orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Reason: "stuck"}))
			got := f.records(t)
			if len(got) != 1 || got[0].GetAction() != tt.action || got[0].GetSucceeded() != tt.ok || got[0].GetTarget() != "ork-r" || !strings.HasSuffix(got[0].GetDetail(), `reason="stuck"`) ||
				strings.Contains(got[0].GetDetail(), "request_id=fc-1") != (tt.answer.RequestID != "") {
				t.Fatalf("recorded %v, want one %s", got, tt.action)
			}
		})
	}
	for action, req := range map[string]*orkstackv1.ForceCancelRunRequest{"": {RunId: "ork-r", Preview: true}, "run.cancel.force.stopped": {RunId: "ork-r", Stop: true}} {
		f := newAuditFixture(t, auditOptions{forceCancels: &fakeForceCancels{}})
		f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_RUNNING, OrkflowName: "hello-ork"})
		_, err := orkstackv1connect.NewRunServiceClient(f.server.Client(), f.server.URL).ForceCancelRun(context.Background(), connect.NewRequest(req))
		must.OK(t, err)
		got := f.records(t)
		if action == "" && len(got) != 0 || action != "" && (len(got) != 1 || got[0].GetAction() != action || !strings.Contains(got[0].GetDetail(), "request_id=fc-1")) {
			t.Errorf("%v recorded %v", req, got)
		}
	}
}

// TestGetForceCancel: what a force cancel reports of itself, of a run of the
// request's namespace.
func TestGetForceCancel(t *testing.T) {
	started := time.Date(2026, 3, 14, 11, 2, 44, 0, time.UTC)
	fc := &fakeForceCancels{status: engine.ForceCancelStatus{StartedAt: started, State: engine.FCStateForceCanceled, Cause: "event_unpublished", Incomplete: true, Unaudited: true,
		Decision: engine.ForceCancelDecision{RequestID: "fc-1", Principal: "p", PrincipalVerified: true, Reason: "why", GraceSeconds: 90}}}
	f := forceCancelFixture(t, fc)
	resp, err := f.runClient().GetForceCancel(context.Background(), connect.NewRequest(&orkstackv1.GetForceCancelRequest{RunId: "ork-r", RequestId: "fc-1"}))
	must.OK(t, err)
	want := &orkstackv1.GetForceCancelResponse{RequestId: "fc-1", State: orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_FORCE_CANCELED, Cause: "event_unpublished",
		StartedAt: timestamppb.New(started), Principal: "p", PrincipalVerified: true, Reason: "why", Grace: durationpb.New(90 * time.Second), Incomplete: true, Unaudited: true, OpenDescendants: &orkstackv1.ForceCancelSubtree{}}
	if resp.Msg.String() != want.String() || len(fc.read) != 1 || fc.read[0] != "default/ork-r/fc-1" {
		t.Fatalf("read %v: %v, want %v", fc.read, resp.Msg, want)
	}
	for _, tt := range []struct {
		req  *orkstackv1.GetForceCancelRequest
		err  error
		code connect.Code
	}{
		{&orkstackv1.GetForceCancelRequest{}, nil, connect.CodeInvalidArgument},
		{&orkstackv1.GetForceCancelRequest{RunId: "ork-x"}, nil, connect.CodeNotFound},
		{&orkstackv1.GetForceCancelRequest{RunId: "ork-r"}, engine.ErrForceCancelNotFound, connect.CodeNotFound},
		{&orkstackv1.GetForceCancelRequest{RunId: "ork-r"}, serviceerror.NewUnavailable("down"), connect.CodeUnavailable},
	} {
		fc.err = tt.err
		if _, err := f.runClient().GetForceCancel(context.Background(), connect.NewRequest(tt.req)); connect.CodeOf(err) != tt.code {
			t.Errorf("%v, %v = %v, want %s", tt.req, tt.err, err, tt.code)
		}
	}
	if _, err := forceCancelFixture(t, nil).runClient().GetForceCancel(context.Background(), connect.NewRequest(&orkstackv1.GetForceCancelRequest{RunId: "ork-r"})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("not configured = %v", err)
	}
}

// TestForceCancelRun_Cascade: a request that cascades is asked as one, of an
// ended run too; a preview lists and does nothing, and a stop only stops.
func TestForceCancelRun_Cascade(t *testing.T) {
	ctx := context.Background()
	fc := &fakeForceCancels{answer: engine.ForceCancelAnswer{State: engine.FCStateRequested}, below: engine.FCSubtree{Truncated: "scan", Excluded: map[string]string{"z": engine.FCExcludedNamespace},
		Open: []engine.FCBelow{{RunID: "a", ParentRunID: "ork-r", Orkflow: "billing/pay", Depth: 1}}}}
	f := forceCancelFixture(t, fc)
	for _, status := range []orkstackv1.Status{orkstackv1.Status_STATUS_RUNNING, orkstackv1.Status_STATUS_SUCCEEDED} {
		saveForceCancelRun(f, status, "")
		fc.asks = nil
		_, err := f.runClient().ForceCancelRun(ctx, connect.NewRequest(&orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Reason: "stuck", Cascade: true}))
		if err != nil || len(fc.asks) != 1 || !fc.asks[0].Cascade {
			t.Errorf("%s: %v, asked %+v", status, err, fc.asks)
		}
	}
	fc.err = engine.ErrRunNotFound
	if _, err := f.runClient().ForceCancelRun(ctx, connect.NewRequest(&orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Reason: "stuck", Cascade: true})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("of a run gone from the cluster: %v", err)
	}
	fc.asks, fc.err = nil, nil
	resp, err := f.runClient().ForceCancelRun(ctx, connect.NewRequest(&orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Preview: true}))
	must.OK(t, err)
	want := &orkstackv1.ForceCancelSubtree{TruncatedBy: "scan", Exclusions: []*orkstackv1.ForceCancelExclusion{{Cause: "other_namespace"}},
		Runs: []*orkstackv1.ForceCancelRunBelow{{RunId: "a", ParentRunId: "ork-r", Orkflow: "billing/pay", Depth: 1}}}
	if resp.Msg.GetOpenDescendants().String() != want.String() || len(fc.asks) != 0 || len(fc.stopped) != 0 || len(fc.listed) != 1 || fc.listed[0] != "default/ork-r" {
		t.Errorf("%v; asked %v, stopped %v, listed %v", resp.Msg, fc.asks, fc.stopped, fc.listed)
	}
	stop := connect.NewRequest(&orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Stop: true})
	resp, err = f.runClient().ForceCancelRun(ctx, stop)
	if err != nil || resp.Msg.GetRequestId() != "fc-1" || len(fc.asks) != 0 || len(fc.stopped) != 1 || fc.stopped[0] != "default/ork-r" {
		t.Errorf("%v, %v; stopped %v", resp, err, fc.stopped)
	}
	for _, tt := range []struct {
		name string
		req  *orkstackv1.ForceCancelRunRequest
		fc   *fakeForceCancels
		code connect.Code
	}{
		{"nothing to stop", stop.Msg, &fakeForceCancels{stopErr: engine.ErrForceCancelNotFound}, connect.CodeFailedPrecondition},
		{"a stop the cluster did not take", stop.Msg, &fakeForceCancels{stopErr: serviceerror.NewUnavailable("down")}, connect.CodeUnavailable},
		{"a list that cannot be read", &orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Preview: true}, &fakeForceCancels{belowErr: serviceerror.NewUnavailable("down")}, connect.CodeUnavailable},
		{"a run nobody has", &orkstackv1.ForceCancelRunRequest{RunId: "ork-x", Preview: true}, &fakeForceCancels{}, connect.CodeNotFound},
		{"not configured", stop.Msg, nil, connect.CodeFailedPrecondition},
	} {
		if _, err := forceCancelFixture(t, tt.fc).runClient().ForceCancelRun(ctx, connect.NewRequest(tt.req)); connect.CodeOf(err) != tt.code {
			t.Errorf("%s: %v", tt.name, err)
		}
	}
}

// TestGetForceCancel_Cascade: what a cascade reports, and what is open below a
// run ended without one.
func TestGetForceCancel_Cascade(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 3, 14, 11, 2, 44, 0, time.UTC)
	row := func(id, state string) engine.FCBelowResult {
		return engine.FCBelowResult{FCBelow: engine.FCBelow{RunID: id, Orkflow: "pay", Depth: 1}, State: state, RequestID: "fc-1-" + id, Cause: "why"}
	}
	cascade := &engine.ForceCancelCascade{State: engine.FCCascadePartial, AsOf: at, Covered: 2, NotStarted: 3, ByRoot: 1, Truncated: "scan", Rerun: true, Cause: "could_not_read", Unread: true, Excluded: map[string]string{"z": "unreadable"},
		After:   &engine.FCAfter{Count: 4, RunIDs: []string{"a", "q"}, Unconfirmed: engine.FCCauseTooManyRuns},
		Results: []engine.FCBelowResult{row("a", engine.FCStateForceCanceled), row("b", engine.FCBelowWaiting), row("c", engine.FCBelowAttached), row("d", engine.FCBelowPending), row("e", engine.FCStateFailed)}}
	fc := &fakeForceCancels{status: engine.ForceCancelStatus{State: engine.FCStateForceCanceled, Cascade: cascade}}
	f := forceCancelFixture(t, fc)
	get := func() *orkstackv1.GetForceCancelResponse {
		t.Helper()
		resp, err := f.runClient().GetForceCancel(ctx, connect.NewRequest(&orkstackv1.GetForceCancelRequest{RunId: "ork-r"}))
		must.OK(t, err)
		return resp.Msg
	}
	got := get().GetCascade()
	const prefix = "FORCE_CANCEL_STATE_"
	var states []string
	for _, r := range got.GetRuns() {
		states = append(states, r.GetRunId()+" "+strings.TrimPrefix(r.GetState().String(), prefix))
		if r.GetRequestId() != "fc-1-"+r.GetRunId() || r.GetCause() != "why" {
			t.Errorf("%v", r)
		}
	}
	if want := []string{"a FORCE_CANCELED", "b REQUESTED", "c WAITING", "d PENDING", "e FAILED"}; !slices.Equal(states, want) {
		t.Errorf("%v, want %v", states, want)
	}
	if got.GetState() != orkstackv1.ForceCancelCascadeState_FORCE_CANCEL_CASCADE_STATE_PARTIAL || !got.GetAsOf().AsTime().Equal(at) || got.GetLeftToParents() != 2 || got.GetNotStarted() != 3 || got.GetFoundByRootIndex() != 1 ||
		got.GetTruncatedBy() != "scan" || !got.GetRerunSuggested() || got.GetCause() != "could_not_read" || !got.GetUnread() || len(got.GetExclusions()) != 1 || got.GetOpenAfter().GetCount() != 4 || !slices.Equal(got.GetOpenAfter().GetRunIds(), []string{"a", "q"}) || got.GetOpenAfter().GetUnconfirmedCause() != "too_many_runs" {
		t.Errorf("%v", got)
	}
	if len(fc.listed) != 0 {
		t.Errorf("listed %v", fc.listed)
	}
	fc.status.Cascade, fc.below = nil, engine.FCSubtree{Open: []engine.FCBelow{{RunID: "a"}}}
	if got := get(); got.GetCascade() != nil || len(got.GetOpenDescendants().GetRuns()) != 1 {
		t.Errorf("%v", got)
	}
	fc.belowErr = errors.New("down")
	if got := get(); got.GetState() != orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_FORCE_CANCELED || got.GetOpenDescendants().GetTruncatedBy() != "could_not_read" {
		t.Errorf("a list that fails hides the rest: %v", got)
	}
	fc.status.State, fc.listed = engine.FCStateRequested, nil
	if get(); len(fc.listed) != 0 {
		t.Error("one that has not ended its run lists what is below")
	}
}
