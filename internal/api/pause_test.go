package api_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// pauseFixture is a service holding one running, pausable run with three
// events, and a fleet whose answer the test sets.
func pauseFixture(t *testing.T, caps *fakeCapabilities) *fixture {
	t.Helper()
	f := newFixtureWith(t, func(cfg *api.Config) { cfg.Capabilities = caps })
	f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_RUNNING, Pausable: true, OrkflowName: "hello-ork"})
	for seq := int64(1); seq <= 3; seq++ {
		f.bus.Publish(&orkstackv1.Event{RunId: "ork-r", Sequence: seq, Type: orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, TaskId: "a"}, "")
	}
	return f
}

// A pause sends one request under a fresh id, with the deployment's default
// timeout, the caller and the reason, and answers that id, the sequence it
// read and the warnings.
func TestPauseRun_SendsOneRequest(t *testing.T) {
	f := pauseFixture(t, &fakeCapabilities{})
	req := connect.NewRequest(&orkstackv1.PauseRunRequest{RunId: "ork-r", Reason: "db maintenance"})
	req.Header().Set(api.AuditPrincipalHeader, "alice")
	resp, err := f.runClient().PauseRun(context.Background(), req)
	must.OKf(t, err, "PauseRun")
	f.starter.mu.Lock()
	defer f.starter.mu.Unlock()
	if len(f.starter.pauses) != 1 || f.starter.pauseRunIDs[0] != "ork-r" {
		t.Fatalf("pauses = %v to %v, want one to ork-r", f.starter.pauses, f.starter.pauseRunIDs)
	}
	sig := f.starter.pauses[0]
	if sig.GetFormat() != 1 || !sig.GetPause() || sig.GetPrincipal() != "alice" || sig.GetPrincipalVerified() ||
		sig.GetReason() != "db maintenance" || sig.GetTimeoutSeconds() != int64(engine.DefaultRunPauseTimeout/time.Second) ||
		sig.GetInherited() {
		t.Fatalf("signal = %v", sig)
	}
	if !strings.HasPrefix(sig.GetRequestId(), "pause-") || resp.Msg.GetRequestId() != sig.GetRequestId() {
		t.Fatalf("request id %q, response %q", sig.GetRequestId(), resp.Msg.GetRequestId())
	}
	if resp.Msg.GetAfterSequence() != 3 {
		t.Fatalf("after_sequence = %d, want 3", resp.Msg.GetAfterSequence())
	}
}

// An unpause sends one request with no timeout.
func TestUnpauseRun_SendsOneRequest(t *testing.T) {
	f := pauseFixture(t, &fakeCapabilities{})
	f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_PAUSED, Pausable: true,
		Pause: &orkstackv1.RunPause{PausedBy: "alice"}})
	resp, err := f.runClient().UnpauseRun(context.Background(),
		connect.NewRequest(&orkstackv1.UnpauseRunRequest{RunId: "ork-r", Reason: "done"}))
	must.OKf(t, err, "UnpauseRun")
	f.starter.mu.Lock()
	defer f.starter.mu.Unlock()
	if len(f.starter.pauses) != 1 {
		t.Fatalf("pauses = %v, want one", f.starter.pauses)
	}
	sig := f.starter.pauses[0]
	if sig.GetPause() || sig.GetTimeoutSeconds() != 0 || sig.GetReason() != "done" || sig.GetRequestId() != resp.Msg.GetRequestId() {
		t.Fatalf("signal = %v, response %v", sig, resp.Msg)
	}
}

// The caller a request names follows the mode: verified under enforce.
func TestPauseRun_CarriesAVerifiedPrincipal(t *testing.T) {
	f := pauseFixture(t, &fakeCapabilities{})
	ctx := authn.WithPrincipal(authn.WithMode(context.Background(), authn.ModeEnforce),
		&authn.Principal{Kind: authn.KindUser, ID: "user:corp:ada", Method: authn.MethodOIDC, Issuer: "corp"})
	req := connect.NewRequest(&orkstackv1.PauseRunRequest{RunId: "ork-r"})
	req.Header().Set(api.AuditPrincipalHeader, "mallory")
	_, err := f.svc.PauseRun(ctx, req)
	must.OKf(t, err, "PauseRun")
	f.starter.mu.Lock()
	defer f.starter.mu.Unlock()
	if sig := f.starter.pauses[0]; sig.GetPrincipal() != "user:corp:ada" || !sig.GetPrincipalVerified() {
		t.Fatalf("signal names %q verified %t, want the verified principal", sig.GetPrincipal(), sig.GetPrincipalVerified())
	}
}

// Every refusal is made before anything is sent.
func TestPauseRun_RefusalsSendNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		caps *fakeCapabilities
		req  *orkstackv1.PauseRunRequest
		run  *orkstackv1.Run
		code connect.Code
		says string
	}{
		{"no run id", &fakeCapabilities{}, &orkstackv1.PauseRunRequest{}, nil, connect.CodeInvalidArgument, "run_id is required"},
		{"a reason too long", &fakeCapabilities{}, &orkstackv1.PauseRunRequest{RunId: "ork-r", Reason: strings.Repeat("x", 1025)}, nil,
			connect.CodeInvalidArgument, "above the maximum of 1024"},
		{"a timeout below a second", &fakeCapabilities{}, &orkstackv1.PauseRunRequest{RunId: "ork-r", Timeout: durationpb.New(500 * time.Millisecond)}, nil,
			connect.CodeInvalidArgument, "below the minimum"},
		{"a zero timeout", &fakeCapabilities{}, &orkstackv1.PauseRunRequest{RunId: "ork-r", Timeout: durationpb.New(0)}, nil,
			connect.CodeInvalidArgument, "below the minimum"},
		{"a timeout above the maximum", &fakeCapabilities{}, &orkstackv1.PauseRunRequest{RunId: "ork-r", Timeout: durationpb.New(32 * 24 * time.Hour)}, nil,
			connect.CodeInvalidArgument, "engine.run_pause_timeout_max"},
		{"another namespace's run", &fakeCapabilities{}, &orkstackv1.PauseRunRequest{RunId: "ork-other"},
			&orkstackv1.Run{Id: "ork-other", Namespace: "payments", Status: orkstackv1.Status_STATUS_RUNNING, Pausable: true},
			connect.CodeNotFound, "not found"},
		{"an ended run", &fakeCapabilities{}, &orkstackv1.PauseRunRequest{RunId: "ork-r"},
			&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pausable: true},
			connect.CodeFailedPrecondition, "has ended"},
		{"a fleet that cannot pause", &fakeCapabilities{lacking: []string{"1@old@"}}, &orkstackv1.PauseRunRequest{RunId: "ork-r"}, nil,
			connect.CodeFailedPrecondition, "cannot pause a run; finish the rollout first"},
		{"a fleet that cannot be read", &fakeCapabilities{err: errors.New("frontend down")}, &orkstackv1.PauseRunRequest{RunId: "ork-r"}, nil,
			connect.CodeUnavailable, "could not be read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := pauseFixture(t, tc.caps)
			if tc.run != nil {
				f.runs.Save(tc.run)
			}
			_, err := f.runClient().PauseRun(context.Background(), connect.NewRequest(tc.req))
			if connect.CodeOf(err) != tc.code || !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("PauseRun = %v, want %s saying %q", err, tc.code, tc.says)
			}
			f.starter.mu.Lock()
			defer f.starter.mu.Unlock()
			if len(f.starter.pauses) != 0 {
				t.Fatalf("sent %v", f.starter.pauses)
			}
		})
	}
}

// A run that ended as the request was sent is FAILED_PRECONDITION; a
// cluster that did not answer is UNAVAILABLE.
func TestPauseRun_SendFailures(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code connect.Code
	}{
		{errors.Join(engine.ErrRunEnded, errors.New("closed")), connect.CodeFailedPrecondition},
		{errors.New("frontend down"), connect.CodeUnavailable},
	} {
		f := pauseFixture(t, &fakeCapabilities{})
		f.starter.pauseErr = tc.err
		_, err := f.runClient().PauseRun(context.Background(), connect.NewRequest(&orkstackv1.PauseRunRequest{RunId: "ork-r"}))
		if connect.CodeOf(err) != tc.code {
			t.Errorf("send error %v: PauseRun = %v, want %s", tc.err, err, tc.code)
		}
		if tc.code == connect.CodeUnavailable && !strings.Contains(err.Error(), "request pause-") {
			t.Errorf("an unanswered request does not name its id: %v", err)
		}
	}
}

// A pause holds the run's running child runs, and their own clocks keep
// running as the parent's do: a child run whose timeout ends before the pause
// does fails then, and its composed task with it. The warnings say so for
// every such child below the run, its own children included; one that cannot
// be read is named, never passed over; one already past its timeout, or ending
// after the pause, is not warned of.
func TestPauseRun_WarnsOfAChildRunWhoseTimeoutEndsFirst(t *testing.T) {
	f := pauseFixture(t, &fakeCapabilities{})
	now := time.Now()
	soon := timestamppb.New(now.Add(time.Hour).UTC().Truncate(time.Second))
	later := timestamppb.New(now.Add(engine.DefaultRunPauseTimeout + time.Hour))
	child := func(task, id string, status orkstackv1.Status) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{TaskId: task, Status: status, Child: &orkstackv1.TaskChild{RunId: id}}
	}
	f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_RUNNING, Pausable: true, OrkflowName: "hello-ork",
		TaskRuns: []*orkstackv1.TaskRun{
			child("build", "ork-c", orkstackv1.Status_STATUS_RUNNING),
			child("slow", "ork-s", orkstackv1.Status_STATUS_RUNNING),
			child("gone", "ork-missing", orkstackv1.Status_STATUS_RUNNING),
			child("done", "ork-d", orkstackv1.Status_STATUS_SUCCEEDED),
		}})
	f.runs.Save(&orkstackv1.Run{Id: "ork-c", Status: orkstackv1.Status_STATUS_RUNNING, OrkflowName: "build",
		Timeout:  &orkstackv1.RunTimeout{Deadline: soon},
		TaskRuns: []*orkstackv1.TaskRun{child("inner", "ork-g", orkstackv1.Status_STATUS_RUNNING)}})
	f.runs.Save(&orkstackv1.Run{Id: "ork-g", Status: orkstackv1.Status_STATUS_RUNNING, OrkflowName: "inner",
		Timeout: &orkstackv1.RunTimeout{Deadline: soon}})
	f.runs.Save(&orkstackv1.Run{Id: "ork-s", Status: orkstackv1.Status_STATUS_RUNNING, OrkflowName: "slow",
		Timeout: &orkstackv1.RunTimeout{Deadline: later}})
	f.runs.Save(&orkstackv1.Run{Id: "ork-d", Status: orkstackv1.Status_STATUS_SUCCEEDED, OrkflowName: "done",
		Timeout: &orkstackv1.RunTimeout{Deadline: soon}})

	resp, err := f.runClient().PauseRun(context.Background(), connect.NewRequest(&orkstackv1.PauseRunRequest{RunId: "ork-r", DryRun: true}))
	must.OKf(t, err, "PauseRun dry run")
	got := strings.Join(resp.Msg.GetWarnings(), "\n")
	at := soon.AsTime().Format(time.RFC3339)
	for _, want := range []string{
		"child run ork-c (composed task build)'s timeout ends at " + at + ", before this pause does; it will then fail, and composed task build with it",
		"child run ork-g (composed task inner of child run ork-c)'s timeout ends at " + at + ", before this pause does; it will then fail, and composed task inner with it",
		"child run ork-missing (composed task gone) could not be read, so its timeout was not checked",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("warnings\n%s\nlack %q", got, want)
		}
	}
	for _, not := range []string{"child run ork-s ", "child run ork-d "} {
		if strings.Contains(got, not) {
			t.Errorf("warns of %s, whose timeout ends after the pause or which has ended:\n%s", not, got)
		}
	}
}

// A dry run makes the checks and answers the warnings, and sends nothing.
func TestPauseRun_DryRunSendsNothing(t *testing.T) {
	f := pauseFixture(t, &fakeCapabilities{})
	f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_RUNNING, Pausable: true,
		Error: &orkstackv1.Error{Code: "X"},
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "h", Status: orkstackv1.Status_STATUS_PENDING,
			Join: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED}}})
	resp, err := f.runClient().PauseRun(context.Background(), connect.NewRequest(&orkstackv1.PauseRunRequest{RunId: "ork-r", DryRun: true}))
	must.OKf(t, err, "PauseRun dry run")
	if resp.Msg.GetRequestId() != "" || len(resp.Msg.GetWarnings()) == 0 || !strings.Contains(resp.Msg.GetWarnings()[0], "the run is failing") {
		t.Fatalf("dry run answered %v", resp.Msg)
	}
	f.starter.mu.Lock()
	defer f.starter.mu.Unlock()
	if len(f.starter.pauses) != 0 {
		t.Fatalf("a dry run sent %v", f.starter.pauses)
	}
	f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pausable: true})
	if _, err := f.runClient().PauseRun(context.Background(), connect.NewRequest(&orkstackv1.PauseRunRequest{RunId: "ork-r", DryRun: true})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("a dry run of an ended run = %v, want the refusal", err)
	}
}
