package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

// fakeResumeService answers ResumeRun from a script and records every
// request. The engine's ResumeRun is not what is under test here (it is not
// served yet): only how the CLI previews, asks, confirms and exits.
type fakeResumeService struct {
	orkstackv1connect.UnimplementedRunServiceHandler

	mu         sync.Mutex
	requests   []*orkstackv1.ResumeRunRequest
	preview    *orkstackv1.ResumeRunResponse
	previewErr error
	confirm    *orkstackv1.ResumeRunResponse
	confirmErr error
	final      *orkstackv1.Run // GetRun's answer for --wait
}

func (f *fakeResumeService) ResumeRun(
	_ context.Context, req *connect.Request[orkstackv1.ResumeRunRequest],
) (*connect.Response[orkstackv1.ResumeRunResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req.Msg)
	if req.Msg.GetDryRun() {
		if f.previewErr != nil {
			return nil, f.previewErr
		}
		return connect.NewResponse(f.preview), nil
	}
	if f.confirmErr != nil {
		return nil, f.confirmErr
	}
	return connect.NewResponse(f.confirm), nil
}

func (f *fakeResumeService) StreamEvents(
	context.Context, *connect.Request[orkstackv1.StreamEventsRequest], *connect.ServerStream[orkstackv1.Event],
) error {
	return nil
}

func (f *fakeResumeService) GetRun(
	context.Context, *connect.Request[orkstackv1.GetRunRequest],
) (*connect.Response[orkstackv1.GetRunResponse], error) {
	return connect.NewResponse(&orkstackv1.GetRunResponse{Run: f.final}), nil
}

func (f *fakeResumeService) all() []*orkstackv1.ResumeRunRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*orkstackv1.ResumeRunRequest(nil), f.requests...)
}

func (f *fakeResumeService) confirms() []*orkstackv1.ResumeRunRequest {
	var out []*orkstackv1.ResumeRunRequest
	for _, r := range f.all() {
		if !r.GetDryRun() {
			out = append(out, r)
		}
	}
	return out
}

func startResumeServer(t *testing.T, svc *fakeResumeService) string {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	path, h := orkstackv1connect.NewRunServiceHandler(svc)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	server := httptest.NewUnstartedServer(mux)
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetHTTP1(true)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	t.Cleanup(server.Close)
	return server.URL
}

func itemIndex(i int32) *int32 { return &i }

// resumePlan is a preview: a failed task, its downstream that succeeded
// before (so a confirm must be agreed to), a kept task and a fan-out item.
func resumePlan() *orkstackv1.ResumeRunResponse {
	return &orkstackv1.ResumeRunResponse{
		RunId: "ork-r", PlanDigest: "digest-1", TerminalSequence: 41, HighestSequence: 41, SeqBase: 43,
		Action: orkstackv1.ResumeAction_RESUME_ACTION_RESUME, ResumesUsed: 2,
		Tasks: []*orkstackv1.ResumeTask{
			{TaskId: "build", Category: orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_FAILED,
				PreviousOutcome: orkstackv1.Status_STATUS_FAILED},
			{TaskId: "publish", Category: orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_DOWNSTREAM,
				PreviousOutcome: orkstackv1.Status_STATUS_SUCCEEDED},
			{TaskId: "fetch", Category: orkstackv1.ResumeCategory_RESUME_CATEGORY_KEPT,
				PreviousOutcome: orkstackv1.Status_STATUS_SUCCEEDED},
			{TaskId: "convert", Item: itemIndex(3), Category: orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_UNFINISHED,
				PreviousOutcome: orkstackv1.Status_STATUS_CANCELED},
		},
	}
}

func confirmedPlan() *orkstackv1.ResumeRunResponse {
	p := resumePlan()
	p.Resume = &orkstackv1.RunResume{Sequence: 43}
	return p
}

func resumeRefused(t *testing.T, r *orkstackv1.ResumeRefusal) error {
	t.Helper()
	err := connect.NewError(connect.CodeFailedPrecondition, errors.New("run can't be resumed"))
	detail, derr := connect.NewErrorDetail(r)
	if derr != nil {
		t.Fatal(derr)
	}
	err.AddDetail(detail)
	return err
}

func TestRunResume_DryRunShowsThePlanAndChangesNothing(t *testing.T) {
	svc := &fakeResumeService{preview: resumePlan()}
	url := startResumeServer(t, svc)
	got := runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--dry-run").requireExit(t, exitOK)
	got.requireContains(t,
		"Resume: run ork-r",
		"You asked for these (2): build, publish",
		"Added because the resume must (1): convert[3]",
		"Kept (1): fetch",
		"resumes used: 2/20")
	if n := len(svc.confirms()); n != 0 {
		t.Fatalf("a dry run sent %d confirms", n)
	}
}

// TestRunResume_ThePreviewSaysWhatItKnowsOfATask: a task's detail is shown
// under its section, as many as its names are.
func TestRunResume_ThePreviewSaysWhatItKnowsOfATask(t *testing.T) {
	plan := resumePlan()
	for _, task := range plan.Tasks {
		if task.TaskId == "fetch" {
			task.Detail = "to be read back: an earlier resume did not read its result"
		}
	}
	for i := range int32(30) {
		plan.Tasks = append(plan.Tasks, &orkstackv1.ResumeTask{TaskId: "each", Item: &i,
			Category: orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_RESULT_GONE, Detail: "its values.json is gone"})
	}
	url := startResumeServer(t, &fakeResumeService{preview: plan})
	got := runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--dry-run").requireExit(t, exitOK)
	got.requireContains(t,
		"Kept (1): fetch\n    fetch: to be read back: an earlier resume did not read its result\n",
		"    each[0]: its values.json is gone\n",
		"    and 10 more\n")
}

func TestRunResume_DryRunJSONIsThePreview(t *testing.T) {
	svc := &fakeResumeService{preview: resumePlan()}
	url := startResumeServer(t, svc)
	got := runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--dry-run", "--json").requireExit(t, exitOK)
	var out map[string]any
	if err := json.Unmarshal([]byte(got.stdout), &out); err != nil {
		t.Fatalf("--json --dry-run is not one JSON document: %v\n%s", err, got.stdout)
	}
	if out["plan_digest"] != "digest-1" && out["planDigest"] != "digest-1" {
		t.Errorf("the preview's plan digest is missing: %s", got.stdout)
	}
}

func TestRunResume_RepeatingSucceededWorkNeedsAnAnswer(t *testing.T) {
	t.Run("no terminal and no --yes", func(t *testing.T) {
		svc := &fakeResumeService{preview: resumePlan(), confirm: confirmedPlan()}
		url := startResumeServer(t, svc)
		runConfirming(t, url, strings.NewReader(""), false, "run", "resume", "ork-r").
			requireExit(t, exitError).requireStderrContains(t, "standard input is not a terminal")
		if n := len(svc.confirms()); n != 0 {
			t.Fatalf("sent %d confirms without an answer", n)
		}
	})
	t.Run("answered no", func(t *testing.T) {
		svc := &fakeResumeService{preview: resumePlan(), confirm: confirmedPlan()}
		url := startResumeServer(t, svc)
		runConfirming(t, url, strings.NewReader("no\n"), true, "run", "resume", "ork-r").
			requireExit(t, exitError).requireStderrContains(t, "runs 1 task(s) again that succeeded before", "cancelled")
		if n := len(svc.confirms()); n != 0 {
			t.Fatalf("sent %d confirms after a no", n)
		}
	})
	t.Run("answered yes", func(t *testing.T) {
		svc := &fakeResumeService{preview: resumePlan(), confirm: confirmedPlan()}
		url := startResumeServer(t, svc)
		runConfirming(t, url, strings.NewReader("yes\n"), true, "run", "resume", "ork-r").requireExit(t, exitOK)
		if n := len(svc.confirms()); n != 1 {
			t.Fatalf("sent %d confirms, want 1", n)
		}
	})
	t.Run("--json never asks", func(t *testing.T) {
		svc := &fakeResumeService{preview: resumePlan(), confirm: confirmedPlan()}
		url := startResumeServer(t, svc)
		runConfirming(t, url, strings.NewReader("yes\n"), true, "run", "resume", "ork-r", "--json").
			requireExit(t, exitError).requireStderrContains(t, "--json never asks")
		if n := len(svc.confirms()); n != 0 {
			t.Fatalf("--json sent %d confirms without --yes", n)
		}
	})
	t.Run("nothing that succeeded reruns", func(t *testing.T) {
		plan := resumePlan()
		plan.Tasks = plan.Tasks[:1]
		svc := &fakeResumeService{preview: plan, confirm: confirmedPlan()}
		url := startResumeServer(t, svc)
		runConfirming(t, url, strings.NewReader(""), false, "run", "resume", "ork-r").requireExit(t, exitOK)
	})
}

func TestRunResume_TheConfirmCarriesWhatThePreviewShowed(t *testing.T) {
	svc := &fakeResumeService{preview: resumePlan(), confirm: confirmedPlan()}
	url := startResumeServer(t, svc)
	runCLIWithInput(t, url, strings.NewReader(""),
		"run", "resume", "ork-r", "--from", "fetch", "--reason", "flaky disk", "--yes", "-n", "payments").
		requireExit(t, exitOK).requireContains(t, "Resumed run ork-r (RUN_RESUMED at event 43)")
	all := svc.all()
	if len(all) != 2 || !all[0].GetDryRun() || all[0].GetFromTaskId() != "fetch" {
		t.Fatalf("requests = %v, want a preview of the same ask, then a confirm", all)
	}
	c := all[1]
	if c.GetDryRun() || c.GetExpectedTerminalSequence() != 41 || c.GetExpectedPlanDigest() != "digest-1" ||
		c.GetFromTaskId() != "fetch" || c.GetReason() != "flaky disk" || c.GetNamespace() != "payments" ||
		c.GetAcceptSequenceGap() || c.GetAcceptedHighestSeq() != 0 || len(c.GetAcknowledgedUnknownAttempts()) != 0 {
		t.Errorf("confirm = %v", c)
	}
}

func TestRunResume_RefusalsExitByClass(t *testing.T) {
	cases := []struct {
		name     string
		refusal  *orkstackv1.ResumeRefusal
		code     int
		contains []string
	}{
		{"permanent", &orkstackv1.ResumeRefusal{Reason: "COMPOSED_TASK",
			Class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT, Message: "task c runs another Orkflow"},
			exitError, []string{"task c runs another Orkflow", "(COMPOSED_TASK)"}},
		{"wait", &orkstackv1.ResumeRefusal{Reason: "STILL_STOPPING",
			Class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT, RetryAfterSeconds: 12, Message: "containers may still be stopping"},
			exitResumeWait, []string{"containers may still be stopping", "try again in 12 s"}},
		{"replan", &orkstackv1.ResumeRefusal{Reason: "PLAN_CHANGED",
			Class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_REPLAN, Message: "what would rerun has changed",
			Diff: &orkstackv1.ResumeDiff{Added: []*orkstackv1.ResumeUnitRef{{TaskId: "lint"}},
				Dropped: []*orkstackv1.ResumeUnitRef{{TaskId: "convert", Item: itemIndex(2)}}}},
			exitResumeReplan, []string{"now also lint", "no longer convert[2]"}},
		{"unconfirmed, the reset's outcome unknown", &orkstackv1.ResumeRefusal{Reason: resumeUnconfirmedReason, Cause: "reset_outcome_unknown",
			Class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT, RetryAfterSeconds: 90, Message: "it will be settled by 12:01"},
			exitResumeUnconfirmed, []string{"not known yet whether it was made", "it will be settled by 12:01", "try again in 90 s"}},
		{"unconfirmed, resumed and not carried on", &orkstackv1.ResumeRefusal{Reason: resumeUnconfirmedReason, Cause: "not_carried_on",
			Class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT, RetryAfterSeconds: 30, Message: "run ork-r was resumed"},
			exitResumeUnconfirmed, []string{"was resumed and has not carried on yet"}},
		{"in progress", &orkstackv1.ResumeRefusal{Reason: "RESUME_IN_PROGRESS",
			Class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT, RetryAfterSeconds: 120, Message: "another resume of this run is being made"},
			exitResumeWait, []string{"another resume of this run is being made", "try again in 120 s"}},
	}
	for _, c := range cases {
		t.Run(c.name+" at confirm", func(t *testing.T) {
			svc := &fakeResumeService{preview: resumePlan(), confirmErr: resumeRefused(t, c.refusal)}
			url := startResumeServer(t, svc)
			runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--yes").
				requireExit(t, c.code).requireStderrContains(t, c.contains...)
		})
		t.Run(c.name+" at preview", func(t *testing.T) {
			svc := &fakeResumeService{previewErr: resumeRefused(t, c.refusal)}
			url := startResumeServer(t, svc)
			runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--yes").
				requireExit(t, c.code).requireStderrContains(t, c.contains...)
			if n := len(svc.confirms()); n != 0 {
				t.Fatalf("a refused preview was followed by %d confirms", n)
			}
		})
	}
}

func TestRunResume_UnknownAttemptsNeedTheirOwnAcknowledgement(t *testing.T) {
	plan := resumePlan()
	plan.Warnings = []*orkstackv1.ResumeWarning{
		{Kind: orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_ATTEMPT_OUTCOME_UNKNOWN,
			Message: "attempt 2 of build on orker-7 may still be running", AcknowledgeKey: "orker-7/build//2"},
		{Kind: orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_WAS_PAUSED, Message: "it was paused"},
	}
	svc := &fakeResumeService{preview: plan, confirm: confirmedPlan()}
	url := startResumeServer(t, svc)
	runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--yes").
		requireExit(t, exitResumeReplan).requireStderrContains(t, "--acknowledge-unknown-attempts", "Note: attempt 2 of build")
	if n := len(svc.confirms()); n != 0 {
		t.Fatalf("--yes alone sent %d confirms", n)
	}
	runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--yes", "--acknowledge-unknown-attempts").
		requireExit(t, exitOK)
	c := svc.confirms()
	if len(c) != 1 || strings.Join(c[0].GetAcknowledgedUnknownAttempts(), ",") != "orker-7/build//2" ||
		c[0].GetExpectedPlanDigest() == "" {
		t.Fatalf("confirm = %v, want the shown key with the plan digest", c)
	}
}

// While resume is switched off the engine answers with a sentence and no
// refusal detail: the command shows the sentence, and it is never the exit of
// a resume whose outcome is unknown.
func TestRunResume_AnEngineWithResumeSwitchedOff(t *testing.T) {
	off := connect.NewError(connect.CodeFailedPrecondition, errors.New("this engine resumes no runs"))
	for name, args := range map[string][]string{
		"preview": {"run", "resume", "ork-r", "--dry-run"},
		"resume":  {"run", "resume", "ork-r", "--yes"},
	} {
		t.Run(name, func(t *testing.T) {
			svc := &fakeResumeService{previewErr: off, confirmErr: off}
			url := startResumeServer(t, svc)
			runCLIWithInput(t, url, strings.NewReader(""), args...).
				requireExit(t, exitError).requireStderrContains(t, "this engine resumes no runs")
			if n := len(svc.confirms()); n != 0 {
				t.Fatalf("%d confirms sent, want none", n)
			}
		})
	}
}

func TestRunResume_AnEngineThatCannotResume(t *testing.T) {
	svc := &fakeResumeService{previewErr: connect.NewError(connect.CodeUnimplemented, errors.New("not served"))}
	url := startResumeServer(t, svc)
	runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r").
		requireExit(t, exitError).requireStderrContains(t, "this engine cannot resume runs")
}

func TestRunResume_ReasonIsBounded(t *testing.T) {
	svc := &fakeResumeService{preview: resumePlan()}
	url := startResumeServer(t, svc)
	runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--reason", strings.Repeat("x", 1025)).
		requireExit(t, exitError).requireStderrContains(t, "at most 1024")
	if n := len(svc.all()); n != 0 {
		t.Fatalf("an over-long reason was sent (%d requests)", n)
	}
}

func TestRunResume_WaitExitsByHowTheRunEnded(t *testing.T) {
	cases := []struct {
		name string
		run  *orkstackv1.Run
		code int
	}{
		{"succeeded", &orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_SUCCEEDED}, exitOK},
		{"failed", &orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_FAILED,
			Resumes: []*orkstackv1.RunResume{{Sequence: 43, Ended: &orkstackv1.RunResumeEnded{Status: orkstackv1.Status_STATUS_FAILED}}}}, exitRunFailed},
		{"canceled", &orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_CANCELED}, exitRunCanceled},
		{"ended before anything reran", &orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_FAILED,
			Resumes: []*orkstackv1.RunResume{
				{Sequence: 12, Ended: &orkstackv1.RunResumeEnded{BeforeAnythingReran: false}},
				{Sequence: 43, Ended: &orkstackv1.RunResumeEnded{Status: orkstackv1.Status_STATUS_FAILED, BeforeAnythingReran: true}},
			}}, exitResumeEndedEarly},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			svc := &fakeResumeService{preview: resumePlan(), confirm: confirmedPlan(), final: c.run}
			url := startResumeServer(t, svc)
			runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--yes", "--wait").requireExit(t, c.code)
		})
	}
}

// slowAnswer answers the prompt only after after has passed, as a person
// reading the plan would.
func slowAnswer(after time.Duration) io.Reader {
	r, w := io.Pipe()
	time.AfterFunc(after, func() {
		_, _ = w.Write([]byte("yes\n"))
		_ = w.Close()
	})
	return r
}

func TestRunResume_TheConfirmGetsItsOwnDeadline(t *testing.T) {
	svc := &fakeResumeService{preview: resumePlan(), confirm: confirmedPlan()}
	url := startResumeServer(t, svc)
	// The answer comes after the whole --timeout has passed: a deadline held
	// across the question would have expired before the confirm was sent.
	runConfirming(t, url, slowAnswer(1500*time.Millisecond), true, "run", "resume", "ork-r", "--timeout", "1s").
		requireExit(t, exitOK)
	if n := len(svc.confirms()); n != 1 {
		t.Fatalf("confirms = %d, want 1", n)
	}
}

// The gap a caller accepts is the event the refusal named, sent with the
// preview and the confirm alike, so both are about the same events.
func TestRunResume_TheAcceptedGapIsSentWithBoth(t *testing.T) {
	svc := &fakeResumeService{preview: resumePlan(), confirm: confirmedPlan()}
	url := startResumeServer(t, svc)
	runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--yes", "--accept-sequence-gap", "57").requireExit(t, exitOK)
	all := svc.all()
	if len(all) != 2 {
		t.Fatalf("%d requests, want the preview and the confirm", len(all))
	}
	for _, r := range all {
		if !r.GetAcceptSequenceGap() || r.GetAcceptedHighestSeq() != 57 || r.GetAcceptUnverifiedStop() {
			t.Errorf("request = %v, want the gap up to 57 accepted", r)
		}
	}
}

func TestRunResume_AGapIsAcceptedOnlyByItsEvent(t *testing.T) {
	svc := &fakeResumeService{preview: resumePlan(), confirm: confirmedPlan()}
	url := startResumeServer(t, svc)
	runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--accept-sequence-gap", "0").
		requireExit(t, exitError).requireStderrContains(t, "the highest event the refusal named")
	if n := len(svc.all()); n != 0 {
		t.Fatalf("%d requests sent, want none", n)
	}
}

func TestRunResume_APreviewWithNoPlanIsNeverConfirmed(t *testing.T) {
	for name, edit := range map[string]func(*orkstackv1.ResumeRunResponse){
		"no digest":            func(p *orkstackv1.ResumeRunResponse) { p.PlanDigest = "" },
		"no terminal sequence": func(p *orkstackv1.ResumeRunResponse) { p.TerminalSequence = 0 },
		"an unkeyed unknown attempt": func(p *orkstackv1.ResumeRunResponse) {
			p.Warnings = []*orkstackv1.ResumeWarning{{Kind: orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_ATTEMPT_OUTCOME_UNKNOWN}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			plan := resumePlan()
			edit(plan)
			svc := &fakeResumeService{preview: plan, confirm: confirmedPlan()}
			url := startResumeServer(t, svc)
			runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--yes", "--acknowledge-unknown-attempts").
				requireExit(t, exitError).requireStderrContains(t, "nothing was resumed")
			if n := len(svc.confirms()); n != 0 {
				t.Fatalf("sent %d confirms", n)
			}
		})
	}
}

func TestRunResume_AConfirmWhoseAnswerIsLostExits4(t *testing.T) {
	for _, code := range []connect.Code{connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeUnknown} {
		t.Run(code.String(), func(t *testing.T) {
			svc := &fakeResumeService{preview: resumePlan(), confirmErr: connect.NewError(code, errors.New("lost"))}
			url := startResumeServer(t, svc)
			runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--yes").
				requireExit(t, exitResumeUnconfirmed).requireStderrContains(t, "may have been sent")
		})
	}
	svc := &fakeResumeService{previewErr: connect.NewError(connect.CodeUnavailable, errors.New("down"))}
	url := startResumeServer(t, svc)
	runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--yes").requireExit(t, exitError)
}

func TestRunResume_EveryCategoryIsShown(t *testing.T) {
	plan := resumePlan()
	plan.Tasks = nil
	for v := range orkstackv1.ResumeCategory_name {
		c := orkstackv1.ResumeCategory(v)
		if c == orkstackv1.ResumeCategory_RESUME_CATEGORY_UNSPECIFIED {
			continue
		}
		if sectionOf(c) == otherSection {
			t.Errorf("category %v is in no section", c)
		}
		plan.Tasks = append(plan.Tasks, &orkstackv1.ResumeTask{TaskId: strings.ToLower(c.String())})
		plan.Tasks[len(plan.Tasks)-1].Category = c
	}
	plan.Tasks = append(plan.Tasks, &orkstackv1.ResumeTask{TaskId: "from-a-newer-engine", Category: 99})
	svc := &fakeResumeService{preview: plan}
	url := startResumeServer(t, svc)
	got := runCLIWithInput(t, url, strings.NewReader(""), "run", "resume", "ork-r", "--dry-run").requireExit(t, exitOK)
	for _, task := range plan.Tasks {
		got.requireContains(t, task.TaskId)
	}
	got.requireContains(t, "Other (1): from-a-newer-engine")
}
