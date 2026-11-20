package orker

import (
	"context"
	"errors"
	"testing"
	"time"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/orkstack/orkstack/internal/task"
)

// respond runs one report call through the Orker's report interceptor, as the
// SDK's client sends it after an attempt returns, with the cluster answering
// answer.
func respond(t *testing.T, a *Activities, method string, req any, answer error) {
	t.Helper()
	err := a.ReportInterceptor()(context.Background(), method, req, nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return answer })
	if !errors.Is(err, answer) && status.Code(err) != status.Code(answer) {
		t.Fatalf("the interceptor answered %v; want the cluster's own answer %v", err, answer)
	}
}

// drained reports whether every result an attempt returned has reached the
// cluster, waiting at most a moment.
func drained(a *Activities) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	return a.AwaitReports(ctx) == nil
}

// An attempt that returns has not yet told the cluster: the SDK sends its
// result after the activity function returns. Until that report is answered,
// the stopping Orker must not count itself drained, or it exits with the
// result unsent and the task runs again elsewhere (Train 15 / P13b proof
// 44342096120, TestIntegration_AStoppingOrkerFinishesItsTaskWithinTheGrace).
func TestOwedReports_AnAttemptsResultIsOwedUntilTheClusterAnswersIt(t *testing.T) {
	cases := []struct {
		name    string
		method  string
		request func(token []byte) any
		answer  error
		settled bool
	}{
		{"completed and taken", workflowservice.WorkflowService_RespondActivityTaskCompleted_FullMethodName,
			func(tok []byte) any { return &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: tok} }, nil, true},
		{"failed and taken", workflowservice.WorkflowService_RespondActivityTaskFailed_FullMethodName,
			func(tok []byte) any { return &workflowservice.RespondActivityTaskFailedRequest{TaskToken: tok} }, nil, true},
		{"canceled and taken", workflowservice.WorkflowService_RespondActivityTaskCanceled_FullMethodName,
			func(tok []byte) any { return &workflowservice.RespondActivityTaskCanceledRequest{TaskToken: tok} }, nil, true},
		{"the cluster has nothing to take", workflowservice.WorkflowService_RespondActivityTaskCompleted_FullMethodName,
			func(tok []byte) any { return &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: tok} },
			status.Error(codes.NotFound, "activity already timed out"), true},
		{"the cluster did not answer: the SDK tries again", workflowservice.WorkflowService_RespondActivityTaskCompleted_FullMethodName,
			func(tok []byte) any { return &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: tok} },
			status.Error(codes.Unavailable, "connection refused"), false},
		{"the cluster refused it for now: the SDK may send it again", workflowservice.WorkflowService_RespondActivityTaskCompleted_FullMethodName,
			func(tok []byte) any { return &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: tok} },
			status.Error(codes.FailedPrecondition, "namespace handover"), false},
		{"another attempt's report", workflowservice.WorkflowService_RespondActivityTaskCompleted_FullMethodName,
			func([]byte) any {
				return &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: []byte("other")}
			}, nil, false},
		{"not a report", workflowservice.WorkflowService_RecordActivityTaskHeartbeat_FullMethodName,
			func(tok []byte) any { return &workflowservice.RecordActivityTaskHeartbeatRequest{TaskToken: tok} }, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newCordonActivities(t, t.TempDir())
			env := activityEnv(a)
			var token []byte
			// A failure report is for an attempt that failed: one made of a
			// success is never sent (TestOwedReports_AFailureForASucceededAttemptIsNotSent).
			fails := tc.method == workflowservice.WorkflowService_RespondActivityTaskFailed_FullMethodName
			env.RegisterActivityWithOptions(func(ctx context.Context) error {
				token = activity.GetInfo(ctx).TaskToken
				if fails {
					return errors.New("exit 1")
				}
				return nil
			}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
			if _, err := env.ExecuteActivity(task.ActivityExecuteTask); (err != nil) != fails {
				t.Fatalf("the attempt returned %v; want an error only when it fails", err)
			}
			if len(token) == 0 {
				t.Fatal("the test environment gave the attempt no task token")
			}
			if a.RunningTasks() != 0 {
				t.Fatalf("running_tasks = %d after the attempt returned; want 0", a.RunningTasks())
			}
			if drained(a) {
				t.Fatal("the Orker counted itself drained while the attempt's result was still unsent")
			}
			respond(t, a, tc.method, tc.request(token), tc.answer)
			if got := drained(a); got != tc.settled {
				t.Fatalf("drained = %v after the report; want %v", got, tc.settled)
			}
		})
	}
}

// An attempt that reports itself (a hand-back's CompleteActivity) returns
// ErrResultPending to the SDK, which then sends nothing: its report is not
// owed by the SDK, and AwaitHandedBack already waits for it.
func TestOwedReports_AnAttemptThatReportsItselfOwesNothing(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	env := activityEnv(a)
	env.RegisterActivityWithOptions(func(context.Context) error {
		return activity.ErrResultPending
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	_, _ = env.ExecuteActivity(task.ActivityExecuteTask)
	if !drained(a) {
		t.Fatal("an attempt that reported itself left a report owed")
	}
}

// Nothing owed is drained at once, even on a context already done, so an
// idle Orker's stop waits for nothing.
func TestOwedReports_NothingOwedIsDrainedAtOnce(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.AwaitReports(ctx); err != nil {
		t.Fatalf("AwaitReports with nothing owed = %v; want nil", err)
	}
}

// A panic is reported by the SDK as a failure, so its result is owed too.
func TestOwedReports_APanickingAttemptOwesItsFailure(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	env := activityEnv(a)
	env.RegisterActivityWithOptions(func(context.Context) error {
		panic("the workload's driver crashed")
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	_, _ = env.ExecuteActivity(task.ActivityExecuteTask)
	if drained(a) {
		t.Fatal("a panicking attempt's failure was not owed, though the SDK reports it")
	}
}

// An attempt whose deadline passed gets no report from the SDK (the cluster
// has already timed it out), so nothing is owed for it.
func TestOwedReports_AnAttemptPastItsDeadlineOwesNothing(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	a.oweReport(ctx, activity.Info{TaskToken: []byte("late")}, ctx.Err(), false)
	if !drained(a) {
		t.Fatal("an attempt past its deadline left a report owed")
	}
	a.oweReport(context.Background(), activity.Info{TaskToken: []byte("on time")}, nil, true)
	if drained(a) {
		t.Fatal("an attempt within its deadline owed nothing")
	}
}

// A report the SDK never sends, or gives up on, lapses after ReportDrainBound:
// it never holds a stop longer, and it is counted, so the stop can say so.
func TestOwedReports_AnUnansweredReportLapses(t *testing.T) {
	now := time.Date(2026, 11, 18, 16, 24, 11, 0, time.UTC)
	r := &owedReports{now: func() time.Time { return now }}
	r.owe([]byte("a"), "run/work#1", false)
	if labels, _, _ := r.pending(); len(labels) != 1 {
		t.Fatalf("pending = %v; want one", labels)
	}
	now = now.Add(ReportDrainBound - time.Nanosecond)
	if labels, _, _ := r.pending(); len(labels) != 1 {
		t.Fatalf("pending = %v just before the bound; want one", labels)
	}
	now = now.Add(time.Nanosecond)
	if err := r.await(context.Background()); err != nil {
		t.Fatalf("await after the bound = %v; want nil", err)
	}
	if r.lapsedTotal != 1 || len(r.lapsed) != 1 || r.lapsed[0] != "run/work#1" {
		t.Fatalf("lapsed = %d %v; want one, run/work#1", r.lapsedTotal, r.lapsed)
	}
	r.settle([]byte("a"))
	if r.lapsedTotal != 1 {
		t.Fatal("a late answer to a lapsed report changed the count")
	}
}

// A pin's creation activity is reported when its pin closes, so it is owed
// as an attempt is.
func TestOwedReports_AClosedPinsCreationIsOwed(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	env := activityEnv(a)
	env.RegisterActivityWithOptions(func(context.Context) error { return nil },
		activity.RegisterOptions{Name: sessionCreationActivityName})
	if _, err := env.ExecuteActivity(sessionCreationActivityName); err != nil {
		t.Fatalf("the pin's creation failed: %v", err)
	}
	if drained(a) {
		t.Fatal("a closed pin's creation result was not owed, though the SDK reports it")
	}
}

// sent runs one report call through the interceptor and reports whether it
// reached the cluster, and what the interceptor answered the SDK.
func sent(a *Activities, method string, req any) (bool, error) {
	reached := false
	err := a.ReportInterceptor()(context.Background(), method, req, nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			reached = true
			return nil
		})
	return reached, err
}

// The SDK sends an attempt's result on a context its worker's Stop cancels,
// and turns a send cut short into a failure ("context canceled"). A failure
// report for an attempt that returned success is that conversion, never the
// attempt's own outcome: it is not sent, so the cluster times the attempt out
// and its re-dispatch adopts the commit.json the attempt wrote before it
// returned (AGENTS.md §4.1). Its result stays owed until it lapses. A failure
// an attempt returned itself is sent as ever.
func TestOwedReports_AFailureForASucceededAttemptIsNotSent(t *testing.T) {
	failed := workflowservice.WorkflowService_RespondActivityTaskFailed_FullMethodName
	for _, tc := range []struct {
		name     string
		returned error
		method   string
		wantSent bool
	}{
		{"a success turned into a failure", nil, failed, false},
		{"a success reported as a success", nil, workflowservice.WorkflowService_RespondActivityTaskCompleted_FullMethodName, true},
		{"the attempt's own failure", errors.New("exit 1"), failed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newCordonActivities(t, t.TempDir())
			a.oweReport(context.Background(), activity.Info{TaskToken: []byte("tok")}, tc.returned, tc.returned == nil)
			reached, err := sent(a, tc.method, cutShortFailure())
			if reached != tc.wantSent {
				t.Fatalf("reached the cluster = %v (interceptor answered %v); want %v", reached, err, tc.wantSent)
			}
			if !tc.wantSent && (err == nil || drained(a)) {
				t.Fatalf("a refused send answered %v and left drained=%v; want an error to the SDK and the result still owed", err, drained(a))
			}
		})
	}
}

// The bound passes first: a success still unsent when ReportDrainBound lapses
// is no longer owed, so the stop goes on and stops the worker, and the
// failure the SDK then makes of it is still not sent.
func TestOwedReports_ASuccessUnsentPastTheBoundIsStillNeverReportedFailed(t *testing.T) {
	now := time.Date(2026, 11, 20, 16, 0, 0, 0, time.UTC)
	a := newCordonActivities(t, t.TempDir())
	a.reports.now = func() time.Time { return now }
	a.oweReport(context.Background(), activity.Info{TaskToken: []byte("tok")}, nil, true)
	now = now.Add(ReportDrainBound)
	if !drained(a) {
		t.Fatal("a result past ReportDrainBound still held the stop")
	}
	failed := workflowservice.WorkflowService_RespondActivityTaskFailed_FullMethodName
	if reached, err := sent(a, failed, cutShortFailure()); reached || err == nil {
		t.Fatalf("after the bound, a failure for a succeeded attempt reached the cluster = %v (answered %v); want it kept back", reached, err)
	}
	if lapsed, _ := a.LapsedReports(); lapsed != 1 {
		t.Fatalf("lapsed = %d; want the unsent success counted", lapsed)
	}
}

// cutShortFailure is the report the SDK makes of a success whose send its
// worker's Stop cut short.
func cutShortFailure() *workflowservice.RespondActivityTaskFailedRequest {
	return &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken: []byte("tok"),
		Failure:   &failurepb.Failure{Message: context.Canceled.Error()},
	}
}

// Only the SDK's "context canceled" conversion is kept back: any other failure
// it makes of a success (a result over the payload limit) is sent, with its
// own reason, and an attempt that panicked, or a pin's creation, is never
// remembered as a success, so its failure is sent.
func TestOwedReports_OnlyACutShortSuccessIsKeptBack(t *testing.T) {
	failed := workflowservice.WorkflowService_RespondActivityTaskFailed_FullMethodName
	oversize := &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken: []byte("tok"),
		Failure:   &failurepb.Failure{Message: "payload size 3000000 exceeds the limit"},
	}
	t.Run("another conversion of a success", func(t *testing.T) {
		a := newCordonActivities(t, t.TempDir())
		a.oweReport(context.Background(), activity.Info{TaskToken: []byte("tok")}, nil, true)
		if reached, err := sent(a, failed, oversize); !reached {
			t.Fatalf("a payload-limit failure of a success was kept back (%v); want it sent with its reason", err)
		}
	})
	for _, kind := range []struct {
		name     string
		activity string
		run      func(context.Context) error
	}{
		{"a task that panicked", task.ActivityExecuteTask, func(context.Context) error { panic("dispatcher bug") }},
		{"a pin's creation", sessionCreationActivityName, func(context.Context) error { return nil }},
	} {
		t.Run(kind.name, func(t *testing.T) {
			a := newCordonActivities(t, t.TempDir())
			env := activityEnv(a)
			var token []byte
			env.RegisterActivityWithOptions(func(ctx context.Context) error {
				token = activity.GetInfo(ctx).TaskToken
				return kind.run(ctx)
			}, activity.RegisterOptions{Name: kind.activity})
			_, _ = env.ExecuteActivity(kind.activity)
			if len(token) == 0 {
				t.Fatal("the test environment gave the attempt no task token")
			}
			req := cutShortFailure()
			req.TaskToken = token
			if reached, err := sent(a, failed, req); !reached {
				t.Fatalf("its failure report was kept back (%v); want it sent", err)
			}
		})
	}
}

// The failure kept back is the one the SDK itself builds of a send its worker's
// Stop cut short: its default failure converter applied to context.Canceled,
// as its postprocess does (visitorErrorToActivityFailure). An SDK that wraps
// or renames it fails this test rather than bringing the stop's bug back.
func TestOwedReports_TheSDKsOwnCutShortFailureIsRecognised(t *testing.T) {
	built := &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken: []byte("tok"),
		Failure:   temporal.GetDefaultFailureConverter().ErrorToFailure(context.Canceled),
	}
	if !cutShort(built) {
		t.Fatalf("the SDK's own failure for a cut-short send (message %q) is not recognised", built.GetFailure().GetMessage())
	}
}
