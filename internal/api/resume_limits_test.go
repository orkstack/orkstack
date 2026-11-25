package api_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	historypb "go.temporal.io/api/history/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// TestResumeRun_HistoryIsReadOnce: a closed execution's history is read once
// a minute however often it is asked about, and another execution's is its
// own.
func TestResumeRun_HistoryIsReadOnce(t *testing.T) {
	f := newResumeFixture(t, nil)
	for range 3 {
		f.preview(t)
	}
	if f.cluster.reads != 1 {
		t.Fatalf("%d history reads, want one", f.cluster.reads)
	}
	f.clock.advance(61 * time.Second)
	f.preview(t)
	if f.cluster.reads != 2 {
		t.Fatalf("%d history reads a minute later, want two", f.cluster.reads)
	}
	e := f.cluster.executions[""]
	e.ExecutionRunID = "exec-other"
	f.cluster.executions[""] = e
	f.cluster.histories["exec-other"] = failedHistory(t, "a", "b")
	f.preview(t)
	if f.cluster.reads != 3 {
		t.Fatalf("%d history reads of another execution, want its own", f.cluster.reads)
	}
}

// TestResumeRun_OnePreviewAtATime: previews of one run wait for each other, a
// caller who gives up is let go, and another run's preview waits for none.
func TestResumeRun_OnePreviewAtATime(t *testing.T) {
	f := newResumeFixture(t, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	f.cluster.onHistory = func() ([]*historypb.HistoryEvent, bool) {
		close(entered)
		<-release
		return nil, false
	}
	first := make(chan error, 1)
	go func() { _, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true}); first <- err }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := f.svc.ResumeRun(ctx, connect.NewRequest(&orkstackv1.ResumeRunRequest{RunId: "ork-r", DryRun: true}))
	if code := connect.CodeOf(err); code != connect.CodeDeadlineExceeded && code != connect.CodeCanceled {
		t.Fatalf("a second preview = %v, want it to wait until its caller gave up", err)
	}
	f.cluster.onHistory = nil
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if _, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true}); err != nil {
		t.Fatal(err)
	}
}

// TestResumeRun_ConfirmsAreThrottled: a run's confirms are bounded a minute,
// previews are not counted among them, and one refused says when to retry.
func TestResumeRun_ConfirmsAreThrottled(t *testing.T) {
	f := newResumeFixture(t, func(f *resumeFixture) { f.settings.ConfirmsPerMinute = 2 })
	f.cluster.onReset = func(engine.ResumeReset) (string, error) { return "", engine.ErrResetOutcomeUnknown }
	for range 5 {
		f.preview(t)
	}
	for n := range 2 {
		_, err := f.confirm(t, nil)
		if r := refusalOf(t, err); r.GetReason() == "RESUME_CHECK_UNAVAILABLE" {
			t.Fatalf("confirm %d was throttled", n)
		}
	}
	_, err := f.confirm(t, nil)
	if r := refusalOf(t, err); r.GetReason() != "RESUME_CHECK_UNAVAILABLE" || r.GetCause() != "throttled" || r.GetRetryAfterSeconds() <= 0 {
		t.Fatalf("third = %v, want it throttled", r)
	}
	f.preview(t)
	f.clock.advance(time.Minute)
	_, err = f.confirm(t, nil)
	if r := refusalOf(t, err); r.GetCause() == "throttled" {
		t.Fatalf("a minute later = %v", r)
	}
}
