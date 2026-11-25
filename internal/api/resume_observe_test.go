package api_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/telemetry"
)

// resumeObserver records what it is told.
type resumeObserver struct {
	mu        sync.Mutex
	requests  []telemetry.ResumeRequest
	shares    []float64
	confirmed []telemetry.ResumeConfirmed
	anomalies []string
}

func (o *resumeObserver) RecordResumeRequest(_ context.Context, r telemetry.ResumeRequest) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, r)
}
func (o *resumeObserver) RecordResumePreflight(_ context.Context, _ string, share float64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.shares = append(o.shares, share)
}
func (o *resumeObserver) RecordResumeConfirmed(_ context.Context, r telemetry.ResumeConfirmed) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.confirmed = append(o.confirmed, r)
}
func (o *resumeObserver) RecordResumeAnomaly(_ context.Context, _, kind string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.anomalies = append(o.anomalies, kind)
}

func observedBy(f *resumeFixture, o *resumeObserver) {
	f.plan.History = &engine.HistoryLimit{MaxEvents: 51_200, MaxBytes: 50 << 20, Reserve: engine.DefaultHistoryReserve}
	f.configure = func(cfg *api.Config) { cfg.ResumeObserver = o }
}

// TestResumeRun_Observed: each answer is counted once by what it was, every
// plan's share of its history and every confirmed resume is recorded, and a
// preview that plans counts as no request to resume.
func TestResumeRun_Observed(t *testing.T) {
	o := &resumeObserver{}
	f := newResumeFixture(t, func(f *resumeFixture) { observedBy(f, o) })
	f.resets(true)
	f.preview(t)
	if len(o.requests) != 0 || len(o.shares) != 1 || o.shares[0] <= 0 || o.shares[0] >= 1 {
		t.Fatalf("a preview: requests %v, shares %v", o.requests, o.shares)
	}
	if _, err := f.confirm(t, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.confirm(t, nil); err != nil {
		t.Fatal(err)
	}
	_, _ = f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	_, _ = f.confirm(t, func(r *orkstackv1.ResumeRunRequest) { r.ExpectedTerminalSequence = 3 })
	var got []string
	for _, r := range o.requests {
		got = append(got, fmt.Sprintf("%s %s", r.Outcome, r.Refusal))
	}
	if want := []string{"resumed ", "already ", "preview_refused STATE_NOT_RESUMABLE", "refused RUN_CHANGED"}; !slices.Equal(got, want) {
		t.Fatalf("outcomes = %q, want %q", got, want)
	}
	if len(o.confirmed) != 1 {
		t.Fatalf("confirmed = %v", o.confirmed)
	}
	c := o.confirmed[0]
	if c.Namespace != "default" || c.Ordinal != 1 || c.Tasks["kept"] != 1 || c.Tasks["rerun_failed"] != 1 || c.SinceFailure != 23*time.Hour {
		t.Fatalf("confirmed = %+v", c)
	}
}

// TestResumeRun_ObservedOutcomes: what is unconfirmed and what could not be
// checked are told apart from a refusal, and a malformed request is none.
func TestResumeRun_ObservedOutcomes(t *testing.T) {
	last := func(o *resumeObserver) string {
		if len(o.requests) == 0 {
			return ""
		}
		return o.requests[len(o.requests)-1].Outcome
	}
	o := &resumeObserver{}
	f := newResumeFixture(t, func(f *resumeFixture) { observedBy(f, o) })
	if _, err := f.resume(&orkstackv1.ResumeRunRequest{}); connect.CodeOf(err) != connect.CodeInvalidArgument || last(o) != "" {
		t.Fatalf("malformed = %v, outcome %q", err, last(o))
	}
	f.cluster.onReset = func(engine.ResumeReset) (string, error) { return "", engine.ErrResetOutcomeUnknown }
	_, _ = f.confirm(t, nil)
	if last(o) != "unconfirmed" {
		t.Fatalf("outcome = %q, want unconfirmed", last(o))
	}
	f.cluster.err = fmt.Errorf("unreachable")
	_, _ = f.confirm(t, nil)
	if last(o) != "unavailable" {
		t.Fatalf("outcome = %q, want unavailable", last(o))
	}
}

// TestResumeRun_Anomalies: a base that names another run than its reset
// answered is counted, and the base's own word is what is answered from.
func TestResumeRun_Anomalies(t *testing.T) {
	o := &resumeObserver{}
	f := newResumeFixture(t, func(f *resumeFixture) { observedBy(f, o) })
	f.resets(true)
	apply := f.cluster.onReset
	f.cluster.onReset = func(r engine.ResumeReset) (string, error) {
		_, _ = apply(r)
		return "exec-elsewhere", nil
	}
	if _, err := f.confirm(t, nil); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(o.anomalies, []string{"reset_run_mismatch"}) {
		t.Fatalf("anomalies = %v", o.anomalies)
	}
	far := &resumeObserver{}
	g := newResumeFixture(t, func(f *resumeFixture) { observedBy(f, far) })
	g.snapshot(t, 90_000)
	_, _ = g.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if !slices.Equal(far.anomalies, []string{"events_above_terminal"}) {
		t.Fatalf("anomalies = %v", far.anomalies)
	}
}
