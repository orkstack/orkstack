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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestResumeRun_Preview: what a preview answers: each task with its section
// and why, the counts, what it costs, and what a confirm must send back. It
// writes nothing and resets nothing.
func TestResumeRun_Preview(t *testing.T) {
	f := newResumeFixture(t, nil)
	got, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetRunId() != "ork-r" || got.GetTerminalSequence() != 9 || got.GetHighestSequence() != 9 || got.GetSeqBase() != 10 ||
		got.GetKeptCount() != 1 || got.GetAction() != orkstackv1.ResumeAction_RESUME_ACTION_RESUME || got.GetResumesUsed() != 0 ||
		got.GetReadbackCalls() != 1 || got.GetEstimatedReadbackSeconds() < 1 || !engine.IsResumePlanDigest(got.GetPlanDigest()) ||
		got.GetAlreadyResumed() || got.GetResume() != nil || len(got.GetWarnings()) != 0 {
		t.Fatalf("preview = %v", got)
	}
	var tasks []string
	for _, r := range got.GetTasks() {
		tasks = append(tasks, fmt.Sprintf("%s %s %s", r.GetTaskId(), r.GetCategory(), r.GetPreviousOutcome()))
		if r.GetWhy() == "" {
			t.Errorf("%s says no why", r.GetTaskId())
		}
	}
	if want := []string{"a RESUME_CATEGORY_KEPT STATUS_SUCCEEDED", "b RESUME_CATEGORY_RERUN_FAILED STATUS_FAILED"}; !slices.Equal(tasks, want) {
		t.Fatalf("tasks = %v, want %v", tasks, want)
	}
	if c := got.GetCounts(); len(c) != 2 || c[0].GetCount() != 1 || c[1].GetCount() != 1 {
		t.Fatalf("counts = %v", c)
	}
	// Planning again is the same plan; retrying from another task is another.
	again, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if err != nil || again.GetPlanDigest() != got.GetPlanDigest() {
		t.Fatalf("again = %q, %v; want %q", again.GetPlanDigest(), err, got.GetPlanDigest())
	}
	from, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true, FromTaskId: "a"})
	if err != nil || from.GetPlanDigest() == got.GetPlanDigest() || from.GetAction() != orkstackv1.ResumeAction_RESUME_ACTION_RETRY_FROM ||
		from.GetKeptCount() != 0 {
		t.Fatalf("from a = %v, %v", from, err)
	}
	if len(f.cluster.resets) != 0 {
		t.Fatal("a preview reset the run")
	}
}

// TestResumeRun_PreviewWarnings: what a caller should know and need not
// accept, and an attempt that must be accepted, with the key to accept it by.
func TestResumeRun_PreviewWarnings(t *testing.T) {
	f := newResumeFixture(t, func(f *resumeFixture) {
		f.run.Pause = &orkstackv1.RunPause{PausedAt: timestamppb.New(resumeEpoch), PausedBy: "ada"}
		f.fleet.failures = map[string]error{"orker-a": errors.New("gone")}
	})
	if _, err := f.attempts.ClaimAttempt(context.Background(), attemptOf("b"), "orker-a"); err != nil {
		t.Fatal(err)
	}
	got, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[orkstackv1.ResumeWarningKind]*orkstackv1.ResumeWarning{}
	for _, w := range got.GetWarnings() {
		kinds[w.GetKind()] = w
	}
	if kinds[orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_WAS_PAUSED] == nil ||
		kinds[orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_ATTEMPT_OUTCOME_UNKNOWN].GetAcknowledgeKey() != "orker-a/b//1" {
		t.Fatalf("warnings = %v", got.GetWarnings())
	}
	// Warnings are not the plan: they come and go without changing it.
	plain := newResumeFixture(t, nil)
	p, err := plain.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if err != nil || p.GetPlanDigest() != got.GetPlanDigest() {
		t.Fatalf("the plan changed with its warnings: %v", err)
	}
}

// TestResumeRun_PlanRefusals: what only the plan, its history and the
// deployment's state now can refuse.
func TestResumeRun_PlanRefusals(t *testing.T) {
	permanent, wait, replan := orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT,
		orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT, orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_REPLAN
	digest := strings.Repeat("ab", 32)
	limited := engine.RunPlan{CommitRoot: resumeCommitRoot, History: &engine.HistoryLimit{MaxEvents: 100, MaxBytes: 1 << 30, Reserve: engine.DefaultHistoryReserve}}
	preview := func() *orkstackv1.ResumeRunRequest { return &orkstackv1.ResumeRunRequest{DryRun: true} }
	for name, tc := range map[string]struct {
		change        func(*resumeFixture)
		after         func(*testing.T, *resumeFixture)
		req           *orkstackv1.ResumeRunRequest
		reason, cause string
		class         orkstackv1.ResumeRefusalClass
	}{
		"a part not built yet": {change: func(f *resumeFixture) {
			f.def.Tasks[0].Await, f.def.Tasks[0].Uses = &orkstackv1.AwaitSpec{}, ""
		}, req: preview(), reason: "RELEASE_UNSUPPORTED", cause: "settle", class: permanent},
		"a run held to a concurrency limit": {change: func(f *resumeFixture) {
			f.plan.Concurrency = &engine.ConcurrencyPlan{}
		}, req: preview(), reason: "RELEASE_UNSUPPORTED", cause: "concurrency_slot", class: permanent},
		"its history is too full": {change: func(f *resumeFixture) { f.plan = limited }, req: preview(), reason: "RESUME_TOO_LARGE", class: permanent},
		"its namespace is held": {change: func(f *resumeFixture) {
			f.run.Namespace, f.def.Namespace = "payments", "payments"
			f.configure = func(cfg *api.Config) {
				cfg.Namespaces = newFakeRegistry(&orkstackv1.Namespace{Name: "payments", Hold: &orkstackv1.NamespaceHold{Reason: "audit"}})
			}
		}, req: &orkstackv1.ResumeRunRequest{DryRun: true, Namespace: "payments"}, reason: "NAMESPACE_HELD", class: wait},
		"no Orker of a pool that reruns": {change: func(f *resumeFixture) { f.pools = livePools{} }, req: preview(), reason: "RERUN_POOL_UNSERVED", class: wait},
		"its release is not running": {change: func(f *resumeFixture) {
			e := f.cluster.executions[""]
			e.DeploymentName, e.BuildID = "ork", "build-7"
			f.cluster.executions[""] = e
			f.configure = func(cfg *api.Config) {
				cfg.Releases = &stubReleases{obs: engine.ReleaseObservation{EngineReplicas: map[string]int{"build-8": 2}}}
			}
		}, req: preview(), reason: "RELEASE_UNAVAILABLE", class: wait},
		"a saved result that cannot be checked": {change: func(f *resumeFixture) {
			f.configure = func(cfg *api.Config) { cfg.Commits = unansweredCommits{} }
		}, req: preview(), reason: "RESUME_CHECK_UNAVAILABLE", class: wait},
		"another plan than the caller saw": {req: &orkstackv1.ResumeRunRequest{ExpectedTerminalSequence: 9, ExpectedPlanDigest: digest},
			reason: "PLAN_CHANGED", class: replan},
		"events above its ending": {after: func(t *testing.T, f *resumeFixture) { f.snapshot(t, 9+4097) }, req: preview(),
			reason: "STATE_UNREADABLE", cause: "events_above_terminal", class: replan},
		"a gap accepted at another sequence": {after: func(t *testing.T, f *resumeFixture) { f.snapshot(t, 9+5000) },
			req:    &orkstackv1.ResumeRunRequest{DryRun: true, AcceptSequenceGap: true, AcceptedHighestSeq: 9 + 4999},
			reason: "PLAN_CHANGED", class: replan},
		"no sequence left": {after: func(t *testing.T, f *resumeFixture) { f.snapshot(t, 1<<53-1) },
			req:    &orkstackv1.ResumeRunRequest{DryRun: true, AcceptSequenceGap: true, AcceptedHighestSeq: 1<<53 - 1},
			reason: "RESUME_SEQUENCE_EXHAUSTED", class: permanent},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, tc.change)
			if tc.after != nil {
				tc.after(t, f)
			}
			_, err := f.resume(proto.Clone(tc.req).(*orkstackv1.ResumeRunRequest))
			r := refusalOf(t, err)
			if r.GetReason() != tc.reason || r.GetCause() != tc.cause || r.GetClass() != tc.class || r.GetMessage() == "" ||
				(tc.class == wait) != (r.GetRetryAfterSeconds() > 0) {
				t.Fatalf("refusal = %v, want %s %s %s", r, tc.reason, tc.cause, tc.class)
			}
		})
	}
}

// TestResumeRun_SequenceGap: a sequence above the ending within the bound is
// passed by; one beyond it is accepted only at the sequence the caller saw.
func TestResumeRun_SequenceGap(t *testing.T) {
	f := newResumeFixture(t, nil)
	f.snapshot(t, 12)
	got, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if err != nil || got.GetSeqBase() != 13 || got.GetHighestSequence() != 12 || got.GetTerminalSequence() != 9 {
		t.Fatalf("within the bound = %v, %v; want seq_base 13", got, err)
	}
	far := newResumeFixture(t, nil)
	far.snapshot(t, 9000)
	_, err = far.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if r := refusalOf(t, err); !strings.Contains(r.GetMessage(), "9000") {
		t.Fatalf("the refusal does not name the highest sequence: %v", r)
	}
	got, err = far.resume(&orkstackv1.ResumeRunRequest{DryRun: true, AcceptSequenceGap: true, AcceptedHighestSeq: 9000})
	if err != nil || got.GetSeqBase() != 9001 || got.GetHighestSequence() != 9000 {
		t.Fatalf("accepted = %v, %v; want seq_base 9001", got, err)
	}
	// The gap moves the sequence, and so the plan a confirm must have seen.
	plain := newResumeFixture(t, nil)
	p, err := plain.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if err != nil || p.GetPlanDigest() == got.GetPlanDigest() {
		t.Fatalf("a plan above a gap has the digest of one without, %v", err)
	}
}

// TestResumeRun_ResultGoneReruns: a saved result that is gone puts its task
// in the plan, and says so.
func TestResumeRun_ResultGoneReruns(t *testing.T) {
	f := newResumeFixture(t, func(f *resumeFixture) { f.run.TaskRuns[0].ResultEpoch = 5 })
	got, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if a := got.GetTasks()[0]; a.GetCategory() != orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_RESULT_GONE || a.GetDetail() == "" || got.GetKeptCount() != 0 {
		t.Fatalf("a = %v", a)
	}
}

func (f *resumeFixture) snapshot(t *testing.T, sequence int64) {
	t.Helper()
	if _, err := f.snaps.WriteSnapshot(context.Background(), "default", "ork-r", sequence, []byte("x")); err != nil {
		t.Fatal(err)
	}
}

func attemptOf(id string) artifacts.AttemptRef {
	return artifacts.AttemptRef{RunID: "ork-r", TaskID: id}
}

// unansweredCommits is a commit store that does not answer: the outage no
// real store produces on demand.
type unansweredCommits struct{ artifacts.CommitStore }

func (unansweredCommits) Get(context.Context, string) (artifacts.CommitRecord, error) {
	return artifacts.CommitRecord{}, errors.New("connection refused")
}

// TestResumeRun_RetentionEnding: a run about to be retired is not resumed,
// where retention is enforced; a policy that cannot be read is a wait.
func TestResumeRun_RetentionEnding(t *testing.T) {
	for name, tc := range map[string]struct {
		ended    time.Duration // before now
		enforced bool
		err      error
		want     string
	}{
		"long before":           {time.Hour, true, nil, ""},
		"just before its end":   {72*time.Hour - 14*time.Minute, true, nil, "RETENTION_ENDING"},
		"nothing is retired":    {100 * time.Hour, false, nil, ""},
		"no maximum is set":     {100 * time.Hour, true, artifacts.ErrNotFound, ""},
		"a policy not answered": {time.Hour, true, errors.New("unreachable"), "RESUME_CHECK_UNAVAILABLE"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, func(f *resumeFixture) {
				f.run.FinishedAt = timestamppb.New(f.clock.Now().Add(-tc.ended))
				f.configure = func(cfg *api.Config) {
					cfg.Retention = archive.NewRetentionController(retentionPolicies{err: tc.err}, &fakeRetentionAdmin{}, "ork-main", time.Hour, nil)
					cfg.RetentionEnforced = tc.enforced
				}
			})
			_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if r := refusalOf(t, err); r.GetReason() != tc.want {
				t.Fatalf("refusal = %v, want %s", r, tc.want)
			}
		})
	}
}

// retentionPolicies is a policy store holding a 72 h maximum, or an error.
type retentionPolicies struct{ err error }

func (p retentionPolicies) Get(context.Context) (*orkstackv1.RetentionPolicy, error) {
	return &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(72 * time.Hour)}, p.err
}

func (p retentionPolicies) Put(_ context.Context, policy *orkstackv1.RetentionPolicy, _ artifacts.PutConditions) (*orkstackv1.RetentionPolicy, error) {
	return policy, p.err
}

// TestGetRun_ResumeHint: a failed run says whether it can be resumed, on an
// engine that resumes runs.
func TestGetRun_ResumeHint(t *testing.T) {
	get := func(f *resumeFixture) *orkstackv1.ResumeHint {
		t.Helper()
		resp, err := f.svc.GetRun(context.Background(), connect.NewRequest(&orkstackv1.GetRunRequest{RunId: "ork-r"}))
		if err != nil {
			t.Fatal(err)
		}
		return resp.Msg.GetRun().GetResumeHint()
	}
	if h := get(newResumeFixture(t, nil)); h.GetState() != orkstackv1.ResumeHintState_RESUME_HINT_STATE_RESUMABLE || len(h.GetTaskActions()) != 2 {
		t.Fatalf("hint = %v", h)
	}
	if h := get(newResumeFixture(t, func(f *resumeFixture) {
		f.configure = func(cfg *api.Config) { cfg.ResumeRecords = nil }
	})); h != nil {
		t.Fatalf("an engine that resumes nothing offers it: %v", h)
	}
}
