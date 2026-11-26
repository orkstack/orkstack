package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordWorld is a real local record store, and a run that awaits gate.
type recordWorld struct {
	t     *testing.T
	dir   string
	store *artifacts.RecordStore
	fault error
	read  func() // before a read
}

func (w *recordWorld) ReadRecordedValues(ctx context.Context, class artifacts.SlotClass, sp *artifacts.RecordSpacings, key string, want artifacts.RecordWant) (artifacts.RecordRead[*artifacts.RecordedFound], error) {
	if w.read != nil {
		w.read()
	}
	if w.fault != nil {
		return artifacts.RecordRead[*artifacts.RecordedFound]{}, w.fault
	}
	return w.store.ReadRecordedValues(ctx, class, sp, key, want)
}

func newRecordWorld(t *testing.T) *recordWorld {
	t.Helper()
	dir := t.TempDir()
	store, err := artifacts.NewRecordStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	return &recordWorld{t: t, dir: dir, store: store}
}

// resolved records gate's resolution: a completion of raw, or a rejection
// when raw is nil.
func (w *recordWorld) resolved(raw []byte, edit func(*orkstackv1.RecordedValues)) string {
	w.t.Helper()
	ctx := context.Background()
	detail, err := w.store.WriteResolutionDetail(ctx, &orkstackv1.ResolutionDetail{SchemaVersion: 1, Namespace: "default", RunId: "ork-1", TaskId: "gate",
		Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "alice", PrincipalVerified: true, Reason: "too risky"})
	if err != nil {
		w.t.Fatal(err)
	}
	rec := &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION, Namespace: "default",
		RunId: "ork-1", TaskId: "gate", AdmissionSha256: strings.Repeat("a", 64), InputBinding: make([]byte, 32), DetailSha256: detail, Rejected: raw == nil}
	if raw != nil {
		values, err := artifacts.NewLocalStore(w.dir)
		if err != nil {
			w.t.Fatal(err)
		}
		ref, err := values.Put(ctx, "values.json", strings.NewReader(string(raw)), artifacts.PutOptions{})
		if err != nil {
			w.t.Fatal(err)
		}
		rec.Values = &orkstackv1.ArtifactRef{Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}
	}
	if edit != nil {
		edit(rec)
	}
	if _, err := w.store.WriteRecordedValues(ctx, rec); err != nil {
		w.t.Fatal(err)
	}
	key, err := artifacts.RecordKey(artifacts.RecordResolution, "default", "ork-1", "gate", 0)
	if err != nil {
		w.t.Fatal(err)
	}
	return filepath.Join(w.dir, filepath.FromSlash(key))
}

// TestRecordRead_BoundaryTable: what the run reads of each answer of the
// store, through the real local-activity boundary.
func TestRecordRead_BoundaryTable(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	unanswered := func(class string) error {
		return fmt.Errorf("read: %w", &artifacts.RecordStoreUnanswered{Class: class, Err: errors.New("no answer")})
	}
	for _, tc := range []struct {
		name  string
		make  func(*recordWorld)
		want  string // the outcome, or "?" and the class
		check func(*testing.T, resolutionAnswer)
	}{
		{"a completion", func(w *recordWorld) { w.resolved([]byte(`{"ok":true}`), nil) }, ResolutionFound, func(t *testing.T, a resolutionAnswer) {
			if a.Rejected || string(a.ValuesJSON) != `{"ok":true}` || a.Values.URI == "" || a.Principal != "alice" || !a.PrincipalVerified ||
				a.via() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API || a.RecordSHA256 == "" {
				t.Fatalf("%+v", a)
			}
		}},
		{"a rejection", func(w *recordWorld) { w.resolved(nil, nil) }, ResolutionFound, func(t *testing.T, a resolutionAnswer) {
			if !a.Rejected || a.Reason != "too risky" || a.Values != nil {
				t.Fatalf("%+v", a)
			}
		}},
		{"stored after the deadline", func(w *recordWorld) {
			late := deadline.Add(2 * time.Second)
			if err := os.Chtimes(w.resolved([]byte(`{"ok":true}`), nil), late, late); err != nil {
				w.t.Fatal(err)
			}
		}, ResolutionLate, func(t *testing.T, a resolutionAnswer) {
			if a.Values != nil || a.ValuesJSON != nil || a.Principal != "" {
				t.Fatalf("a late record gives %+v", a)
			}
		}},
		{"none", func(*recordWorld) {}, ResolutionNone, nil},
		{"another admission", func(w *recordWorld) {
			w.resolved(nil, func(r *orkstackv1.RecordedValues) { r.AdmissionSha256 = strings.Repeat("b", 64) })
		}, ResolutionInvalid, nil},
		{"does not decode", func(w *recordWorld) {
			if err := os.WriteFile(w.resolved(nil, nil), []byte("{"), 0o644); err != nil {
				w.t.Fatal(err)
			}
		}, ResolutionInvalid, nil},
		{"a newer format", func(w *recordWorld) {
			if err := os.WriteFile(w.resolved(nil, nil), []byte(`{"schemaVersion": 9}`), 0o644); err != nil {
				w.t.Fatal(err)
			}
		}, ResolutionUnsupported, func(t *testing.T, a resolutionAnswer) {
			if a.Version != 9 || a.Object == "" {
				t.Fatalf("%+v", a)
			}
		}},
		{"no detail", func(w *recordWorld) {
			w.resolved(nil, nil)
			if err := os.RemoveAll(filepath.Join(w.dir, "metadata", "snapshots")); err != nil {
				w.t.Fatal(err)
			}
		}, ResolutionInvalid, nil},
		{"unavailable", func(w *recordWorld) { w.fault = unanswered(artifacts.UnansweredUnavailable) }, "?unavailable", nil},
		{"denied", func(w *recordWorld) { w.fault = unanswered(artifacts.UnansweredDenied) }, "?denied", nil},
		{"busy", func(w *recordWorld) { w.fault = unanswered(artifacts.UnansweredBusy) }, "?busy", nil},
		{"timeout", func(w *recordWorld) { w.fault = unanswered(artifacts.UnansweredTimeout) }, "?timeout", nil},
		{"a panic", func(w *recordWorld) { w.read = func() { panic("broken") } }, "?internal", nil},
		{"one attempt unanswered, then found", func(w *recordWorld) {
			w.resolved(nil, nil)
			w.fault = unanswered(artifacts.UnansweredUnavailable)
			tries := 0
			w.read = func() {
				if tries++; tries > 1 {
					w.fault = nil
				}
			}
		}, ResolutionFound, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newRecordWorld(t)
			tc.make(w)
			got := readThroughBoundary(t, NewResolutionReadActivities(w), deadline)
			outcome := got.Outcome
			if got.unanswered != "" {
				outcome = "?" + got.unanswered
			}
			if outcome != tc.want || (got.unanswered != "") != (got.err != nil) {
				t.Fatalf("%+v, want %s", got, tc.want)
			}
			if tc.check != nil {
				tc.check(t, got)
			}
		})
	}
	t.Run("an engine that reads none", func(t *testing.T) {
		if got := readThroughBoundary(t, NewResolutionReadActivities(nil), deadline); got.unanswered != artifacts.UnansweredUnavailable {
			t.Fatalf("%+v", got)
		}
	})
}

// readThroughBoundary reads gate's record from workflow code.
func readThroughBoundary(t *testing.T, act *ResolutionReadActivities, deadline time.Time) resolutionAnswer {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(act.ReadResolution, activity.RegisterOptions{Name: ActivityReadResolution})
	budget := DefaultRecordBudget
	budget.MissConfirmMS, budget.ReaskInitialMS, budget.ReaskMaxMS, budget.AttemptMarginMS = 1, 1, 2, 1
	var got resolutionAnswer
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		s := &runState{runID: "ork-1", wf: &orkstackv1.Orkflow{},
			plan:   RunPlan{Identity: &IdentityPlan{AdmissionSHA256: strings.Repeat("a", 64), ValueRecords: 1, RecordBudget: &budget}},
			awaits: map[string]*awaitState{"gate": {deadlineAt: deadline}}}
		got = s.readResolution(ctx, "gate")
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	return got
}

// TestRecordRead_ACancel: the run's is never unanswered, and an engine's stop
// is never the run's.
func TestRecordRead_ACancel(t *testing.T) {
	for _, canceled := range []bool{true, false} {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		env.RegisterActivityWithOptions(func(context.Context, ResolutionReadRequest) (ResolutionRead, error) {
			if canceled {
				env.CancelWorkflow()
			}
			return ResolutionRead{}, temporal.NewCanceledError()
		}, activity.RegisterOptions{Name: ActivityReadResolution})
		budget := DefaultRecordBudget
		var got resolutionAnswer
		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			s := &runState{runID: "ork-1", wf: &orkstackv1.Orkflow{}, plan: RunPlan{Identity: &IdentityPlan{ValueRecords: 1, RecordBudget: &budget}},
				awaits: map[string]*awaitState{"gate": {}}}
			got = s.readResolution(ctx, "gate")
			return nil
		})
		if err := env.GetWorkflowError(); err != nil || got.canceled != canceled || (got.unanswered == "") != canceled || got.err == nil {
			t.Fatalf("canceled %v: %+v, %v", canceled, got, err)
		}
	}
}
