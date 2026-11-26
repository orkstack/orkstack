package api_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

type newestEndings struct {
	has bool
	err error
}

func (n newestEndings) HasRun(context.Context, string) (bool, error) { return n.has, n.err }

// endingsArchive is an archiver over a store that can lose or garble an
// ending's body.
type endingsArchive struct {
	*engine.RunArchiver
	lost map[uint64]error
	body map[uint64][]byte
}

// RunEndings lists no body of an ending that is gone.
func (a endingsArchive) RunEndings(ctx context.Context, runID string) (artifacts.RunEndings, error) {
	endings, err := a.RunArchiver.RunEndings(ctx, runID)
	endings.Archived = slices.DeleteFunc(endings.Archived, func(scope uint64) bool { return errors.Is(a.lost[scope], artifacts.ErrNotFound) })
	return endings, err
}

func (a endingsArchive) LoadEnding(ctx context.Context, runID string, scope uint64) ([]byte, error) {
	if err := a.lost[scope]; err != nil {
		return nil, err
	}
	if body, ok := a.body[scope]; ok {
		return body, nil
	}
	return a.RunArchiver.LoadEnding(ctx, runID, scope)
}

const evidenceResumed = "ork-resumed-1"

// resumedRun archives a run that failed, was resumed under 3 and failed, and
// was resumed under 9, with two numbers skipped, and succeeded.
func (f *evidenceFixture) resumedRun(store artifacts.RunEndingStore) *orkstackv1.Run {
	f.t.Helper()
	ctx := context.Background()
	event := func(seq int64, typ orkstackv1.EventType) *orkstackv1.Event {
		return &orkstackv1.Event{RunId: evidenceResumed, Sequence: seq, Type: typ, Timestamp: timestamppb.New(evidenceNow)}
	}
	first := []*orkstackv1.Event{event(1, orkstackv1.EventType_EVENT_TYPE_RUN_CREATED), event(2, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED)}
	second := append(first[:2:2], event(3, orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED), event(4, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED))
	third := append(second[:4:4], event(9, orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED), event(10, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED))
	run := &orkstackv1.Run{Id: evidenceResumed, OrkflowName: "evidence", OrkflowVersion: "0.1.0", OrkflowDefinitionDigest: f.digest,
		Status: orkstackv1.Status_STATUS_FAILED, Error: &orkstackv1.Error{Code: "EXIT_1"},
		CreatedAt: timestamppb.New(evidenceNow.Add(-time.Hour)), FinishedAt: timestamppb.New(evidenceNow.Add(-time.Minute)),
		// A kept result the run ended before reading back: its commit is
		// stored, and its row names none.
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "t", Status: orkstackv1.Status_STATUS_CANCELED, ReadingBack: true,
			Error: &orkstackv1.Error{Code: "RESUME_NOT_READ_BACK"}}}}
	f.record(evidenceResumed, "t", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, false)
	for _, e := range []struct {
		scope  uint64
		events []*orkstackv1.Event
	}{{0, first}, {3, second}, {9, third}} {
		if e.scope > 0 {
			must.OK(f.t, store.MarkResumed(ctx, evidenceResumed, e.scope, e.scope/9*3))
		}
		if e.scope == 9 {
			run.Status, run.Error = orkstackv1.Status_STATUS_SUCCEEDED, nil
		}
		must.OK(f.t, f.archiver.ArchiveEnding(ctx, run, e.scope, e.events))
	}
	// Marked by resumes whose reset never ran: no ending.
	must.OK(f.t, store.MarkResumed(ctx, evidenceResumed, 7, 3))
	must.OK(f.t, store.MarkResumed(ctx, evidenceResumed, 40, 9))
	f.reader.setRun(run)
	return run
}

func newEndingsFixture(t *testing.T, newest api.NewestEndings, lost map[uint64]error, garbled map[uint64][]byte) (*evidenceFixture, artifacts.RunEndingStore) {
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	must.OK(t, err)
	t.Cleanup(func() { _ = store.Close() })
	archiver := engine.NewRunArchiver(store)
	f := newEvidenceFixture(t, withConfig(func(cfg *api.Config) {
		cfg.Archive = endingsArchive{archiver, lost, garbled}
		cfg.NewestEndings = newest
	}))
	f.archiver = archiver
	return f, store
}

// TestExportRunEvidence_EveryEnding: the bundle of a resumed run holds every
// ending's body, verifies, and is of the newest one.
func TestExportRunEvidence_EveryEnding(t *testing.T) {
	f, store := newEndingsFixture(t, newestEndings{has: true}, nil, nil)
	f.resumedRun(store)
	got := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: evidenceResumed})
	must.OK(t, got.err)
	report, err := evidence.Verify(bytes.NewReader(got.data))
	must.OK(t, err)
	if !report.OK() || got.manifest.SchemaVersion != evidence.SchemaVersion || got.manifest.RunStatus != "SUCCEEDED" {
		t.Fatalf("version %d, %s, failures %+v", got.manifest.SchemaVersion, got.manifest.RunStatus, report.Failures)
	}
	var endings []string
	for _, e := range got.manifest.Endings {
		endings = append(endings, strings.Join([]string{e.Path, e.RunStatus, e.Reason}, " "))
	}
	want := "endings/00000000000000000000.jsonl FAILED EXIT_1\nendings/00000000000000000003.jsonl FAILED EXIT_1\nrun.jsonl SUCCEEDED "
	if text := strings.Join(endings, "\n"); text != want {
		t.Fatalf("endings\n%s\nwant\n%s", text, want)
	}
	for i, origin := range []string{"metadata/run-resumes/" + evidenceResumed + "/00000000000000000009.jsonl",
		"metadata/runs/" + evidenceResumed + ".jsonl", "metadata/run-resumes/" + evidenceResumed + "/00000000000000000003.jsonl"} {
		if m := got.manifest.Members[i]; m.Origin != origin || m.State != evidence.StatePresent {
			t.Errorf("member %d = %s %s, want %s", i, m.State, m.Origin, origin)
		}
	}
	// What was not read back keeps its attempt and says it was canceled.
	if task := got.manifest.Tasks[0]; task.Status != "CANCELED" || len(task.Attempts) != 1 || task.Attempts[0].State != evidence.StatePresent {
		t.Errorf("the row not read back = %+v", task)
	}
}

// TestExportRunEvidence_AnEndingThatIsGone is a member of the bundle, and
// never a smaller bundle that looks whole.
func TestExportRunEvidence_AnEndingThatIsGone(t *testing.T) {
	for name, tc := range map[string]struct {
		lost    map[uint64]error
		garbled map[uint64][]byte
		state   evidence.State
		reason  string
		code    connect.Code
	}{
		"gone":    {lost: map[uint64]error{3: artifacts.ErrNotFound}, state: evidence.StateReclaimed, reason: evidence.ReasonEndingMissing},
		"garbled": {garbled: map[uint64][]byte{3: []byte("what")}, state: evidence.StateRejected, reason: evidence.ReasonEndingDamaged},
		"another run's": {garbled: map[uint64][]byte{3: []byte(`{"kind":"run","run":{"id":"ork-other"}}`)},
			state: evidence.StateRejected, reason: evidence.ReasonEndingDamaged},
		"down": {lost: map[uint64]error{3: errors.New("connection refused")}, code: connect.CodeUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			f, store := newEndingsFixture(t, newestEndings{has: true}, tc.lost, tc.garbled)
			f.resumedRun(store)
			got := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: evidenceResumed})
			if tc.code != 0 {
				if got.code() != tc.code {
					t.Fatalf("= %v, want %v", got.err, tc.code)
				}
				return
			}
			must.OK(t, got.err)
			report, err := evidence.Verify(bytes.NewReader(got.data))
			must.OK(t, err)
			member := got.manifest.Members[2]
			if !report.OK() || len(got.manifest.Endings) != 3 || member.State != tc.state || member.Reason != tc.reason {
				t.Fatalf("%d endings, member %+v, failures %+v", len(got.manifest.Endings), member, report.Failures)
			}
		})
	}
}

// TestExportRunEvidence_TheNewestEndingIsNotArchivedYet: the ending a resume
// set aside is never exported as the run's.
func TestExportRunEvidence_TheNewestEndingIsNotArchivedYet(t *testing.T) {
	for name, tc := range map[string]struct {
		newest newestEndings
		code   connect.Code
	}{
		"not yet":       {newestEndings{}, connect.CodeFailedPrecondition},
		"cannot answer": {newestEndings{err: errors.New("connection refused")}, connect.CodeUnavailable},
	} {
		f, store := newEndingsFixture(t, tc.newest, nil, nil)
		f.resumedRun(store)
		if got := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: evidenceResumed}); got.code() != tc.code {
			t.Errorf("%s = %v, want %v", name, got.err, tc.code)
		}
	}
}

// TestExportRunEvidence_AnEndingLostBeforeTheNewest: an ending whose body was
// gone before the newest was archived is named, because its scope lies in a gap
// of the newest's stream.
func TestExportRunEvidence_AnEndingLostBeforeTheNewest(t *testing.T) {
	f, store := newEndingsFixture(t, newestEndings{has: true}, nil, nil)
	ctx := context.Background()
	run := &orkstackv1.Run{Id: evidenceResumed, OrkflowName: "evidence", OrkflowVersion: "0.1.0", OrkflowDefinitionDigest: f.digest,
		Status: orkstackv1.Status_STATUS_FAILED, CreatedAt: timestamppb.New(evidenceNow), FinishedAt: timestamppb.New(evidenceNow)}
	first := []*orkstackv1.Event{{RunId: evidenceResumed, Sequence: 1, Type: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED}}
	must.OK(t, f.archiver.ArchiveEnding(ctx, run, 0, first))
	for _, scope := range []uint64{2, 9} {
		must.OK(t, store.MarkResumed(ctx, evidenceResumed, scope, scope/9*2))
	}
	must.OK(t, f.archiver.ArchiveEnding(ctx, run, 9, append(first, &orkstackv1.Event{RunId: evidenceResumed, Sequence: 9,
		Type: orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED}), engine.EventGap{First: 2, Last: 8, Reason: "gone"}))
	f.reader.setRun(run)
	got := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: evidenceResumed})
	must.OK(t, got.err)
	report, err := evidence.Verify(bytes.NewReader(got.data))
	must.OK(t, err)
	if e := got.manifest.Endings; len(e) != 3 || e[1].Scope != 2 || got.manifest.Members[2].Reason != evidence.ReasonEndingMissing || !report.OK() {
		t.Fatalf("endings %+v, failures %+v", e, report.Failures)
	}
}
