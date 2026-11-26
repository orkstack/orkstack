package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func resumeEntry(scope uint64, by string) *orkstackv1.RunResume {
	return &orkstackv1.RunResume{Scope: scope, Sequence: int64(scope), Principal: by, SupersededReason: "TASK_FAILED"}
}

func endedEntry(scope uint64, by, reason string, before bool) *orkstackv1.RunResume {
	e := resumeEntry(scope, by)
	e.Ended = &orkstackv1.RunResumeEnded{Status: orkstackv1.Status_STATUS_FAILED, Reason: reason, BeforeAnythingReran: before}
	return e
}

func resumesText(entries []*orkstackv1.RunResume) string {
	var out []string
	for _, e := range entries {
		text := e.GetPrincipal()
		if end := e.GetEnded(); end != nil {
			text += "=" + end.GetReason()
			if end.GetBeforeAnythingReran() {
				text += "(nothing reran)"
			}
		}
		out = append(out, text)
	}
	return strings.Join(out, " ")
}

// TestResumesOf: a run's resumes are those its earlier ending listed, then the
// one this ending is of, and only that one's end is this run's own.
func TestResumesOf(t *testing.T) {
	failed := &orkstackv1.Error{Code: "EXIT_1"}
	started := timestamppb.New(readerEpoch)
	row := func(epoch int64, ran bool) *orkstackv1.TaskRun {
		r := &orkstackv1.TaskRun{TaskId: "a", ResultEpoch: epoch}
		if ran {
			r.StartedAt = started
		}
		return r
	}
	for name, tc := range map[string]struct {
		earlier []*orkstackv1.RunResume
		scope   uint64
		run     *orkstackv1.Run
		want    string
	}{
		"running": {scope: 7, want: "ann",
			run: &orkstackv1.Run{Status: orkstackv1.Status_STATUS_RUNNING, Resumes: []*orkstackv1.RunResume{resumeEntry(7, "ann")}}},
		"ended after a task ran again": {scope: 7, want: "ann=EXIT_1",
			run: &orkstackv1.Run{Status: orkstackv1.Status_STATUS_FAILED, Error: failed, TaskRuns: []*orkstackv1.TaskRun{row(7, true)},
				Resumes: []*orkstackv1.RunResume{resumeEntry(7, "ann")}}},
		"ended before any did": {scope: 7, want: "ann=EXIT_1(nothing reran)",
			run: &orkstackv1.Run{Status: orkstackv1.Status_STATUS_FAILED, Error: failed,
				TaskRuns: []*orkstackv1.TaskRun{row(7, false), row(3, true)}, Resumes: []*orkstackv1.RunResume{resumeEntry(7, "ann")}}},
		"an item ran again": {scope: 7, want: "ann=EXIT_1",
			run: &orkstackv1.Run{Status: orkstackv1.Status_STATUS_FAILED, Error: failed,
				TaskRuns: []*orkstackv1.TaskRun{{TaskId: "f", Items: []*orkstackv1.TaskRun{row(7, true)}}},
				Resumes:  []*orkstackv1.RunResume{resumeEntry(7, "ann")}}},
		"a second resume": {scope: 20, want: "ann=EXIT_1 bob",
			earlier: []*orkstackv1.RunResume{endedEntry(7, "ann", "EXIT_1", false)},
			run:     &orkstackv1.Run{Status: orkstackv1.Status_STATUS_RUNNING, Resumes: []*orkstackv1.RunResume{resumeEntry(20, "bob")}}},
		// Until its RUN_RESUMED, a reset run's state is the one it went back
		// to, the earlier resume's own entry included.
		"not said to be resumed yet": {scope: 20, want: "ann=EXIT_1",
			earlier: []*orkstackv1.RunResume{endedEntry(7, "ann", "EXIT_1", false)},
			run:     &orkstackv1.Run{Status: orkstackv1.Status_STATUS_RUNNING, Resumes: []*orkstackv1.RunResume{resumeEntry(7, "ann")}}},
		// A plan that could not run has no entry of its own, and ended none.
		"a plan that could not run": {scope: 20, want: "ann=EXIT_1",
			earlier: []*orkstackv1.RunResume{endedEntry(7, "ann", "EXIT_1", false)},
			run: &orkstackv1.Run{Status: orkstackv1.Status_STATUS_FAILED, Error: &orkstackv1.Error{Code: "RESUME_PLAN_INVALID"},
				Resumes: []*orkstackv1.RunResume{resumeEntry(7, "ann")}}},
		"the earlier ending is gone": {scope: 20, want: "bob",
			run: &orkstackv1.Run{Status: orkstackv1.Status_STATUS_RUNNING, Resumes: []*orkstackv1.RunResume{resumeEntry(20, "bob")}}},
		"succeeded": {scope: 7, want: "ann=",
			run: &orkstackv1.Run{Status: orkstackv1.Status_STATUS_SUCCEEDED, TaskRuns: []*orkstackv1.TaskRun{row(7, true)},
				Resumes: []*orkstackv1.RunResume{resumeEntry(7, "ann")}}},
	} {
		t.Run(name, func(t *testing.T) {
			before := proto.Clone(tc.run).(*orkstackv1.Run)
			got := resumesOf(tc.earlier, tc.run, tc.scope)
			if resumesText(got) != tc.want {
				t.Fatalf("= %q, want %q", resumesText(got), tc.want)
			}
			// An archive built from what a read answered answers the same.
			again := proto.Clone(tc.run).(*orkstackv1.Run)
			again.Resumes = got
			if twice := resumesOf(tc.earlier, again, tc.scope); resumesText(twice) != tc.want {
				t.Fatalf("again = %q, want %q", resumesText(twice), tc.want)
			}
			if !proto.Equal(before, tc.run) {
				t.Fatal("the run it was asked of was changed")
			}
		})
	}
}

// TestResumedRun_GetRunListsEveryResume: a live read of a resumed run lists
// the resumes its earlier ending recorded, and an unreadable one is no answer.
func TestResumedRun_GetRunListsEveryResume(t *testing.T) {
	const completed = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
	earlier := []*orkstackv1.RunResume{endedEntry(4, "ann", "EXIT_1", false)}
	for name, tc := range map[string]struct {
		earlier []*orkstackv1.RunResume
		err     error
		want    string
		fails   bool
	}{
		"listed":  {earlier: earlier, want: "ann=EXIT_1"},
		"gone":    {err: artifacts.ErrNotFound},
		"damaged": {err: errEndingDamaged},
		"down":    {err: errors.New("store down"), fails: true},
	} {
		t.Run(name, func(t *testing.T) {
			reader, _ := resumedHistory(t, 7, completed, carriedOn(7, 6)...)
			reader = reader.WithEndings(&fakeEndings{resumes: tc.earlier, err: tc.err})
			run, err := reader.GetRun(context.Background(), "ork-r")
			if tc.fails {
				if !errors.Is(err, ErrEventSegmentUnavailable) {
					t.Fatalf("= %v, want %v", err, ErrEventSegmentUnavailable)
				}
				return
			}
			if err != nil || resumesText(run.GetResumes()) != tc.want {
				t.Fatalf("= %q, %v, want %q", resumesText(run.GetResumes()), err, tc.want)
			}
		})
	}
}

// TestArchiveEnding_ListsEveryResume: each ending's body lists every resume up
// to it and how each ended, and the caller's run is left as it was.
func TestArchiveEnding_ListsEveryResume(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	a := NewRunArchiver(store)
	ending := func(scope uint64, by string) *orkstackv1.Run {
		run := &orkstackv1.Run{Id: "ork-r", Namespace: "default", OrkflowName: "wf", OrkflowVersion: "1", Status: orkstackv1.Status_STATUS_FAILED,
			Error: &orkstackv1.Error{Code: "EXIT_1"}, CreatedAt: timestamppb.New(readerEpoch), FinishedAt: timestamppb.New(readerEpoch)}
		if by != "" {
			run.Resumes = []*orkstackv1.RunResume{resumeEntry(scope, by)}
		}
		return run
	}
	for _, e := range []struct {
		scope uint64
		by    string
		want  string
	}{{0, "", ""}, {7, "ann", "ann=EXIT_1(nothing reran)"}, {20, "bob", "ann=EXIT_1(nothing reran) bob=EXIT_1(nothing reran)"}} {
		if e.scope > 0 {
			if err := artifacts.RunArchive(store).(artifacts.RunEndingStore).MarkResumed(ctx, "ork-r", e.scope, e.scope/20*7); err != nil {
				t.Fatal(err)
			}
		}
		run := ending(e.scope, e.by)
		if err := a.ArchiveEnding(ctx, run, e.scope, nil); err != nil {
			t.Fatal(err)
		}
		if len(run.GetResumes()) > 1 || (len(run.GetResumes()) == 1 && run.GetResumes()[0].GetEnded() != nil) {
			t.Fatalf("the caller's run was changed: %q", resumesText(run.GetResumes()))
		}
		got, err := a.LoadRun(ctx, "ork-r")
		if err != nil || resumesText(got.GetResumes()) != e.want {
			t.Fatalf("ending %d lists %q, %v, want %q", e.scope, resumesText(got.GetResumes()), err, e.want)
		}
	}
	// What a read answers is its caller's to change.
	held, err := a.EarlierResumes(ctx, "ork-r", 20)
	if err != nil {
		t.Fatal(err)
	}
	resumesOf(held, &orkstackv1.Run{}, 20)[0].Principal = "changed"
	listed, err := a.EarlierResumes(ctx, "ork-r", 20)
	if err != nil || resumesText(listed) != "ann=EXIT_1(nothing reran)" {
		t.Fatalf("before 20 = %q, %v", resumesText(listed), err)
	}
}

// TestEarlierEnding_OfAnotherRun is damage, not this run's past.
func TestEarlierEnding_OfAnotherRun(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	body, err := MarshalRunArchive(&orkstackv1.Run{Id: "ork-other"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutRun(ctx, artifacts.RunRecord{RunID: "ork-r", Namespace: "default", Orkflow: "wf", Status: "failed", Time: readerEpoch, JSONL: body}); err != nil {
		t.Fatal(err)
	}
	a := NewRunArchiver(store)
	if _, err := a.EarlierResumes(ctx, "ork-r", 7); !errors.Is(err, errEndingDamaged) {
		t.Errorf("its resumes = %v", err)
	}
	if _, _, err := a.EarlierEnding(ctx, "ork-r", 7); !errors.Is(err, errEndingDamaged) {
		t.Errorf("its events = %v", err)
	}
}
