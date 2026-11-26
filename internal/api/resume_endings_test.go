package api_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// fakeResumeEndings are a run's archived endings and the marks of its resumes,
// in the order they were written.
type fakeResumeEndings struct {
	// mu guards every field: concurrent confirms call it at once.
	mu       sync.Mutex
	archived map[uint64]bool
	// steps is what was done, in order.
	steps []string
	// hasErr, archiveErr and markErr refuse each; hang holds the archive write
	// until its context ends. without leaves an archive write with no body.
	hasErr, archiveErr, markErr error
	hang, without               bool
	// asked is the ending last asked about, which an archive write archives,
	// unless scope names another.
	asked, scope uint64
	// body is the run an ending's body is written from, when it is first
	// archived or read; bodies are those written, never changed after.
	body    func() *orkstackv1.Run
	bodies  map[uint64][]byte
	bodyErr error
	// readWithin is how long a body may be read for.
	readWithin time.Duration
	// setAside is the ending the last mark named.
	setAside uint64
}

func (f *fakeResumeEndings) did(step string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.steps = append(f.steps, step)
}

func (f *fakeResumeEndings) HasEnding(_ context.Context, _ string, scope uint64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = scope
	return f.archived[scope], f.hasErr
}

func (f *fakeResumeEndings) GetEnding(ctx context.Context, _ string, scope uint64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if at, ok := ctx.Deadline(); ok {
		f.readWithin = time.Until(at)
	}
	if f.bodyErr != nil {
		return nil, f.bodyErr
	}
	return f.bodyOf(scope)
}

// bodyOf is the body archived under scope, under mu: written once, the first
// time it is archived or read, as a body is immutable.
func (f *fakeResumeEndings) bodyOf(scope uint64) ([]byte, error) {
	if body, ok := f.bodies[scope]; ok {
		return body, nil
	}
	body, err := engine.MarshalRunArchive(f.body(), nil)
	if err != nil {
		return nil, err
	}
	if f.bodies == nil {
		f.bodies = map[uint64][]byte{}
	}
	f.bodies[scope] = body
	return body, nil
}

func (f *fakeResumeEndings) MarkResumed(_ context.Context, _ string, scope, setAside uint64) error {
	f.did("mark")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setAside = setAside
	return f.markErr
}

func (f *fakeResumeEndings) ArchiveFromHistory(ctx context.Context, _ string) error {
	f.did("archive")
	f.mu.Lock()
	hang, archiveErr, without := f.hang, f.archiveErr, f.without
	f.mu.Unlock()
	if hang {
		<-ctx.Done()
		return ctx.Err()
	}
	if archiveErr == nil && !without {
		f.mu.Lock()
		defer f.mu.Unlock()
		scope := f.asked
		if f.scope != 0 {
			scope = f.scope
		}
		f.archived[scope] = true
		if _, err := f.bodyOf(scope); err != nil {
			return err
		}
	}
	return archiveErr
}

// TestResumeFixture_AnArchivedEndingIsWrittenOnce: the ending the fixture's
// archive holds stays as it was written while a reset changes the run, as an
// archive body does, read by confirms at once.
func TestResumeFixture_AnArchivedEndingIsWrittenOnce(t *testing.T) {
	f := newResumeFixture(t, nil)
	f.resets(true)
	ctx := context.Background()
	before, err := f.endings.GetEnding(ctx, "ork-r", 0)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := f.cluster.ResetForResume(ctx, engine.ResumeReset{BaseExecutionRunID: "exec-1",
			Reason: engine.ResumePlanKey{Scope: 10, PlanDigest: strings.Repeat("a", 64)}.String()})
		done <- err
	}()
	during, err := f.endings.GetEnding(ctx, "ork-r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	after, err := f.endings.GetEnding(ctx, "ork-r", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range [][]byte{during, after} {
		run, err := engine.UnmarshalRunLine(body)
		if err != nil || run.GetStatus() != orkstackv1.Status_STATUS_FAILED || string(body) != string(before) {
			t.Fatalf("the archived ending reads %v, %v after a reset", run.GetStatus(), err)
		}
	}
}

// TestResumeRun_ArchivesWhatItSetsAside: a run is reset only once the ending it
// sets aside is archived and the run is marked, in that order; a preview does
// neither.
func TestResumeRun_ArchivesWhatItSetsAside(t *testing.T) {
	for name, tc := range map[string]struct {
		archived bool
		want     string
	}{
		"archived already": {true, "mark reset"},
		"not yet":          {false, "archive mark reset"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, func(f *resumeFixture) { f.endings.archived[0] = tc.archived })
			f.resets(true)
			reset := f.cluster.onReset
			f.cluster.onReset = func(r engine.ResumeReset) (string, error) { f.endings.did("reset"); return reset(r) }
			f.preview(t)
			if len(f.endings.steps) != 0 {
				t.Fatalf("a preview did %v", f.endings.steps)
			}
			if _, err := f.confirm(t, nil); err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(f.endings.steps, " "); got != tc.want {
				t.Fatalf("did %q, want %q", got, tc.want)
			}
			// Reading the body back is bounded, and not by the write's ten
			// seconds: it is as large as the run's stream.
			if f.endings.readWithin < 15*time.Second || f.endings.readWithin > 30*time.Second {
				t.Fatalf("the archived ending may be read for %v", f.endings.readWithin)
			}
		})
	}
}

// TestResumeRun_AnEndingThatCannotBeArchived: nothing is recorded, marked or
// reset while the ending set aside is not archived, however its write fails,
// and the caller is told to wait, never left hanging.
func TestResumeRun_AnEndingThatCannotBeArchived(t *testing.T) {
	for name, change := range map[string]func(*fakeResumeEndings){
		"the question":               func(e *fakeResumeEndings) { e.hasErr = errors.New("s3://bucket refused") },
		"the write":                  func(e *fakeResumeEndings) { e.archiveErr = errors.New("s3://bucket refused") },
		"a write that hangs":         func(e *fakeResumeEndings) { e.hang = true },
		"its body":                   func(e *fakeResumeEndings) { e.archived[0], e.bodyErr = true, errors.New("s3://bucket refused") },
		"a write that left no body":  func(e *fakeResumeEndings) { e.without = true },
		"a write of another ending ": func(e *fakeResumeEndings) { e.scope = 99 },
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, func(f *resumeFixture) {
				quickConfirm(f)
				f.endings.archived = map[uint64]bool{}
				change(f.endings)
			})
			f.resets(true)
			_, err := f.confirm(t, nil)
			r := refusalOf(t, err)
			if r.GetReason() != "RESUME_CHECK_UNAVAILABLE" || r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT ||
				r.GetCause() != "ending_unarchived" || r.GetRetryAfterSeconds() <= 0 || strings.Contains(r.GetMessage(), "s3://") {
				t.Fatalf("refusal = %v", r)
			}
			refs, err := f.records.ListResumePlans(context.Background(), "default", "ork-r")
			if err != nil || len(refs) != 0 || len(f.cluster.resets) != 0 || slices.Contains(f.endings.steps, "mark") {
				t.Fatalf("plans %v (%v), %d resets, did %v; want nothing begun", refs, err, len(f.cluster.resets), f.endings.steps)
			}
		})
	}
	gone := newResumeFixture(t, func(f *resumeFixture) { f.endings.archived = map[uint64]bool{}; f.endings.hang = true })
	ctx, leave := context.WithCancel(context.Background())
	leave()
	if _, err := gone.svc.ResumeRun(ctx, connect.NewRequest(&orkstackv1.ResumeRunRequest{RunId: "ork-r", ExpectedTerminalSequence: 9})); connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("a caller who left = %v, want Canceled", err)
	}
}

// TestResumeRun_AMarkThatCannotBeWritten: no reset is sent without it.
func TestResumeRun_AMarkThatCannotBeWritten(t *testing.T) {
	f := newResumeFixture(t, func(f *resumeFixture) { f.endings.markErr = errors.New("refused") })
	f.resets(true)
	_, err := f.confirm(t, nil)
	if r := refusalOf(t, err); r.GetReason() != "RESUME_CHECK_UNAVAILABLE" || len(f.cluster.resets) != 0 {
		t.Fatalf("refusal = %v, %d resets", r, len(f.cluster.resets))
	}
}

// TestResumeRun_AnotherEndingArchived: a run whose archived ending is not the
// one a resume would set aside (an execution reset by cluster tooling) is
// refused, with nothing recorded, marked or reset.
func TestResumeRun_AnotherEndingArchived(t *testing.T) {
	for name, change := range map[string]func(*orkstackv1.Run){
		"when":   func(r *orkstackv1.Run) { r.FinishedAt = timestamppb.New(resumeEpoch.Add(-time.Hour)) },
		"how":    func(r *orkstackv1.Run) { r.Status = orkstackv1.Status_STATUS_CANCELED },
		"why":    func(r *orkstackv1.Run) { r.Error = &orkstackv1.Error{Code: "EXIT_1"} },
		"of who": func(r *orkstackv1.Run) { r.Id = "ork-other" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, nil)
			other := proto.Clone(f.run).(*orkstackv1.Run)
			change(other)
			f.endings.body = func() *orkstackv1.Run { return other }
			f.resets(true)
			_, err := f.confirm(t, nil)
			r := refusalOf(t, err)
			if r.GetReason() != "STATE_UNREADABLE" || r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT || len(f.endings.steps) != 0 {
				t.Fatalf("refused %v, did %v", r, f.endings.steps)
			}
		})
	}
}

// TestResumeRun_AMarkTaken: a mark that names another ending is a refusal for
// good, with nothing reset, and one that could not be written is a wait.
func TestResumeRun_AMarkTaken(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		reason string
		class  orkstackv1.ResumeRefusalClass
	}{
		"taken":       {fmt.Errorf("mark: %w", artifacts.ErrResumeMarkTaken), "STATE_UNREADABLE", orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT},
		"not written": {errors.New("s3://bucket refused"), "RESUME_CHECK_UNAVAILABLE", orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, func(f *resumeFixture) { f.endings.markErr = tc.err })
			f.cluster.onReset = func(engine.ResumeReset) (string, error) {
				t.Error("the run was reset")
				return "", errors.New("reset")
			}
			_, err := f.confirm(t, nil)
			if r := refusalOf(t, err); r.GetReason() != tc.reason || r.GetClass() != tc.class || strings.Contains(r.GetMessage(), "s3://") {
				t.Fatalf("refused %v", r)
			}
		})
	}
}
