package api_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestResumeRun_UnfinishedAttempts: an attempt that was claimed and never
// released is asked about, of the Orker that claimed it: one that still has
// it holds the resume back, one that has not says it is over, and one that
// does not answer, or answers with part of a list, leaves it unknown, never
// clear. It is asked about whichever execution of the run dispatched it: an
// earlier one's attempts are not in the history of the one a resume started.
func TestResumeRun_UnfinishedAttempts(t *testing.T) {
	running := func(tasks ...string) []*orkstackv1.RunningAttempt {
		var out []*orkstackv1.RunningAttempt
		for _, id := range tasks {
			out = append(out, &orkstackv1.RunningAttempt{RunId: "ork-r", TaskId: id, Ordinal: 1})
		}
		return out
	}
	for name, tc := range map[string]struct {
		claim   string // the task claimed, by orker-a
		item    artifacts.Item
		release bool
		answer  *orkstackv1.DescribeOrkerResult
		failure error
		ack     []string
		// wantReason, wantCause: the refusal of a confirm; "" goes on.
		wantReason, wantCause string
	}{
		"released":                  {claim: "b", release: true, failure: errors.New("gone")},
		"no task of the run":        {claim: "zz", failure: errors.New("gone")},
		"an earlier execution's":    {claim: "b", item: artifacts.ItemAt(3), failure: errors.New("gone"), wantReason: "UNKNOWN_ATTEMPTS_NOT_ACKNOWLEDGED"},
		"its Orker still has it":    {claim: "b", answer: &orkstackv1.DescribeOrkerResult{OrkerId: "orker-a", Running: running("x", "b")}, wantReason: "STILL_STOPPING", wantCause: "container_still_running"},
		"its Orker has others":      {claim: "b", answer: &orkstackv1.DescribeOrkerResult{OrkerId: "orker-a", Running: running("x")}},
		"its Orker lists part":      {claim: "b", answer: &orkstackv1.DescribeOrkerResult{OrkerId: "orker-a", Running: running("x"), RunningTruncated: true}, wantReason: "UNKNOWN_ATTEMPTS_NOT_ACKNOWLEDGED"},
		"it is in the part listed":  {claim: "b", answer: &orkstackv1.DescribeOrkerResult{OrkerId: "orker-a", Running: running("b"), RunningTruncated: true}, wantReason: "STILL_STOPPING", wantCause: "container_still_running"},
		"its Orker does not answer": {claim: "b", failure: errors.New("no poller"), wantReason: "UNKNOWN_ATTEMPTS_NOT_ACKNOWLEDGED"},
		"another Orker answers":     {claim: "b", answer: &orkstackv1.DescribeOrkerResult{OrkerId: "orker-b"}, wantReason: "UNKNOWN_ATTEMPTS_NOT_ACKNOWLEDGED"},
		"acknowledged":              {claim: "b", failure: errors.New("no poller"), ack: []string{"orker-a/b//1"}},
		"another one acknowledged":  {claim: "b", failure: errors.New("no poller"), ack: []string{"orker-a/a//1"}, wantReason: "UNKNOWN_ATTEMPTS_NOT_ACKNOWLEDGED"},
		"more than are unknown":     {claim: "b", failure: errors.New("no poller"), ack: []string{"orker-a/b//1", "orker-z/a//9"}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, func(f *resumeFixture) {
				f.fleet.described = map[string]*orkstackv1.DescribeOrkerResult{"orker-a": tc.answer}
				f.fleet.failures = map[string]error{"orker-a": tc.failure}
			})
			ref := artifacts.AttemptRef{RunID: "ork-r", TaskID: tc.claim, Item: tc.item}
			got, err := f.attempts.ClaimAttempt(context.Background(), ref, "orker-a")
			if err != nil {
				t.Fatal(err)
			}
			ref.Attempt = got.Ordinal
			if tc.release {
				if err := f.attempts.ReleaseAttempt(context.Background(), ref, "orker-a"); err != nil {
					t.Fatal(err)
				}
			}
			req := &orkstackv1.ResumeRunRequest{ExpectedTerminalSequence: 9, AcknowledgedUnknownAttempts: tc.ack}
			if len(tc.ack) > 0 {
				// The plan is the same whatever is still unknown.
				plain := newResumeFixture(t, nil)
				preview, err := plain.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
				if err != nil {
					t.Fatal(err)
				}
				req.ExpectedPlanDigest = preview.GetPlanDigest()
			}
			_, err = f.resume(req)
			if tc.wantReason == "" {
				if connect.CodeOf(err) == connect.CodeFailedPrecondition {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			r := refusalOf(t, err)
			if r.GetReason() != tc.wantReason || r.GetCause() != tc.wantCause || !slices.Contains(r.GetTasks(), tc.claim) ||
				!strings.Contains(r.GetMessage(), "orker-a") {
				t.Fatalf("refusal = %v, want %s %s naming task %s and orker-a", r, tc.wantReason, tc.wantCause, tc.claim)
			}
		})
	}
}

// TestResumeRun_UnfinishedAttemptsAskEachOrkerOnce: many claims of one Orker
// cost one question, and none is asked before the containers' bound.
func TestResumeRun_UnfinishedAttemptsAskEachOrkerOnce(t *testing.T) {
	f := newResumeFixture(t, func(f *resumeFixture) {
		f.fleet.described = map[string]*orkstackv1.DescribeOrkerResult{"orker-a": {OrkerId: "orker-a"}}
	})
	for _, id := range []string{"a", "b", "b"} {
		if _, err := f.attempts.ClaimAttempt(context.Background(), artifacts.AttemptRef{RunID: "ork-r", TaskID: id}, "orker-a"); err != nil {
			t.Fatal(err)
		}
	}
	_, _ = f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if !slices.Equal(f.fleet.describes, []string{"orker-a"}) {
		t.Fatalf("asked %v, want orker-a once", f.fleet.describes)
	}
	early := newResumeFixture(t, func(f *resumeFixture) { f.clock.set(resumeEpoch) })
	if _, err := early.attempts.ClaimAttempt(context.Background(), artifacts.AttemptRef{RunID: "ork-r", TaskID: "b"}, "orker-a"); err != nil {
		t.Fatal(err)
	}
	_, _ = early.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if len(early.fleet.describes) != 0 {
		t.Fatalf("asked %v before the bound", early.fleet.describes)
	}
}

// TestDescribeOrker_KeepsRunIDsFromItsAnswer: what an Orker runs is the
// engine's to read: the API's answer names no run of any namespace.
func TestDescribeOrker_KeepsRunIDsFromItsAnswer(t *testing.T) {
	svc := api.NewService(api.Config{Orkers: describerFunc(func(context.Context) (*orkstackv1.DescribeOrkerResult, error) {
		return &orkstackv1.DescribeOrkerResult{OrkerId: "orker-a", RunningTruncated: true,
			Running: []*orkstackv1.RunningAttempt{{RunId: "ork-secret", TaskId: "t", Ordinal: 1}}}, nil
	})})
	resp, err := svc.DescribeOrker(context.Background(), connect.NewRequest(&orkstackv1.DescribeOrkerRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Msg.GetOrker(); got.GetOrkerId() != "orker-a" || len(got.GetRunning()) != 0 || got.GetRunningTruncated() {
		t.Fatalf("answer = %v, want no running attempts", got)
	}
}

type describerFunc func(context.Context) (*orkstackv1.DescribeOrkerResult, error)

func (f describerFunc) DescribeOrker(ctx context.Context) (*orkstackv1.DescribeOrkerResult, error) {
	return f(ctx)
}

// failingClaims is an attempt store whose open claims cannot be listed.
type failingClaims struct {
	*artifacts.MemoryAttemptStore
	err error
}

func (s failingClaims) OpenAttemptClaims(context.Context, string) ([]artifacts.OpenAttemptClaim, error) {
	return nil, s.err
}

// TestResumeRun_TooManyUnfinishedAttempts: a run with more open attempts than
// one confirm can accept is told so for good, not asked to retry.
func TestResumeRun_TooManyUnfinishedAttempts(t *testing.T) {
	f := newResumeFixture(t, func(f *resumeFixture) { f.claimsErr = artifacts.ErrTooManyOpenClaims })
	_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if r := refusalOf(t, err); r.GetReason() != "RESUME_TOO_LARGE" || r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT {
		t.Fatalf("refusal = %v, want RESUME_TOO_LARGE, permanent", r)
	}
}
