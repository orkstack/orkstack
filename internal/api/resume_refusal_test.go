package api

import (
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// TestResumeRefusalError: a refusal reaches the caller as FailedPrecondition
// RUN_NOT_RESUMABLE with a ResumeRefusal detail the CLI and UI read by class
// and cause: reason, class, retry_after_seconds (rounded up, never 0 for a
// wait that has one), cause, tasks, message and the plan diff.
func TestResumeRefusalError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		refusal resumeRefusal
		want    *orkstackv1.ResumeRefusal
	}{
		{"permanent", resumeRefusal{ResumeRefusal: engine.ResumeRefusal{Code: engine.RefuseTooManyResumes,
			Class: engine.RefusalPermanent, Message: "resumed 20 times"}},
			&orkstackv1.ResumeRefusal{Reason: "TOO_MANY_RESUMES", Class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT,
				Message: "resumed 20 times"}},
		{"wait, with its retry", resumeRefusal{ResumeRefusal: engine.ResumeRefusal{Code: resumeUnconfirmed, Class: engine.RefusalWait,
			Cause: causeResetOutcomeUnknown, Message: "sent"}, RetryAfter: 1500 * time.Millisecond},
			&orkstackv1.ResumeRefusal{Reason: "RESUME_UNCONFIRMED", Class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT,
				RetryAfterSeconds: 2, Cause: "reset_outcome_unknown", Message: "sent"}},
		{"replan, with its diff and tasks", resumeRefusal{ResumeRefusal: engine.ResumeRefusal{Code: resumePlanChanged, Class: engine.RefusalReplan,
			Tasks: []string{"b"}, Message: "the plan changed"},
			Diff: &orkstackv1.ResumeDiff{Added: []*orkstackv1.ResumeUnitRef{{TaskId: "b"}}}},
			&orkstackv1.ResumeRefusal{Reason: "PLAN_CHANGED", Class: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_REPLAN,
				Tasks: []string{"b"}, Message: "the plan changed", Diff: &orkstackv1.ResumeDiff{Added: []*orkstackv1.ResumeUnitRef{{TaskId: "b"}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.refusal.connectError()
			var ce *connect.Error
			if !errors.As(err, &ce) || ce.Code() != connect.CodeFailedPrecondition {
				t.Fatalf("err = %v, want FailedPrecondition", err)
			}
			got, ok := refusalDetail(ce)
			if !ok {
				t.Fatalf("no ResumeRefusal detail on %v", err)
			}
			if !proto.Equal(got, tc.want) {
				t.Fatalf("detail = %v, want %v", got, tc.want)
			}
			if !strings.Contains(ce.Message(), "RUN_NOT_RESUMABLE") || !strings.Contains(ce.Message(), tc.want.GetReason()) {
				t.Fatalf("message %q does not name RUN_NOT_RESUMABLE and %s", ce.Message(), tc.want.GetReason())
			}
		})
	}
}

// refusalDetail is the ResumeRefusal a connect error carries.
func refusalDetail(ce *connect.Error) (*orkstackv1.ResumeRefusal, bool) {
	for _, d := range ce.Details() {
		v, err := d.Value()
		if err != nil {
			continue
		}
		if r, ok := v.(*orkstackv1.ResumeRefusal); ok {
			return r, true
		}
	}
	return nil, false
}
