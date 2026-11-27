package api

import (
	"fmt"
	"math"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// ResumeRun's own refusal codes, beside the planner's (engine.Refuse*):
// those that only the API's checks, made outside the planner's pure scope,
// can reach (resume design §10, §12.2; docs/REASONS.md).
const (
	resumeUnconfirmed      engine.ResumeRefusalCode = "RESUME_UNCONFIRMED"
	resumePlanChanged      engine.ResumeRefusalCode = "PLAN_CHANGED"
	resumeRunChanged       engine.ResumeRefusalCode = "RUN_CHANGED"
	resumeInProgress       engine.ResumeRefusalCode = "RESUME_IN_PROGRESS"
	resumePlanStranded     engine.ResumeRefusalCode = "RESUME_PLAN_STRANDED"
	resumeStillStopping    engine.ResumeRefusalCode = "STILL_STOPPING"
	resumeHistoryGone      engine.ResumeRefusalCode = "HISTORY_UNAVAILABLE"
	resumeRetentionEnding  engine.ResumeRefusalCode = "RETENTION_ENDING"
	resumeUnknownAttempts  engine.ResumeRefusalCode = "UNKNOWN_ATTEMPTS_NOT_ACKNOWLEDGED"
	resumeSequenceExhaust  engine.ResumeRefusalCode = "RESUME_SEQUENCE_EXHAUSTED"
	resumeReleaseUnsupport engine.ResumeRefusalCode = "RELEASE_UNSUPPORTED"
	resumeReleaseDown      engine.ResumeRefusalCode = "RELEASE_UNAVAILABLE"
	resumePoolUnserved     engine.ResumeRefusalCode = "RERUN_POOL_UNSERVED"
	resumeImagePolicy      engine.ResumeRefusalCode = "IMAGE_POLICY_CHANGED"
	resumeTaskOverCap      engine.ResumeRefusalCode = "TASK_OVER_CAP"
	resumeIdentityPolicy   engine.ResumeRefusalCode = "IDENTITY_POLICY_CHANGED"
	resumeStoreUnscoped    engine.ResumeRefusalCode = "RERUN_STORE_UNSCOPED"
)

// The causes RESUME_UNCONFIRMED carries, which the CLI and UI read to say
// whether the reset is known to have been made (decision S6b PR5-2).
const (
	// causeResetOutcomeUnknown: the reset was sent and no answer says
	// whether it was made.
	causeResetOutcomeUnknown = "reset_outcome_unknown"
	// causeNotCarriedOn: the reset was made and the run has not carried on
	// within the confirm wait.
	causeNotCarriedOn = "not_carried_on"
	// causeResumeEnded: the resume was made, and its run could not carry the
	// plan out.
	causeResumeEnded = "resume_ended"
)

// resumeRefusal is a refusal ResumeRun answers: the planner's, or one of
// its own, with what only the API knows, how long to wait and the plan's
// diff.
type resumeRefusal struct {
	engine.ResumeRefusal
	RetryAfter time.Duration
	Diff       *orkstackv1.ResumeDiff
	// Highest is the run's highest stored event, when a gap below it is what
	// must be accepted.
	Highest int64
}

// refuseResume is one of ResumeRun's own refusals.
func refuseResume(code engine.ResumeRefusalCode, class engine.ResumeRefusalClass, format string, args ...any) *resumeRefusal {
	return &resumeRefusal{ResumeRefusal: engine.ResumeRefusal{Code: code, Class: class, Message: fmt.Sprintf(format, args...)}}
}

func (r *resumeRefusal) Error() string { return r.ResumeRefusal.Error() }

// refusalClasses maps a planner class to the wire's.
var refusalClasses = map[engine.ResumeRefusalClass]orkstackv1.ResumeRefusalClass{
	engine.RefusalPermanent: orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT,
	engine.RefusalWait:      orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT,
	engine.RefusalReplan:    orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_REPLAN,
}

// connectError is the refusal on the wire: FailedPrecondition
// RUN_NOT_RESUMABLE, with a ResumeRefusal detail the clients read by class
// and cause (§10.4). A retry_after is rounded up to whole seconds, so a wait
// never reads as "now".
func (r *resumeRefusal) connectError() error {
	detail := &orkstackv1.ResumeRefusal{
		Reason:  string(r.Code),
		Class:   refusalClasses[r.Class],
		Cause:   r.Cause,
		Tasks:   r.Tasks,
		Message: r.Message,
		Diff:    r.Diff,

		HighestSequence: r.Highest,
	}
	if r.RetryAfter > 0 {
		detail.RetryAfterSeconds = int64(math.Ceil(r.RetryAfter.Seconds()))
	}
	err := connect.NewError(connect.CodeFailedPrecondition,
		fmt.Errorf("RUN_NOT_RESUMABLE (%s): %s", r.Code, r.Message))
	if d, derr := connect.NewErrorDetail(detail); derr == nil {
		err.AddDetail(d)
	}
	return err
}
