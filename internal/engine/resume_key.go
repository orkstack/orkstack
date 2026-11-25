package engine

import (
	"fmt"
	"strconv"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

// A resume travels as one reset of the run's closed execution (E85 §30.3):
// the reset's reason is the plan key, which names the immutable resume
// record ResumeRun wrote before the reset, and the reset run loads and
// verifies that record at R (LoadResumePlan). No signal carries it.

const (
	// resumeKeyFamily begins every plan key of every version: a reason that
	// begins with it is never a reset made by hand.
	resumeKeyFamily = "ork-resume:"
	resumeKeyPrefix = resumeKeyFamily + "v1:"
	// maxResumeKeyBytes bounds a plan key (§30.3).
	maxResumeKeyBytes = 100
	// maxResumeScope is the largest scope: a scope is an event sequence,
	// which stays within 2^53 − 1 so every JSON reader holds it exactly
	// (§28.15, RESUME_SEQUENCE_EXHAUSTED).
	maxResumeScope = 1<<53 - 1
	// maxResumePlanNumber is the highest plan number of a scope, from 0, as
	// the identity records number them (pkg/artifacts).
	maxResumePlanNumber = 998
)

// ResumePlanKey is what a reset's reason names: the resume's scope (its
// first seq_base), the plan number n within it, and the SHA-256 of the
// record's stored bytes.
type ResumePlanKey struct {
	Scope      uint64
	N          int
	PlanDigest string
}

// String renders the key as a reset's reason: "ork-resume:v1:<scope>.<n:03>:<digest>".
func (k ResumePlanKey) String() string {
	return fmt.Sprintf("%s%d.%03d:%s", resumeKeyPrefix, k.Scope, k.N, k.PlanDigest)
}

// ParseResumePlanKey reads a reset's reason. false for anything that is not
// a plan key exactly as String renders it: a cluster reset made by hand is
// not a resume, and its run carries on as a reset run does (docs/DEPLOY.md).
func ParseResumePlanKey(reason string) (ResumePlanKey, bool) {
	if len(reason) > maxResumeKeyBytes {
		return ResumePlanKey{}, false
	}
	rest, ok := strings.CutPrefix(reason, resumeKeyPrefix)
	if !ok {
		return ResumePlanKey{}, false
	}
	plan, digest, ok := strings.Cut(rest, ":")
	if !ok || !lowerHexDigest(digest) {
		return ResumePlanKey{}, false
	}
	scopeText, nText, ok := strings.Cut(plan, ".")
	if !ok || len(nText) != 3 || !decimal(scopeText) || !decimal(nText) {
		return ResumePlanKey{}, false
	}
	scope, err := strconv.ParseUint(scopeText, 10, 64)
	if err != nil || scope == 0 || scope > maxResumeScope || strconv.FormatUint(scope, 10) != scopeText {
		return ResumePlanKey{}, false
	}
	n, err := strconv.Atoi(nText)
	if err != nil || n > maxResumePlanNumber {
		return ResumePlanKey{}, false
	}
	return ResumePlanKey{Scope: scope, N: n, PlanDigest: digest}, true
}

func decimal(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// IsResumePlanDigest reports whether s is shaped as a plan digest: the
// lowercase hex SHA-256 a plan record is named by.
func IsResumePlanDigest(s string) bool { return lowerHexDigest(s) }

func lowerHexDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// ResetFacts is what the reset that made this run recorded (§30.3, T20):
// the base execution it reset, the reason it was given, and the ids of R's
// workflow task in that base.
type ResetFacts struct {
	BaseRunID        string `json:"base_run_id"`
	Reason           string `json:"reason"`
	ScheduledEventID int64  `json:"scheduled_event_id"`
	StartedEventID   int64  `json:"started_event_id"`
}

// findResetEvent is the reset event of run ownRunID among events, read in
// order: the WorkflowTaskFailed whose cause is a reset and whose new run is
// this one. A run reset again copies its base's prefix, which can hold the
// base's own reset event; that one names another new run and is passed by.
func findResetEvent(events []*historypb.HistoryEvent, ownRunID string) (ResetFacts, bool) {
	for _, e := range events {
		if e.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
			continue
		}
		a := e.GetWorkflowTaskFailedEventAttributes()
		if a.GetCause() != enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW || a.GetNewRunId() != ownRunID {
			continue
		}
		return ResetFacts{
			BaseRunID:        a.GetBaseRunId(),
			Reason:           a.GetFailure().GetMessage(),
			ScheduledEventID: a.GetScheduledEventId(),
			StartedEventID:   a.GetStartedEventId(),
		}, true
	}
	return ResetFacts{}, false
}
