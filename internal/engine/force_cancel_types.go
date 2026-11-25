package engine

import "time"

// ForceCancelWorkflowType is the force cancel's workflow type.
const ForceCancelWorkflowType = "ForceCancelWorkflow"

// The force cancel's local activities, run on the engine worker only.
const (
	ActivityFCVerify    = "ForceCancelVerify"
	ActivityFCRefuse    = "ForceCancelRefuse"
	ActivityFCAudit     = "ForceCancelAudit"
	ActivityFCDescribe  = "ForceCancelDescribe"
	ActivityFCCancel    = "ForceCancelCancel"
	ActivityFCScan      = "ForceCancelScan"
	ActivityFCGates     = "ForceCancelGates"
	ActivityFCTerminate = "ForceCancelTerminate"
	ActivityFCClassify  = "ForceCancelClassify"
	ActivityFCPublish   = "ForceCancelPublish"
)

// forceCancelStepAttempts is how many times one step is tried.
const forceCancelStepAttempts = 3

// forceCancelTerminateTries bounds the terminations of a run that keeps
// moving to a newer execution under them (design §22.1.2).
const forceCancelTerminateTries = 3

// forceCancelSettleFirst and forceCancelSettleMax space the retries of what a
// terminated run still needs (its classification, its ending, its audit).
const (
	forceCancelSettleFirst = 5 * time.Second
	forceCancelSettleMax   = time.Minute
	// forceCancelPatience is how long any step is tried, however much of its
	// shared wait is spent.
	forceCancelPatience = 2 * time.Minute
	// forceCancelSettleLeast is how long at least what an ended run needs is
	// tried, however short its ceiling.
	forceCancelSettleLeast = 10 * time.Minute
)

// forceCancelPublishTimeout bounds one build of a run's ending, which reads
// the whole run; it heartbeats every third of forceCancelPublishHeartbeat.
const (
	forceCancelPublishTimeout   = 10 * time.Minute
	forceCancelPublishStart     = 2 * time.Minute
	forceCancelPublishHeartbeat = 30 * time.Second
)

// The states a force cancel ends in (design §22.1.1).
const (
	FCStateForceCanceled = "force_canceled"
	FCStateCanceled      = "canceled"
	FCStateEnded         = "ended"
	FCStateAlreadyEnded  = "already_ended"
	FCStateFailed        = "failed"
)

// The causes a force cancel says beside its state (design §22.1.4 and later).
// Every one but the last three says the run was not ended by this force
// cancel as far as it can tell; the last three say what is still missing of a
// run it did end.
const (
	FCCauseDecisionUnverified = "decision_unverified"
	FCCauseDecisionStale      = "decision_stale"
	FCCauseKeysUnavailable    = "keys_unavailable"
	FCCauseAuditUnwritable    = "audit_unwritable"
	FCCauseRecordTooLarge     = "record_too_large"
	FCCauseTextRefused        = "text_refused"
	FCCauseClockSkew          = "clock_skew"
	FCCauseRunKeptSwitching   = "run_kept_switching"
	FCCauseRetriesExhausted   = "retries_exhausted"
	FCCauseStopped            = "stopped"
	FCCauseUnknownKey         = "unknown_key"
	FCCauseUnverifiedRecord   = "unverified_record"
	FCCauseUnclassified       = "terminated_unclassified"
	FCCauseEventSequence      = "event_sequence"
	FCCauseEventUnpublished   = "event_unpublished"
)

// ForceCancelDecision is what the API decided, signed (fc.root-decision.v1)
// and bound to the run. It carries everything the force cancel acts on and
// everything its record repeats, so nothing a copied decision is started with
// beside it changes either.
type ForceCancelDecision struct {
	RequestID            string    `json:"request_id"`
	Namespace            string    `json:"namespace"`
	RunID                string    `json:"run_id"`
	TargetExecutionRunID string    `json:"target_execution_run_id"`
	Cascade              bool      `json:"cascade,omitempty"`
	Principal            string    `json:"principal"`
	PrincipalVerified    bool      `json:"principal_verified"`
	AuthMethod           string    `json:"auth_method,omitempty"`
	PrincipalIssuer      string    `json:"principal_issuer,omitempty"`
	Permission           string    `json:"permission"`
	Scope                string    `json:"scope"`
	Rule                 string    `json:"rule,omitempty"`
	PolicyGeneration     int64     `json:"policy_generation,omitempty"`
	Reason               string    `json:"reason"`
	GraceSeconds         int64     `json:"grace_seconds"`
	CeilingSeconds       int64     `json:"ceiling_seconds"`
	IssuedAt             time.Time `json:"issued_at"`
	ExpiresAt            time.Time `json:"expires_at"`
	// Of is the force cancel that cascades to this run; nil for one a caller
	// asked for. The request id is then that one's, "-" and a nonce.
	Of *ForceCancelOf `json:"of,omitempty"`
}

// ForceCancelOf is the execution a cascaded force cancel is a child of, and the
// run its cascade began at.
type ForceCancelOf struct {
	RunID      string `json:"run_id"`
	WorkflowID string `json:"workflow_id"`
	Execution  string `json:"execution"`
}

// Grace is the stall window; Ceiling how long a progressing run is waited for.
func (d ForceCancelDecision) Grace() time.Duration {
	return time.Duration(d.GraceSeconds) * time.Second
}

// Ceiling is how long the run may keep making progress before it is ended.
func (d ForceCancelDecision) Ceiling() time.Duration {
	return time.Duration(d.CeilingSeconds) * time.Second
}

// WorkflowID is the one workflow id this decision acts under.
func (d ForceCancelDecision) WorkflowID() string {
	return ForceCancelWorkflowIDPrefix(d.Namespace, d.RunID) + d.RequestID
}

// ForceCancelInput is one force cancel: its decision, signed, or, where this
// deployment signs nothing, unsigned; and the settings pinned at its start.
// Nothing else says what to do: anyone who can start a workflow on the
// cluster can start this one.
type ForceCancelInput struct {
	Decision string               `json:"decision,omitempty"`
	Unsigned *ForceCancelDecision `json:"unsigned,omitempty"`
	// Derived is the decision of a cascaded force cancel, signed as one, in
	// place of Decision.
	Derived       string        `json:"derived,omitempty"`
	ScanEvents    int           `json:"scan_events"`
	PageSize      int           `json:"page_size"`
	RecordTimeout time.Duration `json:"record_timeout"`
	// Timings are what its workflow spaces and bounds its steps with.
	Timings ForceCancelTimings `json:"timings"`
	// Settings are what a cascade's own force cancels are decided with.
	Settings *ForceCancelSettings `json:"settings,omitempty"`
}

// forceCancelAttemptMax bounds one attempt of a step whatever the input says.
const forceCancelAttemptMax = 5 * time.Minute

// attempt is how long one attempt of a step may take.
func (in ForceCancelInput) attempt() time.Duration {
	return min(max(in.RecordTimeout, time.Second), forceCancelAttemptMax)
}

// ForceCancelResult is what a force cancel ended in. Its flags are for
// display: whether a later request finishes a force cancel is decided from
// the run, never from them.
type ForceCancelResult struct {
	State                string `json:"state"`
	Cause                string `json:"cause,omitempty"`
	RequestID            string `json:"request_id,omitempty"`
	ProgressingAtCeiling bool   `json:"progressing_at_ceiling,omitempty"`
	// EndedBy is the request whose record the run closed with, when it is not
	// this one: a force cancel that ended it before, finished here.
	EndedBy string `json:"ended_by,omitempty"`
	// Finishing: it did what that one left undone, or tried to.
	Finishing bool `json:"finishing,omitempty"`
	// Ends: its termination closed a run that had announced its own end, which
	// State is then, so that end and its slots are owed all the same.
	Ends bool `json:"ends,omitempty"`
	// Terminated: a termination of the run was sent, or may have been: its
	// answer was lost.
	Terminated bool `json:"terminated,omitempty"`
	// Released: every gate the run held or queued at was told it is gone.
	Released          bool `json:"released,omitempty"`
	TerminalPublished bool `json:"terminal_published,omitempty"`
	// Unaudited: a record of this force cancel could not be written.
	Unaudited bool `json:"unaudited,omitempty"`
	// Cascade is what became of the runs below, of one that cascades.
	Cascade *ForceCancelCascade `json:"cascade,omitempty"`
}

// succeeded is a result that misses nothing.
func (r ForceCancelResult) succeeded() bool {
	return r.State != FCStateFailed && r.Cause == "" && !r.lacks()
}

// lacks reports a run that ended by a force cancel and is owed something of
// its end.
func (r ForceCancelResult) lacks() bool {
	return (r.State == FCStateForceCanceled || r.Finishing || r.Ends) && !(r.Released && r.TerminalPublished)
}

// detail is the result in one line, for its record.
func (r ForceCancelResult) detail() string {
	out := r.State
	if r.EndedBy != "" {
		out += " by request " + r.EndedBy
	}
	for _, part := range []struct {
		is   bool
		text string
	}{{r.Terminated, "terminated, or may have been"}, {r.ProgressingAtCeiling, "still progressing at its ceiling"},
		{r.Released, "slots released"}, {r.TerminalPublished, "ending published"}} {
		if part.is {
			out += ", " + part.text
		}
	}
	return out
}

// FCVerified is step 0's answer: the decision that verified, or why none did.
type FCVerified struct {
	Decision *ForceCancelDecision `json:"decision,omitempty"`
	Cause    string               `json:"cause,omitempty"`
}

// FCStep is what every acting step is given: the input its decision is
// verified from again, and the execution and bounds it acts on.
type FCStep struct {
	Input                ForceCancelInput `json:"input"`
	ExecutionRunID       string           `json:"execution_run_id,omitempty"`
	L0                   int64            `json:"l0,omitempty"`
	ProgressingAtCeiling bool             `json:"progressing_at_ceiling,omitempty"`
}

// FCAuditInput is one record of a force cancel, written once.
type FCAuditInput struct {
	Input  ForceCancelInput  `json:"input"`
	Kind   string            `json:"kind"`
	Result ForceCancelResult `json:"result"`
}

// FCDescribed is the run as the cluster describes it now.
type FCDescribed struct {
	Open           bool   `json:"open"`
	ExecutionRunID string `json:"execution_run_id"`
}

// FCCanceled is where the ordinary cancel was recorded: its event's id in
// execution ExecutionRunID, or that the run had closed.
type FCCanceled struct {
	L0             int64  `json:"l0"`
	ExecutionRunID string `json:"execution_run_id"`
	Closed         bool   `json:"closed,omitempty"`
	// Asked: the cluster took the request, so a run found closed may have
	// ended on it.
	Asked bool `json:"asked,omitempty"`
	// Cause is why the run was not asked: it is not the one decided on.
	Cause string `json:"cause,omitempty"`
}

// FCScanned is a stall test's answer and the execution it read.
type FCScanned struct {
	Stall          ForceCancelStall `json:"stall"`
	ExecutionRunID string           `json:"execution_run_id"`
	Open           bool             `json:"open"`
}

// FCGates is what the run holds or queues at. Unreadable: some of it could
// not be read, so a release of what was read is not a release of it all.
type FCGates struct {
	Gates      []string `json:"gates,omitempty"`
	SeqBase    int64    `json:"seq_base"`
	Unreadable bool     `json:"unreadable,omitempty"`
}

// FCTerminated is that a termination was sent, or why it was not: a cause, or
// that the run's own history no longer shows it stalled. Empty, the execution
// had closed before.
type FCTerminated struct {
	Sent        bool   `json:"sent,omitempty"`
	Cause       string `json:"cause,omitempty"`
	Progressing bool   `json:"progressing,omitempty"`
	// Unsent is why the call could not be made, or was refused.
	Unsent string `json:"unsent,omitempty"`
}

// FCClassified is the run's state: open, or how it ended. Retry: it could not
// be told now and may be later.
type FCClassified struct {
	Open  bool   `json:"open,omitempty"`
	State string `json:"state,omitempty"`
	Cause string `json:"cause,omitempty"`
	Retry bool   `json:"retry,omitempty"`
	// Request is whose record the close carries, verified, when it is another
	// request's.
	Request string `json:"request,omitempty"`
}

// FCPublished is what to publish, or why there is nothing to.
type FCPublished struct {
	// Event is the event's own bytes.
	Event       []byte `json:"event,omitempty"`
	SnapshotURI string `json:"snapshot_uri,omitempty"`
	Cause       string `json:"cause,omitempty"`
	// Ended: the run announced its own end before it was terminated, and
	// reads as it said. Event is then that end, when its announcement never
	// finished, under the run's own DedupeKey.
	Ended     bool   `json:"ended,omitempty"`
	DedupeKey string `json:"dedupe_key,omitempty"`
}
