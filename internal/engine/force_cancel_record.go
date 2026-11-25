package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A force cancel ends a run that would not end on its own by terminating it
// (AGENTS.md §7, "Force cancel"), and the termination carries the record of
// who decided it, signed by an engine's key: the one thing every reader of
// the run (GetRun, the projection, the archive sweep, the evidence bundle and
// a resume) reads its ending from. A termination without a record that
// verifies for this run and this execution is not a force cancel, whatever
// its reason says, and reads as the plain RUN_TERMINATED it always did.

// RunTerminatedCode is the ending of a run terminated from outside by no force
// cancel that verifies.
const RunTerminatedCode = "RUN_TERMINATED"

// RunForceCanceledCode is the run's ending after a force cancel.
const RunForceCanceledCode = "RUN_FORCE_CANCELED"

// RunForceCancelUnverifiedCode is the ending of a run archived while a force
// cancel's record in its close could not be verified: nobody is named, and
// the run is not resumed from it.
const RunForceCancelUnverifiedCode = "RUN_FORCE_CANCEL_UNVERIFIED"

// ForceCancelTerminateReason is the reason a force cancel's termination gives.
const ForceCancelTerminateReason = "orkstack force cancel"

// ForceCancelRecordFormat is the format of the record this release writes.
const ForceCancelRecordFormat = 2

// ForceCancelRecordMaxBytes bounds a record as the termination carries it,
// signature and all: a reason that would take it past this is refused before
// anything is ended (FORCE_CANCEL_REASON_TOO_LONG).
const ForceCancelRecordMaxBytes = 6 << 10

// ForceCancelRecord is what a force cancel's termination says of itself.
type ForceCancelRecord struct {
	Format                   int    `json:"format"`
	Namespace                string `json:"namespace"`
	RunID                    string `json:"run_id"`
	TerminatedExecutionRunID string `json:"terminated_execution_run_id"`
	// ResumeScope is E85's to set; nothing sets it yet, so an unset one says
	// nothing, and never that the run was not resumed.
	ResumeScope          int64     `json:"resume_scope,omitempty"`
	RequestID            string    `json:"request_id"`
	WorkflowID           string    `json:"workflow_id"`
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
	ProgressingAtCeiling bool      `json:"progressing_at_ceiling,omitempty"`
	At                   time.Time `json:"at"`
}

// forceCancelDetails is the termination's one detail: the signed record, or,
// where this deployment signs nothing, the record itself.
type forceCancelDetails struct {
	Signed   string             `json:"signed,omitempty"`
	Unsigned *ForceCancelRecord `json:"unsigned,omitempty"`
}

// ForceCancelWorkflowIDPrefix is how every force cancel of one run is named:
// `ork-force-<32 hex of sha256(namespace, 0, run)>-`, the request id after it.
func ForceCancelWorkflowIDPrefix(namespace, runID string) string {
	sum := sha256.Sum256([]byte(namespace + "\x00" + runID))
	return "ork-force-" + hex.EncodeToString(sum[:])[:32] + "-"
}

// forceCancelBind is what a record's signed body begins with: its run.
func forceCancelBind(namespace, runID string) []byte {
	return identity.Bind(namespace, runID)
}

// RecordSigner signs a decision of a kind (identity.Issuer).
type RecordSigner interface {
	SignDecision(kind jwt.DecisionKind, body []byte) (string, error)
}

// ForceCancelVerifier verifies what a record says and judges one that is
// unsigned (identity.Issuer).
type ForceCancelVerifier interface {
	VerifyDecision(ctx context.Context, msg string, kind jwt.DecisionKind, at time.Time, bind []byte) ([]byte, string, identity.Verdict)
	UnsignedRecord(mode string, closedAt time.Time) identity.UnsignedVerdict
}

// KeylessEngine verifies for an engine that holds no keys. It cannot verify a
// signed record; but in asserted mode, where no engine of the deployment ever
// published a key, nobody signed it, and it is bad rather than one to be
// decided later, which would leave the run unreadable for good. Where any key
// was published, or the published keys cannot be read, an engine that holds
// keys decides.
type KeylessEngine struct {
	none      *identity.Issuer // nil: what an engine without keys answers to the rest
	published PublishedKeys
	mode      func() string
}

// SignDecision signs nothing: an engine without keys cannot.
func (k KeylessEngine) SignDecision(kind jwt.DecisionKind, body []byte) (string, error) {
	return k.none.SignDecision(kind, body)
}

// UnsignedDecision is what an unsigned decision counts as in mode.
func (k KeylessEngine) UnsignedDecision(mode string) identity.UnsignedVerdict {
	return k.none.UnsignedDecision(mode)
}

// UnsignedRecord is what an unsigned record counts as in mode.
func (k KeylessEngine) UnsignedRecord(mode string, closedAt time.Time) identity.UnsignedVerdict {
	return k.none.UnsignedRecord(mode, closedAt)
}

// PublishedKeys reads the deployment's published keys document.
type PublishedKeys interface {
	Read(ctx context.Context) (artifacts.PublishedKeys, error)
}

// NewKeylessEngine returns it over the deployment's document and mode.
func NewKeylessEngine(published PublishedKeys, mode func() string) KeylessEngine {
	return KeylessEngine{published: published, mode: mode}
}

// keylessReadTimeout bounds one read of the published keys document.
const keylessReadTimeout = 5 * time.Second

func (k KeylessEngine) VerifyDecision(ctx context.Context, _ string, _ jwt.DecisionKind, _ time.Time, _ []byte) ([]byte, string, identity.Verdict) {
	if k.published == nil || k.mode == nil || k.mode() != "asserted" {
		return nil, "", identity.VerdictUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, keylessReadTimeout)
	defer cancel()
	doc, err := k.published.Read(ctx)
	if err != nil || len(doc.Keys) > 0 {
		return nil, "", identity.VerdictUnavailable
	}
	return nil, "", identity.VerdictBad
}

// ErrForceCancelRecordTooLarge is a record past ForceCancelRecordMaxBytes.
var ErrForceCancelRecordTooLarge = errors.New("the force cancel record is too large")

// ErrForceCancelText is a record or decision whose text no surface may show.
var ErrForceCancelText = errors.New("the force cancel's text is not plain or is over its bound")

// forceCancelText is the text a force cancel's decision and record carry,
// each with its bound in bytes (design §22.26.16 N9).
type forceCancelText struct {
	name, text string
	max        int
}

// checkForceCancelText holds each field to its bound, valid UTF-8 and no
// control character: all of it is shown on every surface of the run.
func checkForceCancelText(fields ...forceCancelText) error {
	for _, f := range fields {
		if len(f.text) > f.max || !utf8.ValidString(f.text) || strings.ContainsFunc(f.text, hidden) {
			return fmt.Errorf("%w: %s", ErrForceCancelText, f.name)
		}
	}
	return nil
}

// hidden is a character no surface shows as itself: a control, or one that
// reorders or hides what is around it.
func hidden(r rune) bool {
	return unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp)
}

// check holds the record's own text to its bounds. The scope is a namespace
// and an Orkflow name (200 B each) beside an owner's and its kind.
func (r ForceCancelRecord) check() error {
	if r.Reason == "" || r.Principal == "" || r.RequestID == "" {
		return fmt.Errorf("%w: a reason, a principal and a request id are required", ErrForceCancelText)
	}
	return checkForceCancelText(
		forceCancelText{"reason", r.Reason, 512}, forceCancelText{"principal", r.Principal, 256},
		forceCancelText{"principal_issuer", r.PrincipalIssuer, 256}, forceCancelText{"request_id", r.RequestID, 128},
		forceCancelText{"workflow_id", r.WorkflowID, 256}, forceCancelText{"auth_method", r.AuthMethod, 64},
		forceCancelText{"permission", r.Permission, 64}, forceCancelText{"scope", r.Scope, 1024},
		forceCancelText{"rule", r.Rule, 256})
}

// SignForceCancelRecord is the termination's details for rec, signed. A
// record past ForceCancelRecordMaxBytes is refused.
func SignForceCancelRecord(signer RecordSigner, rec ForceCancelRecord) (*commonpb.Payloads, error) {
	if err := rec.check(); err != nil {
		return nil, err
	}
	rec.Format = ForceCancelRecordFormat
	body, err := stablejson.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("force cancel record: %w", err)
	}
	msg, err := signer.SignDecision(jwt.KindForceCancelRecord, append(forceCancelBind(rec.Namespace, rec.RunID), body...))
	if errors.Is(err, jwt.ErrDecisionTooLarge) {
		return nil, fmt.Errorf("%w: %w", ErrForceCancelRecordTooLarge, err)
	}
	if err != nil {
		return nil, fmt.Errorf("force cancel record: sign: %w", err)
	}
	if len(msg) > ForceCancelRecordMaxBytes {
		return nil, fmt.Errorf("%w: %d bytes signed, at most %d", ErrForceCancelRecordTooLarge, len(msg), ForceCancelRecordMaxBytes)
	}
	return DataConverter().ToPayloads(forceCancelDetails{Signed: msg})
}

// UnsignedForceCancelRecord is the termination's details for rec where this
// deployment signs nothing: readers accept it only in asserted mode.
func UnsignedForceCancelRecord(rec ForceCancelRecord) (*commonpb.Payloads, error) {
	if err := rec.check(); err != nil {
		return nil, err
	}
	rec.Format = ForceCancelRecordFormat
	body, err := stablejson.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("force cancel record: %w", err)
	}
	if len(body) > ForceCancelRecordMaxBytes {
		return nil, fmt.Errorf("%w: %d bytes, at most %d", ErrForceCancelRecordTooLarge, len(body), ForceCancelRecordMaxBytes)
	}
	return DataConverter().ToPayloads(forceCancelDetails{Unsigned: &rec})
}

// forceCancelCarriedMaxBytes bounds a record as a run carries it.
const forceCancelCarriedMaxBytes = 2 * ForceCancelRecordMaxBytes

// ErrForceCancelUndecided is a force cancel's close that could not be read
// now: its keys or its execution could not be. A later read may decide it.
var ErrForceCancelUndecided = errors.New("how a force cancel ended the run could not be decided now")

// errForceCancelUnread is the one undecided close an archive may settle as its
// last resort: the run did end, and only its record could not be verified. A
// run that cannot be told from one resumed since is not.
var errForceCancelUnread = fmt.Errorf("%w: the keys its force cancel record was signed under could not be read", ErrForceCancelUndecided)

// ForceCancelVerification is what a termination's record proved.
type ForceCancelVerification int

const (
	// ForceCancelNone: the termination carries no force cancel's record.
	ForceCancelNone ForceCancelVerification = iota
	// ForceCancelVerified: a record signed by this deployment, of this run
	// and this execution.
	ForceCancelVerified
	// ForceCancelAsserted: an unsigned record a deployment in asserted mode
	// takes as said.
	ForceCancelAsserted
	// ForceCancelNotThisRun: a record that verifies but is of another run,
	// execution, namespace or force cancel.
	ForceCancelNotThisRun
	// ForceCancelBad: a record that does not verify, or an unsigned one where
	// callers are verified.
	ForceCancelBad
	// ForceCancelUnknownKey: signed by a key this deployment does not know.
	ForceCancelUnknownKey
	// ForceCancelUnavailable: the keys could not be read to decide.
	ForceCancelUnavailable
	// ForceCancelUnverified: nobody could decide before the cluster deletes
	// the run (ArchiveLastResort).
	ForceCancelUnverified
)

var forceCancelVerificationNames = [...]string{"none", "verified", "asserted", "not_this_run", "bad", "unknown_key", "unavailable", "unverified"}

func (v ForceCancelVerification) String() string {
	if v < 0 || int(v) >= len(forceCancelVerificationNames) {
		return fmt.Sprintf("force_cancel_verification(%d)", int(v))
	}
	return forceCancelVerificationNames[v]
}

// ForceCancelClose is what one termination says of a force cancel.
type ForceCancelClose struct {
	Verification ForceCancelVerification
	// Record is the record, set when it verified or was asserted.
	Record ForceCancelRecord
	// KID is the key the record names, when it named one.
	KID string
	// Carried is the record as a run carries it (Run.force_cancel_record).
	Carried string
}

// IsForceCancel reports whether the close is the force cancel its record says.
func (c ForceCancelClose) IsForceCancel() bool {
	return c.Verification == ForceCancelVerified || c.Verification == ForceCancelAsserted
}

// built reports whether the run's ending is built from this close: no code of
// the run ran after it.
func (c ForceCancelClose) built() bool {
	return c.IsForceCancel() || c.Verification == ForceCancelUnverified
}

// ReadForceCancelClose reads what the termination of run's execution
// executionRunID in namespace says, as of closedAt (the cluster's time of the
// close). mode is the deployment's authentication mode. A nil verifier reads
// every record as unavailable.
func ReadForceCancelClose(
	ctx context.Context, verifier ForceCancelVerifier, mode, namespace, runID, executionRunID string,
	closeEvent *historypb.HistoryEvent, closedAt time.Time,
) ForceCancelClose {
	attrs := closeEvent.GetWorkflowExecutionTerminatedEventAttributes()
	if attrs == nil || attrs.GetReason() != ForceCancelTerminateReason || len(attrs.GetDetails().GetPayloads()) != 1 {
		return ForceCancelClose{}
	}
	var details forceCancelDetails
	if err := DataConverter().FromPayload(attrs.GetDetails().GetPayloads()[0], &details); err != nil {
		return ForceCancelClose{Verification: ForceCancelBad}
	}
	if executionRunID == "" {
		return ForceCancelClose{Verification: ForceCancelUnavailable}
	}
	return details.read(ctx, verifier, mode, namespace, runID, executionRunID, closedAt)
}

// forceCancelEndWithin is how far an archived run's end may be from when its
// force cancel's record was signed, just before the termination was sent: by
// an engine's clock and not the cluster's. A close is held to its execution
// instead, whatever any clock said.
const forceCancelEndWithin = 15 * time.Minute

// readArchivedForceCancel reads the record an archived run carries
// (Run.force_cancel_record), as ReadForceCancelClose reads its close. Which
// execution it ended only the run's history says, so it is held to the run's
// ending instead: a run resumed since has another.
func readArchivedForceCancel(ctx context.Context, verifier ForceCancelVerifier, mode string, run *orkstackv1.Run) ForceCancelClose {
	var details forceCancelDetails
	if len(run.GetForceCancelRecord()) > forceCancelCarriedMaxBytes || json.Unmarshal([]byte(run.GetForceCancelRecord()), &details) != nil ||
		!run.GetFinishedAt().IsValid() || run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		return ForceCancelClose{Verification: ForceCancelBad}
	}
	ended := run.GetFinishedAt().AsTime()
	return details.read(ctx, verifier, mode, artifacts.ResolveNamespace(run.GetNamespace()), run.GetId(), "", ended)
}

// read is what the record says of run's execution executionRunID; for "", of
// any that ended near closedAt.
func (details forceCancelDetails) read(
	ctx context.Context, verifier ForceCancelVerifier, mode, namespace, runID, executionRunID string, closedAt time.Time,
) ForceCancelClose {
	if verifier == nil {
		return ForceCancelClose{Verification: ForceCancelUnavailable}
	}
	var rec ForceCancelRecord
	verdict := ForceCancelVerified
	var kid string
	switch {
	case details.Signed != "" && details.Unsigned == nil:
		body, k, answer := verifier.VerifyDecision(ctx, details.Signed, jwt.KindForceCancelRecord, closedAt, forceCancelBind(namespace, runID))
		kid = k
		switch answer {
		case identity.VerdictValid:
		case identity.VerdictUnknown:
			return ForceCancelClose{Verification: ForceCancelUnknownKey, KID: kid}
		case identity.VerdictUnavailable:
			return ForceCancelClose{Verification: ForceCancelUnavailable, KID: kid}
		default:
			// A body bound to another run is bad as signed; it is not this
			// run's either way.
			return ForceCancelClose{Verification: ForceCancelBad, KID: kid}
		}
		if err := json.Unmarshal(bytes.TrimPrefix(body, forceCancelBind(namespace, runID)), &rec); err != nil {
			return ForceCancelClose{Verification: ForceCancelBad, KID: kid}
		}
	case details.Unsigned != nil && details.Signed == "":
		switch verifier.UnsignedRecord(mode, closedAt) {
		case identity.UnsignedAsserted:
			verdict = ForceCancelAsserted
		case identity.UnsignedUnavailable:
			return ForceCancelClose{Verification: ForceCancelUnavailable}
		default:
			return ForceCancelClose{Verification: ForceCancelBad}
		}
		rec = *details.Unsigned
	default:
		return ForceCancelClose{Verification: ForceCancelBad}
	}
	if rec.check() != nil {
		return ForceCancelClose{Verification: ForceCancelBad, KID: kid}
	}
	if rec.Format != ForceCancelRecordFormat || rec.Namespace != namespace || rec.RunID != runID ||
		executionRunID == "" && closedAt.Sub(rec.At).Abs() > forceCancelEndWithin ||
		executionRunID != "" && rec.TerminatedExecutionRunID != executionRunID ||
		!strings.HasPrefix(rec.WorkflowID, ForceCancelWorkflowIDPrefix(namespace, runID)) {
		return ForceCancelClose{Verification: ForceCancelNotThisRun, KID: kid}
	}
	carried, err := stablejson.Marshal(details)
	if err != nil {
		return ForceCancelClose{Verification: ForceCancelBad, KID: kid}
	}
	return ForceCancelClose{Verification: verdict, Record: rec, KID: kid, Carried: string(carried)}
}

// showForceCancel makes a run read from its archive name who canceled it only
// as far as it proves, and answer no record: one a force cancel ended says what
// its record says, and without one that verifies its principal is asserted;
// only an ordinary cancel names anyone besides. The record itself says more
// than a run's reader needs (the rule and the scope it was decided by), so it
// stays in the archive's body, which an evidence bundle carries.
func showForceCancel(ctx context.Context, verifier ForceCancelVerifier, mode string, run *orkstackv1.Run) {
	defer func() { run.ForceCancelRecord = "" }()
	switch run.GetError().GetCode() {
	case runCanceledCode:
		return
	case RunForceCanceledCode:
	default:
		run.CanceledBy, run.CanceledByVerified = "", false
		return
	}
	read := readArchivedForceCancel(ctx, verifier, mode, run)
	if read.IsForceCancel() {
		_, run.Error = read.Outcome("")
		run.CanceledBy = read.Record.Principal
		run.CanceledByVerified = read.Verification == ForceCancelVerified && read.Record.PrincipalVerified
		return
	}
	run.CanceledByVerified = false
	if checkForceCancelText(forceCancelText{"principal", run.GetCanceledBy(), CanceledByMaxBytes}) != nil {
		run.CanceledBy = ""
	}
	by := run.GetCanceledBy()
	if by != "" {
		by = " by " + by
	}
	run.Error.Message = "force-canceled" + by + " (asserted): its record could not be verified"
}

// carriedForceCancel is a termination's record as a run carries it, whether or
// not it verifies: for a run archived before anyone could.
func carriedForceCancel(closeEvent *historypb.HistoryEvent) string {
	var details forceCancelDetails
	payloads := closeEvent.GetWorkflowExecutionTerminatedEventAttributes().GetDetails().GetPayloads()
	if len(payloads) != 1 || DataConverter().FromPayload(payloads[0], &details) != nil {
		return ""
	}
	carried, err := stablejson.Marshal(details)
	if err != nil || len(carried) > forceCancelCarriedMaxBytes {
		return ""
	}
	return string(carried)
}

// Outcome is the run's status and error for this close. plain is what a
// termination that is no force cancel says (its reason, or "run terminated").
func (c ForceCancelClose) Outcome(plain string) (orkstackv1.Status, *orkstackv1.Error) {
	if plain == "" {
		plain = "run terminated"
	}
	switch c.Verification {
	case ForceCancelVerified, ForceCancelAsserted:
		r := c.Record
		by := r.Principal
		if c.Verification == ForceCancelAsserted || !r.PrincipalVerified {
			by += " (asserted)"
		}
		grace := (time.Duration(r.GraceSeconds) * time.Second).String()
		how := "after " + grace + " without progress"
		if r.ProgressingAtCeiling {
			how = "at its ceiling, still making progress after " + grace
		}
		return orkstackv1.Status_STATUS_CANCELED, &orkstackv1.Error{
			Code:     RunForceCanceledCode,
			Message:  fmt.Sprintf("force-canceled by %s %s: %s; it ended without its own cleanup", by, how, r.Reason),
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED,
		}
	case ForceCancelUnknownKey:
		return orkstackv1.Status_STATUS_CANCELED, &orkstackv1.Error{Code: "RUN_TERMINATED",
			Message: fmt.Sprintf("%s: its force cancel record could not be verified (unknown key %s)", plain, c.KID)}
	case ForceCancelUnavailable:
		return orkstackv1.Status_STATUS_CANCELED, &orkstackv1.Error{Code: "RUN_TERMINATED",
			Message: plain + ": its force cancel record could not be checked now"}
	case ForceCancelUnverified:
		return orkstackv1.Status_STATUS_CANCELED, &orkstackv1.Error{Code: RunForceCancelUnverifiedCode,
			Message:  "the run was ended from outside with a force cancel's record that could not be verified before the cluster deleted the run",
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED}
	case ForceCancelBad, ForceCancelNotThisRun:
		return orkstackv1.Status_STATUS_CANCELED, &orkstackv1.Error{Code: "RUN_TERMINATED",
			Message: plain + ": it carried a force cancel record that is not this run's or does not verify"}
	}
	return orkstackv1.Status_STATUS_CANCELED, &orkstackv1.Error{Code: "RUN_TERMINATED", Message: plain}
}
