package orker

import (
	"fmt"
	"strings"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// The five failure categories of the OCI Activity Contract (§8.1).
//
// They are re-exported from internal/task rather than redeclared. The Orker
// constructs them and the engine reads them back as the exact Temporal
// ApplicationError type strings, so a second spelling on this side would
// silently misclassify a failure. One owner, one string.
const (
	CategoryActivityFailed      = task.CategoryActivityFailed
	CategoryInfrastructureError = task.CategoryInfrastructureError
	CategoryContractViolation   = task.CategoryContractViolation
	CategoryTimedOut            = task.CategoryTimedOut
	CategoryCancelled           = task.CategoryCancelled
)

// Reason codes — the exhaustive §8.2 table. Every terminal failure names one,
// so a failure is machine-readable as well as human-readable (AGENTS.md §15).
//
// There is deliberately NO exit-code table. An earlier draft classified 126 as
// "not executable" and 127 as "command not found", both permanent — but a
// workload choosing `exit 126` produces a byte-identical status to the CLI's
// own diagnostic (§14.13), and POSIX makes 127 an ordinary shell "command not
// found", so a routine activity-shell failure would have been permanently
// un-retryable. The signal that genuinely separates "the runtime refused" from
// "the workload failed" is which runtime call failed, not a number: Create and
// Start returning an error means the container never ran.
const (
	ReasonContainerExitNonzero = "CONTAINER_EXIT_NONZERO"
	ReasonContainerTimeout     = "CONTAINER_TIMEOUT"
	ReasonRuntimeRefused       = "RUNTIME_REFUSED"
	ReasonWorkspaceNotBindable = "WORKSPACE_NOT_BINDABLE"
	ReasonWorkdirNotBindable   = "WORKDIR_NOT_BINDABLE"
	ReasonImagePullFailed      = "IMAGE_PULL_FAILED"
	ReasonImageDigestMismatch  = "IMAGE_DIGEST_MISMATCH"
	ReasonImageNotPinned       = "IMAGE_NOT_PINNED"
	ReasonValuesMissing        = "VALUES_MISSING"
	ReasonValuesMalformed      = "VALUES_MALFORMED"
	ReasonValuesTooLarge       = "VALUES_TOO_LARGE"
	ReasonArtifactNotAFile     = "ARTIFACT_NOT_A_FILE"
	ReasonArtifactLimit        = "ARTIFACT_LIMIT_EXCEEDED"
	ReasonArtifactNameNotUTF8  = "ARTIFACT_NAME_NOT_UTF8"
	ReasonArtifactUploadFailed = "ARTIFACT_UPLOAD_FAILED"
	ReasonInputUnavailable     = "INPUT_ARTIFACT_UNAVAILABLE"
	ReasonInputCorrupt         = "INPUT_ARTIFACT_CORRUPT"
	ReasonInputTooLarge        = "INPUT_TOO_LARGE"
	ReasonRenderFailed         = "RENDER_FAILED"
	ReasonWorkdirFailed        = "WORKDIR_FAILED"
	ReasonSecretUnresolved     = "SECRET_UNRESOLVED"
	ReasonSecretSchemeForbid   = "SECRET_SCHEME_FORBIDDEN"
	// ReasonSecretNotPermitted is a source outside this Orker's
	// ORK_ORKER_SECRET_ALLOW floor. A policy refusal refuses identically
	// forever, so it is a ContractViolation and never retried.
	ReasonSecretNotPermitted   = "SECRET_NOT_PERMITTED"
	ReasonCommitReadFailed     = "COMMIT_READ_FAILED"
	ReasonCommitWriteFailed    = "COMMIT_WRITE_FAILED"
	ReasonCommitSchema         = "COMMIT_SCHEMA_UNSUPPORTED"
	ReasonCommitIdentity       = "COMMIT_IDENTITY_MISMATCH"
	ReasonCommitUndecodable    = "COMMIT_UNDECODABLE"
	ReasonCommitTooLarge       = "COMMIT_TOO_LARGE"
	ReasonCommitNotConditional = "COMMIT_STORE_NOT_CONDITIONAL"
	ReasonOutputNotReadable    = "OUTPUT_NOT_READABLE"
	ReasonValuesCorrupt        = "VALUES_CORRUPT"
	ReasonValuesRefetchFailed  = "VALUES_REFETCH_FAILED"
	ReasonResourceRefused      = "RESOURCE_REQUEST_REFUSED"
	ReasonNetworkModeRefused   = "NETWORK_MODE_REFUSED"
	// ReasonEachItemUnavailable is a fan-out item whose array no longer
	// resolves to the one the engine counted:
	// unreachable while the store honours its digests, stated rather than
	// guessed at.
	ReasonEachItemUnavailable = "EACH_ITEM_UNAVAILABLE"

	// Image supply-chain policy (docs/DEPLOY.md "Image supply-chain policy"), re-exported
	// from internal/task so the engine and the Orker spell one refusal one
	// way. The first three refuse identically forever; the fourth is a
	// registry that could not be asked, classified as IMAGE_PULL_FAILED is.
	ReasonImageNotPermitted         = task.ReasonImageNotPermitted
	ReasonImageNotSigned            = task.ReasonImageNotSigned
	ReasonImageSignatureInvalid     = task.ReasonImageSignatureInvalid
	ReasonImageSignatureUnavailable = task.ReasonImageSignatureUnavailable

	// ReasonPanic is not a Failure the Orker constructs and reports to the
	// engine — a panic unwinds through Execute's own re-panic, and Temporal's
	// activity worker turns that into its own error, never a Failure.
	// AttemptObserver.End still needs a code for it: an observer wired to
	// per-reason metrics must see something for the one exit path that isn't
	// a normal return, rather than a blank Reason indistinguishable from
	// success.
	ReasonPanic = "PANIC"
)

// reasonCategory is the §8.2 table, as data.
//
// It is a map rather than a switch so a code with no row is a detectable
// programming error rather than a silent default. categoryFor panics on a
// miss in tests via TestReasonCodes_AllHaveCategories; at runtime an unknown
// code degrades to InfrastructureError, which is retryable — the safe
// direction, because the alternative is refusing to retry something nobody
// classified.
var reasonCategory = map[string]string{
	ReasonContainerExitNonzero: CategoryActivityFailed,
	ReasonContainerTimeout:     CategoryTimedOut,
	ReasonRuntimeRefused:       CategoryInfrastructureError,
	ReasonWorkspaceNotBindable: CategoryInfrastructureError,
	ReasonWorkdirNotBindable:   CategoryInfrastructureError,
	ReasonImagePullFailed:      CategoryInfrastructureError,
	ReasonImageDigestMismatch:  CategoryContractViolation,
	ReasonImageNotPinned:       CategoryContractViolation,
	ReasonValuesMissing:        CategoryContractViolation,
	ReasonValuesMalformed:      CategoryContractViolation,
	ReasonValuesTooLarge:       CategoryContractViolation,
	ReasonArtifactNotAFile:     CategoryContractViolation,
	ReasonArtifactLimit:        CategoryContractViolation,
	ReasonArtifactNameNotUTF8:  CategoryContractViolation,
	ReasonArtifactUploadFailed: CategoryInfrastructureError,
	ReasonInputUnavailable:     CategoryInfrastructureError,
	ReasonInputCorrupt:         CategoryContractViolation,
	ReasonInputTooLarge:        CategoryContractViolation,
	ReasonRenderFailed:         CategoryContractViolation,
	ReasonWorkdirFailed:        CategoryInfrastructureError,
	ReasonSecretUnresolved:     CategoryInfrastructureError,
	ReasonSecretSchemeForbid:   CategoryContractViolation,
	ReasonSecretNotPermitted:   CategoryContractViolation,
	ReasonCommitReadFailed:     CategoryInfrastructureError,
	ReasonCommitWriteFailed:    CategoryInfrastructureError,
	ReasonCommitSchema:         CategoryContractViolation,
	ReasonCommitIdentity:       CategoryContractViolation,
	ReasonCommitUndecodable:    CategoryContractViolation,
	ReasonCommitTooLarge:       CategoryContractViolation,
	ReasonCommitNotConditional: CategoryInfrastructureError,
	ReasonOutputNotReadable:    CategoryContractViolation,
	ReasonValuesCorrupt:        CategoryContractViolation,
	ReasonValuesRefetchFailed:  CategoryInfrastructureError,
	ReasonResourceRefused:      CategoryContractViolation,
	ReasonNetworkModeRefused:   CategoryContractViolation,
	ReasonEachItemUnavailable:  CategoryContractViolation,

	ReasonImageNotPermitted:         CategoryContractViolation,
	ReasonImageNotSigned:            CategoryContractViolation,
	ReasonImageSignatureInvalid:     CategoryContractViolation,
	ReasonImageSignatureUnavailable: CategoryInfrastructureError,
}

// retryableReason overrides the category's own default for the three codes
// where they differ (§8.2).
//
// WORKSPACE_NOT_BINDABLE, WORKDIR_NOT_BINDABLE, and
// COMMIT_STORE_NOT_CONDITIONAL are InfrastructureError — the category whose
// default is "retry" — but all three are host misconfigurations that retrying
// cannot fix. Retrying them would loop against a deterministic fact, which
// AGENTS.md §11 forbids as loudly as it forbids a stuck state.
var retryableReason = map[string]bool{
	ReasonWorkspaceNotBindable: false,
	ReasonWorkdirNotBindable:   false,
	ReasonCommitNotConditional: false,
}

// CategoryFor answers which of the five categories a reason code belongs to.
func CategoryFor(reason string) string {
	if category, ok := reasonCategory[reason]; ok {
		return category
	}
	return CategoryInfrastructureError
}

// RetryableReason answers whether Temporal should try a reason code again.
//
// The category supplies the default and the override table supplies the three
// exceptions. ContractViolation and TimedOut are never retryable (the same
// statement task.NonRetryableCategories makes); the answer becomes the
// non-retryable flag on the ApplicationError, which wins over any policy.
func RetryableReason(reason string) bool {
	if explicit, ok := retryableReason[reason]; ok {
		return explicit
	}
	switch CategoryFor(reason) {
	case CategoryContractViolation, CategoryTimedOut, CategoryCancelled:
		return false
	default:
		return true
	}
}

// CategoryEnum maps a category string to the proto enum the engine records on
// Error.category, so the UI can say "contract violation — this image did not
// write values.json" instead of rendering a Go error string (§8.3).
func CategoryEnum(category string) orkstackv1.FailureCategory {
	switch category {
	case CategoryActivityFailed:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED
	case CategoryInfrastructureError:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	case CategoryContractViolation:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION
	case CategoryTimedOut:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT
	case CategoryCancelled:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED
	default:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED
	}
}

// Failure is one terminal activity failure, before it becomes a Temporal
// error. It pairs the machine-readable reason with the human-readable message
// AGENTS.md §15 requires of every terminal failure, and carries the detail
// message that makes a failed attempt as inspectable as a successful one.
type Failure struct {
	// Reason is the §8.2 code.
	Reason string
	// Message is the human-readable explanation. It names the specific thing:
	// the file and its mode for OUTPUT_NOT_READABLE, the uri and its size for
	// COMMIT_UNDECODABLE.
	Message string
	// Detail is the transport for everything a failed attempt produced —
	// uploaded artifacts, stream references, scan notes. Nil when the failure
	// happened before there was anything to report.
	Detail *orkstackv1.ActivityFailureDetail
	// Cause is the underlying error, kept for %w chains and for the Orker's
	// own logs. It is never what the engine reports: the reason and message
	// are.
	Cause error
}

// Error implements error.
func (f *Failure) Error() string {
	if f.Message == "" {
		return f.Reason
	}
	return f.Reason + ": " + f.Message
}

// Unwrap exposes the cause to errors.Is / errors.As.
func (f *Failure) Unwrap() error { return f.Cause }

// Category is the §8.1 category this failure's reason belongs to.
func (f *Failure) Category() string { return CategoryFor(f.Reason) }

// GetDetail is the nil-safe accessor, spelled the way the generated proto
// getters are: a Failure is nil on the success and panic paths, and a caller
// projecting an attempt reads the detail without first asking which path it is
// on.
func (f *Failure) GetDetail() *orkstackv1.ActivityFailureDetail {
	if f == nil {
		return nil
	}
	return f.Detail
}

// Fail builds a Failure with a formatted message.
func Fail(reason string, format string, args ...any) *Failure {
	return &Failure{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// FailCause builds a Failure that wraps an underlying error.
func FailCause(reason string, cause error, format string, args ...any) *Failure {
	return &Failure{Reason: reason, Message: fmt.Sprintf(format, args...), Cause: cause}
}

// WithDetail attaches the failure transport and returns the same Failure, so a
// construction site can stay one expression.
func (f *Failure) WithDetail(detail *orkstackv1.ActivityFailureDetail) *Failure {
	if detail != nil {
		detail.ReasonCode = f.Reason
		detail.Category = CategoryEnum(f.Category())
	}
	f.Detail = detail
	return f
}

// ErrorProto renders this failure as the shared Error message, in exactly the
// shape the engine's recordFailure produces for the same failure once it has
// crossed Temporal: code is the §8.2 reason, category the §8.1 category,
// retryable comes from RetryableReason, message from Error(), and details are
// the scan notes attached to the task's own name.
//
// It exists so the attempt record (§7.1, pkg/artifacts/attempts.go) says
// exactly what TaskRun.error will say for the LAST attempt — a timeline whose
// intermediate rows were spelled differently from its final row would make an
// operator compare two vocabularies. The engine cannot call this: by the time
// it records a failure, the Failure is gone and what it holds is a Temporal
// ApplicationError carrying an ActivityFailureDetail. Two producers of one
// shape, kept in step by TestFailureErrorProto_MatchesTheEngineShape.
//
// taskID is a parameter rather than a field because a Failure names the reason
// and the message, not the coordinates: every construction site would have had
// to remember to fill one.
func (f *Failure) ErrorProto(taskID string) *orkstackv1.Error {
	if f == nil {
		return nil
	}
	details := fmt.Sprintf("task %q", taskID)
	if notes := f.Detail.GetScanNotes(); len(notes) > 0 {
		// Scan notes are notes ON the workload's error, never a replacement
		// for it (§7.1 step 14).
		details = details + "; " + strings.Join(notes, "; ")
	}
	return &orkstackv1.Error{
		Code:      f.Reason,
		Message:   f.Error(),
		Details:   details,
		Retryable: RetryableReason(f.Reason),
		Category:  CategoryEnum(f.Category()),
	}
}

// TemporalError converts a Failure into the error an activity returns.
//
// The category is the ApplicationError TYPE, because that is what
// NonRetryableErrorTypes matches. The reason code is not the type: it is far
// more granular than a retry policy should have to enumerate, and it travels
// in the detail instead, where the engine reads it into Error.code.
//
// A non-retryable category is additionally constructed with
// NewNonRetryableApplicationError. The constructor's flag always wins over the
// policy list, so a task whose retry: policy forgot to list the category still
// cannot loop on a contract violation.
func (f *Failure) TemporalError() error {
	detail := f.Detail
	if detail == nil {
		detail = &orkstackv1.ActivityFailureDetail{
			ReasonCode: f.Reason,
			Category:   CategoryEnum(f.Category()),
		}
	}
	category := f.Category()
	message := f.Error()
	if !RetryableReason(f.Reason) {
		return temporal.NewNonRetryableApplicationError(message, category, f.Cause, detail)
	}
	// NewApplicationErrorWithCause, NOT NewApplicationError.
	//
	// temporal.NewApplicationError's signature is
	// (message, errType string, details ...interface{}) — it takes NO cause.
	// Passing one to it puts the cause in details[0] and the real detail in
	// details[1], so the engine's Details(&detail) decodes the wrong slot and
	// every RETRYABLE failure loses its artifacts, exit code and scan notes.
	// That is the most common failure path there is: CONTAINER_EXIT_NONZERO,
	// where a failing build's test report is exactly what somebody opens the
	// run to read. Found by
	// TestIntegration_AFailedTaskStillPublishesWhatItProduced.
	return temporal.NewApplicationErrorWithCause(message, category, f.Cause, detail)
}

// StampStart records when the attempt began on the failure's transport, so
// TaskRun.started_at is filled for a failed task exactly as for a succeeded
// one. A failure that reached here without a detail — a Validate refusal, a
// workdir error — gets the same minimal detail TemporalError would have
// built for it, with the start added.
func (f *Failure) StampStart(startedAt *timestamppb.Timestamp) *Failure {
	if f == nil {
		return nil
	}
	if f.Detail == nil {
		f.Detail = &orkstackv1.ActivityFailureDetail{
			ReasonCode: f.Reason,
			Category:   CategoryEnum(f.Category()),
		}
	}
	f.Detail.StartedAt = startedAt
	return f
}
