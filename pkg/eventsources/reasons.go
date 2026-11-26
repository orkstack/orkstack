package eventsources

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Reasons of a source: a refused write, or a state on read
// (docs/REASONS.md, "Event sources").
const (
	ReasonDisabled              = "EVENT_SOURCES_DISABLED"
	ReasonRequireAuthentication = "EVENT_SOURCES_REQUIRE_AUTHENTICATION"
	ReasonRequireVersioning     = "EVENT_SOURCES_REQUIRE_VERSIONING"
	ReasonLimit                 = "EVENT_SOURCE_LIMIT"
	ReasonNamespaceMismatch     = "EVENT_SOURCE_NAMESPACE_MISMATCH"
	ReasonMappingInvalid        = "EVENT_SOURCE_MAPPING_INVALID"
	ReasonConfigInvalid         = "EVENT_SOURCE_CONFIG_INVALID"
	ReasonDeliveryMismatch      = "EVENT_SOURCE_DELIVERY_MISMATCH"
	ReasonImageUnsupported      = "EVENT_SOURCE_IMAGE_UNSUPPORTED"
	ReasonOrkflowUnsupported    = "EVENT_SOURCE_ORKFLOW_UNSUPPORTED"
	ReasonIDMismatch            = "EVENT_SOURCE_ID_MISMATCH"
	ReasonPaused                = "EVENT_SOURCE_PAUSED"
	ReasonNotConverged          = "EVENT_SOURCE_NOT_CONVERGED"
	ReasonDocumentUnreadable    = "EVENT_SOURCE_DOCUMENT_UNREADABLE"
	// ReasonDocumentMissing is a source that runs and whose document this
	// engine does not find.
	ReasonDocumentMissing = "EVENT_SOURCE_DOCUMENT_MISSING"
	// ReasonDeletedDocumentReappeared is a source that was deleted.
	ReasonDeletedDocumentReappeared = "EVENT_SOURCE_DELETED_DOCUMENT_REAPPEARED"
	// ReasonDeleting is a source a delete has marked.
	ReasonDeleting = "EVENT_SOURCE_DELETING"
	// ReasonNotHeld answers an Orker that called for an instance it does not
	// hold.
	ReasonNotHeld = "EVENT_SOURCE_NOT_HELD"
	// ReasonHostNotPermitted is an Orker whose role lacks a permission at the
	// source's pool.
	ReasonHostNotPermitted       = "SOURCE_HOST_NOT_PERMITTED"
	ReasonServiceContractUnknown = "SERVICE_CONTRACT_UNSUPPORTED"
	ReasonServiceProfileUnknown  = "SERVICE_PROFILE_UNSUPPORTED"
	// ReasonInvalid is a document that breaks a bound or a rule of its own,
	// before anything else is read.
	ReasonInvalid = "EVENT_SOURCE_INVALID"
	// ReasonIdentityUnsupported refuses identities, credentials and
	// grant-backed secrets on a source until task identity is available.
	ReasonIdentityUnsupported = "EVENT_SOURCE_IDENTITY_UNSUPPORTED"
)

// The codes OrkStack pauses a source with.
const (
	PauseRefused          = "EVENT_SOURCE_REFUSED"
	PauseAllRejected      = "EVENT_SOURCE_ALL_REJECTED"
	PauseSettleFailing    = "EVENT_SOURCE_SETTLE_FAILING"
	PauseSettleUnanswered = "EVENT_SOURCE_SETTLE_UNANSWERED"
	PauseInstanceFailed   = "EVENT_SOURCE_INSTANCE_FAILED"
)

// pauseSentences are each code's one fixed sentence: a pause of OrkStack's
// carries no other text.
var pauseSentences = map[string]string{
	PauseRefused:          "A run could not be started for a reason every message would meet.",
	PauseAllRejected:      "Every message an instance received in a row was rejected.",
	PauseSettleFailing:    "The broker refused an instance's verdicts.",
	PauseSettleUnanswered: "The broker answered everything but an instance's verdicts.",
	PauseInstanceFailed:   "What the source runs failed, or is no longer permitted, for a reason trying again cannot change.",
}

// PauseSentence is the fixed sentence of a pause code; "" for a code OrkStack
// never pauses with.
func PauseSentence(code string) string { return pauseSentences[code] }

// EnginePause is the pause OrkStack gives a source: nil for a code it pauses
// nothing with. detail is of a closed set, or a name, beside the code's own
// sentence; orker is the Orker that asked, or none.
func EnginePause(code, refusal, detail, orker string, at time.Time) *orkstackv1.EventSourcePause {
	sentence := PauseSentence(code)
	if sentence == "" {
		return nil
	}
	if detail != "" {
		sentence += " (" + SafeText(detail, maxPauseDetail) + ")"
	}
	return &orkstackv1.EventSourcePause{Cause: orkstackv1.PauseCause_PAUSE_CAUSE_ENGINE, Code: code, RefusalCode: refusal,
		Reason: sentence, Since: timestamppb.New(at.UTC()), RequestedByOrker: orker}
}

// maxPauseDetail bounds what a pause's reason says beside its sentence.
const maxPauseDetail = 768

// Reasons an attempt of a service ends with (docs/ORKER_SERVICE_CONTRACT.md):
// the Orker's verdict on one container of one instance.
const (
	ReasonServiceExited            = "SERVICE_EXITED"
	ReasonServiceFailed            = "SERVICE_FAILED"
	ReasonServiceOOMKilled         = "SERVICE_OOM_KILLED"
	ReasonServiceNotReady          = "SERVICE_NOT_READY"
	ReasonServiceConfigRejected    = "SERVICE_CONFIG_REJECTED"
	ReasonServiceUnresponsive      = "SERVICE_UNRESPONSIVE"
	ReasonServiceHostStalled       = "SERVICE_HOST_STALLED"
	ReasonServiceContractViolation = "SERVICE_CONTRACT_VIOLATION"
	ReasonServiceStopStuck         = "SERVICE_STOP_STUCK"
	// ReasonServiceRunsAsRoot is an image that declares the root user and
	// ended before its first status: service containers run as the Orker's
	// own non-root uid, so an image that needs root cannot work.
	ReasonServiceRunsAsRoot = "SERVICE_RUNS_AS_ROOT"
)

// Reasons of a message (a receipt's reason, and an evaluation's).
const (
	ReasonMessageTooLarge = "MESSAGE_TOO_LARGE"
	// ReasonMessageDeliveriesExhausted is a message not accepted at its
	// broker's delivery limit, given back to be dead-lettered.
	ReasonMessageDeliveriesExhausted = "MESSAGE_DELIVERIES_EXHAUSTED"
	// ReasonMessageReleasedHoldExpired is a message its container released
	// itself, no verdict having come within its hold bound.
	ReasonMessageReleasedHoldExpired = "MESSAGE_RELEASED_HOLD_EXPIRED"
	// ReasonMessageReleasedAcceptFailing is a message given back while its
	// instance waits: nothing was wrong with it.
	ReasonMessageReleasedAcceptFailing = "MESSAGE_RELEASED_ACCEPT_FAILING"
	ReasonMessageAttributesTooLarge    = "MESSAGE_ATTRIBUTES_TOO_LARGE"
	ReasonMessageIdentityInvalid       = "MESSAGE_IDENTITY_INVALID"
	ReasonMessageTextInvalid           = "MESSAGE_TEXT_INVALID"
	ReasonMessageNotJSON               = "MESSAGE_NOT_JSON"
	ReasonMessageJSONTooDeep           = "MESSAGE_JSON_TOO_DEEP"
	ReasonMessageJSONTooLarge          = "MESSAGE_JSON_TOO_LARGE"
	ReasonMessageFieldMissing          = "MESSAGE_FIELD_MISSING"
	ReasonMessageFieldType             = "MESSAGE_FIELD_TYPE"
	ReasonMessageInputTooLarge         = "MESSAGE_INPUT_TOO_LARGE"
	// ReasonMessageConcurrencyKeyInvalid is a message whose values make the
	// key of its Orkflow's concurrency limit invalid.
	ReasonMessageConcurrencyKeyInvalid = "MESSAGE_CONCURRENCY_KEY_INVALID"
	// ReasonMessageRunIDConflict is a message whose claimed run id is held by
	// a run no message of its identity started.
	ReasonMessageRunIDConflict = "MESSAGE_RUN_ID_CONFLICT"
	ReasonMessageUnprocessable = "MESSAGE_UNPROCESSABLE"
	ReasonMessageNotMatched    = "MESSAGE_NOT_MATCHED"
)
