package eventsources

// Reasons of a source: a refused write, or a state on read
// (docs/REASONS.md, "Event sources").
const (
	ReasonDisabled               = "EVENT_SOURCES_DISABLED"
	ReasonRequireAuthentication  = "EVENT_SOURCES_REQUIRE_AUTHENTICATION"
	ReasonRequireVersioning      = "EVENT_SOURCES_REQUIRE_VERSIONING"
	ReasonLimit                  = "EVENT_SOURCE_LIMIT"
	ReasonNamespaceMismatch      = "EVENT_SOURCE_NAMESPACE_MISMATCH"
	ReasonMappingInvalid         = "EVENT_SOURCE_MAPPING_INVALID"
	ReasonConfigInvalid          = "EVENT_SOURCE_CONFIG_INVALID"
	ReasonDeliveryMismatch       = "EVENT_SOURCE_DELIVERY_MISMATCH"
	ReasonImageUnsupported       = "EVENT_SOURCE_IMAGE_UNSUPPORTED"
	ReasonOrkflowUnsupported     = "EVENT_SOURCE_ORKFLOW_UNSUPPORTED"
	ReasonIDMismatch             = "EVENT_SOURCE_ID_MISMATCH"
	ReasonPaused                 = "EVENT_SOURCE_PAUSED"
	ReasonNotConverged           = "EVENT_SOURCE_NOT_CONVERGED"
	ReasonDocumentUnreadable     = "EVENT_SOURCE_DOCUMENT_UNREADABLE"
	ReasonServiceContractUnknown = "SERVICE_CONTRACT_UNSUPPORTED"
	ReasonServiceProfileUnknown  = "SERVICE_PROFILE_UNSUPPORTED"
	// ReasonInvalid is a document that breaks a bound or a rule of its own,
	// before anything else is read.
	ReasonInvalid = "EVENT_SOURCE_INVALID"
	// ReasonIdentityUnsupported refuses identities, credentials and
	// grant-backed secrets on a source until task identity is available.
	ReasonIdentityUnsupported = "EVENT_SOURCE_IDENTITY_UNSUPPORTED"
)

// Reasons of a message (a receipt's reason, and an evaluation's).
const (
	ReasonMessageTooLarge           = "MESSAGE_TOO_LARGE"
	ReasonMessageAttributesTooLarge = "MESSAGE_ATTRIBUTES_TOO_LARGE"
	ReasonMessageIdentityInvalid    = "MESSAGE_IDENTITY_INVALID"
	ReasonMessageTextInvalid        = "MESSAGE_TEXT_INVALID"
	ReasonMessageNotJSON            = "MESSAGE_NOT_JSON"
	ReasonMessageJSONTooDeep        = "MESSAGE_JSON_TOO_DEEP"
	ReasonMessageJSONTooLarge       = "MESSAGE_JSON_TOO_LARGE"
	ReasonMessageFieldMissing       = "MESSAGE_FIELD_MISSING"
	ReasonMessageFieldType          = "MESSAGE_FIELD_TYPE"
	ReasonMessageInputTooLarge      = "MESSAGE_INPUT_TOO_LARGE"
	ReasonMessageUnprocessable      = "MESSAGE_UNPROCESSABLE"
	ReasonMessageNotMatched         = "MESSAGE_NOT_MATCHED"
)
