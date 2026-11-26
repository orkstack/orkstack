package eventsources

// messageSentences is a receipt's fixed sentence for each reason of a message.
var messageSentences = map[string]string{
	ReasonMessageTooLarge:              "The message's body is over its bound.",
	ReasonMessageDeliveriesExhausted:   "The message was not accepted at its broker's delivery limit and was given back for the broker to dead-letter.",
	ReasonMessageReleasedHoldExpired:   "No verdict came within the message's hold bound, and its container gave it back for another delivery.",
	ReasonMessageReleasedAcceptFailing: "The instance was waiting and gave the message back for another delivery; nothing was wrong with it.",
	ReasonMessageAttributesTooLarge:    "The message's attributes are over their bound.",
	ReasonMessageIdentityInvalid:       "The message's identity is too long or is not text.",
	ReasonMessageTextInvalid:           "The message's subject, an attribute or its group is not valid text.",
	ReasonMessageNotJSON:               "The message's body is not one strict JSON value.",
	ReasonMessageJSONTooDeep:           "The message's body nests too deep.",
	ReasonMessageJSONTooLarge:          "The message's body holds too many key names open at once.",
	ReasonMessageFieldMissing:          "A selector for a required input selected nothing.",
	ReasonMessageFieldType:             "A selected value is not of its input's declared type.",
	ReasonMessageInputTooLarge:         "A mapped value or the mapped input is over its bound.",
	ReasonMessageConcurrencyKeyInvalid: "The message's values make the key of the Orkflow's concurrency limit invalid.",
	ReasonMessageRunIDConflict:         "The run id of the message's claim is held by a run no message of its identity started.",
	ReasonMessageUnprocessable:         "The message could not be processed.",
	ReasonMessageNotMatched:            "The message does not match the source's match.",
	ReasonPaused:                       "The source is paused, and the message was given back.",
}

// MessageSentence is the one fixed sentence of a receipt's reason; a reason of
// another release has none.
func MessageSentence(reason string) string { return messageSentences[reason] }
