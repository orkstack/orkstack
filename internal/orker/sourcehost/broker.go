package sourcehost

import (
	"encoding/json/v2"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
)

// The profile's check reasons the Orker itself gives a status (06 §5).
const (
	CheckObjectInvalid = "OBJECT_INVALID"
	CheckMessageLimit  = "MESSAGE_LIMIT"
)

// brokerBlock is the part of the status's broker block the Orker holds an
// image to. Everything in it is the image's word; the rest of the block
// (counts, retention, the object's name) is read where it is shown.
type brokerBlock struct {
	MaxDeliveries     *int   `json:"maxDeliveries"`
	MessageLimitBytes *int64 `json:"messageLimitBytes"`
}

// JudgeStatus applies the event-source profile's rules to a status read
// (06 §5) and returns it as the Orker treats it, with the delivery limit to
// observe (0 when the status reports none the Orker accepts).
//
// A status whose check is ok must report, in its broker block, the
// largest message the object delivers (else MESSAGE_LIMIT) and, for an
// at-least-once image, a delivery limit of 2 to 100 (else OBJECT_INVALID on
// broker.maxDeliveries): an image cannot switch the dead-letter receipt off
// by reporting a limit nobody reaches. A block that is not of its shape is
// OBJECT_INVALID on broker. A check that is not ok is the image's own word
// and stays as it is.
func JudgeStatus(s servicehost.Status, atMostOnce bool) (servicehost.Status, int) {
	var b brokerBlock
	shaped := len(s.Block) > 0 && json.Unmarshal(s.Block, &b) == nil
	limit := 0
	if shaped && b.MaxDeliveries != nil && ValidDeliveryLimit(*b.MaxDeliveries) {
		limit = *b.MaxDeliveries
	}
	if !s.OK {
		return s, limit
	}
	invalid := func(reason, field string) (servicehost.Status, int) {
		s.OK, s.Reason, s.Field = false, reason, field
		return s, 0
	}
	switch {
	case !shaped:
		return invalid(CheckObjectInvalid, "broker")
	case !atMostOnce && limit == 0:
		return invalid(CheckObjectInvalid, "broker.maxDeliveries")
	case b.MessageLimitBytes == nil || *b.MessageLimitBytes <= 0:
		return invalid(CheckMessageLimit, "broker.messageLimitBytes")
	}
	return s, limit
}
