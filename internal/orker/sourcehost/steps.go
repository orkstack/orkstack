// Package sourcehost is the event-source profile of the Orker Service
// Contract on the Orker's side (docs/EVENT_SOURCES.md,
// docs/ORKER_SERVICE_CONTRACT.md): the steps of a lane, what the Orker
// makes of each message before it offers it, and the dispositions that
// settle every message a lane holds.
package sourcehost

import (
	"encoding/base64"
	"encoding/json/v2"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// The step names of a lane (06 §4), under answer/lanes/<k>/ and
// ask/lanes/<k>/.
const (
	StepPoll    = "poll.json"
	StepMessage = "message.json"
	StepBody    = "body"
	StepEmpty   = "empty.json"
	StepVerdict = "verdict.json"
	StepSettled = "settled.json"
)

// Bounds of the steps (06 §4.2, §8).
const (
	MaxMessageStepBytes = 128 << 10
	MaxSmallStepBytes   = 4 << 10
	maxBrokerFacts      = 16
	maxBrokerBytes      = 1 << 10
	maxErrorCode        = 64
)

// LanePath is a lane's step name, relative to answer/ or ask/.
func LanePath(lane int, step string) string { return fmt.Sprintf("lanes/%d/%s", lane, step) }

// Poll is ask/lanes/<k>/poll.json.
type Poll struct {
	SchemaVersion int   `json:"schemaVersion"`
	Seq           int64 `json:"seq"`
	WaitSeconds   int   `json:"waitSeconds"`
	LeaseSeconds  int   `json:"leaseSeconds"`
}

// Verdict actions (06 §4.4).
const (
	ActionAck     = "ack"
	ActionRelease = "release"
	ActionReject  = "reject"
)

// Verdict is ask/lanes/<k>/verdict.json.
type Verdict struct {
	SchemaVersion int   `json:"schemaVersion"`
	Seq           int64 `json:"seq"`
	// VerdictID names this verdict: the settle that answers it carries it
	// back, so a settle that answers none is recognised whenever it lands.
	VerdictID    string `json:"verdictId"`
	Action       string `json:"action"`
	DelaySeconds int    `json:"delaySeconds"`
}

// HandedMessage is a message.json the Orker read and checked, with the
// body's copy beside it.
type HandedMessage struct {
	Seq           int64
	ID            string
	BodyBytes     int64
	OversizeBytes int64
	Oversize      bool
	Subject       string
	Attributes    map[string][]string
	Binary        map[string][]byte
	PublishedAt   time.Time
	DeliveryCount int
	Group         string
	Broker        map[string]string

	// Taken is when the Orker took the message from its container, and
	// BodySHA256 the digest of its copy of the body, "" for one it never read.
	Taken      time.Time
	BodySHA256 string
}

// ReadMessage reads message.json from the Orker's copy. What breaks the
// contract (the shape, a count below zero, both or neither of bodyBytes
// and oversizeBytes, broker facts over their bound) is a violation that
// ends the attempt; what is the message's own (its text, its size) is left
// for Prejudge to reject.
func ReadMessage(raw []byte) (HandedMessage, error) {
	h, err := servicehost.ReadHeader(StepMessage, raw)
	if err != nil {
		return HandedMessage{}, err
	}
	var m struct {
		ID               *string             `json:"id"`
		BodyBytes        *int64              `json:"bodyBytes"`
		OversizeBytes    *int64              `json:"oversizeBytes"`
		Subject          string              `json:"subject"`
		Attributes       map[string][]string `json:"attributes"`
		BinaryAttributes map[string]string   `json:"binaryAttributes"`
		PublishedAt      string              `json:"publishedAt"`
		DeliveryCount    int                 `json:"deliveryCount"`
		Group            string              `json:"group"`
		Broker           map[string]string   `json:"broker"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return HandedMessage{}, violation(StepMessage, "not of message.json's shape")
	}
	out := HandedMessage{Seq: h.Seq, Subject: m.Subject, Attributes: m.Attributes, DeliveryCount: m.DeliveryCount, Group: m.Group, Broker: m.Broker}
	switch {
	case m.ID == nil || *m.ID == "":
		return HandedMessage{}, violation(StepMessage, "no id")
	case (m.BodyBytes == nil) == (m.OversizeBytes == nil):
		return HandedMessage{}, violation(StepMessage, "exactly one of bodyBytes and oversizeBytes")
	case m.BodyBytes != nil && *m.BodyBytes < 0, m.OversizeBytes != nil && *m.OversizeBytes < 0:
		return HandedMessage{}, violation(StepMessage, "a size below 0")
	case m.DeliveryCount < 0:
		return HandedMessage{}, violation(StepMessage, "deliveryCount below 0")
	case len(m.Broker) > maxBrokerFacts:
		return HandedMessage{}, violation(StepMessage, "more than %d broker facts", maxBrokerFacts)
	}
	out.ID = *m.ID
	if m.BodyBytes != nil {
		out.BodyBytes = *m.BodyBytes
	} else {
		out.OversizeBytes, out.Oversize = *m.OversizeBytes, true
	}
	brokerBytes := 0
	for k, v := range m.Broker {
		brokerBytes += len(k) + len(v)
	}
	if brokerBytes > maxBrokerBytes {
		return HandedMessage{}, violation(StepMessage, "broker facts over %d bytes", maxBrokerBytes)
	}
	if len(m.BinaryAttributes) > 0 {
		out.Binary = make(map[string][]byte, len(m.BinaryAttributes))
		for k, v := range m.BinaryAttributes {
			b, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return HandedMessage{}, violation(StepMessage, "a binary attribute that is not base64")
			}
			out.Binary[k] = b
		}
	}
	if m.PublishedAt != "" {
		t, err := time.Parse(time.RFC3339Nano, m.PublishedAt)
		if err != nil {
			return HandedMessage{}, violation(StepMessage, "publishedAt is not RFC 3339")
		}
		out.PublishedAt = t
	}
	return out, nil
}

// Message is the message as pkg/eventsources judges it, with the body's
// copy. Binary attributes are judged as text only where they are text.
func (m HandedMessage) Message(body []byte) eventsources.Message {
	return eventsources.Message{ID: m.ID, Subject: m.Subject, Group: m.Group, PublishedAt: m.PublishedAt, Attributes: m.Attributes, Body: body}
}

// BodyMatches reports whether the body's copy is the size the container
// said.
func (m HandedMessage) BodyMatches(body []byte) bool {
	return !m.Oversize && int64(len(body)) == m.BodyBytes
}

// StepError is an empty.json's or a settled.json's error: the kind and
// the broker's own short code, shown as the image's word.
type StepError struct {
	Kind string `json:"kind"`
	Code string `json:"code"`
}

// The kinds of a receive's or a settlement's failure (06 §4.3, §4.5).
const (
	KindTransient   = "transient"
	KindRefused     = "refused"
	KindLocal       = "local"
	KindLeaseLost   = "lease_lost"
	KindHoldExpired = "hold_expired"
)

// Empty is an empty.json: a quiet poll, or a receive that failed.
type Empty struct {
	Seq   int64
	Error *StepError
}

// ReadEmpty reads empty.json.
func ReadEmpty(raw []byte) (Empty, error) {
	h, err := servicehost.ReadHeader(StepEmpty, raw)
	if err != nil {
		return Empty{}, err
	}
	var e struct {
		Error *StepError `json:"error"`
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		return Empty{}, violation(StepEmpty, "not of empty.json's shape")
	}
	if e.Error != nil {
		if err := checkStepError(StepEmpty, e.Error, KindTransient, KindRefused, KindLocal); err != nil {
			return Empty{}, err
		}
	}
	return Empty{Seq: h.Seq, Error: e.Error}, nil
}

// Settled is a settled.json: how the broker answered a verdict, or the
// container's own release at its hold bound.
type Settled struct {
	Seq int64
	// VerdictID is the verdict this settle answers; "" for a container's own
	// close at its hold bound, which answers none.
	VerdictID string
	OK        bool
	Error     *StepError
}

// ReadSettled reads settled.json.
func ReadSettled(raw []byte) (Settled, error) {
	h, err := servicehost.ReadHeader(StepSettled, raw)
	if err != nil {
		return Settled{}, err
	}
	var s struct {
		VerdictID string     `json:"verdictId"`
		OK        *bool      `json:"ok"`
		Error     *StepError `json:"error"`
	}
	if err := json.Unmarshal(raw, &s); err != nil || s.OK == nil {
		return Settled{}, violation(StepSettled, "not of settled.json's shape")
	}
	if len(s.VerdictID) > maxVerdictID || !printableASCII(s.VerdictID) {
		return Settled{}, violation(StepSettled, "verdictId is at most %d bytes of printable ASCII", maxVerdictID)
	}
	if *s.OK != (s.Error == nil) {
		return Settled{}, violation(StepSettled, "ok and error disagree")
	}
	if s.Error != nil {
		if err := checkStepError(StepSettled, s.Error, KindLeaseLost, KindTransient, KindRefused, KindHoldExpired); err != nil {
			return Settled{}, err
		}
	}
	return Settled{Seq: h.Seq, VerdictID: s.VerdictID, OK: *s.OK, Error: s.Error}, nil
}

// maxVerdictID bounds the verdict id a settle carries back.
const maxVerdictID = 64

func checkStepError(step string, e *StepError, kinds ...string) error {
	known := false
	for _, k := range kinds {
		known = known || e.Kind == k
	}
	if !known {
		return violation(step, "error.kind is not a word of the profile")
	}
	if len(e.Code) > maxErrorCode || !printableASCII(e.Code) {
		return violation(step, "error.code is at most %d bytes of printable ASCII", maxErrorCode)
	}
	return nil
}

func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return utf8.ValidString(s)
}

func violation(name, format string, args ...any) error {
	return &servicehost.ViolationError{Name: name, Reason: fmt.Sprintf(format, args...)}
}

// SafeWord is a container's word (a code, an id) as the Orker may show it:
// printable, bounded, never OrkStack's sentence.
func SafeWord(s string, limit int) string {
	var b strings.Builder
	for _, r := range s {
		if !unicode.IsPrint(r) || r == utf8.RuneError {
			r = '?'
		}
		if b.Len()+utf8.RuneLen(r) > limit {
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}
