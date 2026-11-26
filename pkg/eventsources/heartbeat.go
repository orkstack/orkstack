package eventsources

import (
	json "encoding/json/v2"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// heartbeatFormat is the version of an instance's heartbeat details.
const heartbeatFormat = 1

// MaxHeartbeatBytes bounds an instance's heartbeat details.
const MaxHeartbeatBytes = 4 << 10

// HeartbeatDetails is what an instance's attempt beats: what a source's status
// is read from, and the three instants an attempt takes over from the one
// before it. It holds no message, credential or path; what an image reported in
// it is shown as reported by the image.
type HeartbeatDetails struct {
	V int `json:"v"`
	// Epoch and Revision are the instance's, Judging the judging revision it
	// judges by.
	Epoch    int64     `json:"epoch"`
	Revision int64     `json:"revision,omitzero"`
	Judging  int64     `json:"judging,omitzero"`
	Phase    string    `json:"phase,omitzero"`
	State    string    `json:"state,omitzero"`
	Reason   string    `json:"reason,omitzero"`
	Since    time.Time `json:"since,omitzero"`

	Lanes    LaneDetails    `json:"lanes,omitzero"`
	Broker   *BrokerDetails `json:"broker,omitzero"`
	Warnings []string       `json:"warnings,omitempty"`

	ConfirmedAt    time.Time `json:"confirmedAt,omitzero"`
	LastAcceptedAt time.Time `json:"lastAcceptedAt,omitzero"`
	LastPollOkAt   time.Time `json:"lastPollOkAt,omitzero"`
	Image          string    `json:"image,omitzero"`

	// Held is what the engine's judgment waits on ("held=…", "unread=…"), and
	// HeldSince since when, over every attempt of the activity.
	Held      string    `json:"held,omitzero"`
	HeldSince time.Time `json:"heldSince,omitzero"`
	// RefusedSince is when a provider began to refuse this activity's
	// attempts; none after an attempt that ended otherwise.
	RefusedSince time.Time `json:"refusedSince,omitzero"`
}

// LaneDetails is what an instance's lanes are doing.
type LaneDetails struct {
	Busy              int `json:"busy"`
	Parked            int `json:"parked"`
	Wedged            int `json:"wedged"`
	OldestStepSeconds int `json:"oldestStepSeconds"`
}

// BrokerDetails is what the image reported of its broker object.
type BrokerDetails struct {
	CheckedAt        time.Time `json:"checkedAt,omitzero"`
	Object           string    `json:"object,omitzero"`
	MaxDeliveries    int       `json:"maxDeliveries"`
	LeaseSeconds     int64     `json:"leaseSeconds"`
	RetentionSeconds int64     `json:"retentionSeconds"`
	Pending          int64     `json:"pending"`
	Unacknowledged   int64     `json:"unacknowledged"`
	Redelivered      int64     `json:"redelivered"`
	DeadLetterDepth  int64     `json:"deadLetterDepth"`
}

// Encode is d within MaxHeartbeatBytes: a text too long is cut, and where the
// whole is, what the image reported goes first.
func (d HeartbeatDetails) Encode() []byte {
	d.V = heartbeatFormat
	d.Phase, d.State, d.Reason = oneLine(d.Phase, 32), oneLine(d.State, 32), oneLine(d.Reason, 96)
	d.Held, d.Image = oneLine(d.Held, 64), oneLine(d.Image, 384)
	for _, at := range []*time.Time{&d.Since, &d.ConfirmedAt, &d.LastAcceptedAt, &d.LastPollOkAt, &d.HeldSince, &d.RefusedSince} {
		*at = encodable(*at)
	}
	if d.Broker != nil {
		b := *d.Broker
		b.Object, b.CheckedAt = oneLine(b.Object, 200), encodable(b.CheckedAt)
		d.Broker = &b
	}
	var warnings []string
	for _, w := range d.Warnings {
		if len(warnings) < 16 {
			warnings = append(warnings, oneLine(w, 40))
		}
	}
	d.Warnings = warnings
	raw, err := json.Marshal(d)
	if err != nil || len(raw) > MaxHeartbeatBytes {
		// Nothing above leaves anything to refuse or so long; what is left
		// here cannot be.
		d.Warnings, d.Broker, d.Image = nil, nil, ""
		raw, _ = json.Marshal(d)
	}
	return raw
}

// ReadHeartbeatDetails reads an earlier attempt's details. Those of another
// format, or of no shape, are not read. A field it does not know is ignored.
func ReadHeartbeatDetails(raw []byte) (HeartbeatDetails, bool) {
	if len(raw) > MaxHeartbeatBytes {
		return HeartbeatDetails{}, false
	}
	var d HeartbeatDetails
	if err := json.Unmarshal(raw, &d); err != nil || d.V != heartbeatFormat {
		return HeartbeatDetails{}, false
	}
	return d, true
}

// oneLine is s as one printable line of at most limit bytes.
func oneLine(s string, limit int) string {
	if len(s) > limit {
		s = s[:limit]
		for len(s) > 0 && !utf8.ValidString(s[len(s)-1:]) && !utf8.RuneStart(s[len(s)-1]) {
			s = s[:len(s)-1]
		}
	}
	return strings.Map(func(r rune) rune {
		if r == utf8.RuneError || !unicode.IsPrint(r) {
			return -1
		}
		return r
	}, strings.ToValidUTF8(s, ""))
}

// encodable is at in UTC, or none where JSON holds no such year.
func encodable(at time.Time) time.Time {
	at = at.UTC()
	if at.Year() < 1 || at.Year() > 9999 {
		return time.Time{}
	}
	return at
}
