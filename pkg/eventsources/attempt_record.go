package eventsources

import (
	json "encoding/json/v2"
	"time"
)

// attemptRecordFormat is the version of an instance's attempt record.
const attemptRecordFormat = 1

// MaxAttemptRecordBytes bounds an instance's attempt record.
const MaxAttemptRecordBytes = 8 << 10

// AttemptRecord is one attempt of an instance, as its Orker leaves it in object
// storage when it ends. A record: what a source shows of its past attempts, and
// nothing is decided from it. It holds no message, credential or path.
type AttemptRecord struct {
	V         int    `json:"v"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	ID        string `json:"id"`
	Index     int32  `json:"index"`
	Epoch     int64  `json:"epoch"`
	Attempt   int32  `json:"attempt"`
	OrkerID   string `json:"orkerId"`
	BuildID   string `json:"buildId,omitzero"`
	Pool      string `json:"pool,omitzero"`
	// Service is the digest of the service spec the attempt ran, Judging the
	// judging revision it last held, and Image the image.
	Service string `json:"service,omitzero"`
	Judging int64  `json:"judging,omitzero"`
	Image   string `json:"image,omitzero"`
	// Phase is the phase the attempt ended in. StartedAt is when its container
	// was started and ReadyAt when it was first ready, each none if it never was.
	Phase     string    `json:"phase"`
	TakenAt   time.Time `json:"takenAt"`
	StartedAt time.Time `json:"startedAt,omitzero"`
	ReadyAt   time.Time `json:"readyAt,omitzero"`
	EndedAt   time.Time `json:"endedAt"`
	// Reason is what the attempt ended with, none for a rotation or a stop
	// that was asked for, and Cause the code behind it.
	Reason   string `json:"reason,omitzero"`
	Cause    string `json:"cause,omitzero"`
	ExitCode *int32 `json:"exitCode,omitempty"`
	// Held is the last thing the engine's judgment or a mint waited on, and
	// HeldSince since when.
	Held      string    `json:"held,omitzero"`
	HeldSince time.Time `json:"heldSince,omitzero"`
	// LogBytes is the size of the attempt's stored log.
	LogBytes int64 `json:"logBytes,omitzero"`
}

// Encode is r within MaxAttemptRecordBytes: a text too long is cut.
func (r AttemptRecord) Encode() []byte {
	r.V = attemptRecordFormat
	for s, limit := range map[*string]int{&r.Namespace: 200, &r.Name: 200, &r.ID: 64, &r.OrkerID: 200, &r.BuildID: 128, &r.Pool: 200,
		&r.Service: 64, &r.Image: 384, &r.Phase: 32, &r.Reason: 96, &r.Cause: 96, &r.Held: 64} {
		*s = oneLine(*s, limit)
	}
	for _, at := range []*time.Time{&r.TakenAt, &r.StartedAt, &r.ReadyAt, &r.EndedAt, &r.HeldSince} {
		*at = encodable(*at)
	}
	// Every field above is bounded, so nothing is left to refuse.
	raw, _ := json.Marshal(r)
	return raw
}

// ReadAttemptRecord reads a stored record. One of another format, over the
// bound or of no shape is not read. A field it does not know is ignored.
func ReadAttemptRecord(raw []byte) (AttemptRecord, bool) {
	if len(raw) > MaxAttemptRecordBytes {
		return AttemptRecord{}, false
	}
	var r AttemptRecord
	if err := json.Unmarshal(raw, &r); err != nil || r.V != attemptRecordFormat {
		return AttemptRecord{}, false
	}
	return r, true
}
