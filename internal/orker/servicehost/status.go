package servicehost

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"time"
	"unicode"
)

// Status files and their bounds (14 §5).
const (
	// StatusName is the container's readiness and health.
	StatusName = "status.json"
	// MaxStatusBytes bounds status.json.
	MaxStatusBytes = 64 << 10
	// maxStatusField bounds check.field.
	maxStatusField = 64
	// maxWarnings bounds check.warnings.
	maxWarnings = 16
)

// The general check reasons every profile has (14 §5).
const (
	CheckConfigInvalid = "CONFIG_INVALID"
	CheckNotAuthorized = "NOT_AUTHORIZED"
	CheckUnreachable   = "UNREACHABLE"
)

// Vocabulary is what a profile adds to a status: its check reasons, its
// warnings, and the name of the block it lets a service report.
type Vocabulary struct {
	Reasons  map[string]bool
	Warnings map[string]bool
	Block    string
}

// Status is a status.json the Orker read and checked: every word from a
// closed set, every text bounded and printable. Block is the profile's
// block as the container wrote it, for the profile to read.
type Status struct {
	CheckSeq int64
	Ready    bool
	Fatal    bool
	OK       bool
	Reason   string
	Field    string
	Warnings []string
	Block    jsontext.Value
}

// ReadStatus reads status.json from the Orker's copy. A higher
// schemaVersion, a reason or warning outside the vocabulary, a field that
// is too long or not printable, and bytes that are not the shape are
// violations; a field the Orker does not know is ignored (rule 3).
func ReadStatus(raw []byte, v Vocabulary) (Status, error) {
	var doc map[string]jsontext.Value
	if err := json.Unmarshal(raw, &doc); err != nil {
		return Status{}, violation(StatusName, "not one JSON object")
	}
	var s struct {
		SchemaVersion *int  `json:"schemaVersion"`
		CheckSeq      int64 `json:"checkSeq"`
		Ready         bool  `json:"ready"`
		Fatal         bool  `json:"fatal"`
		Check         *struct {
			OK       bool     `json:"ok"`
			Reason   string   `json:"reason"`
			Field    string   `json:"field"`
			Warnings []string `json:"warnings"`
		} `json:"check"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return Status{}, violation(StatusName, "not of the status's shape")
	}
	switch {
	case s.SchemaVersion == nil:
		return Status{}, violation(StatusName, "no schemaVersion")
	case *s.SchemaVersion != SchemaVersion:
		return Status{}, violation(StatusName, "schemaVersion %d; this Orker reads %d", *s.SchemaVersion, SchemaVersion)
	case s.CheckSeq < 0:
		return Status{}, violation(StatusName, "checkSeq below 0")
	case s.Check == nil:
		return Status{}, violation(StatusName, "no check")
	}
	c := s.Check
	if c.OK != (c.Reason == "") {
		return Status{}, violation(StatusName, "check.ok and check.reason disagree")
	}
	if c.Reason != "" && !generalReason(c.Reason) && !v.Reasons[c.Reason] {
		return Status{}, violation(StatusName, "check.reason is not a word of the profile")
	}
	if len(c.Field) > maxStatusField || !printable(c.Field) {
		return Status{}, violation(StatusName, "check.field is at most %d printable bytes", maxStatusField)
	}
	if len(c.Warnings) > maxWarnings {
		return Status{}, violation(StatusName, "at most %d warnings", maxWarnings)
	}
	for _, w := range c.Warnings {
		if !v.Warnings[w] {
			return Status{}, violation(StatusName, "a warning that is not a word of the profile")
		}
	}
	out := Status{CheckSeq: s.CheckSeq, Ready: s.Ready, Fatal: s.Fatal, OK: c.OK, Reason: c.Reason, Field: c.Field,
		Warnings: append([]string(nil), c.Warnings...)}
	if v.Block != "" {
		out.Block = doc[v.Block]
	}
	return out, nil
}

func generalReason(r string) bool {
	return r == CheckConfigInvalid || r == CheckNotAuthorized || r == CheckUnreachable
}

func printable(s string) bool {
	for _, r := range s {
		if !unicode.IsPrint(r) || r == unicode.ReplacementChar {
			return false
		}
	}
	return true
}

// Health is what the Orker makes of a service's status at one instant.
type Health int

const (
	// HealthStarting: no status yet, within startupSeconds.
	HealthStarting Health = iota
	// HealthNotReady: a status, ready false, check ok.
	HealthNotReady
	// HealthReady: ready, check ok, fresh: the Orker may start exchanges.
	HealthReady
	// HealthUnhealthy: check.ok false; the service stays up and no exchange
	// starts (14 §5, rule 1).
	HealthUnhealthy
	// HealthNoVerdict: the last status is older than three statusSeconds;
	// treated as a late step on every lane (rule 3).
	HealthNoVerdict
	// HealthNeverReported: no status within startupSeconds; the attempt
	// fails SERVICE_NOT_READY and is retried (rule 2).
	HealthNeverReported
	// HealthFatal: the container says it can never work with this
	// configuration; the attempt ends SERVICE_CONFIG_REJECTED, not retried.
	HealthFatal
)

// StatusWatch keeps the last status a service wrote and judges it against
// the service's limits on the caller's clock.
type StatusWatch struct {
	started  time.Time
	startup  time.Duration
	interval time.Duration
	last     Status
	readAt   time.Time
	seen     bool
}

// NewStatusWatch starts watching a container that started at started.
func NewStatusWatch(started time.Time, startupSeconds, statusSeconds int) *StatusWatch {
	startupSeconds, statusSeconds = max(startupSeconds, 1), max(statusSeconds, 1)
	return &StatusWatch{started: started, startup: time.Duration(startupSeconds) * time.Second,
		interval: time.Duration(statusSeconds) * time.Second}
}

// Observe records a status the Orker read at readAt. A status whose bytes
// did not change since the last read is no refresh; the caller passes
// changed accordingly.
func (w *StatusWatch) Observe(s Status, readAt time.Time, changed bool) {
	if changed || !w.seen {
		w.readAt = readAt
	}
	w.last, w.seen = s, true
}

// Last is the last status read, false before any.
func (w *StatusWatch) Last() (Status, bool) { return w.last, w.seen }

// Health judges the service at now.
func (w *StatusWatch) Health(now time.Time) Health {
	switch {
	case !w.seen && now.Sub(w.started) > w.startup:
		return HealthNeverReported
	case !w.seen:
		return HealthStarting
	case w.last.Fatal:
		return HealthFatal
	case now.Sub(w.readAt) > 3*w.interval:
		return HealthNoVerdict
	case !w.last.OK:
		return HealthUnhealthy
	case !w.last.Ready:
		return HealthNotReady
	}
	return HealthReady
}
