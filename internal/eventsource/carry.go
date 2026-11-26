// Package eventsource decides what one event source's workflow does: which
// instances run, on what, and when one is started again (docs/EVENT_SOURCES.md).
// It is a machine over facts and instants it is given, with no clock, no IO
// and no Temporal in it, so a workflow that is replayed decides the same.
package eventsource

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"maps"
	"slices"
	"time"
)

// CarryFormat is the one format of a carry this release writes and reads.
// Within it a field is only ever added, with a default under which a release
// that does not know it behaves as before: a reader keeps what it does not
// know and writes it back. A change that cannot be made so is a new number,
// which an engine that does not know it starts nothing from.
const CarryFormat = 1

// Pinned is every number a command of the workflow is decided by, pinned when
// the source's first execution starts and again only at a restart.
type Pinned struct {
	RollWait              time.Duration `json:"rollWait"`
	CancelWait            time.Duration `json:"cancelWait"`
	LoadTimeout           time.Duration `json:"loadTimeout"`
	LoadBackoff           time.Duration `json:"loadBackoff"`
	LoadBackoffMax        time.Duration `json:"loadBackoffMax"`
	DailyLoad             time.Duration `json:"dailyLoad"`
	PurgeBatch            int           `json:"purgeBatch"`
	FreshStarts           int           `json:"freshStarts"`
	FreshWindow           time.Duration `json:"freshWindow"`
	MaxAge                time.Duration `json:"maxAge"`
	RotateWithin          time.Duration `json:"rotateWithin"`
	RestartBase           time.Duration `json:"restartBase"`
	RestartMax            time.Duration `json:"restartMax"`
	Healthy               time.Duration `json:"healthy"`
	RevokeHold            time.Duration `json:"revokeHold"`
	RevokeUnavailableMax  time.Duration `json:"revokeUnavailableMax"`
	CheckTimeout          time.Duration `json:"checkTimeout"`
	CheckTries            int           `json:"checkTries"`
	CheckBackoff          time.Duration `json:"checkBackoff"`
	ProviderRefusedMax    time.Duration `json:"providerRefusedMax"`
	ConfirmUnavailableMax time.Duration `json:"confirmUnavailableMax"`
	JudgmentPauseMax      time.Duration `json:"judgmentPauseMax"`
	// The history an execution carries on past at the next moment nothing
	// runs; ForcedTimes it, at the next moment no replacement is pending; and,
	// in hundredths of what the deployment declares, the line nothing crosses.
	BudgetEvents int64 `json:"budgetEvents"`
	BudgetBytes  int64 `json:"budgetBytes"`
	ForcedTimes  int64 `json:"forcedTimes"`
	LimitPercent int64 `json:"limitPercent"`
	// MemoEvents and MemoBytes are the steps the memo says the history in, so
	// that it is not written for an event.
	MemoEvents int64 `json:"memoEvents"`
	MemoBytes  int64 `json:"memoBytes"`
	// Heartbeat is an instance's heartbeat timeout. An instance's activity
	// may last AgeBound maximum ages and StopGrace more.
	Heartbeat time.Duration `json:"heartbeat"`
	AgeBound  int           `json:"ageBound"`
	StopGrace time.Duration `json:"stopGrace"`
}

// Spec is what an instance is started on: a service revision, its spec's
// digest and the save whose record admits it. Two are the same target by
// revision and digest; the save follows the newest load.
type Spec struct {
	Revision      int64  `json:"revision"`
	Digest        string `json:"digest"`
	AdmissionSave string `json:"admissionSave,omitzero"`
	// Queue is the channel of its pool's source hosts, and CPU and Memory what
	// an instance of it reserves: an instance is started on the last good one
	// as that one was.
	Queue  string `json:"queue,omitzero"`
	CPU    string `json:"cpu,omitzero"`
	Memory string `json:"memory,omitzero"`
}

// same reports whether the two are one target.
func (s Spec) same(o Spec) bool { return s.Revision == o.Revision && s.Digest == o.Digest }

// FailedSpec is a target that was tried and did not work.
type FailedSpec struct {
	Spec   `json:",inline"`
	Reason string `json:"reason"`
	// Refused: it was never tried, because its spec was not admitted. Only
	// such a one is tried again without a new target.
	Refused bool `json:"refused,omitzero"`
}

// LoadedFacts is what the last load that succeeded returned.
type LoadedFacts struct {
	Revision  int64 `json:"revision"`
	Paused    bool  `json:"paused,omitzero"`
	Held      bool  `json:"held,omitzero"`
	Deleting  bool  `json:"deleting,omitzero"`
	Instances int32 `json:"instances"`
	ResumeSeq int64 `json:"resumeSeq"`
	// TrialCode is the breaker the last resume is on trial for.
	TrialCode string `json:"trialCode,omitzero"`
}

// Revoke is a revoke that was observed and not yet decided.
type Revoke struct {
	Cause   string    `json:"cause"`
	Digest  string    `json:"digest"`
	Save    string    `json:"save"`
	FirstAt time.Time `json:"firstAt"`
	Due     time.Time `json:"due"`
}

// IndexFacts is what is kept of one index across its activities.
type IndexFacts struct {
	// Fresh are the starts of the last day that were made only so that a
	// healthy instance's back-off begins again.
	Fresh []time.Time `json:"fresh,omitzero"`
	// Stops is how often in a row it was scheduled again with no healthy run
	// in between.
	Stops int `json:"stops,omitzero"`
	// TrialSeq is the resume its trial flag was last given for.
	TrialSeq int64 `json:"trialSeq,omitzero"`
}

// How an execution ended, or the one before it did.
const (
	EndedIdle      = "idle"
	EndedForced    = "forced"
	EndedLimit     = "limit"
	EndedHandover  = "handover"
	EndedPurged    = "purged"
	EndedAbandoned = "abandoned"
)

// Continued is when and how an execution ended.
type Continued struct {
	When time.Time `json:"when"`
	How  string    `json:"how"`
}

// Carry is everything a new execution needs to go on where the one before it
// stopped: every way of starting a source's workflow reads it from the latest
// execution there is.
type Carry struct {
	Format        int                  `json:"format"`
	Pinned        Pinned               `json:"pinned"`
	Target        *Spec                `json:"target,omitzero"`
	Good          *Spec                `json:"good,omitzero"`
	Failed        *FailedSpec          `json:"failed,omitzero"`
	InPlace       bool                 `json:"inPlace,omitzero"`
	Loaded        *LoadedFacts         `json:"loaded,omitzero"`
	NextEpoch     int64                `json:"nextEpoch"`
	Revoke        *Revoke              `json:"revoke,omitzero"`
	UnreadFirstAt map[int32]time.Time  `json:"unreadFirstAt,omitzero"`
	Index         map[int32]IndexFacts `json:"index,omitzero"`
	Continued     *Continued           `json:"continued,omitzero"`

	// Unknown is what a later release wrote, kept and written back.
	Unknown jsontext.Value `json:",embed"`
}

// clone shares nothing with c.
func (c Carry) clone() Carry {
	c.Target, c.Good, c.Failed = copied(c.Target), copied(c.Good), copied(c.Failed)
	c.Loaded, c.Revoke, c.Continued = copied(c.Loaded), copied(c.Revoke), copied(c.Continued)
	c.UnreadFirstAt, c.Index = maps.Clone(c.UnreadFirstAt), maps.Clone(c.Index)
	for i, f := range c.Index {
		f.Fresh = slices.Clone(f.Fresh)
		c.Index[i] = f
	}
	c.Unknown = slices.Clone(c.Unknown)
	return c
}

func copied[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// carryOptions read and write a carry: nanoseconds as a number, so no release
// reads another's spelling of a duration.
var carryOptions = json.JoinOptions(json.Deterministic(true), json.WithMarshalers(json.MarshalFunc(func(d time.Duration) ([]byte, error) {
	return json.Marshal(int64(d))
})), json.WithUnmarshalers(json.UnmarshalFunc(func(b []byte, d *time.Duration) error {
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*d = time.Duration(n)
	return nil
})))

// Encode is the carry as the memo holds it.
func (c Carry) Encode() ([]byte, error) { return json.Marshal(c, carryOptions) }

// DecodeCarry reads a carry. One of a format this release does not know is
// ErrCarryFormat, and nothing is started from it.
func DecodeCarry(b []byte) (Carry, error) {
	var head struct {
		Format int `json:"format"`
	}
	if err := json.Unmarshal(b, &head); err != nil {
		return Carry{}, fmt.Errorf("eventsource: a carry that does not decode: %w", err)
	}
	if head.Format != CarryFormat {
		return Carry{}, fmt.Errorf("%w: %d", ErrCarryFormat, head.Format)
	}
	var c Carry
	if err := json.Unmarshal(b, &c, carryOptions); err != nil {
		return Carry{}, fmt.Errorf("eventsource: a carry that does not decode: %w", err)
	}
	return c, nil
}
