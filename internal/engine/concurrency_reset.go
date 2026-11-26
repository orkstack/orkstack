package engine

import (
	"fmt"
	"slices"
	"time"
)

// The operator's reset of a concurrency limit (E88 §5.1.11): the gate is
// frozen, its state is judged and becomes the seeds of a new gate, and the
// new gate replaces it. A request_id acts once by the gate's own state
// (Resets), never by a record read back.

const (
	// ResetRequestIDMax and resetBodyMax bound what a gate's carry holds of a
	// request: its id and the digest of what it asks.
	ResetRequestIDMax = 48
	resetBodyMax      = 32
	// gateResetMemory is how many applied requests a gate remembers at once.
	// At the bound a further reset is refused: none that could still act is
	// forgotten for it.
	gateResetMemory = 32
	// resetClockAhead is how far in the future of the gate's clock a request
	// may say it was issued.
	resetClockAhead = 5 * time.Minute
)

// gateReset is a reset request as a gate decides it: its id, when its client
// issued it, and the digest of what it asks. The id and IssuedAt together
// name a request (CR-1).
type gateReset struct {
	ID       string    `json:"id"`
	IssuedAt time.Time `json:"at"`
	Body     string    `json:"body"`
	// Done: the gate the reset started finished rebuilding its record.
	Done bool `json:"done,omitempty"`
}

func (r gateReset) same(o gateReset) bool {
	return r.ID == o.ID && r.Body == o.Body && r.IssuedAt.Equal(o.IssuedAt)
}

// resetRefusal is a reset request the gate does not take now.
type resetRefusal struct {
	// Retryable: the same request may be asked again.
	Retryable bool
	Reason    string
}

func (r *resetRefusal) Error() string { return r.Reason }

// resetKnownError is the answer to a request the gate has applied already: it
// is never applied again.
type resetKnownError struct{ Request gateReset }

func (r *resetKnownError) Error() string { return "this reset has already been applied" }

// expired reports that the reset's window has passed by the clock given: it
// can never act, and the gate has forgotten it.
func (g *gateState) expired(r gateReset, now time.Time) bool {
	return !now.Before(r.IssuedAt.Add(g.Tunables.ResetWindow))
}

// freeze stops the gate for the reset r for freeze_timeout: it takes no
// acquire and grants nothing, and keeps taking releases. Asked again by the
// same request it is the same freeze, its time begun again; another request
// is refused while it is frozen, and for as long as the reset this gate began
// with may still be repeated (ResetHeldUntil). The age is decided here, by the
// clock the gate forgets by, so a request that waited on the way to it cannot
// act after its id was forgotten.
func (g *gateState) freeze(r gateReset, now time.Time) error {
	if err := g.checkReset(r, now); err != nil {
		return err
	}
	switch {
	case g.Draining:
		return &resetRefusal{Retryable: true, Reason: "the limit is carrying its record on; ask again"}
	case g.frozen(now) && g.Frozen.ID == r.ID && !g.Frozen.same(r):
		return &resetRefusal{Reason: "a reset with this request id asked something else, or was issued at another time"}
	case g.frozen(now) && g.Frozen.ID != r.ID, g.resetHeld(now):
		return &resetRefusal{Retryable: true, Reason: "another reset holds the limit; ask again"}
	}
	g.FrozenUntil, g.Frozen = now.Add(g.Tunables.FreezeTimeout), r
	return nil
}

// resetHeld reports that the start this gate began with may still be repeated
// by a replica that read the same state: the gate carries nothing on, does not
// close and takes no other reset, so a repeat finds the execution it started.
func (g *gateState) resetHeld(now time.Time) bool { return now.Before(g.ResetHeldUntil) }

// checkReset refuses a request this gate may not apply: malformed, issued
// outside the window, applied already, another request under an applied id,
// or any while the gate cannot know what was applied before it or cannot
// remember one more.
func (g *gateState) checkReset(r gateReset, now time.Time) error {
	refuse := func(format string, args ...any) error {
		return &resetRefusal{Reason: fmt.Sprintf(format, args...)}
	}
	switch {
	case g.Tunables.ResetWindow <= 0:
		return refuse("this limit has no reset window, so it cannot know which resets have been applied; ask again once it has been idle and started anew")
	case r.ID == "" || r.Body == "" || r.IssuedAt.IsZero():
		return refuse("a reset names its request id, when it was issued and what it asks")
	case len(r.ID) > ResetRequestIDMax || len(r.Body) > resetBodyMax:
		return refuse("a reset's request id is %d bytes and its digest %d; at most %d and %d are carried", len(r.ID), len(r.Body), ResetRequestIDMax, resetBodyMax)
	case r.IssuedAt.After(now.Add(resetClockAhead)):
		return refuse("the reset says it was issued at %s, later than the limit's own clock", r.IssuedAt.UTC().Format(time.RFC3339))
	case g.expired(r, now):
		return refuse("the reset was issued more than %s ago: start a new one", g.Tunables.ResetWindow)
	}
	g.forgetResets(now)
	if i := slices.IndexFunc(g.Resets, func(o gateReset) bool { return o.ID == r.ID }); i >= 0 {
		if g.Resets[i].same(r) {
			return &resetKnownError{Request: r}
		}
		return refuse("another reset with this request id has already been applied")
	}
	switch {
	case now.Before(g.ResetsUnknownUntil):
		return refuse("the limit cannot know which resets its earlier record applied; ask again after %s", g.ResetsUnknownUntil.UTC().Format(time.RFC3339))
	case len(g.Resets) >= gateResetMemory:
		return refuse("%d resets of this key are still remembered; ask again when the oldest is %s old", len(g.Resets), g.Tunables.ResetWindow)
	}
	return nil
}

// unfreeze ends r's freeze before its time, and reports whether it did:
// another request's, or one already over, is left alone.
func (g *gateState) unfreeze(r gateReset, now time.Time) bool {
	if !g.frozen(now) || !g.Frozen.same(r) {
		return false
	}
	g.FrozenUntil, g.Frozen = time.Time{}, gateReset{}
	return true
}

// forgetResets drops every applied request whose window has passed.
func (g *gateState) forgetResets(now time.Time) {
	g.Resets = slices.DeleteFunc(g.Resets, func(r gateReset) bool { return g.expired(r, now) })
}

// resetKnown reports whether the gate remembers applying id.
func (g *gateState) resetKnown(id string, now time.Time) bool {
	return slices.ContainsFunc(g.Resets, func(r gateReset) bool { return r.ID == id && !g.expired(r, now) })
}

// noteReset remembers an applied reset until its window has passed.
func (g *gateState) noteReset(r gateReset, now time.Time) {
	g.forgetResets(now)
	if !slices.ContainsFunc(g.Resets, func(o gateReset) bool { return o.ID == r.ID }) && !g.expired(r, now) {
		g.Resets = append(g.Resets, r)
	}
}

// gateSeeds is what a reset hands the new gate.
type gateSeeds struct {
	Labels     []string    `json:"labels,omitempty"`
	Holders    []gateEntry `json:"holders,omitempty"`
	Queue      []gateEntry `json:"queue,omitempty"`
	Tombstones []gateTomb  `json:"tombstones,omitempty"`
	Resets     []gateReset `json:"resets,omitempty"`
	Seq        int64       `json:"seq"`
	Epoch      int64       `json:"epoch"`
}

// judgeReset is the seeds of g's reset and how many entries it drops: those
// whose run has ended, is gone or has moved to a newer incarnation, by the
// findings given. A live entry of a run's current incarnation is never
// dropped (§10.1), whatever the run's record says, and neither is one whose
// run could not be checked; every entry kept is checked again by the new
// gate's passes. g is not changed.
func (g *gateState) judgeReset(findings []gateRunFinding) (gateSeeds, int) {
	byRun := make(map[string]gateRunFinding, len(findings))
	for _, f := range findings {
		byRun[f.Run] = f
	}
	dropped := 0
	keep := func(entries []gateEntry, held bool) []gateEntry {
		var out []gateEntry
		for _, e := range entries {
			f, checked := byRun[e.Run]
			if checked && f.Err == "" && (f.NotFound || !f.Open || f.MemoSeq > e.SeqBase) {
				dropped++
				continue
			}
			e.VerifyPending = held // a holder's mark only
			out = append(out, e)
		}
		return out
	}
	return gateSeeds{Labels: slices.Clone(g.Labels), Holders: keep(g.Holders, true), Queue: keep(g.Queue, false),
		Tombstones: slices.Clone(g.Tombstones), Resets: slices.Clone(g.Resets), Seq: g.Seq, Epoch: g.Epoch}, dropped
}

// seed puts s into a new gate started for the reset r, which the gate then
// remembers, and starts the epoch after the one s was read at.
func (g *gateState) seed(s gateSeeds, r gateReset, now time.Time) {
	g.Labels, g.Holders, g.Queue = s.Labels, s.Holders, s.Queue
	g.Tombstones, g.Resets, g.Seq = s.Tombstones, s.Resets, s.Seq
	g.Epoch = max(g.Epoch, s.Epoch+1)
	g.noteReset(r, now)
	g.ResetID = r.ID
}

// resetDone marks the reset this gate began with as finished, and reports
// whether that is news. One already forgotten has nothing to mark.
func (g *gateState) resetDone() bool {
	if g.ResetID == "" {
		return false
	}
	i := slices.IndexFunc(g.Resets, func(o gateReset) bool { return o.ID == g.ResetID })
	if i < 0 || g.Resets[i].Done {
		return false
	}
	g.Resets[i].Done = true
	return true
}

// resetUnfinished reports that this record began with a reset whose rebuild
// has not finished.
func (g *gateState) resetUnfinished() bool { return g.ResetID != "" && g.rebuilding() }

// gateResetMemo is what a gate execution's memo says of the resets applied to
// its key, for the start that follows it and for a reader it does not answer.
type gateResetMemo struct {
	// Current is the reset this execution began with, "" for none.
	Current      string      `json:"current,omitempty"`
	Resets       []gateReset `json:"resets,omitempty"`
	UnknownUntil time.Time   `json:"unknown_until,omitzero"`
	// Unreadable: what an earlier execution remembered could not be read.
	// The gate it starts takes no reset until its window has passed.
	Unreadable bool `json:"unreadable,omitempty"`
	// FrozenUntil: until then a reset holds the execution.
	FrozenUntil time.Time `json:"frozen_until,omitzero"`
}

func (m *gateResetMemo) empty() bool {
	return m == nil || (len(m.Resets) == 0 && m.UnknownUntil.IsZero() && m.FrozenUntil.IsZero() && !m.Unreadable)
}

func (g *gateState) resetMemo() gateResetMemo {
	m := gateResetMemo{Current: g.ResetID, Resets: g.Resets, UnknownUntil: g.ResetsUnknownUntil, FrozenUntil: g.FrozenUntil}
	if g.ResetHeldUntil.After(m.FrozenUntil) {
		m.FrozenUntil = g.ResetHeldUntil
	}
	return m
}

// inheritResets takes what the key's earlier executions remembered (CR-1),
// and reports whether the gate's memo must say something its start's did not.
func (g *gateState) inheritResets(m *gateResetMemo, now time.Time) bool {
	if m == nil {
		return false
	}
	g.Resets, g.ResetsUnknownUntil = slices.Clone(m.Resets), m.UnknownUntil
	g.forgetResets(now)
	if m.Unreadable {
		g.refuseResets(now)
	}
	return m.Unreadable
}

// refuseResets takes no reset until every one an earlier record may have
// applied is older than its window. Acquires are unaffected.
func (g *gateState) refuseResets(now time.Time) {
	if until := now.Add(g.Tunables.ResetWindow + resetClockAhead); until.After(g.ResetsUnknownUntil) {
		g.ResetsUnknownUntil = until
	}
}

// The reasons of a reset that is refused, or that ended otherwise than
// completed (§9.6).
const (
	ResetConflictCode    = "CONCURRENCY_RESET_CONFLICT"
	ResetUnansweredCode  = "CONCURRENCY_RESET_UNANSWERED"
	ResetRefusedCode     = "CONCURRENCY_RESET_REFUSED"
	ResetFailedCode      = "CONCURRENCY_RESET_FAILED"
	ResetInterruptedCode = "CONCURRENCY_RESET_INTERRUPTED"
	ResetNotAppliedCode  = "CONCURRENCY_RESET_NOT_APPLIED"
	// ConcurrencyKeyUnusedCode opens GetConcurrencyLimit's NOT_FOUND for a
	// key nothing holds or waits for.
	ConcurrencyKeyUnusedCode = "CONCURRENCY_KEY_UNUSED"
)

// The consequence codes of a reset (§10.1).
const (
	ResetAnswering        = "ANSWERING"
	ResetAbsent           = "ABSENT"
	ResetUnansweredForced = "UNANSWERED_FORCED"
)

// ResetVerdict is what a reset, or its dry run, found.
type ResetVerdict struct {
	Code           string
	Key            string
	Epoch          int64
	Holders        int
	Queued         int
	Dropped        int
	Limit          int
	AcquireTimeout time.Duration
}

// ResetConsequence is the §10.1 text of v, the one text a reset shows on every
// surface: "" for a code it has none for.
func ResetConsequence(v ResetVerdict) string {
	switch v.Code {
	case ResetAnswering:
		return fmt.Sprintf("Reset key %s (epoch %d): %d running and %d queued runs keep their slots and their order; "+
			"%d entries whose runs have ended or moved on are dropped. No run is stopped, restarted or moved in line. "+
			"Runs that ask for this key during the reset wait, then ask the new record; a run that refuses or skips at "+
			"this key and is not answered within %s fails RUN_CONCURRENCY_UNAVAILABLE.",
			v.Key, v.Epoch, v.Holders, v.Queued, v.Dropped, v.AcquireTimeout)
	case ResetAbsent:
		return fmt.Sprintf("Key %s has no record. The reset starts a new one from the records of the runs holding or "+
			"waiting for it, which keep their slots and their order.", v.Key)
	case ResetUnansweredForced:
		return fmt.Sprintf("The limit is not answering, so the runs it holds cannot all be known. The reset rebuilds it "+
			"from the runs' own records; until every run that holds the key is found, up to %d more runs than the limit "+
			"(%d) may run at once on key %s.", v.Limit, v.Limit, v.Key)
	}
	return ""
}
