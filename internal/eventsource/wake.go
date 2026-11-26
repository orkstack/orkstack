package eventsource

import (
	json "encoding/json/v2"
	"fmt"
	"maps"
	"slices"
	"time"
)

// line is the part of what the deployment declares that nothing crosses.
func (m *Machine) line(declared int64) int64 { return declared / 100 * m.Pinned.LimitPercent }

// over reports whether the history passed either bound, each held under the
// cluster's own line.
func (m *Machine) over(events, bytes int64) bool {
	if m.maxEvents > 0 {
		events = min(events, m.line(m.maxEvents)-1)
	}
	if m.maxBytes > 0 {
		bytes = min(bytes, m.line(m.maxBytes)-1)
	}
	return m.events >= events || m.bytes >= bytes
}

// atLimit reports whether the cluster's own line is reached; none is declared
// before the first load.
func (m *Machine) atLimit() bool {
	return (m.maxEvents > 0 && m.events >= m.line(m.maxEvents)) || (m.maxBytes > 0 && m.bytes >= m.line(m.maxBytes))
}

// mustCarryOn decides whether everything is stopped for the history's sake:
// at the cluster's own line whatever is pending, and past the forced one
// where no load fails and no replacement is pending.
func (m *Machine) mustCarryOn() {
	switch {
	case m.ending != "":
	case m.atLimit():
		m.ending = EndedLimit
	case m.over(m.Pinned.BudgetEvents*m.Pinned.ForcedTimes, m.Pinned.BudgetBytes*m.Pinned.ForcedTimes) && m.load == loadOK && !m.replacing():
		m.ending = EndedForced
	}
}

// replacing reports whether a replacement of any index is not yet ready.
func (m *Machine) replacing() bool {
	for _, s := range m.slots {
		if s.replacement != nil {
			return true
		}
	}
	return false
}

// rotates is when index i's instance is replaced for its age; zero where none
// is, or one already is being.
func (m *Machine) rotates(s *slot) time.Time {
	a := s.current
	if a == nil || a.stops() || a.startedAt.IsZero() || s.replacement != nil || s.stopping() {
		return time.Time{}
	}
	at := a.startedAt.Add(m.Pinned.MaxAge - m.Pinned.RotateWithin)
	if s.beside.After(at) {
		// What was beside it closed, and waits its delay.
		return s.beside
	}
	return at
}

// Next is when the machine is to be woken, zero for never.
func (m *Machine) Next(now time.Time) time.Time {
	if m.ended {
		return time.Time{}
	}
	var at []time.Time
	add := func(t time.Time) {
		if !t.IsZero() {
			at = append(at, t)
		}
	}
	if !m.loading {
		add(m.nextLoad)
	}
	if m.purgeFailing && !m.purging && m.running() == 0 {
		add(m.nextPurge)
	}
	if m.Revoke != nil && !m.checking {
		add(m.Revoke.Due)
	}
	want := m.wanted()
	for i, s := range m.slots {
		for _, a := range s.old {
			if a.phase == PhaseStopping {
				add(a.stopBy)
			}
		}
		if i >= want || m.pausing != pauseNone {
			continue
		}
		if s.current == nil && !s.stopping() {
			add(s.due)
		}
		if s.current != nil && s.replacement == nil && !s.stopping() && s.beside.After(now) {
			add(s.beside)
		}
		add(m.rotates(s))
	}
	for i, f := range m.Index {
		if i < want && len(f.Fresh) > 0 && len(f.Fresh) >= m.Pinned.FreshStarts {
			add(f.Fresh[0].Add(m.Pinned.FreshWindow))
		}
	}
	if len(at) == 0 {
		return time.Time{}
	}
	return slices.MinFunc(at, time.Time.Compare)
}

// Wake does what is due.
func (m *Machine) Wake(now time.Time) []Command {
	if m.ended {
		return nil
	}
	var out []Command
	due := func(t time.Time) bool { return !t.IsZero() && !now.Before(t) }
	if !m.loading && due(m.nextLoad) {
		out = m.startLoad()
	}
	if m.Revoke != nil && !m.checking && due(m.Revoke.Due) {
		m.checking, m.checkOf = true, m.Revoke.FirstAt
		out = append(out, StartCheck{Specs: m.specs()})
	}
	for _, i := range m.indexes() {
		s := m.slots[i]
		for _, a := range s.old {
			if a.phase == PhaseStopping && due(a.stopBy) {
				a.phase = PhaseStuck
			}
		}
		if i < m.wanted() && m.pausing == pauseNone && due(m.rotates(s)) {
			out = append(out, m.beside(now, i, s.current.spec, forRearm)...)
		}
	}
	for _, i := range slices.Sorted(maps.Keys(m.Index)) {
		f := m.Index[i]
		f.Fresh = m.recent(now, f.Fresh)
		m.Index[i] = f
	}
	return append(out, m.apply(now)...)
}

// SlotEpoch is what one epoch was scheduled with, as the memo says it.
type SlotEpoch struct {
	Epoch         int64  `json:"epoch"`
	Digest        string `json:"digest"`
	AdmissionSave string `json:"admissionSave,omitzero"`
	Phase         Phase  `json:"phase"`
}

// SlotMemo is one slot as the memo says it.
type SlotMemo struct {
	Current     SlotEpoch  `json:"current"`
	Replacement *SlotEpoch `json:"replacement,omitzero"`
	// Awaits are the reports the workflow waits for, by the epoch they are
	// of; Had those it was given, with an invalid's reason.
	Awaits map[int64][]string          `json:"awaits,omitzero"`
	Had    map[int64]map[string]string `json:"had,omitzero"`
}

// Source is a source as its workflow's memo names it. Pool and Instances are
// empty where nothing is wanted.
type Source struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Pool      string `json:"pool,omitzero"`
	Instances int32  `json:"instances,omitzero"`
}

// Now is the memo's other block: what the engines' pass and a read compare a
// describe with. Nothing is started again from it.
type Now struct {
	// StartedAfter is set by what started the execution, in the first memo
	// only, where the one before it was ended from outside.
	StartedAfter string `json:"startedAfter,omitzero"`
	// Source is whose workflow this is, the pool it asks for slots in and how
	// many: its four keep their names and meanings in every release, whatever
	// the carry's format.
	Source     Source             `json:"source"`
	Load       string             `json:"load"`
	Purge      string             `json:"purge,omitzero"`
	Wanted     int32              `json:"wanted"`
	Running    int                `json:"running"`
	Events     int64              `json:"events,omitzero"`
	Bytes      int64              `json:"bytes,omitzero"`
	Slots      map[int32]SlotMemo `json:"slot,omitzero"`
	UnknownEnd *UnknownEnd        `json:"unknownEnd,omitzero"`
}

// rounded is n down to a whole number of steps.
func rounded(n, step int64) int64 { return n / max(step, 1) * max(step, 1) }

// Now is what the memo says now. The history is rounded, so that a memo is
// not written for an event.
func (m *Machine) Now(now time.Time) Now {
	n := Now{Load: m.load, Wanted: m.wanted(), Running: m.running(), Events: rounded(m.events, m.Pinned.MemoEvents), Bytes: rounded(m.bytes, m.Pinned.MemoBytes),
		UnknownEnd: m.unknown}
	if m.purgeFailing {
		n.Purge = loadFailing
	}
	for i, s := range m.slots {
		if s.current == nil && s.replacement == nil {
			continue
		}
		memo := SlotMemo{Awaits: map[int64][]string{}, Had: map[int64]map[string]string{}}
		if s.current != nil {
			memo.Current = epochOf(s.current)
		}
		for _, a := range []*activity{s.current, s.replacement} {
			if a == nil {
				continue
			}
			for _, what := range []string{ReportStarted, ReportReady, ReportInvalid, ReportHealthy} {
				if m.awaited(now, i, a, what) {
					memo.Awaits[a.epoch] = append(memo.Awaits[a.epoch], what)
				}
			}
			if len(a.had) > 0 {
				memo.Had[a.epoch] = maps.Clone(a.had)
			}
		}
		if s.replacement != nil {
			r := epochOf(s.replacement)
			memo.Replacement = &r
		}
		if n.Slots == nil {
			n.Slots = map[int32]SlotMemo{}
		}
		n.Slots[i] = memo
	}
	return n
}

// DecodeNow reads the block of any release: what it does not know is left.
func DecodeNow(body []byte) (Now, error) {
	var n Now
	if err := json.Unmarshal(body, &n, carryOptions); err != nil {
		return Now{}, fmt.Errorf("eventsource: the memo's now: %w", err)
	}
	return n, nil
}

// Encode is the block as the memo holds it.
func (n Now) Encode() ([]byte, error) { return json.Marshal(n, carryOptions) }

func epochOf(a *activity) SlotEpoch {
	return SlotEpoch{Epoch: a.epoch, Digest: a.spec.Digest, AdmissionSave: a.spec.AdmissionSave, Phase: a.phase}
}
