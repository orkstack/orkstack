package eventsource

import (
	"slices"
	"time"
)

// The codes a machine pauses a source with.
const (
	pauseInstanceFailed = "EVENT_SOURCE_INSTANCE_FAILED"
	refusalRevoked      = "IDENTITY_REVOKED"
	refusalUnread       = "IDENTITY_UNAVAILABLE"
)

// wanted is how many instances the source is to have, from the last load that
// succeeded.
func (m *Machine) wanted() int32 {
	l := m.Loaded
	if l == nil || l.Paused || l.Held || l.Deleting || m.ending != "" {
		return 0
	}
	return l.Instances
}

// base is what an instance is started on: the last one that worked, or the
// target where none has.
func (m *Machine) base() *Spec {
	if m.Good != nil {
		return m.Good
	}
	return m.Target
}

// apply makes what runs match what the document asked for at the last load
// that succeeded, one comparison at a time, and looks at what is running.
func (m *Machine) apply(now time.Time) []Command {
	if m.ended || !m.begun || m.Loaded == nil {
		return nil
	}
	m.mustCarryOn()
	var out []Command
	want := m.wanted()
	for _, i := range m.indexes() {
		if i >= want {
			out = append(out, m.stopSlot(now, i)...)
		}
	}
	if want == 0 {
		return append(out, m.idle(now)...)
	}
	base := m.base()
	if base == nil || m.pausing != pauseNone {
		return out
	}
	for i := range want {
		out = append(out, m.applyIndex(now, i, *base)...)
	}
	return append(out, m.roll(now, want)...)
}

// idle is what is done while nothing is wanted, once nothing runs.
func (m *Machine) idle(now time.Time) []Command {
	if m.running() > 0 {
		return nil
	}
	switch {
	case m.Loaded.Deleting && m.ending == "":
		if m.purging || (m.purgeFailing && now.Before(m.nextPurge)) {
			return nil
		}
		m.purging = true
		return []Command{StartPurge{}}
	case m.ending != "":
		return m.end(now, m.ending)
	}
	if m.over(m.Pinned.BudgetEvents, m.Pinned.BudgetBytes) && !m.Loaded.Deleting {
		return m.end(now, EndedIdle)
	}
	return nil
}

func (m *Machine) end(now time.Time, how string) []Command {
	m.ended = true
	m.Continued = &Continued{When: now, How: how}
	if how == EndedIdle || how == EndedForced || how == EndedLimit {
		return []Command{ContinueAsNew{}}
	}
	return []Command{End{}}
}

// slotOf is index i's slot, made where it has none.
func (m *Machine) slotOf(i int32) *slot {
	s, ok := m.slots[i]
	if !ok {
		s = &slot{}
		m.slots[i] = s
	}
	return s
}

func (m *Machine) applyIndex(now time.Time, i int32, base Spec) []Command {
	s := m.slotOf(i)
	var out []Command
	if r := s.replacement; r != nil && !r.spec.same(*m.Target) && (m.Good == nil || !r.spec.same(*m.Good)) {
		out = m.stopReplacement(now, i)
	}
	if s.current == nil && s.replacement != nil && s.replacement.spec.same(base) && !s.replacement.stops() {
		// What waits beside it runs the same: it is the instance now.
		s.current, s.replacement = s.replacement, nil
		s.current.purpose, s.due = forInstance, time.Time{}
	}
	switch cur := s.current; {
	case cur == nil:
		if now.Before(s.due) || s.stopping() {
			// What is stopping is gone before its slot is used again.
			return out
		}
		s.due = time.Time{}
		spec, tried := base, false
		if m.tryingInPlace() && m.placeTurn(i) {
			spec, tried = *m.Target, true
		}
		a := m.newActivity(spec, forInstance)
		a.tried = tried
		s.current = a
		return append(out, m.schedule(now, i, a))
	case m.Failed != nil && m.Good != nil && cur.spec.same(m.Failed.Spec) && !m.Failed.same(*m.Good):
		if r := s.replacement; r != nil && r.spec.same(m.Failed.Spec) {
			// It was to run what the instance runs, which has failed.
			return append(out, m.stopReplacement(now, i)...)
		} else if r != nil || m.rollTakes(i) {
			// What is beside it, or the roll that takes it next, moves it off.
			return out
		}
		return append(out, m.beside(now, i, *m.Good, forReturn)...)
	case !cur.spec.same(*m.Target) && (m.Good == nil || !cur.spec.same(*m.Good)):
		return append(out, m.stop(now, i, cur)...)
	}
	return out
}

// beside schedules a replacement of index i, which has at most one, and at
// most two activities in all.
func (m *Machine) beside(now time.Time, i int32, spec Spec, why purpose) []Command {
	s := m.slotOf(i)
	if r := s.replacement; r != nil {
		if r.spec.same(spec) && (why != forRoll || r.purpose == forRoll) || r.stops() {
			return nil
		}
		return m.stopReplacement(now, i)
	}
	if s.stopping() || now.Before(s.beside) {
		return nil
	}
	a := m.newActivity(spec, why)
	a.tried = why == forRoll
	s.replacement = a
	return []Command{m.schedule(now, i, a)}
}

func (m *Machine) newActivity(spec Spec, why purpose) *activity {
	a := &activity{epoch: m.NextEpoch, spec: spec, purpose: why, phase: PhaseScheduled, had: map[string]string{}}
	m.NextEpoch++
	return a
}

func (m *Machine) schedule(now time.Time, i int32, a *activity) Command {
	f := m.Index[i]
	trial := a.purpose == forInstance && m.Loaded.ResumeSeq > f.TrialSeq
	if trial {
		f.TrialSeq = m.Loaded.ResumeSeq
		m.Index[i] = f
	}
	out := Schedule{Index: i, Epoch: a.epoch, Spec: a.spec, Trial: trial, OnTrial: a.tried, CompleteAfterHealthy: m.mayFreshen(now, i), Instances: m.Loaded.Instances}
	if trial {
		out.TrialCode = m.Loaded.TrialCode
	}
	return out
}

// mayFreshen reports whether index i has a fresh start of the day left.
func (m *Machine) mayFreshen(now time.Time, i int32) bool {
	return len(m.recent(now, m.Index[i].Fresh)) < m.Pinned.FreshStarts
}

// roll moves one index at a time onto a target that is neither good nor
// failed, once every index has an instance.
func (m *Machine) roll(now time.Time, want int32) []Command {
	if m.Target == nil || (m.Failed != nil && m.Failed.same(*m.Target)) {
		return nil
	}
	for i := range want {
		if m.slotOf(i).current == nil {
			return nil
		}
	}
	ready := true
	for i := range want {
		cur := m.slotOf(i).current
		if !cur.spec.same(*m.Target) {
			if m.Good == nil || m.Good.same(*m.Target) {
				return nil
			}
			if m.InPlace {
				return m.stop(now, i, cur)
			}
			return m.beside(now, i, *m.Target, forRoll)
		}
		if cur.phase != PhaseReady {
			ready = false
			if m.Good != nil {
				// One index at a time, and the next once this one works.
				return nil
			}
		}
	}
	if ready && (m.Good == nil || !m.Good.same(*m.Target)) {
		good := *m.Target
		m.Good, m.InPlace = &good, false
		for i := range want {
			m.slotOf(i).current.tried = false
		}
	}
	return nil
}

// rolling reports whether the target is one to roll to.
func (m *Machine) rolling() bool {
	return m.Target != nil && m.Good != nil && !m.Good.same(*m.Target) && (m.Failed == nil || !m.Failed.same(*m.Target))
}

// rollTakes reports whether a roll moves index i onto the target next.
func (m *Machine) rollTakes(i int32) bool {
	if !m.rolling() {
		return false
	}
	for j := range m.wanted() {
		cur := m.slotOf(j).current
		if cur == nil || (j < i && !cur.spec.same(*m.Target)) {
			return false
		}
	}
	return true
}

// tryingInPlace reports whether the target is being tried by stopping each
// instance first.
func (m *Machine) tryingInPlace() bool {
	return m.InPlace && m.rolling()
}

// placeTurn reports whether index i is the first that is not on the target,
// and what is before it works there.
func (m *Machine) placeTurn(i int32) bool {
	for j := range i {
		if cur := m.slotOf(j).current; cur == nil || !cur.spec.same(*m.Target) || cur.phase != PhaseReady {
			return false
		}
	}
	return true
}

// stopSlot stops everything of index i, and forgets it once nothing runs.
func (m *Machine) stopSlot(now time.Time, i int32) []Command {
	s := m.slots[i]
	s.due = time.Time{}
	var out []Command
	if s.replacement != nil {
		out = append(out, m.stopReplacement(now, i)...)
	}
	if s.current != nil {
		out = append(out, m.stop(now, i, s.current)...)
	}
	if len(s.activities()) == 0 {
		delete(m.slots, i)
	}
	return out
}

// stop cancels index i's instance and sets it aside until it has closed.
func (m *Machine) stop(now time.Time, i int32, a *activity) []Command {
	s := m.slots[i]
	s.current = nil
	return m.setAside(now, i, s, a)
}

func (m *Machine) stopReplacement(now time.Time, i int32) []Command {
	s := m.slots[i]
	a := s.replacement
	s.replacement = nil
	return m.setAside(now, i, s, a)
}

func (m *Machine) setAside(now time.Time, i int32, s *slot, a *activity) []Command {
	s.old = append(s.old, a)
	if a.stops() {
		return nil
	}
	a.phase, a.stopBy = PhaseStopping, now.Add(m.Pinned.CancelWait)
	return []Command{Cancel{Index: i, Epoch: a.epoch}}
}

// find is index i's activity of that epoch.
func (m *Machine) find(i int32, epoch int64) (*slot, *activity) {
	s, ok := m.slots[i]
	if !ok {
		return nil, nil
	}
	for _, a := range s.activities() {
		if a.epoch == epoch {
			return s, a
		}
	}
	return nil, nil
}

// OnReport is what the engine relays of what an instance's Orker told it.
func (m *Machine) OnReport(now time.Time, i int32, epoch int64, what, reason string) []Command {
	s, a := m.find(i, epoch)
	if a == nil || m.ended || !m.awaited(now, i, a, what) {
		return nil
	}
	a.had[what] = reason
	switch what {
	case ReportStarted:
		a.phase, a.startedAt = PhaseStarted, now
	case ReportReady:
		a.phase, a.readyAt = PhaseReady, now
		if a.startedAt.IsZero() {
			a.startedAt = now
		}
		if a == s.replacement {
			// The old one is stopped only now.
			old := s.current
			s.current, s.replacement = a, nil
			a.purpose, s.due = forInstance, time.Time{}
			if old != nil {
				return append(m.setAside(now, i, s, old), m.apply(now)...)
			}
		}
	case ReportInvalid:
		if a.tried {
			return m.rollFailed(now, i, s, a, reason)
		}
	case ReportHealthy:
		f := m.Index[i]
		f.Stops = 0
		m.Index[i] = f
		if a == s.current && s.replacement == nil {
			return m.rearm(now, i, a)
		}
	}
	return m.apply(now)
}

// rearm starts a fresh activity beside a healthy instance, on what it runs.
func (m *Machine) rearm(now time.Time, i int32, a *activity) []Command {
	m.countFresh(now, i)
	return append(m.beside(now, i, a.spec, forRearm), m.apply(now)...)
}

// countFresh counts a fresh start of index i. The newest are kept, as many as
// the bound: an Orker that ends one past it is counted, and adds nothing.
func (m *Machine) countFresh(now time.Time, i int32) {
	f := m.Index[i]
	f.Fresh = append(m.recent(now, f.Fresh), now)
	f.Fresh = f.Fresh[max(0, len(f.Fresh)-m.Pinned.FreshStarts):]
	m.Index[i] = f
}

func (m *Machine) recent(now time.Time, at []time.Time) []time.Time {
	return slices.DeleteFunc(slices.Clone(at), func(t time.Time) bool { return now.Sub(t) >= m.Pinned.FreshWindow })
}

// awaited reports whether the workflow waits for that report of a.
func (m *Machine) awaited(now time.Time, i int32, a *activity, what string) bool {
	if _, had := a.had[what]; had || a.stops() {
		return false
	}
	switch what {
	case ReportStarted:
		return a.phase == PhaseScheduled
	case ReportReady, ReportInvalid:
		return a.phase != PhaseReady
	case ReportHealthy:
		return m.mayFreshen(now, i)
	}
	return false
}

// rollFailed ends the trial of the target: what was being tried is stopped,
// and apply puts back what had been replaced.
func (m *Machine) rollFailed(now time.Time, i int32, s *slot, a *activity, reason string) []Command {
	m.failTarget(reason)
	m.InPlace = false
	var out []Command
	switch a {
	case s.replacement:
		out = m.stopReplacement(now, i)
	case s.current:
		out = m.stop(now, i, a)
	}
	return append(out, m.apply(now)...)
}

// OnClosed is an activity that closed.
func (m *Machine) OnClosed(now time.Time, i int32, epoch int64, c Closing) []Command {
	s, a := m.find(i, epoch)
	if a == nil || m.ended {
		return nil
	}
	wasCurrent, wasReplacement := a == s.current, a == s.replacement
	switch {
	case wasCurrent:
		s.current = nil
	case wasReplacement:
		s.replacement = nil
	default:
		s.old = slices.DeleteFunc(s.old, func(o *activity) bool { return o == a })
	}
	if len(s.activities()) == 0 && i >= m.wanted() {
		delete(m.slots, i)
	}
	if a.stops() || (!wasCurrent && !wasReplacement) {
		return m.apply(now)
	}
	f := m.Index[i]
	if !a.readyAt.IsZero() && now.Sub(a.readyAt) >= m.Pinned.Healthy {
		f.Stops = 0
		m.Index[i] = f
	}
	var out []Command
	result := c.Result
	if result == nil {
		result = &Result{Kind: EndUnmet}
	}
	switch result.Kind {
	case EndRotated:
		// What waits beside it takes its place, and where nothing does it is
		// scheduled at once.
		return m.apply(now)
	case EndStopped:
		out = m.startLoad()
	case EndAfterHealthy:
		m.countFresh(now, i)
	case EndJudgment:
		return append(m.unread(now, i, s, a, wasReplacement, result.Unread), m.apply(now)...)
	case EndUnchangeable:
		if a.tried {
			return m.rollFailed(now, i, s, a, result.Reason)
		}
		// A pause that cannot be written is tried again by the next attempt,
		// which waits as any does.
		m.restartLater(now, i, s, wasReplacement)
		return append(m.pause(pauseInstanceFailed, result.Reason, result.Cause), m.apply(now)...)
	case EndUnknown:
		m.unknown = &UnknownEnd{Reason: result.Reason, At: now}
	}
	m.restartLater(now, i, s, wasReplacement)
	return append(out, m.apply(now)...)
}

// restartLater spaces the workflow's own reschedules of an index.
func (m *Machine) restartLater(now time.Time, i int32, s *slot, replacement bool) {
	if i >= m.wanted() {
		return
	}
	f := m.Index[i]
	delay := backoff(m.Pinned.RestartBase, m.Pinned.RestartMax, f.Stops)
	if delay < m.Pinned.RestartMax {
		f.Stops++
	}
	m.Index[i] = f
	m.waits(s, replacement, now.Add(delay))
}

// waits sets when what closed is scheduled again: the instance, or what was
// beside one.
func (m *Machine) waits(s *slot, replacement bool, until time.Time) {
	if replacement && s.current != nil {
		s.beside = until
		return
	}
	s.due = until
}

// unread is an attempt that could not be judged for its own spec or record:
// tried again after a while, and past the bound a pause.
func (m *Machine) unread(now time.Time, i int32, s *slot, a *activity, replacement bool, what string) []Command {
	m.waits(s, replacement, now.Add(m.Pinned.ConfirmUnavailableMax))
	if a.spec.AdmissionSave != m.Target.AdmissionSave {
		// Of an older save than the newest loaded, whose attempt counts.
		return nil
	}
	first, ok := m.UnreadFirstAt[i]
	if !ok {
		m.UnreadFirstAt[i] = now
		return nil
	}
	if now.Sub(first) < m.Pinned.JudgmentPauseMax {
		return nil
	}
	return m.pause(pauseInstanceFailed, refusalUnread, "unread="+what)
}

func (m *Machine) pause(code, refusal, detail string) []Command {
	if m.pausing != pauseNone || m.Loaded == nil {
		return nil
	}
	m.pausing = pauseWriting
	return []Command{WritePause{Code: code, Refusal: refusal, Detail: detail, Revision: m.Loaded.Revision, ResumeSeq: m.Loaded.ResumeSeq}}
}

// OnPaused is the answer of a pause's write: where it was made, or the source
// was not there to be paused, the document is loaded again; where it was not,
// the next cause writes it again.
func (m *Machine) OnPaused(now time.Time, written bool) []Command {
	if !written {
		m.pausing = pauseNone
		return m.apply(now)
	}
	m.pausing = pauseWritten
	return m.startLoad()
}

// OnJudged is an accepting confirmation of an instance of index i.
func (m *Machine) OnJudged(i int32, observed, judged time.Time) {
	if first, ok := m.UnreadFirstAt[i]; ok && first.Equal(observed) && !first.After(judged) {
		delete(m.UnreadFirstAt, i)
	}
}

// OnRevoked is a revoke an engine observed. Nothing but the check it sets a
// time for decides anything.
func (m *Machine) OnRevoked(now time.Time, cause, digest, save string) {
	if m.Revoke != nil || m.ended || m.wanted() == 0 || !m.runs(digest, save) {
		return
	}
	m.Revoke = &Revoke{Cause: cause, Digest: digest, Save: save, FirstAt: now, Due: now.Add(m.Pinned.RevokeHold)}
}

func (m *Machine) runs(digest, save string) bool {
	return slices.ContainsFunc(m.specs(), func(s Spec) bool { return s.Digest == digest && s.AdmissionSave == save })
}

// specs are what the slots run, each once.
func (m *Machine) specs() []Spec {
	var out []Spec
	for _, i := range m.indexes() {
		for _, a := range m.slots[i].activities() {
			if !slices.ContainsFunc(out, func(s Spec) bool { return s.Digest == a.spec.Digest && s.AdmissionSave == a.spec.AdmissionSave }) {
				out = append(out, a.spec)
			}
		}
	}
	return out
}

// OnChecked is the hold-down's check: allowed, refused with its detail, or not
// had.
func (m *Machine) OnChecked(now time.Time, allowed, refused bool, detail string) []Command {
	m.checking = false
	if m.Revoke == nil || !m.Revoke.FirstAt.Equal(m.checkOf) {
		// It was of a revoke that has ended.
		return nil
	}
	switch {
	case allowed:
		m.Revoke = nil
	case refused, now.Sub(m.Revoke.FirstAt) >= m.Pinned.RevokeUnavailableMax:
		if !refused {
			detail = "recheck=unanswered " + m.Revoke.Cause
		}
		// It stands until the pause does, which ends it: one that cannot be
		// written is checked and written again.
		m.Revoke.Due = now.Add(m.Pinned.RevokeHold)
		return m.pause(pauseInstanceFailed, refusalRevoked, detail)
	default:
		m.Revoke.Due = now.Add(m.Pinned.RevokeHold)
	}
	return nil
}

// OnReroute is an activity something took and never confirmed.
func (m *Machine) OnReroute(now time.Time, i int32, epoch int64) []Command {
	s, a := m.find(i, epoch)
	if a == nil || m.ended || a.stops() || a.phase != PhaseScheduled || !m.mayFreshen(now, i) {
		return nil
	}
	m.countFresh(now, i)
	if a == s.replacement {
		if s.current != nil {
			// What asked for a re-arm may ask again.
			delete(s.current.had, ReportHealthy)
		}
		return append(m.stopReplacement(now, i), m.apply(now)...)
	}
	return append(m.stop(now, i, a), m.apply(now)...)
}

// Handover ends this execution for one on another release.
func (m *Machine) OnHandover(now time.Time) []Command {
	if m.ended {
		return nil
	}
	m.ending = EndedHandover
	return m.stopAll(now)
}

func (m *Machine) stopAll(now time.Time) []Command {
	if m.Loaded == nil {
		return m.end(now, m.ending)
	}
	return m.apply(now)
}

// OnPurged is a purge's answer.
func (m *Machine) OnPurged(now time.Time, done bool, failed bool) []Command {
	m.purging = false
	switch {
	case failed:
		m.purgeFailing = true
		m.nextPurge = now.Add(backoff(m.Pinned.LoadBackoff, m.Pinned.LoadBackoffMax, m.purgeTries))
		m.purgeTries++
		return nil
	case done:
		return m.end(now, EndedPurged)
	}
	m.purgeFailing, m.purgeTries = false, 0
	return m.apply(now)
}

// OnHistory is how large the execution's history is.
func (m *Machine) OnHistory(now time.Time, events, bytes int64) []Command {
	m.events, m.bytes = events, bytes
	return m.apply(now)
}
