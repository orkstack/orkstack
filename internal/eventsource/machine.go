package eventsource

import (
	"errors"
	"maps"
	"slices"
	"time"
)

// ErrCarryFormat is a carry a later release wrote in a format this one does
// not know.
var ErrCarryFormat = errors.New("eventsource: a carry of a format this release does not know")

// Phase is where an activity of an instance is.
type Phase string

const (
	PhaseScheduled Phase = "scheduled"
	PhaseStarted   Phase = "started"
	PhaseReady     Phase = "ready"
	PhaseStopping  Phase = "stopping"
	PhaseStuck     Phase = "stuck"
)

// What an instance reports.
const (
	ReportStarted = "started"
	ReportReady   = "ready"
	ReportInvalid = "invalid"
	ReportHealthy = "healthy"
)

// purpose is why a replacement was scheduled beside an instance.
type purpose int

const (
	// forInstance is an instance with none beside it.
	forInstance purpose = iota
	// forRoll tries the target.
	forRoll
	// forRearm runs what the instance runs, so that its next retry is near.
	forRearm
	// forReturn puts an index back on the last good one after a failed roll.
	forReturn
)

// activity is one activity the workflow scheduled and has not seen close.
type activity struct {
	epoch   int64
	spec    Spec
	purpose purpose
	phase   Phase
	// readyAt and startedAt are when the workflow was told.
	startedAt, readyAt time.Time
	// stopBy is when one that is stopping is gone on without.
	stopBy time.Time
	had    map[string]string
	// tried: what it runs is the target a roll or a change made in place is
	// trying, so that what it says can fail it.
	tried bool
}

func (a *activity) stops() bool { return a.phase == PhaseStopping || a.phase == PhaseStuck }

// slot is one index's: its instance, what is to replace it, and what is
// stopping.
type slot struct {
	current, replacement *activity
	old                  []*activity
	// due is when the next is scheduled, where one waits for its delay.
	due time.Time
	// beside is when a replacement may be scheduled, where the last one failed.
	beside time.Time
}

// activities are what of the slot has not closed.
func (s *slot) activities() []*activity {
	var out []*activity
	for _, a := range append([]*activity{s.current, s.replacement}, s.old...) {
		if a != nil {
			out = append(out, a)
		}
	}
	return out
}

// stopping reports whether something of the slot is being stopped and is not
// gone, nor gone on without.
func (s *slot) stopping() bool {
	return slices.ContainsFunc(s.old, func(a *activity) bool { return a.phase != PhaseStuck })
}

// Command is what the workflow is to do.
type Command interface{ command() }

type (
	// Schedule schedules an instance's activity.
	Schedule struct {
		Index int32
		Epoch int64
		Spec  Spec
		// Trial is given to the instances a resume starts, for the breaker of
		// that code; Instances is how many there are.
		Trial     bool
		TrialCode string
		Instances int32
		// OnTrial is a replacement a roll tries: one attempt that does not
		// become ready within the roll's wait ends it.
		OnTrial bool
		// CompleteAfterHealthy: a failure after a healthy run completes the
		// activity.
		CompleteAfterHealthy bool
	}
	// Cancel cancels an activity.
	Cancel struct {
		Index int32
		Epoch int64
	}
	// StartLoad loads the source. MissingSaid is when a load last found it
	// missing and said so: one that finds it so again says it once a day.
	StartLoad struct{ MissingSaid time.Time }
	// StartPurge removes what the source kept.
	StartPurge struct{}
	// StartCheck asks whether what runs is still allowed.
	StartCheck struct{ Specs []Spec }
	// WritePause pauses the source.
	WritePause struct {
		Code, Refusal, Detail string
		// Revision is the service revision that was loaded when it was decided:
		// a source with a newer one is not paused for what an older one did.
		Revision int64
		// ResumeSeq is the resume that was loaded: one made since is not undone.
		ResumeSeq int64
	}
	// End ends the workflow; its carry says how.
	End struct{}
	// ContinueAsNew continues it.
	ContinueAsNew struct{}
)

func (Schedule) command()      {}
func (Cancel) command()        {}
func (StartLoad) command()     {}
func (StartPurge) command()    {}
func (StartCheck) command()    {}
func (WritePause) command()    {}
func (End) command()           {}
func (ContinueAsNew) command() {}

// LoadOutcome is what a load found.
type LoadOutcome int

const (
	// LoadFailed could not be made, or not read.
	LoadFailed LoadOutcome = iota
	// LoadMissing found no document of the source's id, and no tombstone. It
	// changes nothing, as a failed one.
	LoadMissing
	// LoadFound found the document.
	LoadFound
	// LoadDeleting found it marked by a delete.
	LoadDeleting
	// LoadDeleted found the source's tombstone.
	LoadDeleted
)

// Load is what a load returned.
type Load struct {
	Outcome LoadOutcome
	Paused  bool
	Held    bool
	Target  Spec
	// Pool is the pool it asks for slots in, as the memo names it.
	Pool      string
	Instances int32
	ResumeSeq int64
	TrialCode string
	// InPlace: the target is to be applied by stopping each instance first.
	InPlace bool
	// Refused is why the target's spec was not admitted; "" where it was.
	Refused string
	// Repin is what an engine would pin today, after a restart.
	Repin *Pinned
	// MaxEvents and MaxBytes are the history limits the deployment declares.
	MaxEvents, MaxBytes int64
}

// Closing is how an activity closed.
type Closing struct {
	// Result is set where the activity completed.
	Result *Result
}

// EndKind is how a completed activity ended, as the workflow's own table
// judged it.
type EndKind int

const (
	// EndUnknown is a reason the table does not hold, or not so.
	EndUnknown EndKind = iota
	// EndUnmet is a listed reason whose condition does not hold.
	EndUnmet
	EndAfterHealthy
	EndRotated
	EndStopped
	EndStuck
	EndJudgment
	EndUnchangeable
)

// Result is a completed activity's end.
type Result struct {
	Kind   EndKind
	Reason string
	// Cause is the code behind the reason, of a closed set, or nothing.
	Cause  string
	Unread string
}

// UnknownEnd is the last end no row of the table holds.
type UnknownEnd struct {
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

// How a load stands.
const (
	loadOK      = "ok"
	loadFailing = "failing"
	loadMissing = "missing"

	// LoadIsOK and LoadIsFailing are what a memo says of it.
	LoadIsOK, LoadIsFailing = loadOK, loadFailing
)

// pauseState is how a pause the workflow decided stands.
type pauseState int

const (
	pauseNone pauseState = iota
	pauseWriting
	pauseWritten
)

// Machine is one source's workflow. Its methods take what happened and the
// workflow's clock, and answer what to do, in order.
type Machine struct {
	Carry
	slots map[int32]*slot

	loading, loadAgain bool
	load               string
	missingSaid        time.Time
	loadTries          int
	nextLoad           time.Time
	// begun: the first load of this execution has been answered.
	begun bool

	purging      bool
	purgeFailing bool
	purgeTries   int
	nextPurge    time.Time

	checking bool
	// pausing: while a pause is being written, and until the document is
	// loaded with it, nothing is started for a source that is to be paused.
	pausing pauseState
	// checkOf is the revoke the check was started for.
	checkOf time.Time

	// ending is why everything is being stopped, for an execution that ends.
	ending string
	ended  bool

	events, bytes       int64
	maxEvents, maxBytes int64
	unknown             *UnknownEnd
}

// New is a source's machine at the start of an execution. carry is the last
// execution's; nil is a source that has none.
func New(carry *Carry, pin Pinned, now time.Time) *Machine {
	m := &Machine{slots: map[int32]*slot{}, load: loadFailing}
	if carry != nil {
		m.Carry = carry.clone()
	} else {
		m.Carry = Carry{Format: CarryFormat, Pinned: pin}
	}
	if m.Index == nil {
		m.Index = map[int32]IndexFacts{}
	}
	if m.UnreadFirstAt == nil {
		m.UnreadFirstAt = map[int32]time.Time{}
	}
	// An epoch is the source's: past every one it has used, at a schedule a
	// second or fewer, whatever execution used it.
	m.NextEpoch = max(m.NextEpoch, now.Unix())
	// An index that was being started again in turn waits its delay again.
	for i, f := range m.Index {
		if f.Stops > 0 {
			m.slots[i] = &slot{due: now.Add(backoff(m.Pinned.RestartBase, m.Pinned.RestartMax, f.Stops))}
		}
	}
	return m
}

// Start begins the execution: it loads, with a carry and without.
func (m *Machine) Start() []Command { return m.startLoad() }

func (m *Machine) startLoad() []Command {
	if m.loading {
		m.loadAgain = true
		return nil
	}
	m.loading, m.loadAgain, m.nextLoad = true, false, time.Time{}
	return []Command{StartLoad{MissingSaid: m.missingSaid}}
}

// missingAgain is how often a document that stays missing is said to be.
const missingAgain = 24 * time.Hour

// SayMissing reports whether a load that finds the document missing says so,
// where one last did at said.
func SayMissing(now, said time.Time) bool { return said.IsZero() || now.Sub(said) >= missingAgain }

// OnSync says the document may have changed. While loads fail it changes
// nothing: the back-off's next load sees it.
func (m *Machine) OnSync() []Command {
	if m.ended || (m.begun && m.load != loadOK) {
		return nil
	}
	return m.startLoad()
}

// OnLoaded is a load's answer.
func (m *Machine) OnLoaded(now time.Time, l Load) []Command {
	m.loading, m.begun = false, true
	switch l.Outcome {
	case LoadFailed, LoadMissing:
		m.load = loadFailing
		if l.Outcome == LoadMissing {
			m.load = loadMissing
			if SayMissing(now, m.missingSaid) {
				m.missingSaid = now
			}
		}
		m.nextLoad = now.Add(backoff(m.Pinned.LoadBackoff, m.Pinned.LoadBackoffMax, m.loadTries))
		m.loadTries++
		// An execution never depends on its load: one with a carry starts
		// what the carry says ran, and keeps it up.
		return m.apply(now)
	}
	m.load, m.loadTries, m.nextLoad, m.missingSaid = loadOK, 0, now.Add(m.Pinned.DailyLoad), time.Time{}
	m.maxEvents, m.maxBytes = l.MaxEvents, l.MaxBytes
	if l.Repin != nil {
		m.Pinned = *l.Repin
	}
	if m.pausing == pauseWritten && !m.loadAgain {
		// A load that was asked for before it was written may not have seen it.
		m.pausing = pauseNone
	}
	was := m.Loaded
	if l.Outcome == LoadFound {
		if l.Paused || (was != nil && l.ResumeSeq > was.ResumeSeq) {
			// A pause, and a resume of a paused source, end a revoke that was
			// observed; a resume of a running one raises no resume_seq.
			m.Revoke = nil
		}
		m.Loaded = &LoadedFacts{Revision: l.Target.Revision, Paused: l.Paused, Held: l.Held, Instances: l.Instances, ResumeSeq: l.ResumeSeq, TrialCode: l.TrialCode}
		m.trials(was, l)
		m.take(l)
	} else {
		m.Revoke = nil
		m.Loaded = &LoadedFacts{Deleting: true}
		if was != nil {
			*m.Loaded = *was
			m.Loaded.Deleting = true
		}
	}
	var out []Command
	if m.loadAgain {
		out = m.startLoad()
	}
	return append(out, m.apply(now)...)
}

// trials settles whose next start a resume's trial is for: every index it
// resumes, once. A source's first start is no resume, and neither is that of
// an index more instances add.
func (m *Machine) trials(was *LoadedFacts, l Load) {
	from := l.Instances
	switch {
	case was == nil:
		from = 0
	case l.ResumeSeq == was.ResumeSeq:
		from = was.Instances
	}
	for i := from; i < l.Instances; i++ {
		f := m.Index[i]
		f.TrialSeq = l.ResumeSeq
		m.Index[i] = f
	}
}

// take takes the target a load found. Its save follows the newest load, and a
// new save ends what was seen of the one before it.
func (m *Machine) take(l Load) {
	if m.Target != nil && m.Target.AdmissionSave != l.Target.AdmissionSave {
		clear(m.UnreadFirstAt)
	}
	if m.Target == nil || !m.Target.same(l.Target) {
		m.InPlace = l.InPlace
	}
	target := l.Target
	m.Target = &target
	if m.Good != nil && m.Good.same(target) {
		*m.Good = target
	}
	failed := m.Failed != nil && m.Failed.same(target)
	switch {
	case l.Refused != "" && !failed:
		m.failTarget(l.Refused)
		m.Failed.Refused = true
	case l.Refused == "" && failed && m.Failed.Refused:
		// A refusal whose cause cleared: the target is tried again.
		m.Failed = nil
	}
}

func (m *Machine) failTarget(reason string) {
	m.Failed = &FailedSpec{Spec: *m.Target, Reason: reason}
}

// backoff is base doubled tries times, at most bound.
func backoff(base, bound time.Duration, tries int) time.Duration {
	d := base
	for range tries {
		if d >= bound {
			break
		}
		d *= 2
	}
	return min(d, bound)
}

// running is how many activities the workflow has not seen close, but for
// those it has gone on without.
func (m *Machine) running() int {
	n := 0
	for _, s := range m.slots {
		for _, a := range s.activities() {
			if a.phase != PhaseStuck {
				n++
			}
		}
	}
	return n
}

func (m *Machine) indexes() []int32 { return slices.Sorted(maps.Keys(m.slots)) }
