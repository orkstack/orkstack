package engine

// The resume planner (E85, PR 3): which tasks of a failed or force-cancelled
// run rerun when it is resumed, and what every other task keeps.
//
// PlanResume is pure. It runs in the API process, never in workflow code, and
// decides only from its input: the base's terminal state as every reader
// derives it, the pinned definition, what the base's history says
// (ResumeBaseFacts, built by ScanResumeBase), and what verification found in
// object storage (ResumeEvidence). It reads no clock, no store and no
// cluster, so a preview and a confirm given the same input plan the same.
//
// It fails closed: an input it cannot account for (a row the definition does
// not declare, a status it does not know, a kept candidate with no
// verification answer, a recorded settle with no follows) is an error, never
// a plan, and a run ending it does not know is refused.

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// MaxResumesPerRun is the cap on resumes of one run that got going
// (docs/RESUME.md, "The resume budget").
const MaxResumesPerRun = 20

// forceCanceledReason is E88's run ending for a force cancel (E88 emits it
// and documents it).
const forceCanceledReason = "RUN_FORCE_CANCELED"

// taskStoppedByRunTimeoutCode is E107's reason for a task its run's timeout
// stopped; such a task is a failed seed, as a task failure is.
const taskStoppedByRunTimeoutCode = "TASK_STOPPED_BY_RUN_TIMEOUT"

// ResumeAction is what the caller asked for.
type ResumeAction int

const (
	// ResumeActionResume reruns the failed tasks and their downstream.
	ResumeActionResume ResumeAction = iota
	// ResumeActionRetryFrom also reruns the chosen task and its downstream.
	ResumeActionRetryFrom
)

// ResumeBaseClass is how the base execution ended.
type ResumeBaseClass int

const (
	// ResumeBaseFailed is a base that closed FAILED with a terminal snapshot.
	ResumeBaseFailed ResumeBaseClass = iota
	// ResumeBaseForceCanceled is a base E88's force cancel ended, whose
	// ForceCancelRecord the caller verified. Its state is the reader's own
	// synthesized terminal state.
	ResumeBaseForceCanceled
)

// ResumeUnit names one planned unit: a task, a fan-out's node (Item -1), or
// one item of a fan-out.
type ResumeUnit struct {
	Task string
	Item int
}

func (u ResumeUnit) String() string {
	if u.Item < 0 {
		return u.Task
	}
	return fmt.Sprintf("%s[%d]", u.Task, u.Item)
}

func nodeUnit(task string) ResumeUnit { return ResumeUnit{Task: task, Item: -1} }

// ResumeCategory is the dialog section a unit is listed under (design §3.4).
type ResumeCategory string

const (
	CategoryRerunFailed     ResumeCategory = "rerun_failed"
	CategoryRerunSelected   ResumeCategory = "rerun_selected"
	CategoryRerunDownstream ResumeCategory = "rerun_downstream"
	CategoryRerunResultGone ResumeCategory = "rerun_result_gone"
	CategoryRerunUnfinished ResumeCategory = "rerun_unfinished"
	CategoryRerunPinMember  ResumeCategory = "rerun_pin_member"
	CategoryRerunEachSource ResumeCategory = "rerun_each_source"
	CategoryKept            ResumeCategory = "kept"
	CategorySettled         ResumeCategory = "settled"
	CategorySkipped         ResumeCategory = "skipped"
	CategoryWaitsAgain      ResumeCategory = "waits_again"
	CategorySleepKept       ResumeCategory = "sleep_kept"
	CategoryEarlier         ResumeCategory = "earlier"
)

// rerunPriority orders the rerun categories: a unit that several rules put
// in S is listed under the first one that applies: what the caller asked
// for (failed, selected, downstream of those) before what the resume adds.
var rerunPriority = []ResumeCategory{
	CategoryRerunFailed, CategoryRerunSelected, CategoryRerunDownstream, CategoryRerunResultGone,
	CategoryRerunPinMember, CategoryRerunEachSource, CategoryRerunUnfinished,
}

func higherPriority(a, b ResumeCategory) bool {
	return slices.Index(rerunPriority, a) < slices.Index(rerunPriority, b)
}

// ResumeDisposition is what the resumed run does with a unit. It mirrors
// orkstack.v1.ResumeDisposition, whose values land with the resume block.
type ResumeDisposition int

const (
	DispositionNone ResumeDisposition = iota // earlier generation: as checkpointed
	DispositionRerun
	DispositionAdopt
	DispositionSettle
	DispositionKeepSkipped
	DispositionWait
	DispositionSleepUntil
)

// Proto is the disposition on the wire; none for a unit left as checkpointed.
func (d ResumeDisposition) Proto() orkstackv1.ResumeDisposition {
	return wireDispositions[d]
}

var dispositionNames = [...]string{"NONE", "RERUN", "ADOPT", "SETTLE", "KEEP_SKIPPED", "WAIT", "SLEEP_UNTIL"}

func (d ResumeDisposition) String() string {
	if d < 0 || int(d) >= len(dispositionNames) {
		return fmt.Sprintf("ResumeDisposition(%d)", int(d))
	}
	return dispositionNames[d]
}

// KeptAnswer is what verification (design §3.5) found for a kept candidate's
// saved result: its commit for a container task or item, its values object
// or record for an await or a composed task.
type KeptAnswer int

const (
	keptUnasked KeptAnswer = iota
	// KeptValid: the result is there and is this unit's.
	KeptValid
	// KeptGone: absent, undecodable, or another unit's. The unit reruns;
	// ResumeEvidence.Why says which, for the dialog.
	KeptGone
	// KeptUnavailable: the store did not answer. The resume waits.
	KeptUnavailable
	// KeptUnreadable: a record of a format this release cannot read. The
	// resume is refused, never planned around it (design §28.29 E85-25).
	KeptUnreadable
)

// ResolutionAnswer is what is on record, or in the base's history after R,
// for an await that did not succeed in the base (design §28.1, §26.5): a
// completion or a rejection made while the await was open. Neither means
// nothing usable (none, late, a scope mismatch, or an unrecorded Signal of a
// recorded run). A recorded answer's follows are in ResumeEvidence.Follows.
type ResolutionAnswer struct {
	Completed, Rejected bool
}

// ResumeFollow is one row of a record's follows[].
type ResumeFollow struct {
	Task         string
	ResultSHA256 string
}

// ResumeEvidence is what verification found. Every kept candidate the
// planner may keep must have an answer in Kept; a missing one is a bug in
// the caller, not a reason to keep or to rerun.
type ResumeEvidence struct {
	Kept map[ResumeUnit]KeptAnswer
	// Why is the dialog detail of a KeptGone answer (absent, undecodable,
	// another unit's) or a KeptUnavailable one (the store's error).
	Why map[ResumeUnit]string
	// Recorded says the run carries value records (identity's ValueRecords):
	// an await or composed task then settles only under the follows rule,
	// and only from its record.
	Recorded bool
	// Follows is, for a recorded run, the follows[] of every record a settle
	// would rest on (a kept or resolved await, a composed task), by task.
	Follows map[string][]ResumeFollow
	// ResultSHA256 is each kept task's current result digest, which a
	// follows row is compared with.
	ResultSHA256 map[string]string
	// Resolutions is, for an await that did not succeed in the base, the
	// answer on record (or, for a run without records, in history after R).
	Resolutions map[string]ResolutionAnswer
	// ChildOnRecord names the composed tasks of a recorded root run that
	// did not succeed in the base but whose child closed SUCCEEDED with its
	// outputs on record (design §28.3): they settle under the follows rule.
	ChildOnRecord map[string]bool
}

// ResetPoint is R: the base workflow task the reset re-executes from (design
// §3.2). A zero CompletedEventID means the base has none.
type ResetPoint struct {
	CompletedEventID int64
	ScheduledEventID int64
	StartedEventID   int64
}

// Found reports whether the base has a reset point.
func (r ResetPoint) Found() bool { return r.CompletedEventID > 0 }

// ResumeBaseFacts is what the base's history says, from ScanResumeBase.
type ResumeBaseFacts struct {
	ResetPoint ResetPoint
	// Continued: the base is a continued generation (not generation 0).
	Continued bool
	// Earlier names the tasks whose ending came from an earlier generation.
	Earlier map[string]bool
	// Dispatched is D: the units with a task attempt scheduled in the
	// current generation. It is required: nil is an error.
	Dispatched map[ResumeUnit]bool
	// OpenPins names the pins a force cancel caught open (OpenPinsAt).
	// Required, possibly empty, for a force-cancelled base.
	OpenPins map[string]bool
	// StateUnreadable: the checkpoint or by-reference definition the
	// generation started from does not verify.
	StateUnreadable bool
}

// ResumePlanInput is everything PlanResume decides from.
type ResumePlanInput struct {
	Definition  *orkstackv1.Orkflow
	Base        *orkstackv1.Run
	Class       ResumeBaseClass
	Facts       ResumeBaseFacts
	Evidence    ResumeEvidence
	Action      ResumeAction
	FromTask    string
	ResumesUsed int
}

// ResumePlanUnit is one unit's plan.
type ResumePlanUnit struct {
	Unit        ResumeUnit
	Category    ResumeCategory
	Disposition ResumeDisposition
	InRerunSet  bool
	// Detail is extra dialog text, for example a late commit's.
	Detail string
}

// ResumePlan is the planner's answer.
type ResumePlan struct {
	// Units lists every unit of the run once, in definition order, a task's
	// items after it.
	Units []ResumePlanUnit
	// AddsNothing: Retry from here plans exactly what Resume would.
	AddsNothing bool
}

// RerunSet is S, in the order of Units.
func (p *ResumePlan) RerunSet() []ResumeUnit {
	var out []ResumeUnit
	for _, u := range p.Units {
		if u.InRerunSet {
			out = append(out, u.Unit)
		}
	}
	return out
}

// lateCommitDetail is the dialog detail of a task a force cancel caught
// running whose commit landed after the run's last record (U3-1).
const lateCommitDetail = "finished after the run's last record"

// ---------------------------------------------------------------- refusals

// ResumeRefusalCode is a RUN_NOT_RESUMABLE sub-reason (docs/REASONS.md).
type ResumeRefusalCode string

const (
	RefuseStateNotResumable   ResumeRefusalCode = "STATE_NOT_RESUMABLE"
	RefuseNeverStarted        ResumeRefusalCode = "NEVER_STARTED"
	RefuseNoResumePoint       ResumeRefusalCode = "NO_RESUME_POINT"
	RefuseNotATaskFailure     ResumeRefusalCode = "NOT_A_TASK_FAILURE"
	RefuseFailedInEarlierPart ResumeRefusalCode = "FAILED_IN_EARLIER_PART"
	RefuseStateUnreadable     ResumeRefusalCode = "STATE_UNREADABLE"
	RefuseFailsBeforeDispatch ResumeRefusalCode = "FAILS_BEFORE_DISPATCH"
	RefuseFailureNotRecorded  ResumeRefusalCode = "FAILURE_NOT_RECORDED"
	RefuseFromTaskUnknown     ResumeRefusalCode = "FROM_TASK_UNKNOWN"
	RefuseFromTaskRequired    ResumeRefusalCode = "FROM_TASK_REQUIRED"
	RefuseRetryFromTooEarly   ResumeRefusalCode = "RETRY_FROM_TOO_EARLY"
	RefuseComposedTask        ResumeRefusalCode = "COMPOSED_TASK"
	RefuseChildRun            ResumeRefusalCode = "CHILD_RUN"
	RefuseTooManyResumes      ResumeRefusalCode = "TOO_MANY_RESUMES"
	RefuseCheckUnavailable    ResumeRefusalCode = "RESUME_CHECK_UNAVAILABLE"
	RefuseTooLarge            ResumeRefusalCode = "RESUME_TOO_LARGE"
)

// ResumeRefusalClass says what a caller can do about a refusal.
type ResumeRefusalClass string

const (
	RefusalPermanent ResumeRefusalClass = "permanent"
	RefusalWait      ResumeRefusalClass = "wait"
	RefusalReplan    ResumeRefusalClass = "replan"
)

// ComposedCause is why a composed task would have to rerun.
type ComposedCause string

const (
	ComposedFailed       ComposedCause = "failed"
	ComposedStopped      ComposedCause = "stopped"
	ComposedNeverStarted ComposedCause = "never_started"
	ComposedSelected     ComposedCause = "selected"
	ComposedDownstream   ComposedCause = "downstream"
	ComposedResultGone   ComposedCause = "result_gone"
	// ComposedRunningAtForceCancel: its child was open at the force cancel.
	ComposedRunningAtForceCancel ComposedCause = "running_at_force_cancel"
	// ComposedFollowsChanged: what its child's values followed has changed.
	ComposedFollowsChanged ComposedCause = "follows_changed"
)

// ResumeRefusal is a planned refusal: the run cannot be resumed as asked.
type ResumeRefusal struct {
	Code    ResumeRefusalCode
	Class   ResumeRefusalClass
	Cause   string
	Tasks   []string
	Message string
}

func (r *ResumeRefusal) Error() string {
	return fmt.Sprintf("run can't be resumed (%s): %s", r.Code, r.Message)
}

func refuse(code ResumeRefusalCode, class ResumeRefusalClass, format string, args ...any) *ResumeRefusal {
	return &ResumeRefusal{Code: code, Class: class, Message: fmt.Sprintf(format, args...)}
}

// resumableRunEndings are the run-level reasons a FAILED base may carry and
// still be resumed (design §12.2, §22, §28.16). A reason not listed is not
// resumable: a new reason is refused until it is classified here.
var resumableRunEndings = map[string]bool{
	"RUN_FAILED":        true,
	RunTimedOutCode:     true,
	RunPauseTimeoutCode: true,
	// E88's endings after RUN_STARTED; before it the base has no R and is
	// refused NEVER_STARTED.
	"RUN_QUEUE_TIMEOUT":           true,
	"RUN_CONCURRENCY_LIMIT":       true,
	"RUN_CONCURRENCY_FULL":        true,
	"RUN_CONCURRENCY_UNAVAILABLE": true,
	"RUN_CONCURRENCY_FENCED":      true,
	"RUN_CONCURRENCY_UNSUPPORTED": true,
	// A resume whose plan could not be run left its base as it was.
	ReasonResumePlanInvalid:    true,
	ReasonResumePlanUnreadable: true,
}

// ResumePlanNeverRan reports whether code is the ending of a resume whose plan
// could not be run: its run ended as the one before it had, with nothing run
// again.
func ResumePlanNeverRan(code string) bool {
	return code == ReasonResumePlanInvalid || code == ReasonResumePlanUnreadable
}

// ---------------------------------------------------------------- the plan

// planTask is one definition task with its base row.
type planTask struct {
	def   *orkstackv1.Task
	row   *orkstackv1.TaskRun
	items []int // the item indexes the base row holds, ascending
}

type taskKind int

const (
	kindContainer taskKind = iota
	kindFanOut             // a fan-out's node; its items are container units
	kindAwait
	kindSleep
	kindComposed
)

func (t *planTask) kind() taskKind {
	switch {
	case t.def.GetAwait() != nil:
		return kindAwait
	case t.def.GetSleep() != nil:
		return kindSleep
	case strings.HasPrefix(t.def.GetUses(), "orkflow:"):
		return kindComposed
	case t.def.GetEach() != nil:
		return kindFanOut
	}
	return kindContainer
}

// unitKind is a unit's own kind: a fan-out's item runs a container.
func (t *planTask) unitKind(u ResumeUnit) taskKind {
	if u.Item >= 0 {
		return kindContainer
	}
	return t.kind()
}

// unitRow is a unit's row in the base.
func (t *planTask) unitRow(u ResumeUnit) *orkstackv1.TaskRun {
	if u.Item < 0 {
		return t.row
	}
	for _, it := range t.row.GetItems() {
		if int(it.GetItem()) == u.Item {
			return it
		}
	}
	return nil
}

func (t *planTask) units() []ResumeUnit {
	out := []ResumeUnit{nodeUnit(t.def.GetId())}
	for _, i := range t.items {
		out = append(out, ResumeUnit{Task: t.def.GetId(), Item: i})
	}
	return out
}

type planner struct {
	in         ResumePlanInput
	order      []string
	tasks      map[string]*planTask
	dependents map[string][]string
	pins       map[string][]string
	eachOf     map[string][]string // each source task -> fan-out tasks over it

	kept      map[ResumeUnit]bool // a kept candidate whose result verified
	settled   map[ResumeUnit]bool // kept await or composed task, settle allowed
	withdrawn map[ResumeUnit]bool // a settle the follows rule withdrew
	waits     map[ResumeUnit]bool // an await or sleep that waits again, outside S
	late      map[ResumeUnit]bool // force cancel: dispatched, committed, adopted
	roots     map[ResumeUnit]ResumeCategory
	inS       map[ResumeUnit]ResumeCategory

	// Answers classify could not plan on, refused after the closure only for
	// a unit that stays outside S (a unit that reruns needs no verified
	// result), in a fixed order: permanent before wait (design §25.6).
	unreadable  []ResumeUnit
	unavailable []ResumeUnit
}

// PlanResume plans a resume. It returns a *ResumeRefusal when the run cannot
// be resumed as asked, and any other error when its input is inconsistent.
func PlanResume(in ResumePlanInput) (*ResumePlan, error) {
	if in.Definition == nil || in.Base == nil {
		return nil, errors.New("engine: plan resume: definition and base are required")
	}
	if in.ResumesUsed < 0 {
		return nil, fmt.Errorf("engine: plan resume: %d resumes used", in.ResumesUsed)
	}
	if r := refuseBase(in); r != nil {
		return nil, r
	}
	plan, err := planOnce(in, in.Action, in.FromTask)
	if err != nil {
		return nil, err
	}
	if in.Action == ResumeActionRetryFrom && in.Class == ResumeBaseFailed {
		resume, err := planOnce(in, ResumeActionResume, "")
		var r *ResumeRefusal
		switch {
		case err == nil:
			plan.AddsNothing = slices.Equal(resume.RerunSet(), plan.RerunSet())
		case !errors.As(err, &r):
			return nil, err
		}
	}
	if err := checkResumePlan(in, plan); err != nil {
		return nil, fmt.Errorf("engine: plan resume: the plan breaks an invariant: %w", err)
	}
	return plan, nil
}

func planOnce(in ResumePlanInput, action ResumeAction, from string) (*ResumePlan, error) {
	p, err := newPlanner(in)
	if err != nil {
		return nil, err
	}
	if r := p.refuseRequest(action, from); r != nil {
		return nil, r
	}
	if err := p.classify(); err != nil {
		return nil, err
	}
	return p.solve(action, from)
}

// RefuseResumeState refuses a run that is not in a state class resumes, from
// its status alone, so ResumeRun answers a running run before it reads its
// history.
func RefuseResumeState(base *orkstackv1.Run, class ResumeBaseClass) *ResumeRefusal {
	status := base.GetStatus()
	switch class {
	case ResumeBaseFailed:
		if status != orkstackv1.Status_STATUS_FAILED {
			return refuse(RefuseStateNotResumable, RefusalPermanent,
				"run %s is %s; only failed or force-cancelled runs can be resumed. Run again starts a fresh run",
				base.GetId(), resumeStatusWord(status))
		}
	case ResumeBaseForceCanceled:
		if status != orkstackv1.Status_STATUS_CANCELED || base.GetError().GetCode() != forceCanceledReason {
			return refuse(RefuseStateNotResumable, RefusalPermanent,
				"run %s is %s and was not force-cancelled; only failed or force-cancelled runs can be resumed. Run again starts a fresh run",
				base.GetId(), resumeStatusWord(status))
		}
	default:
		return refuse(RefuseStateNotResumable, RefusalPermanent, "run %s is in a state that can't be resumed", base.GetId())
	}
	return nil
}

// refuseBase refuses a base the resume cannot start from, before any task
// is looked at.
func refuseBase(in ResumePlanInput) *ResumeRefusal {
	if r := RefuseResumeState(in.Base, in.Class); r != nil {
		return r
	}
	code := in.Base.GetError().GetCode()
	if in.ResumesUsed >= MaxResumesPerRun {
		return refuse(RefuseTooManyResumes, RefusalPermanent,
			"run %s has been resumed %d times, counting resumes that ended waiting for a concurrency slot. Run again",
			in.Base.GetId(), MaxResumesPerRun)
	}
	if in.Base.GetOrigin().GetKind() == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN {
		return refuse(RefuseChildRun, RefusalPermanent,
			"this run was started by run %s's task %s; resuming it isn't supported yet. Run again on that run reruns everything",
			in.Base.GetOrigin().GetId(), in.Base.GetOrigin().GetParentTaskId())
	}
	if in.Class == ResumeBaseFailed {
		if code == "" {
			return refuse(RefuseFailureNotRecorded, RefusalPermanent, "its ending was never recorded. Run again")
		}
		if !resumableRunEndings[code] {
			return refuse(RefuseNotATaskFailure, RefusalPermanent,
				"it stopped because %s; rerunning tasks can't fix that. Run again", code)
		}
	}
	if in.Facts.StateUnreadable {
		return refuse(RefuseStateUnreadable, RefusalPermanent, "this run's saved state can't be read back. Run again")
	}
	if !in.Facts.ResetPoint.Found() {
		if in.Facts.Continued {
			return refuse(RefuseNoResumePoint, RefusalPermanent,
				"this run's latest part never got going before it ended; Run again")
		}
		return refuse(RefuseNeverStarted, RefusalPermanent,
			"run %s never started: its start was never recorded as done. Run again starts it fresh", in.Base.GetId())
	}
	if in.Class == ResumeBaseForceCanceled && in.FromTask == "" {
		return refuse(RefuseFromTaskRequired, RefusalReplan,
			"run %s was force-cancelled; choose the task to retry from (--from TASK)", in.Base.GetId())
	}
	return nil
}

func resumeStatusWord(s orkstackv1.Status) string {
	switch s {
	case orkstackv1.Status_STATUS_RUNNING, orkstackv1.Status_STATUS_PENDING, orkstackv1.Status_STATUS_AWAITING,
		orkstackv1.Status_STATUS_SLEEPING, orkstackv1.Status_STATUS_PAUSED:
		return "running"
	case orkstackv1.Status_STATUS_SUCCEEDED:
		return "succeeded"
	case orkstackv1.Status_STATUS_CANCELED:
		return "canceled"
	case orkstackv1.Status_STATUS_FAILED:
		return "failed"
	}
	return "in another state"
}

func knownRowStatus(s orkstackv1.Status) bool {
	switch s {
	case orkstackv1.Status_STATUS_PENDING, orkstackv1.Status_STATUS_RUNNING, orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_CANCELED, orkstackv1.Status_STATUS_SKIPPED,
		orkstackv1.Status_STATUS_AWAITING, orkstackv1.Status_STATUS_SLEEPING, orkstackv1.Status_STATUS_PAUSED:
		return true
	}
	return false
}

func newPlanner(in ResumePlanInput) (*planner, error) {
	if in.Facts.Dispatched == nil {
		return nil, errors.New("engine: plan resume: the history facts carry no dispatch set")
	}
	if in.Class == ResumeBaseForceCanceled && in.Facts.OpenPins == nil {
		return nil, errors.New("engine: plan resume: a force-cancelled base's facts carry no open pins")
	}
	p := &planner{
		in:         in,
		tasks:      map[string]*planTask{},
		dependents: map[string][]string{},
		pins:       map[string][]string{},
		eachOf:     map[string][]string{},
		kept:       map[ResumeUnit]bool{},
		settled:    map[ResumeUnit]bool{},
		withdrawn:  map[ResumeUnit]bool{},
		waits:      map[ResumeUnit]bool{},
		late:       map[ResumeUnit]bool{},
		roots:      map[ResumeUnit]ResumeCategory{},
		inS:        map[ResumeUnit]ResumeCategory{},
	}
	rows := map[string]*orkstackv1.TaskRun{}
	for _, r := range in.Base.GetTaskRuns() {
		if _, dup := rows[r.GetTaskId()]; dup {
			return nil, fmt.Errorf("engine: plan resume: the base has two rows for task %q", r.GetTaskId())
		}
		rows[r.GetTaskId()] = r
	}
	for _, d := range in.Definition.GetTasks() {
		id := d.GetId()
		if _, dup := p.tasks[id]; dup {
			return nil, fmt.Errorf("engine: plan resume: the definition declares task %q twice", id)
		}
		row := rows[id]
		if row == nil {
			// Every task has a row from the run's start (state.start), so a
			// missing one is a truncated state, never a task that never ran.
			return nil, fmt.Errorf("engine: plan resume: the base has no row for task %q", id)
		}
		if !knownRowStatus(row.GetStatus()) {
			return nil, fmt.Errorf("engine: plan resume: task %q has status %v", id, row.GetStatus())
		}
		t := &planTask{def: d, row: row}
		seen := map[int]bool{}
		for _, it := range row.GetItems() {
			if it == nil || it.Item == nil || it.GetItem() < 0 || seen[int(it.GetItem())] {
				return nil, fmt.Errorf("engine: plan resume: task %q has an item with no index, or one given twice", id)
			}
			if !knownRowStatus(it.GetStatus()) {
				return nil, fmt.Errorf("engine: plan resume: item %s[%d] has status %v", id, it.GetItem(), it.GetStatus())
			}
			seen[int(it.GetItem())] = true
			t.items = append(t.items, int(it.GetItem()))
		}
		slices.Sort(t.items)
		if len(t.items) > 0 && d.GetEach() == nil {
			return nil, fmt.Errorf("engine: plan resume: task %q has items but declares no each", id)
		}
		p.tasks[id] = t
		p.order = append(p.order, id)
		if pin := d.GetPin(); pin != "" {
			p.pins[pin] = append(p.pins[pin], id)
		}
		if src := d.GetEach().GetSource().GetTaskId(); src != "" {
			p.eachOf[src] = append(p.eachOf[src], id)
		}
	}
	for id := range rows {
		if p.tasks[id] == nil {
			return nil, fmt.Errorf("engine: plan resume: the base has a row for task %q, which the definition does not declare", id)
		}
	}
	for _, id := range p.order {
		for _, need := range p.tasks[id].def.GetNeeds() {
			if p.tasks[need] == nil {
				return nil, fmt.Errorf("engine: plan resume: task %q needs %q, which the definition does not declare", id, need)
			}
			p.dependents[need] = append(p.dependents[need], id)
		}
		// A lines fan-out reruns with its producer because the producer is
		// one of its own needs. Without that edge its items could be kept
		// over lines that were replaced. A "_" producer is no task: what the
		// run itself received (_inputs) never changes.
		if producer := p.tasks[id].def.GetEach().GetSource().GetArtifact().GetProducer(); producer != "" &&
			!strings.HasPrefix(producer, "_") && !slices.Contains(p.tasks[id].def.GetNeeds(), producer) {
			return nil, fmt.Errorf("engine: plan resume: fan-out %q reads the lines of %q, which it does not need", id, producer)
		}
	}
	for src := range p.eachOf {
		if p.tasks[src] == nil {
			return nil, fmt.Errorf("engine: plan resume: a fan-out is over task %q, which the definition does not declare", src)
		}
	}
	for id := range in.Facts.Earlier {
		if p.tasks[id] == nil {
			return nil, fmt.Errorf("engine: plan resume: the facts name earlier task %q, which the definition does not declare", id)
		}
	}
	for pin := range in.Facts.OpenPins {
		if p.pins[pin] == nil {
			return nil, fmt.Errorf("engine: plan resume: the facts name open pin %q, which no task declares", pin)
		}
	}
	if in.Evidence.Recorded {
		// The follows rule compares each succeeded need's current digest; a
		// succeeded task of a recorded run without one would pass unchecked.
		for _, id := range p.order {
			task := p.tasks[id]
			k := task.kind()
			if in.Facts.Earlier[id] || k == kindSleep || task.row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				continue
			}
			if _, has := in.Evidence.ResultSHA256[id]; !has && len(p.dependents[id]) > 0 {
				return nil, fmt.Errorf("engine: plan resume: succeeded task %q of a recorded run has no result digest", id)
			}
		}
	}
	return p, nil
}

// refuseRequest refuses what the caller asked for, independent of S.
func (p *planner) refuseRequest(action ResumeAction, from string) *ResumeRefusal {
	in := p.in
	switch {
	case action == ResumeActionRetryFrom && from == "":
		return refuse(RefuseFromTaskRequired, RefusalReplan, "retry from here needs a task (--from TASK)")
	case action != ResumeActionRetryFrom && from != "":
		return refuse(RefuseFromTaskUnknown, RefusalPermanent, "a task (%s) is chosen only to retry from it", from)
	case from != "":
		if p.tasks[from] == nil {
			return refuse(RefuseFromTaskUnknown, RefusalPermanent, "run %s has no task %s", in.Base.GetId(), from)
		}
		if in.Facts.Earlier[from] {
			r := refuse(RefuseRetryFromTooEarly, RefusalPermanent,
				"task %s finished in an earlier part of this run's history, before it carried on; retry from a later task, or Run again",
				from)
			r.Tasks = []string{from}
			return r
		}
	}
	for _, id := range p.order {
		if in.Facts.Earlier[id] && isFailedSeed(p.tasks[id].row) {
			r := refuse(RefuseFailedInEarlierPart, RefusalPermanent,
				"this run failed before it carried on into its current part; resuming it isn't supported. Run again")
			r.Tasks = []string{id}
			return r
		}
	}
	return nil
}

func isFailedSeed(row *orkstackv1.TaskRun) bool {
	return row.GetStatus() == orkstackv1.Status_STATUS_FAILED ||
		row.GetError().GetCode() == taskStoppedByRunTimeoutCode
}

// keptCandidate reports whether a row's result is a kept candidate: a
// success, or a row an earlier resume kept and had not read back.
func keptCandidate(row *orkstackv1.TaskRun) bool {
	return row.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED || row.GetReadingBack() ||
		row.GetError().GetCode() == ReasonResumeNotReadBack
}

// answer returns the verification answer for a kept candidate, recording
// an unavailable or unreadable one for the refusal decided after the
// closure. ok says the answer may be planned on: valid or gone.
func (p *planner) answer(u ResumeUnit) (a KeptAnswer, ok bool, err error) {
	a = p.in.Evidence.Kept[u]
	switch a {
	case KeptValid, KeptGone:
		return a, true, nil
	case KeptUnavailable:
		p.unavailable = append(p.unavailable, u)
		return a, false, nil
	case KeptUnreadable:
		p.unreadable = append(p.unreadable, u)
		return a, false, nil
	case keptUnasked:
		return a, false, fmt.Errorf("engine: plan resume: no verification answer for %s, a kept candidate", u)
	}
	return a, false, fmt.Errorf("engine: plan resume: verification answer %d for %s is not one the planner knows", int(a), u)
}

func (p *planner) why(u ResumeUnit, fallback string) string {
	if w := p.in.Evidence.Why[u]; w != "" {
		return w
	}
	return fallback
}

// classify decides each current-generation unit's starting point: kept,
// settled, a root, or waiting again.
func (p *planner) classify() error {
	for _, id := range p.order {
		if p.in.Facts.Earlier[id] {
			continue
		}
		task := p.tasks[id]
		for _, u := range task.units() {
			if task.unitKind(u) == kindFanOut {
				// A fan-out's node has no result of its own to verify: it is
				// kept when every item is, and the node rule puts it in S
				// once any item reruns.
				switch row := task.row; {
				case row.GetReadingBack() || row.GetError().GetCode() == ReasonResumeNotReadBack:
					// An earlier resume had not read it back: it stays kept
					// (§4.4); its items carry their own answers.
					p.kept[u] = true
				case isFailedSeed(row):
					p.roots[u] = CategoryRerunFailed
				case row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED &&
					row.GetStatus() != orkstackv1.Status_STATUS_SKIPPED:
					p.roots[u] = CategoryRerunUnfinished
				}
				continue
			}
			if err := p.classifyUnit(task, u, task.unitRow(u)); err != nil {
				return err
			}
		}
	}
	if p.in.Class == ResumeBaseForceCanceled {
		// G-d: every member of a pin open at the force cancel reruns.
		for pin := range p.in.Facts.OpenPins {
			for _, id := range p.pins[pin] {
				if !p.in.Facts.Earlier[id] {
					p.root(nodeUnit(id), CategoryRerunPinMember)
				}
			}
		}
	}
	return nil
}

// root makes u a root under cat, keeping a higher-priority category.
func (p *planner) root(u ResumeUnit, cat ResumeCategory) {
	if old, ok := p.roots[u]; ok && !higherPriority(cat, old) {
		return
	}
	p.roots[u] = cat
}

func (p *planner) classifyUnit(task *planTask, u ResumeUnit, row *orkstackv1.TaskRun) error {
	in := p.in
	kind := task.unitKind(u)
	status := row.GetStatus()
	switch {
	case status == orkstackv1.Status_STATUS_SKIPPED && !row.GetReadingBack():
		return nil
	case kind == kindSleep:
		switch {
		case status == orkstackv1.Status_STATUS_SUCCEEDED:
			p.kept[u] = true
		case isFailedSeed(row):
			p.roots[u] = CategoryRerunFailed
		default:
			p.waits[u] = true
		}
		return nil
	}
	// A force-cancelled base: a container unit the base dispatched and that
	// was still unfinished is judged by its commit, whatever the synthesized
	// snapshot says (design §24.1). A recorded failure stays a failure.
	if in.Class == ResumeBaseForceCanceled && kind == kindContainer && in.Facts.Dispatched[u] &&
		!keptCandidate(row) && !isFailedSeed(row) {
		a, ok, err := p.answer(u)
		switch {
		case err != nil:
			return err
		case !ok:
			p.kept[u] = true // refused once the closure is known
		case a == KeptValid:
			p.kept[u], p.late[u] = true, true
		default:
			p.roots[u] = CategoryRerunUnfinished
		}
		return nil
	}
	if keptCandidate(row) {
		a, ok, err := p.answer(u)
		switch {
		case err != nil:
			return err
		case !ok:
			p.kept[u] = true
			return nil
		case a == KeptGone:
			// An await whose values are gone waits again; any other unit
			// reruns. Either way the closure takes its downstream.
			p.roots[u] = CategoryRerunResultGone
			return nil
		}
		p.kept[u] = true
		if kind == kindAwait || kind == kindComposed {
			p.settled[u] = true
		}
		return nil
	}
	switch kind {
	case kindAwait:
		return p.classifyOpenAwait(task, u, row)
	case kindComposed:
		if in.Evidence.ChildOnRecord[u.Task] {
			if !in.Evidence.Recorded {
				return fmt.Errorf("engine: plan resume: composed task %s has a child on record in a run without records", u)
			}
			p.kept[u], p.settled[u] = true, true
			return nil
		}
	}
	if isFailedSeed(row) {
		p.roots[u] = CategoryRerunFailed
	} else {
		p.roots[u] = CategoryRerunUnfinished
	}
	return nil
}

// classifyOpenAwait classifies an await that did not succeed in the base,
// from the answer on record or in history (design §28.1, §26.5).
func (p *planner) classifyOpenAwait(task *planTask, u ResumeUnit, row *orkstackv1.TaskRun) error {
	in := p.in
	res, ok := in.Evidence.Resolutions[u.Task]
	if ok && res.Completed && res.Rejected {
		return fmt.Errorf("engine: plan resume: await %s was both completed and rejected", u)
	}
	answered := ok && (res.Completed || res.Rejected)
	if answered && in.Evidence.Recorded {
		follows, has := in.Evidence.Follows[u.Task]
		if !has {
			return fmt.Errorf("engine: plan resume: await %s's answer on record has no follows", u)
		}
		answered = p.followsCover(task, follows)
	}
	switch {
	case answered && res.Completed:
		// The answer's values object must still be there (§3.5).
		a, ok, err := p.answer(u)
		switch {
		case err != nil:
			return err
		case !ok:
			p.kept[u] = true
		case a == KeptValid:
			p.kept[u], p.settled[u] = true, true
		case isFailedSeed(row):
			p.roots[u] = CategoryRerunFailed
		default:
			p.waits[u] = true
		}
	case answered && res.Rejected:
		p.roots[u] = CategoryRerunFailed
	case isFailedSeed(row):
		p.roots[u] = CategoryRerunFailed
	default:
		p.waits[u] = true
	}
	return nil
}

// followsCover is the second half of the follows rule (design §28.H2): the
// record follows every need of the task that has a result, and each digest
// it follows still equals that need's current result. The first half, that
// no need is in S, is checked once S is known.
//
// A need's current result is the kept row's digest. A need that has none
// (it was skipped, slept, or reruns) is not compared: if it reruns, the
// first half of the rule withdraws the settle anyway.
func (p *planner) followsCover(task *planTask, follows []ResumeFollow) bool {
	byTask := map[string]string{}
	for _, f := range follows {
		byTask[f.Task] = f.ResultSHA256
	}
	for _, need := range task.def.GetNeeds() {
		cur, has := p.in.Evidence.ResultSHA256[need]
		if !has {
			continue
		}
		if got, ok := byTask[need]; !ok || got != cur {
			return false
		}
	}
	for _, f := range follows {
		if cur, ok := p.in.Evidence.ResultSHA256[f.Task]; !ok || cur != f.ResultSHA256 {
			return false
		}
	}
	return true
}

// solve computes S as a fixpoint and lays out the plan.
func (p *planner) solve(action ResumeAction, from string) (*ResumePlan, error) {
	for u, cat := range p.roots {
		p.add(u, cat)
	}
	if action == ResumeActionRetryFrom && from != "" {
		p.addTaskWhole(from, CategoryRerunSelected)
	}
	// A withdrawn settle can widen S, and a wider S can withdraw another
	// settle: repeat until neither changes anything.
	for {
		p.close()
		changed, err := p.applyFollowsRule()
		if err != nil {
			return nil, err
		}
		if !changed {
			break
		}
	}
	// Refusals in a fixed order: permanent before wait.
	if u, ok := p.firstOutsideS(p.unreadable); ok {
		r := refuse(RefuseStateUnreadable, RefusalPermanent,
			"the saved result of %s is in a format this release can't read. Run again", u)
		r.Tasks = []string{u.Task}
		return nil, r
	}
	if r := p.refuseComposed(); r != nil {
		return nil, r
	}
	if r := p.refuseBeforeDispatch(); r != nil {
		return nil, r
	}
	if u, ok := p.firstOutsideS(p.unavailable); ok {
		r := refuse(RefuseCheckUnavailable, RefusalWait,
			"saved results can't be checked right now (the result of %s: %s); try again shortly",
			u, p.why(u, "the store did not answer"))
		r.Tasks = []string{u.Task}
		return nil, r
	}
	if len(p.inS) == 0 && len(p.waits) == 0 {
		return nil, refuse(RefuseNotATaskFailure, RefusalPermanent,
			"none of its tasks failed or was left unfinished, so rerunning tasks can't fix how it ended. Run again")
	}
	return p.layout()
}

// firstOutsideS is the first of units, in plan order, that S does not take.
func (p *planner) firstOutsideS(units []ResumeUnit) (ResumeUnit, bool) {
	var out []ResumeUnit
	for _, u := range units {
		if _, in := p.inS[u]; !in {
			out = append(out, u)
		}
	}
	if len(out) == 0 {
		return ResumeUnit{}, false
	}
	slices.SortFunc(out, func(a, b ResumeUnit) int {
		if c := slices.Index(p.order, a.Task) - slices.Index(p.order, b.Task); c != 0 {
			return c
		}
		return a.Item - b.Item
	})
	return out[0], true
}

// add puts u in S under cat, keeping a higher-priority category. It reports
// whether S changed.
func (p *planner) add(u ResumeUnit, cat ResumeCategory) bool {
	if old, ok := p.inS[u]; ok && !higherPriority(cat, old) {
		return false
	}
	p.inS[u] = cat
	return true
}

// addTaskWhole puts a task in S with every item it holds.
func (p *planner) addTaskWhole(id string, cat ResumeCategory) bool {
	task := p.tasks[id]
	if task == nil || p.in.Facts.Earlier[id] {
		return false
	}
	changed := false
	for _, u := range task.units() {
		changed = p.add(u, cat) || changed
	}
	return changed
}

// close applies the four closure rules until nothing changes, in a fixed
// order so the result never depends on map iteration.
func (p *planner) close() {
	for changed := true; changed; {
		changed = false
		for _, id := range p.order {
			task := p.tasks[id]
			for _, u := range task.units() {
				cat, in := p.inS[u]
				if !in {
					continue
				}
				if u.Item >= 0 {
					// node: an item in S puts its node in S, under its reason.
					changed = p.add(nodeUnit(id), cat) || changed
					continue
				}
				for _, dep := range p.dependents[id] {
					changed = p.addTaskWhole(dep, CategoryRerunDownstream) || changed
				}
				if pin := task.def.GetPin(); pin != "" {
					for _, m := range p.pins[pin] {
						changed = p.addTaskWhole(m, CategoryRerunPinMember) || changed
					}
				}
				for _, fan := range p.eachOf[id] {
					changed = p.addTaskWhole(fan, CategoryRerunEachSource) || changed
				}
			}
			// A wait outside S produces a new result, so its downstream reruns.
			if u := nodeUnit(id); p.waits[u] {
				if _, in := p.inS[u]; !in {
					for _, dep := range p.dependents[id] {
						changed = p.addTaskWhole(dep, CategoryRerunDownstream) || changed
					}
				}
			}
		}
	}
}

// applyFollowsRule withdraws a settle whose answer no longer stands: a kept
// await or composed task settles only while none of its needs is in S and,
// for a recorded run, its record follows every need's current result
// (§28.H2). A withdrawn await waits again; a withdrawn composed task
// reruns, which is refused.
func (p *planner) applyFollowsRule() (changed bool, err error) {
	for _, id := range p.order {
		u := nodeUnit(id)
		if !p.settled[u] {
			continue
		}
		if _, in := p.inS[u]; in {
			continue
		}
		ok := !p.needInS(id)
		if ok && p.in.Evidence.Recorded {
			follows, has := p.in.Evidence.Follows[id]
			if !has {
				return false, fmt.Errorf("engine: plan resume: %s would settle from a record with no follows", u)
			}
			ok = p.followsCover(p.tasks[id], follows)
		}
		if ok {
			continue
		}
		delete(p.settled, u)
		delete(p.kept, u)
		p.withdrawn[u] = true
		changed = true
		if p.tasks[id].kind() == kindAwait {
			p.waits[u] = true
			continue
		}
		p.add(u, CategoryRerunDownstream)
	}
	return changed, nil
}

// needInS reports whether any transitive need of task id is in S.
func (p *planner) needInS(id string) bool {
	seen := map[string]bool{}
	stack := slices.Clone(p.tasks[id].def.GetNeeds())
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[n] {
			continue
		}
		seen[n] = true
		if _, in := p.inS[nodeUnit(n)]; in {
			return true
		}
		stack = append(stack, p.tasks[n].def.GetNeeds()...)
	}
	return false
}

// refuseComposed refuses a composed task in S (design §7.4, §12.2). Its
// cause comes from why it is in S first, then from its row.
func (p *planner) refuseComposed() *ResumeRefusal {
	for _, id := range p.order {
		task := p.tasks[id]
		cat, in := p.inS[nodeUnit(id)]
		if task.kind() != kindComposed || !in {
			continue
		}
		status := task.row.GetStatus()
		var cause ComposedCause
		switch {
		case p.withdrawn[nodeUnit(id)] && !p.needInS(id):
			cause = ComposedFollowsChanged
		case cat == CategoryRerunSelected:
			cause = ComposedSelected
		case cat == CategoryRerunDownstream || cat == CategoryRerunPinMember || cat == CategoryRerunEachSource:
			cause = ComposedDownstream
		case cat == CategoryRerunResultGone:
			cause = ComposedResultGone
		case p.in.Class == ResumeBaseForceCanceled && status == orkstackv1.Status_STATUS_RUNNING:
			cause = ComposedRunningAtForceCancel
		case status == orkstackv1.Status_STATUS_FAILED:
			cause = ComposedFailed
		case status == orkstackv1.Status_STATUS_PENDING:
			cause = ComposedNeverStarted
		default:
			cause = ComposedStopped
		}
		r := refuse(RefuseComposedTask, RefusalPermanent,
			"task %s runs another Orkflow and would have to run again (%s); that isn't supported yet. Retry from a task after %s, or Run again",
			id, cause, id)
		r.Cause, r.Tasks = string(cause), []string{id}
		return r
	}
	return nil
}

// refuseBeforeDispatch refuses a failed task that failed before it was ever
// dispatched, unless something upstream of it reruns (design §3.2): with
// nothing upstream changed, it would fail the same way.
func (p *planner) refuseBeforeDispatch() *ResumeRefusal {
	d := p.in.Facts.Dispatched
	for _, id := range p.order {
		task := p.tasks[id]
		if task.kind() != kindContainer || p.inS[nodeUnit(id)] != CategoryRerunFailed {
			continue
		}
		if task.row.GetStatus() != orkstackv1.Status_STATUS_FAILED || d[nodeUnit(id)] || p.needInS(id) {
			continue
		}
		r := refuse(RefuseFailsBeforeDispatch, RefusalPermanent,
			"task %s failed before it ran; retry from a task upstream of it", id)
		if len(task.def.GetNeeds()) == 0 {
			r.Message = fmt.Sprintf("task %s failed before it ran; fix the Orkflow or its input and Run again", id)
		}
		r.Tasks = []string{id}
		return r
	}
	return nil
}

// layout writes every unit's category and disposition.
func (p *planner) layout() (*ResumePlan, error) {
	plan := &ResumePlan{}
	for _, id := range p.order {
		task := p.tasks[id]
		for _, u := range task.units() {
			pu, err := p.unitPlan(task, u)
			if err != nil {
				return nil, err
			}
			plan.Units = append(plan.Units, pu)
		}
	}
	return plan, nil
}

func (p *planner) unitPlan(task *planTask, u ResumeUnit) (ResumePlanUnit, error) {
	pu := ResumePlanUnit{Unit: u}
	if p.in.Facts.Earlier[u.Task] {
		pu.Category = CategoryEarlier
		return pu, nil
	}
	kind := task.unitKind(u)
	row := task.unitRow(u)
	if cat, in := p.inS[u]; in {
		pu.InRerunSet, pu.Category = true, cat
		switch kind {
		case kindAwait, kindSleep:
			pu.Disposition = DispositionWait
		default:
			pu.Disposition = DispositionRerun
		}
		if cat == CategoryRerunResultGone {
			pu.Detail = p.why(u, "")
		}
		return pu, nil
	}
	switch {
	case p.waits[u]:
		pu.Category, pu.Disposition = CategoryWaitsAgain, DispositionWait
	case row.GetStatus() == orkstackv1.Status_STATUS_SKIPPED && !p.kept[u]:
		pu.Category, pu.Disposition = CategorySkipped, DispositionKeepSkipped
	case kind == kindFanOut && (row.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED || p.kept[u]):
		// A succeeded node outside S: every item is kept. A fan-out over
		// an empty array succeeds with no items, and keeps nothing.
		pu.Category, pu.Disposition = CategoryKept, DispositionAdopt
	case kind == kindSleep && p.kept[u]:
		pu.Category, pu.Disposition = CategorySleepKept, DispositionSleepUntil
	case p.settled[u]:
		pu.Category, pu.Disposition = CategorySettled, DispositionSettle
	case p.kept[u] && kind != kindFanOut:
		pu.Category, pu.Disposition = CategoryKept, DispositionAdopt
		if p.late[u] {
			pu.Detail = lateCommitDetail
		}
	default:
		return pu, fmt.Errorf("engine: plan resume: %s is outside S and neither kept, settled, skipped nor waiting", u)
	}
	return pu, nil
}

// checkResumePlan checks a plan against the invariants the design fuzzes
// (§3.1, §23.1): S is closed under the closure rules, every unit outside S
// keeps its base ending, no kept unit needs an S member or a wait, S lies in
// the current generation, no composed task is in S, every adopted or
// settled unit's result verified, and every member of a pin open at a force
// cancel reruns. PlanResume runs it on every plan it returns.
func checkResumePlan(in ResumePlanInput, plan *ResumePlan) error {
	p, err := newPlanner(in)
	if err != nil {
		return err
	}
	byUnit := map[ResumeUnit]ResumePlanUnit{}
	for _, u := range plan.Units {
		if _, dup := byUnit[u.Unit]; dup {
			return fmt.Errorf("unit %s planned twice", u.Unit)
		}
		byUnit[u.Unit] = u
	}
	inS := func(u ResumeUnit) bool { return byUnit[u].InRerunSet }
	for _, id := range p.order {
		task := p.tasks[id]
		for _, u := range task.units() {
			pu, ok := byUnit[u]
			if !ok {
				return fmt.Errorf("unit %s is not planned", u)
			}
			if in.Facts.Earlier[id] {
				if pu.InRerunSet || pu.Disposition != DispositionNone {
					return fmt.Errorf("earlier unit %s is planned %v", u, pu.Disposition)
				}
				continue
			}
			if pu.InRerunSet && task.kind() == kindComposed {
				return fmt.Errorf("composed task %s is in S", id)
			}
			if err := checkOutsideS(in, task, u, pu); err != nil {
				return err
			}
		}
		node := byUnit[nodeUnit(id)]
		for _, i := range task.items {
			if inS(ResumeUnit{Task: id, Item: i}) && !node.InRerunSet {
				return fmt.Errorf("item %s[%d] is in S but its node is not", id, i)
			}
		}
		if node.InRerunSet || node.Disposition == DispositionWait {
			// A dependent runs again whole, every item of a fan-out too: an
			// item kept over a replaced result (an array, or the lines of an
			// artifact) would be a silently wrong one.
			for _, dep := range p.dependents[id] {
				if in.Facts.Earlier[dep] {
					continue
				}
				for _, u := range p.tasks[dep].units() {
					if !inS(u) {
						return fmt.Errorf("%s reruns or waits again but its dependent %s does not rerun", id, u)
					}
				}
			}
		}
		if node.InRerunSet {
			if pin := task.def.GetPin(); pin != "" {
				for _, m := range p.pins[pin] {
					if !in.Facts.Earlier[m] && !inS(nodeUnit(m)) {
						return fmt.Errorf("pin %s: %s is in S but member %s is not", pin, id, m)
					}
				}
			}
			for _, fan := range p.eachOf[id] {
				for _, u := range p.tasks[fan].units() {
					if !in.Facts.Earlier[fan] && !inS(u) {
						return fmt.Errorf("each source %s is in S but %s is not", id, u)
					}
				}
			}
		}
		if !node.InRerunSet && node.Disposition != DispositionWait && !in.Facts.Earlier[id] {
			for _, need := range task.def.GetNeeds() {
				if inS(nodeUnit(need)) || byUnit[nodeUnit(need)].Disposition == DispositionWait {
					return fmt.Errorf("kept %s needs %s, which reruns or waits again", id, need)
				}
			}
		}
	}
	if in.Class == ResumeBaseForceCanceled {
		for pin := range in.Facts.OpenPins {
			for _, m := range p.pins[pin] {
				if !in.Facts.Earlier[m] && !inS(nodeUnit(m)) {
					return fmt.Errorf("pin %s was open at the force cancel, but member %s is not in S", pin, m)
				}
			}
		}
	}
	return nil
}

// checkOutsideS checks that a unit outside S ends as in the base: adopted
// or settled only from a verified result, kept skipped only if skipped,
// slept only if its sleep succeeded.
func checkOutsideS(in ResumePlanInput, task *planTask, u ResumeUnit, pu ResumePlanUnit) error {
	if pu.InRerunSet {
		return nil
	}
	row := task.unitRow(u)
	kind := task.unitKind(u)
	switch pu.Disposition {
	case DispositionAdopt:
		if kind == kindFanOut {
			if row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED && !keptCandidate(row) {
				return fmt.Errorf("fan-out %s is adopted but its node did not succeed", u)
			}
			return nil
		}
		if in.Evidence.Kept[u] != KeptValid {
			return fmt.Errorf("%s is adopted without a verified result", u)
		}
		late := in.Class == ResumeBaseForceCanceled && in.Facts.Dispatched[u] && pu.Detail == lateCommitDetail
		if !keptCandidate(row) && !late {
			return fmt.Errorf("%s is adopted but did not succeed in the base", u)
		}
		if isFailedSeed(row) {
			return fmt.Errorf("%s is adopted but failed in the base", u)
		}
	case DispositionSettle:
		if in.Evidence.Kept[u] != KeptValid && !in.Evidence.ChildOnRecord[u.Task] {
			return fmt.Errorf("%s is settled without a verified result", u)
		}
	case DispositionKeepSkipped:
		if row.GetStatus() != orkstackv1.Status_STATUS_SKIPPED {
			return fmt.Errorf("%s is kept skipped but was %v", u, row.GetStatus())
		}
	case DispositionSleepUntil:
		if kind != kindSleep || row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			return fmt.Errorf("%s sleeps until its wake-up but is not a succeeded sleep", u)
		}
	case DispositionWait:
		if kind != kindAwait && kind != kindSleep {
			return fmt.Errorf("%s waits again but is neither an await nor a sleep", u)
		}
	default:
		return fmt.Errorf("%s is outside S with disposition %v", u, pu.Disposition)
	}
	return nil
}
