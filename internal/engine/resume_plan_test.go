package engine

import (
	"errors"
	"slices"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// planFixture builds a ResumePlanInput from a compact description: the
// definition's tasks and the base's rows. Every kept candidate verifies
// unless a test says otherwise.
type planFixture struct {
	in ResumePlanInput
}

func ptask(id string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Needs: needs, Uses: "image:busybox"}
}

func prow(id string, status orkstackv1.Status) *orkstackv1.TaskRun {
	return &orkstackv1.TaskRun{TaskId: id, Status: status}
}

func pitem(i int32, status orkstackv1.Status) *orkstackv1.TaskRun {
	return &orkstackv1.TaskRun{Item: &i, Status: status}
}

const (
	sOK   = orkstackv1.Status_STATUS_SUCCEEDED
	sFail = orkstackv1.Status_STATUS_FAILED
	sSkip = orkstackv1.Status_STATUS_SKIPPED
	sCanc = orkstackv1.Status_STATUS_CANCELED
	sPend = orkstackv1.Status_STATUS_PENDING
	sRun  = orkstackv1.Status_STATUS_RUNNING
	sWait = orkstackv1.Status_STATUS_AWAITING
)

func newPlan(tasks []*orkstackv1.Task, rows ...*orkstackv1.TaskRun) *planFixture {
	f := &planFixture{}
	f.in = ResumePlanInput{
		Definition: &orkstackv1.Orkflow{Name: "wf", Tasks: tasks},
		Base: &orkstackv1.Run{
			Id: "ork-x", Status: sFail, TaskRuns: rows,
			Error: &orkstackv1.Error{Code: "RUN_FAILED"},
		},
		Class: ResumeBaseFailed,
		Facts: ResumeBaseFacts{ResetPoint: ResetPoint{CompletedEventID: 9, ScheduledEventID: 7, StartedEventID: 8}},
		Evidence: ResumeEvidence{
			Kept: map[ResumeUnit]KeptAnswer{},
		},
	}
	// Every unit was dispatched and every kept candidate verifies, unless a
	// test says otherwise.
	f.in.Facts.Dispatched = map[ResumeUnit]bool{}
	for _, r := range rows {
		u := ResumeUnit{Task: r.GetTaskId(), Item: -1}
		f.in.Evidence.Kept[u] = KeptValid
		f.in.Facts.Dispatched[u] = true
		for _, it := range r.GetItems() {
			u := ResumeUnit{Task: r.GetTaskId(), Item: int(it.GetItem())}
			f.in.Evidence.Kept[u] = KeptValid
			f.in.Facts.Dispatched[u] = true
		}
	}
	return f
}

func (f *planFixture) plan(t *testing.T) *ResumePlan {
	t.Helper()
	p, err := PlanResume(f.in)
	if err != nil {
		t.Fatalf("PlanResume: %v", err)
	}
	if err := checkResumePlan(f.in, p); err != nil {
		t.Fatalf("plan breaks an invariant: %v", err)
	}
	return p
}

func (f *planFixture) refusal(t *testing.T) *ResumeRefusal {
	t.Helper()
	_, err := PlanResume(f.in)
	var r *ResumeRefusal
	if !errors.As(err, &r) {
		t.Fatalf("PlanResume = %v, want a refusal", err)
	}
	return r
}

func rerunTasks(p *ResumePlan) []string {
	var out []string
	for _, u := range p.Units {
		if u.InRerunSet && u.Unit.Item < 0 {
			out = append(out, u.Unit.Task)
		}
	}
	return out
}

func unitOf(t *testing.T, p *ResumePlan, task string, item int) ResumePlanUnit {
	t.Helper()
	for _, u := range p.Units {
		if u.Unit == (ResumeUnit{Task: task, Item: item}) {
			return u
		}
	}
	t.Fatalf("no unit %s[%d] in the plan", task, item)
	return ResumePlanUnit{}
}

func TestResumePlan_ResumeRerunsFailedAndDownstream(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{
		ptask("a"), ptask("b", "a"), ptask("c", "b"), ptask("d"),
	}, prow("a", sOK), prow("b", sFail), prow("c", sCanc), prow("d", sOK))
	p := f.plan(t)
	if got, want := rerunTasks(p), []string{"b", "c"}; !slices.Equal(got, want) {
		t.Fatalf("S = %v, want %v", got, want)
	}
	if u := unitOf(t, p, "b", -1); u.Category != CategoryRerunFailed || u.Disposition != DispositionRerun {
		t.Errorf("b = %+v", u)
	}
	if u := unitOf(t, p, "c", -1); u.Category != CategoryRerunDownstream {
		t.Errorf("c category = %v, want downstream", u.Category)
	}
	if u := unitOf(t, p, "a", -1); u.Category != CategoryKept || u.Disposition != DispositionAdopt {
		t.Errorf("a = %+v", u)
	}
}

func TestResumePlan_UnfinishedRowsAreRoots(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("a"), ptask("p"), ptask("x")},
		prow("a", sCanc), prow("p", sPend), prow("x", sFail))
	p := f.plan(t)
	for _, id := range []string{"a", "p"} {
		if u := unitOf(t, p, id, -1); !u.InRerunSet || u.Category != CategoryRerunUnfinished {
			t.Errorf("%s = %+v, want rerun_unfinished", id, u)
		}
	}
}

func TestResumePlan_RetryFromAddsTheTaskAndItsDownstream(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{
		ptask("a"), ptask("b", "a"), ptask("c"), ptask("d", "c"),
	}, prow("a", sOK), prow("b", sOK), prow("c", sFail), prow("d", sCanc))
	f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "a"
	p := f.plan(t)
	if got, want := rerunTasks(p), []string{"a", "b", "c", "d"}; !slices.Equal(got, want) {
		t.Fatalf("S = %v, want %v", got, want)
	}
	if u := unitOf(t, p, "a", -1); u.Category != CategoryRerunSelected {
		t.Errorf("a category = %v", u.Category)
	}
	if p.AddsNothing {
		t.Error("AddsNothing set for a task that adds a and b")
	}
}

func TestResumePlan_RetryFromAFailedTaskAddsNothing(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("a"), ptask("b", "a")},
		prow("a", sFail), prow("b", sCanc))
	f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "a"
	if p := f.plan(t); !p.AddsNothing {
		t.Error("AddsNothing = false for a failed task")
	}
}

func TestResumePlan_SkippedStaysSkippedUnlessReached(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("a"), ptask("s"), ptask("t", "a")},
		prow("a", sFail), prow("s", sSkip), prow("t", sSkip))
	p := f.plan(t)
	if u := unitOf(t, p, "s", -1); u.Disposition != DispositionKeepSkipped || u.Category != CategorySkipped {
		t.Errorf("s = %+v", u)
	}
	if u := unitOf(t, p, "t", -1); !u.InRerunSet {
		t.Errorf("t, downstream of a, = %+v", u)
	}
}

func TestResumePlan_ReadingBackRowsStayKept(t *testing.T) {
	a := prow("a", sCanc)
	a.ReadingBack = true
	a.Error = &orkstackv1.Error{Code: notReadBackReason}
	f := newPlan([]*orkstackv1.Task{ptask("a"), ptask("b", "a")}, a, prow("b", sFail))
	p := f.plan(t)
	if u := unitOf(t, p, "a", -1); u.InRerunSet || u.Disposition != DispositionAdopt {
		t.Errorf("reading-back a = %+v, want kept", u)
	}
	f.in.Evidence.Kept[ResumeUnit{Task: "a", Item: -1}] = KeptGone
	p = f.plan(t)
	if u := unitOf(t, p, "a", -1); !u.InRerunSet || u.Category != CategoryRerunResultGone {
		t.Errorf("gone a = %+v, want rerun_result_gone", u)
	}
}

func TestResumePlan_KeptResultGoneIsARoot(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("a"), ptask("b", "a"), ptask("x")},
		prow("a", sOK), prow("b", sOK), prow("x", sFail))
	f.in.Evidence.Kept[ResumeUnit{Task: "a", Item: -1}] = KeptGone
	p := f.plan(t)
	if got, want := rerunTasks(p), []string{"a", "b", "x"}; !slices.Equal(got, want) {
		t.Fatalf("S = %v, want %v", got, want)
	}
}

func TestResumePlan_StoreUnansweredRefusesToWait(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("a"), ptask("x")}, prow("a", sOK), prow("x", sFail))
	f.in.Evidence.Kept[ResumeUnit{Task: "a", Item: -1}] = KeptUnavailable
	r := f.refusal(t)
	if r.Code != RefuseCheckUnavailable || r.Class != RefusalWait {
		t.Errorf("refusal = %+v", r)
	}
}

func TestResumePlan_PinRerunsWhole(t *testing.T) {
	a, b, c := ptask("a"), ptask("b", "a"), ptask("c")
	a.Pin, b.Pin = "p", "p"
	f := newPlan([]*orkstackv1.Task{a, b, c}, prow("a", sOK), prow("b", sFail), prow("c", sOK))
	p := f.plan(t)
	if u := unitOf(t, p, "a", -1); !u.InRerunSet || u.Category != CategoryRerunPinMember {
		t.Errorf("pin member a = %+v", u)
	}
	if u := unitOf(t, p, "c", -1); u.InRerunSet {
		t.Errorf("c = %+v, want kept", u)
	}
}

func eachTask(id, source string, needs ...string) *orkstackv1.Task {
	t := ptask(id, needs...)
	t.Each = &orkstackv1.EachSpec{Over: "x", Source: &orkstackv1.EachSource{TaskId: source, Path: []string{"xs"}}}
	return t
}

func fanRow(id string, status orkstackv1.Status, items ...*orkstackv1.TaskRun) *orkstackv1.TaskRun {
	r := prow(id, status)
	r.Items = items
	return r
}

func TestResumePlan_FanOutItemsKeepTheirOwnDisposition(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("src"), eachTask("fan", "src", "src"), ptask("after", "fan")},
		prow("src", sOK),
		fanRow("fan", sFail, pitem(0, sOK), pitem(1, sFail), pitem(2, sOK)),
		prow("after", sCanc))
	p := f.plan(t)
	if u := unitOf(t, p, "fan", 0); u.InRerunSet || u.Disposition != DispositionAdopt {
		t.Errorf("item 0 = %+v, want adopted", u)
	}
	if u := unitOf(t, p, "fan", 1); !u.InRerunSet || u.Category != CategoryRerunFailed {
		t.Errorf("item 1 = %+v", u)
	}
	if u := unitOf(t, p, "fan", -1); !u.InRerunSet {
		t.Errorf("node = %+v, want in S by the node rule", u)
	}
	if u := unitOf(t, p, "after", -1); !u.InRerunSet {
		t.Errorf("after = %+v", u)
	}
}

func TestResumePlan_EachSourceInSRerunsEveryItem(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("src"), eachTask("fan", "src", "src")},
		prow("src", sOK), fanRow("fan", sFail, pitem(0, sOK), pitem(1, sFail)))
	f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "src"
	p := f.plan(t)
	if u := unitOf(t, p, "fan", 0); !u.InRerunSet {
		t.Errorf("item 0 = %+v, want rerun: its source reruns", u)
	}
}

func awaitTask(id string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Needs: needs, Await: &orkstackv1.AwaitSpec{}}
}

func sleepTask(id string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Needs: needs, Sleep: &orkstackv1.SleepSpec{}}
}

func TestResumePlan_AwaitsAndSleeps(t *testing.T) {
	sl := prow("slept", sOK)
	sl.Sleep = &orkstackv1.TaskSleep{Seconds: 60}
	f := newPlan([]*orkstackv1.Task{
		awaitTask("approved"), awaitTask("open"), sleepTask("slept"), awaitTask("timed"), ptask("x"),
	}, prow("approved", sOK), prow("open", sWait), sl, prow("timed", sFail), prow("x", sFail))
	p := f.plan(t)
	cases := []struct {
		task string
		cat  ResumeCategory
		disp ResumeDisposition
	}{
		{"approved", CategorySettled, DispositionSettle},
		{"open", CategoryWaitsAgain, DispositionWait},
		{"slept", CategorySleepKept, DispositionSleepUntil},
		{"timed", CategoryRerunFailed, DispositionWait},
		{"x", CategoryRerunFailed, DispositionRerun},
	}
	for _, c := range cases {
		if u := unitOf(t, p, c.task, -1); u.Category != c.cat || u.Disposition != c.disp {
			t.Errorf("%s = %v/%v, want %v/%v", c.task, u.Category, u.Disposition, c.cat, c.disp)
		}
	}
}

func TestResumePlan_SettledAwaitWhoseValuesAreGoneWaitsAgain(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{awaitTask("ok"), ptask("after", "ok"), ptask("x")},
		prow("ok", sOK), prow("after", sOK), prow("x", sFail))
	f.in.Evidence.Kept[ResumeUnit{Task: "ok", Item: -1}] = KeptGone
	p := f.plan(t)
	if u := unitOf(t, p, "ok", -1); u.Disposition != DispositionWait || !u.InRerunSet {
		t.Errorf("ok = %+v", u)
	}
	if u := unitOf(t, p, "after", -1); !u.InRerunSet {
		t.Errorf("after = %+v, want rerun after the await waits again", u)
	}
}

func composedTask(id string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Needs: needs, Uses: "orkflow:child@1"}
}

func TestResumePlan_ComposedTasks(t *testing.T) {
	t.Run("settled when kept", func(t *testing.T) {
		f := newPlan([]*orkstackv1.Task{composedTask("c"), ptask("x")}, prow("c", sOK), prow("x", sFail))
		if u := unitOf(t, f.plan(t), "c", -1); u.Disposition != DispositionSettle {
			t.Errorf("c = %+v", u)
		}
	})
	cases := []struct {
		name  string
		rows  []*orkstackv1.TaskRun
		tasks []*orkstackv1.Task
		cause ComposedCause
	}{
		{"failed", []*orkstackv1.TaskRun{prow("c", sFail)}, []*orkstackv1.Task{composedTask("c")}, ComposedFailed},
		{"stopped", []*orkstackv1.TaskRun{prow("c", sCanc), prow("x", sFail)},
			[]*orkstackv1.Task{composedTask("c"), ptask("x")}, ComposedStopped},
		{"never started", []*orkstackv1.TaskRun{prow("c", sPend), prow("x", sFail)},
			[]*orkstackv1.Task{composedTask("c"), ptask("x")}, ComposedNeverStarted},
		{"downstream", []*orkstackv1.TaskRun{prow("x", sFail), prow("c", sOK)},
			[]*orkstackv1.Task{ptask("x"), composedTask("c", "x")}, ComposedDownstream},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newPlan(c.tasks, c.rows...)
			r := f.refusal(t)
			if r.Code != RefuseComposedTask || r.Cause != string(c.cause) || r.Tasks[0] != "c" {
				t.Errorf("refusal = %+v, want COMPOSED_TASK %s", r, c.cause)
			}
		})
	}
	t.Run("result gone", func(t *testing.T) {
		f := newPlan([]*orkstackv1.Task{composedTask("c"), ptask("x")}, prow("c", sOK), prow("x", sFail))
		f.in.Evidence.Kept[ResumeUnit{Task: "c", Item: -1}] = KeptGone
		if r := f.refusal(t); r.Code != RefuseComposedTask || r.Cause != string(ComposedResultGone) {
			t.Errorf("refusal = %+v", r)
		}
	})
}

func TestResumePlan_BaseRefusals(t *testing.T) {
	base := func() *planFixture {
		return newPlan([]*orkstackv1.Task{ptask("a")}, prow("a", sFail))
	}
	cases := []struct {
		name  string
		edit  func(*planFixture)
		code  ResumeRefusalCode
		class ResumeRefusalClass
	}{
		{"succeeded", func(f *planFixture) { f.in.Base.Status = sOK }, RefuseStateNotResumable, RefusalPermanent},
		{"canceled", func(f *planFixture) { f.in.Base.Status = sCanc }, RefuseStateNotResumable, RefusalPermanent},
		{"history limit", func(f *planFixture) { f.in.Base.Error.Code = RunHistoryLimitCode }, RefuseNotATaskFailure, RefusalPermanent},
		{"unknown code", func(f *planFixture) { f.in.Base.Error.Code = "SOMETHING_NEW" }, RefuseNotATaskFailure, RefusalPermanent},
		{"no error", func(f *planFixture) { f.in.Base.Error = nil }, RefuseFailureNotRecorded, RefusalPermanent},
		{"child run", func(f *planFixture) {
			f.in.Base.Origin = &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN}
		}, RefuseChildRun, RefusalPermanent},
		{"never started", func(f *planFixture) { f.in.Facts.ResetPoint = ResetPoint{} }, RefuseNeverStarted, RefusalPermanent},
		{"state unreadable", func(f *planFixture) { f.in.Facts.StateUnreadable = true }, RefuseStateUnreadable, RefusalPermanent},
		{"budget", func(f *planFixture) { f.in.ResumesUsed = MaxResumesPerRun }, RefuseTooManyResumes, RefusalPermanent},
		{"unknown from", func(f *planFixture) { f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "zz" }, RefuseFromTaskUnknown, RefusalPermanent},
		{"from on resume", func(f *planFixture) { f.in.FromTask = "a" }, RefuseFromTaskUnknown, RefusalPermanent},
		{"too early", func(f *planFixture) {
			f.in.Definition.Tasks = append(f.in.Definition.Tasks, ptask("e"))
			f.in.Base.TaskRuns = append(f.in.Base.TaskRuns, prow("e", sOK))
			f.in.Facts.Earlier = map[string]bool{"e": true}
			f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "e"
		}, RefuseRetryFromTooEarly, RefusalPermanent},
		{"failed earlier", func(f *planFixture) { f.in.Facts.Earlier = map[string]bool{"a": true} }, RefuseFailedInEarlierPart, RefusalPermanent},
		{"force-canceled needs from", func(f *planFixture) {
			f.in.Class = ResumeBaseForceCanceled
			f.in.Base.Status = sCanc
			f.in.Base.Error.Code = forceCanceledReason
		}, RefuseFromTaskRequired, RefusalReplan},
		{"force-canceled not canceled", func(f *planFixture) {
			f.in.Class = ResumeBaseForceCanceled
			f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "a"
		}, RefuseStateNotResumable, RefusalPermanent},
		{"plain cancel is not a force cancel", func(f *planFixture) {
			f.in.Class = ResumeBaseForceCanceled
			f.in.Base.Status = sCanc
			f.in.Base.Error.Code = "RUN_CANCELED"
			f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "a"
		}, RefuseStateNotResumable, RefusalPermanent},
		{"retry from without a task", func(f *planFixture) { f.in.Action = ResumeActionRetryFrom }, RefuseFromTaskRequired, RefusalReplan},
		{"force-canceled no point", func(f *planFixture) {
			f.in.Class = ResumeBaseForceCanceled
			f.in.Base.Status = sCanc
			f.in.Base.Error.Code = forceCanceledReason
			f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "a"
			f.in.Facts.Continued = true
			f.in.Facts.ResetPoint = ResetPoint{}
		}, RefuseNoResumePoint, RefusalPermanent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := base()
			c.edit(f)
			r := f.refusal(t)
			if r.Code != c.code || r.Class != c.class {
				t.Errorf("refusal = %s/%s, want %s/%s", r.Code, r.Class, c.code, c.class)
			}
			if r.Error() == "" {
				t.Error("empty message")
			}
		})
	}
}

func TestResumePlan_ResumableRunEndings(t *testing.T) {
	for _, code := range []string{"RUN_FAILED", RunTimedOutCode, RunPauseTimeoutCode, "RUN_QUEUE_TIMEOUT",
		"RUN_CONCURRENCY_LIMIT", "RUN_CONCURRENCY_FULL", "RUN_CONCURRENCY_UNAVAILABLE",
		"RUN_CONCURRENCY_FENCED", "RUN_CONCURRENCY_UNSUPPORTED"} {
		t.Run(code, func(t *testing.T) {
			f := newPlan([]*orkstackv1.Task{ptask("a")}, prow("a", sFail))
			f.in.Base.Error.Code = code
			f.plan(t)
		})
	}
}

func TestResumePlan_StoppedByRunTimeoutIsAFailedSeed(t *testing.T) {
	a := prow("a", sCanc)
	a.Error = &orkstackv1.Error{Code: "TASK_STOPPED_BY_RUN_TIMEOUT"}
	f := newPlan([]*orkstackv1.Task{ptask("a")}, a)
	f.in.Base.Error.Code = RunTimedOutCode
	if u := unitOf(t, f.plan(t), "a", -1); u.Category != CategoryRerunFailed {
		t.Errorf("a = %+v", u)
	}
}

func TestResumePlan_FailsBeforeDispatch(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("up"), ptask("a", "up")}, prow("up", sOK), prow("a", sFail))
	f.in.Facts.Dispatched = map[ResumeUnit]bool{}
	r := f.refusal(t)
	if r.Code != RefuseFailsBeforeDispatch || r.Tasks[0] != "a" {
		t.Fatalf("refusal = %+v", r)
	}
	f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "up"
	f.plan(t)
}

func TestResumePlan_EarlierRowsAreLeftAsCheckpointed(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("old"), ptask("a", "old")}, prow("old", sOK), prow("a", sFail))
	f.in.Facts.Earlier = map[string]bool{"old": true}
	f.in.Facts.Continued = true
	delete(f.in.Evidence.Kept, ResumeUnit{Task: "old", Item: -1})
	if u := unitOf(t, f.plan(t), "old", -1); u.Category != CategoryEarlier || u.Disposition != DispositionNone {
		t.Errorf("old = %+v", u)
	}
}

func TestResumePlan_MissingVerificationIsAnError(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("a"), ptask("x")}, prow("a", sOK), prow("x", sFail))
	delete(f.in.Evidence.Kept, ResumeUnit{Task: "a", Item: -1})
	_, err := PlanResume(f.in)
	var r *ResumeRefusal
	if err == nil || errors.As(err, &r) {
		t.Fatalf("PlanResume = %v, want an internal error", err)
	}
}

func TestResumePlan_UnitsPartitionTheRun(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("src"), eachTask("fan", "src", "src"), awaitTask("w"), ptask("x")},
		prow("src", sOK), fanRow("fan", sOK, pitem(0, sOK), pitem(1, sOK)), prow("w", sWait), prow("x", sFail))
	p := f.plan(t)
	seen := map[ResumeUnit]bool{}
	for _, u := range p.Units {
		if seen[u.Unit] {
			t.Fatalf("unit %v twice", u.Unit)
		}
		seen[u.Unit] = true
		if u.Category == "" {
			t.Errorf("unit %v has no category", u.Unit)
		}
	}
	if len(seen) != 6 {
		t.Errorf("%d units, want 6 (4 tasks + 2 items)", len(seen))
	}
}

func forceCanceled(f *planFixture, from string) {
	f.in.Class = ResumeBaseForceCanceled
	f.in.Base.Status = sCanc
	f.in.Base.Error = &orkstackv1.Error{Code: forceCanceledReason}
	f.in.Facts.OpenPins = map[string]bool{}
	f.in.Action, f.in.FromTask = ResumeActionRetryFrom, from
}

func TestResumePlan_ForceCanceledRows(t *testing.T) {
	a, b := ptask("a"), ptask("b")
	a.Pin, b.Pin = "p", "p"
	f := newPlan([]*orkstackv1.Task{
		ptask("late"), ptask("lost"), ptask("queued"), a, b, awaitTask("w"), ptask("tail", "late"),
	}, prow("late", sRun), prow("lost", sRun), prow("queued", sPend),
		prow("a", sOK), prow("b", sRun), prow("w", sWait), prow("tail", sPend))
	forceCanceled(f, "queued")
	f.in.Facts.Dispatched = map[ResumeUnit]bool{
		nodeUnit("late"): true, nodeUnit("lost"): true, nodeUnit("b"): true,
	}
	f.in.Facts.OpenPins = map[string]bool{"p": true}
	f.in.Evidence.Kept[nodeUnit("late")] = KeptValid
	f.in.Evidence.Kept[nodeUnit("lost")] = KeptGone
	f.in.Evidence.Kept[nodeUnit("b")] = KeptValid
	p := f.plan(t)
	if u := unitOf(t, p, "late", -1); u.InRerunSet || u.Disposition != DispositionAdopt || u.Detail != lateCommitDetail {
		t.Errorf("late = %+v, want adopted with the late-commit detail", u)
	}
	if u := unitOf(t, p, "lost", -1); !u.InRerunSet || u.Category != CategoryRerunUnfinished {
		t.Errorf("lost = %+v", u)
	}
	for _, m := range []string{"a", "b"} {
		if u := unitOf(t, p, m, -1); !u.InRerunSet {
			t.Errorf("open pin member %s = %+v, want rerun (G-d)", m, u)
		}
	}
	if u := unitOf(t, p, "w", -1); u.Disposition != DispositionWait {
		t.Errorf("w = %+v", u)
	}
	if p.AddsNothing {
		t.Error("AddsNothing is lifted for a force-cancelled base")
	}
}

func TestResumePlan_ForceCanceledLateCommitReachedByTheClosureReruns(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("up"), ptask("late", "up")}, prow("up", sOK), prow("late", sRun))
	forceCanceled(f, "up")
	f.in.Facts.Dispatched = map[ResumeUnit]bool{nodeUnit("late"): true}
	if u := unitOf(t, f.plan(t), "late", -1); !u.InRerunSet {
		t.Errorf("late = %+v, want rerun: it is downstream of the chosen task", u)
	}
}

func TestResumePlan_FollowsRule(t *testing.T) {
	recorded := func() *planFixture {
		f := newPlan([]*orkstackv1.Task{ptask("up"), awaitTask("ok", "up"), ptask("x")},
			prow("up", sOK), prow("ok", sOK), prow("x", sFail))
		f.in.Evidence.Recorded = true
		f.in.Evidence.ResultSHA256 = map[string]string{"up": "aa"}
		f.in.Evidence.Follows = map[string][]ResumeFollow{"ok": {{Task: "up", ResultSHA256: "aa"}}}
		return f
	}
	t.Run("holds", func(t *testing.T) {
		if u := unitOf(t, recorded().plan(t), "ok", -1); u.Disposition != DispositionSettle {
			t.Errorf("ok = %+v", u)
		}
	})
	t.Run("digest changed", func(t *testing.T) {
		f := recorded()
		f.in.Evidence.ResultSHA256["up"] = "bb"
		if u := unitOf(t, f.plan(t), "ok", -1); u.Disposition != DispositionWait || u.InRerunSet {
			t.Errorf("ok = %+v, want waits again outside S", u)
		}
	})
	t.Run("retry from upstream asks again", func(t *testing.T) {
		f := recorded()
		f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "up"
		if u := unitOf(t, f.plan(t), "ok", -1); u.Disposition != DispositionWait {
			t.Errorf("ok = %+v", u)
		}
	})
	t.Run("composed follows changed", func(t *testing.T) {
		f := newPlan([]*orkstackv1.Task{ptask("up"), composedTask("c", "up"), ptask("x")},
			prow("up", sOK), prow("c", sOK), prow("x", sFail))
		f.in.Evidence.Recorded = true
		f.in.Evidence.ResultSHA256 = map[string]string{"up": "aa"}
		f.in.Evidence.Follows = map[string][]ResumeFollow{"c": {{Task: "up", ResultSHA256: "zz"}}}
		if r := f.refusal(t); r.Code != RefuseComposedTask || r.Cause != string(ComposedFollowsChanged) {
			t.Errorf("refusal = %+v", r)
		}
	})
}

func TestResumePlan_ResolutionsOnRecord(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{awaitTask("yes"), awaitTask("no"), awaitTask("none"), ptask("x")},
		prow("yes", sWait), prow("no", sWait), prow("none", sWait), prow("x", sFail))
	f.in.Evidence.Kept[nodeUnit("yes")] = KeptValid
	f.in.Evidence.Resolutions = map[string]ResolutionAnswer{
		"yes": {Completed: true}, "no": {Rejected: true},
	}
	p := f.plan(t)
	if u := unitOf(t, p, "yes", -1); u.Disposition != DispositionSettle {
		t.Errorf("yes = %+v", u)
	}
	if u := unitOf(t, p, "no", -1); u.Category != CategoryRerunFailed || u.Disposition != DispositionWait {
		t.Errorf("no = %+v", u)
	}
	if u := unitOf(t, p, "none", -1); u.Category != CategoryWaitsAgain {
		t.Errorf("none = %+v", u)
	}
}

func TestResumePlan_InconsistentInputIsAnError(t *testing.T) {
	cases := map[string]func(*planFixture){
		"no dispatch set":        func(f *planFixture) { f.in.Facts.Dispatched = nil },
		"force with no open set": func(f *planFixture) { forceCanceled(f, "a"); f.in.Facts.OpenPins = nil },
		"missing row":            func(f *planFixture) { f.in.Base.TaskRuns = f.in.Base.TaskRuns[:1] },
		"unknown status":         func(f *planFixture) { f.in.Base.TaskRuns[0].Status = orkstackv1.Status_STATUS_UNSPECIFIED },
		"unknown answer":         func(f *planFixture) { f.in.Evidence.Kept[nodeUnit("a")] = KeptAnswer(99) },
		"unknown earlier task":   func(f *planFixture) { f.in.Facts.Earlier = map[string]bool{"zz": true} },
		"negative budget":        func(f *planFixture) { f.in.ResumesUsed = -1 },
		"recorded, no follows": func(f *planFixture) {
			f.in.Definition.Tasks[0] = awaitTask("a")
			f.in.Evidence.Recorded = true
		},
		"completed and rejected": func(f *planFixture) {
			f.in.Definition.Tasks[0] = awaitTask("a")
			f.in.Base.TaskRuns[0].Status = sWait
			f.in.Evidence.Resolutions = map[string]ResolutionAnswer{"a": {Completed: true, Rejected: true}}
		},
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			f := newPlan([]*orkstackv1.Task{ptask("a"), ptask("x")}, prow("a", sOK), prow("x", sFail))
			edit(f)
			_, err := PlanResume(f.in)
			var r *ResumeRefusal
			if err == nil || errors.As(err, &r) {
				t.Fatalf("PlanResume = %v, want an internal error", err)
			}
		})
	}
}

func TestResumePlan_SkippedFanOutNodeStaysSkipped(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("src"), eachTask("fan", "src", "src"), ptask("x")},
		prow("src", sOK), prow("fan", sSkip), prow("x", sFail))
	if u := unitOf(t, f.plan(t), "fan", -1); u.Disposition != DispositionKeepSkipped {
		t.Errorf("skipped node = %+v", u)
	}
}

func TestResumePlan_ForceCanceledCommittedItemIsAdopted(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("src"), eachTask("fan", "src", "src")},
		prow("src", sOK), fanRow("fan", sRun, pitem(0, sRun), pitem(1, sRun), pitem(2, sPend)))
	forceCanceled(f, "fan")
	f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "src"
	f.in.Facts.Dispatched[ResumeUnit{"fan", 2}] = false
	f.in.Evidence.Kept[ResumeUnit{"fan", 1}] = KeptGone
	p := f.plan(t)
	// Retrying from the each-source reruns every item; from the node itself
	// the committed item would be kept.
	if u := unitOf(t, p, "fan", 0); !u.InRerunSet {
		t.Errorf("fan[0] = %+v, want rerun: its source reruns", u)
	}
	f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "fan"
	f.in.Definition.Tasks = append(f.in.Definition.Tasks, ptask("other"))
	f.in.Base.TaskRuns = append(f.in.Base.TaskRuns, prow("other", sPend))
	f.in.FromTask = "other"
	p = f.plan(t)
	if u := unitOf(t, p, "fan", 0); u.InRerunSet || u.Detail != lateCommitDetail {
		t.Errorf("fan[0] = %+v, want adopted as a late commit", u)
	}
	for _, i := range []int{1, 2} {
		if u := unitOf(t, p, "fan", i); !u.InRerunSet || u.Category != CategoryRerunUnfinished {
			t.Errorf("fan[%d] = %+v, want rerun_unfinished", i, u)
		}
	}
}

func TestResumePlan_ForceCanceledFailedRowIsNotALateCommit(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("a"), ptask("b")}, prow("a", sFail), prow("b", sPend))
	forceCanceled(f, "b")
	if u := unitOf(t, f.plan(t), "a", -1); !u.InRerunSet || u.Category != CategoryRerunFailed {
		t.Errorf("a = %+v, want rerun_failed", u)
	}
}

func TestResumePlan_PermanentRefusalsComeBeforeWaits(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("a"), composedTask("c")}, prow("a", sOK), prow("c", sFail))
	f.in.Evidence.Kept[nodeUnit("a")] = KeptUnavailable
	if r := f.refusal(t); r.Code != RefuseComposedTask {
		t.Errorf("refusal = %s, want COMPOSED_TASK before the wait", r.Code)
	}
	f = newPlan([]*orkstackv1.Task{ptask("a"), ptask("b"), ptask("x")}, prow("a", sOK), prow("b", sOK), prow("x", sFail))
	f.in.Evidence.Kept[nodeUnit("a")] = KeptUnavailable
	f.in.Evidence.Kept[nodeUnit("b")] = KeptUnreadable
	if r := f.refusal(t); r.Code != RefuseStateUnreadable {
		t.Errorf("refusal = %s, want STATE_UNREADABLE before the wait", r.Code)
	}
}

func TestResumePlan_NothingToRerunIsRefused(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("a")}, prow("a", sOK))
	if r := f.refusal(t); r.Code != RefuseNotATaskFailure {
		t.Errorf("refusal = %s", r.Code)
	}
}

func TestResumePlan_ComposedChildOnRecordSettles(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{composedTask("c"), ptask("x")}, prow("c", sFail), prow("x", sFail))
	f.in.Evidence.Recorded = true
	f.in.Evidence.ChildOnRecord = map[string]bool{"c": true}
	f.in.Evidence.Follows = map[string][]ResumeFollow{"c": nil}
	if u := unitOf(t, f.plan(t), "c", -1); u.Disposition != DispositionSettle {
		t.Errorf("c = %+v", u)
	}
}

func TestResumePlan_RecordedFollowsMustCoverEveryNeed(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("up"), awaitTask("ok", "up"), ptask("x")},
		prow("up", sOK), prow("ok", sOK), prow("x", sFail))
	f.in.Evidence.Recorded = true
	f.in.Evidence.ResultSHA256 = map[string]string{"up": "aa"}
	f.in.Evidence.Follows = map[string][]ResumeFollow{"ok": {}}
	if u := unitOf(t, f.plan(t), "ok", -1); u.Disposition != DispositionWait {
		t.Errorf("ok = %+v, want waits again: its record follows nothing", u)
	}
}

func TestResumePlan_AnAnswerOfAUnitThatRerunsRefusesNothing(t *testing.T) {
	for _, a := range []KeptAnswer{KeptUnreadable, KeptUnavailable} {
		f := newPlan([]*orkstackv1.Task{ptask("a"), ptask("b", "a"), ptask("c", "b")},
			prow("a", sOK), prow("b", sFail), prow("c", sOK))
		f.in.Evidence.Kept[nodeUnit("c")] = a
		if u := unitOf(t, f.plan(t), "c", -1); !u.InRerunSet {
			t.Errorf("answer %d: c = %+v, want rerun", a, u)
		}
		f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "a"
		f.in.Evidence.Kept[nodeUnit("a")] = a
		f.plan(t)
	}
}

func TestResumePlan_ReadingBackFanOutNodeStaysKept(t *testing.T) {
	node := fanRow("fan", sCanc, pitem(0, sOK))
	node.ReadingBack = true
	node.Error = &orkstackv1.Error{Code: notReadBackReason}
	f := newPlan([]*orkstackv1.Task{ptask("src"), eachTask("fan", "src", "src"), ptask("x")},
		prow("src", sOK), node, prow("x", sFail))
	if u := unitOf(t, f.plan(t), "fan", -1); u.InRerunSet || u.Disposition != DispositionAdopt {
		t.Errorf("node = %+v, want kept", u)
	}
}

func TestResumePlan_RecordedRunNeedsEveryResultDigest(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("up"), awaitTask("ok", "up"), ptask("x")},
		prow("up", sOK), prow("ok", sOK), prow("x", sFail))
	f.in.Evidence.Recorded = true
	f.in.Evidence.Follows = map[string][]ResumeFollow{"ok": {{Task: "up", ResultSHA256: "aa"}}}
	_, err := PlanResume(f.in)
	var r *ResumeRefusal
	if err == nil || errors.As(err, &r) {
		t.Fatalf("PlanResume = %v, want an error: up has no digest", err)
	}
}
