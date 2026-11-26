package api

import (
	"fmt"
	"slices"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// resumeCategories maps the planner's categories to the wire's, in the
// sections' order, with the sentence each lists a task under.
var resumeCategories = []struct {
	plan engine.ResumeCategory
	wire orkstackv1.ResumeCategory
	why  string
}{
	{engine.CategoryRerunFailed, orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_FAILED, "it failed"},
	{engine.CategoryRerunSelected, orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_SELECTED, "you chose to retry from it"},
	{engine.CategoryRerunDownstream, orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_DOWNSTREAM, "it comes after a task that runs again"},
	{engine.CategoryRerunResultGone, orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_RESULT_GONE, "its saved result can't be used"},
	{engine.CategoryRerunUnfinished, orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_UNFINISHED, "it never finished"},
	{engine.CategoryRerunPinMember, orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_PIN_MEMBER, "another task of its pin runs again"},
	{engine.CategoryRerunEachSource, orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_EACH_SOURCE, "the task it fans out over runs again"},
	{engine.CategoryKept, orkstackv1.ResumeCategory_RESUME_CATEGORY_KEPT, "its saved result is kept"},
	{engine.CategorySettled, orkstackv1.ResumeCategory_RESUME_CATEGORY_SETTLED, "its result is on record"},
	{engine.CategorySkipped, orkstackv1.ResumeCategory_RESUME_CATEGORY_SKIPPED, "it stays skipped"},
	{engine.CategoryWaitsAgain, orkstackv1.ResumeCategory_RESUME_CATEGORY_WAITS_AGAIN, "it waits again, from the resume"},
	{engine.CategorySleepKept, orkstackv1.ResumeCategory_RESUME_CATEGORY_SLEEP_KEPT, "it wakes when it would have"},
	{engine.CategoryEarlier, orkstackv1.ResumeCategory_RESUME_CATEGORY_EARLIER, "it finished in an earlier part of the run, before it carried on: its result is kept, and it does not run again"},
}

// resumePreview is a planned resume as a preview answers it.
func resumePreview(base *resumeBase, p *resumePlanned, warnings []*orkstackv1.ResumeWarning) *orkstackv1.ResumeRunResponse {
	out := &orkstackv1.ResumeRunResponse{
		RunId:            base.run.GetId(),
		PlanDigest:       p.digest,
		SeqBase:          p.seqBase,
		Action:           p.block.GetAction(),
		ReadbackCalls:    int32(p.preflight.ReadbackCalls),
		ResumesUsed:      int32(p.input.ResumesUsed),
		TerminalSequence: base.sequence,
		HighestSequence:  p.highest,
		Warnings:         append(slices.Clone(warnings), p.warnings...),

		EstimatedReadbackSeconds: int64(p.preflight.EstimatedReadback.Seconds()),
	}
	forced := base.class == engine.ResumeBaseForceCanceled
	if forced {
		// G-f: it replays on the build it stalled on.
		build := "the build it ran on"
		if b := base.execution.BuildID; b != "" {
			build = "build " + b
		}
		out.Warnings = append(out.Warnings, &orkstackv1.ResumeWarning{
			Kind: orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_FORCE_CANCELED_SAME_BUILD, BuildId: base.execution.BuildID,
			Message: fmt.Sprintf("run %s was force-cancelled on %s; resuming replays on it and may stall the same way", base.run.GetId(), build),
		})
	}
	if base.run.GetPause() != nil || base.run.GetPausedByParent() != nil {
		message := fmt.Sprintf("run %s was paused when it ended; it resumes unpaused", base.run.GetId())
		if forced {
			message = fmt.Sprintf("run %s was paused when it was force-cancelled; it resumes unpaused, and you can pause it again", base.run.GetId())
		}
		out.Warnings = append(out.Warnings, &orkstackv1.ResumeWarning{
			Kind: orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_WAS_PAUSED, Message: message,
		})
	}
	if secs := base.run.GetTimeout().GetSeconds(); secs > 0 {
		out.Warnings = append(out.Warnings, &orkstackv1.ResumeWarning{
			Kind:    orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_FRESH_TIMEOUT,
			Message: fmt.Sprintf("the run gets a fresh timeout of %s when its tasks start again", time.Duration(secs)*time.Second),
		})
	}
	defs := map[string]*orkstackv1.Task{}
	for _, t := range base.definition.GetTasks() {
		defs[t.GetId()] = t
	}
	rows := map[string]*orkstackv1.TaskRun{}
	for _, row := range base.run.GetTaskRuns() {
		rows[row.GetTaskId()] = row
	}
	counts := map[orkstackv1.ResumeCategory]int32{}
	for _, u := range p.plan.Units {
		for _, c := range resumeCategories {
			if c.plan != u.Category {
				continue
			}
			counts[c.wire]++
			t := &orkstackv1.ResumeTask{TaskId: u.Unit.Task, Category: c.wire, Why: c.why, Detail: u.Detail}
			row := rows[u.Unit.Task]
			if u.Unit.Item >= 0 {
				item := int32(u.Unit.Item)
				t.Item = &item
				row = itemRow(row, item)
			}
			t.PreviousOutcome = row.GetStatus()
			if u.Succeeded {
				t.PreviousOutcome = orkstackv1.Status_STATUS_SUCCEEDED
			}
			def := defs[u.Unit.Task]
			if u.InRerunSet && def.GetCache() != nil {
				t.Memo = orkstackv1.ResumeMemoState_RESUME_MEMO_STATE_LOOKED_UP
				if u.Fresh {
					t.Memo = orkstackv1.ResumeMemoState_RESUME_MEMO_STATE_BYPASSED
				}
			}
			t.CallbackWorks = u.Disposition == engine.DispositionWait && def.GetAwait().GetCallback()
			out.Tasks = append(out.Tasks, t)
		}
	}
	for _, c := range resumeCategories {
		if n := counts[c.wire]; n > 0 {
			out.Counts = append(out.Counts, &orkstackv1.ResumeCount{Category: c.wire, Count: n})
		}
	}
	out.KeptCount = counts[orkstackv1.ResumeCategory_RESUME_CATEGORY_KEPT] + counts[orkstackv1.ResumeCategory_RESUME_CATEGORY_SETTLED]
	for c, n := range counts {
		if rerunCategory(c) {
			out.RerunCount += n
		}
	}
	return out
}

// rerunCategory reports whether a unit listed under c runs again.
func rerunCategory(c orkstackv1.ResumeCategory) bool {
	switch c {
	case orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_FAILED, orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_SELECTED,
		orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_DOWNSTREAM, orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_RESULT_GONE,
		orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_UNFINISHED, orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_PIN_MEMBER,
		orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_EACH_SOURCE:
		return true
	}
	return false
}

// resumeWarningKinds names the kinds among warnings, each once.
func resumeWarningKinds(warnings []*orkstackv1.ResumeWarning) string {
	var kinds []string
	for _, w := range warnings {
		kind := strings.ToLower(strings.TrimPrefix(w.GetKind().String(), "RESUME_WARNING_KIND_"))
		if !slices.Contains(kinds, kind) {
			kinds = append(kinds, kind)
		}
	}
	return strings.Join(kinds, ",")
}

// itemRow is one item's row of a fan-out's.
func itemRow(row *orkstackv1.TaskRun, item int32) *orkstackv1.TaskRun {
	for _, it := range row.GetItems() {
		if it.GetItem() == item {
			return it
		}
	}
	return nil
}
