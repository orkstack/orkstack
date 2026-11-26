package main

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Exit codes of `orkctl run resume` beside the shared ones (docs/CLI.md).
const (
	// exitResumeUnconfirmed: the resume was sent and its outcome is not known
	// yet (RESUME_UNCONFIRMED); the run may carry on by itself. It shares 4
	// with other commands' own verdicts, which this command never returns.
	exitResumeUnconfirmed = 4
	// exitResumeEndedEarly: with --wait, the run ended again before any of
	// its tasks ran again.
	exitResumeEndedEarly = 5
	// exitResumeWait: a refusal of the wait class; try again later.
	exitResumeWait = 75
	// exitResumeReplan: a refusal of the replan class; look again and decide.
	exitResumeReplan = 76
)

// maxResumeReason is the longest reason a resume records (E85 §3.1).
const maxResumeReason = 1024

// resumeListLimit caps the units the preview names per section; the rest are
// counted.
const resumeListLimit = 20

// resumeUnconfirmedReason is the refusal that means "sent, outcome not known
// yet".
const resumeUnconfirmedReason = "RESUME_UNCONFIRMED"

func newRunResumeCommand(g *globalFlags) *cobra.Command {
	var o resumeOptions
	cmd := &cobra.Command{
		Use:   "resume <run-id>",
		Short: "Resume a failed run from its failed tasks, or retry it from a task",
		Long: `No engine resumes runs yet: it answers "this engine resumes no runs".

Resume a failed run: its failed tasks and everything downstream of them run
again, and every other task keeps its result. With --from, the chosen task and
everything downstream of it run again too (retry from here). The run keeps its
id, and its events continue after RUN_RESUMED.

The command first previews the plan. When work that succeeded before would run
again, it asks before going on (--yes answers for a script); a plan that
changed since the preview is never accepted.

Exit codes: 0 resumed (with --wait: succeeded); 1 an error, or a refusal no
resume can fix; 2/3 with --wait: the run failed / was canceled; 4 the resume
was sent and its outcome is not known yet; 5 with --wait: the run ended before
anything ran again; 75 refused for now, try again later; 76 refused, look
again and decide. See docs/RESUME.md.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("accept-sequence-gap") && o.acceptGap < 1 {
				return errors.New("--accept-sequence-gap takes the highest event the refusal named")
			}
			if len(o.reason) > maxResumeReason {
				return fmt.Errorf("--reason is %d bytes; at most %d", len(o.reason), maxResumeReason)
			}
			o.runID = args[0]
			return runResume(cmd, g, o)
		},
	}
	cmd.Flags().StringVar(&o.from, "from", "", "retry from this task: it and everything downstream of it run again")
	cmd.Flags().StringVar(&o.reason, "reason", "", "why the run is resumed, recorded with it (at most 1 KiB)")
	cmd.Flags().BoolVar(&o.dryRun, "dry-run", false, "show what would run again and what is kept, and change nothing")
	addYesFlag(cmd, &o.yes)
	cmd.Flags().BoolVar(&o.wait, "wait", false, "after resuming, watch the run until it ends")
	cmd.Flags().BoolVar(&o.acknowledgeUnknown, "acknowledge-unknown-attempts", false,
		"go on although earlier attempts on Orkers that are gone may still be running (--yes does not imply it)")
	cmd.Flags().Int64Var(&o.acceptGap, "accept-sequence-gap", 0,
		"go on above this event, the highest one the refusal named, although some of the run's events were never stored (needs run.resume.accept_gap)")
	return cmd
}

type resumeOptions struct {
	runID, from, reason string
	dryRun, yes, wait   bool
	acknowledgeUnknown  bool
	// acceptGap is the highest stored event the caller resumes above; 0
	// accepts no gap.
	acceptGap int64
}

func runResume(cmd *cobra.Command, g *globalFlags, o resumeOptions) error {
	client := g.runClient()
	// The preview carries the same acceptance as the confirm: a gap the caller
	// accepts must not refuse the plan they are about to be shown. Each call gets its own deadline: none spans the
	// question to the person between them.
	ctx, cancel := g.unaryContext(cmd)
	preview, err := client.ResumeRun(ctx, connect.NewRequest(&orkstackv1.ResumeRunRequest{
		Namespace: g.namespace, RunId: o.runID, Reason: o.reason, FromTaskId: o.from, DryRun: true,
		AcceptSequenceGap: o.acceptGap > 0, AcceptedHighestSeq: o.acceptGap,
	}))
	cancel()
	if err != nil {
		return resumeError("preview the resume", err)
	}
	plan := preview.Msg
	if o.dryRun {
		if g.json {
			return writeJSON(cmd.OutOrStdout(), plan)
		}
		renderResumePlan(cmd.OutOrStdout(), o.runID, plan)
		return nil
	}
	if !g.json {
		renderResumePlan(cmd.ErrOrStderr(), o.runID, plan)
	}
	if plan.GetPlanDigest() == "" || plan.GetTerminalSequence() < 1 {
		return fmt.Errorf("run %s: the engine answered the preview with no plan to confirm; nothing was resumed", o.runID)
	}
	unknown, unkeyed := unknownAttemptKeys(plan)
	if unkeyed > 0 {
		return fmt.Errorf("run %s: the engine named %d earlier attempt(s) of unknown outcome with no way to acknowledge them; nothing was resumed",
			o.runID, unkeyed)
	}
	if len(unknown) > 0 && !o.acknowledgeUnknown {
		return &exitCodeError{code: exitResumeReplan, err: fmt.Errorf(
			"run %s: %d earlier attempt(s) may still be running on Orkers that are gone; check them, then pass --acknowledge-unknown-attempts",
			o.runID, len(unknown))}
	}
	if again := rerunSucceeded(plan); again > 0 {
		if g.json && !o.yes {
			return fmt.Errorf("run %s: %d task(s) that succeeded before would run again; pass --yes (--json never asks)", o.runID, again)
		}
		prompt := fmt.Sprintf("Resuming run %s runs %d task(s) again that succeeded before; their external side effects repeat.",
			o.runID, again)
		if err := confirmDestructive(cmd, g, fmt.Sprintf("resume run %q", o.runID), prompt, o.yes); err != nil {
			return err
		}
	}
	// The confirm carries what the preview showed: a run or a plan that
	// changed since is refused, so --yes never accepts a plan nobody saw.
	request := &orkstackv1.ResumeRunRequest{
		Namespace: g.namespace, RunId: o.runID, Reason: o.reason, FromTaskId: o.from,
		ExpectedTerminalSequence: plan.GetTerminalSequence(), ExpectedPlanDigest: plan.GetPlanDigest(),
		AcceptSequenceGap: o.acceptGap > 0, AcceptedHighestSeq: o.acceptGap,
	}
	if o.acknowledgeUnknown {
		request.AcknowledgedUnknownAttempts = unknown
	}
	ctx, cancel = g.unaryContext(cmd)
	confirmed, err := client.ResumeRun(ctx, connect.NewRequest(request))
	cancel()
	if err != nil {
		return confirmError(err)
	}
	done := confirmed.Msg
	if !o.wait {
		if g.json {
			return writeJSON(cmd.OutOrStdout(), done)
		}
		if done.GetAlreadyResumed() {
			fmt.Fprintf(cmd.OutOrStdout(), "Run %s was already resumed by this plan.\n", done.GetRunId())
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Resumed run %s (RUN_RESUMED at event %d).\n", done.GetRunId(), done.GetSeqBase())
		}
		return nil
	}
	runID := done.GetRunId()
	if runID == "" {
		runID = o.runID
	}
	run, err := watchRunFinal(cmd, g, runID, max(done.GetSeqBase()-1, 0))
	if err != nil {
		return err
	}
	if endedBeforeAnythingReran(run, done.GetSeqBase()) {
		return &exitCodeError{code: exitResumeEndedEarly,
			err: fmt.Errorf("run %s ended %s before any task ran again", run.GetId(), statusLabel(run.GetStatus()))}
	}
	return runExitError(run)
}

// confirmError is resumeError for the confirm, which may have reached the
// engine although its answer did not reach us: a deadline, a cancel or a lost
// connection there is "sent, outcome not known", exit 4, never "nothing done".
func confirmError(err error) error {
	if resumeRefusalOf(err) == nil {
		switch connect.CodeOf(err) {
		case connect.CodeDeadlineExceeded, connect.CodeCanceled, connect.CodeUnavailable, connect.CodeUnknown, connect.CodeInternal:
			return &exitCodeError{code: exitResumeUnconfirmed, err: fmt.Errorf(
				"resume: the confirm may have been sent, and its outcome is not known (%s); check the run before trying again",
				connect.CodeOf(err))}
		}
	}
	return resumeError("resume", err)
}

// resumeError turns a ResumeRun error into the command's: a refusal exits
// by its class, a resume whose outcome is unknown exits 4, and anything else
// is an error.
func resumeError(operation string, err error) error {
	refusal := resumeRefusalOf(err)
	if refusal == nil {
		if connect.CodeOf(err) == connect.CodeUnimplemented {
			return fmt.Errorf("%s: this engine cannot resume runs", operation)
		}
		return rpcError(operation, err)
	}
	text := refusal.GetMessage()
	if text == "" {
		text = refusal.GetReason()
	}
	msg := fmt.Sprintf("%s: %s (%s)", operation, text, refusal.GetReason())
	if refusal.GetReason() == resumeUnconfirmedReason {
		msg = fmt.Sprintf("%s: %s: %s (%s)", operation, unconfirmedLead(refusal.GetCause()), text, refusal.GetReason())
	}
	if refusal.GetHighestSequence() > 0 {
		msg += fmt.Sprintf("; --accept-sequence-gap %d accepts it", refusal.GetHighestSequence())
	}
	if refusal.GetRetryAfterSeconds() > 0 {
		msg += fmt.Sprintf("; try again in %d s", refusal.GetRetryAfterSeconds())
	}
	if d := refusal.GetDiff(); len(d.GetAdded())+len(d.GetDropped()) > 0 {
		msg += "; " + describeDiff(d)
	}
	code := exitError
	switch {
	case refusal.GetReason() == resumeUnconfirmedReason:
		code = exitResumeUnconfirmed
	case refusal.GetClass() == orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT:
		code = exitResumeWait
	case refusal.GetClass() == orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_REPLAN:
		code = exitResumeReplan
	}
	return &exitCodeError{code: code, err: errors.New(msg)}
}

// unconfirmedLead tells a person which of RESUME_UNCONFIRMED's two causes
// stands: a reset that may or may not have been made, or one that was made
// and whose run has not carried on yet.
func unconfirmedLead(cause string) string {
	switch cause {
	case "reset_outcome_unknown":
		return "the resume was sent and it is not known yet whether it was made"
	case "not_carried_on":
		return "the run was resumed and has not carried on yet"
	}
	return "the resume's outcome is not known yet"
}

// resumeRefusalOf is the ResumeRefusal a ResumeRun error carries, or nil.
func resumeRefusalOf(err error) *orkstackv1.ResumeRefusal {
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return nil
	}
	for _, detail := range connectErr.Details() {
		msg, derr := detail.Value()
		if derr != nil {
			continue
		}
		if refusal, ok := msg.(*orkstackv1.ResumeRefusal); ok {
			return refusal
		}
	}
	return nil
}

func describeDiff(d *orkstackv1.ResumeDiff) string {
	var parts []string
	if len(d.GetAdded()) > 0 {
		parts = append(parts, "now also "+unitRefList(d.GetAdded()))
	}
	if len(d.GetDropped()) > 0 {
		parts = append(parts, "no longer "+unitRefList(d.GetDropped()))
	}
	return strings.Join(parts, "; ")
}

func unitRefList(refs []*orkstackv1.ResumeUnitRef) string {
	names := make([]string, 0, len(refs))
	for _, r := range refs {
		names = append(names, unitName(r.GetTaskId(), r.Item))
	}
	return limitedList(names)
}

// unitName renders a task, or one item of a fan-out, for a person. The
// rendering is never parsed.
func unitName(task string, item *int32) string {
	if item == nil {
		return task
	}
	return fmt.Sprintf("%s[%d]", task, *item)
}

func limitedList(names []string) string {
	if len(names) <= resumeListLimit {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:resumeListLimit], ", "), len(names)-resumeListLimit)
}

// resumeSection is a section of the preview, in the order it is shown (E85
// §3.4).
type resumeSection struct {
	title      string
	categories []orkstackv1.ResumeCategory
}

var resumeSections = []resumeSection{
	{"You asked for these", []orkstackv1.ResumeCategory{
		orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_FAILED,
		orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_SELECTED,
		orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_DOWNSTREAM,
	}},
	{"Added because the resume must", []orkstackv1.ResumeCategory{
		orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_RESULT_GONE,
		orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_UNFINISHED,
		orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_PIN_MEMBER,
		orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_EACH_SOURCE,
	}},
	{"Waits again", []orkstackv1.ResumeCategory{orkstackv1.ResumeCategory_RESUME_CATEGORY_WAITS_AGAIN}},
	{"Answered and on record", []orkstackv1.ResumeCategory{
		orkstackv1.ResumeCategory_RESUME_CATEGORY_RESOLVED_ON_RECORD,
		orkstackv1.ResumeCategory_RESUME_CATEGORY_CHILD_OUTPUTS_ON_RECORD,
	}},
	{"Kept", []orkstackv1.ResumeCategory{orkstackv1.ResumeCategory_RESUME_CATEGORY_KEPT}},
	{"Already done", []orkstackv1.ResumeCategory{
		orkstackv1.ResumeCategory_RESUME_CATEGORY_SETTLED,
		orkstackv1.ResumeCategory_RESUME_CATEGORY_SLEEP_KEPT,
	}},
	{"Stays skipped", []orkstackv1.ResumeCategory{orkstackv1.ResumeCategory_RESUME_CATEGORY_SKIPPED}},
	{"From an earlier part of the run", []orkstackv1.ResumeCategory{orkstackv1.ResumeCategory_RESUME_CATEGORY_EARLIER}},
}

// otherSection lists a unit whose category no section claims (one a newer
// engine sends), so a confirm never follows a preview that left a unit out.
const otherSection = "Other"

// sectionOf is the title of the section a category is listed under.
func sectionOf(c orkstackv1.ResumeCategory) string {
	for _, s := range resumeSections {
		if slices.Contains(s.categories, c) {
			return s.title
		}
	}
	return otherSection
}

// isRerunCategory reports whether a category runs the task again.
func isRerunCategory(c orkstackv1.ResumeCategory) bool {
	return slices.Contains(resumeSections[0].categories, c) || slices.Contains(resumeSections[1].categories, c)
}

// rerunSucceeded counts the tasks that would run again although they
// succeeded before: the work a person must agree to repeat (E85 C5). A unit
// of a category this release does not know is counted as rerunning.
func rerunSucceeded(plan *orkstackv1.ResumeRunResponse) int {
	n := 0
	for _, t := range plan.GetTasks() {
		if (isRerunCategory(t.GetCategory()) || sectionOf(t.GetCategory()) == otherSection) &&
			t.GetPreviousOutcome() == orkstackv1.Status_STATUS_SUCCEEDED {
			n++
		}
	}
	return n
}

// unknownAttemptKeys are the acknowledgement keys of the attempts whose
// outcome is unknown, as the preview showed them, and how many such
// warnings carried no key.
func unknownAttemptKeys(plan *orkstackv1.ResumeRunResponse) (keys []string, unkeyed int) {
	for _, w := range plan.GetWarnings() {
		if w.GetKind() != orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_ATTEMPT_OUTCOME_UNKNOWN {
			continue
		}
		if w.GetAcknowledgeKey() == "" {
			unkeyed++
			continue
		}
		keys = append(keys, w.GetAcknowledgeKey())
	}
	return keys, unkeyed
}

func renderResumePlan(w io.Writer, runID string, plan *orkstackv1.ResumeRunResponse) {
	action := "Resume"
	if plan.GetAction() == orkstackv1.ResumeAction_RESUME_ACTION_RETRY_FROM {
		action = "Retry from here"
	}
	fmt.Fprintf(w, "%s: run %s\n", action, runID)
	bySection, details := map[string][]string{}, map[string][]string{}
	for _, t := range plan.GetTasks() {
		title, name := sectionOf(t.GetCategory()), unitName(t.GetTaskId(), t.Item)
		bySection[title] = append(bySection[title], name)
		if d := t.GetDetail(); d != "" {
			details[title] = append(details[title], name+": "+d)
		}
	}
	titles := make([]string, 0, len(resumeSections)+1)
	for _, s := range resumeSections {
		titles = append(titles, s.title)
	}
	for _, title := range append(titles, otherSection) {
		if names := bySection[title]; len(names) > 0 {
			fmt.Fprintf(w, "  %s (%d): %s\n", title, len(names), limitedList(names))
			said := details[title]
			for _, d := range said[:min(len(said), resumeListLimit)] {
				fmt.Fprintf(w, "    %s\n", d)
			}
			if more := len(said) - resumeListLimit; more > 0 {
				fmt.Fprintf(w, "    and %d more\n", more)
			}
		}
	}
	for _, warning := range plan.GetWarnings() {
		fmt.Fprintf(w, "  Note: %s\n", warning.GetMessage())
	}
	if e := plan.GetConcurrencyEffect(); e != "" {
		fmt.Fprintf(w, "  %s\n", e)
	}
	fmt.Fprintf(w, "  resumes used: %d/20\n", plan.GetResumesUsed())
}

// endedBeforeAnythingReran reports whether the resume made at seqBase ended
// the run again before any task ran again.
func endedBeforeAnythingReran(run *orkstackv1.Run, seqBase int64) bool {
	for _, r := range run.GetResumes() {
		if r.GetSequence() == seqBase {
			return r.GetEnded().GetBeforeAnythingReran()
		}
	}
	return false
}
