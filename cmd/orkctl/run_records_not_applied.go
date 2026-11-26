package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/runstatus"
)

// defaultNotAppliedMaxRuns is how many runs `run records --not-applied` reads
// unless told otherwise, and maxNotAppliedRuns the most it may be told: each
// run costs the engine several reads of the store. A pass lists at most
// notAppliedScanFactor times as many marked runs while it looks for them.
const (
	defaultNotAppliedMaxRuns = 200
	maxNotAppliedRuns        = 1000
	notAppliedScanFactor     = 10
)

// notAppliedOptions is what `run records --not-applied` was asked.
type notAppliedOptions struct {
	namespace    string
	includeEnded bool
	since        time.Time
	redeliver    bool
	maxRuns      int
	now          time.Time
}

// notAppliedDoor is what the loop asks of the engine, one call each. The
// command's own is the Connect client; the tests' is a fake door.
type notAppliedDoor interface {
	// listRuns is one page of the namespace's runs whose results the engine
	// records (results_recorded=true), open ones only unless ended.
	listRuns(ctx context.Context, ended bool, token string) (runs []*orkstackv1.Run, next string, err error)
	// listUnapplied is every row of one run's unapplied awaits, and whether
	// every page was complete.
	listUnapplied(ctx context.Context, runID string) (rows []*orkstackv1.RecordedResult, complete bool, err error)
	// redeliver tells a run again of the answer on record for task.
	redeliver(ctx context.Context, runID, taskID string) (*orkstackv1.ResolveAwaitResponse, error)
	// getRun reads one run, for a child's parent.
	getRun(ctx context.Context, runID string) (*orkstackv1.Run, error)
}

// errNotAppliedIncomplete ends a pass that did not read everything, or told a
// run something it did not hear.
var errNotAppliedIncomplete = errors.New("run records --not-applied did not finish cleanly: see the lines above, and run it again")

// notAppliedRow is one line of the pass: a record not applied and what to do.
type notAppliedRow struct {
	run    *orkstackv1.Run
	result *orkstackv1.RecordedResult
	action string
	// tell is set for a row --redeliver would tell: the run is open and its
	// task still awaits before its deadline.
	tell bool
	// told is the answer of a telling, when one was made.
	told string
}

// notApplied finds, across a namespace, every answer on record that a run did
// not take (docs/TASK_IDENTITY.md, "After an outage"), and with redeliver
// tells each open run whose await is still before its deadline. It is a loop
// in the client: no engine keeps any state of it.
func notApplied(ctx context.Context, door notAppliedDoor, o notAppliedOptions, out io.Writer) error {
	scope := "open"
	if o.includeEnded {
		scope = "open, and ended since " + o.since.UTC().Format(time.RFC3339)
	}
	if _, err := fmt.Fprintf(out, "Reading at most %d runs of namespace %s whose results the engine records (%s).\n"+
		"Only runs started with the mark the cluster indexes are read, and only while the cluster holds them: not one started before the\n"+
		"cluster indexed OrkflowResultsRecorded, nor one already retired to the archive.\n",
		o.maxRuns, orDash(o.namespace), scope); err != nil {
		return err
	}
	runs, short, err := notAppliedRuns(ctx, door, o)
	if err != nil {
		return rpcError("list runs", err)
	}
	clean := short == ""
	var rows []notAppliedRow
	for _, run := range runs {
		results, complete, err := door.listUnapplied(ctx, run.GetId())
		if err != nil {
			clean = false
			if _, werr := fmt.Fprintln(out, terminalSafe(fmt.Sprintf("%s: %v", run.GetId(), rpcError("run records", err)))); werr != nil {
				return werr
			}
			continue
		}
		if !complete {
			clean = false
			if _, err := fmt.Fprintf(out, "%s: some of its records could not be read\n", run.GetId()); err != nil {
				return err
			}
		}
		for _, r := range results {
			if r.GetState() == orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_UNREAD {
				clean = false
			}
			rows = append(rows, notAppliedRowOf(run, r, o.now))
		}
	}
	if o.redeliver {
		for i := range rows {
			if !rows[i].tell {
				continue
			}
			told, ok := tellAgain(ctx, door, rows[i])
			rows[i].told = told
			if !ok {
				clean = false
			}
		}
	}
	if err := renderNotApplied(out, o, rows); err != nil {
		return err
	}
	if short != "" {
		if _, err := fmt.Fprintln(out, short); err != nil {
			return err
		}
	}
	if o.redeliver {
		if _, err := fmt.Fprintln(out, "Told is not taken: run this again after a minute to see what the runs took."); err != nil {
			return err
		}
	}
	if !clean {
		return errNotAppliedIncomplete
	}
	return nil
}

// notAppliedRuns is the runs the pass reads, at most o.maxRuns, and why it
// read short of the whole listing, or "" when it did not. An ended run with no
// end time is read when ended runs are, rather than left out unsaid.
func notAppliedRuns(ctx context.Context, door notAppliedDoor, o notAppliedOptions) ([]*orkstackv1.Run, string, error) {
	var out []*orkstackv1.Run
	token, listed := "", 0
	for {
		page, next, err := door.listRuns(ctx, o.includeEnded, token)
		if err != nil {
			return nil, "", err
		}
		for _, run := range page {
			if listed++; listed > notAppliedScanFactor*o.maxRuns {
				return out, fmt.Sprintf("Stopped after listing %d marked runs: more remain unread; give a later --since.", listed-1), nil
			}
			ended := runstatus.IsTerminal(run.GetStatus())
			if ended && (!o.includeEnded || (run.GetFinishedAt() != nil && run.GetFinishedAt().AsTime().Before(o.since))) {
				continue
			}
			if len(out) == o.maxRuns {
				return out, fmt.Sprintf("Stopped at --max-runs %d: more runs remain unread.", o.maxRuns), nil
			}
			out = append(out, run)
		}
		switch next {
		case "":
			return out, "", nil
		case token:
			return out, "The engine's listing of runs did not page on: more runs may remain unread.", nil
		}
		token = next
	}
}

// notAppliedRowOf says what to do with one record a run did not take.
func notAppliedRowOf(run *orkstackv1.Run, r *orkstackv1.RecordedResult, now time.Time) notAppliedRow {
	row := notAppliedRow{run: run, result: r}
	deadline := r.GetDeadline()
	switch {
	case r.GetState() == orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_UNREAD:
		row.action = "could not be read; run this again"
	case r.GetState() == orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_TAKEN:
		row.action = "came after the task stopped waiting; not used"
	case deadline != nil && r.GetStoredAt() != nil && r.GetStoredAt().AsTime().After(deadline.AsTime()):
		row.action = "stored after its deadline; not used"
	case runstatus.IsTerminal(run.GetStatus()) && r.GetInChildRun():
		row.action = "run the parent again; this answer is not carried over"
	case runstatus.IsTerminal(run.GetStatus()):
		row.action = "resume the run"
	case r.GetTaskStatus() != orkstackv1.Status_STATUS_AWAITING && r.GetInChildRun():
		row.action = "the task stopped waiting; run the parent again once it ends, this answer is not carried over"
	case r.GetTaskStatus() != orkstackv1.Status_STATUS_AWAITING:
		row.action = "the task stopped waiting; resume the run once it ends"
	case deadline != nil && !now.Before(deadline.AsTime()):
		row.action = "past its deadline; the run decides"
	default:
		row.action, row.tell = "tell the run again", true
	}
	return row
}

// tellAgain tells a run again of one answer on record, and reports the
// answer's words and whether the run was told. A child whose parent has ended
// is not told: nothing will take it.
func tellAgain(ctx context.Context, door notAppliedDoor, row notAppliedRow) (string, bool) {
	if origin := row.run.GetOrigin(); row.result.GetInChildRun() && origin.GetKind() == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN && origin.GetId() != "" {
		parent, err := door.getRun(ctx, origin.GetId())
		switch {
		case err != nil:
			return "skipped: its parent could not be read: " + refusalWords(err), false
		case runstatus.IsTerminal(parent.GetStatus()):
			return "skipped: its parent has ended, so nothing will take it", true
		}
	}
	resp, err := door.redeliver(ctx, row.run.GetId(), row.result.GetTaskId())
	switch {
	case err != nil:
		return "refused: " + refusalWords(err), false
	case resp.GetDeadlinePassed():
		return "past its deadline; the run decides", true
	case resp.GetDeliveryPending():
		return "pending", false
	case resp.GetToldAt() != nil:
		return "told at " + formatTime(resp.GetToldAt()), true
	}
	return "pending", false
}

// reasonPrefix is a reason code at the head of an engine's refusal
// ("AWAIT_DEADLINE_PASSED: ...").
var reasonPrefix = regexp.MustCompile(`^([A-Z][A-Z0-9_]+):`)

// refusalWords is a refusal's reason code when the engine named one, else its
// code, then its message.
func refusalWords(err error) string {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return err.Error()
	}
	if m := reasonPrefix.FindStringSubmatch(ce.Message()); m != nil {
		return m[1]
	}
	return ce.Code().String() + ": " + ce.Message()
}

func renderNotApplied(out io.Writer, o notAppliedOptions, rows []notAppliedRow) error {
	if len(rows) == 0 {
		_, err := fmt.Fprintln(out, "No answer on record is waiting to be taken.")
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	header := "RUN\tTASK\tBY\tASKED\tSTORED\tDEADLINE\tWHAT TO DO"
	if o.redeliver {
		header += "\tTOLD"
	}
	fmt.Fprintln(tw, header)
	for _, row := range rows {
		r := row.result
		fields := []string{row.run.GetId(), recordedTask(r), recordedBy(r), formatTime(r.GetAskedAt()),
			formatTime(r.GetStoredAt()), formatTime(r.GetDeadline()), row.action}
		if o.redeliver {
			told := row.told
			if told == "" {
				told = "skipped: " + row.action
			}
			fields = append(fields, told)
		}
		// What the engine sent (a principal, a refusal) reaches the terminal
		// as text only.
		for i := range fields {
			fields[i] = terminalSafe(fields[i])
		}
		fmt.Fprintln(tw, strings.Join(fields, "\t"))
	}
	return tw.Flush()
}

// connectNotAppliedDoor is the door the command asks: the engine's RunService.
type connectNotAppliedDoor struct {
	g    *globalFlags
	call func() (context.Context, context.CancelFunc)
}

func (d connectNotAppliedDoor) listRuns(ctx context.Context, ended bool, token string) ([]*orkstackv1.Run, string, error) {
	filters := []*orkstackv1.FieldFilter{{Field: "results_recorded", Op: orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS, Values: []string{"true"}}}
	if !ended {
		filters = append(filters, &orkstackv1.FieldFilter{Field: "status", Op: orkstackv1.FilterOperator_FILTER_OPERATOR_IN,
			Values: []string{"PENDING", "RUNNING", "PAUSED"}})
	}
	cctx, cancel := d.call()
	defer cancel()
	resp, err := d.g.runClient().ListRuns(cctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
		Namespace: d.g.namespace, Filters: filters, PageSize: 200, PageToken: token}))
	if err != nil {
		return nil, "", err
	}
	return resp.Msg.GetRuns(), resp.Msg.GetNextPageToken(), nil
}

func (d connectNotAppliedDoor) listUnapplied(ctx context.Context, runID string) ([]*orkstackv1.RecordedResult, bool, error) {
	return listRecorded(ctx, pause, func(_ context.Context, token string) (*orkstackv1.ListRecordedResultsResponse, error) {
		cctx, cancel := d.call()
		defer cancel()
		resp, err := d.g.runClient().ListRecordedResults(cctx, connect.NewRequest(&orkstackv1.ListRecordedResultsRequest{
			Namespace: d.g.namespace, RunId: runID, UnappliedOnly: true, PageSize: 50, PageToken: token}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	})
}

func (d connectNotAppliedDoor) redeliver(ctx context.Context, runID, taskID string) (*orkstackv1.ResolveAwaitResponse, error) {
	cctx, cancel := d.call()
	defer cancel()
	resp, err := d.g.runClient().ResolveAwait(cctx, connect.NewRequest(&orkstackv1.ResolveAwaitRequest{
		Namespace: d.g.namespace, RunId: runID, TaskId: taskID, Redeliver: true}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (d connectNotAppliedDoor) getRun(ctx context.Context, runID string) (*orkstackv1.Run, error) {
	cctx, cancel := d.call()
	defer cancel()
	resp, err := d.g.runClient().GetRun(cctx, connect.NewRequest(&orkstackv1.GetRunRequest{Namespace: d.g.namespace, RunId: runID}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRun(), nil
}
