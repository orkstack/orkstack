package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const (
	// maxPagesNoNewRow is how many pages in a row may bring no row before a
	// listing stops.
	maxPagesNoNewRow = 3
	// recordsPace is how long a listing waits before it asks again, of an
	// engine that answered it too often.
	recordsPace = time.Second
	// maxRecordsPaced is how often in a row it waits.
	maxRecordsPaced = 30
)

// errRecordsIncomplete ends a listing that did not read every row.
var errRecordsIncomplete = errors.New("the listing is not complete: some records could not be read; run it again")

// newRunRecordsCommand lists what the engine recorded for a run
// (docs/TASK_IDENTITY.md, "Reading what is on record").
func newRunRecordsCommand(g *globalFlags) *cobra.Command {
	var task string
	cmd := &cobra.Command{
		Use:   "records <run-id>",
		Short: "List what the engine recorded for a run that holds a task identity",
		Long: "records lists, for a run whose results the engine records, each result,\n" +
			"resolution, composed task and outputs on record, and each result the run's\n" +
			"state calls succeeded that has none.\n\n" +
			"not on record is the alarm: the run says the task succeeded, and no engine\n" +
			"recorded it. recorded, not applied is an answer the run has not taken.\n\n" +
			"It exits non-zero when a row could not be read.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			rows, complete, err := listRecorded(cmd.Context(), pause, func(_ context.Context, token string) (*orkstackv1.ListRecordedResultsResponse, error) {
				ctx, cancel := g.unaryContext(cmd)
				defer cancel()
				resp, err := g.runClient().ListRecordedResults(ctx, connect.NewRequest(&orkstackv1.ListRecordedResultsRequest{
					Namespace: g.namespace, RunId: args[0], TaskId: task, PageSize: 50, PageToken: token}))
				if err != nil {
					return nil, err
				}
				return resp.Msg, nil
			})
			if err != nil {
				return rpcError("run records", err)
			}
			if g.json {
				err = writeJSON(cmd.OutOrStdout(), &orkstackv1.ListRecordedResultsResponse{Results: rows, ReadComplete: complete})
			} else {
				err = renderRecorded(cmd.OutOrStdout(), g.namespace, args[0], rows)
			}
			if err == nil && !complete {
				return errRecordsIncomplete
			}
			return err
		},
	}
	cmd.Flags().StringVarP(&task, "task", "t", "", "only this task's")
	return cmd
}

func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// listRecorded is every page ask answers, and whether each was complete. It
// waits, as often as it must, for an engine that says it was asked too often,
// and stops after three pages in a row that brought no row.
func listRecorded(ctx context.Context, wait func(context.Context, time.Duration) error,
	ask func(ctx context.Context, token string) (*orkstackv1.ListRecordedResultsResponse, error)) (rows []*orkstackv1.RecordedResult, complete bool, err error) {
	complete = true
	token, empty, paced := "", 0, 0
	for {
		page, err := ask(ctx, token)
		if connect.CodeOf(err) == connect.CodeResourceExhausted && paced < maxRecordsPaced {
			paced++
			if err := wait(ctx, recordsPace); err != nil {
				return nil, false, err
			}
			continue
		}
		if err != nil {
			return nil, false, err
		}
		paced = 0
		rows = append(rows, page.GetResults()...)
		budget := page.GetIncompleteReason() == orkstackv1.RecordedReadStop_RECORDED_READ_STOP_READ_BUDGET
		if !page.GetReadComplete() && !budget {
			complete = false
		}
		if empty++; len(page.GetResults()) > 0 {
			empty = 0
		}
		was := token
		if token = page.GetNextPageToken(); token == "" {
			return rows, complete, nil
		}
		if empty >= maxPagesNoNewRow || (token == was && !budget) {
			return rows, false, nil
		}
		// The run's reads of this minute come back as it passes.
		if budget {
			if err := wait(ctx, 10*recordsPace); err != nil {
				return nil, false, err
			}
		}
	}
}

// recordedWords is a state as a person reads it.
func recordedWords(r *orkstackv1.RecordedResult) string {
	awaits := r.GetTaskStatus() == orkstackv1.Status_STATUS_AWAITING
	switch r.GetState() {
	case orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_ON_RECORD:
		return "on record"
	case orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_APPLIED:
		switch {
		case awaits:
			return "recorded; the run has not taken it"
		case r.GetInChildRun():
			return "recorded, not applied; run the parent again, this answer is not carried over"
		}
		return "recorded, not applied"
	case orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_TAKEN:
		return "not used: it came too late, or before a resume replaced it"
	case orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_ON_RECORD:
		return "NOT ON RECORD"
	case orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_RECLAIMED:
		return "removed with the run's records"
	case orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_UNREAD:
		return "could not be read"
	case orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_RECORDED_NEWER_FORMAT:
		return "recorded in a newer format than this engine reads; ask a newer replica"
	}
	return "unknown; update this client"
}

func recordedBy(r *orkstackv1.RecordedResult) string {
	switch {
	case r.GetPrincipal() != "" && r.GetPrincipalVerified():
		return r.GetPrincipal()
	case r.GetPrincipal() != "":
		return r.GetPrincipal() + " (asserted)"
	case r.GetVia() == orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK:
		return "its callback"
	case r.GetOrkerId() != "":
		return r.GetOrkerId()
	}
	return dash
}

func recordedTask(r *orkstackv1.RecordedResult) string {
	name := r.GetTaskId()
	if r.Item != nil {
		name += "[" + strconv.Itoa(int(r.GetItem())) + "]"
	}
	if r.GetKind() == orkstackv1.RecordedResultKind_RECORDED_RESULT_KIND_OUTPUTS {
		name = "(outputs)"
	}
	return orDash(name)
}

func renderRecorded(out io.Writer, namespace, runID string, rows []*orkstackv1.RecordedResult) error {
	if len(rows) == 0 {
		_, err := fmt.Fprintln(out, "The engine records no result of this run.")
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TASK\tRECORD\tSTATE\tBY\tSTORED\tVALUES")
	missing := 0
	for _, r := range rows {
		kind := strings.ToLower(strings.TrimPrefix(r.GetKind().String(), "RECORDED_RESULT_KIND_"))
		if r.GetRejected() {
			kind = "rejection"
		}
		values := dash
		if v := r.GetValuesSha256(); len(v) >= 12 {
			values = "sha256:" + v[:12]
		}
		if r.GetState() == orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_ON_RECORD {
			missing++
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", recordedTask(r), kind, recordedWords(r), recordedBy(r), formatTime(r.GetStoredAt()), values)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, r := range rows {
		if r.GetKind() == orkstackv1.RecordedResultKind_RECORDED_RESULT_KIND_RESOLUTION &&
			r.GetState() == orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_APPLIED && r.GetTaskStatus() == orkstackv1.Status_STATUS_AWAITING {
			if _, err := fmt.Fprintf(out, "Tell the run again: %s\n", orkctlCommand(namespace, "run", "resolve", runID, "--task", r.GetTaskId(), "--redeliver")); err != nil {
				return err
			}
		}
	}
	if missing > 0 {
		_, err := fmt.Fprintf(out, "%d not on record: the run says they succeeded, and no engine recorded them.\n", missing)
		return err
	}
	return nil
}
