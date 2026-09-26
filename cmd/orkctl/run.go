package main

import (
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func newRunCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Submit and observe orkflow runs",
	}
	cmd.AddCommand(
		newRunSubmitCommand(g),
		newRunGetCommand(g),
		newRunListCommand(g),
		newRunWatchCommand(g),
		newRunLogsCommand(g),
		newRunArtifactsCommand(g),
		newRunArtifactCommand(g),
		newRunAttemptsCommand(g),
		newRunDiffCommand(g),
		newRunTimingCommand(g),
		newRunAgainCommand(g),
		newRunCancelCommand(g),
	)
	return cmd
}

func parseRFC3339Bound(flag, value string) (*timestamppb.Timestamp, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, fmt.Errorf("%s must be RFC3339: %w", flag, err)
	}
	return timestamppb.New(parsed), nil
}

func newRunSubmitCommand(g *globalFlags) *cobra.Command {
	var (
		inputs         []string
		inputJSON      string
		version        string
		idempotencyKey string
		watch          bool
		allowEmptyPool bool
	)
	cmd := &cobra.Command{
		Use:   "submit <orkflow>",
		Short: "Submit a run of a registered orkflow",
		Long: "submit starts a run of a registered orkflow.\n\n" +
			"Each --input value is parsed as JSON when it is valid JSON (so 3, true,\n" +
			"[1,2] and {\"a\":1} keep their types) and kept as a plain string otherwise.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			payload, err := buildInputPayload(inputs, inputJSON)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			client := g.runClient()
			resp, err := client.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
				OrkflowName:    args[0],
				OrkflowVersion: version,
				Input:          payload,
				IdempotencyKey: idempotencyKey,
				Namespace:      g.namespace,
				AllowEmptyPool: allowEmptyPool,
			}))
			if err != nil {
				return rpcError("submit run", err)
			}
			cancel()

			run := resp.Msg.GetRun()
			if !watch {
				if g.json {
					return writeJSON(cmd.OutOrStdout(), resp.Msg)
				}
				return renderSubmitted(cmd.OutOrStdout(), run)
			}
			if !g.json {
				if err := renderSubmitted(cmd.OutOrStdout(), run); err != nil {
					return err
				}
				fmt.Fprintln(cmd.OutOrStdout())
			}
			return watchRun(cmd, g, run.GetId(), 0)
		},
	}
	cmd.Flags().StringArrayVar(&inputs, "input", nil,
		"run input as key=value (repeatable); values that parse as JSON keep their JSON type")
	cmd.Flags().StringVar(&inputJSON, "input-json", "",
		"run input as a JSON object; merged under --input values")
	cmd.Flags().StringVar(&version, "version", "", "orkflow version (default: the current one)")
	cmd.Flags().StringVar(&idempotencyKey, "idempotency-key", "",
		"dedupe key for repeated submissions, scoped to this namespace")
	cmd.Flags().BoolVar(&watch, "watch", false, "stream events until the run reaches a terminal state")
	cmd.Flags().BoolVar(&allowEmptyPool, "allow-empty-pool", false, "submit even when the selected pool has no live Orkers")
	return cmd
}

func newRunGetCommand(g *globalFlags) *cobra.Command {
	var withAttempts, withTiming bool
	cmd := &cobra.Command{
		Use:   "get <run-id>",
		Short: "Show one run",
		Long: "get shows one run of the namespace this command acts in. A run id\n" +
			"addresses a run inside a namespace, so a run of another one is not found\n" +
			"here even when its id is known.\n\n" +
			"The task table shows each task's FINAL outcome, which is all Temporal\n" +
			"keeps: a try that failed and was retried away is not in it. --attempts\n" +
			"adds the per-attempt timeline underneath, the same table\n" +
			"`orkctl run attempts` prints. --timing adds the wait/run/total table and\n" +
			"each attempt's phases, the same output `orkctl run timing` prints.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if withAttempts && g.json {
				// Two responses cannot share one JSON document, and printing
				// them one after another would hand a script two concatenated
				// objects. The machine-readable timeline has its own command.
				return fmt.Errorf(
					"--attempts is a second response and cannot be added to --json output; "+
						"read the timeline with: %s",
					orkctlCommand(g.namespace, "run", "attempts", args[0], "--json"))
			}
			if withTiming && g.json {
				return fmt.Errorf(
					"--timing is a second response and cannot be added to --json output; "+
						"read the timing with: %s",
					orkctlCommand(g.namespace, "run", "timing", args[0], "--json"))
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.runClient().GetRun(ctx, connect.NewRequest(
				&orkstackv1.GetRunRequest{RunId: args[0], Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("get run", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			if err := renderRun(cmd.OutOrStdout(), resp.Msg.GetRun()); err != nil {
				return err
			}
			if withAttempts {
				if err := appendRunAttempts(cmd, g, args[0]); err != nil {
					return err
				}
			}
			if withTiming {
				return appendRunTiming(cmd, g, args[0])
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&withAttempts, "attempts", false,
		"also print every attempt of every task, including the ones that were retried away")
	cmd.Flags().BoolVar(&withTiming, "timing", false,
		"also print each task's wait, run and total time and every attempt's phases")
	return cmd
}

// appendRunAttempts prints the attempt timeline under a rendered run.
//
// It is a second RPC, made only when the flag asks for it: `run get` is the
// command an operator types constantly, and the retry history is a read of the
// object store that most of those invocations do not need.
func appendRunAttempts(cmd *cobra.Command, g *globalFlags, runID string) error {
	ctx, cancel := g.unaryContext(cmd)
	defer cancel()

	resp, err := g.runClient().ListTaskAttempts(ctx, connect.NewRequest(
		&orkstackv1.ListTaskAttemptsRequest{RunId: runID, Namespace: g.namespace},
	))
	if err != nil {
		return attemptsError(err)
	}
	fmt.Fprintln(cmd.OutOrStdout())
	return renderAttemptList(cmd.OutOrStdout(), resp.Msg.GetAttempts())
}

func newRunListCommand(g *globalFlags) *cobra.Command {
	var (
		orkflowName  string
		statusFilter string
		limit        int32
		pageToken    string
		sinceText    string
		untilText    string
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List runs, newest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			status, err := parseStatus(statusFilter)
			if err != nil {
				return err
			}
			since, err := parseRFC3339Bound("--since", sinceText)
			if err != nil {
				return err
			}
			until, err := parseRFC3339Bound("--until", untilText)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
				OrkflowName: orkflowName,
				Status:      status,
				PageSize:    limit,
				Namespace:   g.namespace,
				PageToken:   pageToken, CreatedAfter: since, CreatedBefore: until,
			}))
			if err != nil {
				return rpcError("list runs", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			if err := renderRunList(cmd.OutOrStdout(), resp.Msg.GetRuns()); err != nil {
				return err
			}
			if next := resp.Msg.GetNextPageToken(); next != "" {
				_, err := fmt.Fprintf(cmd.OutOrStdout(), "\nNext page: orkctl -n %s run list --page-token %s\n", g.namespace, next)
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&orkflowName, "orkflow", "", "only runs of this orkflow")
	cmd.Flags().StringVar(&statusFilter, "status", "",
		"only runs with this status (pending, running, succeeded, failed, canceled, skipped)")
	cmd.Flags().Int32Var(&limit, "limit", 0, "maximum number of runs to return (0 = server default)")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token from a previous list")
	cmd.Flags().StringVar(&sinceText, "since", "", "created at or after RFC3339 timestamp")
	cmd.Flags().StringVar(&untilText, "until", "", "created before RFC3339 timestamp")
	return cmd
}

func newRunCancelCommand(g *globalFlags) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "cancel <run-id>",
		Short: "Request cancellation of a run",
		Long: "cancel requests cancellation of a run of the namespace this command acts\n" +
			"in. A run of another namespace is not found here: cancelling is a write,\n" +
			"and holding an id is not authority over somebody else's tenant.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.runClient().CancelRun(ctx, connect.NewRequest(
				&orkstackv1.CancelRunRequest{RunId: args[0], Reason: reason, Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("cancel run", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Cancellation requested for run %s\n", resp.Msg.GetRun().GetId())
			return nil
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "human readable cancellation reason")
	return cmd
}

// renderSubmitted prints the freshly created run, id first.
func renderSubmitted(out io.Writer, run *orkstackv1.Run) error {
	fmt.Fprintf(out, "Submitted run %s\n", run.GetId())
	table := newTable(out)
	fmt.Fprintf(table, "Orkflow:\t%s\n", run.GetOrkflowName())
	fmt.Fprintf(table, "Version:\t%s\n", run.GetOrkflowVersion())
	fmt.Fprintf(table, "Status:\t%s\n", statusLabel(run.GetStatus()))
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// renderRun prints a run detail view with its task runs.
func renderRun(out io.Writer, run *orkstackv1.Run) error {
	header := newTable(out)
	fmt.Fprintf(header, "Run:\t%s\n", run.GetId())
	fmt.Fprintf(header, "Orkflow:\t%s\n", run.GetOrkflowName())
	fmt.Fprintf(header, "Version:\t%s\n", run.GetOrkflowVersion())
	fmt.Fprintf(header, "Status:\t%s\n", statusLabel(run.GetStatus()))
	fmt.Fprintf(header, "Created:\t%s\n", formatTime(run.GetCreatedAt()))
	fmt.Fprintf(header, "Started:\t%s\n", formatTime(run.GetStartedAt()))
	fmt.Fprintf(header, "Finished:\t%s\n", formatTime(run.GetFinishedAt()))
	fmt.Fprintf(header, "Input:\t%s\n", payloadSummary(run.GetInput()))
	fmt.Fprintf(header, "Output:\t%s\n", payloadSummary(run.GetOutput()))
	if run.GetError() != nil {
		fmt.Fprintf(header, "Error:\t%s\n", errorLine(run.GetError()))
	}
	if err := header.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)

	if len(run.GetTaskRuns()) == 0 {
		fmt.Fprintln(out, "No task runs yet.")
		return nil
	}
	table := newTable(out)
	// POOL/PIN/ORKER sit between the identity and the outcome: WHERE a task ran
	// is the question asked right after WHAT it is and HOW it ended. PIN and
	// ORKER together are the client-visible proof a pin held — same pin, same
	// orker. ORKER is filled for every task that reached an Orker, pinned or
	// not; a dash therefore means "never got there", not "not pinned".
	//
	// SCHEDULED is when the engine dispatched the task; STARTED is when the
	// Orker began the attempt that decided it. The gap between them is the
	// wait `orkctl run timing` breaks down.
	fmt.Fprintln(table, "TASK\tSTATUS\tPOOL\tPIN\tORKER\tATTEMPT\tEXIT\tSCHEDULED\tSTARTED\tFINISHED\tERROR")
	for _, taskRun := range run.GetTaskRuns() {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
			taskRun.GetTaskId(), taskStatusLabel(taskRun),
			orDash(taskRun.GetPool()), orDash(taskRun.GetPin()), orDash(taskRun.GetOrkerId()),
			taskRun.GetAttempt(), taskRun.GetExitCode(), formatTime(taskRun.GetScheduledAt()),
			formatTime(taskRun.GetStartedAt()),
			formatTime(taskRun.GetFinishedAt()), errorLine(taskRun.GetError()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func renderRunList(out io.Writer, runs []*orkstackv1.Run) error {
	if len(runs) == 0 {
		fmt.Fprintln(out, "No runs.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "ID\tORKFLOW\tVERSION\tSTATUS\tCREATED\tFINISHED")
	for _, run := range runs {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n",
			run.GetId(), run.GetOrkflowName(), run.GetOrkflowVersion(),
			statusLabel(run.GetStatus()), formatTime(run.GetCreatedAt()), formatTime(run.GetFinishedAt()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
