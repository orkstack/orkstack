package main

import (
	"fmt"
	"io"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

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
		newRunCancelCommand(g),
	)
	return cmd
}

func newRunSubmitCommand(g *globalFlags) *cobra.Command {
	var (
		inputs         []string
		inputJSON      string
		version        string
		idempotencyKey string
		watch          bool
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
	return cmd
}

func newRunGetCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get <run-id>",
		Short: "Show one run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.runClient().GetRun(ctx, connect.NewRequest(
				&orkstackv1.GetRunRequest{RunId: args[0]},
			))
			if err != nil {
				return rpcError("get run", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderRun(cmd.OutOrStdout(), resp.Msg.GetRun())
		},
	}
}

func newRunListCommand(g *globalFlags) *cobra.Command {
	var (
		orkflowName  string
		statusFilter string
		limit        int32
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
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
				OrkflowName: orkflowName,
				Status:      status,
				PageSize:    limit,
				Namespace:   g.namespace,
			}))
			if err != nil {
				return rpcError("list runs", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderRunList(cmd.OutOrStdout(), resp.Msg.GetRuns())
		},
	}
	cmd.Flags().StringVar(&orkflowName, "orkflow", "", "only runs of this orkflow")
	cmd.Flags().StringVar(&statusFilter, "status", "",
		"only runs with this status (pending, running, succeeded, failed, canceled, skipped)")
	cmd.Flags().Int32Var(&limit, "limit", 0, "maximum number of runs to return (0 = server default)")
	return cmd
}

func newRunCancelCommand(g *globalFlags) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "cancel <run-id>",
		Short: "Request cancellation of a run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.runClient().CancelRun(ctx, connect.NewRequest(
				&orkstackv1.CancelRunRequest{RunId: args[0], Reason: reason},
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
	fmt.Fprintln(table, "TASK\tPLUGIN\tSTATUS\tATTEMPT\tSTARTED\tFINISHED\tERROR")
	for _, taskRun := range run.GetTaskRuns() {
		fmt.Fprintf(table, "%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
			taskRun.GetTaskId(), taskRun.GetPlugin(), statusLabel(taskRun.GetStatus()),
			taskRun.GetAttempt(), formatTime(taskRun.GetStartedAt()),
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
