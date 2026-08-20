package main

import (
	"fmt"
	"io"
	"os"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func newApplyCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "apply <file.ork.yaml>",
		Short: "Compile and register an orkflow from a DSL file",
		Long: "apply uploads an orkflow DSL document to the engine, which compiles,\n" +
			"validates, and stores it. Compilation errors are reported by the engine.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			source, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read orkflow file: %w", err)
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.orkflowClient().ApplyOrkflow(ctx, connect.NewRequest(
				&orkstackv1.ApplyOrkflowRequest{Yaml: string(source), Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("apply", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderApply(cmd.OutOrStdout(), resp.Msg)
		},
	}
}

// renderApply prints the registered orkflow and any compiler warnings.
func renderApply(out io.Writer, resp *orkstackv1.ApplyOrkflowResponse) error {
	wf := resp.GetOrkflow()
	fmt.Fprintf(out, "Applied orkflow %s (version %s)\n", wf.GetName(), wf.GetVersion())
	if description := wf.GetDescription(); description != "" {
		fmt.Fprintf(out, "%s\n", description)
	}
	if warnings := resp.GetWarnings(); len(warnings) > 0 {
		fmt.Fprintf(out, "\nWarnings:\n")
		for _, warning := range warnings {
			fmt.Fprintf(out, "  - %s\n", warning)
		}
	}
	fmt.Fprintln(out)
	return renderTaskTable(out, wf.GetTasks())
}

// renderTaskTable prints an orkflow's task DAG.
func renderTaskTable(out io.Writer, tasks []*orkstackv1.Task) error {
	if len(tasks) == 0 {
		fmt.Fprintln(out, "No tasks.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "TASK\tPLUGIN\tNEEDS\tTIMEOUT")
	for _, task := range tasks {
		fmt.Fprintf(table, "%s\t%s\t%s\t%ds\n",
			task.GetId(), task.GetPlugin(), joinOrDash(task.GetNeeds()), task.GetTimeoutSeconds())
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
