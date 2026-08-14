package main

import (
	"fmt"
	"io"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func newWorkflowCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "workflow",
		Aliases: []string{"wf"},
		Short:   "Inspect registered workflows",
	}
	cmd.AddCommand(newWorkflowListCommand(g), newWorkflowGetCommand(g))
	return cmd
}

func newWorkflowListCommand(g *globalFlags) *cobra.Command {
	var limit int32
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List registered workflows",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.workflowClient().ListWorkflows(ctx, connect.NewRequest(
				&orkstackv1.ListWorkflowsRequest{PageSize: limit},
			))
			if err != nil {
				return rpcError("list workflows", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderWorkflowList(cmd.OutOrStdout(), resp.Msg.GetWorkflows())
		},
	}
	cmd.Flags().Int32Var(&limit, "limit", 0, "maximum number of workflows to return (0 = server default)")
	return cmd
}

func newWorkflowGetCommand(g *globalFlags) *cobra.Command {
	var version string
	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Show one workflow",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.workflowClient().GetWorkflow(ctx, connect.NewRequest(
				&orkstackv1.GetWorkflowRequest{Name: args[0], Version: version},
			))
			if err != nil {
				return rpcError("get workflow", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderWorkflow(cmd.OutOrStdout(), resp.Msg.GetWorkflow())
		},
	}
	cmd.Flags().StringVar(&version, "version", "", "workflow version (default: latest applied)")
	return cmd
}

func renderWorkflowList(out io.Writer, workflows []*orkstackv1.Workflow) error {
	if len(workflows) == 0 {
		fmt.Fprintln(out, "No workflows.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "NAME\tVERSION\tTASKS\tCREATED\tDESCRIPTION")
	for _, wf := range workflows {
		fmt.Fprintf(table, "%s\t%s\t%d\t%s\t%s\n",
			wf.GetName(), wf.GetVersion(), len(wf.GetTasks()),
			formatTime(wf.GetCreatedAt()), orDash(wf.GetDescription()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func renderWorkflow(out io.Writer, wf *orkstackv1.Workflow) error {
	header := newTable(out)
	fmt.Fprintf(header, "Name:\t%s\n", wf.GetName())
	fmt.Fprintf(header, "Version:\t%s\n", wf.GetVersion())
	fmt.Fprintf(header, "Description:\t%s\n", orDash(wf.GetDescription()))
	fmt.Fprintf(header, "Created:\t%s\n", formatTime(wf.GetCreatedAt()))
	if err := header.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)
	if err := renderWorkflowInputs(out, wf.GetInputs()); err != nil {
		return err
	}
	return renderTaskTable(out, wf.GetTasks())
}

// renderWorkflowInputs prints the declared inputs, best-effort: the schema is a
// free-form struct, so unknown shapes render as a dash rather than failing.
func renderWorkflowInputs(out io.Writer, inputs *structpb.Struct) error {
	fields := inputs.GetFields()
	if len(fields) == 0 {
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "INPUT\tTYPE\tDEFAULT\tREQUIRED")
	for _, name := range sortedKeys(fields) {
		spec := fields[name].GetStructValue().GetFields()
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n",
			name,
			orDash(spec["type"].GetStringValue()),
			orDash(scalarString(spec["default"])),
			orDash(scalarString(spec["required"])),
		)
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)
	return nil
}

// scalarString renders a struct value for a single table cell.
func scalarString(value *structpb.Value) string {
	if value == nil {
		return ""
	}
	switch value.GetKind().(type) {
	case *structpb.Value_NullValue, nil:
		return ""
	case *structpb.Value_StringValue:
		return value.GetStringValue()
	default:
		encoded, err := value.MarshalJSON()
		if err != nil {
			return ""
		}
		return string(encoded)
	}
}
