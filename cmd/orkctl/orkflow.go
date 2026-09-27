package main

import (
	"fmt"
	"io"
	"os"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func newOrkflowCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "orkflow",
		Aliases: []string{"of"},
		Short:   "Inspect registered orkflows and their versions",
		Long: "An orkflow's versions are immutable: applying an edit adds a version and\n" +
			"never rewrites one. Which version runs is a pointer you move with\n" +
			"`promote`, and move back the same way.",
	}
	cmd.AddCommand(
		newOrkflowListCommand(g),
		newOrkflowValidateCommand(g),
		newOrkflowGetCommand(g),
		newOrkflowVersionsCommand(g),
		newOrkflowPromoteCommand(g),
		newOrkflowDiffCommand(g),
		newOrkflowEditCommand(g),
		newOrkflowDeleteCommand(g),
	)
	return cmd
}

func newOrkflowValidateCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{Use: "validate <file.ork.yaml>", Short: "Validate an orkflow without storing it", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		source, err := os.ReadFile(args[0])
		if err != nil {
			return fmt.Errorf("read orkflow file: %w", err)
		}
		ctx, cancel := g.unaryContext(cmd)
		defer cancel()
		response, err := g.orkflowClient().ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: string(source), Namespace: g.namespace}))
		if err != nil {
			return rpcError("validate orkflow", err)
		}
		if g.json {
			return writeJSON(cmd.OutOrStdout(), response.Msg)
		}
		out := cmd.OutOrStdout()
		failed, warnings := false, 0
		for _, issue := range response.Msg.GetIssues() {
			severity := "warning"
			if issue.GetSeverity() == orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR {
				severity, failed = "error", true
			} else {
				warnings++
			}
			if issue.GetField() != "" {
				fmt.Fprintf(out, "%-8s %s: %s\n", severity, issue.GetField(), issue.GetMessage())
			} else {
				fmt.Fprintf(out, "%-8s %s\n", severity, issue.GetMessage())
			}
		}
		// Only an ERROR fails validation: a warned document is one apply
		// accepts, and docs/UI_CLI_PARITY.md says "invalid documents exit
		// nonzero" — not warned ones.
		if !failed {
			wf := response.Msg.GetOrkflow()
			if warnings == 0 {
				fmt.Fprintf(out, "Valid orkflow %s (version %s)\n", wf.GetName(), wf.GetVersion())
			} else {
				fmt.Fprintf(out, "Valid orkflow %s (version %s), %s\n", wf.GetName(), wf.GetVersion(), countNoun(warnings, "warning"))
			}
			return nil
		}
		return fmt.Errorf("orkflow validation failed")
	}}
}

func newOrkflowListCommand(g *globalFlags) *cobra.Command {
	var limit int32
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List registered orkflows",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.orkflowClient().ListOrkflows(ctx, connect.NewRequest(
				&orkstackv1.ListOrkflowsRequest{PageSize: limit, Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("list orkflows", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderOrkflowList(cmd.OutOrStdout(), resp.Msg.GetOrkflows())
		},
	}
	cmd.Flags().Int32Var(&limit, "limit", 0, "maximum number of orkflows to return (0 = server default)")
	return cmd
}

func newOrkflowGetCommand(g *globalFlags) *cobra.Command {
	var version string
	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Show one orkflow",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.orkflowClient().GetOrkflow(ctx, connect.NewRequest(
				&orkstackv1.GetOrkflowRequest{Name: args[0], Version: version, Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("get orkflow", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderOrkflow(cmd.OutOrStdout(), resp.Msg.GetOrkflow())
		},
	}
	cmd.Flags().StringVar(&version, "version", "", "orkflow version (default: the current one)")
	return cmd
}

func renderOrkflowList(out io.Writer, orkflows []*orkstackv1.Orkflow) error {
	if len(orkflows) == 0 {
		fmt.Fprintln(out, "No orkflows.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "NAME\tVERSION\tTASKS\tCREATED\tDESCRIPTION")
	for _, wf := range orkflows {
		fmt.Fprintf(table, "%s\t%s\t%d\t%s\t%s\n",
			wf.GetName(), wf.GetVersion(), len(wf.GetTasks()),
			formatTime(wf.GetCreatedAt()), orDash(wf.GetDescription()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func renderOrkflow(out io.Writer, wf *orkstackv1.Orkflow) error {
	header := newTable(out)
	fmt.Fprintf(header, "Name:\t%s\n", wf.GetName())
	fmt.Fprintf(header, "Version:\t%s\n", wf.GetVersion())
	fmt.Fprintf(header, "Description:\t%s\n", orDash(wf.GetDescription()))
	fmt.Fprintf(header, "Created:\t%s\n", formatTime(wf.GetCreatedAt()))
	if err := header.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)
	if err := renderOrkflowInputs(out, wf.GetInputs()); err != nil {
		return err
	}
	return renderTaskTable(out, wf.GetTasks())
}

// renderOrkflowInputs prints the declared inputs, best-effort: the schema is a
// free-form struct, so unknown shapes render as a dash rather than failing.
func renderOrkflowInputs(out io.Writer, inputs *structpb.Struct) error {
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
