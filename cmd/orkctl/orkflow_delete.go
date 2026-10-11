package main

import (
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const orkflowDeleteWarning = "This action will delete all versions and cannot be reversed."

func newOrkflowDeleteCommand(g *globalFlags) *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete an orkflow and all its versions",
		Long: "delete permanently removes an orkflow definition and all its stored versions.\n" +
			orkflowDeleteWarning + "\n\n" +
			"It asks for confirmation first, and refuses when standard input is not a\n" +
			"terminal unless --yes is given. Deletion is still refused when active or\n" +
			"recent runs use the orkflow: --yes skips only the local confirmation prompt;\n" +
			"server run-safety checks still apply.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			prompt := fmt.Sprintf("WARNING: %s\nDelete orkflow %q in namespace %q?",
				orkflowDeleteWarning, name, displayNamespace(g.namespace))
			if err := confirmDestructive(cmd, g, fmt.Sprintf("delete orkflow %q", name), prompt, yes); err != nil {
				return err
			}

			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.orkflowClient().DeleteOrkflow(ctx, connect.NewRequest(
				&orkstackv1.DeleteOrkflowRequest{Name: name, Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("delete orkflow", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted orkflow %q in namespace %q\n",
				resp.Msg.GetName(), resp.Msg.GetNamespace())
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false,
		"do not ask for confirmation (required when standard input is not a terminal); "+
			"server run-safety checks still apply")
	return cmd
}
