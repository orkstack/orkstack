package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const orkflowDeleteWarning = "This action will delete all versions and cannot be reversed."

func newOrkflowDeleteCommand(g *globalFlags) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete an orkflow and all its versions",
		Long: "delete permanently removes an orkflow definition and all its stored versions.\n" +
			orkflowDeleteWarning + "\n\n" +
			"Deletion is still refused when active or recent runs use the orkflow.\n" +
			"--force skips only the local confirmation prompt; server run-safety checks still apply.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if !force {
				confirmed, err := confirmOrkflowDeletion(cmd, name, g.namespace)
				if err != nil {
					return err
				}
				if !confirmed {
					fmt.Fprintln(cmd.ErrOrStderr(), "Cancelled.")
					return nil
				}
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
	cmd.Flags().BoolVar(&force, "force", false,
		"skip only the local confirmation prompt; server run-safety checks still apply")
	return cmd
}

func confirmOrkflowDeletion(cmd *cobra.Command, name, namespace string) (bool, error) {
	if namespace == "" {
		namespace = "default"
	}
	fmt.Fprintln(cmd.ErrOrStderr(), "WARNING:", orkflowDeleteWarning)
	fmt.Fprintf(cmd.ErrOrStderr(),
		"Delete orkflow %q in namespace %q? Type 'yes' to confirm:", name, namespace)

	answer, err := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("read confirmation: %w", err)
	}
	return strings.TrimSpace(answer) == "yes", nil
}
