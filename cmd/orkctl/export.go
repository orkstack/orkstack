package main

import (
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/automation"
)

func newExportCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "export",
		Short: "Print a namespace's automation as a document",
		Long: "export renders the automation document a namespace's current settings,\n" +
			"schedules, triggers and webhooks would be applied from (docs/AUTOMATION.md),\n" +
			"so a namespace managed by hand can start being reviewed in Git:\n\n" +
			"  orkctl export -n payments > automation.ork.yaml\n\n" +
			"An unnamed trigger or webhook is outside every document. The export lists\n" +
			"each one in a trailing comment with the command that names it in place —\n" +
			"keeping its token or secret — and repeats that list on stderr.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.automationClient().ExportAutomation(ctx, connect.NewRequest(&orkstackv1.ExportAutomationRequest{
				Namespace: g.namespace,
			}))
			if connect.CodeOf(err) == connect.CodeUnimplemented {
				return fmt.Errorf("export: this engine does not serve AutomationService; upgrade the engine")
			}
			if err != nil {
				return rpcError("export", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			if _, err := fmt.Fprint(cmd.OutOrStdout(), resp.Msg.GetYaml()); err != nil {
				return fmt.Errorf("write output: %w", err)
			}
			if unmanaged := resp.Msg.GetUnmanaged(); len(unmanaged) > 0 {
				stderr := cmd.ErrOrStderr()
				fmt.Fprintf(stderr, "%d object(s) have no name and are not in this document:\n", len(unmanaged))
				for _, step := range unmanaged {
					fmt.Fprintf(stderr, "  %s %s  %s\n", automation.KindSlug(step.GetKind()), step.GetId(), step.GetSummary())
					for _, note := range step.GetNotes() {
						fmt.Fprintf(stderr, "    %s\n", note)
					}
				}
			}
			return nil
		},
	}
}
