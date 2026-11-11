package main

import (
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/automation"
)

func newAutomationCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "automation",
		Short: "Apply and export automation documents",
		Long: "An automation document (kind: Automation) declares one namespace's\n" +
			"settings, schedules, triggers and webhooks by name (docs/AUTOMATION.md).\n" +
			"apply plans it against what the namespace holds and writes the plan;\n" +
			"export prints the document a namespace's current objects would be applied\n" +
			"from. Orkflows are created with orkctl orkflow create.",
	}
	cmd.AddCommand(newAutomationApplyCommand(g), newAutomationExportCommand(g))
	return cmd
}

func newAutomationApplyCommand(g *globalFlags) *cobra.Command {
	var opts applyOptions
	cmd := &cobra.Command{
		Use:   "apply <file|dir>...",
		Short: "Apply automation documents",
		Long: "apply plans each automation document (kind: Automation) against what its\n" +
			"namespace holds, then sends each planned write to the existing RPC it\n" +
			"names, in order. Nothing is written until every document has been\n" +
			"validated and planned. One document describes one namespace.\n\n" +
			"A directory is read non-recursively for *.ork.yaml and *.ork.yml files.\n" +
			"Its activity interfaces (kind: ActivityInterface) are skipped, each named\n" +
			"on stderr. An Orkflow (kind: Orkflow) is refused, in a directory too:\n" +
			"create Orkflows with orkctl orkflow create first, then apply the\n" +
			"automation that names them.\n\n" +
			"An automation document names its own namespace, which is authoritative:\n" +
			"only an -n given on this command line is checked against it, and\n" +
			"ORK_NAMESPACE and the saved default never apply to it.\n\n" +
			"--dry-run prints the plan and writes nothing. --check is a dry run that\n" +
			"exits 4 when anything would change. Only --prune deletes, and only named\n" +
			"objects of the sections a document declares. --expect-plan refuses a plan\n" +
			"whose digest differs from the one reviewed. A trigger token or a generated\n" +
			"webhook secret is shown once; --secrets-out writes them to a new 0600 file\n" +
			"instead.",
		Example: "  orkctl automation apply automation.ork.yaml --dry-run\n" +
			"  orkctl automation apply environments/prod/ --check\n" +
			"  orkctl automation apply automation.ork.yaml --prune --expect-plan sha256:3f9a… --secrets-out secrets.json",
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.dryRun && opts.check {
				return fmt.Errorf("--check is a dry run already; give one of --dry-run and --check")
			}
			return runApplyDocuments(cmd, g, args, opts, automationDocument)
		},
	}
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "print the plan and write nothing")
	cmd.Flags().BoolVar(&opts.check, "check", false, "a dry run that exits 4 when anything would change")
	cmd.Flags().BoolVar(&opts.prune, "prune", false, "also delete named objects of a declared section the document no longer declares")
	cmd.Flags().StringVar(&opts.expectPlan, "expect-plan", "", "refuse unless the plan digest equals this one (sha256:HEX)")
	cmd.Flags().StringVar(&opts.secretsOut, "secrets-out", "", "write disclosed one-time secrets to this NEW file (mode 0600) instead of stdout")
	return cmd
}

func newAutomationExportCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "export",
		Short: "Print a namespace's automation as a document",
		Long: "export renders the automation document a namespace's current settings,\n" +
			"schedules, triggers and webhooks would be applied from (docs/AUTOMATION.md),\n" +
			"so a namespace managed by hand can start being reviewed in Git:\n\n" +
			"  orkctl automation export -n payments > automation.ork.yaml\n\n" +
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
				return fmt.Errorf("automation export: this engine does not serve AutomationService; upgrade the engine")
			}
			if err != nil {
				return rpcError("automation export", err)
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
