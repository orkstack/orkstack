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
	var opts applyOptions
	cmd := &cobra.Command{
		Use:   "apply <file|dir>...",
		Short: "Apply orkflow and automation documents",
		Long: "apply uploads orkflow DSL documents to the engine, which compiles,\n" +
			"validates, and stores them. Compilation errors are reported by the engine.\n\n" +
			"It also applies automation documents (kind: Automation): one YAML file per\n" +
			"namespace declaring its settings, schedules, triggers and webhooks by name\n" +
			"(docs/AUTOMATION.md). The engine plans; this command sends each planned\n" +
			"write to the existing RPC it names, in order. A directory is read\n" +
			"non-recursively for *.ork.yaml and *.ork.yml files; orkflows are applied\n" +
			"before automation, and nothing is written until every document has been\n" +
			"validated and planned.\n\n" +
			"An automation document names its own namespace, which is authoritative:\n" +
			"only an -n given on this command line is checked against it, and\n" +
			"ORK_NAMESPACE and the saved default never apply to it.\n\n" +
			"--dry-run prints the plan and writes nothing. --check is a dry run that\n" +
			"exits 4 when anything would change. Only --prune deletes, and only named\n" +
			"objects of the sections a document declares. --expect-plan refuses a plan\n" +
			"whose digest differs from the one reviewed. A trigger token or a generated\n" +
			"webhook secret is shown once; --secrets-out writes them to a new 0600 file\n" +
			"instead.",
		Example: "  orkctl apply hello.ork.yaml\n" +
			"  orkctl apply environments/prod/ --dry-run\n" +
			"  orkctl apply automation.ork.yaml --prune --expect-plan sha256:3f9a… --secrets-out secrets.json",
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.dryRun && opts.check {
				return fmt.Errorf("--check is a dry run already; give one of --dry-run and --check")
			}
			anyNew := opts.dryRun || opts.check || opts.prune || opts.expectPlan != "" || opts.secretsOut != ""
			if len(args) == 1 && !anyNew {
				// The shape that existed before automation documents — one
				// orkflow file, no new flag — runs exactly as it always has.
				if info, err := os.Stat(args[0]); err != nil || !info.IsDir() {
					source, err := os.ReadFile(args[0])
					if err != nil {
						return fmt.Errorf("read orkflow file: %w", err)
					}
					in, err := classifyDocument(args[0], source)
					if err != nil {
						return err
					}
					if in.kind == orkflowDocument {
						return applyOneOrkflow(cmd, g, source)
					}
				}
			}
			return runApplyDocuments(cmd, g, args, opts)
		},
	}
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "print the plan and write nothing")
	cmd.Flags().BoolVar(&opts.check, "check", false, "a dry run that exits 4 when anything would change")
	cmd.Flags().BoolVar(&opts.prune, "prune", false, "also delete named objects of a declared section the document no longer declares")
	cmd.Flags().StringVar(&opts.expectPlan, "expect-plan", "", "refuse unless the plan digest equals this one (sha256:HEX)")
	cmd.Flags().StringVar(&opts.secretsOut, "secrets-out", "", "write disclosed one-time secrets to this NEW file (mode 0600) instead of stdout")
	return cmd
}

// applyOneOrkflow is `orkctl apply FILE` as it existed before automation
// documents: one ApplyOrkflow, printed exactly as before.
func applyOneOrkflow(cmd *cobra.Command, g *globalFlags, source []byte) error {
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
	fmt.Fprintln(table, "TASK\tUSES\tNEEDS\tTIMEOUT")
	for _, task := range tasks {
		fmt.Fprintf(table, "%s\t%s\t%s\t%ds\n",
			task.GetId(), task.GetUses(), joinOrDash(task.GetNeeds()), task.GetTimeoutSeconds())
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
