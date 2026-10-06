package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
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
	if err := renderTaskTable(out, wf.GetTasks()); err != nil {
		return err
	}
	if err := renderComposeTables(out, wf); err != nil {
		return err
	}
	return renderCachedTable(out, wf.GetTasks())
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
		// An awaited task runs no image: USES says so, and TIMEOUT is its
		// deadline rather than a container's wall clock.
		uses, timeout := task.GetUses(), task.GetTimeoutSeconds()
		if spec := task.GetAwait(); spec != nil {
			uses, timeout = "await", spec.GetTimeoutSeconds()
		}
		timeoutCell := fmt.Sprintf("%ds", timeout)
		if task.GetOrkflow() != nil {
			// A composed task has no timeout of its own: its child's tasks
			// carry theirs.
			timeoutCell = dash
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n",
			task.GetId(), uses, joinOrDash(task.GetNeeds()), timeoutCell)
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if err := renderEachTable(out, tasks); err != nil {
		return err
	}
	return renderJoinTable(out, tasks)
}

// renderJoinTable is the table an Orkflow with a join gets after its EACH
// table: each joined task's policy and the needs it decides on. An Orkflow
// with none prints exactly what it always did.
func renderJoinTable(out io.Writer, tasks []*orkstackv1.Task) error {
	var joined []*orkstackv1.Task
	for _, task := range tasks {
		if task.GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED {
			joined = append(joined, task)
		}
	}
	if len(joined) == 0 {
		return nil
	}
	fmt.Fprintln(out)
	table := newTable(out)
	fmt.Fprintln(table, "JOIN\tPOLICY\tNEEDS")
	for _, task := range joined {
		fmt.Fprintf(table, "%s\t%s\t%s\n", task.GetId(), dsl.JoinWord(task.GetJoin()), joinOrDash(task.GetNeeds()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// renderEachTable is the second table an Orkflow with a fan-out gets: each
// fan-out's reference, bounds and item name. An Orkflow with none prints
// exactly what it always did.
func renderEachTable(out io.Writer, tasks []*orkstackv1.Task) error {
	var fanOuts []*orkstackv1.Task
	for _, task := range tasks {
		if task.GetEach() != nil {
			fanOuts = append(fanOuts, task)
		}
	}
	if len(fanOuts) == 0 {
		return nil
	}
	fmt.Fprintln(out)
	table := newTable(out)
	fmt.Fprintln(table, "EACH\tOVER\tMAX ITEMS\tMAX PARALLEL\tAS")
	for _, task := range fanOuts {
		spec := task.GetEach()
		parallel := dash
		if spec.GetMaxParallel() > 0 {
			parallel = fmt.Sprintf("%d", spec.GetMaxParallel())
		}
		fmt.Fprintf(table, "%s\t%s\t%d\t%s\t%s\n",
			task.GetId(), spec.GetOver(), spec.GetMaxItems(), parallel, spec.GetAs())
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// renderComposeTables is what an Orkflow that composes another, or declares
// outputs, adds after its task tables: the COMPOSED table (each composed
// task's pinned child and its digest, shortened) and the OUTPUT table (each
// declared output's template). An Orkflow with neither prints exactly what it
// always did.
func renderComposeTables(out io.Writer, wf *orkstackv1.Orkflow) error {
	var composed []*orkstackv1.Task
	for _, task := range wf.GetTasks() {
		if task.GetOrkflow() != nil {
			composed = append(composed, task)
		}
	}
	if len(composed) > 0 {
		fmt.Fprintln(out)
		table := newTable(out)
		fmt.Fprintln(table, "COMPOSED\tORKFLOW\tDIGEST")
		for _, task := range composed {
			ref := task.GetOrkflow()
			fmt.Fprintf(table, "%s\t%s@%s\t%s\n", task.GetId(), ref.GetName(), ref.GetVersion(),
				shortDefinitionDigest(ref.GetDefinitionDigest()))
		}
		if err := table.Flush(); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	if outputs := wf.GetOutputs(); len(outputs) > 0 {
		keys := make([]string, 0, len(outputs))
		for k := range outputs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprintln(out)
		table := newTable(out)
		fmt.Fprintln(table, "OUTPUT\tTEMPLATE")
		for _, k := range keys {
			fmt.Fprintf(table, "%s\t%s\n", k, outputs[k])
		}
		if err := table.Flush(); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	return nil
}

// shortDefinitionDigest shortens a digest as the UI's shortDigest does: the
// algorithm, the first 12 and the last 6 hex digits.
func shortDefinitionDigest(digest string) string {
	if digest == "" {
		return dash
	}
	algo, hex, found := strings.Cut(digest, ":")
	if !found {
		algo, hex = "", digest
	}
	if len(hex) <= 20 {
		return digest
	}
	prefix := ""
	if algo != "" {
		prefix = algo + ":"
	}
	return prefix + hex[:12] + "…" + hex[len(hex)-6:]
}
