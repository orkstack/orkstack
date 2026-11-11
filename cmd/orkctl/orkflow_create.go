package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

func newOrkflowCreateCommand(g *globalFlags) *cobra.Command {
	var opts applyOptions
	cmd := &cobra.Command{
		Use:   "create <file|dir>...",
		Short: "Create orkflow versions from Orkflow documents",
		Long: "create uploads Orkflow documents (kind: Orkflow) to the engine, which\n" +
			"compiles, validates and stores each one as a version. Compilation errors\n" +
			"are reported by the engine.\n\n" +
			"A version is immutable: creating a document whose version is already\n" +
			"stored with the same source writes nothing, and one stored with different\n" +
			"source is refused. An edit is a new version; a document with no version:\n" +
			"is given one when it is first created.\n\n" +
			"A directory is read non-recursively for *.ork.yaml and *.ork.yml files,\n" +
			"and nothing is stored until every document has been validated. A\n" +
			"directory's activity interfaces (kind: ActivityInterface) are skipped,\n" +
			"each named on stderr; one named on the command line is refused. An\n" +
			"automation document (kind: Automation) is refused, in a directory too:\n" +
			"orkctl automation apply applies those.\n\n" +
			"--dry-run prints what would be stored and writes nothing. --check is a dry\n" +
			"run that exits 4 when a version is not stored yet.",
		Example: "  orkctl orkflow create each-fan-out.ork.yaml\n" +
			"  orkctl orkflow create orkflows/ --dry-run",
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.dryRun && opts.check {
				return fmt.Errorf("--check is a dry run already; give one of --dry-run and --check")
			}
			if len(args) == 1 && !opts.dryRun && !opts.check {
				// One file and no flag: one ApplyOrkflow, its response printed
				// whole, task tables and all.
				if info, err := os.Stat(args[0]); err != nil || !info.IsDir() {
					source, err := os.ReadFile(args[0])
					if err != nil {
						return fmt.Errorf("read orkflow file: %w", err)
					}
					if _, err := classifyDocument(args[0], source, orkflowDocument); err != nil {
						return err
					}
					return createOneOrkflow(cmd, g, source)
				}
			}
			return runApplyDocuments(cmd, g, args, opts, orkflowDocument)
		},
	}
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "print what would be stored and write nothing")
	cmd.Flags().BoolVar(&opts.check, "check", false, "a dry run that exits 4 when a version is not stored yet")
	return cmd
}

// createOneOrkflow is `orkctl orkflow create FILE`: one ApplyOrkflow, its
// response printed whole.
func createOneOrkflow(cmd *cobra.Command, g *globalFlags, source []byte) error {
	ctx, cancel := g.unaryContext(cmd)
	defer cancel()

	resp, err := g.orkflowClient().ApplyOrkflow(ctx, connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: string(source), Namespace: g.namespace},
	))
	if err != nil {
		return rpcError("create", err)
	}
	if g.json {
		return writeJSON(cmd.OutOrStdout(), resp.Msg)
	}
	return renderCreated(cmd.OutOrStdout(), resp.Msg)
}

// createdLine is the first line of a create: the version it stored, or that
// the version was already stored with this exact source.
func createdLine(resp *orkstackv1.ApplyOrkflowResponse) string {
	wf := resp.GetOrkflow()
	if !resp.GetCreated() {
		return fmt.Sprintf("Orkflow %s version %s is already stored with this source; nothing was written",
			wf.GetName(), wf.GetVersion())
	}
	return fmt.Sprintf("Created orkflow %s (version %s)", wf.GetName(), wf.GetVersion())
}

// renderCreated prints the stored orkflow and any compiler warnings.
func renderCreated(out io.Writer, resp *orkstackv1.ApplyOrkflowResponse) error {
	wf := resp.GetOrkflow()
	fmt.Fprintln(out, createdLine(resp))
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
		if spec := task.GetSleep(); spec != nil {
			// Nor has a sleeping task: how long it sleeps is not a timeout,
			// so USES names it.
			uses, timeoutCell = "sleep "+(time.Duration(spec.GetSeconds())*time.Second).String(), dash
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
