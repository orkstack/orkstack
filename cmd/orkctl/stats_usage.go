package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/enumname"
)

// usageCaveat is the sentence every usage surface prints, always. The UI
// prints the same literal (ui/src/lib/usage.ts, USAGE_CAVEAT).
const usageCaveat = "cpu-seconds and GiB-seconds are RESERVED cpu and memory × attempt time, not measured utilization."

// usageFanOut is how many namespaces `--group-by namespace` asks at once.
const usageFanOut = 4

func newStatsUsageCommand(g *globalFlags) *cobra.Command {
	var since, until, groupBy string
	cmd := &cobra.Command{
		Use:   "usage",
		Short: "Show how much cpu and memory a namespace RESERVED over a window",
		Long: "usage reports a namespace's reserved consumption over a window, from the\n" +
			"per-attempt records the Orker writes: task-seconds (how long Orkers held\n" +
			"its attempts), and cpu-seconds and GiB-seconds — each attempt's reserved\n" +
			"cpu and memory multiplied by that time. They are RESERVATIONS, not measured\n" +
			"utilization. The runs counted are the window's finished runs, the ones\n" +
			"`orkctl stats` counts as finished; a run still in flight is counted when it\n" +
			"finishes.\n\n" +
			"Records reclaimed on the run's retention schedule are reported as missing\n" +
			"evidence with a coverage boundary, never folded into a smaller number.\n" +
			"A window with more finished runs than the server reads at once is refused\n" +
			"rather than answered from a subset.\n\n" +
			"With a budget, `--since <its period>` asks the question the admission gate\n" +
			"asks, and prints the budget's share. --group-by namespace reports every\n" +
			"namespace side by side, one call each.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			req := &orkstackv1.GetUsageRequest{Namespace: g.namespace}
			if since != "" {
				window, at, err := parseSince(since)
				if err != nil {
					return err
				}
				// As `stats --since`: a duration is half a window, and the
				// server's clock is the other half.
				if window > 0 {
					req.Lookback = durationpb.New(window)
				} else {
					req.Since = timestamppb.New(at)
				}
			}
			if until != "" {
				at, err := parseUntil(until)
				if err != nil {
					return err
				}
				req.Until = timestamppb.New(at)
			}
			if groupsByNamespace(groupBy) {
				if cmd.Flags().Changed("namespace") {
					return fmt.Errorf("--group-by namespace reports every namespace; drop --namespace")
				}
				if g.json {
					return fmt.Errorf("--group-by namespace renders one GetUsage call per namespace; " +
						"for JSON, ask each with orkctl -n NS stats usage --json")
				}
				return usageByNamespace(cmd, g, req, since)
			}
			if groupBy != "" {
				dims, err := parseUsageGroupBy(groupBy)
				if err != nil {
					return err
				}
				req.GroupBy = dims
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.statsClient().GetUsage(ctx, connect.NewRequest(req))
			if err != nil {
				return rpcError("get usage", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderUsage(cmd.OutOrStdout(), resp.Msg)
		},
	}
	cmd.Flags().StringVar(&since, "since", "",
		"how far back to look: a duration such as 720h, or an RFC3339 instant "+
			"(default: the last 24 hours)")
	cmd.Flags().StringVar(&until, "until", "",
		"the window's exclusive end, an RFC3339 instant (default: now); a duration "+
			"--since is measured back from it")
	cmd.Flags().StringVar(&groupBy, "group-by", "",
		"comma-separated dimensions, in column order: orkflow, task, pool; or namespace alone, "+
			"which reports every namespace (default: the namespace total)")
	return cmd
}

// parseUsageGroupBy reads --group-by with the stats family's parser, then
// holds it to the three dimensions a reservation is attributed to.
func parseUsageGroupBy(raw string) ([]orkstackv1.FailureGroupBy, error) {
	dims, err := parseGroupBy(raw)
	if err != nil {
		return nil, err
	}
	for _, dim := range dims {
		switch dim {
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL:
		default:
			return nil, fmt.Errorf("--group-by %s: usage is grouped by orkflow, task or pool", groupBySpelling(dim))
		}
	}
	return dims, nil
}

// groupsByNamespace reports whether --group-by is the one word that fans out
// over every namespace, in ASCII case, as the dimensions are read.
func groupsByNamespace(raw string) bool {
	return enumname.UpperASCII(strings.TrimSpace(raw)) == "NAMESPACE"
}

// unitSeconds renders one of the three sums: whole seconds, no grouping.
func unitSeconds(value float64) string { return fmt.Sprintf("%.0f", value) }

func usageCell(row *orkstackv1.UsageRow, dim orkstackv1.FailureGroupBy) string {
	switch dim {
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW:
		return orDash(row.GetOrkflowName())
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK:
		return orDash(row.GetTaskId())
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL:
		return orDash(row.GetPool())
	}
	return dash
}

func usageNumbers(row *orkstackv1.UsageRow) []string {
	return []string{
		fmt.Sprint(row.GetRuns()), fmt.Sprint(row.GetTasks()), fmt.Sprint(row.GetAttempts()),
		unitSeconds(row.GetTaskSeconds()), unitSeconds(row.GetCpuSeconds()), unitSeconds(row.GetGibSeconds()),
	}
}

var usageNumberColumns = []string{"RUNS", "TASKS", "ATTEMPTS", "TASK-SECONDS", "CPU-SECONDS", "GiB-SECONDS"}

// renderUsage writes the human report. As renderStats does, it prints the
// namespace the SERVER answered for.
func renderUsage(out io.Writer, resp *orkstackv1.GetUsageResponse) error {
	header := newTable(out)
	fmt.Fprintf(header, "Namespace:\t%s\n", orDash(resp.GetNamespace()))
	fmt.Fprintf(header, "Window:\t%s to %s\n", formatTime(resp.GetWindowSince()), formatTime(resp.GetWindowUntil()))
	fmt.Fprintf(header, "Computed:\t%s\n", formatTime(resp.GetComputedAt()))
	fmt.Fprintf(header, "Grouped by:\t%s\n", usageGroupedBy(resp.GetGroupBy()))
	if err := header.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}

	fmt.Fprintln(out)
	if resp.GetRunsDispatched() == 0 {
		fmt.Fprintln(out, "No task was dispatched in this window.")
	} else {
		table := newTable(out)
		columns := make([]string, 0, len(resp.GetGroupBy())+len(usageNumberColumns))
		for _, dim := range resp.GetGroupBy() {
			columns = append(columns, strings.ToUpper(groupBySpelling(dim)))
		}
		columns = append(columns, usageNumberColumns...)
		fmt.Fprintln(table, strings.Join(columns, "\t"))
		for _, row := range resp.GetRows() {
			cells := make([]string, 0, len(columns))
			for _, dim := range resp.GetGroupBy() {
				cells = append(cells, usageCell(row, dim))
			}
			fmt.Fprintln(table, strings.Join(append(cells, usageNumbers(row)...), "\t"))
		}
		total := make([]string, 0, len(columns))
		for i := range resp.GetGroupBy() {
			if i == 0 {
				total = append(total, "TOTAL")
			} else {
				total = append(total, "")
			}
		}
		fmt.Fprintln(table, strings.Join(append(total, usageNumbers(resp.GetTotal())...), "\t"))
		if err := table.Flush(); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}

	fmt.Fprintln(out)
	fmt.Fprintf(out, "%-13s%s\n", "Budget:", usageBudgetLine(resp))
	for _, line := range usageFooter(resp) {
		fmt.Fprintln(out, line)
	}
	return nil
}

// usageGroupedBy spells the response's dimensions in the flag's vocabulary.
func usageGroupedBy(dims []orkstackv1.FailureGroupBy) string {
	if len(dims) == 0 {
		return "nothing (the namespace total)"
	}
	spellings := make([]string, 0, len(dims))
	for _, dim := range dims {
		spellings = append(spellings, groupBySpelling(dim))
	}
	return strings.Join(spellings, ", ")
}

// budgetDimension is one bounded dimension of a budget against a total.
type budgetDimension struct {
	unit, short string
	used        float64
	bound       int64
}

func budgetDimensions(budget *orkstackv1.Budget, total *orkstackv1.UsageRow) []budgetDimension {
	var dims []budgetDimension
	if bound := budget.GetCpuSeconds(); bound > 0 {
		dims = append(dims, budgetDimension{"cpu-seconds", "cpu-s", total.GetCpuSeconds(), bound})
	}
	if bound := budget.GetGibSeconds(); bound > 0 {
		dims = append(dims, budgetDimension{"GiB-seconds", "GiB-s", total.GetGibSeconds(), bound})
	}
	return dims
}

func budgetMode(budget *orkstackv1.Budget) string {
	if budget.GetEnforce() {
		return "enforced"
	}
	return "soft"
}

// usageBudgetLine is the Budget: line: the word unbounded; the budget and
// how to ask for its share outside its window; or, in it, each bounded
// dimension's share and what reaching it does.
func usageBudgetLine(resp *orkstackv1.GetUsageResponse) string {
	budget := resp.GetBudget()
	if budget == nil {
		return "unbounded"
	}
	period := budget.GetPeriod().AsDuration()
	dims := budgetDimensions(budget, resp.GetTotal())
	if !resp.GetBudgetWindow() {
		bounds := make([]string, 0, len(dims))
		for _, dim := range dims {
			bounds = append(bounds, fmt.Sprintf("%d %s", dim.bound, dim.unit))
		}
		return fmt.Sprintf("%s per %s, %s — this window is not the budget's; ask with --since %s for its share",
			strings.Join(bounds, " and "), period, budgetMode(budget), period)
	}
	shares := make([]string, 0, len(dims))
	for _, dim := range dims {
		shares = append(shares, fmt.Sprintf("%s of %d %s (%.0f%%)", unitSeconds(dim.used), dim.bound, dim.unit,
			dim.used/float64(dim.bound)*100))
	}
	mode := budgetMode(budget)
	switch {
	case budget.GetEnforce() && resp.GetBudgetReached():
		mode = "enforced: new runs are refused"
	case !budget.GetEnforce() && resp.GetBudgetReached():
		mode = "soft (reached; reported, never refuses)"
	}
	return fmt.Sprintf("%s over the last %s, %s", strings.Join(shares, "; "), period, mode)
}

// usageFooter is the sentences under the table, in order, each only when its
// condition holds. The UI prints the same ones from the same fields
// (ui/src/lib/usage.ts, usageFooter).
func usageFooter(resp *orkstackv1.GetUsageResponse) []string {
	total := resp.GetTotal()
	lines := []string{usageCaveat}
	if resp.GetRunsInWindow() > 0 {
		lines = append(lines, fmt.Sprintf(
			"scanned %d of %d finished runs in the window; %d dispatched a task; attempt records complete for %d, missing for %d",
			resp.GetRunsScanned(), resp.GetRunsInWindow(), resp.GetRunsDispatched(),
			resp.GetRunsEvidenceComplete(), resp.GetRunsEvidenceMissing()))
	}
	if missing := resp.GetRunsEvidenceMissing(); missing > 0 {
		lines = append(lines, fmt.Sprintf(
			"%d runs' attempt records were reclaimed or never written: their consumption is NOT in these numbers", missing))
	}
	if coverage := resp.GetCoverageSince(); coverage != nil {
		lines = append(lines, fmt.Sprintf(
			"coverage begins %s, the oldest run in this window with attempt records", formatTime(coverage)))
	} else if resp.GetRunsDispatched() > 0 {
		lines = append(lines,
			"no run in this window has an attempt record: every sum is zero because nothing was measured, not because nothing ran")
	}
	if n := total.GetAttemptsWithoutResources(); n > 0 {
		lines = append(lines, fmt.Sprintf(
			"%d attempts carry no reservation (recorded before this release, or refused): their time is in TASK-SECONDS, not in CPU- or GiB-SECONDS", n))
	}
	if n := total.GetUnreportedAttempts(); n > 0 {
		lines = append(lines, fmt.Sprintf(
			"%d attempts left no record (their Orker died): their consumption is unknown and not counted", n))
	}
	if n := total.GetAttemptsUnmeasured(); n > 0 {
		lines = append(lines, fmt.Sprintf(
			"%d attempt records have no usable start and finish time and are counted in nothing", n))
	}
	return append(lines, "runs still in flight are counted when they finish")
}

// usageNamespaceBudgetCell is the BUDGET cell of `--group-by namespace`: the
// worst bounded dimension's share in the budget's window, the budget alone
// outside it, or the word unbounded. The UI renders the same string
// (ui/src/lib/usage.ts, budgetCellLabel).
func usageNamespaceBudgetCell(resp *orkstackv1.GetUsageResponse) string {
	budget := resp.GetBudget()
	if budget == nil {
		return "unbounded"
	}
	period := budget.GetPeriod().AsDuration()
	dims := budgetDimensions(budget, resp.GetTotal())
	if !resp.GetBudgetWindow() {
		bounds := make([]string, 0, len(dims))
		for _, dim := range dims {
			bounds = append(bounds, fmt.Sprintf("%d %s", dim.bound, dim.short))
		}
		return fmt.Sprintf("%s per %s (window differs)", strings.Join(bounds, " + "), period)
	}
	var worst budgetDimension
	ratio := -1.0
	for _, dim := range dims {
		if r := dim.used / float64(dim.bound); r > ratio {
			worst, ratio = dim, r
		}
	}
	cell := fmt.Sprintf("%.0f%% of %d %s per %s, %s", ratio*100, worst.bound, worst.short, period, budgetMode(budget))
	if resp.GetBudgetReached() {
		cell += ", reached"
	}
	return cell
}

// usageByNamespace is `--group-by namespace`: ListNamespaces, then one
// GetUsage per namespace (the window as given, no grouping), usageFanOut at
// a time, each under its own request deadline. A namespace whose call fails
// renders its error in place of its numbers; the others still render, and
// the command exits nonzero after the table.
func usageByNamespace(cmd *cobra.Command, g *globalFlags, template *orkstackv1.GetUsageRequest, since string) error {
	ctx, cancel := g.unaryContext(cmd)
	listed, err := g.namespaceClient().ListNamespaces(ctx, connect.NewRequest(&orkstackv1.ListNamespacesRequest{}))
	cancel()
	if err != nil {
		return rpcError("list namespaces", err)
	}
	namespaces := listed.Msg.GetNamespaces()
	type answer struct {
		resp *orkstackv1.GetUsageResponse
		err  error
	}
	answers := make([]answer, len(namespaces))
	var (
		wg    sync.WaitGroup
		slots = make(chan struct{}, usageFanOut)
	)
	client := g.statsClient()
	for i, ns := range namespaces {
		wg.Add(1)
		slots <- struct{}{}
		go func(i int, name string) {
			defer wg.Done()
			defer func() { <-slots }()
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := client.GetUsage(ctx, connect.NewRequest(&orkstackv1.GetUsageRequest{
				Namespace: name, Since: template.GetSince(), Until: template.GetUntil(), Lookback: template.GetLookback(),
			}))
			if err != nil {
				answers[i] = answer{err: err}
				return
			}
			answers[i] = answer{resp: resp.Msg}
		}(i, ns.GetName())
	}
	wg.Wait()

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Window:   %s, each namespace measured on its own (see COMPUTED)\n\n", usageFanOutWindow(template, since))
	table := newTable(out)
	fmt.Fprintln(table, strings.Join(append(append([]string{"NAMESPACE"}, usageNumberColumns...), "COMPUTED", "BUDGET"), "\t"))
	var failed int
	var missingRuns, missingNamespaces int64
	for i, ns := range namespaces {
		a := answers[i]
		if a.err != nil {
			// The error stands where the budget would, behind a dash for every
			// number: a short row would end tabwriter's column block and
			// misalign every namespace after it.
			failed++
			cells := []string{ns.GetName()}
			for range len(usageNumberColumns) + 1 {
				cells = append(cells, dash)
			}
			fmt.Fprintln(table, strings.Join(append(cells, usageCallError(a.err)), "\t"))
			continue
		}
		cells := append([]string{ns.GetName()}, usageNumbers(a.resp.GetTotal())...)
		cells = append(cells, formatTime(a.resp.GetComputedAt()), usageNamespaceBudgetCell(a.resp))
		fmt.Fprintln(table, strings.Join(cells, "\t"))
		if missing := a.resp.GetRunsEvidenceMissing(); missing > 0 {
			missingRuns += missing
			missingNamespaces++
		}
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, usageCaveat)
	if missingRuns > 0 {
		noun := "namespaces"
		if missingNamespaces == 1 {
			noun = "namespace"
		}
		fmt.Fprintf(out, "%d runs across %d %s have missing attempt evidence; ask one namespace with -n NS for detail\n",
			missingRuns, missingNamespaces, noun)
	}
	if failed > 0 {
		return fmt.Errorf("get usage: %d of %d namespaces could not be measured", failed, len(namespaces))
	}
	return nil
}

// usageFanOutWindow says which window every row answers.
func usageFanOutWindow(req *orkstackv1.GetUsageRequest, since string) string {
	switch {
	case req.GetSince() != nil && req.GetUntil() != nil:
		return fmt.Sprintf("%s to %s", formatTime(req.GetSince()), formatTime(req.GetUntil()))
	case req.GetSince() != nil:
		return "since " + formatTime(req.GetSince())
	case req.GetLookback() != nil && req.GetUntil() != nil:
		return fmt.Sprintf("the %s before %s", req.GetLookback().AsDuration(), formatTime(req.GetUntil()))
	case req.GetLookback() != nil:
		return "the last " + req.GetLookback().AsDuration().String()
	case since == "" && req.GetUntil() != nil:
		return fmt.Sprintf("the %s before %s", 24*time.Hour, formatTime(req.GetUntil()))
	default:
		return "the last " + (24 * time.Hour).String()
	}
}

// usageCallError renders one failed namespace's call in place of its numbers.
func usageCallError(err error) string {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return fmt.Sprintf("error: %s: %s", connectErr.Code(), connectErr.Message())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "error: deadline_exceeded: " + err.Error()
	}
	return "error: " + err.Error()
}
