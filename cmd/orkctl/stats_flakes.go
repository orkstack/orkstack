package main

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/enumname"
)

func newStatsFlakesCommand(g *globalFlags) *cobra.Command {
	var (
		since, until, orkflow, groupBy string
		limit                          int32
	)
	cmd := &cobra.Command{
		Use:   "flakes",
		Short: "Show which tasks needed more than one attempt, across a namespace's runs",
		Long: "flakes groups the window's dispatched tasks — of every finished run, succeeded,\n" +
			"failed or canceled — and reports how often each needed more than one attempt,\n" +
			"flakiest first. A task that failed twice and passed on the third attempt makes\n" +
			"a SUCCEEDED run: invisible to `orkctl stats` and `orkctl stats failures`, and\n" +
			"counted here. The runs counted are the ones `orkctl stats` counts as finished\n" +
			"for the same window.\n\n" +
			"The numbers come from the per-attempt records the Orker writes, which are\n" +
			"reclaimed on the run's retention schedule. A run whose records are gone is\n" +
			"EXCLUDED from every rate and counted, never treated as clean; a task with no\n" +
			"record anywhere in the window reads `unavailable`, never 0%. A window with more\n" +
			"finished runs than the server reads at once is refused rather than answered\n" +
			"from a subset; narrow it with --since, --until or --orkflow.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			req := &orkstackv1.GetFlakeIndexRequest{
				Namespace:   g.namespace,
				OrkflowName: orkflow,
				Limit:       limit,
			}
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
			if groupBy != "" {
				dims, err := parseFlakeGroupBy(groupBy)
				if err != nil {
					return err
				}
				req.GroupBy = dims
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.statsClient().GetFlakeIndex(ctx, connect.NewRequest(req))
			if err != nil {
				return flakesError(err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderFlakeIndex(cmd.OutOrStdout(), resp.Msg, req.GetGroupBy())
		},
	}
	cmd.Flags().StringVar(&since, "since", "",
		"how far back to look: a duration such as 168h, or an RFC3339 instant "+
			"(default: the last 24 hours)")
	cmd.Flags().StringVar(&until, "until", "",
		"the window's exclusive end, an RFC3339 instant (default: now); a duration "+
			"--since is measured back from it")
	cmd.Flags().StringVar(&orkflow, "orkflow", "", "only this orkflow's runs")
	cmd.Flags().StringVar(&groupBy, "group-by", "",
		"comma-separated dimensions, in column order: "+strings.Join(flakeGroupBySpellings(), ", ")+
			" (default: task; orkflow is always a column)")
	cmd.Flags().Int32Var(&limit, "limit", 0, "groups to show, flakiest first (0: the server's default)")
	return cmd
}

// flakesError renders an engine with no attempt store in the words `orkctl
// run attempts` uses for it, and every other failure as rpcError does.
func flakesError(err error) error {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) && connectErr.Code() == connect.CodeFailedPrecondition &&
		strings.Contains(connectErr.Message(), "task attempt store") {
		return attemptsError(err)
	}
	return rpcError("get flake index", err)
}

// flakeRefusedDimension reports whether a dimension is one a flake group
// reports rather than is keyed by.
func flakeRefusedDimension(dim orkstackv1.FailureGroupBy) bool {
	return dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CODE ||
		dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CATEGORY
}

// flakeGroupBySpellings is every dimension the flake index accepts, in the
// enum's order: the breakdown's spellings minus code and category.
func flakeGroupBySpellings() []string {
	values := orkstackv1.FailureGroupBy(0).Descriptor().Values()
	out := make([]string, 0, values.Len())
	for i := 0; i < values.Len(); i++ {
		dim := orkstackv1.FailureGroupBy(values.Get(i).Number())
		if dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_UNSPECIFIED || flakeRefusedDimension(dim) {
			continue
		}
		out = append(out, groupBySpelling(dim))
	}
	return out
}

// parseFlakeGroupBy reads --group-by for the flake index: the breakdown's
// vocabulary, case-insensitive, order kept, with code and category refused —
// they are what a group reports as its dominant cause. An unknown or repeated
// name is an error naming every accepted spelling.
func parseFlakeGroupBy(raw string) ([]orkstackv1.FailureGroupBy, error) {
	valid := strings.Join(flakeGroupBySpellings(), ", ")
	var out []orkstackv1.FailureGroupBy
	seen := map[orkstackv1.FailureGroupBy]bool{}
	for _, part := range strings.Split(raw, ",") {
		name := enumname.UpperASCII(strings.TrimSpace(part))
		value, ok := orkstackv1.FailureGroupBy_value[groupByPrefix+name]
		dim := orkstackv1.FailureGroupBy(value)
		if !ok || name == "" || dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_UNSPECIFIED {
			return nil, fmt.Errorf("invalid --group-by %q: %q is not a dimension; want any of %s",
				raw, strings.TrimSpace(part), valid)
		}
		if flakeRefusedDimension(dim) {
			return nil, fmt.Errorf("--group-by %s: a flake group reports its dominant code and "+
				"category; it cannot be keyed by them", groupBySpelling(dim))
		}
		if seen[dim] {
			return nil, fmt.Errorf("invalid --group-by %q: %q is named twice; want each of %s at most once",
				raw, groupBySpelling(dim), valid)
		}
		seen[dim] = true
		out = append(out, dim)
	}
	return out, nil
}

// flakeCell renders one keyed dimension of one group. An empty value is a
// dash: a keyed dimension has no "unavailable" case (stats.proto, FlakeGroup).
func flakeCell(group *orkstackv1.FlakeGroup, dim orkstackv1.FailureGroupBy) string {
	switch dim {
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW:
		return orDash(group.GetOrkflowName())
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK:
		return orDash(group.GetTaskId())
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE:
		return orDash(group.GetImagePinned())
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL:
		return orDash(group.GetPool())
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE:
		return orDash(group.GetPhase())
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKER:
		return orDash(group.GetOrkerId())
	}
	return dash
}

// flakeRate renders a group's retry rate, or `unavailable` when no run in the
// window has a record of it — never 0%.
func flakeRate(group *orkstackv1.FlakeGroup) string {
	if !group.GetRetryRateDefined() {
		return unavailable
	}
	return fmt.Sprintf("%.1f%%", group.GetRetryRate()*100)
}

// flakeAttemptsPerRun renders the nearest-rank attempts per run.
func flakeAttemptsPerRun(group *orkstackv1.FlakeGroup) string {
	if !group.GetRetryRateDefined() {
		return dash
	}
	return fmt.Sprintf("p50 %d · p95 %d", group.GetP50Attempts(), group.GetP95Attempts())
}

// flakeCause renders the dominant cause as one triple with its share:
// `CATEGORY CODE phase (count/retries)`, each empty component a dash.
func flakeCause(group *orkstackv1.FlakeGroup) string {
	if group.GetRetryAttempts() == 0 {
		return dash
	}
	return fmt.Sprintf("%s %s %s (%d/%d)", categoryLabel(group.GetDominantCategory()),
		orDash(group.GetDominantCode()), orDash(group.GetDominantPhase()),
		group.GetDominantCount(), group.GetRetryAttempts())
}

// flakeGroupedBy spells the dimensions for the header in the flag's
// vocabulary, omitting the implied orkflow when the caller did not name it,
// so the line can be pasted back as --group-by.
func flakeGroupedBy(resp *orkstackv1.GetFlakeIndexResponse, requested []orkstackv1.FailureGroupBy) string {
	named := false
	for _, dim := range requested {
		if dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW {
			named = true
		}
	}
	spellings := make([]string, 0, len(resp.GetGroupBy()))
	for _, dim := range resp.GetGroupBy() {
		if dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW && !named {
			continue
		}
		spellings = append(spellings, groupBySpelling(dim))
	}
	if len(spellings) == 0 {
		// Only orkflow, implied: the caller asked for it by naming it alone.
		return groupBySpelling(orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW)
	}
	return strings.Join(spellings, ", ")
}

// renderFlakeIndex writes the human report. requested is the group_by the
// request carried, which decides whether the implied orkflow is spelled in the
// header. As renderStats does, it prints the namespace the SERVER answered for.
func renderFlakeIndex(out io.Writer, resp *orkstackv1.GetFlakeIndexResponse, requested []orkstackv1.FailureGroupBy) error {
	header := newTable(out)
	fmt.Fprintf(header, "Namespace:\t%s\n", orDash(resp.GetNamespace()))
	fmt.Fprintf(header, "Window:\t%s to %s\n",
		formatTime(resp.GetWindowSince()), formatTime(resp.GetWindowUntil()))
	fmt.Fprintf(header, "Computed:\t%s\n", formatTime(resp.GetComputedAt()))
	fmt.Fprintf(header, "Grouped by:\t%s\n", flakeGroupedBy(resp, requested))
	if err := header.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}

	fmt.Fprintln(out)
	if len(resp.GetGroups()) == 0 {
		fmt.Fprintln(out, "No task was dispatched in this window.")
	} else {
		table := newTable(out)
		columns := make([]string, 0, len(resp.GetGroupBy())+10)
		for _, dim := range resp.GetGroupBy() {
			columns = append(columns, strings.ToUpper(groupBySpelling(dim)))
		}
		columns = append(columns, "RUNS", "RETRIED", "RATE", "ATTEMPTS/RUN", "RETRIES", "UNREPORTED",
			"DOMINANT CAUSE", "LAST RETRIED", "EXAMPLE")
		fmt.Fprintln(table, strings.Join(columns, "\t"))
		for _, group := range resp.GetGroups() {
			cells := make([]string, 0, len(columns))
			for _, dim := range resp.GetGroupBy() {
				cells = append(cells, flakeCell(group, dim))
			}
			example := dash
			if ids := group.GetExampleRunIds(); len(ids) > 0 {
				example = ids[0]
			}
			cells = append(cells,
				fmt.Sprint(group.GetRunsObserved()),
				fmt.Sprint(group.GetRunsRetried()),
				flakeRate(group),
				flakeAttemptsPerRun(group),
				fmt.Sprint(group.GetRetryAttempts()),
				fmt.Sprint(group.GetUnreportedAttempts()),
				flakeCause(group),
				formatTime(group.GetLastRetriedAt()),
				example)
			fmt.Fprintln(table, strings.Join(cells, "\t"))
		}
		if err := table.Flush(); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}

	fmt.Fprintln(out)
	for _, line := range flakeFooter(resp) {
		fmt.Fprintln(out, line)
	}
	return nil
}

// flakeFooter is the sentences under the table, in order. The UI prints the
// same ones from the same fields (ui/src/lib/flakes.ts, flakeFooter).
func flakeFooter(resp *orkstackv1.GetFlakeIndexResponse) []string {
	dispatched, complete, missing := resp.GetRunsDispatched(), resp.GetRunsEvidenceComplete(), resp.GetRunsEvidenceMissing()
	lines := []string{fmt.Sprintf(
		"scanned %d of %d finished runs in the window; %d dispatched a task; attempt records complete for %d, missing for %d",
		resp.GetRunsScanned(), resp.GetRunsInWindow(), dispatched, complete, missing)}
	if missing > 0 {
		lines = append(lines, fmt.Sprintf(
			"%d runs' attempt records were reclaimed or never written: they are EXCLUDED from every rate above, not counted as clean",
			missing))
	}
	if shown := int64(len(resp.GetGroups())); shown < resp.GetGroupsTotal() {
		lines = append(lines, fmt.Sprintf("showing %d of %d groups (raise --limit)", shown, resp.GetGroupsTotal()))
	}
	if dispatched > 0 && complete == 0 {
		lines = append(lines, "retry rates are unavailable: no run in this window has an attempt record")
	}
	if !resp.GetBodyKeyed() {
		lines = append(lines,
			"unreported and evidence-missing counts are totals only: a group keyed by orker, phase or image cannot attribute them")
	}
	return lines
}
