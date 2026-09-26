package main

import (
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const (
	groupByPrefix  = "FAILURE_GROUP_BY_"
	categoryPrefix = "FAILURE_CATEGORY_"
)

func newStatsFailuresCommand(g *globalFlags) *cobra.Command {
	var (
		since, until, orkflow, groupBy, category string
		limit                                    int32
	)
	cmd := &cobra.Command{
		Use:   "failures",
		Short: "Group a namespace's failed runs by what failed and why",
		Long: "failures groups the window's failed runs by fingerprint — which orkflow,\n" +
			"which task failed first, its code and failure category, and on request the\n" +
			"image the final attempt ran, the pool, the Orker and the phase it ended in —\n" +
			"worst first. The runs counted are the same ones `orkctl stats` counts as\n" +
			"failed for the same window.\n\n" +
			"Image and phase come from attempt records, which are reclaimed on the run's\n" +
			"retention schedule: a group whose evidence is gone says `unavailable`, never\n" +
			"a blank. A window with more failed runs than the server reads at once is\n" +
			"refused rather than answered from a subset; narrow it with --since, --until\n" +
			"or --orkflow.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			req := &orkstackv1.GetFailureBreakdownRequest{
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
				dims, err := parseGroupBy(groupBy)
				if err != nil {
					return err
				}
				req.GroupBy = dims
			}
			if category != "" {
				parsed, err := parseFailureCategory(category)
				if err != nil {
					return err
				}
				req.Category = parsed
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.statsClient().GetFailureBreakdown(ctx, connect.NewRequest(req))
			if err != nil {
				return rpcError("get failure breakdown", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderFailureBreakdown(cmd.OutOrStdout(), resp.Msg, req.GetCategory())
		},
	}
	cmd.Flags().StringVar(&since, "since", "",
		"how far back to look: a duration such as 6h, or an RFC3339 instant "+
			"(default: the last 24 hours)")
	cmd.Flags().StringVar(&until, "until", "",
		"the window's exclusive end, an RFC3339 instant (default: now); a duration "+
			"--since is measured back from it")
	cmd.Flags().StringVar(&orkflow, "orkflow", "", "only this orkflow's failed runs")
	cmd.Flags().StringVar(&groupBy, "group-by", "",
		"comma-separated dimensions, in column order: "+strings.Join(groupBySpellings(), ", ")+
			" (default: orkflow,task,code,category)")
	cmd.Flags().StringVar(&category, "category", "",
		"only failures of one category: "+strings.Join(categorySpellings(), ", "))
	cmd.Flags().Int32Var(&limit, "limit", 0, "groups to show, worst first (0: the server's default)")
	return cmd
}

// parseUntil reads --until as an instant, in the layouts --since accepts for
// one. A duration is refused: "until six hours" names no instant.
func parseUntil(raw string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf(
		"invalid --until %q: want an instant (2026-05-01 or 2026-05-01T09:00:00Z)", raw)
}

// groupBySpelling is a dimension in the flag's vocabulary: the enum value's
// name without its prefix, lower case.
func groupBySpelling(dim orkstackv1.FailureGroupBy) string {
	if dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_UNSPECIFIED {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(dim.String(), groupByPrefix))
}

// groupBySpellings is every dimension's spelling, in the enum's order. It is
// derived from the generated enum, so a dimension added to the proto has a
// spelling without an edit here.
func groupBySpellings() []string {
	values := orkstackv1.FailureGroupBy(0).Descriptor().Values()
	out := make([]string, 0, values.Len())
	for i := 0; i < values.Len(); i++ {
		if spelling := groupBySpelling(orkstackv1.FailureGroupBy(values.Get(i).Number())); spelling != "" {
			out = append(out, spelling)
		}
	}
	return out
}

// parseGroupBy reads --group-by: comma-separated, case-insensitive, order
// kept. An unknown or repeated name is an error naming every spelling.
func parseGroupBy(raw string) ([]orkstackv1.FailureGroupBy, error) {
	valid := func() string { return strings.Join(groupBySpellings(), ", ") }
	var out []orkstackv1.FailureGroupBy
	seen := map[orkstackv1.FailureGroupBy]bool{}
	for _, part := range strings.Split(raw, ",") {
		name := strings.ToUpper(strings.TrimSpace(part))
		value, ok := orkstackv1.FailureGroupBy_value[groupByPrefix+name]
		dim := orkstackv1.FailureGroupBy(value)
		if !ok || name == "" || dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_UNSPECIFIED {
			return nil, fmt.Errorf("invalid --group-by %q: %q is not a dimension; want any of %s",
				raw, strings.TrimSpace(part), valid())
		}
		if seen[dim] {
			return nil, fmt.Errorf("invalid --group-by %q: %q is named twice; want each of %s at most once",
				raw, groupBySpelling(dim), valid())
		}
		seen[dim] = true
		out = append(out, dim)
	}
	return out, nil
}

// categorySpellings is every failure category in the flag's vocabulary.
func categorySpellings() []string {
	values := orkstackv1.FailureCategory(0).Descriptor().Values()
	out := make([]string, 0, values.Len())
	for i := 0; i < values.Len(); i++ {
		category := orkstackv1.FailureCategory(values.Get(i).Number())
		if category == orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
			continue
		}
		out = append(out, strings.ToLower(strings.TrimPrefix(category.String(), categoryPrefix)))
	}
	return out
}

// parseFailureCategory reads --category, case-insensitive, with or without
// the enum's prefix. CANCELLED is the enum's own spelling, two Ls — unlike
// the run status CANCELED — and only that spelling is accepted.
func parseFailureCategory(raw string) (orkstackv1.FailureCategory, error) {
	name := strings.ToUpper(strings.TrimSpace(raw))
	name = strings.TrimPrefix(name, categoryPrefix)
	value, ok := orkstackv1.FailureCategory_value[categoryPrefix+name]
	if !ok || value == int32(orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED) {
		return 0, fmt.Errorf("invalid --category %q: want one of %s",
			raw, strings.Join(categorySpellings(), ", "))
	}
	return orkstackv1.FailureCategory(value), nil
}

// categoryLabel is a category in the JSON dialect's short form, or a dash.
func categoryLabel(category orkstackv1.FailureCategory) string {
	if category == orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
		return dash
	}
	return strings.TrimPrefix(category.String(), categoryPrefix)
}

// failureCell renders one dimension of one group.
//
// An empty IMAGE or PHASE in a group whose attempt evidence is missing is
// `unavailable` — the record was reclaimed or never written. Every other empty
// value is a dash: the snapshot genuinely carries none. The two are distinct
// facts (stats.proto, FailureGroup) and must not render alike.
func failureCell(group *orkstackv1.FailureGroup, dim orkstackv1.FailureGroupBy) string {
	var value string
	switch dim {
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW:
		value = group.GetOrkflowName()
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK:
		value = group.GetTaskId()
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CODE:
		value = group.GetCode()
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CATEGORY:
		return categoryLabel(group.GetCategory())
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE:
		value = group.GetImagePinned()
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL:
		value = group.GetPool()
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE:
		value = group.GetPhase()
	case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKER:
		value = group.GetOrkerId()
	}
	if value == "" && group.GetAttemptEvidenceMissing() > 0 &&
		(dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE ||
			dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE) {
		return unavailable
	}
	return orDash(value)
}

// plural renders "1 run" / "2 runs".
func plural(n int64, noun string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

// renderFailureBreakdown writes the human report. category is the one the
// request asked for, which the response does not echo.
//
// As renderStats does, it prints the namespace the SERVER answered for.
func renderFailureBreakdown(
	out io.Writer,
	resp *orkstackv1.GetFailureBreakdownResponse,
	category orkstackv1.FailureCategory,
) error {
	spellings := make([]string, len(resp.GetGroupBy()))
	for i, dim := range resp.GetGroupBy() {
		spellings[i] = groupBySpelling(dim)
	}
	header := newTable(out)
	fmt.Fprintf(header, "Namespace:\t%s\n", orDash(resp.GetNamespace()))
	fmt.Fprintf(header, "Window:\t%s to %s\n",
		formatTime(resp.GetWindowSince()), formatTime(resp.GetWindowUntil()))
	fmt.Fprintf(header, "Computed:\t%s\n", formatTime(resp.GetComputedAt()))
	fmt.Fprintf(header, "Grouped by:\t%s\n", strings.Join(spellings, ", "))
	if err := header.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}

	fmt.Fprintln(out)
	filtered := category != orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED
	switch {
	case len(resp.GetGroups()) == 0 && filtered:
		fmt.Fprintf(out, "No failed run in this window has category %s.\n", categoryLabel(category))
	case len(resp.GetGroups()) == 0:
		fmt.Fprintln(out, "No run failed in this window.")
	default:
		table := newTable(out)
		columns := make([]string, 0, len(spellings)+4)
		for _, spelling := range spellings {
			columns = append(columns, strings.ToUpper(spelling))
		}
		columns = append(columns, "RUNS", "FIRST", "LAST", "EXAMPLE")
		fmt.Fprintln(table, strings.Join(columns, "\t"))
		for _, group := range resp.GetGroups() {
			cells := make([]string, 0, len(columns))
			for _, dim := range resp.GetGroupBy() {
				cells = append(cells, failureCell(group, dim))
			}
			example := dash
			if ids := group.GetExampleRunIds(); len(ids) > 0 {
				example = ids[0]
			}
			cells = append(cells, fmt.Sprint(group.GetRunCount()),
				formatTime(group.GetFirstSeen()), formatTime(group.GetLastSeen()), example)
			fmt.Fprintln(table, strings.Join(cells, "\t"))
		}
		if err := table.Flush(); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}

	fmt.Fprintln(out)
	for _, line := range failureFooter(resp, category) {
		fmt.Fprintln(out, line)
	}
	return nil
}

// failureFooter is the sentences under the table, in order. The UI prints the
// same ones from the same fields.
func failureFooter(resp *orkstackv1.GetFailureBreakdownResponse, category orkstackv1.FailureCategory) []string {
	lines := []string{fmt.Sprintf("scanned %d of %d failed runs in the window; %d grouped into %s",
		resp.GetFailedRunsScanned(), resp.GetFailedRunsInWindow(), resp.GetFailedRunsGrouped(),
		plural(resp.GetGroupsTotal(), "group"))}
	if shown := int64(len(resp.GetGroups())); shown < resp.GetGroupsTotal() {
		lines = append(lines, fmt.Sprintf("showing %d of %d groups (raise --limit)", shown, resp.GetGroupsTotal()))
	}
	if category != orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
		lines = append(lines, fmt.Sprintf("category %s: %d of %d failed runs",
			categoryLabel(category), resp.GetFailedRunsGrouped(), resp.GetFailedRunsScanned()))
	}
	if missing := resp.GetAttemptEvidenceMissing(); missing > 0 {
		var groups int64
		for _, group := range resp.GetGroups() {
			if group.GetAttemptEvidenceMissing() > 0 {
				groups++
			}
		}
		lines = append(lines, fmt.Sprintf(
			"image and phase unavailable for %s: attempt records for %s were reclaimed or never written",
			plural(groups, "group"), plural(missing, "run")))
	}
	return lines
}
