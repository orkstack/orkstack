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

// unavailable is what a number that cannot be answered renders as.
//
// It is never a zero. A blank success rate and a 0% success rate look the same
// to nobody: one says nothing finished, the other says everything failed, and
// a dashboard that spells the first as the second wakes people up.
const unavailable = "unavailable"

func newStatsCommand(g *globalFlags) *cobra.Command {
	var since string
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Summarise a namespace's runs",
		Long: "stats reports how a namespace is doing over a time window: how many runs\n" +
			"are running and how the finished ones ended, the success rate, how long\n" +
			"runs take, which orkflows fail most, and which pools have Orkers.\n\n" +
			"Every number is about the runs the window CREATED, whenever they end, so\n" +
			"the percentiles are over the same runs the success rate is a rate over.\n" +
			"A run started inside the window and still going is counted as running and\n" +
			"has no duration yet.\n\n" +
			"Every number is derived when it is asked for, from the same records\n" +
			"`orkctl run list` and `orkctl namespace get` read, so any engine replica\n" +
			"gives the same answer. Nothing here is a stored counter.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			req := &orkstackv1.GetStatsRequest{Namespace: g.namespace}
			if since != "" {
				window, at, err := parseSince(since)
				if err != nil {
					return err
				}
				// A relative window travels as a duration, so the server —
				// which owns the clock every stored time is compared against —
				// resolves both ends of it. An instant travels as itself.
				if window > 0 {
					req.Lookback = durationpb.New(window)
				} else {
					req.Since = timestamppb.New(at)
				}
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.statsClient().GetStats(ctx, connect.NewRequest(req))
			if err != nil {
				return rpcError("get stats", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderStats(cmd.OutOrStdout(), resp.Msg)
		},
	}
	cmd.Flags().StringVar(&since, "since", "",
		"how far back to look: a duration such as 6h, or an RFC3339 instant "+
			"(default: the last 24 hours)")
	cmd.AddCommand(newStatsFailuresCommand(g))
	cmd.AddCommand(newStatsFlakesCommand(g))
	cmd.AddCommand(newStatsUsageCommand(g))
	return cmd
}

// parseSince reads --since as either a lookback duration or an instant, and
// says which by returning one and leaving the other zero.
//
// Both spellings exist because both questions are ordinary: "the last six
// hours" is what somebody watching a deployment means, and an exact instant is
// what somebody comparing against an incident report means. A value that is
// neither is an error naming both forms, rather than a silent fall back to the
// default window — which would answer a question nobody asked and look right.
//
// A duration is deliberately NOT turned into an instant here. The server holds
// the clock every stored time is compared against; resolving "six hours ago"
// on this machine would pair two clocks across one window.
func parseSince(raw string) (lookback time.Duration, at time.Time, err error) {
	if window, parseErr := time.ParseDuration(raw); parseErr == nil {
		if window <= 0 {
			return 0, time.Time{}, fmt.Errorf(
				"invalid --since %q: a window has to reach backwards, e.g. 6h", raw)
		}
		return window, time.Time{}, nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02"} {
		if parsed, parseErr := time.Parse(layout, raw); parseErr == nil {
			at, err := wireInstant("--since", raw, parsed)
			return 0, at, err
		}
	}
	return 0, time.Time{}, fmt.Errorf(
		"invalid --since %q: want a duration (6h, 90m, 7d is 168h) or an instant "+
			"(2026-05-01 or 2026-05-01T09:00:00Z)", raw)
}

// renderStats writes the human report.
//
// The namespace it prints is the one the SERVER answered for, not the one this
// command was asked to act in. An empty --namespace means "the deployment's
// default", whose name this CLI deliberately does not know (see
// globalFlags.resolveNamespace), and a header reading "-" would leave a reader
// guessing which tenant the numbers below belong to.
func renderStats(out io.Writer, stats *orkstackv1.GetStatsResponse) error {
	header := newTable(out)
	fmt.Fprintf(header, "Namespace:\t%s\n", orDash(stats.GetNamespace()))
	fmt.Fprintf(header, "Window:\t%s to %s\n",
		formatTime(stats.GetWindowSince()), formatTime(stats.GetWindowUntil()))
	fmt.Fprintf(header, "Computed:\t%s\n", formatTime(stats.GetComputedAt()))
	fmt.Fprintf(header, "Success rate:\t%s\n", successRateLabel(stats))
	fmt.Fprintf(header, "Run duration:\t%s\n", durationLabel(stats))
	if err := header.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}

	fmt.Fprintln(out)
	counts := newTable(out)
	fmt.Fprintln(counts, "RUNS\tCOUNT")
	// A fixed order, not the map's: a table whose rows move between calls is
	// unreadable, and running comes first because it is the only live number.
	for _, status := range []string{"running", "succeeded", "failed", "canceled"} {
		fmt.Fprintf(counts, "%s\t%d\n", status, stats.GetRunsByStatus()[status])
	}
	for _, status := range sortedKeys(stats.GetRunsByStatus()) {
		switch status {
		case "running", "succeeded", "failed", "canceled":
			continue
		}
		fmt.Fprintf(counts, "%s\t%d\n", status, stats.GetRunsByStatus()[status])
	}
	if err := counts.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}

	fmt.Fprintln(out)
	if len(stats.GetMostFailing()) == 0 {
		fmt.Fprintln(out, "No orkflow failed in this window.")
	} else {
		failing := newTable(out)
		fmt.Fprintln(failing, "MOST FAILING\tFAILED\tFINISHED")
		for _, row := range stats.GetMostFailing() {
			fmt.Fprintf(failing, "%s\t%d\t%d\n",
				row.GetOrkflowName(), row.GetFailed(), row.GetTotal())
		}
		if err := failing.Flush(); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}

	fmt.Fprintln(out)
	if len(stats.GetRunning()) == 0 {
		fmt.Fprintln(out, "No runs are running.")
	} else {
		running := newTable(out)
		fmt.Fprintln(running, "RUNNING NOW\tORKFLOW\tSTARTED")
		for _, run := range stats.GetRunning() {
			fmt.Fprintf(running, "%s\t%s\t%s\n",
				run.GetId(), orDash(run.GetOrkflowName()), formatTime(run.GetCreatedAt()))
		}
		if err := running.Flush(); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}

	fmt.Fprintln(out)
	if len(stats.GetPools()) == 0 {
		fmt.Fprintln(out, "No pools are in scope for this namespace.")
		return nil
	}
	pools := newTable(out)
	fmt.Fprintln(pools, "POOL\tORKERS")
	for _, pool := range stats.GetPools() {
		name := pool.GetName()
		if pool.GetIsDefault() {
			name += " (default)"
		}
		fmt.Fprintf(pools, "%s\t%d\n", name, pool.GetLiveOrkerCount())
	}
	if err := pools.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// successRateLabel renders the rate, or says it is unanswerable.
func successRateLabel(stats *orkstackv1.GetStatsResponse) string {
	finished := stats.GetRunsByStatus()["succeeded"] +
		stats.GetRunsByStatus()["failed"] +
		stats.GetRunsByStatus()["canceled"]
	if !stats.GetSuccessRateDefined() {
		return unavailable + " (no run finished in this window)"
	}
	return fmt.Sprintf("%.1f%% of %d finished", stats.GetSuccessRate()*100, finished)
}

// durationLabel renders the percentiles with the coverage they were measured
// over, because a percentile without its window is a number with no meaning.
//
// The samples are the window's OWN runs that have finished — the same set
// successRateLabel counts — so the two lines beside each other describe one
// population.
func durationLabel(stats *orkstackv1.GetStatsResponse) string {
	if stats.GetDurationSampleCount() == 0 {
		return unavailable + " (no run of this window has finished inside the recorded window)"
	}
	parts := []string{
		"p50 " + stats.GetP50Duration().AsDuration().String(),
		"p95 " + stats.GetP95Duration().AsDuration().String(),
	}
	return fmt.Sprintf("%s (%d of the window's finished runs, since %s)",
		strings.Join(parts, "  "),
		stats.GetDurationSampleCount(), formatTime(stats.GetDurationCoverageSince()))
}
