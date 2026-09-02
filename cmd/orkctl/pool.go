package main

import (
	"fmt"
	"io"
	"strconv"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func newPoolCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pool",
		Short: "Inspect the pools a namespace may dispatch onto",
	}
	cmd.AddCommand(newPoolListCommand(g))
	return cmd
}

func newPoolListCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List the pools in scope, with the Orkers serving each",
		Long: "List the pools this namespace may dispatch onto.\n\n" +
			"A pool is a set of Orkers. The counts come from the cluster — who is polling\n" +
			"for the pool's work right now — so any replica answers the same. A pool with\n" +
			"no live Orkers is listed, and says so: a task sent there waits for a worker\n" +
			"that is not running.\n\n" +
			"This read NARROWS and never refuses. A pool the cluster could not answer for\n" +
			"is still listed, with `unknown (unreachable)` where its count would be and the\n" +
			"reason printed under the table — one unreachable pool must not hide the\n" +
			"healthy ones. `unknown` and `0` are different answers: 0 means start an Orker,\n" +
			"unknown means ask again.\n\n" +
			"Which activities a pool can run is engine configuration, not something an\n" +
			"Orker reports: use `orkctl activity list` for the catalog; this command\n" +
			"answers who is available to run it.\n\n" +
			"LAST SEEN is how fresh the count beside it is; a pool whose Orkers have all\n" +
			"gone quiet shows no time at all rather than the moment the last one was\n" +
			"heard.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.namespaceClient().ListPools(ctx, connect.NewRequest(
				&orkstackv1.ListPoolsRequest{Namespace: g.namespace},
			))
			if err != nil {
				return rpcError("list pools", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderPoolList(cmd.OutOrStdout(), resp.Msg)
		},
	}
}

// renderPoolList prints pool health as a table.
//
// The Orker count carries its meaning with it: `0 (no live orkers)` rather
// than `0`, because a lone zero in a healthy-looking row is exactly what a
// reader skims past, and it is the one number this command exists to show.
//
// A pool the cluster could not be asked about is a THIRD state, and it prints
// as one. Its count is `unknown (unreachable)`, never 0: the two mean opposite
// things to the person reading — start an Orker, or wait and ask again — and
// this command's whole job is that number. The reasons follow the table rather
// than crowding a column, one line each, so a timeout is distinguishable from
// a dead cluster without leaving the terminal.
func renderPoolList(out io.Writer, listing *orkstackv1.ListPoolsResponse) error {
	pools := listing.GetPools()
	if len(pools) == 0 {
		fmt.Fprintln(out, "No pools.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "NAME\tORKERS\tDEFAULT\tLAST SEEN")
	var unreachable []*orkstackv1.Pool
	for _, pool := range pools {
		if pool.GetUnavailableReason() != "" {
			unreachable = append(unreachable, pool)
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n",
			pool.GetName(),
			poolOrkerLabel(pool),
			defaultMark(pool.GetIsDefault()),
			formatTime(pool.GetLastSeen()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintf(out, "\nA task naming no pool runs on %s.\n",
		orDash(listing.GetDefaultPool()))
	if len(unreachable) > 0 {
		fmt.Fprintf(out, "\n%s could not be reached, so its Orkers are unknown "+
			"rather than zero:\n", poolCount(len(unreachable)))
		for _, pool := range unreachable {
			fmt.Fprintf(out, "  %s: %s\n", pool.GetName(), pool.GetUnavailableReason())
		}
	}
	return nil
}

// poolOrkerLabel renders a pool's Orker count so that neither zero nor a
// missing measurement can be read as healthy — and so the two cannot be read
// as each other.
func poolOrkerLabel(pool *orkstackv1.Pool) string {
	if pool.GetUnavailableReason() != "" {
		return "unknown (unreachable)"
	}
	if pool.GetLiveOrkerCount() == 0 {
		return "0 (no live orkers)"
	}
	return strconv.Itoa(int(pool.GetLiveOrkerCount()))
}

func poolCount(n int) string {
	if n == 1 {
		return "1 pool"
	}
	return fmt.Sprintf("%d pools", n)
}

func defaultMark(isDefault bool) string {
	if isDefault {
		return "yes"
	}
	return dash
}
