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
			"for the pool's work right now — and the plugins from the Orkers themselves, so\n" +
			"any replica answers the same. A pool with no live Orkers is listed, and says\n" +
			"so: a task sent there waits for a worker that is not running.\n\n" +
			"PLUGINS is a sample: one of the pool's Orkers answers for the pool, and an\n" +
			"empty column means nobody was asked or nobody answered — never that these\n" +
			"Orkers run nothing. LAST SEEN is how fresh the count beside it is; a pool\n" +
			"whose Orkers have all gone quiet shows no time at all rather than the moment\n" +
			"the last one was heard.",
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
func renderPoolList(out io.Writer, listing *orkstackv1.ListPoolsResponse) error {
	pools := listing.GetPools()
	if len(pools) == 0 {
		fmt.Fprintln(out, "No pools.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "NAME\tORKERS\tPLUGINS\tDEFAULT\tLAST SEEN")
	for _, pool := range pools {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n",
			pool.GetName(),
			orkerCountLabel(pool.GetLiveOrkerCount()),
			joinOrDash(pool.GetPlugins()),
			defaultMark(pool.GetIsDefault()),
			formatTime(pool.GetLastSeen()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintf(out, "\nA task naming no pool runs on %s.\n",
		orDash(listing.GetDefaultPool()))
	return nil
}

// orkerCountLabel renders a live Orker count so zero cannot be read as healthy.
func orkerCountLabel(count int32) string {
	if count == 0 {
		return "0 (no live orkers)"
	}
	return strconv.Itoa(int(count))
}

func defaultMark(isDefault bool) string {
	if isDefault {
		return "yes"
	}
	return dash
}
