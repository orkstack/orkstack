package main

import (
	"fmt"
	"io"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func newPluginCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "Inspect the plugins the engine knows about",
	}
	cmd.AddCommand(newPluginListCommand(g))
	return cmd
}

func newPluginListCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List registered plugins",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.pluginClient().ListPlugins(ctx, connect.NewRequest(
				&orkstackv1.ListPluginsRequest{},
			))
			if err != nil {
				return rpcError("list plugins", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderPluginList(cmd.OutOrStdout(), resp.Msg.GetPlugins())
		},
	}
}

func renderPluginList(out io.Writer, plugins []*orkstackv1.PluginInfo) error {
	if len(plugins) == 0 {
		fmt.Fprintln(out, "No plugins.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "NAME\tVERSION\tDESCRIPTION")
	for _, info := range plugins {
		fmt.Fprintf(table, "%s\t%s\t%s\n",
			info.GetName(), orDash(info.GetVersion()), orDash(info.GetDescription()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
