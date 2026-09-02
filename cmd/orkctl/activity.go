package main

import (
	"fmt"
	"io"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// newActivityCommand inspects the deployment's activity catalog. An activity
// is an OCI image and which images exist is engine configuration (§4.2), so
// this asks the engine, never an Orker.
func newActivityCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "activity",
		Short: "Inspect the activities this deployment can run",
	}
	cmd.AddCommand(newActivityListCommand(g))
	cmd.AddCommand(newActivityTestCommand(g))
	cmd.AddCommand(newActivityBuildCommand(g))
	return cmd
}

func newActivityListCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List the deployment's activity catalog",
		Long: "List the activities this deployment can run.\n\n" +
			"An activity is an OCI image. The catalog maps a short name — \"shell\" — to a\n" +
			"registry reference and the index digest it resolved to, so what a name will\n" +
			"actually run is visible before a run pins it.\n\n" +
			"This reads engine configuration and never asks an Orker: an Orker executes\n" +
			"images, it does not decide which ones exist. Use `orkctl pool list` to ask\n" +
			"whether a pool has Orkers to run them.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.activityClient().ListActivities(ctx, connect.NewRequest(
				&orkstackv1.ListActivitiesRequest{},
			))
			if err != nil {
				return rpcError("list activities", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderActivityList(cmd.OutOrStdout(), resp.Msg.GetActivities())
		},
	}
}

func renderActivityList(out io.Writer, activities []*orkstackv1.ActivityInfo) error {
	if len(activities) == 0 {
		fmt.Fprintln(out, "No activities in this deployment's catalog.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "NAME\tREFERENCE\tDIGEST\tPLATFORMS")
	for _, activity := range activities {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n",
			activity.GetName(),
			activity.GetReference(),
			shortDigest(activity.GetIndexDigest()),
			joinOrDash(activity.GetPlatforms()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// shortDigest renders sha256:<64 hex> as sha256:<first 12>, which is what an
// operator compares by eye. The full value is in --json.
func shortDigest(digest string) string {
	const shown = len("sha256:") + 12
	if len(digest) <= shown {
		if digest == "" {
			return "-"
		}
		return digest
	}
	return digest[:shown] + "…"
}
