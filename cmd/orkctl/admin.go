package main

import (
	"fmt"
	"io"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The admin surface from a terminal. The UI has the same four reads, but an
// operator running an incident check or a proof script has a shell, and a
// deployment fact that can only be read by opening a browser is a fact that
// does not reach a transcript.

func newAdminCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Inspect the deployment: what it is made of, and what is serving it",
	}
	cmd.AddCommand(newAdminInfoCommand(g))
	return cmd
}

func newAdminInfoCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "info",
		Short: "Describe the engine replica that answers",
		Long: "Describe the engine replica that answers.\n\n" +
			"Every value except the server time is read from the replica's own build and\n" +
			"configuration, not observed from the cluster. Two replicas of one release\n" +
			"therefore answer identically; mid-rollout the build id is what differs, which\n" +
			"is the field to compare. Behind a load balancer this describes whichever\n" +
			"replica took the request.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.systemClient().GetSystemInfo(ctx, connect.NewRequest(
				&orkstackv1.GetSystemInfoRequest{},
			))
			if err != nil {
				return rpcError("admin info", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderSystemInfo(cmd.OutOrStdout(), resp.Msg)
		},
	}
}

// renderSystemInfo prints the deployment summary.
//
// Two renderings are decided here rather than left to the raw fields. An empty
// build id prints as `off`, because empty is the documented off switch for
// Worker Deployment Versioning and a blank cell would read as missing data on
// the one surface an operator checks during a rollout. And the provenance line
// at the end says the values are configuration, so nothing in this table is
// mistaken for something measured.
func renderSystemInfo(out io.Writer, info *orkstackv1.GetSystemInfoResponse) error {
	table := newTable(out)
	fmt.Fprintf(table, "engine:\t%s\n", orDash(info.GetEngineVersion()))
	fmt.Fprintf(table, "go:\t%s\n", orDash(info.GetGoVersion()))
	fmt.Fprintf(table, "deployment:\t%s\n", orDash(info.GetDeploymentName()))
	fmt.Fprintf(table, "versioning:\t%s\n", versioningLabel(info))
	fmt.Fprintf(table, "artifacts:\t%s (%s)\n",
		orDash(info.GetArtifactLocation()), orDash(info.GetArtifactBackend()))
	fmt.Fprintf(table, "temporal:\t%s\n", orDash(info.GetTemporalAddress()))
	fmt.Fprintf(table, "server time:\t%s\n", formatTime(info.GetServerTime()))
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, "Everything above except the server time is this replica's own "+
		"configuration and build,")
	fmt.Fprintln(out, "not a measurement of the cluster.")
	return nil
}

// versioningLabel renders the versioning switch as the state it names.
func versioningLabel(info *orkstackv1.GetSystemInfoResponse) string {
	if !info.GetVersioningActive() {
		return "off"
	}
	return "on (build " + info.GetBuildId() + ")"
}
