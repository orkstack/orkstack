package main

import (
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The admin surface from a terminal. The UI has the same reads, but an
// operator running an incident check or a proof script has a shell, and a
// deployment fact that can only be read by opening a browser is a fact that
// does not reach a transcript.

func newAdminCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Inspect the deployment: what it is made of, and what is serving it",
	}
	cmd.AddCommand(newAdminInfoCommand(g))
	cmd.AddCommand(newAdminRetentionCommand(g))
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

// newAdminRetentionCommand groups the deployment-wide Temporal execution
// retention read and write.
//
// It lives under `admin`, NOT under `namespace`, on purpose: this is ONE
// Temporal cluster setting for the whole deployment
// (internal/engine/router.go's SingleRouter serves every OrkStack namespace
// from one binding), not a property of an orkstack.v1.Namespace. `orkctl
// namespace` manages that other, already-shipped, per-tenant registry entity
// — orkflow names, runs and pool defaults — and has nothing to do with how
// long Temporal keeps workflow history.
func newAdminRetentionCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retention",
		Short: "Read or change the deployment-wide Temporal execution retention",
		Long: "retention reads or changes how long Temporal keeps a finished workflow's\n" +
			"history for THIS DEPLOYMENT'S ONE Temporal cluster namespace.\n\n" +
			"This is a single cluster-wide setting, not a per-OrkStack-namespace one, and\n" +
			"it is unrelated to `orkctl namespace`: that command manages the OrkStack\n" +
			"registry entity owning orkflow names, runs and pool defaults, of which this\n" +
			"deployment may have several. There is exactly one Temporal namespace behind\n" +
			"the whole deployment, and this is its one retention setting.",
	}
	cmd.AddCommand(newAdminRetentionGetCommand(g))
	cmd.AddCommand(newAdminRetentionSetCommand(g))
	return cmd
}

func newAdminRetentionGetCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get",
		Short: "Read the deployment-wide Temporal execution retention, live from the cluster",
		Long: "Read the deployment-wide Temporal execution retention, live from the cluster\n" +
			"(Temporal's DescribeNamespace) — unlike `orkctl admin info`, this leaves the\n" +
			"process and asks Temporal directly.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.systemClient().GetExecutionRetention(ctx, connect.NewRequest(
				&orkstackv1.GetExecutionRetentionRequest{},
			))
			if err != nil {
				return rpcError("admin retention get", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderExecutionRetention(cmd.OutOrStdout(),
				resp.Msg.GetTemporalNamespace(), resp.Msg.GetRetention().AsDuration())
		},
	}
}

func newAdminRetentionSetCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "set <duration>",
		Short: "Change the deployment-wide Temporal execution retention",
		Long: "Change the deployment-wide Temporal execution retention, through Temporal's\n" +
			"real UpdateNamespace RPC.\n\n" +
			"<duration> uses Go's time.ParseDuration syntax (\"168h\", \"24h30m\", …), the same\n" +
			"flavour every ORK_*_TTL/ORK_*_INTERVAL environment variable in this deployment\n" +
			"uses. It must be positive: a zero or negative duration is refused before any\n" +
			"request leaves this process. Temporal may still refuse a positive value below\n" +
			"its own configured minimum, and that refusal is printed as Temporal reported\n" +
			"it, not translated or hidden.\n\n" +
			"This changes ONE setting for the WHOLE deployment's one Temporal namespace —\n" +
			"never a per-OrkStack-namespace value, and unrelated to `orkctl namespace`.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			retention, err := time.ParseDuration(args[0])
			if err != nil {
				return fmt.Errorf("parse duration %q: %w", args[0], err)
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.systemClient().UpdateExecutionRetention(ctx, connect.NewRequest(
				&orkstackv1.UpdateExecutionRetentionRequest{
					Retention: durationpb.New(retention),
				},
			))
			if err != nil {
				return rpcError("admin retention set", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderExecutionRetention(cmd.OutOrStdout(),
				resp.Msg.GetTemporalNamespace(), resp.Msg.GetRetention().AsDuration())
		},
	}
}

// renderExecutionRetention prints the deployment-wide retention read or the
// value a write just confirmed, in the same table style as `admin info`, and
// says in words that this is one cluster setting, not a per-namespace one.
func renderExecutionRetention(out io.Writer, temporalNamespace string, retention time.Duration) error {
	table := newTable(out)
	fmt.Fprintf(table, "temporal namespace:\t%s\n", orDash(temporalNamespace))
	fmt.Fprintf(table, "execution retention:\t%s\n", retention)
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, "This is ONE Temporal cluster setting for the whole deployment, not a")
	fmt.Fprintln(out, "per-OrkStack-namespace value — see `orkctl namespace` for that, unrelated,")
	fmt.Fprintln(out, "concept.")
	return nil
}
