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
	cmd.AddCommand(newAdminImagePolicyCommand(g))
	cmd.AddCommand(newAdminAuditCommand(g))
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

// newAdminRetentionCommand groups the deployment's maximum run retention read
// and write.
//
// It lives under `admin`, NOT under `namespace`, on purpose: this is the
// deployment-wide ceiling, not a property of any one orkstack.v1.Namespace.
// Every namespace's own run retention (`orkctl namespace update
// --run-retention`) is bounded by it, and a namespace that sets none
// inherits it.
func newAdminRetentionCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retention",
		Short: "Read or change the deployment's maximum run retention",
		Long: "retention reads or changes how long a finished run may be kept, at most, in\n" +
			"this deployment. Every namespace's own run retention (`orkctl namespace\n" +
			"update --run-retention`) is bounded by it, and a namespace that sets none\n" +
			"inherits it.",
	}
	cmd.AddCommand(newAdminRetentionGetCommand(g))
	cmd.AddCommand(newAdminRetentionSetCommand(g))
	return cmd
}

func newAdminRetentionGetCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get",
		Short: "Read the deployment's maximum run retention",
		Long: "Read the deployment's maximum run retention from the store, so any replica\n" +
			"answers the same.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.systemClient().GetRetentionPolicy(ctx, connect.NewRequest(
				&orkstackv1.GetRetentionPolicyRequest{},
			))
			if err != nil {
				return rpcError("admin retention get", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderRetentionPolicy(cmd.OutOrStdout(), resp.Msg)
		},
	}
}

func newAdminRetentionSetCommand(g *globalFlags) *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "set <duration>",
		Short: "Change the deployment's maximum run retention",
		Long: "Change the deployment's maximum run retention.\n\n" +
			"<duration> uses Go's time.ParseDuration syntax (\"168h\", \"24h30m\", …), the same\n" +
			"flavour every ORK_*_TTL/ORK_*_INTERVAL environment variable in this deployment\n" +
			"uses. It must be positive: a zero or negative duration is refused before any\n" +
			"request leaves this process.\n\n" +
			"Lowering it below a namespace's own run retention is refused and names that\n" +
			"namespace — lower the namespace first (`orkctl namespace update\n" +
			"--run-retention`). Namespaces that set no run retention follow the new maximum\n" +
			"at once: finished runs already older than it are retired on the next cleanup\n" +
			"pass.\n\n" +
			"Lowering it reads the current maximum, states what the next cleanup pass\n" +
			"removes and asks for confirmation; it refuses when standard input is not a\n" +
			"terminal unless --yes is given. Raising it asks nothing.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			retention, err := time.ParseDuration(args[0])
			if err != nil {
				return fmt.Errorf("parse duration %q: %w", args[0], err)
			}
			if retention <= 0 {
				// Refused HERE, before a context is created or the Connect
				// client is even asked for — nothing reaches the wire for a
				// non-positive duration. The handler in internal/api also
				// rejects one (it must, since it cannot trust a caller that
				// isn't this CLI), but that is defense in depth, not where
				// this promise is kept for `orkctl`.
				return fmt.Errorf("retention must be a positive duration, got %s", retention)
			}
			if !yes {
				if err := confirmRetentionLowering(cmd, g, retention); err != nil {
					return err
				}
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.systemClient().UpdateRetentionPolicy(ctx, connect.NewRequest(
				&orkstackv1.UpdateRetentionPolicyRequest{
					MaxRunRetention: durationpb.New(retention),
				},
			))
			if err != nil {
				return rpcError("admin retention set", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderRetentionPolicy(cmd.OutOrStdout(), resp.Msg)
		},
	}
	addYesFlag(cmd, &yes)
	return cmd
}

// confirmRetentionLowering reads the maximum in force and, only when the new
// one is lower, states what the next cleanup pass removes and asks. Raising
// the maximum, or setting the value it already has, loses nothing.
func confirmRetentionLowering(cmd *cobra.Command, g *globalFlags, retention time.Duration) error {
	ctx, cancel := g.unaryContext(cmd)
	defer cancel()
	read, err := g.systemClient().GetRetentionPolicy(ctx, connect.NewRequest(&orkstackv1.GetRetentionPolicyRequest{}))
	if err != nil {
		return rpcError("admin retention set: read the current maximum", err)
	}
	current := read.Msg.GetPolicy().GetMaxRunRetention().AsDuration()
	if retention >= current {
		return nil
	}
	prompt := fmt.Sprintf("Lowering the maximum run retention from %s to %s: every finished run older than %s\n"+
		"in a namespace without its own run retention loses its logs, attempt records, snapshots and commits\n"+
		"on the next cleanup pass.",
		shortDuration(current), shortDuration(retention), shortDuration(retention))
	return confirmDestructive(cmd, g, "lower the maximum run retention", prompt, false)
}

// retentionPolicyMessage is the shape GetRetentionPolicyResponse and
// UpdateRetentionPolicyResponse share, so one renderer serves both commands.
type retentionPolicyMessage interface {
	GetPolicy() *orkstackv1.RetentionPolicy
	GetNamespaceRetentionEnforced() bool
}

// renderRetentionPolicy prints the deployment's maximum run retention read or
// the value a write just confirmed, in the same table style as `admin info`.
func renderRetentionPolicy(out io.Writer, resp retentionPolicyMessage) error {
	table := newTable(out)
	fmt.Fprintf(table, "maximum run retention:\t%s\n", resp.GetPolicy().GetMaxRunRetention().AsDuration())
	if resp.GetNamespaceRetentionEnforced() {
		fmt.Fprintf(table, "namespace retention:\tenforced\n")
	} else {
		fmt.Fprintf(table, "namespace retention:\tnot enforced (ORK_CLEANUP_ENABLED=false: every run is kept\n"+
			"\tfor the maximum plus the enforcement margin)\n")
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, "Each namespace may keep runs for less — see `orkctl namespace update")
	fmt.Fprintln(out, "--run-retention`.")
	return nil
}
