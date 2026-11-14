package main

import (
	"fmt"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

func (g *globalFlags) accessClient() orkstackv1connect.AccessServiceClient {
	return orkstackv1connect.NewAccessServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

// newAdminAuthCommand groups the deployment's authentication mode
// (docs/AUTHENTICATION.md, "The mode record").
func newAdminAuthCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Read or lower the deployment's authentication mode",
		Long: "auth reads the deployment's authentication mode and lowers it. The mode record\n" +
			"holds the highest mode the deployment runs; an engine does not start beneath\n" +
			"it, so a mode comes down by a command and a restart, never by one replica's\n" +
			"configuration.",
	}
	cmd.AddCommand(newAdminAuthStatusCommand(g), newAdminAuthLowerCommand(g))
	return cmd
}

func newAdminAuthStatusCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show the authentication mode, the mode record and the engines that do not enforce",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.accessClient().GetAccess(ctx, connect.NewRequest(&orkstackv1.GetAccessRequest{}))
			if err != nil {
				return rpcError("admin auth status", err)
			}
			st := resp.Msg.GetStatus()
			if g.json {
				return writeJSON(cmd.OutOrStdout(), st)
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "engine mode:    %s\n", st.GetEngineMode())
			recorded := st.GetRecordedMode()
			if recorded == "" {
				recorded = "(none)"
			}
			fmt.Fprintf(out, "recorded mode:  %s\n", recorded)
			if st.GetModeRecordMissing() {
				fmt.Fprintln(out, "                the record this engine saw went missing; it wrote it again")
			}
			if st.GetDocumentAge() != nil {
				stale := ""
				if st.GetDocumentStale() {
					stale = " (stale: the store did not answer)"
				}
				fmt.Fprintf(out, "access document: read %s ago%s\n", st.GetDocumentAge().AsDuration().Round(1e9), stale)
			}
			if t := st.GetAssertedSafeFrom(); t != nil {
				fmt.Fprintf(out, "asserted safe:  from %s\n", t.AsTime().Format("2006-01-02T15:04:05Z07:00"))
			}
			for _, e := range st.GetEngines() {
				fmt.Fprintf(out, "not enforcing:  %s\n", e.GetId())
			}
			return nil
		},
	}
}

func newAdminAuthLowerCommand(g *globalFlags) *cobra.Command {
	var acceptDuplicates bool
	cmd := &cobra.Command{
		Use:   "lower MODE",
		Short: "Lower the deployment's authentication mode to observe or asserted",
		Long: "lower rewrites the mode record to MODE (observe or asserted), below the mode it\n" +
			"names; then restart the engines in MODE. It never raises one: an engine raises\n" +
			"the record when it starts in a higher mode.\n\n" +
			"enforce to observe is never refused. To asserted is refused until the\n" +
			"deployment has been out of enforce for one idempotency key's life\n" +
			"(engine.idempotency_ttl and five minutes): a retried submission under a key\n" +
			"claimed by a principal would otherwise start a second run. Lower to observe\n" +
			"first; --accept-duplicate-retries skips the wait and is recorded in the audit\n" +
			"trail. It needs access.write at the deployment.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mode := args[0]
			if mode != "observe" && mode != "asserted" {
				return fmt.Errorf("MODE is observe or asserted, not %q", mode)
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			client := g.accessClient()
			status, err := client.GetAccess(ctx, connect.NewRequest(&orkstackv1.GetAccessRequest{}))
			if err != nil {
				return rpcError("admin auth lower", err)
			}
			resp, err := client.LowerAuthMode(ctx, connect.NewRequest(&orkstackv1.LowerAuthModeRequest{
				Mode: mode, RecordEtag: status.Msg.GetStatus().GetModeRecordEtag(), AcceptDuplicateRetries: acceptDuplicates,
			}))
			if err != nil {
				return rpcError("admin auth lower", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "mode record lowered from %s to %s; restart the engines with auth.mode: %s\n",
				resp.Msg.GetPreviousMode(), resp.Msg.GetMode(), resp.Msg.GetMode())
			return nil
		},
	}
	cmd.Flags().BoolVar(&acceptDuplicates, "accept-duplicate-retries", false,
		"lower to asserted now, accepting that a retried submission may start a second run")
	return cmd
}
