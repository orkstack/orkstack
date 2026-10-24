package main

import (
	"fmt"
	"io"
	"regexp"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// auditCaption is printed under every human rendering of the trail, an empty
// page included: the principal column is an assertion, and a reader must not
// have to find a footnote to learn that.
const auditCaption = "principal is the identity the deployment's gateway asserted in Ork-Principal;\n" +
	"OrkStack records it and does not authenticate it.\n"

// auditUnattributed is what an empty principal renders as. A blank cell would
// look like a name that happens to be empty; this reads as the gap it is.
const auditUnattributed = "unattributed"

// newAdminAuditCommand reads the control-plane audit trail back.
//
// It is a deployment-wide listing, like every other `admin` command: a
// namespace narrows it only as a filter (--filter namespace=<ns>), and
// `--filter namespace=` selects the deployment-wide writes. The global -n is
// refused when typed on this command line; ORK_NAMESPACE and the namespace
// saved by `orkctl namespace use` are ignored, which would otherwise silently
// hide every deployment-wide write from an operator who happened to have a
// namespace selected.
func newAdminAuditCommand(g *globalFlags) *cobra.Command {
	var (
		limit       int32
		pageToken   string
		filterTexts *[]string
	)
	cmd := &cobra.Command{
		Use:   "audit",
		Short: "List control-plane writes: who changed what, when, newest first",
		Long: "List the control-plane audit trail: one immutable record per write RPC\n" +
			"(namespace, orkflow, trigger, webhook, schedule, retention, image policy,\n" +
			"release, cache purge, run cancel, await resolve and Orker cordon changes),\n" +
			"succeeded or refused, read from the shared store so any replica answers.\n" +
			"Reads, run submissions and trigger invocations are never recorded.\n\n" +
			"Every namespace is listed unless a namespace filter narrows it; -n is refused\n" +
			"here, and ORK_NAMESPACE and the namespace saved by 'orkctl namespace use' are\n" +
			"ignored. Deployment-wide writes (retention, image policy, webhooks, releases,\n" +
			"Orker cordons) carry no namespace: --filter 'namespace=' selects them.\n\n" +
			"The window is at: --filter 'at<T' ends it (default now) and --filter 'at>=T'\n" +
			"starts it (default seven days before its end), both RFC 3339 instants with a\n" +
			"zone. A window with more records than the engine will scan for one page is\n" +
			"refused — narrow it.\n\n" +
			"PRINCIPAL is whatever the caller's gateway asserted in the Ork-Principal\n" +
			"header (ORK_PRINCIPAL makes this CLI assert one); OrkStack records it and\n" +
			"does not authenticate it, so a gateway that authenticates callers must\n" +
			"overwrite the header rather than pass it through.",
		Example: "  orkctl admin audit --filter 'action^namespace.' --filter 'at>=2026-10-01T00:00:00Z'\n" +
			"  orkctl admin audit --filter 'namespace='",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := refuseNamespaceFlag(cmd, "admin audit"); err != nil {
				return err
			}
			filters, err := parseFilterFlags(*filterTexts)
			if err != nil {
				return err
			}
			if limit < 0 {
				return fmt.Errorf("--limit must not be negative, got %d", limit)
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.auditClient().ListAuditRecords(ctx, connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{
				Filters:   filters,
				PageSize:  limit,
				PageToken: pageToken,
			}))
			if err != nil {
				return rpcError("admin audit", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			out := cmd.OutOrStdout()
			if err := renderAuditRecords(out, filters, resp.Msg); err != nil {
				return err
			}
			if next := resp.Msg.GetNextPageToken(); next != "" {
				if _, err := fmt.Fprintf(out, "\nNext page: %s\n",
					nextPageCommand([]string{"admin", "audit"}, filters, limit, next)); err != nil {
					return fmt.Errorf("write output: %w", err)
				}
			}
			return nil
		},
	}
	cmd.Flags().Int32Var(&limit, "limit", 0, "records per page (0 = server default, 50; at most 200)")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token from a previous listing with the same filters")
	filterTexts = addFilterFlag(cmd, &orkstackv1.ListAuditRecordsRequest{})
	return cmd
}

// renderAuditRecords prints the table, the window line and the caption. The
// window line and caption are printed on every rendering, empty page included.
func renderAuditRecords(out io.Writer, filters []*orkstackv1.FieldFilter, resp *orkstackv1.ListAuditRecordsResponse) error {
	records := resp.GetRecords()
	switch {
	case len(records) == 0 && len(filters) > 0:
		fmt.Fprint(out, noMatchSentence("audit records", filters))
	case len(records) == 0:
		fmt.Fprintln(out, "No audit records in this window.")
	default:
		table := newTable(out)
		fmt.Fprintln(table, "WHEN\tPRINCIPAL\tACTION\tNAMESPACE\tTARGET\tRESULT")
		for _, rec := range records {
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n",
				formatTime(rec.GetAt()),
				auditPrincipalLabel(rec.GetPrincipal()),
				orDash(rec.GetAction()),
				orDash(rec.GetNamespace()),
				orDash(rec.GetTarget()),
				auditResultLabel(rec))
		}
		if err := table.Flush(); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	fmt.Fprintf(out, "\nWindow: %s to %s (exclusive), newest first.\n%s",
		formatTime(resp.GetSince()), formatTime(resp.GetUntil()), everyNamespaceLine(filters))
	_, err := io.WriteString(out, auditCaption)
	if err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func auditPrincipalLabel(principal string) string {
	if principal == "" {
		return auditUnattributed
	}
	return principal
}

func auditResultLabel(rec *orkstackv1.AuditRecord) string {
	if rec.GetSucceeded() {
		return "ok"
	}
	return "refused: " + orDash(rec.GetErrorCode())
}

var shellSafe = regexp.MustCompile(`^[A-Za-z0-9._:/@=+,-]+$`)

// shellArg returns value as one shell word: bare when it is plainly safe,
// single-quoted otherwise.
func shellArg(value string) string {
	if shellSafe.MatchString(value) {
		return value
	}
	return shellQuote(value)
}
