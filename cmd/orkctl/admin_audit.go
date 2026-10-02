package main

import (
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

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
// Its scope is the deployment by default, like every other `admin` command,
// so the global -n narrows it only when it was typed on THIS command line —
// never from ORK_NAMESPACE or the namespace saved by `orkctl namespace use`,
// which would silently hide every deployment-wide write from an operator who
// happened to have a namespace selected. The Scope line says which it was.
func newAdminAuditCommand(g *globalFlags) *cobra.Command {
	var (
		sinceText, untilText string
		action, principal    string
		limit                int32
		pageToken            string
	)
	cmd := &cobra.Command{
		Use:   "audit",
		Short: "List control-plane writes: who changed what, when, newest first",
		Long: "List the control-plane audit trail: one immutable record per write RPC\n" +
			"(namespace, orkflow, trigger, webhook, retention, image policy, run-cancel and Orker\n" +
			"cordon changes), succeeded or refused, read from the shared store so any\n" +
			"replica answers.\n" +
			"Reads and run submissions are never recorded.\n\n" +
			"The scope is every namespace unless -n is given on this command line;\n" +
			"ORK_NAMESPACE and the namespace saved by 'orkctl namespace use' are ignored.\n" +
			"Deployment-wide writes (retention, image policy, webhooks, Orker cordons) carry no\n" +
			"namespace and appear only in the every-namespace scope.\n\n" +
			"--since and --until are RFC 3339 instants (a bare YYYY-MM-DD means midnight\n" +
			"UTC); the default window is the seven days before now. A window with more\n" +
			"records than the engine will scan for one page is refused — narrow it.\n\n" +
			"PRINCIPAL is whatever the caller's gateway asserted in the Ork-Principal\n" +
			"header (ORK_PRINCIPAL makes this CLI assert one); OrkStack records it and\n" +
			"does not authenticate it, so a gateway that authenticates callers must\n" +
			"overwrite the header rather than pass it through.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			since, err := parseAuditBound("--since", sinceText)
			if err != nil {
				return err
			}
			until, err := parseAuditBound("--until", untilText)
			if err != nil {
				return err
			}
			if since != nil && until != nil && !until.AsTime().After(since.AsTime()) {
				return fmt.Errorf("--until %s must be after --since %s",
					until.AsTime().UTC().Format(time.RFC3339Nano), since.AsTime().UTC().Format(time.RFC3339Nano))
			}
			if limit < 0 {
				return fmt.Errorf("--limit must not be negative, got %d", limit)
			}
			namespace := ""
			if cmd.Flags().Changed("namespace") {
				namespace = g.namespace
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.auditClient().ListAuditRecords(ctx, connect.NewRequest(&orkstackv1.ListAuditRecordsRequest{
				Namespace:    namespace,
				ActionPrefix: action,
				Principal:    principal,
				Since:        since,
				Until:        until,
				PageSize:     limit,
				PageToken:    pageToken,
			}))
			if err != nil {
				return rpcError("admin audit", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			out := cmd.OutOrStdout()
			if err := renderAuditRecords(out, namespace, resp.Msg); err != nil {
				return err
			}
			if next := resp.Msg.GetNextPageToken(); next != "" {
				args := auditNextPageArgs(namespace, action, principal, sinceText, untilText, limit)
				if _, err := fmt.Fprintf(out, "\nNext page: orkctl admin audit %s--page-token %s\n", args, next); err != nil {
					return fmt.Errorf("write output: %w", err)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&sinceText, "since", "", "inclusive start of the window, RFC3339 (default: seven days before --until)")
	cmd.Flags().StringVar(&untilText, "until", "", "exclusive end of the window, RFC3339 (default: now)")
	cmd.Flags().StringVar(&action, "action", "", "only actions starting with this prefix (namespace., orkflow.promote, …)")
	cmd.Flags().StringVar(&principal, "principal", "", "only records with exactly this asserted principal")
	cmd.Flags().Int32Var(&limit, "limit", 0, "records per page (0 = server default, 50; at most 200)")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token from a previous listing with the same filters")
	return cmd
}

// parseAuditBound reads --since/--until: RFC 3339, or a bare date meaning
// midnight UTC, which is how a compliance window is usually written.
func parseAuditBound(flag, value string) (*timestamppb.Timestamp, error) {
	if value == "" {
		return nil, nil
	}
	if day, err := time.Parse(time.DateOnly, value); err == nil {
		return timestamppb.New(day.UTC()), nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil, fmt.Errorf("%s must be RFC3339 (or YYYY-MM-DD): %w", flag, err)
	}
	return timestamppb.New(parsed), nil
}

// renderAuditRecords prints the table, the scope line and the caption. The
// scope line and caption are printed on every rendering, empty page included.
func renderAuditRecords(out io.Writer, namespace string, resp *orkstackv1.ListAuditRecordsResponse) error {
	records := resp.GetRecords()
	if len(records) == 0 {
		fmt.Fprintln(out, "No audit records in this window.")
	} else {
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
	scope := "every namespace"
	if namespace != "" {
		scope = "namespace " + namespace
	}
	fmt.Fprintf(out, "\nScope: %s, %s to %s (exclusive), newest first.\n",
		scope, formatTime(resp.GetSince()), formatTime(resp.GetUntil()))
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

// auditNextPageArgs repeats the filters the page token is bound to, so the
// printed command continues the same listing instead of being refused as
// "filters changed". Each value is shell-quoted only when it needs to be.
func auditNextPageArgs(namespace, action, principal, since, until string, limit int32) string {
	var b strings.Builder
	add := func(flag, value string) {
		if value != "" {
			b.WriteString(flag + " " + shellArg(value) + " ")
		}
	}
	add("-n", namespace)
	add("--since", since)
	add("--until", until)
	add("--action", action)
	add("--principal", principal)
	if limit > 0 {
		add("--limit", fmt.Sprint(limit))
	}
	return b.String()
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
