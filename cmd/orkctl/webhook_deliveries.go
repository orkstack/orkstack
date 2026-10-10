package main

import (
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// deliveriesCaption follows every human rendering of delivery records: the
// record proves what was sent and what the receiver answered, and it keeps a
// digest rather than the body.
const deliveriesCaption = "The body is not stored; each record carries its SHA-256 and the signature that was sent.\n"

// deliveryInstantLayout renders an attempt's instant to the millisecond.
const deliveryInstantLayout = "2006-01-02T15:04:05.000Z"

// newWebhookDeliveriesCommand lists delivery records.
//
// It is a deployment-wide listing, like the webhook surface it belongs to: a
// namespace narrows it only as a filter (--filter namespace=<ns>). The global
// -n is refused when typed on this command line, so muscle memory cannot
// silently widen or narrow what an operator reads; ORK_NAMESPACE and the
// saved namespace are ignored.
func newWebhookDeliveriesCommand(g *globalFlags) *cobra.Command {
	var (
		pageToken   string
		limit       int32
		filterTexts *[]string
	)
	cmd := &cobra.Command{
		Use:   "deliveries",
		Short: "List what each webhook delivery did, newest first",
		Long: "List delivery records: one immutable record per settled delivery — every\n" +
			"attempt, its status and error, the signature sent and the SHA-256 of the\n" +
			"exact body bytes (the body itself is not stored). Records are read from the\n" +
			"shared store, so any replica answers. Nothing is queued: a record is a log.\n\n" +
			"Every namespace is listed unless a namespace filter narrows it; -n is refused\n" +
			"here, and ORK_NAMESPACE and the namespace saved by 'orkctl namespace use' are\n" +
			"ignored. Every filter is decided from a record's key, so a body is read only\n" +
			"for a record that is listed.\n\n" +
			"The window is created_at: --filter 'created_at<T' ends it (default now) and\n" +
			"--filter 'created_at>=T' starts it, both RFC 3339 instants with a zone. With\n" +
			"no start, one run_id= filter starts the window at that run's start; otherwise\n" +
			"it is the seven days before its end. A window with more keys than the engine\n" +
			"will scan for one page is refused — narrow it.",
		Example: "  orkctl webhook deliveries --filter run_id=ork-2026-09-21-8f31\n" +
			"  orkctl webhook deliveries --filter webhook_id=wh-4a1c0e2b7d3f8159 --filter delivered=false",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := refuseNamespaceFlag(cmd, "webhook deliveries"); err != nil {
				return err
			}
			filters, err := parseFilterFlags(*filterTexts)
			if err != nil {
				return err
			}
			if limit < 0 || limit > 200 {
				return fmt.Errorf("--limit must be between 0 and 200, got %d", limit)
			}
			req := &orkstackv1.ListDeliveriesRequest{Filters: filters, PageSize: limit, PageToken: pageToken}

			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.webhookClient().ListDeliveries(ctx, connect.NewRequest(req))
			if err != nil {
				return rpcError("webhook deliveries", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			out := cmd.OutOrStdout()
			if err := renderDeliveries(out, req, resp.Msg); err != nil {
				return err
			}
			if next := resp.Msg.GetNextPageToken(); next != "" {
				// The token binds the filters and carries the resolved window,
				// so the next-page command repeats the filters and no window.
				if _, err := fmt.Fprintf(out, "\nNext page: %s\n",
					nextPageCommand([]string{"webhook", "deliveries"}, filters, limit, next)); err != nil {
					return fmt.Errorf("write output: %w", err)
				}
			}
			return nil
		},
	}
	cmd.Flags().Int32Var(&limit, "limit", 0, "records per page (0 = server default, 50; at most 200)")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token from a previous listing with the same filters")
	filterTexts = addFilterFlag(cmd, &orkstackv1.ListDeliveriesRequest{})
	return cmd
}

// renderDeliveries prints the table, the window line and the caption. The
// window line and caption follow every rendering, an empty page included.
func renderDeliveries(out io.Writer, req *orkstackv1.ListDeliveriesRequest, resp *orkstackv1.ListDeliveriesResponse) error {
	switch {
	case len(resp.GetDeliveries()) == 0 && len(req.GetFilters()) > 0:
		fmt.Fprint(out, noMatchSentence("deliveries", req.GetFilters()))
	case len(resp.GetDeliveries()) == 0:
		fmt.Fprintln(out, "No deliveries.")
	default:
		table := newTable(out)
		fmt.Fprintln(table, "DELIVERY\tWEBHOOK\tWHEN\tEVENT\tRUN\tTASK\tDELIVERED\tATTEMPTS\tLAST")
		for _, d := range resp.GetDeliveries() {
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
				d.GetId(), d.GetWebhookId(), formatTime(d.GetCreatedAt()), deliveryEventLabel(d.GetEvent()),
				d.GetRunId(), orDash(d.GetTaskId()), yesNo(d.GetDelivered()), len(d.GetAttempts()),
				lastAttemptLabel(d))
		}
		if err := table.Flush(); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	from := formatTime(resp.GetSince())
	// The engine says where the start came from; the CLI never infers it.
	if resp.GetSinceIsRunStart() {
		from += " (the run's start)"
	}
	_, err := fmt.Fprintf(out, "\nWindow: %s to %s, newest first.\n%s%s",
		from, formatTime(resp.GetUntil()), everyNamespaceLine(req.GetFilters()), deliveriesCaption)
	if err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func deliveryEventLabel(event orkstackv1.EventType) string {
	return strings.TrimPrefix(event.String(), "EVENT_TYPE_")
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

// lastAttemptLabel is the last attempt's status code — or the first 40 bytes
// of its error when no response arrived — and when it was sent.
func lastAttemptLabel(d *orkstackv1.Delivery) string {
	attempts := d.GetAttempts()
	if len(attempts) == 0 {
		return dash
	}
	last := attempts[len(attempts)-1]
	outcome := fmt.Sprint(last.GetStatusCode())
	if last.GetStatusCode() == 0 {
		outcome = truncateBytes(orDash(last.GetError()), 40)
	}
	if last.GetAt() == nil {
		return outcome
	}
	return outcome + " at " + last.GetAt().AsTime().UTC().Format("15:04:05.000Z")
}

func truncateBytes(value string, max int) string {
	if len(value) <= max {
		return value
	}
	cut := max
	for cut > 0 && !utf8Start(value[cut]) {
		cut--
	}
	return value[:cut]
}

func utf8Start(b byte) bool { return b&0xC0 != 0x80 }

func newWebhookDeliveryCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delivery <id>",
		Short: "Show one delivery record: every attempt, the signature and the body digest",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !webhooks.ValidDeliveryID(args[0]) {
				return fmt.Errorf("%q is not a delivery id (dlv-…, as X-Ork-Delivery carried it)", args[0])
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.webhookClient().GetDelivery(ctx, connect.NewRequest(&orkstackv1.GetDeliveryRequest{Id: args[0]}))
			if err != nil {
				return rpcError("webhook delivery", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderDelivery(cmd.OutOrStdout(), resp.Msg.GetDelivery())
		},
	}
}

func newWebhookRedeliverCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "redeliver <id>",
		Short: "POST one delivery again, once, now — and record it as a new delivery",
		Long: "Redeliver POSTs the delivery's event to its subscription once, now,\n" +
			"synchronously, signed with the subscription's CURRENT secret and embedding\n" +
			"the run as it reads today; the transition's own fields and timestamp are the\n" +
			"original's, and " + webhooks.RedeliveryHeader + " names the delivery it repeats.\n" +
			"Nothing is queued and nothing retries: you are the retry policy.\n\n" +
			"The outcome is recorded as a new delivery. The command exits 1 when the\n" +
			"receiver did not accept it (after printing it, --json included).",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !webhooks.ValidDeliveryID(args[0]) {
				return fmt.Errorf("%q is not a delivery id (dlv-…, as X-Ork-Delivery carried it)", args[0])
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.webhookClient().RedeliverWebhook(ctx, connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{Id: args[0]}))
			if err != nil {
				return rpcError("webhook redeliver", err)
			}
			rec := resp.Msg.GetDelivery()
			out := cmd.OutOrStdout()
			if g.json {
				if err := writeJSON(out, resp.Msg); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprintf(out, "Redelivered %s as %s: %s\n\n", args[0], rec.GetId(), redeliveryOutcome(rec)); err != nil {
					return fmt.Errorf("write output: %w", err)
				}
				if err := renderDelivery(out, rec); err != nil {
					return err
				}
			}
			if !rec.GetDelivered() {
				return &exitCodeError{code: exitError, err: fmt.Errorf("redelivery %s was not delivered: %s", rec.GetId(), redeliveryOutcome(rec))}
			}
			return nil
		},
	}
}

// redeliveryOutcome is "delivered, 200 in 91ms", "not delivered, 502 in 77ms"
// or "not delivered, dial tcp …".
func redeliveryOutcome(rec *orkstackv1.Delivery) string {
	verdict := "not delivered"
	if rec.GetDelivered() {
		verdict = "delivered"
	}
	attempts := rec.GetAttempts()
	if len(attempts) == 0 {
		return verdict
	}
	last := attempts[len(attempts)-1]
	if last.GetStatusCode() == 0 {
		return verdict + ", " + orDash(last.GetError())
	}
	return fmt.Sprintf("%s, %d in %s", verdict, last.GetStatusCode(), millis(last.GetDuration().AsDuration()))
}

func millis(d time.Duration) string {
	return d.Round(time.Millisecond).String()
}

func instant(ts *timestamppb.Timestamp) string {
	if ts == nil || !ts.IsValid() || ts.AsTime().IsZero() {
		return dash
	}
	return ts.AsTime().UTC().Format(deliveryInstantLayout)
}

// renderDelivery prints one record: its coordinates, what was sent, and the
// attempt timeline.
func renderDelivery(out io.Writer, d *orkstackv1.Delivery) error {
	fmt.Fprintf(out, "Delivery %s\n", d.GetId())
	table := newTable(out)
	fmt.Fprintf(table, "WEBHOOK\t%s  %s\n", d.GetWebhookId(), d.GetUrl())
	fmt.Fprintf(table, "EVENT\t%s\n", deliveryEventLabel(d.GetEvent()))
	fmt.Fprintf(table, "RUN\t%s  (namespace %s, orkflow %s)\n", d.GetRunId(), orDash(d.GetNamespace()), orDash(d.GetOrkflowName()))
	if d.GetTaskId() != "" {
		task := d.GetTaskId()
		if d.GetAttempt() > 0 {
			task = fmt.Sprintf("%s, attempt %d", task, d.GetAttempt())
		}
		fmt.Fprintf(table, "TASK\t%s\n", task)
	}
	fmt.Fprintf(table, "DELIVERED\t%s\n", yesNo(d.GetDelivered()))
	fmt.Fprintf(table, "SIGNATURE\t%s\n", orDash(d.GetSignature()))
	fmt.Fprintf(table, "BODY\tsha256 %s, %d bytes (not stored; the run is readable with: orkctl -n %s run get %s)\n",
		orDash(d.GetBodySha256()), d.GetBodyBytes(), shellArg(orDash(d.GetNamespace())), shellArg(d.GetRunId()))
	if d.GetRedeliveryOf() != "" {
		fmt.Fprintf(table, "REDELIVERY OF\t%s\n", d.GetRedeliveryOf())
	}
	fmt.Fprintf(table, "EVENT AT\t%s\n", instant(d.GetEventAt()))
	fmt.Fprintf(table, "QUEUED\t%s\n", instant(d.GetCreatedAt()))
	fmt.Fprintf(table, "SETTLED\t%s\n", instant(d.GetSettledAt()))
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)
	attempts := newTable(out)
	fmt.Fprintln(attempts, "#\tAT\tSTATUS\tDURATION\tBACKOFF\tERROR")
	for _, a := range d.GetAttempts() {
		status := dash
		if a.GetStatusCode() != 0 {
			status = fmt.Sprint(a.GetStatusCode())
		}
		backoff := dash
		if b := a.GetBackoff().AsDuration(); b > 0 {
			backoff = millis(b)
		}
		fmt.Fprintf(attempts, "%d\t%s\t%s\t%s\t%s\t%s\n",
			a.GetNumber(), instant(a.GetAt()), status, millis(a.GetDuration().AsDuration()), backoff, orDash(a.GetError()))
	}
	if err := attempts.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
