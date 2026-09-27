package main

import (
	"errors"
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
// Its scope is the deployment by default, like the webhook surface it
// belongs to, so the global -n narrows it only when it was typed on THIS
// command line — never from ORK_NAMESPACE or the saved namespace, which would
// silently hide the delivery an operator is looking for.
func newWebhookDeliveriesCommand(g *globalFlags) *cobra.Command {
	var (
		webhookID, runID, eventText, deliveredText string
		sinceText, untilText, pageToken            string
		limit                                      int32
	)
	cmd := &cobra.Command{
		Use:   "deliveries",
		Short: "List what each webhook delivery did, newest first",
		Long: "List delivery records: one immutable record per settled delivery — every\n" +
			"attempt, its status and error, the signature sent and the SHA-256 of the\n" +
			"exact body bytes (the body itself is not stored). Records are read from the\n" +
			"shared store, so any replica answers. Nothing is queued: a record is a log.\n\n" +
			"The scope is every namespace unless -n is given on this command line;\n" +
			"ORK_NAMESPACE and the namespace saved by 'orkctl namespace use' are ignored.\n\n" +
			"--since takes a lookback (24h; 7d is 168h) or an RFC 3339 instant, --until\n" +
			"an instant. With --run and no --since the window starts at the run's start;\n" +
			"otherwise it is the seven days before --until (default now). A window with\n" +
			"more keys than the engine will scan for one page is refused — narrow it.",
		Example: "  orkctl webhook deliveries --run ork-2026-09-21-8f31\n" +
			"  orkctl webhook deliveries --webhook wh-4a1c0e2b7d3f8159 --delivered=false --since 24h",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			req := &orkstackv1.ListDeliveriesRequest{WebhookId: webhookID, RunId: runID, PageToken: pageToken}
			if eventText != "" {
				event, err := webhooks.ParseEvent(eventText)
				if err != nil {
					return err
				}
				req.Event = eventType(event)
			}
			switch deliveredText {
			case "":
			case "true":
				req.Outcome = orkstackv1.DeliveryOutcome_DELIVERY_OUTCOME_DELIVERED
			case "false":
				req.Outcome = orkstackv1.DeliveryOutcome_DELIVERY_OUTCOME_FAILED
			default:
				return fmt.Errorf("invalid --delivered %q: want true or false", deliveredText)
			}
			if untilText != "" {
				if _, err := time.ParseDuration(untilText); err == nil {
					return fmt.Errorf("invalid --until %q: want an instant (2026-09-21 or 2026-09-21T09:00:00Z), not a duration", untilText)
				}
				_, at, err := parseSince(untilText)
				if err != nil {
					return errors.New(strings.Replace(err.Error(), "--since", "--until", 1))
				}
				req.Until = timestamppb.New(at)
			}
			if sinceText != "" {
				lookback, at, err := parseSince(sinceText)
				if err != nil {
					return err
				}
				if lookback > 0 {
					// The RPC takes instants. A lookback is measured back from
					// --until when one was given, so the window is one clock;
					// otherwise from this machine's now.
					end := time.Now().UTC()
					if req.Until != nil {
						end = req.Until.AsTime()
					}
					at = end.Add(-lookback)
				}
				req.Since = timestamppb.New(at)
			}
			if req.Since != nil && req.Until != nil && !req.Until.AsTime().After(req.Since.AsTime()) {
				return fmt.Errorf("--until %s must be after --since %s",
					req.Until.AsTime().UTC().Format(time.RFC3339Nano), req.Since.AsTime().UTC().Format(time.RFC3339Nano))
			}
			if limit < 0 || limit > 200 {
				return fmt.Errorf("--limit must be between 0 and 200, got %d", limit)
			}
			req.PageSize = limit
			if cmd.Flags().Changed("namespace") {
				req.Namespace = g.namespace
			}

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
				if _, err := fmt.Fprintf(out, "\nNext page: orkctl webhook deliveries %s--page-token %s\n",
					deliveriesNextPageArgs(req, eventText, deliveredText), next); err != nil {
					return fmt.Errorf("write output: %w", err)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&webhookID, "webhook", "", "only deliveries to this webhook id")
	cmd.Flags().StringVar(&runID, "run", "", "only deliveries about this run id")
	cmd.Flags().StringVar(&eventText, "event", "", "only this event: "+strings.Join(eventNames(), " | "))
	cmd.Flags().StringVar(&deliveredText, "delivered", "", "true for landed deliveries only, false for failed only")
	cmd.Flags().StringVar(&sinceText, "since", "", "start of the window: a lookback (24h) or an RFC 3339 instant")
	cmd.Flags().StringVar(&untilText, "until", "", "exclusive end of the window, an RFC 3339 instant (default: now)")
	cmd.Flags().Int32Var(&limit, "limit", 0, "records per page (0 = server default, 50; at most 200)")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token from a previous listing with the same filters")
	return cmd
}

// deliveriesNextPageArgs repeats every filter the page token is bound to —
// the window as the INSTANTS that were sent, so a lookback does not move
// between pages — each shell-quoted only when it needs to be.
func deliveriesNextPageArgs(req *orkstackv1.ListDeliveriesRequest, event, delivered string) string {
	var b strings.Builder
	add := func(flag, value string) {
		if value != "" {
			b.WriteString(flag + " " + shellArg(value) + " ")
		}
	}
	add("-n", req.GetNamespace())
	add("--webhook", req.GetWebhookId())
	add("--run", req.GetRunId())
	add("--event", event)
	if delivered != "" {
		b.WriteString("--delivered=" + delivered + " ")
	}
	if req.GetSince() != nil {
		add("--since", req.GetSince().AsTime().UTC().Format(time.RFC3339Nano))
	}
	if req.GetUntil() != nil {
		add("--until", req.GetUntil().AsTime().UTC().Format(time.RFC3339Nano))
	}
	if req.GetPageSize() > 0 {
		add("--limit", fmt.Sprint(req.GetPageSize()))
	}
	return b.String()
}

// renderDeliveries prints the table, the scope line and the caption. The
// scope line and caption follow every rendering, an empty page included.
func renderDeliveries(out io.Writer, req *orkstackv1.ListDeliveriesRequest, resp *orkstackv1.ListDeliveriesResponse) error {
	if len(resp.GetDeliveries()) == 0 {
		fmt.Fprintln(out, "No deliveries.")
	} else {
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
	scope := "every namespace"
	if req.GetNamespace() != "" {
		scope = "namespace " + req.GetNamespace()
	}
	from := formatTime(resp.GetSince())
	if req.GetSince() == nil && req.GetRunId() != "" && req.GetPageToken() == "" {
		from += " (the run's start)"
	}
	_, err := fmt.Fprintf(out, "\nScope: %s, %s to %s, newest first.\n%s",
		scope, from, formatTime(resp.GetUntil()), deliveriesCaption)
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
