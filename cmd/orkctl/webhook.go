package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// webhookEnvelope and webhookListEnvelope preserve the established CLI JSON
// shape while the wire protocol uses the generated WebhookService.
type webhookEnvelope struct {
	Webhook webhooks.Webhook `json:"webhook"`
}

type webhookListEnvelope struct {
	Webhooks []webhooks.Webhook `json:"webhooks"`
}

func newWebhookCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "webhook",
		Short: "Register and inspect run-event callbacks",
		Long: "A webhook receives a signed POST when a run or one of its tasks changes\n" +
			"state. Run transitions: RUN_STARTED, RUN_SUCCEEDED, RUN_FAILED,\n" +
			"RUN_CANCELED. Task outcomes: TASK_SCHEDULED, TASK_STARTED, TASK_SUCCEEDED,\n" +
			"TASK_FAILED.\n\n" +
			"Omitting --event means every run transition; task events are opt-in by\n" +
			"name. Task events are best-effort hints with a short retry budget: a\n" +
			"subscriber that must not miss one reconciles with GetRun.\n\n" +
			"A subscription is deployment-wide unless it names a scope: a namespace\n" +
			"(-n), an orkflow (--orkflow) and a task (--task), each an exact match.",
	}
	cmd.AddCommand(
		newWebhookRegisterCommand(g),
		newWebhookListCommand(g),
		newWebhookDeleteCommand(g),
	)
	return cmd
}

func newWebhookRegisterCommand(g *globalFlags) *cobra.Command {
	var (
		events      []string
		secret      string
		orkflowName string
		taskID      string
	)
	cmd := &cobra.Command{
		Use:   "register <url>",
		Short: "Register a callback URL for run events",
		Long: "Register a callback URL. Repeat --event to select transitions;\n" +
			"omitting it subscribes to every run transition. Task events are opt-in\n" +
			"and are best-effort hints: reconcile with GetRun if one must not be missed.\n\n" +
			"Scope defaults to every namespace; only an explicit -n narrows it.\n" +
			"ORK_NAMESPACE and the saved default do not apply here. --orkflow and\n" +
			"--task narrow further; each is an exact match, empty means every, and\n" +
			"--task needs at least one TASK_* event.\n\n" +
			"The signing secret is printed once, here. Deliveries carry it as\n" +
			"an HMAC-SHA256 of the request body in the " + webhooks.SignatureHeader + " header.",
		Example: "  orkctl webhook register https://example.test/hooks --event RUN_SUCCEEDED --event RUN_FAILED\n" +
			"  orkctl webhook register https://example.test/load -n analytics --orkflow nightly-etl \\\n" +
			"      --task extract --event TASK_SUCCEEDED",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Reject an unknown event before a round trip, and normalize the
			// spelling so the server sees canonical names.
			selected := make([]orkstackv1.EventType, 0, len(events))
			parsed := make([]webhooks.Event, 0, len(events))
			for _, raw := range events {
				for _, part := range strings.Split(raw, ",") {
					if strings.TrimSpace(part) == "" {
						continue
					}
					event, err := webhooks.ParseEvent(part)
					if err != nil {
						return err
					}
					selected = append(selected, eventType(event))
					parsed = append(parsed, event)
				}
			}
			if _, err := webhooks.ValidateURL(args[0]); err != nil {
				return err
			}
			// A subscription's default scope is every namespace, so only an
			// explicit -n narrows it: a saved or environment namespace
			// silently narrowing an audit sink would invert its meaning.
			namespace := ""
			if cmd.Flags().Changed("namespace") {
				namespace = g.namespace
			}
			if err := webhooks.ValidateScope(namespace, orkflowName, taskID, parsed); err != nil {
				return err
			}

			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			response, err := g.webhookClient().RegisterWebhook(ctx, connect.NewRequest(&orkstackv1.RegisterWebhookRequest{
				Url: args[0], Events: selected, Secret: secret,
				Namespace: namespace, OrkflowName: orkflowName, TaskId: taskID,
			}))
			if err != nil {
				return err
			}
			hook := webhookFromProto(response.Msg.GetWebhook())
			hook.Secret = response.Msg.GetSecret()
			if g.json {
				return writeIndentedJSON(cmd.OutOrStdout(), webhookEnvelope{Webhook: hook})
			}
			return renderWebhookRegistered(cmd.OutOrStdout(), hook)
		},
	}
	cmd.Flags().StringArrayVar(&events, "event", nil,
		"event to subscribe to (repeatable): "+strings.Join(eventNames(), " | "))
	cmd.Flags().StringVar(&secret, "secret", "",
		"signing secret to use instead of a generated one")
	cmd.Flags().StringVar(&orkflowName, "orkflow", "",
		"narrow to runs of this orkflow (exact name; empty means every orkflow)")
	cmd.Flags().StringVar(&taskID, "task", "",
		"narrow task events to this task (exact id; needs a TASK_* --event)")
	return cmd
}

func newWebhookListCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List registered webhooks",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			response, err := g.webhookClient().ListWebhooks(ctx, connect.NewRequest(&orkstackv1.ListWebhooksRequest{}))
			if err != nil {
				return err
			}
			hooks := make([]webhooks.Webhook, 0, len(response.Msg.GetWebhooks()))
			for _, hook := range response.Msg.GetWebhooks() {
				hooks = append(hooks, webhookFromProto(hook))
			}
			if g.json {
				return writeIndentedJSON(cmd.OutOrStdout(), webhookListEnvelope{Webhooks: hooks})
			}
			return renderWebhookList(cmd.OutOrStdout(), hooks)
		},
	}
}

func newWebhookDeleteCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <id>",
		Short: "Remove a registered webhook",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			if _, err := g.webhookClient().DeleteWebhook(ctx, connect.NewRequest(&orkstackv1.DeleteWebhookRequest{Id: args[0]})); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted webhook %s\n", args[0])
			return nil
		},
	}
}

func eventType(event webhooks.Event) orkstackv1.EventType {
	return orkstackv1.EventType(orkstackv1.EventType_value["EVENT_TYPE_"+string(event)])
}

func webhookFromProto(hook *orkstackv1.Webhook) webhooks.Webhook {
	if hook == nil {
		return webhooks.Webhook{}
	}
	events := make([]webhooks.Event, 0, len(hook.GetEvents()))
	for _, event := range hook.GetEvents() {
		if parsed, err := webhooks.ParseEvent(event.String()); err == nil {
			events = append(events, parsed)
		}
	}
	created := time.Time{}
	if hook.GetCreatedAt() != nil {
		created = hook.GetCreatedAt().AsTime().UTC()
	}
	return webhooks.Webhook{
		ID: hook.GetId(), URL: hook.GetUrl(), Events: events, CreatedAt: created,
		Namespace: hook.GetNamespace(), OrkflowName: hook.GetOrkflowName(), TaskID: hook.GetTaskId(),
	}
}

func renderWebhookRegistered(out io.Writer, hook webhooks.Webhook) error {
	fmt.Fprintf(out, "Registered webhook %s\n", hook.ID)
	table := newTable(out)
	fmt.Fprintf(table, "URL\t%s\n", hook.URL)
	fmt.Fprintf(table, "EVENTS\t%s\n", webhookEventCell(hook))
	fmt.Fprintf(table, "SCOPE\t%s\n", webhookScopeCell(hook))
	fmt.Fprintf(table, "SECRET\t%s\n", orDash(hook.Secret))
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintf(out, "\nThe secret is shown once. Deliveries sign the request body with it (%s).\n",
		webhooks.SignatureHeader)
	return nil
}

func renderWebhookList(out io.Writer, hooks []webhooks.Webhook) error {
	if len(hooks) == 0 {
		fmt.Fprintln(out, "No webhooks.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "ID\tURL\tEVENTS\tSCOPE\tCREATED")
	for _, hook := range hooks {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n",
			hook.ID, hook.URL, webhookEventCell(hook), webhookScopeCell(hook), hook.CreatedAt.UTC().Format(time.RFC3339))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// webhookEventCell renders a subscription's events; none means all of them.
func webhookEventCell(hook webhooks.Webhook) string {
	if len(hook.Events) == 0 {
		return "ALL"
	}
	names := make([]string, 0, len(hook.Events))
	for _, event := range hook.Events {
		names = append(names, string(event))
	}
	return strings.Join(names, ",")
}

// webhookScopeCell renders a subscription's scope positionally as
// namespace/orkflow/task with "*" for an unset component, or ALL when all three
// are unset. "/" and "*" cannot occur in a slug, so the cell is unambiguous.
func webhookScopeCell(hook webhooks.Webhook) string {
	if hook.Namespace == "" && hook.OrkflowName == "" && hook.TaskID == "" {
		return "ALL"
	}
	part := func(value string) string {
		if value == "" {
			return "*"
		}
		return value
	}
	return part(hook.Namespace) + "/" + part(hook.OrkflowName) + "/" + part(hook.TaskID)
}

// eventNames lists the subscribable transitions for flag help.
func eventNames() []string {
	names := make([]string, 0, len(webhooks.KnownEvents()))
	for _, event := range webhooks.KnownEvents() {
		names = append(names, string(event))
	}
	return names
}

// writeIndentedJSON prints a plain Go value as indented JSON. Protobuf
// messages go through writeJSON instead, which owns the enum dialect.
func writeIndentedJSON(out io.Writer, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	if _, err := out.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}
