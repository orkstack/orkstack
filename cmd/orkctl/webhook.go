package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// Webhook management is the one part of the engine API that is not a Connect
// service: proto stays the source of truth for cross-process types, and this
// surface does not appear there. These commands therefore speak plain JSON
// over the same base URL and the same transport as every other command.

// webhookEnvelope and webhookListEnvelope mirror the server's response bodies.
type webhookEnvelope struct {
	Webhook webhooks.Webhook `json:"webhook"`
}

type webhookListEnvelope struct {
	Webhooks []webhooks.Webhook `json:"webhooks"`
}

type webhookErrorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func newWebhookCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "webhook",
		Short: "Register and inspect run-event callbacks",
		Long: "A webhook receives a signed POST when a run changes state:\n" +
			"RUN_STARTED, RUN_SUCCEEDED, RUN_FAILED or RUN_CANCELED.",
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
		events []string
		secret string
	)
	cmd := &cobra.Command{
		Use:   "register <url>",
		Short: "Register a callback URL for run events",
		Long: "Register a callback URL. Repeat --event to select transitions;\n" +
			"omitting it subscribes to every run transition.\n\n" +
			"The signing secret is printed once, here. Deliveries carry it as\n" +
			"an HMAC-SHA256 of the request body in the " + webhooks.SignatureHeader + " header.",
		Example: "  orkctl webhook register https://example.test/hooks --event RUN_SUCCEEDED --event RUN_FAILED",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Reject an unknown event before a round trip, and normalize the
			// spelling so the server sees canonical names.
			selected := make([]string, 0, len(events))
			for _, raw := range events {
				for _, part := range strings.Split(raw, ",") {
					if strings.TrimSpace(part) == "" {
						continue
					}
					event, err := webhooks.ParseEvent(part)
					if err != nil {
						return err
					}
					selected = append(selected, string(event))
				}
			}
			if _, err := webhooks.ValidateURL(args[0]); err != nil {
				return err
			}

			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			var response webhookEnvelope
			if err := g.webhookRequest(ctx, http.MethodPost, api.WebhookCollectionPath, map[string]any{
				"url":    args[0],
				"events": selected,
				"secret": secret,
			}, &response); err != nil {
				return err
			}
			if g.json {
				return writeIndentedJSON(cmd.OutOrStdout(), response)
			}
			return renderWebhookRegistered(cmd.OutOrStdout(), response.Webhook)
		},
	}
	cmd.Flags().StringArrayVar(&events, "event", nil,
		"run transition to subscribe to (repeatable): "+strings.Join(eventNames(), " | "))
	cmd.Flags().StringVar(&secret, "secret", "",
		"signing secret to use instead of a generated one")
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

			var response webhookListEnvelope
			if err := g.webhookRequest(ctx, http.MethodGet, api.WebhookCollectionPath, nil, &response); err != nil {
				return err
			}
			if g.json {
				return writeIndentedJSON(cmd.OutOrStdout(), response)
			}
			return renderWebhookList(cmd.OutOrStdout(), response.Webhooks)
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

			path := strings.Replace(api.WebhookItemPath, "{id}", args[0], 1)
			if err := g.webhookRequest(ctx, http.MethodDelete, path, nil, nil); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted webhook %s\n", args[0])
			return nil
		},
	}
}

// webhookRequest performs one JSON call against the engine and decodes the
// response into out (nil discards it). A non-2xx answer is rendered as the
// same short, actionable error rpcError produces for Connect calls.
func (g *globalFlags) webhookRequest(
	ctx context.Context,
	method, path string,
	body any,
	out any,
) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, g.baseURL()+path, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := g.httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("%s %s: read response: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return webhookHTTPError(method, path, resp.StatusCode, payload)
	}
	if out == nil || len(bytes.TrimSpace(payload)) == 0 {
		return nil
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return fmt.Errorf("%s %s: decode response: %w", method, path, err)
	}
	return nil
}

// webhookHTTPError turns a failure body into a one-line message, falling back
// to the status when the body is not the API's error envelope.
func webhookHTTPError(method, path string, status int, payload []byte) error {
	var envelope webhookErrorEnvelope
	if err := json.Unmarshal(payload, &envelope); err == nil && envelope.Error.Message != "" {
		return fmt.Errorf("%s %s: %s: %s", method, path, envelope.Error.Code, envelope.Error.Message)
	}
	return fmt.Errorf("%s %s: %s", method, path, strings.TrimSpace(http.StatusText(status)))
}

func renderWebhookRegistered(out io.Writer, hook webhooks.Webhook) error {
	fmt.Fprintf(out, "Registered webhook %s\n", hook.ID)
	table := newTable(out)
	fmt.Fprintf(table, "URL\t%s\n", hook.URL)
	fmt.Fprintf(table, "EVENTS\t%s\n", webhookEventCell(hook))
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
	fmt.Fprintln(table, "ID\tURL\tEVENTS\tCREATED")
	for _, hook := range hooks {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n",
			hook.ID, hook.URL, webhookEventCell(hook), hook.CreatedAt.UTC().Format(time.RFC3339))
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
