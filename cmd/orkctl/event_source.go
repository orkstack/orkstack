package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/automation"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// eventSourceDeleteAttempts is how many times a delete is sent, with the
// same id, while its outcome is not known (docs/AUTOMATION.md,
// "event_sources").
const eventSourceDeleteAttempts = 5

func (g *globalFlags) eventSourceClient() orkstackv1connect.EventSourceServiceClient {
	return orkstackv1connect.NewEventSourceServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

func newEventSourceCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "event-source",
		Short: "Create, inspect and evaluate event sources",
		Long: "An event source binds one Orkflow to one source image: each message the\n" +
			"image receives from its broker starts one run (docs/EVENT_SOURCES.md).\n\n" +
			"In this release no source consumes anything: a source is stored, read\n" +
			"and evaluated. Event sources need event_sources.enabled, a deployment run\n" +
			"as a release, and authenticated callers (or event_sources.allow_asserted).",
	}
	cmd.AddCommand(
		newEventSourceCreateCommand(g),
		newEventSourceListCommand(g),
		newEventSourceGetCommand(g),
		newEventSourceUpdateCommand(g),
		newEventSourcePauseCommand(g),
		newEventSourceResumeCommand(g),
		newEventSourceDeleteCommand(g),
		newEventSourceEvaluateCommand(g),
		newEventSourceImagesCommand(g),
	)
	return cmd
}

// sourceFlags are the fields a command may set without a file.
type sourceFlags struct {
	file, orkflow, version, uses, description, pool, format, delivery string
	with                                                              []string
	instances, concurrency, rate                                      int32
}

func (f *sourceFlags) register(cmd *cobra.Command, create bool) {
	cmd.Flags().StringVarP(&f.file, "file", "f", "", "read the source from this kind: EventSource document")
	if create {
		cmd.Flags().StringVar(&f.orkflow, "orkflow", "", "the Orkflow each message starts, fixed at creation")
	}
	cmd.Flags().StringVar(&f.version, "version", "", "the Orkflow version to pin (default: the current one, for each message)")
	cmd.Flags().StringVar(&f.uses, "uses", "", "the source image's registry reference")
	cmd.Flags().StringVar(&f.description, "description", "", "an operator's label")
	cmd.Flags().StringVar(&f.pool, "pool", "", "the pool whose source hosts run the instances")
	cmd.Flags().StringVar(&f.format, "format", "", "how a body is read: bytes or json")
	cmd.Flags().StringVar(&f.delivery, "delivery", "", "the image's delivery class: at-least-once or at-most-once")
	cmd.Flags().StringArrayVar(&f.with, "with", nil, "an image setting, KEY=VALUE, as a string; repeatable")
	cmd.Flags().Int32Var(&f.instances, "instances", 0, "how many containers of the source run")
	cmd.Flags().Int32Var(&f.concurrency, "concurrency", 0, "messages one instance handles at once")
	cmd.Flags().Int32Var(&f.rate, "rate", 0, "messages a second one instance starts runs for")
}

// apply writes the flags a command was given onto src.
func (f *sourceFlags) apply(cmd *cobra.Command, src *orkstackv1.EventSource) error {
	changed := cmd.Flags().Changed
	if changed("orkflow") {
		src.OrkflowName = f.orkflow
	}
	if changed("version") {
		src.OrkflowVersion = f.version
	}
	if changed("uses") {
		src.Uses = f.uses
	}
	if changed("description") {
		src.Description = f.description
	}
	if changed("pool") {
		src.Pool = f.pool
	}
	if changed("instances") {
		src.Instances = f.instances
	}
	if changed("concurrency") {
		src.Concurrency = f.concurrency
	}
	if changed("rate") {
		src.Rate = f.rate
	}
	if changed("format") {
		value, ok := map[string]orkstackv1.MessageFormat{"bytes": orkstackv1.MessageFormat_MESSAGE_FORMAT_BYTES, "json": orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON}[f.format]
		if !ok {
			return fmt.Errorf("--format is bytes or json")
		}
		src.Format = value
	}
	if changed("delivery") {
		value, ok := map[string]orkstackv1.DeliveryClass{"at-least-once": orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_LEAST_ONCE, "at-most-once": orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE}[f.delivery]
		if !ok {
			return fmt.Errorf("--delivery is at-least-once or at-most-once")
		}
		src.Delivery = value
	}
	if len(f.with) > 0 {
		if src.With == nil {
			src.With = &structpb.Struct{Fields: map[string]*structpb.Value{}}
		}
		for _, kv := range f.with {
			key, value, ok := strings.Cut(kv, "=")
			if !ok || key == "" {
				return fmt.Errorf("--with %q: want KEY=VALUE", kv)
			}
			src.With.Fields[key] = structpb.NewStringValue(value)
		}
	}
	return nil
}

// sourceFromFile reads --file, a kind: EventSource document, through the
// strict mapping, and holds its namespace to -n and its name to the
// command's: a document that names one is acted on there, and a flag or
// argument naming another is refused, naming both. name is "" where the
// command takes none (a candidate evaluation).
func (g *globalFlags) sourceFromFile(cmd *cobra.Command, path, name string) (*orkstackv1.EventSource, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	doc, err := automation.ParseEventSourceFile(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	src := doc.Source
	switch {
	case !doc.HasNamespace:
		src.Namespace = g.namespace
	case cmd.Flags().Changed("namespace") && resolvedNamespace(g.namespace) != resolvedNamespace(doc.Namespace):
		return nil, fmt.Errorf("%s: %s: the document names namespace %q and -n names %q; give one",
			path, eventsources.ReasonNamespaceMismatch, doc.Namespace, g.namespace)
	default:
		src.Namespace = doc.Namespace
	}
	switch {
	case name == "":
	case doc.Name == "":
		src.Name = name
	case doc.Name != name:
		return nil, fmt.Errorf("%s: the document names event source %q and the command names %q; give one",
			path, terminalSafe(doc.Name), terminalSafe(name))
	}
	return src, nil
}

func newEventSourceCreateCommand(g *globalFlags) *cobra.Command {
	var flags sourceFlags
	var consent string
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create an event source",
		Long: "create stores a new event source after checking it against its Orkflow's\n" +
			"declared inputs, its image's declared interface, the namespace's pools and\n" +
			"secrets, and the deployment's quotas. Its name and its Orkflow are fixed.\n\n" +
			"Give the source as flags, or as a kind: EventSource document (--file);\n" +
			"flags given beside a document override it.",
		Example: "  orkctl event-source create orders --orkflow handle-order --uses ghcr.io/acme/source:1 --with stream=ORDERS\n" +
			"  orkctl event-source create orders -n payments --file orders.yaml",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			src := &orkstackv1.EventSource{Namespace: g.namespace}
			if flags.file != "" {
				var err error
				if src, err = g.sourceFromFile(cmd, flags.file, args[0]); err != nil {
					return err
				}
			}
			src.Name = args[0]
			if err := flags.apply(cmd, src); err != nil {
				return err
			}
			if src.GetOrkflowName() == "" || src.GetUses() == "" {
				return fmt.Errorf("an event source needs --orkflow and --uses (or orkflow: and uses: in --file)")
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.eventSourceClient().CreateEventSource(ctx, connect.NewRequest(&orkstackv1.CreateEventSourceRequest{EventSource: src, Consent: consent}))
			if err != nil {
				return rpcError("create event source", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			renderSourceSaved(cmd.OutOrStdout(), "Created", resp.Msg.GetEventSource(), resp.Msg.GetSaveWarnings())
			return nil
		},
	}
	flags.register(cmd, true)
	addConsentFlag(cmd, &consent, false)
	return cmd
}

func newEventSourceUpdateCommand(g *globalFlags) *cobra.Command {
	var flags sourceFlags
	var consent string
	var rebind bool
	cmd := &cobra.Command{
		Use:   "update <name>",
		Short: "Change an event source",
		Long: "update reads the source, applies the flags given, and writes it back under\n" +
			"the version it read. With --file it replaces every field you write with the\n" +
			"file's. Its name and its Orkflow cannot change.\n\n" +
			"A change of the Orkflow version, the constants, the mapping or the artifacts\n" +
			"makes you the source's binder; --rebind does without changing anything else.",
		Example: "  orkctl event-source update orders --rate 20\n" +
			"  orkctl event-source update orders -n payments --file orders.yaml",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			namespace := g.namespace
			var fromFile *orkstackv1.EventSource
			if flags.file != "" {
				var err error
				if fromFile, err = g.sourceFromFile(cmd, flags.file, args[0]); err != nil {
					return err
				}
				namespace = fromFile.GetNamespace()
			}
			got, err := g.eventSourceClient().GetEventSource(ctx, connect.NewRequest(&orkstackv1.GetEventSourceRequest{Namespace: namespace, Name: args[0]}))
			if err != nil {
				return rpcError("read event source", err)
			}
			src := got.Msg.GetEventSource()
			// What it follows now, kept before the flags change src.
			stored := proto.Clone(src).(*orkstackv1.EventSource)
			if fromFile != nil {
				fromFile.Name, fromFile.Etag = args[0], src.GetEtag()
				if fromFile.GetOrkflowName() == "" {
					fromFile.OrkflowName = src.GetOrkflowName()
				}
				src = fromFile
			}
			if err := flags.apply(cmd, src); err != nil {
				return err
			}
			consent, err := g.resolveConsent(cmd, consent, "event-source update "+args[0], stored.GetAsksFor(), stored.GetConsented(),
				src.GetOrkflowVersion() != stored.GetOrkflowVersion())
			if err != nil {
				return err
			}
			resp, err := g.eventSourceClient().UpdateEventSource(ctx, connect.NewRequest(&orkstackv1.UpdateEventSourceRequest{EventSource: src, Consent: consent, Rebind: rebind}))
			if err != nil {
				return rpcError("update event source", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			renderSourceSaved(cmd.OutOrStdout(), "Updated", resp.Msg.GetEventSource(), resp.Msg.GetSaveWarnings())
			return nil
		},
	}
	flags.register(cmd, false)
	addConsentFlag(cmd, &consent, true)
	cmd.Flags().BoolVar(&rebind, "rebind", false, "make yourself the source's binder")
	return cmd
}

func renderSourceSaved(out io.Writer, verb string, src *orkstackv1.EventSource, warnings []string) {
	fmt.Fprintf(out, "%s event source %s/%s (%s), revision %d, judging revision %d.\n",
		verb, src.GetNamespace(), src.GetName(), src.GetId(), src.GetServiceRevision(), src.GetJudgingRevision())
	for _, w := range warnings {
		fmt.Fprintf(out, "warning: %s\n", terminalSafe(w))
	}
	fmt.Fprintln(out, "No source consumes anything in this release: it is stored, and can be evaluated with orkctl event-source evaluate.")
}

func newEventSourceListCommand(g *globalFlags) *cobra.Command {
	var filterTexts *[]string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the event sources of a namespace",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			filters, err := parseFilterFlags(*filterTexts)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.eventSourceClient().ListEventSources(ctx, connect.NewRequest(&orkstackv1.ListEventSourcesRequest{Namespace: g.namespace, Filters: filters}))
			if err != nil {
				return rpcError("list event sources", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			if len(filters) > 0 && len(resp.Msg.GetEventSources()) == 0 {
				fmt.Fprint(cmd.OutOrStdout(), noMatchSentence("event sources", filters))
				return nil
			}
			return renderSourceList(cmd.OutOrStdout(), resp.Msg.GetEventSources())
		},
	}
	filterTexts = addFilterFlag(cmd, &orkstackv1.ListEventSourcesRequest{})
	return cmd
}

func renderSourceList(out io.Writer, list []*orkstackv1.EventSource) error {
	if len(list) == 0 {
		fmt.Fprintln(out, "No event sources.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "NAME\tID\tORKFLOW\tIMAGE\tPOOL\tINSTANCES\tPAUSED")
	for _, src := range list {
		e := eventsources.Effective(src, eventsources.Limits{})
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%d\t%t\n", terminalSafe(src.GetName()), terminalSafe(src.GetId()),
			terminalSafe(src.GetOrkflowName()+"@"+triggerVersionLabel(src.GetOrkflowVersion())), terminalSafe(src.GetUses()),
			terminalSafe(orDash(src.GetPool())), e.GetInstances(), src.GetPaused())
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func newEventSourceGetCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get <name>",
		Short: "Show an event source and its state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.eventSourceClient().GetEventSource(ctx, connect.NewRequest(&orkstackv1.GetEventSourceRequest{Namespace: g.namespace, Name: args[0]}))
			if err != nil {
				return rpcError("read event source", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderSource(cmd.OutOrStdout(), resp.Msg.GetEventSource(), resp.Msg.GetStatus())
		},
	}
}

func renderSource(out io.Writer, src *orkstackv1.EventSource, st *orkstackv1.EventSourceStatus) error {
	e := eventsources.Effective(src, eventsources.Limits{})
	table := newTable(out)
	row := func(k, v string) { fmt.Fprintf(table, "%s\t%s\n", k, v) }
	row("Name", terminalSafe(src.GetNamespace()+"/"+src.GetName()))
	row("Id", terminalSafe(src.GetId()))
	row("State", strings.TrimPrefix(st.GetState().String(), "EVENT_SOURCE_STATE_")+" ("+terminalSafe(st.GetReason())+")")
	row("", terminalSafe(st.GetMessage()))
	row("Orkflow", terminalSafe(src.GetOrkflowName()+"@"+triggerVersionLabel(src.GetOrkflowVersion())))
	row("Image", terminalSafe(src.GetUses())+" → "+terminalSafe(src.GetImage()))
	row("Delivery", deliveryLabel(e.GetDelivery()))
	row("Pool", terminalSafe(orDash(src.GetPool())))
	row("Instances", fmt.Sprintf("%d, %d at once each, %d a second each", e.GetInstances(), e.GetConcurrency(), e.GetRate()))
	row("Revisions", fmt.Sprintf("service %d, judging %d", src.GetServiceRevision(), src.GetJudgingRevision()))
	if p := src.GetPause(); p != nil {
		row("Paused", terminalSafe(p.GetReason())+" — orkctl event-source resume "+src.GetName())
	}
	row("Bound by", terminalSafe(orDash(src.GetBoundBy()))+verifiedNote(src.GetBoundByVerified()))
	row("Created", formatTime(src.GetCreatedAt()))
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if e.GetDelivery() == orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE {
		fmt.Fprintln(out, "At-most-once: a message this source does not accept is lost.")
	}
	return nil
}

func verifiedNote(verified bool) string {
	if verified {
		return " (verified)"
	}
	return " (asserted)"
}

func deliveryLabel(d orkstackv1.DeliveryClass) string {
	if d == orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE {
		return "at-most-once"
	}
	return "at-least-once"
}

func newEventSourcePauseCommand(g *globalFlags) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "pause <name>",
		Short: "Pause an event source",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(reason) == "" {
				return fmt.Errorf("--reason is required: say why the source is paused")
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.eventSourceClient().PauseEventSource(ctx, connect.NewRequest(&orkstackv1.PauseEventSourceRequest{Namespace: g.namespace, Name: args[0], Reason: reason}))
			if err != nil {
				return rpcError("pause event source", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Paused event source %s.\n", args[0])
			for _, w := range resp.Msg.GetWarnings() {
				fmt.Fprintf(cmd.OutOrStdout(), "warning: %s\n", terminalSafe(w))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "why the source is paused (required)")
	return cmd
}

func newEventSourceResumeCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "resume <name>",
		Short: "Resume a paused event source",
		Long:  "resume clears a source's pause after the checks a create makes. It neither\nrebinds nor consents.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.eventSourceClient().ResumeEventSource(ctx, connect.NewRequest(&orkstackv1.ResumeEventSourceRequest{Namespace: g.namespace, Name: args[0]}))
			if err != nil {
				return rpcError("resume event source", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			if notice := resp.Msg.GetNotice(); notice != "" {
				fmt.Fprintln(cmd.OutOrStdout(), terminalSafe(notice))
				return nil
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Resumed event source %s.\n", args[0])
			return nil
		},
	}
}

func newEventSourceDeleteCommand(g *globalFlags) *cobra.Command {
	var id string
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete an event source",
		Long: "delete reads the source the name holds and deletes that source, by its id:\n" +
			"a source created under the name meanwhile is never touched.\n\n" +
			"--id deletes the source of that id without reading it first: the way to\n" +
			"remove a source this engine cannot read (EVENT_SOURCE_DOCUMENT_UNREADABLE).",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			client := g.eventSourceClient()
			target := sourceDelete{Namespace: resolvedNamespace(g.namespace), Name: args[0], ID: id}
			if id != "" && !eventsources.ValidID(id) {
				return fmt.Errorf("--id %q is not an event source's id (evs- and 16 hex digits)", id)
			}
			if id == "" {
				got, err := client.GetEventSource(ctx, connect.NewRequest(&orkstackv1.GetEventSourceRequest{Namespace: g.namespace, Name: args[0]}))
				if err != nil {
					return rpcError("read event source", err)
				}
				src := got.Msg.GetEventSource()
				target = sourceDelete{Namespace: src.GetNamespace(), Name: src.GetName(), ID: src.GetId(),
					Pool: src.GetPool(), Instances: eventsources.Effective(src, eventsources.Limits{}).GetInstances()}
			}
			if err := deleteEventSourceByID(ctx, client, target); err != nil {
				return err
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), &orkstackv1.DeleteEventSourceResponse{})
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted event source %s/%s (%s).\n", target.Namespace, target.Name, target.ID)
			return nil
		},
	}
	cmd.Flags().StringVar(&id, "id", "", "delete the source of this id without reading it first")
	return cmd
}

// sourceDelete is the source a delete means: its id, and what a report of
// a delete that did not finish names.
type sourceDelete struct {
	Namespace, Name, ID, Pool string
	Instances                 int32
}

// errDeleteNotFinished marks a delete whose outcome is not a success.
var errDeleteNotFinished = errors.New("the delete is not finished")

// deleteEventSourceByID is the one rule every client of OrkStack's deletes
// a source by (docs/AUTOMATION.md, "event_sources"): every attempt carries
// the id; a mismatch is refused and nothing else is sent; NOT_FOUND is a
// delete that did not finish, never "already gone"; an outcome that is not
// known is sent again with the same id, five attempts in all; a refusal of
// the request itself fails at once.
func deleteEventSourceByID(ctx context.Context, client orkstackv1connect.EventSourceServiceClient, d sourceDelete) error {
	var last error
	for attempt := 1; attempt <= eventSourceDeleteAttempts; attempt++ {
		_, err := client.DeleteEventSource(ctx, connect.NewRequest(&orkstackv1.DeleteEventSourceRequest{Namespace: d.Namespace, Name: d.Name, Id: d.ID}))
		if err == nil {
			return nil
		}
		last = err
		code := connect.CodeOf(err)
		var ce *connect.Error
		switch {
		case errors.As(err, &ce) && strings.HasPrefix(ce.Message(), eventsources.ReasonIDMismatch):
			return fmt.Errorf("delete event source %s/%s (%s): %s", d.Namespace, d.Name, d.ID, terminalSafe(ce.Message()))
		case code == connect.CodeNotFound && attempt > 1:
			return notFinished(d, "no source of that id is under the name now; an earlier attempt whose answer was lost may have deleted it, but no record says so")
		case code == connect.CodeNotFound:
			return notFinished(d, "no source of that id is under the name, and no record says it was deleted")
		case code == connect.CodePermissionDenied, code == connect.CodeUnauthenticated, code == connect.CodeInvalidArgument, code == connect.CodeFailedPrecondition:
			return rpcError("delete event source", err)
		}
		if attempt < eventSourceDeleteAttempts {
			timer := time.NewTimer(time.Duration(attempt) * 200 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return notFinished(d, ctx.Err().Error())
			case <-timer.C:
			}
		}
	}
	return notFinished(d, stepError(last))
}

func notFinished(d sourceDelete, why string) error {
	pool := d.Pool
	if pool == "" {
		pool = "the namespace's default pool"
	}
	return fmt.Errorf("%w: event source %s/%s (%s): %s; it may still hold %d instance slot(s) in %s — "+
		"see orkctl release status and docs/DEPLOY.md", errDeleteNotFinished, d.Namespace, d.Name, d.ID, terminalSafe(why), d.Instances, pool)
}

func newEventSourceEvaluateCommand(g *globalFlags) *cobra.Command {
	var file, bodyFile, subject, id, group string
	var attributes []string
	cmd := &cobra.Command{
		Use:   "evaluate [<name>]",
		Short: "Run a source's filter and mapping over a message, and say what would happen",
		Long: "evaluate runs a stored source (by name) or a candidate (--file) over the\n" +
			"message given, in the engine, with the code a source host judges by. It\n" +
			"starts nothing and writes nothing.",
		Example: "  orkctl event-source evaluate orders --body-file order.json --attribute Ce-Type=order.created\n" +
			"  orkctl event-source evaluate --file orders.yaml --body-file order.json",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if (len(args) == 1) == (file != "") {
				return fmt.Errorf("name a stored source, or give a candidate with --file")
			}
			req := &orkstackv1.EvaluateEventSourceMessageRequest{Namespace: g.namespace,
				Metadata: &orkstackv1.MessageMetadata{Id: id, Subject: subject, Group: group, PublishedAt: timestamppb.Now()}}
			if len(args) == 1 {
				req.Source = &orkstackv1.EvaluateEventSourceMessageRequest_Name{Name: args[0]}
			} else {
				src, err := g.sourceFromFile(cmd, file, "")
				if err != nil {
					return err
				}
				if src.Name == "" {
					src.Name = "candidate"
				}
				req.Source = &orkstackv1.EvaluateEventSourceMessageRequest_Candidate{Candidate: src}
			}
			if bodyFile != "" {
				body, err := os.ReadFile(bodyFile)
				if err != nil {
					return fmt.Errorf("read %s: %w", bodyFile, err)
				}
				req.Body = body
			}
			for _, kv := range attributes {
				key, value, ok := strings.Cut(kv, "=")
				if !ok || key == "" {
					return fmt.Errorf("--attribute %q: want KEY=VALUE", kv)
				}
				if req.Metadata.Attributes == nil {
					req.Metadata.Attributes = map[string]*orkstackv1.AttributeValues{}
				}
				if req.Metadata.Attributes[key] == nil {
					req.Metadata.Attributes[key] = &orkstackv1.AttributeValues{}
				}
				req.Metadata.Attributes[key].Values = append(req.Metadata.Attributes[key].Values, value)
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.eventSourceClient().EvaluateEventSourceMessage(ctx, connect.NewRequest(req))
			if err != nil {
				return rpcError("evaluate", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderEvaluation(cmd.OutOrStdout(), resp.Msg)
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "a candidate source to evaluate, as a kind: EventSource document")
	cmd.Flags().StringVar(&bodyFile, "body-file", "", "the message's body")
	cmd.Flags().StringArrayVar(&attributes, "attribute", nil, "a message attribute, KEY=VALUE; repeatable")
	cmd.Flags().StringVar(&subject, "subject", "", "the message's subject")
	cmd.Flags().StringVar(&id, "id", "", "the message's identity")
	cmd.Flags().StringVar(&group, "group", "", "the message's group")
	return cmd
}

func renderEvaluation(out io.Writer, r *orkstackv1.EvaluateEventSourceMessageResponse) error {
	fmt.Fprintf(out, "Outcome: %s", strings.TrimPrefix(r.GetOutcome().String(), "EVALUATION_OUTCOME_"))
	if r.GetReason() != "" {
		fmt.Fprintf(out, " (%s)", terminalSafe(r.GetReason()))
	}
	fmt.Fprintln(out)
	if r.GetDetail() != "" {
		fmt.Fprintln(out, terminalSafe(r.GetDetail()))
	}
	for _, t := range r.GetTests() {
		answer := "did not hold"
		if t.GetHeld() {
			answer = "held"
		}
		selected := t.GetSelected()
		if selected == "" {
			selected = "nothing"
		}
		fmt.Fprintf(out, "  match[%d] %s: %s (selected %s)\n", t.GetIndex(), terminalSafe(t.GetSelector()), answer, terminalSafe(selected))
	}
	if r.GetInput() != nil {
		line, err := protoJSONLine(r.GetInput().GetData())
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "Run input: %s\n", terminalSafe(line))
	}
	if len(r.GetArtifacts()) > 0 {
		fmt.Fprintf(out, "Artifacts: %s\n", terminalSafe(strings.Join(r.GetArtifacts(), ", ")))
	}
	if v := r.GetOrkflowVersion(); v != "" {
		fmt.Fprintf(out, "Judged against Orkflow version %s.\n", terminalSafe(v))
	}
	return nil
}

func protoJSONLine(m proto.Message) (string, error) {
	var b strings.Builder
	if err := writeJSON(&b, m); err != nil {
		return "", err
	}
	return strings.Join(strings.Fields(b.String()), " "), nil
}

func newEventSourceImagesCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "images [<reference>]",
		Short: "Show what a source image declares",
		Long: "images reads one source image's labels: the contract and profile versions it\n" +
			"implements and its declared interface. With no reference it lists the\n" +
			"first-party source images this release ships, which are none yet.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref := ""
			if len(args) == 1 {
				ref = args[0]
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.eventSourceClient().InspectSourceImage(ctx, connect.NewRequest(&orkstackv1.InspectSourceImageRequest{Namespace: g.namespace, Reference: ref}))
			if err != nil {
				return rpcError("inspect source image", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderSourceImages(cmd.OutOrStdout(), resp.Msg.GetImages())
		},
	}
}

func renderSourceImages(out io.Writer, images []*orkstackv1.SourceImage) error {
	if len(images) == 0 {
		fmt.Fprintln(out, "No first-party source images ship with this release.")
		return nil
	}
	for _, img := range images {
		fmt.Fprintf(out, "%s → %s\n", terminalSafe(img.GetReference()), terminalSafe(img.GetImage()))
		fmt.Fprintf(out, "  contract %s, event-source profile %s\n", joinInt32(img.GetContractVersions()), joinInt32(img.GetProfileVersions()))
		iface := img.GetInterface()
		if iface == nil {
			fmt.Fprintln(out, "  declares no interface: any with: is accepted")
			continue
		}
		decl := iface.GetEventSource()
		fmt.Fprintf(out, "  delivery %s, at most %d at once\n", deliveryLabel(decl.GetDelivery()), max(decl.GetMaxConcurrency(), 1))
		keys := make([]string, 0, len(iface.GetWith()))
		for k := range iface.GetWith() {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			f := iface.GetWith()[key]
			required := ""
			if f.GetRequired() {
				required = ", required"
			}
			fmt.Fprintf(out, "  with.%s: %s%s\n", terminalSafe(key), f.GetType(), required)
		}
	}
	return nil
}

func joinInt32(vs []int32) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = strconv.Itoa(int(v))
	}
	return strings.Join(parts, ",")
}
