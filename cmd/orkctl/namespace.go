package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func newNamespaceCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "namespace",
		Aliases: []string{"ns"},
		Short:   "Manage namespaces",
		Long: "A namespace scopes orkflow names, runs and pool defaults. Two namespaces\n" +
			"may each hold an orkflow called \"deploy\" and never see each other's.",
	}
	cmd.AddCommand(
		newNamespaceCreateCommand(g),
		newNamespaceListCommand(g),
		newNamespaceGetCommand(g),
		newNamespaceUpdateCommand(g),
		newNamespaceDeleteCommand(g),
		newNamespaceUseCommand(g),
	)
	return cmd
}

func newNamespaceCreateCommand(g *globalFlags) *cobra.Command {
	var (
		description         string
		defaultPool         string
		allowedPools        []string
		labels              []string
		runRetention        string
		telemetrySink       string
		taskLogs            string
		telemetryAttributes []string
	)
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Register a namespace",
		Long: "create registers a namespace. It is idempotent for an identical spec, so a\n" +
			"provisioning script can be re-run; a second create that differs is refused\n" +
			"and says which field differs.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			parsed, err := parseLabels(labels)
			if err != nil {
				return err
			}
			retention, err := parseRunRetentionFlag(runRetention)
			if err != nil {
				return err
			}
			telemetry, err := applyTelemetryFlags(cmd, nil, telemetrySink, taskLogs, telemetryAttributes)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.namespaceClient().CreateNamespace(ctx, connect.NewRequest(
				&orkstackv1.CreateNamespaceRequest{Namespace: &orkstackv1.Namespace{
					Name:         args[0],
					Description:  description,
					Labels:       parsed,
					DefaultPool:  defaultPool,
					AllowedPools: allowedPools,
					RunRetention: retention,
					Telemetry:    telemetry,
				}},
			))
			if err != nil {
				return rpcError("create namespace", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			verb := "Created"
			if !resp.Msg.GetCreated() {
				verb = "Unchanged"
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s namespace %s\n",
				verb, resp.Msg.GetNamespace().GetName())
			return renderNamespace(cmd.OutOrStdout(), resp.Msg.GetNamespace())
		},
	}
	namespaceSpecFlags(cmd, &description, &defaultPool, &allowedPools, &labels, &runRetention,
		&telemetrySink, &taskLogs, &telemetryAttributes)
	return cmd
}

func newNamespaceListCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List namespaces",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.namespaceClient().ListNamespaces(ctx, connect.NewRequest(
				&orkstackv1.ListNamespacesRequest{},
			))
			if err != nil {
				return rpcError("list namespaces", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderNamespaceList(cmd.OutOrStdout(), resp.Msg.GetNamespaces())
		},
	}
}

func newNamespaceGetCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get [name]",
		Short: "Show one namespace",
		Long: "get shows one namespace. With no argument it shows the one this command\n" +
			"would act in — the --namespace flag, ORK_NAMESPACE, or the saved default.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := g.namespace
			if len(args) == 1 {
				name = args[0]
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.namespaceClient().GetNamespace(ctx, connect.NewRequest(
				&orkstackv1.GetNamespaceRequest{Name: name},
			))
			if err != nil {
				return rpcError("get namespace", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderNamespace(cmd.OutOrStdout(), resp.Msg.GetNamespace())
		},
	}
}

func newNamespaceUpdateCommand(g *globalFlags) *cobra.Command {
	var (
		description         string
		defaultPool         string
		allowedPools        []string
		labels              []string
		runRetention        string
		telemetrySink       string
		taskLogs            string
		telemetryAttributes []string
	)
	cmd := &cobra.Command{
		Use:   "update <name>",
		Short: "Change a namespace",
		Long: "update reads the namespace, applies the flags given, and writes it back\n" +
			"compare-and-swapping on the version it read. A flag left out keeps its\n" +
			"current value; if somebody else changed the namespace in between, the\n" +
			"write is refused rather than silently overwriting theirs.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			parsed, err := parseLabels(labels)
			if err != nil {
				return err
			}
			retention, err := parseRunRetentionFlag(runRetention)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			client := g.namespaceClient()
			// Read first: an update sends the whole document back, so the
			// fields the user did not mention have to come from somewhere, and
			// the only honest source is the current document. The etag comes
			// with it, which is what makes the write a compare-and-swap.
			current, err := client.GetNamespace(ctx, connect.NewRequest(
				&orkstackv1.GetNamespaceRequest{Name: args[0]},
			))
			if err != nil {
				return rpcError("update namespace", err)
			}
			edited := current.Msg.GetNamespace()
			telemetry, err := applyTelemetryFlags(cmd, edited.GetTelemetry(), telemetrySink, taskLogs, telemetryAttributes)
			if err != nil {
				return err
			}
			if cmd.Flags().Changed("description") {
				edited.Description = description
			}
			if cmd.Flags().Changed("default-pool") {
				edited.DefaultPool = defaultPool
			}
			if cmd.Flags().Changed("allow-pool") {
				edited.AllowedPools = allowedPools
			}
			if cmd.Flags().Changed("label") {
				edited.Labels = parsed
			}
			if cmd.Flags().Changed("run-retention") {
				edited.RunRetention = retention
			}
			edited.Telemetry = telemetry

			resp, err := client.UpdateNamespace(ctx, connect.NewRequest(
				&orkstackv1.UpdateNamespaceRequest{Namespace: edited},
			))
			if err != nil {
				return rpcError("update namespace", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Updated namespace %s\n",
				resp.Msg.GetNamespace().GetName())
			return renderNamespace(cmd.OutOrStdout(), resp.Msg.GetNamespace())
		},
	}
	namespaceSpecFlags(cmd, &description, &defaultPool, &allowedPools, &labels, &runRetention,
		&telemetrySink, &taskLogs, &telemetryAttributes)
	return cmd
}

func newNamespaceDeleteCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Remove an empty namespace",
		Long: "delete removes a namespace that holds no orkflows. One that still holds any\n" +
			"is refused, and the refusal names them: removing the namespace would not\n" +
			"remove the definitions, and they would survive where nothing lists them.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.namespaceClient().DeleteNamespace(ctx, connect.NewRequest(
				&orkstackv1.DeleteNamespaceRequest{Name: args[0]},
			))
			if err != nil {
				return rpcError("delete namespace", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted namespace %s\n", args[0])
			return nil
		},
	}
}

func newNamespaceUseCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "use <name>",
		Short: "Remember a namespace as the default for later commands",
		Long: "use saves a namespace in orkctl's config file so it does not have to be\n" +
			"typed on every command. --namespace and ORK_NAMESPACE still win over it.\n\n" +
			"The namespace is read from the engine first: saving a name that does not\n" +
			"exist would make every later command fail for a reason recorded in a file\n" +
			"the user has stopped thinking about.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.namespaceClient().GetNamespace(ctx, connect.NewRequest(
				&orkstackv1.GetNamespaceRequest{Name: args[0]},
			))
			if err != nil {
				return rpcError("use namespace", err)
			}
			cancel()

			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			cfg.Namespace = resp.Msg.GetNamespace().GetName()
			path, err := saveConfig(cfg)
			if err != nil {
				return err
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Now using namespace %s (saved in %s)\n",
				cfg.Namespace, path)
			return nil
		},
	}
}

// namespaceSpecFlags registers the flags that describe a namespace. create and
// update share them so the two commands cannot drift apart.
func namespaceSpecFlags(
	cmd *cobra.Command,
	description, defaultPool *string,
	allowedPools, labels *[]string,
	runRetention *string,
	telemetrySink, taskLogs *string,
	telemetryAttributes *[]string,
) {
	cmd.Flags().StringVar(description, "description", "", "human description")
	cmd.Flags().StringVar(defaultPool, "default-pool", "",
		"pool a task of this namespace runs on when it names none (default \"main\")")
	cmd.Flags().StringArrayVar(allowedPools, "allow-pool", nil,
		"pool this namespace may dispatch onto (repeatable; empty = unrestricted)")
	cmd.Flags().StringArrayVar(labels, "label", nil, "label as key=value (repeatable)")
	cmd.Flags().StringVar(runRetention, "run-retention", "",
		"how long a finished run stays readable, e.g. \"168h\" "+
			"(default: the deployment maximum; \"inherit\" clears a value)")
	cmd.Flags().StringVar(telemetrySink, "telemetry-sink", "",
		"telemetry sink this namespace's runs use, by name, from `orkctl system telemetry-sinks` "+
			"(default: the deployment's default sink)")
	cmd.Flags().StringVar(taskLogs, "task-logs", "inherit",
		"override the Orker's task-log export for this namespace's runs: enabled, disabled, or inherit")
	cmd.Flags().StringArrayVar(telemetryAttributes, "telemetry-attribute", nil,
		"attribute stamped on every signal of this namespace's runs, as key=value (repeatable; "+
			"on update, replaces the whole set)")
}

// parseTaskLogsFlag turns --task-logs's raw value into the TaskLogExport it
// becomes. "inherit" (and, for create, simply leaving the flag out) means the
// namespace overrides nothing — the Orker's own ORK_ORKER_TASK_LOG_EXPORT
// applies — which is exactly what TASK_LOG_EXPORT_UNSPECIFIED means on the
// wire (proto/orkstack/v1/telemetry.proto).
func parseTaskLogsFlag(raw string) (orkstackv1.TaskLogExport, error) {
	switch raw {
	case "", "inherit":
		return orkstackv1.TaskLogExport_TASK_LOG_EXPORT_UNSPECIFIED, nil
	case "enabled":
		return orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED, nil
	case "disabled":
		return orkstackv1.TaskLogExport_TASK_LOG_EXPORT_DISABLED, nil
	default:
		return orkstackv1.TaskLogExport_TASK_LOG_EXPORT_UNSPECIFIED, fmt.Errorf(
			"invalid --task-logs %q: want one of enabled, disabled, inherit", raw)
	}
}

// applyTelemetryFlags builds the TelemetryPolicy a create or update sends,
// starting from base (nil for create; the namespace's current policy for
// update) and overwriting only the fields whose flag the caller actually
// passed — the same "a flag left out keeps its current value" rule
// namespaceSpecFlags's other fields already follow, drawn per-field rather
// than for the policy as a whole so `--telemetry-sink` alone does not reset
// `--telemetry-attribute` back to empty.
//
// When none of the three flags were passed, base is returned unchanged
// (nil for create, meaning "no policy at all" — inherited end to end).
func applyTelemetryFlags(
	cmd *cobra.Command,
	base *orkstackv1.TelemetryPolicy,
	sinkRaw, taskLogsRaw string,
	attributePairs []string,
) (*orkstackv1.TelemetryPolicy, error) {
	sinkChanged := cmd.Flags().Changed("telemetry-sink")
	taskLogsChanged := cmd.Flags().Changed("task-logs")
	attributesChanged := cmd.Flags().Changed("telemetry-attribute")
	if !sinkChanged && !taskLogsChanged && !attributesChanged {
		return base, nil
	}
	policy := &orkstackv1.TelemetryPolicy{}
	if base != nil {
		policy.Sink = base.GetSink()
		policy.TaskLogs = base.GetTaskLogs()
		policy.Attributes = base.GetAttributes()
	}
	if sinkChanged {
		// An explicit empty string is not "leave alone" — it is the
		// documented reset to the deployment's default sink.
		policy.Sink = sinkRaw
	}
	if taskLogsChanged {
		taskLogs, err := parseTaskLogsFlag(taskLogsRaw)
		if err != nil {
			return nil, err
		}
		policy.TaskLogs = taskLogs
	}
	if attributesChanged {
		attributes, err := parseKeyValuePairs("telemetry-attribute", attributePairs)
		if err != nil {
			return nil, err
		}
		policy.Attributes = attributes
	}
	return policy, nil
}

// parseRunRetentionFlag turns --run-retention's raw value into the proto
// field it becomes. Blank or "inherit" means the namespace sets none of its
// own — the deployment maximum applies, which create leaves unset and
// update clears explicitly. Anything else is Go's time.ParseDuration syntax,
// held to the same positive rule the write path (internal/api) enforces
// again — refused here so a bad flag is a local error, not a round trip.
func parseRunRetentionFlag(raw string) (*durationpb.Duration, error) {
	if raw == "" || raw == "inherit" {
		return nil, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return nil, fmt.Errorf("parse --run-retention %q: %w", raw, err)
	}
	if d <= 0 {
		return nil, fmt.Errorf("--run-retention must be a positive duration, got %s", d)
	}
	return durationpb.New(d), nil
}

// parseLabels turns repeated --label key=value flags into a map.
func parseLabels(pairs []string) (map[string]string, error) {
	return parseKeyValuePairs("label", pairs)
}

// parseKeyValuePairs turns any repeated key=value flag into a map. flag names
// the flag in the error message, so --label and --telemetry-attribute each
// report their own name rather than a generic one.
func parseKeyValuePairs(flag string, pairs []string) (map[string]string, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	values := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		key, value, found := strings.Cut(pair, "=")
		if !found || key == "" {
			return nil, fmt.Errorf("invalid --%s %q: want key=value", flag, pair)
		}
		values[key] = value
	}
	return values, nil
}

func renderNamespaceList(out io.Writer, namespaces []*orkstackv1.Namespace) error {
	if len(namespaces) == 0 {
		fmt.Fprintln(out, "No namespaces.")
		return nil
	}
	table := newTable(out)
	fmt.Fprintln(table, "NAME\tDEFAULT POOL\tALLOWED POOLS\tRUN RETENTION\tCREATED\tDESCRIPTION")
	for _, ns := range namespaces {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n",
			ns.GetName(), defaultPoolLabel(ns), joinOrDash(ns.GetAllowedPools()),
			runRetentionLabel(ns), formatTime(ns.GetCreatedAt()), orDash(ns.GetDescription()))
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func renderNamespace(out io.Writer, ns *orkstackv1.Namespace) error {
	table := newTable(out)
	fmt.Fprintf(table, "Name:\t%s\n", ns.GetName())
	fmt.Fprintf(table, "Description:\t%s\n", orDash(ns.GetDescription()))
	fmt.Fprintf(table, "Default pool:\t%s\n", defaultPoolLabel(ns))
	fmt.Fprintf(table, "Allowed pools:\t%s\n", allowedPoolsLabel(ns))
	fmt.Fprintf(table, "Run retention:\t%s\n", runRetentionLabel(ns))
	fmt.Fprintf(table, "Telemetry sink:\t%s\n", telemetrySinkLabel(ns))
	fmt.Fprintf(table, "Task logs:\t%s\n", taskLogsLabel(ns))
	fmt.Fprintf(table, "Telemetry attributes:\t%s\n", telemetryAttributesLabel(ns))
	fmt.Fprintf(table, "Labels:\t%s\n", labelsLabel(ns))
	fmt.Fprintf(table, "Created:\t%s\n", formatTime(ns.GetCreatedAt()))
	fmt.Fprintf(table, "Updated:\t%s\n", formatTime(ns.GetUpdatedAt()))
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// defaultPoolLabel renders an unset default pool as the pool it means, so a
// reader is never shown a dash where a task actually goes somewhere.
func defaultPoolLabel(ns *orkstackv1.Namespace) string {
	if pool := ns.GetDefaultPool(); pool != "" {
		return pool
	}
	return "main"
}

// allowedPoolsLabel spells out what an empty allow-list means: not "none", but
// "no restriction". A dash here would read as the opposite of the truth.
func allowedPoolsLabel(ns *orkstackv1.Namespace) string {
	if pools := ns.GetAllowedPools(); len(pools) > 0 {
		return strings.Join(pools, ",")
	}
	return "(unrestricted)"
}

// runRetentionLabel renders an unset run_retention as what it means: this
// namespace inherits the deployment maximum, never a dash that could be
// mistaken for "no retention".
func runRetentionLabel(ns *orkstackv1.Namespace) string {
	if d := ns.GetRunRetention(); d != nil {
		return d.AsDuration().String()
	}
	return "inherited (deployment maximum)"
}

// telemetrySinkLabel renders the sink a namespace's telemetry policy names:
// an unset policy or an empty sink both mean the deployment's default sink,
// so both print "default (inherited)" rather than a dash a reader could
// mistake for "no telemetry".
func telemetrySinkLabel(ns *orkstackv1.Namespace) string {
	if sink := ns.GetTelemetry().GetSink(); sink != "" {
		return sink
	}
	return "default (inherited)"
}

// taskLogsLabel renders a namespace's task-log override: UNSPECIFIED (no
// policy, or a policy that does not set it) means the Orker's own default
// applies, never "disabled" — an override this namespace never asked for is
// not the same as one that explicitly turned logs off.
func taskLogsLabel(ns *orkstackv1.Namespace) string {
	switch ns.GetTelemetry().GetTaskLogs() {
	case orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED:
		return "enabled"
	case orkstackv1.TaskLogExport_TASK_LOG_EXPORT_DISABLED:
		return "disabled"
	default:
		return "inherited (Orker default)"
	}
}

// telemetryAttributesLabel renders a namespace's telemetry attributes the
// same way labelsLabel renders labels — sorted key=value pairs, or a dash
// for none.
func telemetryAttributesLabel(ns *orkstackv1.Namespace) string {
	return sortedPairsLabel(ns.GetTelemetry().GetAttributes())
}

func labelsLabel(ns *orkstackv1.Namespace) string {
	return sortedPairsLabel(ns.GetLabels())
}

// sortedPairsLabel renders a string-to-string map as "key=value" pairs,
// joined by commas in sorted-key order, or a dash for an empty map —
// telemetryAttributesLabel's and labelsLabel's shared body, since both
// render the identical shape for two different maps.
func sortedPairsLabel(m map[string]string) string {
	if len(m) == 0 {
		return dash
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, key+"="+m[key])
	}
	return strings.Join(pairs, ",")
}
