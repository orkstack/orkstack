package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/artifacts"
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
		allowedSecrets      []string
		labels              []string
		runRetention        string
		telemetrySink       string
		taskLogs            string
		telemetryAttributes []string
		holdReason          string
		maxRunning          int32
		budget              budgetFlagValues
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
			admission := &orkstackv1.Namespace{}
			if err := applyAdmissionFlags(cmd, admission, holdReason, maxRunning); err != nil {
				return err
			}
			if err := checkAllowSecretFlags(allowedSecrets); err != nil {
				return err
			}
			budgetSpec, err := applyBudgetFlags(cmd, nil, budget, false)
			if err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()

			resp, err := g.namespaceClient().CreateNamespace(ctx, connect.NewRequest(
				&orkstackv1.CreateNamespaceRequest{Namespace: &orkstackv1.Namespace{
					Name:           args[0],
					Description:    description,
					Labels:         parsed,
					DefaultPool:    defaultPool,
					AllowedPools:   allowedPools,
					RunRetention:   retention,
					AllowedSecrets: allowedSecrets,
					Telemetry:      telemetry,
					Hold:           admission.GetHold(),
					MaxRunning:     admission.GetMaxRunning(),
					Budget:         budgetSpec,
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
	namespaceSpecFlags(cmd, &description, &defaultPool, &allowedPools, &allowedSecrets, &labels, &runRetention,
		&telemetrySink, &taskLogs, &telemetryAttributes, &holdReason, &maxRunning, &budget)
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
		allowedSecrets      []string
		labels              []string
		runRetention        string
		telemetrySink       string
		taskLogs            string
		telemetryAttributes []string
		holdReason          string
		maxRunning          int32
		releaseHold         bool
		clearSecrets        bool
		budget              budgetFlagValues
		clearBudget         bool
	)
	cmd := &cobra.Command{
		Use:   "update <name>",
		Short: "Change a namespace",
		Long: "update reads the namespace, applies the flags given, and writes it back\n" +
			"compare-and-swapping on the version it read. A flag left out keeps its\n" +
			"current value; if somebody else changed the namespace in between, the\n" +
			"write is refused rather than silently overwriting theirs.\n\n" +
			"--hold REASON closes the namespace to new runs (runs already accepted\n" +
			"finish untouched); --release-hold reopens it.\n\n" +
			"--budget-cpu-seconds/--budget-gib-seconds/--budget-period bound the\n" +
			"namespace's RESERVED cpu and memory over a rolling period; --budget-enforce\n" +
			"refuses new runs once it is reached, --budget-enforce=false only reports it,\n" +
			"and --clear-budget removes it.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if releaseHold && cmd.Flags().Changed("hold") {
				return fmt.Errorf("--hold and --release-hold contradict each other: give one")
			}
			if clearSecrets && cmd.Flags().Changed("allow-secret") {
				return fmt.Errorf("--allow-secret and --clear-allowed-secrets contradict each other: give one")
			}
			if err := checkAllowSecretFlags(allowedSecrets); err != nil {
				return err
			}
			// The budget flags are checked on a scratch document too, so a
			// contradiction or a bad number never costs a round trip.
			if _, err := applyBudgetFlags(cmd, &orkstackv1.Budget{Period: durationpb.New(time.Hour)}, budget, clearBudget); err != nil {
				return err
			}
			// The admission flags are checked before any round trip, on a
			// scratch document, so a blank reason or a negative cap is a local
			// error; they are applied to the real one after it is read.
			if err := applyAdmissionFlags(cmd, &orkstackv1.Namespace{}, holdReason, maxRunning); err != nil {
				return err
			}
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
			if cmd.Flags().Changed("allow-secret") {
				edited.AllowedSecrets = allowedSecrets
			}
			if clearSecrets {
				edited.AllowedSecrets = nil
			}
			if cmd.Flags().Changed("label") {
				edited.Labels = parsed
			}
			if cmd.Flags().Changed("run-retention") {
				edited.RunRetention = retention
			}
			edited.Telemetry = telemetry
			if err := applyAdmissionFlags(cmd, edited, holdReason, maxRunning); err != nil {
				return err
			}
			if releaseHold {
				edited.Hold = nil
			}
			edited.Budget, err = applyBudgetFlags(cmd, edited.GetBudget(), budget, clearBudget)
			if err != nil {
				return err
			}

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
	namespaceSpecFlags(cmd, &description, &defaultPool, &allowedPools, &allowedSecrets, &labels, &runRetention,
		&telemetrySink, &taskLogs, &telemetryAttributes, &holdReason, &maxRunning, &budget)
	cmd.Flags().BoolVar(&clearBudget, "clear-budget", false,
		"remove the namespace's budget, leaving it unbounded")
	cmd.Flags().BoolVar(&releaseHold, "release-hold", false,
		"reopen a held namespace to new runs (nothing refused while it was held is started)")
	cmd.Flags().BoolVar(&clearSecrets, "clear-allowed-secrets", false,
		"remove the namespace's secret-source list, leaving it unrestricted "+
			"(each Orker's ORK_ORKER_SECRET_ALLOW still applies)")
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
	allowedPools, allowedSecrets, labels *[]string,
	runRetention *string,
	telemetrySink, taskLogs *string,
	telemetryAttributes *[]string,
	holdReason *string,
	maxRunning *int32,
	budget *budgetFlagValues,
) {
	cmd.Flags().StringVar(description, "description", "", "human description")
	cmd.Flags().StringVar(defaultPool, "default-pool", "",
		"pool a task of this namespace runs on when it names none (default \"main\")")
	cmd.Flags().StringArrayVar(allowedPools, "allow-pool", nil,
		"pool this namespace may dispatch onto (repeatable; empty = unrestricted)")
	cmd.Flags().StringArrayVar(allowedSecrets, "allow-secret", nil,
		"secret source this namespace's tasks may name, as scheme:pattern, e.g. 'env:SANDBOX_*' "+
			"(repeatable; empty = unrestricted; on update, replaces the whole set)")
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
	cmd.Flags().StringVar(holdReason, "hold", "",
		"close this namespace to new runs, with the reason the next operator reads "+
			"(runs already accepted finish untouched)")
	cmd.Flags().Int32Var(maxRunning, "max-running", 0,
		"most runs of this namespace in flight at once (0 = unlimited)")
	cmd.Flags().Int64Var(&budget.cpuSeconds, "budget-cpu-seconds", 0,
		"bound on the namespace's RESERVED cpu-seconds (cpu x attempt time) over --budget-period; 0 unbounds the dimension")
	cmd.Flags().Int64Var(&budget.gibSeconds, "budget-gib-seconds", 0,
		"bound on the namespace's RESERVED GiB-seconds (memory x attempt time) over --budget-period; 0 unbounds the dimension")
	cmd.Flags().StringVar(&budget.period, "budget-period", "",
		fmt.Sprintf("the rolling period a budget is held against, e.g. 720h (%s to %s)",
			artifacts.BudgetMinPeriod, artifacts.BudgetMaxPeriod))
	cmd.Flags().BoolVar(&budget.enforce, "budget-enforce", false,
		"refuse new runs once the budget is reached (=false only reports it); check that "+
			"`orkctl stats usage --since <period>` answers before enforcing: an enforced budget "+
			"that cannot be measured refuses every submit")
}

// budgetFlagValues holds the four --budget-* flags create and update share.
type budgetFlagValues struct {
	cpuSeconds int64
	gibSeconds int64
	period     string
	enforce    bool
}

// budgetFlagNames are the flags that edit a budget, in help order.
var budgetFlagNames = []string{"budget-cpu-seconds", "budget-gib-seconds", "budget-period", "budget-enforce"}

// applyBudgetFlags builds the budget a create or update sends, starting from
// current (nil for create; the stored budget for update) and overwriting
// only the fields whose flag was given — the "a flag left out keeps its
// current value" rule. clear removes the budget and contradicts every
// --budget-* flag. A negative number, an unparsable period, or a budget with
// no period is a local error; every other rule (the period's bounds, at
// least one bounded dimension) is the registry's, and its refusal prints
// through rpcError.
func applyBudgetFlags(cmd *cobra.Command, current *orkstackv1.Budget, values budgetFlagValues, clear bool) (*orkstackv1.Budget, error) {
	var changed []string
	for _, name := range budgetFlagNames {
		if cmd.Flags().Changed(name) {
			changed = append(changed, "--"+name)
		}
	}
	if clear {
		if len(changed) > 0 {
			return nil, fmt.Errorf("--clear-budget and %s contradict each other: give one", strings.Join(changed, ", "))
		}
		return nil, nil
	}
	if len(changed) == 0 {
		return current, nil
	}
	budget := &orkstackv1.Budget{}
	if current != nil {
		budget = proto.Clone(current).(*orkstackv1.Budget)
	}
	if cmd.Flags().Changed("budget-cpu-seconds") {
		if values.cpuSeconds < 0 {
			return nil, fmt.Errorf("--budget-cpu-seconds must be 0 (unbounded) or positive, got %d", values.cpuSeconds)
		}
		budget.CpuSeconds = values.cpuSeconds
	}
	if cmd.Flags().Changed("budget-gib-seconds") {
		if values.gibSeconds < 0 {
			return nil, fmt.Errorf("--budget-gib-seconds must be 0 (unbounded) or positive, got %d", values.gibSeconds)
		}
		budget.GibSeconds = values.gibSeconds
	}
	if cmd.Flags().Changed("budget-period") {
		period, err := time.ParseDuration(values.period)
		if err != nil {
			return nil, fmt.Errorf("parse --budget-period %q: %w", values.period, err)
		}
		budget.Period = durationpb.New(period)
	}
	if cmd.Flags().Changed("budget-enforce") {
		budget.Enforce = values.enforce
	}
	if budget.GetPeriod() == nil {
		return nil, fmt.Errorf("--budget-period is required to set a budget on a namespace that has none (e.g. --budget-period 720h)")
	}
	return budget, nil
}

// checkAllowSecretFlags holds --allow-secret entries to the registry's own
// rule (dsl.ValidSecretPattern, the same implementation, not a restatement)
// before any round trip, so a bad pattern is a local error. The registry
// stays the authority and refuses the same entries for any other client.
func checkAllowSecretFlags(entries []string) error {
	for _, entry := range entries {
		if err := dsl.ValidSecretPattern(entry); err != nil {
			return fmt.Errorf("invalid --allow-secret %q: %w", entry, err)
		}
	}
	if err := dsl.ValidSecretPatterns(entries); err != nil {
		return fmt.Errorf("invalid --allow-secret: %w", err)
	}
	return nil
}

// applyAdmissionFlags writes --hold and --max-running onto ns, each only when
// its flag was given — the "a flag left out keeps its current value" rule
// every other update flag follows. A blank reason and a negative cap are
// refused here, before any round trip; the registry refuses both again.
// The hold's since is never sent: the registry stamps it.
func applyAdmissionFlags(cmd *cobra.Command, ns *orkstackv1.Namespace, holdReason string, maxRunning int32) error {
	if cmd.Flags().Changed("hold") {
		if strings.TrimSpace(holdReason) == "" {
			return fmt.Errorf("--hold needs a reason: the next operator has to decide whether to release it")
		}
		if ns.GetHold().GetReason() != holdReason {
			ns.Hold = &orkstackv1.NamespaceHold{Reason: holdReason}
		}
	}
	if cmd.Flags().Changed("max-running") {
		if maxRunning < 0 {
			return fmt.Errorf("--max-running must be 0 (unlimited) or positive, got %d", maxRunning)
		}
		ns.MaxRunning = maxRunning
	}
	return nil
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
	fmt.Fprintln(table, "NAME\tDEFAULT POOL\tALLOWED POOLS\tALLOWED SECRETS\tRUN RETENTION\tHOLD\tMAX RUNNING\tBUDGET\tCREATED\tDESCRIPTION")
	for _, ns := range namespaces {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			ns.GetName(), defaultPoolLabel(ns), joinOrDash(ns.GetAllowedPools()),
			allowedSecretsCellLabel(ns), runRetentionLabel(ns), holdCellLabel(ns), maxRunningLabel(ns),
			budgetCellLabel(ns.GetBudget()), formatTime(ns.GetCreatedAt()), orDash(ns.GetDescription()))
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
	fmt.Fprintf(table, "Allowed secrets:\t%s\n", allowedSecretsLabel(ns))
	fmt.Fprintf(table, "Run retention:\t%s\n", runRetentionLabel(ns))
	fmt.Fprintf(table, "Hold:\t%s\n", holdLabel(ns))
	fmt.Fprintf(table, "Max running:\t%s\n", maxRunningLabel(ns))
	fmt.Fprintf(table, "Budget:\t%s\n", budgetLabel(ns.GetBudget()))
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

// allowedSecretsLabel spells out what an empty secret list means, on the rule
// allowedPoolsLabel states: not "none", but "no restriction" — and says what
// still bounds it, so the permissive default is never mistaken for a
// configured one.
func allowedSecretsLabel(ns *orkstackv1.Namespace) string {
	if secrets := ns.GetAllowedSecrets(); len(secrets) > 0 {
		return strings.Join(secrets, ",")
	}
	return "(unrestricted; bounded only by each Orker's ORK_ORKER_SECRET_ALLOW)"
}

// allowedSecretsCellLabel is allowedSecretsLabel for a table cell. It is the
// word "unrestricted", not joinOrDash's "-": under a heading that names a
// security bound, a dash invites the reading "none allowed", the opposite of
// what an empty list means.
func allowedSecretsCellLabel(ns *orkstackv1.Namespace) string {
	if secrets := ns.GetAllowedSecrets(); len(secrets) > 0 {
		return strings.Join(secrets, ",")
	}
	return "unrestricted"
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

// holdLabel renders a namespace's hold for `namespace get`: "accepting" when
// it has none — spelled out, never a dash, which would read as the opposite
// of the truth — and otherwise since when, and why.
func holdLabel(ns *orkstackv1.Namespace) string {
	hold := ns.GetHold()
	if hold == nil {
		return "accepting"
	}
	return fmt.Sprintf("since %s — %q", formatTime(hold.GetSince()), hold.GetReason())
}

// holdCellLabel is holdLabel for a table cell: the reason is a sentence and
// belongs to `namespace get`, so the list says only whether and since when.
func holdCellLabel(ns *orkstackv1.Namespace) string {
	hold := ns.GetHold()
	if hold == nil {
		return "accepting"
	}
	return "since " + formatTime(hold.GetSince())
}

// maxRunningLabel renders a namespace's cap, with 0 spelled as what it means.
func maxRunningLabel(ns *orkstackv1.Namespace) string {
	if limit := ns.GetMaxRunning(); limit > 0 {
		return fmt.Sprintf("%d", limit)
	}
	return "unlimited"
}

// budgetLabel renders a namespace's budget for `namespace get`: the word
// "unbounded" when it has none — never a dash — and otherwise every bounded
// dimension, the period, and what reaching it does.
func budgetLabel(b *orkstackv1.Budget) string {
	if b == nil {
		return "unbounded"
	}
	var bounds []string
	if cpu := b.GetCpuSeconds(); cpu > 0 {
		bounds = append(bounds, fmt.Sprintf("%d cpu-seconds", cpu))
	}
	if gib := b.GetGibSeconds(); gib > 0 {
		bounds = append(bounds, fmt.Sprintf("%d GiB-seconds", gib))
	}
	mode := "soft (reported, never refuses)"
	if b.GetEnforce() {
		mode = "enforced (new runs are refused at the bound)"
	}
	return fmt.Sprintf("%s per %s, %s", strings.Join(bounds, " and "), b.GetPeriod().AsDuration(), mode)
}

// budgetCellLabel is budgetLabel for a table cell: the bounds in short units,
// the period, and the mode in one word. The UI's Budget column renders the
// same string (ui/src/lib/usage.ts, budgetConfigLabel).
func budgetCellLabel(b *orkstackv1.Budget) string {
	if b == nil {
		return "unbounded"
	}
	var bounds []string
	if cpu := b.GetCpuSeconds(); cpu > 0 {
		bounds = append(bounds, fmt.Sprintf("%d cpu-s", cpu))
	}
	if gib := b.GetGibSeconds(); gib > 0 {
		bounds = append(bounds, fmt.Sprintf("%d GiB-s", gib))
	}
	mode := "soft"
	if b.GetEnforce() {
		mode = "enforced"
	}
	return fmt.Sprintf("%s/%s %s", strings.Join(bounds, " + "), b.GetPeriod().AsDuration(), mode)
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
