package automation

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"gopkg.in/yaml.v3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// settingRow is one managed key of the `settings` section: the document key,
// the Namespace field it sets, how a document value is parsed into that
// field, how the field renders, how two values compare, and how an export
// writes it. The table below is the complete mapping (docs/AUTOMATION.md
// "settings").
type settingRow struct {
	key   string
	field protoreflect.Name
	// parse reads the document's node into ns's field, or reports why it
	// cannot. Problems are messages; the caller anchors them.
	parse func(node *yaml.Node, ns *orkstackv1.Namespace) error
	// render is the value as a plan shows it.
	render func(ns *orkstackv1.Namespace) string
	// equal compares two documents on this key with specDifference's
	// semantics (internal/api/namespaces.go).
	equal func(a, b *orkstackv1.Namespace) bool
	// export is the node an exported document writes for this key.
	export func(ns *orkstackv1.Namespace) *yaml.Node
}

// namespaceSettings is the closed table of managed keys, in document order.
var namespaceSettings = []settingRow{
	{
		key: "description", field: "description",
		parse: func(node *yaml.Node, ns *orkstackv1.Namespace) error {
			value, err := stringScalar(node)
			ns.Description = value
			return err
		},
		render: func(ns *orkstackv1.Namespace) string { return renderString(ns.GetDescription()) },
		equal:  func(a, b *orkstackv1.Namespace) bool { return a.GetDescription() == b.GetDescription() },
		export: func(ns *orkstackv1.Namespace) *yaml.Node { return strNode(ns.GetDescription()) },
	},
	{
		key: "labels", field: "labels",
		parse: func(node *yaml.Node, ns *orkstackv1.Namespace) error {
			values, err := stringMap(node)
			ns.Labels = values
			return err
		},
		render: func(ns *orkstackv1.Namespace) string { return renderMap(ns.GetLabels()) },
		equal:  func(a, b *orkstackv1.Namespace) bool { return equalMaps(a.GetLabels(), b.GetLabels()) },
		export: func(ns *orkstackv1.Namespace) *yaml.Node { return mapNode(ns.GetLabels()) },
	},
	{
		key: "default_pool", field: "default_pool",
		parse: func(node *yaml.Node, ns *orkstackv1.Namespace) error {
			value, err := stringScalar(node)
			ns.DefaultPool = value
			return err
		},
		render: func(ns *orkstackv1.Namespace) string { return renderString(engine.DefaultPoolFor(ns)) },
		equal: func(a, b *orkstackv1.Namespace) bool {
			return engine.DefaultPoolFor(a) == engine.DefaultPoolFor(b)
		},
		export: func(ns *orkstackv1.Namespace) *yaml.Node { return strNode(engine.DefaultPoolFor(ns)) },
	},
	{
		key: "allowed_pools", field: "allowed_pools",
		parse: func(node *yaml.Node, ns *orkstackv1.Namespace) error {
			values, err := stringList(node)
			ns.AllowedPools = values
			return err
		},
		render: func(ns *orkstackv1.Namespace) string { return renderStrings(ns.GetAllowedPools()) },
		equal:  func(a, b *orkstackv1.Namespace) bool { return equalSets(a.GetAllowedPools(), b.GetAllowedPools()) },
		export: func(ns *orkstackv1.Namespace) *yaml.Node { return listNode(ns.GetAllowedPools()) },
	},
	{
		key: "run_retention", field: "run_retention",
		parse: func(node *yaml.Node, ns *orkstackv1.Namespace) error {
			value, err := stringScalar(node)
			if err != nil {
				return err
			}
			if value == "inherit" {
				ns.RunRetention = nil
				return nil
			}
			d, err := time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("run_retention %q is not a duration (e.g. 168h) or inherit", value)
			}
			if d <= 0 {
				return fmt.Errorf("run_retention must be a positive duration, got %s", d)
			}
			ns.RunRetention = durationpb.New(d)
			return nil
		},
		render: func(ns *orkstackv1.Namespace) string { return renderRetention(ns.GetRunRetention()) },
		equal: func(a, b *orkstackv1.Namespace) bool {
			return optionalDurationEqual(a.GetRunRetention(), b.GetRunRetention())
		},
		export: func(ns *orkstackv1.Namespace) *yaml.Node { return plainNode(renderRetention(ns.GetRunRetention())) },
	},
	{
		key: "telemetry", field: "telemetry",
		parse:  parseTelemetry,
		render: func(ns *orkstackv1.Namespace) string { return renderTelemetry(ns.GetTelemetry()) },
		equal: func(a, b *orkstackv1.Namespace) bool {
			x, y := a.GetTelemetry(), b.GetTelemetry()
			return x.GetSink() == y.GetSink() && x.GetTaskLogs() == y.GetTaskLogs() &&
				equalMaps(x.GetAttributes(), y.GetAttributes())
		},
		export: exportTelemetry,
	},
	{
		key: "max_running", field: "max_running",
		parse: func(node *yaml.Node, ns *orkstackv1.Namespace) error {
			if node.Kind != yaml.ScalarNode || node.Tag != "!!int" {
				return fmt.Errorf("max_running must be an integer of at least 0")
			}
			n, err := strconv.ParseInt(node.Value, 0, 32)
			if err != nil || n < 0 {
				return fmt.Errorf("max_running must be an integer of at least 0, got %s", node.Value)
			}
			ns.MaxRunning = int32(n)
			return nil
		},
		render: func(ns *orkstackv1.Namespace) string { return strconv.Itoa(int(ns.GetMaxRunning())) },
		equal:  func(a, b *orkstackv1.Namespace) bool { return a.GetMaxRunning() == b.GetMaxRunning() },
		export: func(ns *orkstackv1.Namespace) *yaml.Node {
			return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.Itoa(int(ns.GetMaxRunning()))}
		},
	},
	{
		key: "allowed_secrets", field: "allowed_secrets",
		parse: func(node *yaml.Node, ns *orkstackv1.Namespace) error {
			values, err := stringList(node)
			ns.AllowedSecrets = values
			return err
		},
		render: func(ns *orkstackv1.Namespace) string { return renderStrings(ns.GetAllowedSecrets()) },
		equal:  func(a, b *orkstackv1.Namespace) bool { return equalSets(a.GetAllowedSecrets(), b.GetAllowedSecrets()) },
		export: func(ns *orkstackv1.Namespace) *yaml.Node { return listNode(ns.GetAllowedSecrets()) },
	},
	{
		key: "require_signed_by", field: "require_signed_by",
		parse: func(node *yaml.Node, ns *orkstackv1.Namespace) error {
			value, err := stringScalar(node)
			ns.RequireSignedBy = value
			return err
		},
		render: func(ns *orkstackv1.Namespace) string { return renderString(ns.GetRequireSignedBy()) },
		equal:  func(a, b *orkstackv1.Namespace) bool { return a.GetRequireSignedBy() == b.GetRequireSignedBy() },
		export: func(ns *orkstackv1.Namespace) *yaml.Node { return strNode(ns.GetRequireSignedBy()) },
	},
	{
		// budget (namespace-budgets) is configuration, managed as a unit like
		// telemetry; its sub-keys are the --budget-* flags of `orkctl
		// namespace create/update` with the prefix dropped.
		key: "budget", field: "budget",
		parse:  parseBudget,
		render: func(ns *orkstackv1.Namespace) string { return renderBudget(ns.GetBudget()) },
		equal: func(a, b *orkstackv1.Namespace) bool {
			return budgetKey(a.GetBudget()) == budgetKey(b.GetBudget())
		},
		export: exportBudget,
	},
}

// unmanagedNamespaceFields is every Namespace field a document never
// manages, each with its reason. A field in neither table fails
// TestAutomationSettings_ClassifyEveryNamespaceField.
var unmanagedNamespaceFields = map[protoreflect.Name]string{
	"name":       "it is the document's namespace:",
	"created_at": "registry-owned; never supplied by a client",
	"updated_at": "registry-owned; never supplied by a client",
	"etag":       "registry-owned; never supplied by a client",
	"hold":       "operational state: a hold is set and released with orkctl namespace update, never by a document",
}

// holdRefusal is the ERROR a `settings.hold` key earns.
const holdRefusal = "a hold is operational state and is not managed by a document; " +
	"use `orkctl namespace update --hold`/`--release-hold`"

func settingKeys() []string {
	out := make([]string, 0, len(namespaceSettings))
	for _, row := range namespaceSettings {
		out = append(out, row.key)
	}
	return out
}

func settingRowFor(key string) (settingRow, bool) {
	for _, row := range namespaceSettings {
		if row.key == key {
			return row, true
		}
	}
	return settingRow{}, false
}

// NamespaceFieldChanges reports every managed settings key whose value
// differs between two Namespace documents, with specDifference's semantics,
// in table order. keys selects which keys are compared; nil compares every
// managed key.
func NamespaceFieldChanges(stored, desired *orkstackv1.Namespace, keys map[string]bool) []*orkstackv1.AutomationFieldChange {
	var out []*orkstackv1.AutomationFieldChange
	for _, row := range namespaceSettings {
		if keys != nil && !keys[row.key] {
			continue
		}
		if row.equal(stored, desired) {
			continue
		}
		out = append(out, &orkstackv1.AutomationFieldChange{
			Field: row.key, Before: row.render(stored), After: row.render(desired),
		})
	}
	return out
}

// overlay clones live (or a fresh Namespace{name} when live is nil) and, for
// each key the document wrote, copies that one field from the document's
// values — set when the document gave it a value, cleared otherwise. It never
// assigns a whole message, so a field the table does not name (a hold, an
// etag) survives untouched.
func overlay(name string, live *orkstackv1.Namespace, settings *Settings) *orkstackv1.Namespace {
	var out *orkstackv1.Namespace
	if live != nil {
		out = proto.Clone(live).(*orkstackv1.Namespace)
	} else {
		out = &orkstackv1.Namespace{Name: name}
	}
	if settings == nil {
		return out
	}
	values := proto.Clone(settings.Values).(*orkstackv1.Namespace)
	src, dst := values.ProtoReflect(), out.ProtoReflect()
	fields := dst.Descriptor().Fields()
	for _, row := range namespaceSettings {
		if !settings.Present[row.key] {
			continue
		}
		fd := fields.ByName(row.field)
		if src.Has(fd) {
			dst.Set(fd, src.Get(fd))
		} else {
			dst.Clear(fd)
		}
	}
	return out
}

func equalMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}

func equalSets(a, b []string) bool {
	x := append([]string(nil), a...)
	y := append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func optionalDurationEqual(a, b *durationpb.Duration) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.AsDuration() == b.AsDuration()
}

// parseTelemetry reads the `telemetry` mapping as a unit: absent sub-keys take
// their zero, and an all-zero policy is written as unset.
func parseTelemetry(node *yaml.Node, ns *orkstackv1.Namespace) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("telemetry must be a mapping of sink, task_logs and attributes")
	}
	policy := &orkstackv1.TelemetryPolicy{}
	allowed := []string{"sink", "task_logs", "attributes"}
	seen := map[string]bool{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i].Value, node.Content[i+1]
		if seen[key] {
			return fmt.Errorf("line %d: telemetry.%s is written twice", node.Content[i].Line, key)
		}
		seen[key] = true
		switch key {
		case "sink":
			sink, err := stringScalar(value)
			if err != nil {
				return fmt.Errorf("telemetry.sink: %w", err)
			}
			policy.Sink = sink
		case "task_logs":
			raw, err := stringScalar(value)
			if err != nil {
				return fmt.Errorf("telemetry.task_logs: %w", err)
			}
			switch raw {
			case "", "inherit":
			case "enabled":
				policy.TaskLogs = orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED
			case "disabled":
				policy.TaskLogs = orkstackv1.TaskLogExport_TASK_LOG_EXPORT_DISABLED
			default:
				return fmt.Errorf("telemetry.task_logs %q: want one of enabled, disabled, inherit", raw)
			}
		case "attributes":
			attributes, err := stringMap(value)
			if err != nil {
				return fmt.Errorf("telemetry.attributes: %w", err)
			}
			policy.Attributes = attributes
		default:
			return fmt.Errorf("line %d: unknown key telemetry.%s; allowed: %s", node.Content[i].Line, key, joinKeys(allowed))
		}
	}
	if telemetryZero(policy) {
		ns.Telemetry = nil
		return nil
	}
	ns.Telemetry = policy
	return nil
}

func exportTelemetry(ns *orkstackv1.Namespace) *yaml.Node {
	policy := ns.GetTelemetry()
	if telemetryZero(policy) {
		return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Style: yaml.FlowStyle}
	}
	out := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	out.Content = append(out.Content,
		plainNode("sink"), strNode(policy.GetSink()),
		plainNode("task_logs"), plainNode(taskLogsSlug(policy.GetTaskLogs())),
		plainNode("attributes"), mapNode(policy.GetAttributes()))
	return out
}

// budgetSubKeys are the sub-keys of `settings.budget`, in document order.
var budgetSubKeys = []string{"cpu_seconds", "gib_seconds", "period", "enforce"}

// parseBudget reads the `budget` mapping as a unit: absent sub-keys take
// their zero, and an all-zero budget is written as unset (`budget: {}` is
// --clear-budget). What a budget may be held to — a period inside the
// allowed range, at least one bounded dimension — is ValidateNamespaceSpec's,
// so a plan reports it exactly as the write would refuse it.
func parseBudget(node *yaml.Node, ns *orkstackv1.Namespace) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("budget must be a mapping of %s", joinKeys(budgetSubKeys))
	}
	budget := &orkstackv1.Budget{}
	seen := map[string]bool{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i].Value, node.Content[i+1]
		if seen[key] {
			return fmt.Errorf("line %d: budget.%s is written twice", node.Content[i].Line, key)
		}
		seen[key] = true
		switch key {
		case "cpu_seconds", "gib_seconds":
			if value.Kind != yaml.ScalarNode || value.Tag != "!!int" {
				return fmt.Errorf("budget.%s must be an integer of at least 0", key)
			}
			n, err := strconv.ParseInt(value.Value, 0, 64)
			if err != nil || n < 0 {
				return fmt.Errorf("budget.%s must be an integer of at least 0, got %s", key, value.Value)
			}
			if key == "cpu_seconds" {
				budget.CpuSeconds = n
			} else {
				budget.GibSeconds = n
			}
		case "period":
			raw, err := stringScalar(value)
			if err != nil {
				return fmt.Errorf("budget.period: %w", err)
			}
			d, err := time.ParseDuration(raw)
			if err != nil {
				return fmt.Errorf("budget.period %q is not a duration (e.g. 720h)", raw)
			}
			budget.Period = durationpb.New(d)
		case "enforce":
			if value.Kind != yaml.ScalarNode || value.Tag != "!!bool" {
				return fmt.Errorf("budget.enforce must be true or false, got %s", describeNode(value))
			}
			enforce, err := strconv.ParseBool(value.Value)
			if err != nil {
				return fmt.Errorf("budget.enforce must be true or false, got %s", value.Value)
			}
			budget.Enforce = enforce
		default:
			return fmt.Errorf("line %d: unknown key budget.%s; allowed: %s", node.Content[i].Line, key, joinKeys(budgetSubKeys))
		}
	}
	if budgetZero(budget) {
		ns.Budget = nil
		return nil
	}
	ns.Budget = budget
	return nil
}

func budgetZero(b *orkstackv1.Budget) bool {
	return b.GetCpuSeconds() == 0 && b.GetGibSeconds() == 0 && b.GetPeriod() == nil && !b.GetEnforce()
}

// budgetKey is the budget's identity for comparison: specDifference's
// budgetLabel (internal/api/namespaces.go) — unset is "none", anything else
// every field — so the two can never disagree on whether a budget changed.
func budgetKey(b *orkstackv1.Budget) string {
	if b == nil {
		return "none"
	}
	return fmt.Sprintf("{cpu_seconds=%d gib_seconds=%d period=%s enforce=%t}",
		b.GetCpuSeconds(), b.GetGibSeconds(), b.GetPeriod().AsDuration(), b.GetEnforce())
}

// renderBudget is the budget as a plan shows it: {} when unset.
func renderBudget(b *orkstackv1.Budget) string {
	if b == nil {
		return "{}"
	}
	return fmt.Sprintf("{cpu_seconds: %d, gib_seconds: %d, period: %s, enforce: %t}",
		b.GetCpuSeconds(), b.GetGibSeconds(), renderBudgetPeriod(b.GetPeriod()), b.GetEnforce())
}

func renderBudgetPeriod(d *durationpb.Duration) string {
	if d == nil {
		return "0s"
	}
	return shortDuration(d.AsDuration())
}

func exportBudget(ns *orkstackv1.Namespace) *yaml.Node {
	b := ns.GetBudget()
	if b == nil {
		return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Style: yaml.FlowStyle}
	}
	out := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	out.Content = append(out.Content,
		plainNode("cpu_seconds"), &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.FormatInt(b.GetCpuSeconds(), 10)},
		plainNode("gib_seconds"), &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.FormatInt(b.GetGibSeconds(), 10)},
		plainNode("period"), plainNode(renderBudgetPeriod(b.GetPeriod())),
		plainNode("enforce"), &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(b.GetEnforce())})
	return out
}
