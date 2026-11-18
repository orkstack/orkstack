// Package alerts holds the observability kit's Prometheus alerting and
// recording rules as Go tables (rules.go), with each rule's promtool cases
// and its runbook entry beside it, and writes the files Prometheus and
// promtool read:
//
//	deploy/observability/prometheus/rules/orkstack-alerts.yml
//	deploy/observability/prometheus/rules/orkstack-alerts_test.yaml
//	deploy/observability/prometheus/rules/orkstack-records.yml
//	deploy/observability/prometheus/rules/orkstack-records_test.yaml
//
// and the generated part of docs/RUNBOOKS.md. Tables rather than hand-written
// YAML, because several rules share long sub-expressions (the macros in
// macros.go) that must not drift apart, and because a rule, its cases and its
// runbook belong together. The YAML is plain Prometheus rule and promtool
// test syntax, marshalled with go.yaml.in/yaml/v4; promtool
// (scripts/check-alerts.sh), at the Prometheus the kit pins, is what parses
// and evaluates it. Prometheus's own rulefmt package was weighed as the
// writer and checker and not used: it would add the whole
// prometheus/prometheus module for a format three structs describe, and
// promtool already checks the result with that same code.
//
//	go run ./internal/alerts/cmd/genalerts
package alerts

// Prefix starts every alert's name, so an operator's Alertmanager can route
// OrkStack's alerts as one family.
const Prefix = "OrkStack"

// RunbookBase is where every alert's runbook_url points, followed by the
// alert's anchor in docs/RUNBOOKS.md.
const RunbookBase = "https://github.com/orkstack/orkstack/blob/main/docs/RUNBOOKS.md#"

// DashboardPath is the OrkStack dashboard's path under the operator's own
// Grafana: the kit provisions it with this uid (internal/dashboards).
const DashboardPath = "/d/orkstack"

// Severity is an alert's severity label.
type Severity string

const (
	Critical Severity = "critical"
	Warning  Severity = "warning"
	Info     Severity = "info"
	// None is the Watchdog's: it pages nobody, and its silence is the page.
	None Severity = "none"
)

// Rule is one alerting rule, with what its runbook entry says and the
// promtool cases that prove it.
type Rule struct {
	// Name is the alert's name without Prefix.
	Name string
	// Group is the rule group, one per area; it is also the runbook's section.
	Group     string
	Expr      string
	For       string
	Severity  Severity
	Component string
	// Summary and Description are plain text; a label is written
	// {{ $labels.name }}.
	Summary     string
	Description string
	// The runbook entry: what it means, what it costs, how to confirm it
	// without the kit (an orkctl command, "kit", or an engine log line),
	// what to do, and the alerts that travel with it.
	Meaning string
	Impact  string
	Confirm string
	Fix     string
	Related []string
	Cases   []Case
}

// Record is one recording rule and its promtool cases.
type Record struct {
	Name  string
	Expr  string
	Cases []RecordCase
}

// Series is one input series of a promtool case, in promtool's own
// expanding notation ("0+1x30", "_x10", "stale").
type Series struct {
	Series string
	Values string
}

// Case is one promtool alert case: the input, an evaluation time, and the
// alerts that fire then. A case whose Fires is empty proves the rule quiet.
type Case struct {
	Name     string
	Interval string
	Input    []Series
	EvalAt   string
	// Fires holds the expression's output labels of each firing alert,
	// without the rule's own severity and component labels.
	Fires []map[string]string
	// Also lists other rules this case's input must fire or leave quiet at
	// the same instant: a staleness partner, or EngineAllDown with the
	// pipeline up.
	Also []Expect
}

// Expect is another rule's expected alerts in the same case.
type Expect struct {
	Rule  string
	Fires []map[string]string
}

// RecordCase is one promtool expression case for a recording rule.
type RecordCase struct {
	Name   string
	Input  []Series
	EvalAt string
	Expr   string
	Want   []Sample
}

// Sample is one expected sample: its labels and value.
type Sample struct {
	Labels string
	Value  float64
}
