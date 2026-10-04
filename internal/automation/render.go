package automation

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// This file is the ONE set of renderings a plan's summaries, changes and an
// export use (docs/AUTOMATION.md "Renderings"). Every client shows these
// strings verbatim; none of them parses one.

// unsetLabel is how an absent optional value reads.
const unsetLabel = "unset"

// currentLabel is how an unpinned version reads: unquoted, so it can never
// be mistaken for a pinned version named "current".
const currentLabel = "current"

// renderString is Go %q.
func renderString(value string) string { return fmt.Sprintf("%q", value) }

// renderVersion renders a version pin: current when unpinned, quoted when
// pinned.
func renderVersion(version string) string {
	if version == "" {
		return currentLabel
	}
	return renderString(version)
}

// versionLabel is the version in a summary: the bare pin, or current.
func versionLabel(version string) string {
	if version == "" {
		return currentLabel
	}
	return version
}

// renderStrings renders a list of strings: [] when empty, each item quoted.
func renderStrings(values []string) string {
	items := make([]string, 0, len(values))
	for _, value := range values {
		items = append(items, renderString(value))
	}
	return "[" + strings.Join(items, ", ") + "]"
}

// renderMap renders a string map sorted by key: {} when empty.
func renderMap(values map[string]string) string {
	if len(values) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, key+": "+renderString(values[key]))
	}
	return "{" + strings.Join(pairs, ", ") + "}"
}

// shortDuration is a duration's canonical short form: time.Duration.String()
// with trailing zero units removed — 168h, 1h30m, 30s, 1h0m30s.
func shortDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

// renderDuration renders an optional duration: unset when nil.
func renderDuration(d *durationpb.Duration) string {
	if d == nil {
		return unsetLabel
	}
	return shortDuration(d.AsDuration())
}

// renderRetention renders run_retention: inherit when unset.
func renderRetention(d *durationpb.Duration) string {
	if d == nil {
		return "inherit"
	}
	return shortDuration(d.AsDuration())
}

// renderTimestamp renders an optional instant: RFC 3339 UTC, unset when nil.
func renderTimestamp(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return unsetLabel
	}
	return ts.AsTime().UTC().Format(time.RFC3339)
}

// renderInput renders a run input: compact JSON with sorted keys (what
// encoding/json produces for a map), unset when absent or empty.
func renderInput(input *orkstackv1.Payload) string {
	if inputEmpty(input) {
		return unsetLabel
	}
	return compactInput(input)
}

// compactInput is the compact JSON of a non-empty input.
func compactInput(input *orkstackv1.Payload) string {
	encoded, err := json.Marshal(input.GetData().AsMap())
	if err != nil {
		return unsetLabel
	}
	return string(encoded)
}

func inputEmpty(input *orkstackv1.Payload) bool {
	return input == nil || input.GetData() == nil || len(input.GetData().GetFields()) == 0
}

// renderEvents renders a subscription's events in lifecycle order; [] means
// every run transition.
func renderEvents(events []webhooks.Event) string {
	names := make([]string, 0, len(events))
	for _, event := range events {
		names = append(names, string(event))
	}
	return "[" + strings.Join(names, ", ") + "]"
}

// renderTelemetry renders a telemetry policy: {} when unset or all-zero.
func renderTelemetry(policy *orkstackv1.TelemetryPolicy) string {
	if telemetryZero(policy) {
		return "{}"
	}
	return fmt.Sprintf("{sink: %s, task_logs: %s, attributes: %s}",
		renderString(policy.GetSink()), taskLogsSlug(policy.GetTaskLogs()), renderMap(policy.GetAttributes()))
}

func telemetryZero(policy *orkstackv1.TelemetryPolicy) bool {
	return policy.GetSink() == "" &&
		policy.GetTaskLogs() == orkstackv1.TaskLogExport_TASK_LOG_EXPORT_UNSPECIFIED &&
		len(policy.GetAttributes()) == 0
}

// taskLogsSlug is the document spelling of a task-log export switch, the
// spelling `orkctl namespace update --task-logs` takes.
func taskLogsSlug(value orkstackv1.TaskLogExport) string {
	switch value {
	case orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED:
		return "enabled"
	case orkstackv1.TaskLogExport_TASK_LOG_EXPORT_DISABLED:
		return "disabled"
	}
	return "inherit"
}

// overlapSlug is the user-facing spelling of an overlap policy: the value's
// name after OVERLAP_POLICY_, lowercased, "_" as "-". UNSPECIFIED is recorded
// as skip, so it reads as skip.
func overlapSlug(p orkstackv1.OverlapPolicy) string {
	if p == orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
		p = orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP
	}
	name, ok := orkstackv1.OverlapPolicy_name[int32(p)]
	if !ok {
		return fmt.Sprintf("%d", int32(p))
	}
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(name, "OVERLAP_POLICY_")), "_", "-")
}

// parseOverlap is overlapSlug's inverse over the declared values.
func parseOverlap(slug string) (orkstackv1.OverlapPolicy, bool) {
	for number := range orkstackv1.OverlapPolicy_name {
		p := orkstackv1.OverlapPolicy(number)
		if p == orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
			continue
		}
		if overlapSlug(p) == slug {
			return p, true
		}
	}
	return orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED, false
}

// overlapSlugs lists every declared overlap slug, in enum order.
func overlapSlugs() []string {
	numbers := make([]int, 0, len(orkstackv1.OverlapPolicy_name))
	for number := range orkstackv1.OverlapPolicy_name {
		if number != 0 {
			numbers = append(numbers, int(number))
		}
	}
	sort.Ints(numbers)
	out := make([]string, 0, len(numbers))
	for _, number := range numbers {
		out = append(out, overlapSlug(orkstackv1.OverlapPolicy(number)))
	}
	return out
}

// whenLabel is a schedule spec's WHEN: crons joined " | ", then
// "every {interval}", then " ({timezone})" — `orkctl schedule list`'s column.
func whenLabel(spec *orkstackv1.ScheduleSpec) string {
	parts := append([]string(nil), spec.GetCron()...)
	if iv := spec.GetInterval(); iv != nil {
		parts = append(parts, "every "+shortDuration(iv.AsDuration()))
	}
	if len(parts) == 0 {
		return "-"
	}
	label := strings.Join(parts, " | ")
	if tz := spec.GetTimezone(); tz != "" && len(spec.GetCron()) > 0 {
		label += " (" + tz + ")"
	}
	return label
}

// scheduleSummary is a schedule step's one line.
func scheduleSummary(orkflow, version string, spec *orkstackv1.ScheduleSpec) string {
	return orkflow + "@" + versionLabel(version) + "  " + whenLabel(spec)
}

// triggerSummary is a trigger step's one line.
func triggerSummary(orkflow, version string) string {
	return orkflow + "@" + versionLabel(version)
}

// webhookSummary is a webhook step's one line: the URL and the scope cell
// `orkctl webhook list` renders.
func webhookSummary(url, namespace, orkflow, task string) string {
	part := func(value string) string {
		if value == "" {
			return "*"
		}
		return value
	}
	return url + "  " + part(namespace) + "/" + part(orkflow) + "/" + part(task)
}
