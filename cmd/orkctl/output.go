package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
)

// dash is what an empty cell renders as, so tables never have holes.
const dash = "-"

// marshalJSON encodes a protobuf message as JSON in OrkStack's dialect: enum
// values lose their type prefix ("status": "SUCCEEDED"), which is what the API
// and the WebSocket bridge emit too. api.MarshalJSON is the one encoder for
// that dialect — encoding/json cannot round-trip google.protobuf.Struct, and
// re-indenting removes protojson's deliberately unstable whitespace.
func marshalJSON(message proto.Message) ([]byte, error) {
	compact, err := api.MarshalJSON(message)
	if err != nil {
		return nil, err
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, compact, "", "  "); err != nil {
		return nil, fmt.Errorf("encode json: %w", err)
	}
	return indented.Bytes(), nil
}

// writeJSON prints a protobuf message as indented JSON followed by a newline.
func writeJSON(out io.Writer, message proto.Message) error {
	encoded, err := marshalJSON(message)
	if err != nil {
		return err
	}
	if _, err := out.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// newTable returns the shared aligned-column writer. Callers must Flush.
func newTable(out io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
}

// statusLabel renders a Status without its enum prefix.
func statusLabel(status orkstackv1.Status) string {
	return strings.TrimPrefix(status.String(), "STATUS_")
}

// taskStatusLabel renders a task run's status the way statusLabel does,
// except a CANCELED task that never reached an Orker reads as "CANCELED
// (never started)" instead of a bare CANCELED. STATUS_CANCELED is the
// engine's status for both a task that was running and got killed, and one
// that never ran because the run ended first (markCanceled never stamps
// scheduled_at for the latter) — a bare label can't tell them apart, which is
// what made a run's unstarted tasks read as if they had been running.
//
// It keys on scheduled_at, the dispatch instant: started_at is the Orker's
// report and is unset for a task canceled while it ran. A snapshot written
// before scheduled_at existed held the dispatch instant in started_at, so
// that is still read when scheduled_at is absent.
// Mirrors ui/src/lib/status.ts's taskDispatched.
func taskStatusLabel(taskRun *orkstackv1.TaskRun) string {
	label := statusLabel(taskRun.GetStatus())
	if taskRun.GetStatus() == orkstackv1.Status_STATUS_CANCELED &&
		formatTime(taskRun.GetScheduledAt()) == dash && formatTime(taskRun.GetStartedAt()) == dash {
		return label + " (never started)"
	}
	if taskRun.GetCache().GetOutcome() == orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
		return label + " (cached)"
	}
	return label
}

// eventLabel renders an EventType without its enum prefix.
func eventLabel(eventType orkstackv1.EventType) string {
	return strings.TrimPrefix(eventType.String(), "EVENT_TYPE_")
}

// formatTime renders a timestamp in UTC RFC3339, or a dash when unset.
func formatTime(ts *timestamppb.Timestamp) string {
	if ts == nil || !ts.IsValid() || ts.AsTime().IsZero() {
		return dash
	}
	return ts.AsTime().UTC().Format(time.RFC3339)
}

// orDash keeps table cells non-empty.
func orDash(value string) string {
	if value == "" {
		return dash
	}
	return value
}

// joinOrDash renders a string list for a single table cell.
func joinOrDash(values []string) string {
	if len(values) == 0 {
		return dash
	}
	return strings.Join(values, ",")
}

// errorLine renders a structured run or task error on one line.
func errorLine(runErr *orkstackv1.Error) string {
	if runErr == nil {
		return dash
	}
	code := runErr.GetCode()
	message := runErr.GetMessage()
	switch {
	case code != "" && message != "":
		return code + ": " + message
	case code != "":
		return code
	case message != "":
		return message
	default:
		return dash
	}
}

// payloadSummaryMax is the most of a payload run get prints on its one line.
const payloadSummaryMax = 512

// payloadSummary renders a Payload compactly for human output: its JSON on
// one line, cut at payloadSummaryMax bytes (on a character boundary) with
// its full size and the command that prints it whole — a 256 KiB output on
// one terminal line hides everything after it. --json is never cut.
func payloadSummary(payload *orkstackv1.Payload, runID, namespace string) string {
	data := payload.GetData()
	if data == nil || len(data.GetFields()) == 0 {
		return dash
	}
	encoded, err := json.Marshal(structToMap(data))
	if err != nil {
		return dash
	}
	if len(encoded) <= payloadSummaryMax {
		return string(encoded)
	}
	cut := payloadSummaryMax
	for cut > 0 && !utf8.RuneStart(encoded[cut]) {
		cut--
	}
	return fmt.Sprintf("%s… (%d bytes; %s prints it whole)", encoded[:cut], len(encoded),
		orkctlCommand(namespace, "run", "get", runID, "--json"))
}

// structToMap converts a protobuf Struct to plain Go values.
func structToMap(data *structpb.Struct) map[string]any {
	if data == nil {
		return map[string]any{}
	}
	return data.AsMap()
}

// sortedKeys returns a map's keys in a stable order for deterministic tables.
func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// indentJSONBlock re-indents an already-indented JSON block so it can be
// nested inside a larger document. The first line keeps its position.
func indentJSONBlock(encoded, prefix string) string {
	lines := strings.Split(encoded, "\n")
	for i := 1; i < len(lines); i++ {
		lines[i] = prefix + lines[i]
	}
	return strings.Join(lines, "\n")
}
