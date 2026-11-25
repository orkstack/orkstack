package telemetry

import (
	"slices"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"go.opentelemetry.io/otel/attribute"
)

// The reasons an ending's counters name under AttrFailureReason: the ones an
// alert rule reads (internal/alerts) and no counter of their own shows. The
// sets are closed, so the label is bounded whatever code an error carries:
// any other reason leaves it out, and a deployment that meets none of these
// keeps its series exactly.
//
// A run whose pin failed ends RUN_FAILED: the pin's reason is its tasks', so
// it is the task counters that name it.
var (
	runFailureReasons  = map[string]bool{"RUN_HISTORY_LIMIT": true}
	taskFailureReasons = map[string]bool{"PIN_HOST_LOST": true, "PIN_UNAVAILABLE": true}
)

// withFailureReason is attrs with err's reason when known holds it. It never
// writes to attrs' own array, which the durations beside the counter share.
func withFailureReason(attrs []attribute.KeyValue, err *orkstackv1.Error, known map[string]bool) []attribute.KeyValue {
	if !known[err.GetCode()] {
		return attrs
	}
	return append(slices.Clip(attrs), attribute.String(AttrFailureReason, err.GetCode()))
}
