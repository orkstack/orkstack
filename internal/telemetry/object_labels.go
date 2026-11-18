package telemetry

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ObjectLabelsVar is telemetry.object_labels: whether the authored names of
// a namespace's objects — a schedule's name (ork.schedule), a webhook's id
// (ork.webhook.id) — are labels on their metrics. Off by default: each name
// is a series of its own, bounded only by how many objects the namespaces
// hold and by telemetry.metric_cardinality_limit (docs/DEPLOY.md, "Label
// policy").
const ObjectLabelsVar = "ORK_TELEMETRY_OBJECT_LABELS"

// DefaultObjectLabels is telemetry.object_labels' default.
const DefaultObjectLabels = false

// ObjectLabelsFromEnv reads telemetry.object_labels from the process's
// environment.
func ObjectLabelsFromEnv() (bool, error) { return objectLabelsFrom(os.LookupEnv) }

func objectLabelsFrom(lookup func(string) (string, bool)) (bool, error) {
	raw, ok := lookup(ObjectLabelsVar)
	raw = strings.TrimSpace(raw)
	if !ok || raw == "" {
		return DefaultObjectLabels, nil
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s=%q must be true or false", ObjectLabelsVar, raw)
	}
	return parsed, nil
}
