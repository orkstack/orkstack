package docscheck

import (
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

func knownEventNames() []string {
	var names []string
	for _, event := range webhooks.KnownEvents() {
		names = append(names, string(event))
	}
	slices.Sort(names)
	return names
}

// tableEvents returns the events named in the first column of every markdown
// table row of doc: "| `EVENT` | ...".
func tableEvents(doc string) []string {
	row := regexp.MustCompile("(?m)^\\| `([A-Z_]+)` \\|")
	var names []string
	for _, m := range row.FindAllStringSubmatch(doc, -1) {
		names = append(names, m[1])
	}
	return names
}

func missingEvents(have []string) []string {
	var missing []string
	for _, name := range knownEventNames() {
		if !slices.Contains(have, name) {
			missing = append(missing, name)
		}
	}
	return missing
}

// TestWebhookEvents_DocsTablesCoverKnownEvents: every event a subscription can
// name has a row in the reference doc's Events table and in the site's.
func TestWebhookEvents_DocsTablesCoverKnownEvents(t *testing.T) {
	root := repoRoot(t)
	for _, doc := range []string{"docs/WEBHOOKS.md", "site/content/how-to/webhooks.md"} {
		if missing := missingEvents(tableEvents(readFile(t, filepath.Join(root, doc)))); len(missing) > 0 {
			t.Errorf("%s has no events-table row for %s", doc, strings.Join(missing, ", "))
		}
	}
}

// TestWebhookEvents_DeliveryFilterAllowsKnownEvents: the proto allowed_values
// of the delivery list's event filter are KnownEvents, as sets.
func TestWebhookEvents_DeliveryFilterAllowsKnownEvents(t *testing.T) {
	proto := readFile(t, filepath.Join(repoRoot(t), "proto/orkstack/v1/webhook_service.proto"))
	block := regexp.MustCompile(`(?s)path:\s*"event"[^}]*?allowed_values:\s*\[([^\]]*)\]`).FindStringSubmatch(proto)
	if block == nil {
		t.Fatal("no allowed_values found for the event filter in webhook_service.proto")
	}
	var got []string
	for _, m := range regexp.MustCompile(`"([A-Z_]+)"`).FindAllStringSubmatch(block[1], -1) {
		got = append(got, m[1])
	}
	slices.Sort(got)
	if want := knownEventNames(); !slices.Equal(got, want) {
		t.Errorf("event filter allowed_values = %v, want KnownEvents %v", got, want)
	}
}
