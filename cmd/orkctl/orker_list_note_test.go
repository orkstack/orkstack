package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// TestOrkerList_SaysAStoppedOrkerStaysListed: after a restart under another id
// the old id is a row here and a count in `orkctl pool list` until its poller
// records leave the freshness window. The list says so when it shows such a
// row, and only then, and the time it names is that window.
func TestOrkerList_SaysAStoppedOrkerStaysListed(t *testing.T) {
	t.Parallel()
	if want := fmt.Sprintf("up to %d seconds", int(engine.DefaultPollerFreshness.Seconds())); !strings.Contains(stoppedOrkerNote, want) {
		t.Errorf("the note says %q, and the freshness window makes it %q", stoppedOrkerNote, want)
	}
	tests := []struct {
		name string
		rows []*orkstackv1.Orker
		want bool
	}{
		{"every Orker answered", []*orkstackv1.Orker{{OrkerId: "b", Pool: "main"}}, false},
		{"the old id beside the new one", []*orkstackv1.Orker{
			{OrkerId: "a", UnavailableReason: "context deadline exceeded"},
			{OrkerId: "b", Pool: "main"},
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer
			if err := renderOrkerList(&out, tt.rows); err != nil {
				t.Fatal(err)
			}
			if got := strings.Contains(out.String(), stoppedOrkerNote); got != tt.want {
				t.Errorf("note printed = %v, want %v:\n%s", got, tt.want, out.String())
			}
		})
	}
}
