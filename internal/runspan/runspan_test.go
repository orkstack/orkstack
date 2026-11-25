package runspan_test

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/runspan"
)

const (
	traceID = "0af7651916cd43dd8448eb211c80319c"
	spanID  = "b7ad6b7169203331"
	sampled = "00-" + traceID + "-" + spanID + "-01"
)

// The id is a contract: an exporter in another process, or another release,
// must name the same span.
func TestRunSpanID_Vectors(t *testing.T) {
	for runID, want := range map[string]string{
		"":                               "e4232c1f2387484c",
		"ork-01jf00000000000000000000aa": "84d9f2758971dc7c",
	} {
		id := runspan.RunSpanID(runID)
		if got := hex.EncodeToString(id[:]); got != want {
			t.Errorf("RunSpanID(%q) = %s, want %s", runID, got, want)
		}
	}
	if runspan.RunSpanID("ork-a") == runspan.RunSpanID("ork-b") {
		t.Error("two runs share a span id")
	}
}

func TestParse(t *testing.T) {
	for _, tc := range []struct {
		name, in string
		ok       bool
	}{
		{"sampled", sampled, true},
		{"not sampled", "00-" + traceID + "-" + spanID + "-00", true},
		{"other flag bits", "00-" + traceID + "-" + spanID + "-ff", true},
		{"empty", "", false},
		{"another version", "01-" + traceID + "-" + spanID + "-01", false},
		{"the forbidden version", "ff-" + traceID + "-" + spanID + "-01", false},
		{"a longer version 00", sampled + "-x", false},
		{"upper case", strings.ToUpper(sampled), false},
		{"a zero trace id", "00-" + strings.Repeat("0", 32) + "-" + spanID + "-01", false},
		{"a zero span id", "00-" + traceID + "-" + strings.Repeat("0", 16) + "-01", false},
		{"a short trace id", "00-" + traceID[1:] + "-" + spanID + "-01", false},
		{"not hex", "00-" + traceID[:31] + "g-" + spanID + "-01", false},
		{"no flags", "00-" + traceID + "-" + spanID, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := runspan.Parse(tc.in)
			if ok != tc.ok {
				t.Fatalf("Parse(%q) ok = %v, want %v", tc.in, ok, tc.ok)
			}
			if ok && (got.TraceID != traceID || got.SpanID != spanID || got.String() != tc.in) {
				t.Fatalf("Parse(%q) = %+v", tc.in, got)
			}
		})
	}
}

// A child's context keeps its parent's trace and flags, and names its
// parent's run span.
func TestChildTraceparent(t *testing.T) {
	id := runspan.RunSpanID("ork-parent")
	want := "00-" + traceID + "-" + hex.EncodeToString(id[:]) + "-01"
	if got := runspan.ChildTraceparent(sampled, "ork-parent"); got != want {
		t.Errorf("child of a sampled run = %s, want %s", got, want)
	}
	if got := runspan.ChildTraceparent("00-"+traceID+"-"+spanID+"-00", "ork-parent"); !strings.HasSuffix(got, "-00") {
		t.Errorf("child of an unsampled run = %s", got)
	}
	for _, none := range []string{"", "not a traceparent"} {
		if got := runspan.ChildTraceparent(none, "ork-parent"); got != "" {
			t.Errorf("child of %q = %q, want none", none, got)
		}
	}
}

// FuzzChildTraceparent: whatever it is given, the answer is empty or a
// version 00 traceparent of the same trace and flags.
func FuzzChildTraceparent(f *testing.F) {
	for _, seed := range []string{sampled, "", "00-" + traceID + "-" + spanID + "-00", "ff-x", strings.ToUpper(sampled)} {
		f.Add(seed, "ork-parent")
	}
	f.Fuzz(func(t *testing.T, parent, runID string) {
		child := runspan.ChildTraceparent(parent, runID)
		was, ok := runspan.Parse(parent)
		if !ok {
			if child != "" {
				t.Fatalf("ChildTraceparent(%q) = %q for no parent", parent, child)
			}
			return
		}
		got, ok := runspan.Parse(child)
		id := runspan.RunSpanID(runID)
		if !ok || got.TraceID != was.TraceID || got.Flags != was.Flags || got.SpanID != hex.EncodeToString(id[:]) {
			t.Fatalf("ChildTraceparent(%q, %q) = %q", parent, runID, child)
		}
	})
}
