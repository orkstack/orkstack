package config

import (
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
)

// TestConfig_StopGraceDefaultAndBounds: orker.stop.grace (ORK_ORKER_STOP_GRACE)
// defaults to 24h, a file value of 0s through 744h (31 days) is applied as
// written, and what lies outside that range is refused. The bound itself is
// the Orker's own parse (orker.ParseStopGrace, run at its boot), which the
// Schema's duration kind does not duplicate, so the test follows a value the
// whole way: Load applies it, the Orker's parse reads it. 745h and a negative
// value are refused there naming the variable and the bound; `31d` is no Go
// duration and is refused by Load itself, naming the key. 0s is a zero grace:
// the stopping Orker waits for nothing and hands back what is running.
func TestConfig_StopGraceDefaultAndBounds(t *testing.T) {
	v, ok := Lookup(orker.StopGraceVar)
	if !ok || v.Path != "orker.stop.grace" {
		t.Fatalf("Lookup(%s) = %+v, %v; want the orker.stop.grace entry", orker.StopGraceVar, v, ok)
	}
	if v.Kind != KindDuration {
		t.Errorf("orker.stop.grace has kind %v, want a duration", v.Kind)
	}
	if v.Default != "24h" || formatDuration(orker.DefaultStopGrace) != v.Default {
		t.Errorf("default = %q, want 24h bound to orker.DefaultStopGrace (%s)", v.Default, orker.DefaultStopGrace)
	}
	if got, err := orker.ParseStopGrace(v.Default); err != nil || got != 24*time.Hour {
		t.Errorf("the default parses to (%v, %v), want 24h", got, err)
	}
	if orker.MaxStopGrace != 744*time.Hour {
		t.Errorf("the bound is %v, want 744h (31 days)", orker.MaxStopGrace)
	}

	for _, tc := range []struct {
		name, value string
		want        time.Duration // when accepted
		loadRefuses bool          // Load itself refuses it, naming the key
		bootRefuses bool          // the Orker's parse refuses it, naming the variable and the bound
	}{
		{name: "the default's own spelling", value: "24h", want: 24 * time.Hour},
		{name: "zero: no grace, running tasks are handed back at once", value: "0s", want: 0},
		{name: "one second", value: "1s", want: time.Second},
		{name: "the bound", value: "744h", want: 744 * time.Hour},
		{name: "one hour over", value: "745h", bootRefuses: true},
		{name: "far over", value: "100000h", bootRefuses: true},
		{name: "one nanosecond over", value: "744h0m0.000000001s", bootRefuses: true},
		{name: "negative", value: "-1s", bootRefuses: true},
		{name: "days are no duration", value: "31d", loadRefuses: true},
		{name: "not a duration", value: "soon", loadRefuses: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemp(t, "grace.yaml", "orker:\n  stop:\n    grace: '"+tc.value+"'\n")
			doc, err := Load(path, BinaryOrker)
			if tc.loadRefuses {
				if err == nil || !strings.Contains(err.Error(), "orker.stop.grace") {
					t.Fatalf("Load(%q) = %v, want a refusal naming orker.stop.grace", tc.value, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load(%q): %v", tc.value, err)
			}
			applied := doc.values[orker.StopGraceVar].value
			if applied != tc.value {
				t.Fatalf("Load applied %q, want %q as written", applied, tc.value)
			}
			got, err := orker.ParseStopGrace(applied)
			if tc.bootRefuses {
				if err == nil || !strings.Contains(err.Error(), orker.StopGraceVar) || !strings.Contains(err.Error(), "744h") {
					t.Fatalf("ParseStopGrace(%q) = (%v, %v), want a refusal naming %s and 744h", applied, got, err, orker.StopGraceVar)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("ParseStopGrace(%q) = (%v, %v), want %v", applied, got, err, tc.want)
			}
		})
	}

	t.Run("only the Orker reads it", func(t *testing.T) {
		path := writeTemp(t, "grace.yaml", "orker:\n  stop:\n    grace: 1h\n")
		doc, err := Load(path, BinaryOrk)
		if err != nil {
			t.Fatalf("Load for ork: %v", err)
		}
		if _, set := doc.values[orker.StopGraceVar]; set {
			t.Errorf("Load(binary=ork) set %s, which belongs only to orker", orker.StopGraceVar)
		}
	})
}
