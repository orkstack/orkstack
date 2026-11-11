package automation

import (
	"math"
	"testing"
)

// TestParse_MinusZeroIsTheIntegerZero: -0 for an integer setting is 0, and a
// fair_share_weight of -0 is the default, stored as unset.
func TestParse_MinusZeroIsTheIntegerZero(t *testing.T) {
	doc := mustParse(t, header+"settings:\n  max_running: -0\n")
	if !doc.Settings.Present["max_running"] || doc.Settings.Values.GetMaxRunning() != 0 {
		t.Errorf("max_running: -0 = %v", doc.Settings.Values)
	}
	doc = mustParse(t, header+"settings:\n  budget:\n    cpu_seconds: -0\n    gib_seconds: 5\n    period: 1h\n")
	if b := doc.Settings.Values.GetBudget(); b.GetCpuSeconds() != 0 || b.GetGibSeconds() != 5 {
		t.Errorf("budget.cpu_seconds: -0 = %v", b)
	}
	for _, text := range []string{"-0", "-0.0"} {
		doc = mustParse(t, header+"settings:\n  fair_share_weight: "+text+"\n")
		if w := doc.Settings.Values.GetFairShareWeight(); w != 0 || math.Signbit(w) {
			t.Errorf("fair_share_weight: %s = %v, want 0", text, w)
		}
	}
}

// TestParse_TheNonSpecificTagIsAString: `! x` is the string x, as YAML says,
// in the header and in every string setting.
func TestParse_TheNonSpecificTagIsAString(t *testing.T) {
	doc := mustParse(t, "apiVersion: ! orkstack/v1\nkind: ! Automation\nnamespace: ! payments\n"+
		"settings:\n  description: ! the payments team\n  default_pool: ! gpu\n")
	if got := doc.Settings.Values.GetDescription(); got != "the payments team" {
		t.Errorf("description = %q", got)
	}
	if got := doc.Settings.Values.GetDefaultPool(); got != "gpu" {
		t.Errorf("default_pool = %q", got)
	}
	doc = mustParse(t, header+"settings:\n  description: ! 12\n")
	if got := doc.Settings.Values.GetDescription(); got != "12" {
		t.Errorf("description: ! 12 = %q, want the string 12", got)
	}
}

// TestParse_RefusesEveryMerge: a merge key, a << value and an explicit
// !!merge tag on a key or a value are refused, as anchors and aliases are.
func TestParse_RefusesEveryMerge(t *testing.T) {
	for _, source := range []string{
		"settings:\n  max_running: !!merge 1\n",
		"settings:\n  description: <<\n",
		"!!merge settings:\n  max_running: 1\n",
		"settings:\n  !!merge max_running: 1\n",
		"settings:\n  <<: {max_running: 1}\n",
	} {
		_, issues := Parse([]byte(header + source))
		requireIssue(t, issues, "", "anchors, aliases and merge keys are not supported in an automation document")
	}
}
