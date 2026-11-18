package alerts

import "strings"

// The cases' deployment: every input series carries it, as a promoted
// resource attribute would.
const caseDeployment = "d"

// s is one input series: name{labels} with the case deployment added.
func s(name, labels, values string) Series {
	l := D + `="` + caseDeployment + `"`
	if labels != "" {
		l = labels + "," + l
	}
	return Series{Series: name + "{" + l + "}", Values: values}
}

// out is an expected alert's labels: the case deployment and kv pairs.
func out(kv ...string) map[string]string {
	m := map[string]string{D: caseDeployment}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

// fire and quiet build a case at a 1-minute interval.
func fire(name, evalAt string, want map[string]string, in ...Series) Case {
	return Case{Name: name, EvalAt: evalAt, Input: in, Fires: []map[string]string{want}}
}

func quiet(name, evalAt string, in ...Series) Case {
	return Case{Name: name, EvalAt: evalAt, Input: in}
}

// The six cases every NEW rule has, by name: TestAlerts_EveryRuleHasPromtoolCases
// requires them.
var newCaseNames = []string{"appears", "steady for 2 days", "10m gap, same value", "growth during a 20m gap", "same Orker under a new instance", "a new Prometheus meets a counter already above 0"}

// newCases are a NEW rule's six cases for its counter name{labels}, which
// fires with want. They run at a 5-minute interval (10 for the two days),
// with over a day of history wherever the offset needs it, and the
// deployment's build info reporting throughout but in the last.
func newCases(name, labels string, want map[string]string) []Case {
	build := s("ork_build_info", "", "1x420")
	inst := func(i string) string {
		if labels == "" {
			return `instance="` + i + `"`
		}
		return labels + `,instance="` + i + `"`
	}
	return []Case{
		{Name: newCaseNames[0], Interval: "5m", EvalAt: "1955m", Input: []Series{build, s(name, labels, "_x390 1x20")}, Fires: []map[string]string{want}},
		{Name: newCaseNames[1], Interval: "10m", EvalAt: "48h", Input: []Series{s("ork_build_info", "", "1x300"), s(name, labels, "5x300")}},
		{Name: newCaseNames[2], Interval: "5m", EvalAt: "2030m", Input: []Series{build, s(name, labels, "1x400 _x2 1x20")}},
		{Name: newCaseNames[3], Interval: "5m", EvalAt: "2030m", Input: []Series{build, s(name, labels, "1x400 _x4 2x20")}, Fires: []map[string]string{want}},
		{Name: newCaseNames[4], Interval: "5m", EvalAt: "1955m", Input: []Series{
			build,
			s(name, inst("a"), "1x400"),
			s(name, inst("b"), "_x390 1x10"),
		}, Fires: []map[string]string{want}},
		{Name: newCaseNames[5], Interval: "5m", EvalAt: "5m", Input: []Series{s("ork_build_info", "", "1x20"), s(name, labels, "7x20")}},
	}
}

// isNEW is whether a rule's expression is a NEW one.
func isNEW(expr string) bool { return strings.Contains(expr, "unless last_over_time(") }
