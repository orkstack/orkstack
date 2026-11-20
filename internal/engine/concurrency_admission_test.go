package engine

import (
	"strings"
	"testing"
)

// TestConcurrencyAdmissionQueries: the counts read only the namespace's own
// running gates and runs, by the attributes a gate and a queued run carry.
func TestConcurrencyAdmissionQueries(t *testing.T) {
	for _, tc := range []struct {
		name, got string
		want      []string
	}{
		{"active gates", activeGatesQuery("payments"),
			[]string{`WorkflowType = "` + ConcurrencyGateWorkflowType + `"`, `OrkflowNamespace = "payments"`,
				`OrkflowGatePhase = "` + gatePhaseActive + `"`, `ExecutionStatus = "Running"`}},
		{"queued runs", queuedRunsQuery(""),
			[]string{`WorkflowType = "` + RunWorkflowType + `"`, `OrkflowNamespace = "default"`,
				`OrkflowRunQueued = "true"`, `ExecutionStatus = "Running"`}},
	} {
		for _, term := range tc.want {
			if !strings.Contains(tc.got, term) {
				t.Errorf("%s query %q lacks %q", tc.name, tc.got, term)
			}
		}
	}
}

func TestConcurrencyOpenGatesQuery(t *testing.T) {
	q := openGatesQuery()
	for _, term := range []string{`WorkflowType = "` + ConcurrencyGateWorkflowType + `"`, `ExecutionStatus = "Running"`} {
		if !strings.Contains(q, term) {
			t.Errorf("open gates query %q lacks %q", q, term)
		}
	}
}
