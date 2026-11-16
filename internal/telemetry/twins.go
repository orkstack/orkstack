package telemetry

import (
	"go.opentelemetry.io/otel/metric"
)

// Operator twins: for each instrument an operator rule reads, a twin named
// ork.deployment.<name>, recorded for every namespace on the default sink
// only, with every namespace, authored-name and selection label removed. A
// namespace on another sink is invisible on the default sink except through
// its twins, so an operator's alerts stay sighted without seeing a tenant's
// names (docs/DEPLOY.md, "Operator twins").
const (
	DeploymentRunsMetric          = "ork.deployment.runs"
	DeploymentTasksMetric         = "ork.deployment.tasks"
	DeploymentRunsRefusedMetric   = "ork.deployment.runs.refused"
	DeploymentContinuationsMetric = "ork.deployment.run.continuations"
)

// TwinInstruments are the engine's operator twins, on the default sink.
type TwinInstruments struct {
	Runs          metric.Int64Counter
	Tasks         metric.Int64Counter
	RunsRefused   metric.Int64Counter
	Continuations metric.Int64Counter
}

// NewTwinInstruments builds the twins on p, the default sink; on disabled
// Providers they record nothing.
func NewTwinInstruments(p *Providers) (*TwinInstruments, error) {
	meter := meterFor(p)
	runs, err := meter.Int64Counter(DeploymentRunsMetric,
		metric.WithDescription("Runs reaching a terminal state in every namespace, by status."),
		metric.WithUnit("{run}"))
	if err != nil {
		return nil, err
	}
	tasks, err := meter.Int64Counter(DeploymentTasksMetric,
		metric.WithDescription("Tasks reaching a terminal state in every namespace, by pool, status, failure category and cache outcome."),
		metric.WithUnit("{task}"))
	if err != nil {
		return nil, err
	}
	refused, err := meter.Int64Counter(DeploymentRunsRefusedMetric,
		metric.WithDescription("Submissions the admission gate refused in every namespace, by reason."),
		metric.WithUnit("{submission}"))
	if err != nil {
		return nil, err
	}
	continuations, err := meter.Int64Counter(DeploymentContinuationsMetric,
		metric.WithDescription("Run continuations into a fresh history in every namespace, by outcome."),
		metric.WithUnit("{switch}"))
	if err != nil {
		return nil, err
	}
	return &TwinInstruments{Runs: runs, Tasks: tasks, RunsRefused: refused, Continuations: continuations}, nil
}
