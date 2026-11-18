package alerts

import (
	"fmt"
	"strings"
)

// D is the deployment label: the promoted ork.deployment resource attribute
// (and a datapoint attribute of ork.build.info). Every aggregation keeps it,
// so one Prometheus can watch several deployments; without promotion it is
// empty on every series and the rules still hold for one deployment.
const D = "ork_deployment"

// by is an aggregation's grouping, the deployment first.
func by(labels ...string) string {
	return "by (" + strings.Join(append([]string{D}, labels...), ", ") + ")"
}

// on is a vector match, the deployment first.
func on(labels ...string) string {
	return "on (" + strings.Join(append([]string{D}, labels...), ", ") + ")"
}

// NEW is true for each group whose counter grew in the last w, or appeared
// with no sample in the day before w ago while its deployment was already
// reporting then. It is evaluated per series before the sum, so a new
// process (a new instance) counts as growth; a gap of up to a day that
// returns the same value is quiet; a Prometheus that is new, or lost its
// history, sees no deployment w ago and so takes no counter's lifetime value
// for growth, nor does a deployment's first process in its first w. Its one
// false fire is a gap longer than a day that returns the same value. With
// no ork_build_info at all every NEW rule is quiet, which EngineAllDown
// reports. It needs no rate(), so a counter that moved once fires once.
func NEW(m, w string, labels ...string) string {
	return fmt.Sprintf("sum %s (((%s - last_over_time(%s[1d] offset %s)) > 0) or ((%s unless last_over_time(%s[1d] offset %s)) and %s (count %s (last_over_time(ork_build_info[1d] offset %s)) > 0))) > 0",
		by(labels...), m, m, w, m, m, w, on(), by(), w)
}

var (
	// P is each pool's pollers, as the engine's census reads them.
	P = "max " + by("ork_pool") + " (ork_pool_pollers)"
	// F is each pool's Orkers exporting metrics, one per ork_orker_id
	// whatever its instances.
	F = "count " + by("ork_pool") + " (count " + by("ork_pool", "ork_orker_id") + " (ork_orker_present))"
	// W is a pool whose backlog has been non-zero for 30 minutes and is
	// growing, or whose oldest task has waited over ten minutes.
	W = "(max " + by("ork_pool") + " (max_over_time(ork_pool_backlog_tasks[30m])) > 0) and " + on("ork_pool") +
		" ((max " + by("ork_pool") + " (deriv(ork_pool_backlog_tasks[15m])) > 0) or (max " + by("ork_pool") +
		" (max_over_time(ork_pool_backlog_age_seconds[30m])) > 600))"
	// ENG is an engine's build info: an Orker's carries its ork_orker_id.
	ENG = `ork_build_info{ork_orker_id=""}`
	// PipeUp and PipeOK read the Collector's own metrics, which carry no
	// deployment: the Collector is up, and up and exporting metrics.
	PipeUp = "count(max_over_time(otelcol_process_uptime_seconds_total[5m])) > 0"
	PipeOK = PipeUp + " and on () ((sum(rate(otelcol_exporter_send_failed_metric_points_total[5m])) or vector(0)) == 0)"
	// IV, RB and GR are the cleanup interval, the retention buffer and the
	// GC grace in force: thresholds read the setting, never a default.
	IV = "max " + by() + " (ork_cleanup_interval_seconds)"
	RB = "max " + by() + " (ork_cleanup_retention_buffer_seconds)"
	GR = "max " + by() + " (ork_gc_grace_seconds)"
)

// pipelineMacros are the sub-expressions that read the Collector's own
// metrics and so carry no deployment label.
var pipelineMacros = []string{PipeOK, PipeUp}

// anyRate is true when any of the Collector's counters moved in w.
func anyRate(w string, metrics ...string) string {
	var parts []string
	for _, m := range metrics {
		parts = append(parts, fmt.Sprintf("(sum(rate(%s[%s])) > 0)", m, w))
	}
	return strings.Join(parts, " or ")
}

// rTemporal is a Temporal SDK client counter's rate per process kind, 0
// where the process makes requests but none of this kind failed.
func rTemporal(m string) string {
	return fmt.Sprintf("(sum %s (rate(%s[5m])) or (sum %s (rate(temporal_request_total[5m])) * 0))",
		by("ork_component"), m, by("ork_component"))
}

var (
	tFail  = "(" + rTemporal("temporal_request_failure_total") + " + " + rTemporal("temporal_long_request_failure_total") + ")"
	tTotal = "clamp_min(" + rTemporal("temporal_request_total") + " + " + rTemporal("temporal_long_request_total") + ", 1e-9)"
)

// integrityReasons are the attempt failures that mean stored data is not
// what was written: never a workload's fault, and never retried away.
const integrityReasons = "INPUT_ARTIFACT_CORRUPT|VALUES_CORRUPT|COMMIT_IDENTITY_MISMATCH|COMMIT_UNDECODABLE|COMMIT_STORE_NOT_CONDITIONAL|PANIC"
