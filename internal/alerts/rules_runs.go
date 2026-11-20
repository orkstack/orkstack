package alerts

// RunsFailureRatio is the recording rule the run-failure alert reads: per
// namespace, the share of finished runs that failed over 15 minutes, 0
// where runs finished and none failed.
const RunsFailureRatio = "ork:runs_failure_ratio:rate15m"

// Records are the recording rules.
func Records() []Record {
	ns := `ork_namespace="n"`
	return []Record{{
		Name: RunsFailureRatio,
		Expr: "(sum " + by("ork_namespace") + ` (rate(ork_runs_total{status="failed"}[15m])) or (sum ` + by("ork_namespace") +
			" (rate(ork_runs_total[15m])) * 0)) / sum " + by("ork_namespace") + " (rate(ork_runs_total[15m]))",
		Cases: []RecordCase{
			{Name: "a quarter failed", EvalAt: "20m", Expr: RunsFailureRatio,
				Input: []Series{s("ork_runs_total", ns+`,status="failed"`, "0+1x30"), s("ork_runs_total", ns+`,status="succeeded"`, "0+3x30")},
				Want:  []Sample{{Labels: RunsFailureRatio + `{ork_deployment="d",ork_namespace="n"}`, Value: 0.25}}},
			{Name: "none failed", EvalAt: "20m", Expr: RunsFailureRatio,
				Input: []Series{s("ork_runs_total", ns+`,status="succeeded"`, "0+3x30")},
				Want:  []Sample{{Labels: RunsFailureRatio + `{ork_deployment="d",ork_namespace="n"}`, Value: 0}}},
		},
	}}
}

func runRules() []Rule {
	const g, c = "runs", "runs"
	ns := `ork_namespace="n"`
	wantNS := out("ork_namespace", "n")
	return []Rule{
		{
			Name: "RunFailureRatioHigh", Group: g, Component: c, Severity: Warning, For: "15m",
			Expr:        RunsFailureRatio + " > 0.2 and " + on("ork_namespace") + " sum " + by("ork_namespace") + " (increase(ork_runs_total[15m])) >= 10",
			Summary:     "Over a fifth of a namespace's runs fail.",
			Description: `Over 20% of namespace "{{ $labels.ork_namespace }}"'s finished runs failed in the last 15 minutes, out of at least 10. Namespaces exported to this sink only.`,
			Meaning:     "Many runs of the namespace end FAILED: a broken Orkflow or image, a dependency they call, or the platform under them.",
			Impact:      "The namespace's work is not getting done.",
			Confirm:     "`orkctl stats failures -n NS` shows the failing tasks and their reasons.",
			Fix:         "Follow the top reason: a task's own failures go to its owner; infrastructure reasons to the alerts beside this one.",
			Related:     []string{"InfrastructureErrors", "IntegrityFailure"},
			Cases: []Case{
				fire("half fail", "45m", wantNS, s("ork_runs_total", ns+`,status="failed"`, "0+1x60"), s("ork_runs_total", ns+`,status="succeeded"`, "0+1x60")),
				quiet("few runs", "45m", s("ork_runs_total", ns+`,status="failed"`, "0+0.1x60")),
				quiet("few failures", "45m", s("ork_runs_total", ns+`,status="failed"`, "0+0.1x60"), s("ork_runs_total", ns+`,status="succeeded"`, "0+2x60")),
			},
		},
		{
			Name: "InfrastructureErrors", Group: g, Component: c, Severity: Warning, For: "15m",
			Expr: "sum " + by("ork_namespace") + ` (rate(ork_tasks_total{failure_category="infrastructure_error"}[15m])) / sum ` + by("ork_namespace") +
				" (rate(ork_tasks_total[15m])) > 0.05",
			Summary:     "Tasks fail for infrastructure reasons.",
			Description: `Over 5% of namespace "{{ $labels.ork_namespace }}"'s finished tasks failed with an infrastructure error in the last 15 minutes. Namespaces exported to this sink only.`,
			Meaning:     "Tasks fail for reasons that are not the workload's: image pulls, the object store, the container runtime, a lost Orker.",
			Impact:      "Runs fail or retry for reasons their authors cannot fix.",
			Confirm:     "`orkctl stats failures -n NS` shows the reasons.",
			Fix:         "Follow the reason: `IMAGE_PULL_FAILED` is the registry, `ARTIFACT_UPLOAD_FAILED` and `COMMIT_*` the object store, `RUNTIME_*` the Orker's host.",
			Related:     []string{"ObjectStoreErrors", "HeartbeatLosses"},
			Cases: []Case{
				fire("a tenth", "30m", wantNS, s("ork_tasks_total", ns+`,status="failed",failure_category="infrastructure_error"`, "0+1x40"), s("ork_tasks_total", ns+`,status="succeeded"`, "0+9x40")),
				quiet("workload failures only", "30m", s("ork_tasks_total", ns+`,status="failed",failure_category="activity_failed"`, "0+5x40"), s("ork_tasks_total", ns+`,status="succeeded"`, "0+9x40")),
			},
		},
		{
			Name: "IntegrityFailure", Group: g, Component: c, Severity: Critical, For: "0m",
			Expr:        NEW(`ork_deployment_orker_tasks_total{ork_failure_reason=~"`+IntegrityReasons+`"}`, "15m", "ork_failure_reason"),
			Summary:     "Stored data is not what was written.",
			Description: `An attempt failed "{{ $labels.ork_failure_reason }}": a stored object, commit or value did not match what was written, or the Orker panicked.`,
			Meaning:     "An input artifact or stored value failed its hash, a commit is unreadable or not this task's, the object store does not honour conditional writes, or the Orker hit a bug.",
			Impact:      "The task fails explicitly rather than use damaged data. Treat it as data damage until you know otherwise.",
			Confirm:     "`orkctl stats failures` shows the reason and its runs; `orkctl run get RUN` the task.",
			Fix:         "Export the run's evidence (`orkctl run evidence export RUN`) and check the store and its versioning. Never edit or delete records to get past it.",
			Related:     []string{"ObjectStoreErrors"},
			Cases:       newCases("ork_deployment_orker_tasks_total", `ork_failure_reason="COMMIT_UNDECODABLE"`, out("ork_failure_reason", "COMMIT_UNDECODABLE")),
		},
		{
			Name: "RunStateFailed", Group: g, Component: c, Severity: Critical, For: "0m",
			Expr:        NEW(`ork_deployment_run_continuations_total{outcome=~"RUN_STATE_.*"}`, "15m", "outcome"),
			Summary:     "A run could not load its saved state.",
			Description: `A run carried on into a fresh history could not load its saved state ("{{ $labels.outcome }}") and failed.`,
			Meaning:     "The saved state a long run carries forward was missing, corrupt or of a format this release does not read.",
			Impact:      "The run ended FAILED, explicitly, with its terminal event and archive.",
			Confirm:     "`orkctl stats failures` shows the `RUN_STATE_*` failures and their runs.",
			Fix:         "`RUN_STATE_UNAVAILABLE`: the object store; `RUN_STATE_CORRUPT`: data damage, as IntegrityFailure; `RUN_STATE_UNSUPPORTED`: a rollback below the release that wrote it.",
			Related:     []string{"ContinuationWriteFailed", "IntegrityFailure"},
			Cases:       newCases("ork_deployment_run_continuations_total", `outcome="RUN_STATE_CORRUPT"`, out("outcome", "RUN_STATE_CORRUPT")),
		},
		{
			Name: "ContinuationWriteFailed", Group: g, Component: c, Severity: Warning, For: "0m",
			Expr:        NEW(`ork_deployment_run_continuations_total{outcome="write_failed"}`, "15m"),
			Summary:     "A long run could not write its saved state.",
			Description: "A run that needed to carry on into a fresh history could not write its saved state; it carries on in its current history and tries again.",
			Meaning:     "Object storage refused the run's saved state.",
			Impact:      "The run keeps growing its history; if the store stays down it may fail `RUN_HISTORY_LIMIT`.",
			Confirm:     "the engine log names the run; kit: `ork_deployment_run_continuations_total{outcome=\"write_failed\"}`.",
			Fix:         "Restore object storage (ObjectStoreErrors).",
			Related:     []string{"ObjectStoreErrors"},
			Cases:       newCases("ork_deployment_run_continuations_total", `outcome="write_failed"`, out()),
		},
		{
			Name: "AdmissionRefusing", Group: g, Component: c, Severity: Info, For: "15m",
			Expr:        "sum " + by("ork_namespace", "ork_refusal_reason") + ` (rate(ork_runs_refused_total{ork_refusal_reason!="budget_unmeasurable"}[15m])) > 0`,
			Summary:     "A namespace refuses submissions.",
			Description: `Namespace "{{ $labels.ork_namespace }}" has refused submissions for 15 minutes: {{ $labels.ork_refusal_reason }}. Namespaces exported to this sink only.`,
			Meaning:     "The namespace's hold, its `max_running` or its enforced budget refuses new runs, as set.",
			Impact:      "Callers get refusals; nothing already running is touched.",
			Confirm:     "`orkctl namespace get NS` shows the hold, `max_running` and budget.",
			Fix:         "Nothing, if intended. Otherwise release the hold, raise the bound or the budget.",
			Related:     []string{"BudgetUnmeasurable"},
			Cases: []Case{
				fire("held", "30m", out("ork_namespace", "n", "ork_refusal_reason", "namespace_held"), s("ork_runs_refused_total", ns+`,ork_refusal_reason="namespace_held"`, "0+1x40")),
				quiet("budget unmeasurable is its own alert", "30m", s("ork_runs_refused_total", ns+`,ork_refusal_reason="budget_unmeasurable"`, "0+1x40")),
			},
		},
		{
			Name: "BudgetUnmeasurable", Group: g, Component: c, Severity: Warning, For: "0m",
			Expr:        NEW(`ork_deployment_runs_refused_total{ork_refusal_reason="budget_unmeasurable"}`, "15m"),
			Summary:     "A budget could not be measured, so submissions were refused.",
			Description: "A namespace with an enforced budget refused a submission because its usage could not be read.",
			Meaning:     "An enforced budget fails closed: when the engine cannot read the namespace's usage, it refuses.",
			Impact:      "Submissions to namespaces with enforced budgets are refused until usage reads again.",
			Confirm:     "`orkctl stats usage -n NS` fails or is incomplete.",
			Fix:         "Restore what usage reads: Temporal's visibility and the object store.",
			Related:     []string{"AdmissionRefusing", "CensusStale"},
			Cases:       newCases("ork_deployment_runs_refused_total", `ork_refusal_reason="budget_unmeasurable"`, out()),
		},
	}
}
