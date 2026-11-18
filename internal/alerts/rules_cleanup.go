package alerts

func cleanupRules() []Rule {
	const g = "cleanup"
	enabled := func(v string) Series { return s("ork_cleanup_enabled_ratio", "", v) }
	interval := func(v string) Series { return s("ork_cleanup_interval_seconds", "", v) }
	mode := func(m, v string) Series { return s("ork_gc_mode", `mode="`+m+`"`, v) }
	held := func(r string) Expect {
		return Expect{Rule: "ArtifactGCHeld", Fires: []map[string]string{out("ork_gc_hold_reason", r)}}
	}
	fiveMin := func(c Case) Case { c.Interval = "5m"; return c }
	return []Rule{
		{
			Name: "CleanupDisabled", Group: g, Component: "cleanup", Severity: Info, For: "24h",
			Expr:        "max " + by() + " (ork_cleanup_enabled_ratio) == 0",
			Summary:     "Retention cleanup is off.",
			Description: "Retention cleanup has been off on every engine for a day.",
			Meaning:     "`ORK_CLEANUP_ENABLED` is false: finished runs past retention are never retired, and the artifact GC never runs.",
			Impact:      "The bucket only grows; the cluster's own backstop deletes executions at the retention buffer.",
			Confirm:     "`orkctl admin gc preview` shows `cleanup_enabled` false.",
			Fix:         "Nothing, if intended (during a restore it is). Otherwise turn it on.",
			Related:     []string{"CleanupNotReporting"},
			Cases: []Case{
				fiveMin(fire("off for 2 days", "26h", out(), enabled("0x320"))),
				fiveMin(quiet("on", "26h", enabled("1x320"))),
			},
		},
		{
			Name: "CleanupNotReporting", Group: g, Component: "cleanup", Severity: Warning, For: "10m",
			Expr:        "max " + by() + " (ork_cleanup_enabled_ratio) == 1 unless " + on() + " (time() - max " + by() + " (ork_cleanup_last_pass_seconds) < 3 * " + IV + ")",
			Summary:     "Retention cleanup has not completed a walk for three intervals.",
			Description: "Retention cleanup is on, and no engine has completed a walk of the run index for three cleanup intervals.",
			Meaning:     "Cleanup's passes fail or never reach the end of the index: usually Temporal's visibility or the object store. It also fires while an engine's first walk since boot has not completed, as on a large index or after a restore, and while walks cannot read some run, since such a walk does not count.",
			Impact:      "Runs past retention are not retired; past the retention buffer, the cluster's backstop deletes their executions, archived or not (see the archive-sweep risk, above).",
			Confirm:     "the engine log line `artifact cleanup iteration completed` stops appearing; kit: `ork_cleanup_last_pass_seconds`.",
			Fix:         "Restore what the engine log names. Read CleanupFailing beside it.",
			Related:     []string{"CleanupFailing", "CleanupBehindRetention", "#the-archive-sweep-risk"},
			Cases: []Case{
				fire("no walk", "30m", out(), enabled("1x60"), interval("60x60"), s("ork_cleanup_last_pass_seconds", "", "0x60")),
				quiet("walking", "30m", enabled("1x60"), interval("60x60"), s("ork_cleanup_last_pass_seconds", "", "0+60x60")),
				quiet("interval 6 h", "60m", enabled("1x90"), interval("21600x90"), s("ork_cleanup_last_pass_seconds", "", "0x90")),
			},
		},
		{
			Name: "CleanupFailing", Group: g, Component: "cleanup", Severity: Warning, For: "0m",
			Expr:        NEW(`ork_cleanup_passes_total{outcome="failed"}`, "2h") + " or " + NEW(`ork_deployment_cleanup_runs_total{outcome="failed"}`, "2h"),
			Summary:     "Retention cleanup fails.",
			Description: "A retention cleanup pass, or the retirement of a run, failed in the last 2 hours; a later pass tries again.",
			Meaning:     "A step of cleanup failed: reading the policy or the index, checking an archive, retiring or deleting.",
			Impact:      "Runs due for retirement wait for a later pass.",
			Confirm:     "the engine log names the failing step; kit: `ork_cleanup_errors_total` by `ork_cleanup_step`.",
			Fix:         "Restore what the step needs: Temporal for the index, the object store for the rest.",
			Related:     []string{"CleanupNotReporting"},
			Cases:       newCases("ork_cleanup_passes_total", `outcome="failed"`, out()),
		},
		{
			Name: "CleanupBehindRetention", Group: g, Component: "cleanup", Severity: Warning, For: "1h",
			Expr:        "max " + by() + " (ork_deployment_cleanup_retention_lag_seconds) > 0.5 * " + RB,
			Summary:     "Retention cleanup is far behind.",
			Description: "The most overdue run cleanup left unretired is past its due time by over half the retention buffer.",
			Meaning:     "Cleanup retires runs slower than they become due.",
			Impact:      "Past the retention buffer, the cluster's backstop deletes executions, and an unarchived one is lost.",
			Confirm:     "kit: `ork_deployment_cleanup_retention_lag_seconds` against `ork_cleanup_retention_buffer_seconds`.",
			Fix:         "Shorten `ORK_CLEANUP_INTERVAL` or raise its batch; check CleanupFailing.",
			Related:     []string{"CleanupFailing", "ArchiveUnconfirmed", "#the-archive-sweep-risk"},
			Cases: []Case{
				fire("behind", "90m", out(), s("ork_deployment_cleanup_retention_lag_seconds", "", "100000x100"), s("ork_cleanup_retention_buffer_seconds", "", "86400x100")),
				quiet("keeping up", "90m", s("ork_deployment_cleanup_retention_lag_seconds", "", "60x100"), s("ork_cleanup_retention_buffer_seconds", "", "86400x100")),
			},
		},
		{
			Name: "ExecutionsMissing", Group: g, Component: "cleanup", Severity: Critical, For: "0m",
			Expr:        NEW(`ork_deployment_cleanup_runs_total{outcome="not_found_not_due"}`, "1h"),
			Summary:     "Runs are missing from Temporal before their retention ended.",
			Description: "Cleanup found runs whose executions are gone from Temporal although their retention has not ended.",
			Meaning:     "Temporal lost executions it should still hold: a restore to an older point, a namespace retention shorter than OrkStack's, or data loss. Cleanup holds those runs rather than retire them early.",
			Impact:      "Those runs' live records are gone; their archives, if written, still answer. The artifact GC holds (`executions_missing`).",
			Confirm:     "`orkctl admin info` shows the cluster; `orkctl admin gc preview` shows the hold `executions_missing`.",
			Fix:         "If Temporal was restored, follow Backup and disaster recovery, above. Otherwise check the Temporal namespace's retention.",
			Related:     []string{"ArtifactGCHeld", "#backup-and-disaster-recovery"},
			Cases:       newCases("ork_deployment_cleanup_runs_total", `outcome="not_found_not_due"`, out()),
		},
		{
			Name: "ArtifactGCFailing", Group: g, Component: "gc", Severity: Warning, For: "0m",
			Expr:        "sum " + by() + ` (increase(ork_gc_passes_total{outcome="failed"}[6h])) >= 3 or ` + NEW(`ork_deployment_gc_objects_total{outcome="failed"}`, "6h", "ork_gc_reason"),
			Summary:     "The artifact GC fails.",
			Description: "The artifact GC failed at least 3 passes in 6 hours, or failed to delete objects.",
			Meaning:     "The GC could not list, check or delete what it reclaims.",
			Impact:      "Space is not reclaimed; nothing is lost.",
			Confirm:     "the engine log names the failure; kit: `ork_gc_passes_total{outcome=\"failed\"}` and `ork_deployment_gc_objects_total{outcome=\"failed\"}` by reason.",
			Fix:         "Restore the object store or Temporal, whichever the engine log names.",
			Related:     []string{"ArtifactGCNotReporting"},
			Cases:       newCases("ork_deployment_gc_objects_total", `outcome="failed",ork_gc_reason="store_error"`, out("ork_gc_reason", "store_error")),
		},
		{
			Name: "ArtifactGCNotReporting", Group: g, Component: "gc", Severity: Warning, For: "10m",
			Expr: "(max " + by() + " (ork_cleanup_enabled_ratio) == 1 and " + on() + " max " + by() + ` (ork_gc_mode{mode=~"dry_run|delete"}) == 1) unless ` +
				on() + " (time() - max " + by() + " (ork_gc_last_pass_seconds) < 3 * " + IV + ")",
			Summary:     "The artifact GC has not made a pass for three intervals.",
			Description: "The artifact GC is on, and no engine has made a pass for three cleanup intervals.",
			Meaning:     "The GC runs inside retention cleanup; cleanup is not reaching it, or no engine has made a pass since it booted.",
			Impact:      "Space is not reclaimed.",
			Confirm:     "`orkctl admin gc preview` shows the mode and that cleanup is on; kit: `ork_gc_last_pass_seconds`.",
			Fix:         "See CleanupNotReporting.",
			Related:     []string{"CleanupNotReporting"},
			Cases: []Case{
				fire("no pass", "30m", out(), enabled("1x60"), interval("60x60"), mode("dry_run", "1x60"), s("ork_gc_last_pass_seconds", "", "0x60")),
				quiet("passing", "30m", enabled("1x60"), interval("60x60"), mode("dry_run", "1x60"), s("ork_gc_last_pass_seconds", "", "0+60x60")),
				quiet("cleanup disabled, dry_run", "30m", enabled("0x60"), interval("60x60"), mode("dry_run", "1x60"), s("ork_gc_last_pass_seconds", "", "0x60")),
			},
		},
		{
			Name: "ArtifactGCNoProgress", Group: g, Component: "gc", Severity: Warning, For: "1h",
			Expr:        "max " + by() + " (ork_gc_due_runs) > 0 and " + on() + " (time() - max " + by() + " (ork_gc_last_progress_seconds) > 6 * " + IV + ")",
			Summary:     "The artifact GC makes no progress.",
			Description: "Runs are due for reclaim, and the artifact GC has finished none and deleted nothing for six cleanup intervals.",
			Meaning:     "Passes run but stop before they finish a run: each is cut short by its budget, or fails on the same run.",
			Impact:      "Space is not reclaimed.",
			Confirm:     "kit: `ork_gc_due_runs` and `ork_gc_last_progress_seconds`; the engine log of each pass.",
			Fix:         "Read the engine log for the run every pass stops at; raise the GC's batch if passes are only cut short.",
			Related:     []string{"ArtifactGCFailing", "ArtifactGCBehind"},
			Cases: []Case{
				fire("stuck", "80m", out(), interval("60x90"), s("ork_gc_due_runs", "", "5x90"), s("ork_gc_last_progress_seconds", "", "0x90")),
				quiet("progressing", "80m", interval("60x90"), s("ork_gc_due_runs", "", "5x90"), s("ork_gc_last_progress_seconds", "", "0+60x90")),
			},
		},
		{
			Name: "ArtifactGCBehind", Group: g, Component: "gc", Severity: Warning, For: "2h",
			Expr:        "max " + by() + " (ork_gc_lag_seconds) > 21600",
			Summary:     "The artifact GC is over 6 hours behind.",
			Description: "The oldest run due for reclaim has waited over 6 hours since it retired.",
			Meaning:     "Runs retire faster than the GC reclaims them.",
			Impact:      "Space is reclaimed late.",
			Confirm:     "kit: `ork_gc_lag_seconds` and `ork_gc_due_runs`.",
			Fix:         "Raise the GC's batch (`ORK_CLEANUP_ARTIFACT_GC_BATCH`) or shorten the cleanup interval.",
			Related:     []string{"ArtifactGCNoProgress"},
			Cases: []Case{
				fiveMin(fire("behind", "3h", out(), s("ork_gc_lag_seconds", "", "30000x40"))),
				fiveMin(quiet("current", "3h", s("ork_gc_lag_seconds", "", "60x40"))),
			},
		},
		{
			Name: "ArtifactGCHeld", Group: g, Component: "gc", Severity: Info, For: "2h",
			Expr:        "max " + by("ork_gc_hold_reason") + ` (ork_gc_held{ork_gc_hold_reason!="local_backend"}) == 1`,
			Summary:     "The artifact GC is held.",
			Description: `The artifact GC has held its passes for 2 hours: {{ $labels.ork_gc_hold_reason }}.`,
			Meaning:     "The GC refuses to delete while a condition it cannot verify stands. Waiting on the low-water mark is not a hold, and neither is a warming pass.",
			Impact:      "Nothing is reclaimed; nothing is lost.",
			Confirm:     "`orkctl admin gc preview` shows the hold reason; `orkctl release status` for a mixed fleet.",
			Fix:         "`engines_lack_composed_artifacts`: finish the release rollout. `capabilities_unreadable`: Temporal. `lowwater_unknown`: a sweep that was not clean (the archive-sweep risk, above), Temporal's visibility, or a pass that clears itself. `clock_skew`: fix NTP (StoreClockSkew). `executions_missing`: ExecutionsMissing.",
			Related:     []string{"ArtifactGCHeldLong", "StoreClockSkew", "ExecutionsMissing"},
			Cases: []Case{
				fiveMin(fire("clock skew", "3h", out("ork_gc_hold_reason", "clock_skew"), s("ork_gc_held", `ork_gc_hold_reason="clock_skew"`, "1x40"))),
				fiveMin(quiet("local backend is not a hold", "3h", s("ork_gc_held", `ork_gc_hold_reason="local_backend"`, "1x40"))),
			},
		},
		{
			Name: "ArtifactGCHeldLong", Group: g, Component: "gc", Severity: Warning, For: "24h",
			Expr:        "max " + by("ork_gc_hold_reason") + ` (ork_gc_held{ork_gc_hold_reason!="local_backend"}) == 1`,
			Summary:     "The artifact GC has been held for a day.",
			Description: `The artifact GC has held its passes for a day: {{ $labels.ork_gc_hold_reason }}.`,
			Meaning:     "A hold has stood for a day. Of the low-water causes, a pass mid-switch and a run with no creation time clear on a later pass; a hold that stands a day has a cause that needs an operator.",
			Impact:      "Nothing is reclaimed for as long as it stands.",
			Confirm:     "`orkctl admin gc preview` shows the hold reason.",
			Fix:         "As ArtifactGCHeld.",
			Related:     []string{"ArtifactGCHeld"},
			Cases: []Case{
				{Name: "held for 2 days", Interval: "5m", EvalAt: "26h", Input: []Series{s("ork_gc_held", `ork_gc_hold_reason="executions_missing"`, "1x320")},
					Fires: []map[string]string{out("ork_gc_hold_reason", "executions_missing")}, Also: []Expect{held("executions_missing")}},
				fiveMin(quiet("released", "26h", s("ork_gc_held", `ork_gc_hold_reason="executions_missing"`, "1x120 0x200"))),
			},
		},
		{
			Name: "ArtifactGCLowWaterLag", Group: g, Component: "gc", Severity: Info, For: "1h",
			Expr:        "max " + by() + " (ork_gc_lowwater_age_seconds) > 604800 and " + on() + " max " + by() + " (ork_gc_waiting_runs) > 0",
			Summary:     "A week-old run holds the artifact GC back.",
			Description: "The oldest open or unarchived run is over a week old, and retired runs wait on it before they can be reclaimed.",
			Meaning:     "Not a fault: reclaim waits for every run created before a retirement to finish and be archived.",
			Impact:      "Space from before that run's creation is reclaimed only when it ends.",
			Confirm:     "`orkctl admin gc preview` names the oldest blocking run; then `orkctl run get RUN`.",
			Fix:         "If the run is stuck, finish or cancel it; otherwise accept the lag, which ends when it does.",
			Cases: []Case{
				fire("a month-old await", "90m", out(), s("ork_gc_lowwater_age_seconds", "", "2600000x100"), s("ork_gc_waiting_runs", "", "4x100")),
				quiet("nothing waits", "90m", s("ork_gc_lowwater_age_seconds", "", "2600000x100"), s("ork_gc_waiting_runs", "", "0x100")),
			},
		},
		{
			Name: "ArtifactGCDryRun", Group: g, Component: "gc", Severity: Info, For: "1h",
			Expr: "min_over_time(max " + by() + ` (ork_gc_mode{mode="dry_run"})[7d:1h]) == 1 and ` + on() + " max " + by() +
				` (ork_gc_mode{mode="dry_run"} offset 7d) == 1 and ` + on() + " max " + by() + " (ork_deployment_gc_would_delete_bytes) > 0",
			Summary:     "The artifact GC has run in dry-run for a week with space to reclaim.",
			Description: "The artifact GC has been in dry-run for 7 days and would delete objects.",
			Meaning:     "Dry-run reports what delete mode would reclaim; it was left on.",
			Impact:      "The space it reports is not reclaimed.",
			Confirm:     "`orkctl admin gc preview` shows the mode and what it would delete.",
			Fix:         "Review the preview, then set `ORK_CLEANUP_ARTIFACT_GC=delete`.",
			Cases: []Case{
				fiveMin(fire("a week in dry-run", "190h", out(), mode("dry_run", "1x2300"), s("ork_deployment_gc_would_delete_bytes", "", "1e9x2300"))),
				fiveMin(quiet("two days of history", "48h", mode("dry_run", "1x600"), s("ork_deployment_gc_would_delete_bytes", "", "1e9x600"))),
				fiveMin(quiet("delete mode", "190h", mode("dry_run", "0x2300"), mode("delete", "1x2300"), s("ork_deployment_gc_would_delete_bytes", "", "1e9x2300"))),
			},
		},
		{
			Name: "ArtifactGCLateCommit", Group: g, Component: "gc", Severity: Info, For: "0m",
			Expr:        NEW(`ork_gc_objects_total{outcome="kept",ork_gc_reason="late_commit"}`, "15m"),
			Summary:     "A commit appeared after a run retired; its objects are kept.",
			Description: "The artifact GC kept objects because a commit appeared after their run retired.",
			Meaning:     "An Orker committed for a run after the run retired. The GC keeps such objects for good: a counted leak, never a loss.",
			Impact:      "A little space is never reclaimed.",
			Confirm:     "`orkctl admin gc preview` shows objects kept for `late_commit`.",
			Fix:         "Nothing is required. Frequent late commits mean Orkers that keep working long after their runs end.",
			Cases:       newCases("ork_gc_objects_total", `ork_namespace="n",outcome="kept",ork_gc_reason="late_commit"`, out()),
		},
	}
}

// Rules is every alerting rule, in the order the rule file and the
// runbooks hold them.
func Rules() []Rule {
	var out []Rule
	for _, group := range [][]Rule{pipelineRules(), poolRules(), runRules(), storageRules(), temporalRules(), scheduleRules(), cleanupRules()} {
		out = append(out, group...)
	}
	return out
}
