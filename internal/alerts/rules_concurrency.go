package alerts

// concurrencyRules are the concurrency limits' alerts (E88 §9.9.4), read
// from the census's gauges, the arrivals and exits, the gates' own reports
// and the schedule fires. A limit is its template's label
// (ork_concurrency_limit), never a rendered key. An instrument of theirs past
// its cardinality limit is MetricOverflow's, which reads every ork_* series.
func concurrencyRules() []Rule {
	const g = "concurrency"
	lim := `ork_namespace="n",ork_concurrency_limit="db#1a2b3c4d"`
	run := lim + `,ork_concurrency_level="run"`
	wantLim := out("ork_namespace", "n", "ork_concurrency_limit", "db#1a2b3c4d")
	wantNS := out("ork_namespace", "n")
	tenMin := func(c Case) Case { c.Interval = "10m"; return c }
	return []Rule{
		{
			Name: "ConcurrencyCensusDead", Group: g, Component: "concurrency", Severity: Warning, For: "5m",
			Expr: "(min " + by() + " (ork_concurrency_census_local_age_seconds) > 3 * max " + by() +
				" (ork_concurrency_census_interval_seconds) + 300) or (max " + by() + " (" + ENG + ") unless " + on() +
				" max " + by() + " (ork_concurrency_census_local_age_seconds))",
			Summary:     "The concurrency census has not run.",
			Description: "No engine replica has a concurrency census newer than three intervals plus five minutes, or none reports one.",
			Meaning:     "The census Schedule does not fire, or its pass fails every time: every concurrency gauge is stale, and the alerts below are blind.",
			Impact:      "Queues, stalled holders and rebuilds go unseen; the limits themselves keep working.",
			Confirm:     "`orkctl concurrency list` answers; kit: `ork_concurrency_census_local_age_seconds` grows on every replica.",
			Fix:         "Check the engines' logs for the census, and Temporal. An engine recreates a missing census Schedule at boot: restart one.",
			Related:     []string{"ConcurrencyCensusPartial", "TemporalUnreachable"},
			Cases: []Case{
				fire("stale", "20m", out(), s("ork_concurrency_census_local_age_seconds", "", "600+60x30"), s("ork_concurrency_census_interval_seconds", "", "60x30")),
				quiet("fresh", "20m", s("ork_concurrency_census_local_age_seconds", "", "30x30"), s("ork_concurrency_census_interval_seconds", "", "60x30"),
					s("ork_build_info", `ork_orker_id=""`, "1x30")),
			},
		},
		{
			Name: "ConcurrencyCensusPartial", Group: g, Component: "concurrency", Severity: Info, For: "15m",
			Expr: "(max " + by("ork_namespace") + " (last_over_time(ork_concurrency_census_partial[10m])) == 1) or (max " + by() +
				" (ork_concurrency_census_unavailable) == 1)",
			Summary:     "The concurrency census reads only some limits.",
			Description: "The concurrency census has not read every limit for 15 minutes, or could not run its pass.",
			Meaning:     "Some limits did not answer the census, or the census could not read the cluster.",
			Impact:      "The gauges, and the alerts on them, miss the limits it did not read.",
			Confirm:     "`orkctl concurrency list` shows the limits that are not answering.",
			Fix:         "Check Temporal's health and the engines' logs; a limit that keeps not answering is reported by ConcurrencyRebuildFailing or ConcurrencyHolderStalled.",
			Related:     []string{"ConcurrencyCensusDead"},
			Cases: []Case{
				fire("partial", "30m", wantNS, s("ork_concurrency_census_partial", `ork_namespace="n"`, "1x40")),
				quiet("whole", "30m", s("ork_concurrency_census_partial", `ork_namespace="n"`, "0x40"), s("ork_concurrency_census_unavailable", "", "0x40")),
			},
		},
		{
			Name: "ConcurrencyQueueGrowing", Group: g, Component: "concurrency", Severity: Info, For: "30m",
			Expr: "max " + by("ork_namespace", "ork_concurrency_limit") + " (last_over_time(ork_concurrency_queue_depth[10m])) > max " +
				by("ork_namespace", "ork_concurrency_limit") + " (last_over_time(ork_concurrency_queue_depth[10m] offset 30m)) and min " +
				by("ork_namespace", "ork_concurrency_limit") + " (min_over_time(ork_concurrency_queue_depth[30m])) > 0",
			Summary:     "A concurrency limit's line keeps growing.",
			Description: `The line of limit "{{ $labels.ork_concurrency_limit }}" of namespace "{{ $labels.ork_namespace }}" is longer than 30 minutes ago and has not emptied since.`,
			Meaning:     "Runs arrive faster than the limit lets them start.",
			Impact:      "Waits grow until runs time out in line (`RUN_QUEUE_TIMEOUT`).",
			Confirm:     "`orkctl concurrency list` shows the key's waiting count.",
			Fix:         "Raise the limit, spread the runs over more keys, or cancel queued runs you do not need (`orkctl run list -F queued=true`).",
			Related:     []string{"ConcurrencyQueuedTooLong"},
			Cases: []Case{
				tenMin(fire("growing", "90m", wantLim, s("ork_concurrency_queue_depth", run, "1+1x12"))),
				tenMin(quiet("steady", "90m", s("ork_concurrency_queue_depth", run, "3x12"))),
			},
		},
		{
			Name: "ConcurrencyQueuedTooLong", Group: g, Component: "concurrency", Severity: Warning, For: "5m",
			Expr:        "max " + by("ork_namespace", "ork_concurrency_limit") + " (last_over_time(ork_concurrency_queue_oldest_timeout_fraction[10m])) > 0.5",
			Summary:     "A run has waited over half its queue timeout.",
			Description: `The oldest run waiting for limit "{{ $labels.ork_concurrency_limit }}" of namespace "{{ $labels.ork_namespace }}" has used over half its queue_timeout.`,
			Meaning:     "The key's holders run long, or a holder is stuck.",
			Impact:      "Waiting runs will soon fail `RUN_QUEUE_TIMEOUT`.",
			Confirm:     "`orkctl concurrency get KEY` shows the holders and how long the line has waited.",
			Fix:         "End a stuck holder, raise the limit, or lengthen `queue_timeout`.",
			Related:     []string{"ConcurrencyHolderStalled", "ConcurrencyQueueGrowing"},
			Cases: []Case{
				fire("late", "20m", wantLim, s("ork_concurrency_queue_oldest_timeout_fraction", run, "0.8x30")),
				quiet("early", "20m", s("ork_concurrency_queue_oldest_timeout_fraction", run, "0.2x30")),
			},
		},
		{
			Name: "ConcurrencyQueueExitsUnstarted", Group: g, Component: "concurrency", Severity: Warning, For: "15m",
			Expr: "sum " + by("ork_namespace", "ork_concurrency_limit") +
				` (rate(ork_concurrency_exits_total{ork_concurrency_outcome=~"timed_out|full|unavailable|unsupported|fenced"}[15m])) > 0`,
			Summary:     "Runs waiting in a concurrency limit's line end without starting.",
			Description: `Waits in the line of limit "{{ $labels.ork_concurrency_limit }}" of namespace "{{ $labels.ork_namespace }}" have ended without the run or task starting for 15 minutes.`,
			Meaning:     "The waits reached their queue timeout, or the limit stopped answering, refused or fenced them. A cancel or a supersede does not count.",
			Impact:      "Those runs and tasks fail without doing their work.",
			Confirm:     "kit: `ork_concurrency_exits_total` by `ork_concurrency_outcome` says how they end; `orkctl concurrency get KEY` shows the line.",
			Fix:         "For `timed_out`, raise the limit or lengthen `queue_timeout`; for `unavailable`, see ConcurrencyUnavailable; for `unsupported`, see ConcurrencyIndexMissing.",
			Related:     []string{"ConcurrencyQueuedTooLong", "ConcurrencyUnavailable"},
			Cases: []Case{
				fire("timed out", "40m", wantLim, s("ork_concurrency_exits_total", run+`,ork_concurrency_strategy="queue",ork_concurrency_outcome="timed_out"`, "0+1x50")),
				quiet("one timeout", "40m", s("ork_concurrency_exits_total", run+`,ork_concurrency_strategy="queue",ork_concurrency_outcome="timed_out"`, "0x5 1x60")),
				quiet("started", "40m", s("ork_concurrency_exits_total", run+`,ork_concurrency_strategy="queue",ork_concurrency_outcome="started"`, "0+1x50")),
			},
		},
		{
			Name: "ConcurrencyQueueFillsMaxRunning", Group: g, Component: "concurrency", Severity: Warning, For: "15m",
			Expr: "max " + by("ork_namespace") + " (last_over_time(ork_concurrency_queued_runs[10m])) > 0 and " + on("ork_namespace") + " sum " +
				by("ork_namespace") + ` (rate(ork_runs_refused_total{ork_refusal_reason="max_running_reached"}[15m])) > 0`,
			Summary:     "Queued runs use up a namespace's max_running.",
			Description: `Namespace "{{ $labels.ork_namespace }}" refuses runs at max_running while runs wait in concurrency limits' lines.`,
			Meaning:     "A queued run is a run in flight: a long line counts against `max_running` and other submits are refused.",
			Impact:      "Runs unrelated to the limit cannot start; schedules' fires complete without a run.",
			Confirm:     "`orkctl run list -F queued=true` lists the queued runs; `orkctl namespace get NS` shows max_running.",
			Fix:         "Cancel queued runs you do not need, raise `--max-running`, or bound the line with `max_queued`.",
			Related:     []string{"ConcurrencyQueueGrowing"},
			Cases: []Case{
				fire("filled", "30m", wantNS, s("ork_concurrency_queued_runs", `ork_namespace="n"`, "5x40"),
					s("ork_runs_refused_total", `ork_namespace="n",ork_refusal_reason="max_running_reached"`, "0+1x40")),
				quiet("no queue", "30m", s("ork_concurrency_queued_runs", `ork_namespace="n"`, "0x40"),
					s("ork_runs_refused_total", `ork_namespace="n",ork_refusal_reason="max_running_reached"`, "0+1x40")),
			},
		},
		{
			Name: "ConcurrencyHolderStalled", Group: g, Component: "concurrency", Severity: Warning, For: "15m",
			Expr:        "max " + by("ork_namespace", "ork_concurrency_limit") + " (last_over_time(ork_concurrency_stalled_holders[10m])) > 0",
			Summary:     "A run holding a concurrency slot makes no progress.",
			Description: `A holder of limit "{{ $labels.ork_concurrency_limit }}" of namespace "{{ $labels.ork_namespace }}" has made no progress for over engine.concurrency.stalled_after.`,
			Meaning:     "The run keeps failing to make progress while it holds the slot, so the runs behind it wait.",
			Impact:      "The key's line does not move.",
			Confirm:     "`orkctl concurrency get KEY` marks the holder not making progress.",
			Fix:         "Read the run (`orkctl run get ID`), then cancel it, with `--force` if a cancel does not end it.",
			Related:     []string{"ConcurrencyQueuedTooLong"},
			Cases: []Case{
				fire("stalled", "30m", wantLim, s("ork_concurrency_stalled_holders", lim, "1x40")),
				quiet("moving", "30m", s("ork_concurrency_stalled_holders", lim, "0x40")),
			},
		},
		{
			Name: "ConcurrencySlotsNotReclaimed", Group: g, Component: "concurrency", Severity: Warning, For: "30m",
			Expr: "sum " + by("ork_namespace", "ork_concurrency_limit") + " (rate(ork_concurrency_verify_errors_total[15m])) > 0 and " +
				on("ork_namespace", "ork_concurrency_limit") + " max " + by("ork_namespace", "ork_concurrency_limit") + " (last_over_time(ork_concurrency_keys_full[10m])) > 0",
			Summary:     "A full concurrency limit cannot check who holds it.",
			Description: `Limit "{{ $labels.ork_concurrency_limit }}" of namespace "{{ $labels.ork_namespace }}" has a full key, and its checks of the runs holding or waiting for it have failed for 30 minutes.`,
			Meaning:     "The limit asks the cluster about each run it holds a slot for and cannot get an answer, so it keeps every slot.",
			Impact:      "A slot whose run has already ended is not freed, and the line behind it waits.",
			Confirm:     "The engines' logs say which checks fail (\"kept entries it could not check\"); `orkctl concurrency get KEY` shows the holders.",
			Fix:         "Check Temporal's health; the limit checks again by itself and frees the slots of runs that ended.",
			Related:     []string{"ConcurrencyHolderStalled", "TemporalUnreachable"},
			Cases: []Case{
				fire("full and unchecked", "60m", wantLim, s("ork_concurrency_verify_errors_total", lim, "0+1x70"), s("ork_concurrency_keys_full", lim, "1x70")),
				quiet("not full", "60m", s("ork_concurrency_verify_errors_total", lim, "0+1x70"), s("ork_concurrency_keys_full", lim, "0x70")),
				quiet("checked", "60m", s("ork_concurrency_verify_errors_total", lim, "0x70"), s("ork_concurrency_keys_full", lim, "1x70")),
			},
		},
		{
			Name: "ConcurrencyRebuildFailing", Group: g, Component: "concurrency", Severity: Warning, For: "10m",
			Expr:        "max " + by("ork_namespace") + ` (last_over_time(ork_concurrency_rebuilding{ork_concurrency_state="failing"}[10m])) > 0`,
			Summary:     "A concurrency limit cannot read which runs hold it.",
			Description: `A limit of namespace "{{ $labels.ork_namespace }}" has failed to rebuild its record of running runs for longer than engine.concurrency.rebuild_timeout.`,
			Meaning:     "The limit lost its record, and its reading of the cluster's runs keeps failing.",
			Impact:      "It grants nothing: its line waits, and refuse and skip limits are refused as unavailable.",
			Confirm:     "`orkctl concurrency get KEY` shows the limit reading its holders, failing since when.",
			Fix:         "Check Temporal's visibility (the index of runs); the limit retries by itself.",
			Related:     []string{"ConcurrencyIndexMissing", "TemporalUnreachable"},
			Cases: []Case{
				fire("failing", "30m", wantNS, s("ork_concurrency_rebuilding", `ork_namespace="n",ork_concurrency_state="failing"`, "1x40")),
				quiet("rebuilding", "30m", s("ork_concurrency_rebuilding", `ork_namespace="n",ork_concurrency_state="running"`, "1x40")),
			},
		},
		{
			Name: "ConcurrencyQueueMismatch", Group: g, Component: "concurrency", Severity: Info, For: "30m",
			Expr: "(abs(max " + by("ork_namespace") + " (last_over_time(ork_concurrency_queued_runs[10m])) - sum " + by("ork_namespace") +
				" (max " + by("ork_namespace", "ork_concurrency_limit") + ` (last_over_time(ork_concurrency_queue_depth{ork_concurrency_level="run"}[10m])))) > 0) unless ` +
				on("ork_namespace") + " (max " + by("ork_namespace") + " (last_over_time(ork_concurrency_census_partial[10m])) == 1)",
			Summary:     "The runs marked queued and the limits' lines disagree.",
			Description: `In namespace "{{ $labels.ork_namespace }}", the runs the index marks queued and the runs the limits' lines hold have differed for 30 minutes.`,
			Meaning:     "A run's queued mark, or a limit's record of its line, is out of date.",
			Impact:      "`orkctl run list -F queued=true` and `orkctl concurrency list` show different runs waiting.",
			Confirm:     "Compare `orkctl run list -F queued=true` with `orkctl concurrency list`.",
			Fix:         "Nothing at once: the runs re-assert their place and the limits check their holders. If it persists, read the engines' logs.",
			Related:     []string{"ConcurrencyCensusPartial"},
			Cases: []Case{
				fire("off by two", "50m", wantNS, s("ork_concurrency_queued_runs", `ork_namespace="n"`, "5x60"), s("ork_concurrency_queue_depth", run, "3x60")),
				quiet("agree", "50m", s("ork_concurrency_queued_runs", `ork_namespace="n"`, "3x60"), s("ork_concurrency_queue_depth", run, "3x60")),
			},
		},
		{
			Name: "ConcurrencyResetIncomplete", Group: g, Component: "concurrency", Severity: Warning, For: "5m",
			Expr:        "max " + by() + " (last_over_time(ork_concurrency_resets_incomplete[10m])) > 0",
			Summary:     "A concurrency limit's reset does not finish.",
			Description: "A reset of a concurrency limit has run longer than engine.concurrency.reset_incomplete_after.",
			Meaning:     "The limit was reset and still cannot read which runs hold or wait for its key.",
			Impact:      "The key grants no slot: runs wait in line, and `refuse` and `skip` runs fail `RUN_CONCURRENCY_UNAVAILABLE`.",
			Confirm:     "`orkctl admin audit -F 'action^concurrency.reset.'` names the key; `orkctl concurrency get KEY` shows the limit reading its holders.",
			Fix:         "See ConcurrencyRebuildFailing: the limit keeps trying, and the reset completes once it can read its runs.",
			Related:     []string{"ConcurrencyRebuildFailing", "TemporalUnreachable"},
			Cases: []Case{
				fire("incomplete", "30m", out(), s("ork_concurrency_resets_incomplete", ``, "1x40")),
				quiet("none", "30m", s("ork_concurrency_resets_incomplete", ``, "0x40")),
			},
		},
		{
			Name: "ConcurrencyUnavailable", Group: g, Component: "concurrency", Severity: Warning, For: "5m",
			Expr:        "sum " + by("ork_namespace") + ` (rate(ork_concurrency_arrivals_total{ork_concurrency_outcome="unavailable"}[5m])) > 0`,
			Summary:     "Concurrency limits do not answer.",
			Description: `Requests for a concurrency slot in namespace "{{ $labels.ork_namespace }}" have found the limit unavailable for 5 minutes.`,
			Meaning:     "The limit did not answer within engine.concurrency.acquire_timeout, or was rebuilding its record when asked.",
			Impact:      "Runs fail `RUN_CONCURRENCY_UNAVAILABLE`, and submits are refused `CONCURRENCY_CHECK_UNAVAILABLE`.",
			Confirm:     "`orkctl concurrency list` shows limits not answering.",
			Fix:         "Check Temporal and the engines; see ConcurrencyRebuildFailing.",
			Related:     []string{"ConcurrencyRebuildFailing", "TemporalUnreachable"},
			Cases: []Case{
				fire("unavailable", "20m", wantNS, s("ork_concurrency_arrivals_total", run+`,ork_concurrency_strategy="queue",ork_concurrency_outcome="unavailable"`, "0+1x30")),
				quiet("granted", "20m", s("ork_concurrency_arrivals_total", run+`,ork_concurrency_strategy="queue",ork_concurrency_outcome="granted"`, "0+1x30")),
			},
		},
		{
			Name: "ConcurrencyFireNotStarted", Group: g, Component: "concurrency", Severity: Info, For: "30m",
			Expr:        "sum " + by("ork_namespace") + ` (rate(ork_schedule_fires_total{outcome=~"unavailable|not_started"}[30m])) > 0`,
			Summary:     "Schedules' fires start no run.",
			Description: `Fires of namespace "{{ $labels.ork_namespace }}" have completed without a run, or found the engine unavailable, for 30 minutes.`,
			Meaning:     "A concurrency limit's line was full, max_running was full of queued runs, or the limit could not be checked.",
			Impact:      "Those fires' work is not done; the schedules do not pause for it.",
			Confirm:     "`orkctl schedule get NAME` shows each fire's NOTE.",
			Fix:         "Read the NOTE: shorten the line, raise the limit or max_running, or see ConcurrencyUnavailable.",
			Related:     []string{"ConcurrencyUnavailable", "ScheduleFiresFailing"},
			Cases: []Case{
				fire("not started", "50m", wantNS, s("ork_schedule_fires_total", `ork_namespace="n",outcome="not_started"`, "0+1x60")),
				quiet("started", "50m", s("ork_schedule_fires_total", `ork_namespace="n",outcome="started"`, "0+1x60")),
			},
		},
		{
			Name: "ConcurrencyIndexMissing", Group: g, Component: "concurrency", Severity: Warning, For: "5m",
			Expr:        "max " + by() + " (ork_concurrency_index_unavailable) == 1",
			Summary:     "The cluster does not index concurrency limits.",
			Description: "An engine booted with the concurrency search attributes missing.",
			Meaning:     "Runs of an Orkflow that declares `concurrency:` are refused `CONCURRENCY_UNSUPPORTED (cause=index)`.",
			Impact:      "No limited Orkflow runs.",
			Confirm:     "`orkctl run list -F queued=true` is refused; the engine's boot log says which attribute is missing.",
			Fix:         "Register the search attributes (docs/DEPLOY.md, \"Search attributes\") and restart the engines.",
			Related:     []string{"ConcurrencyRebuildFailing"},
			Cases: []Case{
				fire("missing", "20m", out(), s("ork_concurrency_index_unavailable", "", "1x30")),
				quiet("indexed", "20m", s("ork_concurrency_index_unavailable", "", "0x30")),
			},
		},
	}
}
