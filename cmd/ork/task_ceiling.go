package main

import (
	"fmt"
	"os"

	"github.com/orkstack/orkstack/internal/engine"
)

// taskCeiling is engine.orkflow_max_tasks (ORK_ORKFLOW_MAX_TASKS) as the
// boot decided it: the effective value, whether it was set, and what the
// boot logs about it (docs/DEPLOY.md §1, §5 "Task ceiling and the flat-stage
// bound").
type taskCeiling struct {
	value    int32
	explicit bool
	// flatBound is X, ShapeMaxTasks for the configured settings, with
	// continuation on; zero with it off.
	flatBound int64
	// warnings are logged at boot, Warn, one line each.
	warnings []string
}

// taskCeilingFromEnv reads ORK_ORKFLOW_MAX_TASKS after the configuration
// document has been applied to the environment.
//
//   - With continuation on, unset is engine.DefaultOrkflowMaxTasks (N, the
//     largest size the task soak measured), and any whole number 1 or more
//     is accepted: a deployment lowers or raises it. An explicit value above
//     N warns that the deployment runs unmeasured, and a value above X — the
//     flat-stage bound for the configured history, continuation and
//     in-flight settings (engine.ShapeMaxTasks) — warns that a stage of more
//     than X plain tasks that start together may stop RUN_HISTORY_LIMIT.
//   - With continuation off a run has one history: unset is min(N,
//     history.max_events / engine.UncontinuedEventsPerTask), and an explicit
//     value above that quotient is refused, naming the three keys.
func taskCeilingFromEnv(historyMaxEvents, historyMaxBytes int64, cont continuationConfig, runMaxInFlight int32) (taskCeiling, error) {
	raw, set := os.LookupEnv("ORK_ORKFLOW_MAX_TASKS")
	explicit := set && raw != ""
	value, err := envInt32("ORK_ORKFLOW_MAX_TASKS", engine.DefaultOrkflowMaxTasks)
	if err != nil {
		return taskCeiling{value: engine.DefaultOrkflowMaxTasks}, err
	}
	out := taskCeiling{value: value, explicit: explicit}
	if !cont.enabled {
		quotient := historyMaxEvents / engine.UncontinuedEventsPerTask
		if !explicit {
			out.value = int32(min(int64(engine.DefaultOrkflowMaxTasks), max(quotient, 1)))
			return out, nil
		}
		if int64(value) > quotient {
			return out, fmt.Errorf("ORK_ORKFLOW_MAX_TASKS=%d is above engine.history.max_events "+
				"(ORK_HISTORY_MAX_EVENTS=%d) / %d = %d: with engine.continuation.enabled off "+
				"(ORK_CONTINUATION_ENABLED=false) a run has one history, and each task needs about %d "+
				"events of it; lower engine.orkflow_max_tasks, declare a larger history limit, or turn "+
				"continuation on", value, historyMaxEvents, engine.UncontinuedEventsPerTask, quotient,
				engine.UncontinuedEventsPerTask)
		}
		return out, nil
	}
	measured := engine.DefaultOrkflowMaxTasks
	if explicit && value > measured {
		out.warnings = append(out.warnings, fmt.Sprintf("engine.orkflow_max_tasks (ORK_ORKFLOW_MAX_TASKS=%d) "+
			"is above the largest size measured (%d): this deployment runs unmeasured", value, measured))
	}
	limit := engine.HistoryLimit{MaxEvents: historyMaxEvents, MaxBytes: historyMaxBytes, Reserve: engine.DefaultHistoryReserve}
	budget := engine.DefaultContinuationBudget(cont.events, cont.bytes, cont.restoreTimeout)
	out.flatBound = engine.ShapeMaxTasks(limit, budget, int64(runMaxInFlight))
	if int64(out.value) > out.flatBound {
		out.warnings = append(out.warnings, fmt.Sprintf("engine.orkflow_max_tasks (ORK_ORKFLOW_MAX_TASKS=%d) is "+
			"above %d, the flat-stage bound for engine.history.max_events (%d), engine.continuation.events (%d), "+
			"engine.continuation.bytes (%d), engine.continuation.restore_timeout (%s) and "+
			"engine.run_max_in_flight (%d): a stage of more than %d plain tasks that start together may stop "+
			"RUN_HISTORY_LIMIT in its first history; at or below %d it cannot reach that limit before it "+
			"carries on, though other limits still apply (docs/DEPLOY.md §5)",
			out.value, out.flatBound, historyMaxEvents, cont.events, cont.bytes, cont.restoreTimeout,
			runMaxInFlight, out.flatBound, out.flatBound))
	}
	return out, nil
}
