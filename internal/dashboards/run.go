package dashboards

// The OrkStack / Run dashboard (uid orkstack-run): one run, keyed by $run,
// with $task narrowing the task panels (prod-kit design §2.3.5). It is its
// own dashboard because a collapsed row cannot be opened from a URL, and
// every panel here is one run's.

// runUsage selects the run's usage samples, narrowed by $task.
const runUsage = `{service_name=~".*orker"} | ork_record="usage" | ork_usage_kind="sample" | ork_run_id="$run" | ork_task_id=~"$task"`

// runPanels are the Run view's panels, top to bottom.
func runPanels() []panel {
	return []panel{
		table("Spans of the run", "The run's spans: each attempt, its phases and the engine's work, newest first.", unitNone,
			query{expr: `{ .ork.run_id = "$run" }`, spans: true}).on(tempo).sized(24, 8),
		logs("Engine events", "The engine's event lines for the run.",
			q(`{service_name=~".*ork"} | run_id="$run"`, "")).sized(24, 8),
		logs("Attempt usage", "Each attempt's measured CPU and memory summary, and its samples.",
			q(`{service_name=~".*orker"} | ork_record="usage" | ork_run_id="$run" | ork_task_id=~"$task"`, "")).sized(12, 8),
		logs("Task stderr", "What the run's tasks wrote to stderr.",
			q(`{service_name=~".*orker"} | ork_stream="stderr" | ork_run_id="$run" | ork_task_id=~"$task"`, "")).sized(12, 8),
		logs("Task stdout", "What the run's tasks wrote to stdout.",
			q(`{service_name=~".*orker"} | ork_stream="stdout" | ork_run_id="$run" | ork_task_id=~"$task"`, "")).sized(24, 10),
		series("Memory against the limit", "Each task's memory over time from its attempts' usage samples, with its limit dashed.", unitBytes,
			q(`max by (ork_task_id) (max_over_time(`+runUsage+` | unwrap ork_usage_memory_bytes [$__interval]))`, "{{ork_task_id}}"),
			dash(`max by (ork_task_id) (max_over_time(`+runUsage+` | unwrap ork_usage_memory_limit_bytes [$__interval]))`, "{{ork_task_id}} limit")).on(loki),
		series("CPU in use", "Each task's CPU cores over time from its attempts' usage samples.", unitShort,
			q(`max by (ork_task_id) (max_over_time(`+runUsage+` | unwrap ork_usage_cpu_cores [$__interval]))`, "{{ork_task_id}}")).on(loki),
		logs("Webhook deliveries", "The engine's lines for the run's webhook deliveries that settled: delivered, or failed for good. Never the URL.",
			q(`{service_name=~".*ork"} |~ "webhook delivered|webhook delivery failed" | run_id="$run"`, "")).sized(24, 8),
	}
}
