package dashboards

// The OrkStack / Run dashboard (uid orkstack-run): one run, keyed by $run,
// with $task narrowing the task panels (prod-kit design §2.3.5). It is its
// own dashboard because a collapsed row cannot be opened from a URL, and
// every panel here is one run's.

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
	}
}
