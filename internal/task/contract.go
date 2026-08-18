// Package task defines the engine<->Orker execution seam: the Temporal activity
// contract by which the engine schedules a task and an Orker executes it via a
// plugin. This is a load-bearing internal contract (like proto): the engine
// (internal/engine) schedules ActivityExecuteTask; the Orker (internal/orker)
// registers and implements it. Change it deliberately and update both sides.
//
// Design note — template rendering lives in the Orker, not the engine:
// task `with` values and conditions may reference upstream artifacts via the
// `artifact "<task>/<name>"` helper, which resolves to a local path only the
// Orker has (after materializing the artifact). To keep the engine workflow
// deterministic (no filesystem/network), the engine passes the RAW `with` block
// plus the upstream context, and the Orker performs all template rendering,
// artifact resolution, plugin execution, and artifact upload.
package task

import "github.com/orkstack/orkstack/pkg/plugin"

const (
	// DefaultQueue is the Temporal task queue Orkers poll for task execution.
	DefaultQueue = "ork-tasks"

	// DefaultNamespace is the Temporal namespace OrkStack runs in when
	// ORK_TEMPORAL_NAMESPACE is unset. Binaries provision it on startup when
	// the cluster does not have it yet (see EnsureNamespace).
	DefaultNamespace = "ork-main"

	// ActivityExecuteTask is the registered activity name for task execution.
	ActivityExecuteTask = "ExecuteTask"

	// ActivityListPlugins is the registered activity name for plugin
	// discovery: the engine asks an Orker which plugins it can run.
	ActivityListPlugins = "ListPlugins"
)

// UpstreamResult is a completed upstream task's contribution to a downstream
// task's template + artifact-resolution context.
type UpstreamResult struct {
	Output    plugin.Payload       `json:"output"`
	Artifacts []plugin.ArtifactRef `json:"artifacts"`
}

// ExecuteTaskInput is the activity input the engine sends to an Orker.
type ExecuteTaskInput struct {
	RunID          string `json:"run_id"`
	OrkflowName    string `json:"orkflow_name"`
	OrkflowVersion string `json:"orkflow_version"`

	TaskID string `json:"task_id"`
	Plugin string `json:"plugin"`

	// With is the RAW (un-rendered) plugin config from the DSL. The Orker
	// renders templates against RunInput + Upstream before invoking the plugin.
	With map[string]any `json:"with"`

	// RunInput is the run's input payload, exposed to templates as `.inputs`.
	RunInput plugin.Payload `json:"run_input"`

	// Upstream maps upstream task id -> its result, exposed to templates as
	// `.tasks.<id>` and used to resolve `artifact "<id>/<name>"`.
	Upstream map[string]UpstreamResult `json:"upstream"`

	// TimeoutSeconds is the task's execution timeout (engine also sets the
	// Temporal activity StartToClose timeout from this).
	TimeoutSeconds int `json:"timeout_seconds"`
}

// ExecuteTaskResult is what the Orker returns to the engine after running a task.
type ExecuteTaskResult struct {
	TaskID    string               `json:"task_id"`
	Output    plugin.Payload       `json:"output"`
	Artifacts []plugin.ArtifactRef `json:"artifacts"`
}
