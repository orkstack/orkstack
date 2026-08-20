// Package plugin defines the OrkStack task contract: the fixed interface every
// task implementation honors. A plugin receives an input payload plus input
// artifacts and produces an output payload plus output artifacts. This contract
// is intentionally transport-agnostic so a builtin Go plugin and a future
// containerized plugin are interchangeable.
//
// This package is public (imported by pkg/plugins/* and internal/orker) and MUST
// remain free of engine/Temporal imports so plugins stay portable.
package plugin

import "context"

// Payload is JSON-compatible structured data passed between tasks. Bytes never
// travel here — use artifacts for anything non-trivial.
type Payload map[string]any

// ArtifactRef points to a stored artifact. It mirrors orkstack.v1.ArtifactRef
// but is dependency-free so plugins need not import generated proto.
type ArtifactRef struct {
	Name        string            `json:"name"`
	URI         string            `json:"uri"`
	ContentType string            `json:"content_type,omitempty"`
	SizeBytes   int64             `json:"size_bytes,omitempty"`
	SHA256      string            `json:"sha256,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// ArtifactResolver materializes an upstream artifact (referenced as
// "<taskId>/<name>") to a local filesystem path within the task's workdir.
type ArtifactResolver interface {
	// Resolve downloads/links the named artifact and returns its local path.
	Resolve(ctx context.Context, ref string) (localPath string, err error)
}

// Logger streams one line of a task's live output.
//
// The line goes to the task's LOG stream — numbered chunks in the artifact
// store, readable while the task runs through RunService.StreamTaskLogs — and
// never onto the run's event stream: task output is bytes, and bytes travel as
// stored objects rather than through Temporal (AGENTS.md §1.4).
//
// It is printf-shaped, so a caller streaming a line it did not compose passes
// it as data: Log("%s", line), never Log(line).
type Logger func(format string, args ...any)

// ExecContext carries everything a plugin needs to run one task.
type ExecContext struct {
	// TaskID is the DSL task id being executed.
	TaskID string
	// Config is the rendered `with:` block from the DSL (templates resolved).
	Config map[string]any
	// Inputs is the task input payload (run input merged with upstream refs).
	Inputs Payload
	// Artifacts resolves upstream artifacts to local paths on demand.
	Artifacts ArtifactResolver
	// WorkDir is a scratch directory unique to this task attempt. Plugins may
	// read/write freely here; it is cleaned up after upload.
	WorkDir string
	// Log streams progress to observers.
	Log Logger
}

// ProducedArtifact is a local file a plugin wants persisted as an output
// artifact. The Orker uploads it via pkg/artifacts and returns an ArtifactRef.
type ProducedArtifact struct {
	// Name is the logical artifact name, unique within the task.
	Name string
	// LocalPath is an absolute or WorkDir-relative path to the file.
	LocalPath string
	// ContentType is an optional MIME hint.
	ContentType string
	// Metadata is optional key/value metadata stored with the artifact.
	Metadata map[string]string
}

// Result is a plugin's output: a payload plus any produced artifacts.
type Result struct {
	Output    Payload
	Artifacts []ProducedArtifact
}

// SchemaDoc is a lightweight JSON-schema-ish description for discovery/UI.
type SchemaDoc map[string]any

// Descriptor is plugin metadata surfaced through PluginService/ListPlugins.
type Descriptor struct {
	Name         string
	Version      string
	Description  string
	InputSchema  SchemaDoc
	OutputSchema SchemaDoc
}

// Plugin is the fixed task contract. Implementations must be safe for Temporal
// retries: prefer idempotent behavior or guard non-repeatable side effects.
type Plugin interface {
	// Name is the DSL `plugin:` identifier (e.g. "http").
	Name() string
	// Describe returns metadata and I/O schemas for discovery.
	Describe() Descriptor
	// Execute performs the task. A non-nil error fails the task (subject to the
	// task's retry policy). Use RetryableError to signal retryability explicitly.
	Execute(ctx context.Context, ec *ExecContext) (*Result, error)
}

// Registry maps plugin names to implementations. It is the Orker's dispatch
// table and the source for PluginService discovery.
type Registry struct {
	plugins map[string]Plugin
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry { return &Registry{plugins: map[string]Plugin{}} }

// Register adds a plugin. It panics on duplicate names because registration
// happens at startup and a duplicate is a programming error.
func (r *Registry) Register(p Plugin) {
	if p == nil {
		panic("plugin: Register(nil)")
	}
	name := p.Name()
	if name == "" {
		panic("plugin: Register with empty name")
	}
	if _, exists := r.plugins[name]; exists {
		panic("plugin: duplicate registration for " + name)
	}
	r.plugins[name] = p
}

// Get returns the plugin for name and whether it was found.
func (r *Registry) Get(name string) (Plugin, bool) {
	p, ok := r.plugins[name]
	return p, ok
}

// List returns descriptors for all registered plugins.
func (r *Registry) List() []Descriptor {
	out := make([]Descriptor, 0, len(r.plugins))
	for _, p := range r.plugins {
		out = append(out, p.Describe())
	}
	return out
}

// RetryableError wraps an error to explicitly mark it retryable (or not).
type RetryableError struct {
	Err       error
	Retryable bool
}

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// Retryable marks err as retryable for the engine's retry policy.
func Retryable(err error) error { return &RetryableError{Err: err, Retryable: true} }

// Permanent marks err as non-retryable (fail fast).
func Permanent(err error) error { return &RetryableError{Err: err, Retryable: false} }
