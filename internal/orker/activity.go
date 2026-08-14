package orker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// Error types surfaced to Temporal as non-retryable application errors. The
// engine reports err.Error() verbatim, so messages name the task, plugin and
// reference involved.
const (
	ErrTypeUnknownPlugin = "UNKNOWN_PLUGIN"
	ErrTypeRender        = "RENDER_FAILED"
	ErrTypeWorkDir       = "WORKDIR_FAILED"
	ErrTypeArtifact      = "ARTIFACT_FAILED"
	ErrTypePluginFailed  = "PLUGIN_PERMANENT"
)

// WorkRootVar overrides the parent directory for per-attempt scratch workdirs.
const WorkRootVar = "ORK_ORKER_WORKDIR"

// Options configures an Activities instance. The zero value reads the
// environment for the workdir root and the `env` helper allowlist.
type Options struct {
	// WorkRoot is the parent directory for per-attempt workdirs. Empty means
	// ORK_ORKER_WORKDIR, then the OS temp directory.
	WorkRoot string
	// EnvAllowlist limits which variables the `env` template helper reads.
	// Nil means ORK_ORKER_ENV_ALLOWLIST.
	EnvAllowlist []string
	// LogSink observes plugin log lines in addition to the activity logger.
	LogSink func(in task.ExecuteTaskInput, line string)
}

// Activities implements the engine<->Orker activity seam: it renders task
// config, materializes upstream artifacts, dispatches to a registered plugin,
// and uploads produced artifacts.
type Activities struct {
	registry  *plugin.Registry
	store     artifacts.Store
	workRoot  string
	allowlist map[string]struct{}
	logSink   func(in task.ExecuteTaskInput, line string)
}

// NewActivities builds the activity set with environment-derived defaults.
func NewActivities(registry *plugin.Registry, store artifacts.Store) *Activities {
	return NewActivitiesWithOptions(registry, store, Options{})
}

// NewActivitiesWithOptions builds the activity set with explicit options.
func NewActivitiesWithOptions(registry *plugin.Registry, store artifacts.Store, opts Options) *Activities {
	workRoot := opts.WorkRoot
	if workRoot == "" {
		workRoot = os.Getenv(WorkRootVar)
	}
	allowlist := map[string]struct{}{}
	if opts.EnvAllowlist == nil {
		allowlist = parseAllowlist(os.Getenv(EnvAllowlistVar))
	} else {
		for _, name := range opts.EnvAllowlist {
			allowlist[name] = struct{}{}
		}
	}
	return &Activities{
		registry:  registry,
		store:     store,
		workRoot:  workRoot,
		allowlist: allowlist,
		logSink:   opts.LogSink,
	}
}

// ExecuteTask runs one task: dispatch by plugin name, render `with` against the
// run input and upstream results, execute the plugin in a per-attempt workdir,
// upload produced artifacts, and return the refs.
func (a *Activities) ExecuteTask(ctx context.Context, in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
	var empty task.ExecuteTaskResult

	if a.registry == nil {
		return empty, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("task %q: orker has no plugin registry", in.TaskID),
			ErrTypeUnknownPlugin, nil)
	}
	impl, ok := a.registry.Get(in.Plugin)
	if !ok {
		return empty, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("task %q: unknown plugin %q (registered plugins: %s)",
				in.TaskID, in.Plugin, joinOrNone(a.pluginNames())),
			ErrTypeUnknownPlugin, nil)
	}

	logf := a.logger(ctx, in)
	heartbeat(ctx, "start")

	workDir, err := a.makeWorkDir(ctx, in)
	if err != nil {
		return empty, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeWorkDir, err)
	}
	// The workdir is scratch space for exactly this attempt: remove it on
	// success, plugin failure, panic, and cancellation alike.
	defer func() { _ = os.RemoveAll(workDir) }()
	logf("workdir %s", workDir)

	resolver := newUpstreamResolver(a.store, in.Upstream, workDir)
	config, err := newRenderer(in.TaskID, in.RunInput, in.Upstream, resolver, a.allowlist).
		render(ctx, in.With)
	if err != nil {
		return empty, failTask(err, ErrTypeRender)
	}
	heartbeat(ctx, "rendered")

	ec := &plugin.ExecContext{
		TaskID:    in.TaskID,
		Config:    config,
		Inputs:    in.RunInput,
		Artifacts: resolver,
		WorkDir:   workDir,
		Log:       logf,
	}
	result, err := impl.Execute(ctx, ec)
	if err != nil {
		return empty, pluginFailure(in, err)
	}
	if result == nil {
		result = &plugin.Result{}
	}
	heartbeat(ctx, "executed")

	refs, err := a.upload(ctx, in, workDir, result.Artifacts)
	if err != nil {
		return empty, err
	}
	heartbeat(ctx, "uploaded")

	return task.ExecuteTaskResult{
		TaskID:    in.TaskID,
		Output:    result.Output,
		Artifacts: refs,
	}, nil
}

// upload persists every produced artifact and returns the stored references in
// the order the plugin produced them.
func (a *Activities) upload(
	ctx context.Context,
	in task.ExecuteTaskInput,
	workDir string,
	produced []plugin.ProducedArtifact,
) ([]plugin.ArtifactRef, error) {
	refs := make([]plugin.ArtifactRef, 0, len(produced))
	for _, item := range produced {
		if item.Name == "" {
			err := fmt.Errorf("task %q: plugin %q produced an artifact with an empty name", in.TaskID, in.Plugin)
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeArtifact, err)
		}
		path := item.LocalPath
		if !filepath.IsAbs(path) {
			path = filepath.Join(workDir, path)
		}
		file, err := os.Open(path)
		if err != nil {
			wrapped := fmt.Errorf("task %q: artifact %q: %w", in.TaskID, item.Name, err)
			return nil, temporal.NewNonRetryableApplicationError(wrapped.Error(), ErrTypeArtifact, wrapped)
		}
		if a.store == nil {
			_ = file.Close()
			err := fmt.Errorf("task %q: artifact %q: orker has no artifact store", in.TaskID, item.Name)
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeArtifact, err)
		}
		ref, err := a.store.Put(ctx, item.Name, file, artifacts.PutOptions{
			ContentType: item.ContentType,
			Metadata:    artifactMetadata(in, item),
		})
		_ = file.Close()
		if err != nil {
			// Upload failures are usually transient storage errors; leave them
			// retryable under the task's retry policy.
			return nil, fmt.Errorf("task %q: upload artifact %q: %w", in.TaskID, item.Name, err)
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// artifactMetadata records provenance without overwriting plugin metadata.
func artifactMetadata(in task.ExecuteTaskInput, item plugin.ProducedArtifact) map[string]string {
	meta := make(map[string]string, len(item.Metadata)+4)
	for k, v := range item.Metadata {
		meta[k] = v
	}
	meta["run_id"] = in.RunID
	meta["task_id"] = in.TaskID
	meta["workflow"] = in.WorkflowName
	meta["artifact"] = item.Name
	return meta
}

// makeWorkDir creates the scratch directory for this attempt.
func (a *Activities) makeWorkDir(ctx context.Context, in task.ExecuteTaskInput) (string, error) {
	root := a.workRoot
	if root == "" {
		root = os.TempDir()
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", fmt.Errorf("task %q: create workdir root %s: %w", in.TaskID, root, err)
	}
	pattern := fmt.Sprintf("ork-%s-%s-%d-*",
		safeComponent(in.RunID), safeComponent(in.TaskID), attempt(ctx))
	dir, err := os.MkdirTemp(root, pattern)
	if err != nil {
		return "", fmt.Errorf("task %q: create workdir: %w", in.TaskID, err)
	}
	return dir, nil
}

func (a *Activities) pluginNames() []string {
	descriptors := a.registry.List()
	names := make([]string, 0, len(descriptors))
	for _, d := range descriptors {
		names = append(names, d.Name)
	}
	sort.Strings(names)
	return names
}

// logger streams plugin logs to the activity logger (and the optional sink).
func (a *Activities) logger(ctx context.Context, in task.ExecuteTaskInput) plugin.Logger {
	return func(format string, args ...any) {
		line := fmt.Sprintf(format, args...)
		if activity.IsActivity(ctx) {
			activity.GetLogger(ctx).Info(line,
				"run_id", in.RunID, "task_id", in.TaskID, "plugin", in.Plugin)
		}
		if a.logSink != nil {
			a.logSink(in, line)
		}
	}
}

// heartbeat reports progress. The engine sets no heartbeat timeout, so this is
// observability only and is skipped outside an activity context.
func heartbeat(ctx context.Context, phase string) {
	if activity.IsActivity(ctx) {
		activity.RecordHeartbeat(ctx, phase)
	}
}

func attempt(ctx context.Context) int32 {
	if activity.IsActivity(ctx) {
		return activity.GetInfo(ctx).Attempt
	}
	return 1
}

// failTask marks a deterministic failure non-retryable unless the cause
// explicitly asked to be retried.
func failTask(err error, errType string) error {
	var retryable *plugin.RetryableError
	if errors.As(err, &retryable) && retryable.Retryable {
		return err
	}
	return temporal.NewNonRetryableApplicationError(err.Error(), errType, err)
}

// pluginFailure keeps plugin errors retryable by default and honors an
// explicit plugin.Permanent marker.
func pluginFailure(in task.ExecuteTaskInput, err error) error {
	wrapped := fmt.Errorf("task %q: plugin %q: %w", in.TaskID, in.Plugin, err)
	var retryable *plugin.RetryableError
	if errors.As(err, &retryable) && !retryable.Retryable {
		return temporal.NewNonRetryableApplicationError(wrapped.Error(), ErrTypePluginFailed, wrapped)
	}
	return wrapped
}
