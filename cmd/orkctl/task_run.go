package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// `orkctl task run` — one task of one orkflow, on this machine, against the
// real committed bytes of a real run (or local files), through the Orker's
// own dispatcher.
//
// It never writes to the deployment. It calls four read RPCs — GetRun,
// GetOrkflow, ValidateOrkflow, ReadArtifact — and never contacts Temporal or
// object storage. Its dispatcher has no commit store, no attempt store and no
// log store, and the input it composes carries no commit URI, so the durable
// commit machinery is unreachable by construction. A local run is a NEW
// logical activity with a fresh local-… run id and idempotency key.

// taskRunOptions are the command's flags, as typed.
type taskRunOptions struct {
	task        string
	fromRun     string
	file        string
	inputs      []string
	inputJSON   string
	outputs     []string
	outputsFile string
	artifacts   []string
	secrets     []string
	omitSecrets []string
	// needs is --need TASK=STATUS: a failure handler's need outcome, as
	// typed (docs/DSL.md "Joins and failure handlers").
	needs      []string
	shared     string
	renderOnly bool
	keep       bool
	// item is --item: one item of a fan-out task (required for one).
	item itemFlag
	// itemSelected is item, resolved when the command runs.
	itemSelected *int32
}

// suppliedArtifact is one --artifact TASK/NAME=PATH.
type suppliedArtifact struct {
	flag     string
	producer string
	name     string
	path     string
}

// parsedTaskRun is what the flag checks produce.
type parsedTaskRun struct {
	assumed     map[string]*orkstackv1.Payload
	artifacts   []suppliedArtifact
	secrets     map[string]string // name -> source, a file: path made absolute
	secretFlags map[string]string // name -> the flag value as typed
	omitted     map[string]bool
	shared      string
	policy      orker.SecurityPolicy
	// needs is every --need, in flag order; needStatus the same by task.
	needs      []neededOutcome
	needStatus map[string]orkstackv1.Status
}

// neededOutcome is one --need TASK=STATUS.
type neededOutcome struct {
	flag   string
	taskID string
	status orkstackv1.Status
}

// needStatusWords are --need's four words.
var needStatusWords = map[string]orkstackv1.Status{
	"succeeded": orkstackv1.Status_STATUS_SUCCEEDED,
	"failed":    orkstackv1.Status_STATUS_FAILED,
	"skipped":   orkstackv1.Status_STATUS_SKIPPED,
	"canceled":  orkstackv1.Status_STATUS_CANCELED,
}

func newTaskRunCommand(g *globalFlags) *cobra.Command {
	opts := &taskRunOptions{}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run one task locally, against a run's recorded bytes or local files",
		Long: "run executes ONE task of an orkflow on this machine, through the same dispatcher,\n" +
			"renderer and workspace an Orker uses, and prints the rendered config.json before\n" +
			"the container starts.\n\n" +
			"With --from-run RUN, the task's upstream values and artifacts are the run's\n" +
			"committed bytes, read through the engine's integrity-checked artifact reads. With\n" +
			"--file, an edited, unapplied document is validated by the engine and run instead\n" +
			"of the stored definition. --output and --artifact supply an upstream task's\n" +
			"values.json and output files from local files.\n\n" +
			"A failure handler (join: any_failed or all_done) is told how its needs ended:\n" +
			"with --from-run, as the run recorded it; --need TASK=STATUS overrides one direct\n" +
			"need (\"what would this handler do had smoke failed?\"), and without --from-run\n" +
			"every direct need must be named. A failed need's files come from the run or\n" +
			"from --artifact TASK/NAME=PATH.\n\n" +
			"Nothing is written to the deployment: no run, no commit, no attempt record, no\n" +
			"log. A local run is a new logical activity with its own run id and idempotency\n" +
			"key, and any external side effect it performs is real. Every secret the task\n" +
			"declares must be supplied with --secret or skipped with --omit-secret.\n\n" +
			"Each run labels its container ork.orker=orkctl-task-run-<hex>, from its own local\n" +
			"run id, and holds a lock file for that identity in its work directory\n" +
			"(ORK_ORKER_WORKDIR, else ~/.orkstack/task-run, else the temporary directory) while\n" +
			"it lives. A run that removed its container deletes the file. A second interrupt\n" +
			"abandons the container instead; the next task run on the same host, work\n" +
			"directory and container engine removes it, and any container a killed run left,\n" +
			"but never one of a run that is still alive. --render-only removes none.\n\n" +
			"Exit status: 0 succeeded (or rendered), 2 the task failed, 3 interrupted, 1 the\n" +
			"command could not run the task.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireLocalRunHost(g.hostOS(), "task run"); err != nil {
				return err
			}
			selected, err := opts.item.resolve(cmd, opts.task)
			if err != nil {
				return err
			}
			opts.itemSelected = selected
			return runTaskRun(cmd, g, opts)
		},
	}
	opts.item.register(cmd, "run this item of a fan-out task (required for one)")
	flags := cmd.Flags()
	flags.StringVar(&opts.task, "task", "", "the task to run (required)")
	flags.StringVar(&opts.fromRun, "from-run", "", "replay against this run's recorded upstream bytes")
	flags.StringVarP(&opts.file, "file", "f", "", "run this unapplied orkflow document instead of the stored definition")
	flags.StringArrayVar(&opts.inputs, "input", nil,
		"run input as key=value (repeatable); with --from-run, overrides the run's input")
	flags.StringVar(&opts.inputJSON, "input-json", "", "run input as a JSON object; merged under --input values")
	flags.StringArrayVar(&opts.outputs, "output", nil,
		"supply an upstream task's values.json as TASK=JSON-object (repeatable); wins over --outputs-file")
	flags.StringVar(&opts.outputsFile, "outputs-file", "",
		"a JSON object keyed by task id whose values are the supplied values.json objects")
	flags.StringArrayVar(&opts.artifacts, "artifact", nil,
		"supply one output artifact of an upstream task as TASK/NAME=PATH (repeatable)")
	flags.StringArrayVar(&opts.secrets, "secret", nil,
		"supply a declared secret as NAME=file:PATH or NAME=env:VAR (repeatable)")
	flags.StringArrayVar(&opts.omitSecrets, "omit-secret", nil, "run without this declared secret (repeatable)")
	flags.StringArrayVar(&opts.needs, "need", nil,
		"a failure handler's direct need and how it ended, TASK=succeeded|failed|skipped|canceled (repeatable); "+
			"with --from-run it overrides the recorded outcome, without it every direct need must be named")
	flags.StringVar(&opts.shared, "shared", "", "seed a pinned task's /orkstack/shared from this directory")
	flags.BoolVar(&opts.renderOnly, "render-only", false, "stop once the input tree is sealed; start no container")
	flags.BoolVar(&opts.keep, "keep", false, "keep the work directory and print its path")
	return cmd
}

// ---------------------------------------------------------------- flags

// parseTaskRunFlags applies the flag checks, in order, before any RPC.
func parseTaskRunFlags(opts *taskRunOptions) (*parsedTaskRun, error) {
	if opts.task == "" {
		return nil, errors.New("--task is required")
	}
	if err := dsl.ValidSlug("task", opts.task); err != nil {
		return nil, fmt.Errorf("--task: %w", err)
	}
	if opts.fromRun == "" && opts.file == "" {
		return nil, errors.New("one of --from-run or --file is required")
	}
	if opts.renderOnly {
		for _, used := range []struct {
			name string
			set  bool
		}{
			{"--secret", len(opts.secrets) > 0},
			{"--omit-secret", len(opts.omitSecrets) > 0},
			{"--shared", opts.shared != ""},
		} {
			if used.set {
				return nil, fmt.Errorf("%s has no effect with --render-only: no container starts, so nothing is staged", used.name)
			}
		}
	}
	// The input flags are parsed here too, so a malformed --input-json is
	// refused before anything is sent; the run sequence parses them again
	// against the source run's input.
	if _, err := buildInputPayload(opts.inputs, opts.inputJSON); err != nil {
		return nil, err
	}
	assumed, err := buildAssumedOutputs(opts.outputs, opts.outputsFile)
	if err != nil {
		return nil, err
	}
	parsed := &parsedTaskRun{
		assumed:     assumed,
		secrets:     map[string]string{},
		secretFlags: map[string]string{},
		omitted:     map[string]bool{},
		policy:      orker.SecurityPolicyFromEnv(),
	}
	for _, value := range opts.artifacts {
		supplied, err := parseTaskArtifactFlag(value)
		if err != nil {
			return nil, err
		}
		parsed.artifacts = append(parsed.artifacts, supplied)
	}
	parsed.needStatus = map[string]orkstackv1.Status{}
	for _, value := range opts.needs {
		id, word, found := strings.Cut(value, "=")
		status, known := needStatusWords[word]
		if !found || id == "" || !known {
			return nil, fmt.Errorf("invalid --need %q: want TASK=succeeded|failed|skipped|canceled", value)
		}
		if _, twice := parsed.needStatus[id]; twice {
			return nil, fmt.Errorf("--need %s is given twice", id)
		}
		parsed.needStatus[id] = status
		parsed.needs = append(parsed.needs, neededOutcome{flag: value, taskID: id, status: status})
	}
	for _, value := range opts.secrets {
		name, source, err := parseTaskSecretFlag(value, parsed.policy)
		if err != nil {
			return nil, err
		}
		parsed.secrets[name] = source
		parsed.secretFlags[name] = value
	}
	if opts.shared != "" {
		shared, err := checkSharedSeed(opts.shared)
		if err != nil {
			return nil, err
		}
		parsed.shared = shared
	}
	for _, name := range opts.omitSecrets {
		if _, both := parsed.secrets[name]; both {
			return nil, fmt.Errorf("secret %q is both supplied and omitted", name)
		}
		parsed.omitted[name] = true
	}
	if err := parsed.policy.Validate(); err != nil {
		return nil, err
	}
	return parsed, nil
}

// parseTaskArtifactFlag parses one --artifact TASK/NAME=PATH. The value is
// split at the first "=", then the left side at the first "/". NAME must be a
// name the Orker's output scan can produce: non-empty, valid UTF-8, no "/" and
// no NUL, and not "." or "..". PATH must be a regular file (Lstat: a symlink
// is not one).
func parseTaskArtifactFlag(value string) (suppliedArtifact, error) {
	left, path, found := strings.Cut(value, "=")
	if !found || path == "" {
		return suppliedArtifact{}, fmt.Errorf("invalid --artifact %q: want TASK/NAME=PATH", value)
	}
	producer, name, found := strings.Cut(left, "/")
	if !found || producer == "" {
		return suppliedArtifact{}, fmt.Errorf("invalid --artifact %q: want TASK/NAME=PATH", value)
	}
	if name == "" || !utf8.ValidString(name) || strings.ContainsAny(name, "/\x00") || name == "." || name == ".." {
		return suppliedArtifact{}, fmt.Errorf("invalid --artifact %q: NAME %q is not a file name an Orker can produce", value, name)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return suppliedArtifact{}, fmt.Errorf("invalid --artifact %q: %s is not a regular file", value, path)
	}
	return suppliedArtifact{flag: value, producer: producer, name: name, path: path}, nil
}

// parseTaskSecretFlag parses one --secret NAME=SOURCE and checks the source
// resolves on this machine. It never reads the value: the dispatcher does.
func parseTaskSecretFlag(value string, policy orker.SecurityPolicy) (string, string, error) {
	malformed := fmt.Errorf("invalid --secret %q: want NAME=file:PATH or NAME=env:VAR", value)
	name, source, found := strings.Cut(value, "=")
	if !found || name == "" || !orker.ValidSecretName(name) {
		return "", "", malformed
	}
	scheme, locator, found := strings.Cut(source, ":")
	if !found || scheme == "" || locator == "" {
		return "", "", malformed
	}
	if !policy.AllowsSecretScheme(scheme) {
		return "", "", fmt.Errorf("--secret %s: scheme %q is not one this machine resolves (allowed: %s)",
			name, scheme, strings.Join(policy.SecretSchemes, ", "))
	}
	switch scheme {
	case "file":
		abs, err := filepath.Abs(locator)
		if err != nil {
			return "", "", fmt.Errorf("--secret %s: %s: %v", name, locator, err)
		}
		info, err := os.Stat(abs)
		if err != nil {
			return "", "", fmt.Errorf("--secret %s: %s: no such file", name, locator)
		}
		if !info.Mode().IsRegular() {
			return "", "", fmt.Errorf("--secret %s: %s: not a regular file", name, locator)
		}
		return name, "file:" + abs, nil
	case "env":
		if _, ok := os.LookupEnv(locator); !ok {
			return "", "", fmt.Errorf("--secret %s: environment variable %s is not set", name, locator)
		}
		return name, source, nil
	default:
		return "", "", malformed
	}
}

// checkSharedSeed holds --shared to a directory of regular files and
// directories only (AGENTS.md §3.1's refusal set), and returns it absolute.
func checkSharedSeed(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("--shared: %s: %v", dir, err)
	}
	info, err := os.Lstat(abs)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("--shared: %s is not a directory", dir)
	}
	err = filepath.WalkDir(abs, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Type().IsRegular() {
			return nil
		}
		return fmt.Errorf("--shared: %s is %s; only regular files and directories are copied",
			path, describeFileType(entry.Type()))
	})
	if err != nil {
		return "", err
	}
	return abs, nil
}

// localSecrets turns the definition's SecretRefs into the local input's.
// Under --render-only none is staged: no container starts, so no secret byte
// is needed. Otherwise, in declaration order, a supplied name keeps its name
// and takes the supplied source, and an omitted one is dropped.
func localSecrets(
	declared []*orkstackv1.SecretRef,
	supplied map[string]string,
	omitted map[string]bool,
	renderOnly bool,
) []*orkstackv1.SecretRef {
	if renderOnly {
		return nil
	}
	var out []*orkstackv1.SecretRef
	for _, secret := range declared {
		name := secret.GetName()
		if omitted[name] {
			continue
		}
		if source, ok := supplied[name]; ok {
			out = append(out, &orkstackv1.SecretRef{Name: name, Source: source})
		}
	}
	return out
}

// taskRunExitCode is the exit-status mapping (§10.4), by outcome and category and nothing
// else: an InfrastructureError is this machine's problem, not the task's
// verdict, so it is exit 1 like any command that could not do its job.
func taskRunExitCode(outcome, category string) int {
	switch outcome {
	case taskRunSucceeded, taskRunRendered:
		return exitOK
	case taskRunCanceled:
		return exitRunCanceled
	}
	if category == orker.CategoryInfrastructureError {
		return exitError
	}
	return exitRunFailed
}

// ---------------------------------------------------------------- runner

// taskRunner carries one invocation through the local-run sequence (§10.4).
type taskRunner struct {
	g      *globalFlags
	opts   *taskRunOptions
	parsed *parsedTaskRun
	ctx    context.Context
	t      *transcript
	stdout io.Writer
	rt     orker.ContainerRuntime
	start  time.Time

	run      *orkstackv1.Run
	wf       *orkstackv1.Orkflow
	warnings []*orkstackv1.ValidationIssue
	spec     *orkstackv1.Task
	closure  []string
	input    *orkstackv1.Payload // effective, defaults applied
	localID  string
	in       *orkstackv1.ExecuteTaskInput

	// sources is each closure member's upstream source, in closure order.
	sources []*upstreamSource

	session  string
	store    *replayStore
	observed *observedRuntime

	report *taskRunReport

	// interruptMu guards finished, and the writes of spec and localID that
	// the signal goroutine's interruptNotice reads.
	interruptMu sync.Mutex
	finished    bool
}

// upstreamSource is one closure member and where its bytes come from.
type upstreamSource struct {
	taskID   string
	taskRun  *orkstackv1.TaskRun // nil when the run has no such task (or no run)
	from     string              // "run" | "supplied" | "none"
	values   *orkstackv1.ArtifactRef
	outputs  []*orkstackv1.ArtifactRef
	supplied *suppliedUpstream
}

// suppliedUpstream is a producer taken entirely from local flags.
type suppliedUpstream struct {
	values    []byte
	artifacts []suppliedArtifact
	hashes    []string
	sizes     []int64
}

func runTaskRun(cmd *cobra.Command, g *globalFlags, opts *taskRunOptions) error {
	parsed, err := parseTaskRunFlags(opts)
	if err != nil {
		return err
	}
	out := cmd.OutOrStdout()
	if g.json {
		out = cmd.ErrOrStderr()
	}
	rt := g.containerRuntime
	if rt == nil {
		rt = orker.NewDockerRuntime()
	}

	// The first SIGINT or SIGTERM cancels the context: the dispatcher runs
	// the cancellation sequence — SIGTERM, the grace period, SIGKILL, remove — and the
	// command exits 3. The notifier is stopped on that first signal, so a
	// second one terminates orkctl at once.
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r := &taskRunner{
		g: g, opts: opts, parsed: parsed, ctx: ctx, rt: rt,
		t: &transcript{out: out}, stdout: cmd.OutOrStdout(), start: time.Now(),
	}
	done := make(chan struct{})
	defer func() {
		r.interruptMu.Lock()
		r.finished = true
		r.interruptMu.Unlock()
		close(done)
	}()
	go func() {
		select {
		case <-ctx.Done():
			stop()
			r.interruptMu.Lock()
			defer r.interruptMu.Unlock()
			if !r.finished {
				r.t.println(r.interruptNotice())
			}
		case <-done:
		}
	}()
	return r.execute()
}

// interruptNotice must be called with interruptMu held.
func (r *taskRunner) interruptNotice() string {
	grace := int32(30)
	if r.spec != nil {
		grace = r.spec.GetCancellationGracePeriodSeconds()
		if grace <= 0 {
			grace = 30
		}
	}
	id := r.localID
	if id == "" {
		id = "local-…"
	}
	return fmt.Sprintf("interrupted: stopping the container (SIGTERM, then SIGKILL after %ds); "+
		"interrupt again to abandon it — it stays labelled ork.run=%s until the next task run in this work directory removes it", grace, id)
}

// rpcContext bounds one unary request with --timeout, under the signal
// context.
func (r *taskRunner) rpcContext() (context.Context, context.CancelFunc) {
	if r.g.timeout <= 0 {
		return context.WithCancel(r.ctx)
	}
	return context.WithTimeout(r.ctx, r.g.timeout)
}

// streamContext bounds a download only when --timeout was given explicitly,
// as every other streaming read of this CLI does.
func (r *taskRunner) streamContext() (context.Context, context.CancelFunc) {
	if r.g.timeoutChanged && r.g.timeout > 0 {
		return context.WithTimeout(r.ctx, r.g.timeout)
	}
	return context.WithCancel(r.ctx)
}

func (r *taskRunner) namespace() string { return artifacts.ResolveNamespace(r.g.namespace) }

func (r *taskRunner) execute() error {
	// STEP 2. Runtime preflight, before any download, so a missing daemon
	// never costs a multi-gigabyte fetch.
	if _, err := r.rt.Describe(r.ctx); err != nil {
		return fmt.Errorf("no container runtime answered (%v); orkctl task run executes the task on this machine", err)
	}
	// STEP 3. The source run.
	if r.opts.fromRun != "" {
		ctx, cancel := r.rpcContext()
		response, err := r.g.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{
			RunId: r.opts.fromRun, Namespace: r.g.namespace,
		}))
		cancel()
		if err != nil {
			return rpcError("get source run", err)
		}
		r.run = response.Msg.GetRun()
	}
	// STEP 4. The definition.
	if err := r.definition(); err != nil {
		return err
	}
	// STEP 5. The task and its closure.
	for _, tk := range r.wf.GetTasks() {
		if tk.GetId() == r.opts.task {
			// The signal goroutine reads spec for its notice.
			r.interruptMu.Lock()
			r.spec = tk
			r.interruptMu.Unlock()
		}
	}
	if r.spec == nil {
		return fmt.Errorf("task %q is not in orkflow %s@%s", r.opts.task, r.wf.GetName(), r.wf.GetVersion())
	}
	// A composed task starts a child run, not a container: refused by name
	// before anything is fetched or composed.
	if ref := r.spec.GetOrkflow(); ref != nil {
		return engine.ComposedTaskRefusal(r.opts.task, ref)
	}
	if err := r.checkNeeds(); err != nil {
		return err
	}
	closure, err := engine.TaskClosure(r.wf, r.opts.task)
	if err != nil {
		return err
	}
	r.closure = closure
	// STEP 6. Accounting.
	if err := r.account(); err != nil {
		return err
	}
	// STEP 7. Compose.
	return r.compose()
}

// checkNeeds holds --need to its rules: only for a failure handler, only a
// direct need, and — without --from-run — every direct need exactly once.
func (r *taskRunner) checkNeeds() error {
	handler := dsl.IsFailureHandler(r.spec)
	if !handler {
		if len(r.parsed.needs) > 0 {
			return fmt.Errorf("--need applies to a failure handler only; task %q (join %s) is not one",
				r.opts.task, dsl.JoinWord(r.spec.GetJoin()))
		}
		return nil
	}
	direct := map[string]bool{}
	for _, need := range r.spec.GetNeeds() {
		direct[need] = true
	}
	for _, n := range r.parsed.needs {
		if !direct[n.taskID] {
			return fmt.Errorf("--need %s: %q is not a direct need of %q (needs: %s)",
				n.flag, n.taskID, r.opts.task, strings.Join(r.spec.GetNeeds(), ", "))
		}
	}
	if r.run == nil {
		var missing []string
		for _, need := range r.spec.GetNeeds() {
			if _, ok := r.parsed.needStatus[need]; !ok {
				missing = append(missing, need)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("task %q is a failure handler (join %s): without --from-run, name how each need ended "+
				"with --need TASK=STATUS; missing: %s", r.opts.task, dsl.JoinWord(r.spec.GetJoin()), strings.Join(missing, ", "))
		}
	}
	return nil
}

// joinContext is the failure handler's JoinContext: the run's record with
// --need overriding it, or the --need flags alone. Nil for any other task.
func (r *taskRunner) joinContext() *orkstackv1.JoinContext {
	if !dsl.IsFailureHandler(r.spec) {
		return nil
	}
	join := &orkstackv1.JoinContext{Policy: r.spec.GetJoin()}
	if r.run != nil {
		join = engine.JoinContextFromRun(r.run, r.wf, r.opts.task)
	} else {
		for _, need := range r.spec.GetNeeds() {
			join.Needs = append(join.Needs, &orkstackv1.NeedOutcome{TaskId: need})
		}
	}
	for _, outcome := range join.GetNeeds() {
		status, overridden := r.parsed.needStatus[outcome.GetTaskId()]
		if !overridden || (r.run != nil && status == outcome.GetStatus()) {
			continue
		}
		outcome.Status = status
		outcome.Error = nil
		if status == orkstackv1.Status_STATUS_FAILED {
			outcome.Error = &orkstackv1.Error{Code: "TASK_FAILED", Message: "supplied with --need"}
		}
	}
	return join
}

// definition resolves the task's definition.
func (r *taskRunner) definition() error {
	if r.opts.file != "" {
		source, err := os.ReadFile(r.opts.file)
		if err != nil {
			return fmt.Errorf("read orkflow file: %w", err)
		}
		ctx, cancel := r.rpcContext()
		response, err := r.g.orkflowClient().ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{
			Yaml: string(source), Namespace: r.g.namespace,
		}))
		cancel()
		if err != nil {
			return rpcError("validate orkflow", err)
		}
		failed := false
		for _, issue := range response.Msg.GetIssues() {
			if issue.GetSeverity() == orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR {
				failed = true
			} else {
				r.warnings = append(r.warnings, issue)
			}
		}
		if failed || response.Msg.GetOrkflow() == nil {
			renderValidationIssues(r.t.out, response.Msg.GetIssues())
			return errors.New("the document does not validate; nothing was run")
		}
		r.wf = response.Msg.GetOrkflow()
		if r.run != nil && r.wf.GetName() != r.run.GetOrkflowName() {
			return fmt.Errorf("--file defines orkflow %q but run %s ran %q; --from-run replays one orkflow's bytes into the same orkflow",
				r.wf.GetName(), r.run.GetId(), r.run.GetOrkflowName())
		}
		return nil
	}
	ctx, cancel := r.rpcContext()
	response, err := r.g.orkflowClient().GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{
		Name: r.run.GetOrkflowName(), Version: r.run.GetOrkflowVersion(), Namespace: r.g.namespace,
	}))
	cancel()
	if err != nil {
		if connect.CodeOf(err) == connect.CodeNotFound {
			return fmt.Errorf("the run's definition %s@%s is no longer stored; pass --file with its document",
				r.run.GetOrkflowName(), r.run.GetOrkflowVersion())
		}
		return rpcError("get the run's definition", err)
	}
	r.wf = response.Msg.GetOrkflow()
	if pinned := r.run.GetOrkflowDefinitionDigest(); pinned != "" && pinned != r.wf.GetDefinitionDigest() {
		return fmt.Errorf("run %s pinned %s but the stored %s@%s is %s; nothing was run",
			r.run.GetId(), pinned, r.wf.GetName(), r.wf.GetVersion(), r.wf.GetDefinitionDigest())
	}
	return nil
}

// taskRunOf is the source run's record of one task, nil when there is none.
func (r *taskRunner) taskRunOf(id string) *orkstackv1.TaskRun {
	for _, tr := range r.run.GetTaskRuns() {
		if tr.GetTaskId() == id {
			return tr
		}
	}
	return nil
}

// account checks every input is accounted for: each check names every
// offender, not the first.
func (r *taskRunner) account() error {
	var problems []string
	inClosure := map[string]bool{}
	for _, id := range r.closure {
		inClosure[id] = true
	}
	upstreamList := "none"
	if len(r.closure) > 0 {
		upstreamList = strings.Join(r.closure, ", ")
	}
	supplied := map[string]bool{}
	outputIDs := make([]string, 0, len(r.parsed.assumed))
	for id := range r.parsed.assumed {
		outputIDs = append(outputIDs, id)
	}
	sort.Strings(outputIDs)
	for _, id := range outputIDs {
		supplied[id] = true
		if !inClosure[id] {
			problems = append(problems, fmt.Sprintf("--output %s: %q is not upstream of %q (upstream: %s)",
				id, id, r.opts.task, upstreamList))
		}
	}
	for _, a := range r.parsed.artifacts {
		supplied[a.producer] = true
		if !inClosure[a.producer] {
			problems = append(problems, fmt.Sprintf("--artifact %s/%s: %q is not upstream of %q (upstream: %s)",
				a.producer, a.name, a.producer, r.opts.task, upstreamList))
		}
	}
	declared := map[string]bool{}
	var declaredNames []string
	for _, secret := range r.spec.GetSecrets() {
		declared[secret.GetName()] = true
		declaredNames = append(declaredNames, secret.GetName())
	}
	declaredList := "none"
	if len(declaredNames) > 0 {
		declaredList = strings.Join(declaredNames, ", ")
	}
	check := func(flag string, names []string) {
		sort.Strings(names)
		for _, name := range names {
			if !declared[name] {
				problems = append(problems, fmt.Sprintf("%s %s: task %q declares no secret named %q (declared: %s)",
					flag, name, r.opts.task, name, declaredList))
			}
		}
	}
	check("--secret", keysOfString(r.parsed.secrets))
	check("--omit-secret", keysOf(r.parsed.omitted))
	if !r.opts.renderOnly {
		for _, name := range declaredNames {
			if _, ok := r.parsed.secrets[name]; ok || r.parsed.omitted[name] {
				continue
			}
			problems = append(problems, fmt.Sprintf(
				"task %q declares secret %q; supply it with --secret %s=file:PATH or --secret %s=env:VAR, "+
					"or run without it with --omit-secret %s", r.opts.task, name, name, name, name))
		}
	}
	if r.parsed.shared != "" && r.spec.GetPin() == "" {
		problems = append(problems, fmt.Sprintf("--shared seeds a pin's /orkstack/shared, and task %q is not pinned", r.opts.task))
	}
	if r.run != nil {
		for _, id := range r.closure {
			tr := r.taskRunOf(id)
			if tr == nil || supplied[id] {
				continue
			}
			switch tr.GetStatus() {
			case orkstackv1.Status_STATUS_PENDING, orkstackv1.Status_STATUS_RUNNING:
				problems = append(problems, fmt.Sprintf(
					"upstream task %q is %s in run %s; its output does not exist yet — wait for it, or supply it with --output/--artifact",
					id, statusLabel(tr.GetStatus()), r.run.GetId()))
			}
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "\n"))
	}
	return nil
}

func keysOfString(in map[string]string) []string {
	out := make([]string, 0, len(in))
	for k := range in {
		out = append(out, k)
	}
	return out
}

// newLocalRunID mints `local-` and 16 lowercase hex digits from 8 bytes of
// crypto/rand: a NEW logical activity's identity, never the source run's.
func newLocalRunID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "local-" + hex.EncodeToString(b[:])
}

// newPinInstanceID mints a fresh lowercase uuid (v4), the 36-character shape
// the Orker validates a pin instance id against.
func newPinInstanceID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// compose builds the task's input, and everything after it.
func (r *taskRunner) compose() error {
	var input *orkstackv1.Payload
	var err error
	if r.run != nil {
		input, err = againInput(r.run.GetInput(), r.opts.inputs, r.opts.inputJSON)
	} else {
		input, err = buildInputPayload(r.opts.inputs, r.opts.inputJSON)
	}
	if err != nil {
		return err
	}
	r.input = engine.ApplyInputDefaults(r.wf, input)

	// Each closure member's one source.
	suppliedBy := map[string]*suppliedUpstream{}
	for id, values := range r.parsed.assumed {
		raw, err := json.Marshal(values.GetData().AsMap())
		if err != nil {
			return fmt.Errorf("--output %s: %w", id, err)
		}
		suppliedBy[id] = &suppliedUpstream{values: raw}
	}
	for _, a := range r.parsed.artifacts {
		s := suppliedBy[a.producer]
		if s == nil {
			s = &suppliedUpstream{values: []byte("{}")}
			suppliedBy[a.producer] = s
		}
		sum, size, err := orker.HashFile(a.path)
		if err != nil {
			return fmt.Errorf("invalid --artifact %q: %v", a.flag, err)
		}
		s.artifacts = append(s.artifacts, a)
		s.hashes = append(s.hashes, sum)
		s.sizes = append(s.sizes, size)
	}
	fromRun := engine.UpstreamFromRun(r.run)
	join := r.joinContext()
	if join != nil {
		// A failure handler is handed its direct FAILED needs' artifacts,
		// as the interpreter hands them (docs/DSL.md "Joins and failure handlers").
		for id, ref := range engine.FailedNeedUpstreamFromRun(r.run) {
			for _, need := range r.spec.GetNeeds() {
				if need == id {
					fromRun[id] = ref
				}
			}
		}
	}
	upstream := map[string]*orkstackv1.UpstreamRef{}
	for _, id := range r.closure {
		source := &upstreamSource{taskID: id, taskRun: r.taskRunOf(id), from: "none"}
		switch {
		case suppliedBy[id] != nil:
			source.from = "supplied"
			source.supplied = suppliedBy[id]
		case fromRun[id] != nil:
			source.from = "run"
			source.values = fromRun[id].GetValues()
			source.outputs = fromRun[id].GetArtifacts()
			upstream[id] = fromRun[id]
		}
		r.sources = append(r.sources, source)
	}

	localID := newLocalRunID()
	r.interruptMu.Lock() // the signal goroutine reads localID for its notice
	r.localID = localID
	r.interruptMu.Unlock()
	itemCount, err := r.itemCount()
	if err != nil {
		return err
	}
	in, err := engine.LocalTaskInput(engine.LocalTask{
		Orkflow: r.wf, TaskID: r.opts.task, RunID: r.localID, Input: input, Upstream: upstream,
		Item: r.opts.itemSelected, ItemCount: itemCount, Join: join,
	})
	r.report = r.newReport()
	var refusal *engine.LocalTaskRefusal
	if errors.As(err, &refusal) {
		r.printHeader(nil)
		return r.finish(taskRunFailed, refusal.Category, &taskRunError{
			Code: refusal.Code, Category: shortCategory(refusal.Category),
			Message: strings.TrimPrefix(refusal.Error(), refusal.Code+": "),
		}, nil)
	}
	if err != nil {
		return err
	}
	in.Secrets = localSecrets(r.spec.GetSecrets(), r.parsed.secrets, r.parsed.omitted, r.opts.renderOnly)
	if pin := r.spec.GetPin(); pin != "" {
		in.Pin = &orkstackv1.PinContext{
			Name:           pin,
			InstanceId:     newPinInstanceID(),
			TimeoutSeconds: r.pinTimeout(pin, in),
		}
	}
	r.in = in
	r.report.IdempotencyKey = in.GetIdempotencyKey()
	return r.dispatch()
}

// itemCount is a fan-out task's item count for --item: the run's own count
// with --from-run, or, with --file and no run, the count of the stubbed
// source (--output SRC=JSON). Nil leaves an input source to LocalTaskInput,
// which counts it from the input exactly as the interpreter does.
func (r *taskRunner) itemCount() (*int32, error) {
	spec := r.spec.GetEach()
	if spec == nil || r.opts.itemSelected == nil {
		return nil, nil
	}
	if r.run != nil {
		if node := fanOutRow(r.run, r.opts.task); node.GetEach().GetExpanded() {
			count := node.GetEach().GetCount()
			return &count, nil
		}
		return nil, fmt.Errorf("task %q of run %s has not expanded yet; there are no items to run", r.opts.task, r.run.GetId())
	}
	source := spec.GetSource().GetTaskId()
	if source == "" {
		return nil, nil
	}
	stub, ok := r.parsed.assumed[source]
	if !ok {
		return nil, nil
	}
	n, eerr := engine.EachArrayLength(stub.GetData().AsMap(), spec.GetSource().GetPath(), spec.GetOver())
	if eerr != nil {
		return nil, fmt.Errorf("--output %s: %s", source, eerr.Message)
	}
	return &n, nil
}

// pinTimeout is the Orkflow's Pin.timeout_seconds, or the task's own timeout
// plus grace when the pin derives its lifetime.
func (r *taskRunner) pinTimeout(name string, in *orkstackv1.ExecuteTaskInput) int32 {
	for _, pin := range r.wf.GetPins() {
		if pin.GetName() == name && pin.GetTimeoutSeconds() > 0 {
			return pin.GetTimeoutSeconds()
		}
	}
	return in.GetTimeoutSeconds() + in.GetCancellationGracePeriodSeconds()
}

// dispatch runs the task through the dispatcher and reports it.
func (r *taskRunner) dispatch() (err error) {
	// STEP 8. The work directory, the store, the dispatcher.
	root := localToolRoot("task-run")
	if root == "" {
		root = os.TempDir()
	}
	// This run's identity, and the containers of runs that are gone
	// (task_run_owner.go). The claim is released last, after the dispatcher
	// removed this run's own container.
	fingerprint := taskRunFingerprint(r.rt)
	owner, err := claimTaskRunOwner(root, r.localID, fingerprint)
	if err != nil {
		return err
	}
	defer func() { owner.release(r.observed.settled()) }()
	switch {
	case owner.unlocked != nil:
		r.t.printf("warning: this run's identity cannot be locked (%v); if the run is killed, "+
			"no later run removes its container\n", owner.unlocked)
	case !r.opts.renderOnly:
		r.reapGone(root, fingerprint, owner)
	}
	r.session = filepath.Join(root, "ork-task-run-"+strings.TrimPrefix(r.localID, "local-"))
	for _, dir := range []string{r.session, filepath.Join(r.session, "logs"), filepath.Join(r.session, ".fetched"), filepath.Join(r.session, ".work")} {
		if mkErr := os.MkdirAll(dir, 0o700); mkErr != nil {
			return fmt.Errorf("create the work directory: %w", mkErr)
		}
	}
	defer r.cleanup()
	if stream, openErr := os.Create(filepath.Join(r.session, "logs", "stream.log")); openErr == nil {
		r.t.stream = stream
		defer func() { _ = stream.Close() }()
	}
	store, err := newReplayStore(filepath.Join(r.session, ".store"))
	if err != nil {
		return fmt.Errorf("create the work store: %w", err)
	}
	r.store = store
	defer func() { _ = store.Close() }()

	r.observed = &observedRuntime{
		ContainerRuntime: r.rt,
		session:          r.session,
		keep:             r.opts.keep,
		renderOnly:       r.opts.renderOnly,
		onSealed:         r.onSealed,
	}
	policy := r.parsed.policy
	workRoot := filepath.Join(r.session, ".work")
	acts := orker.NewActivitiesWithOptions(store, orker.Options{
		WorkRoot: workRoot,
		Runtime:  r.observed,
		Refs:     store.local,
		Policy:   &policy,
		OrkerID:  owner.id,
		Phases:   r.t,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		// Commits, Attempts, Logs, TaskLogs, TraceContext: nil — deliberately.
		// A local run has no durable result to honor, records no attempt and
		// streams no chunk anywhere. EnvAllowlist: nil — the caller's
		// ORK_ORKER_ENV_ALLOWLIST, exactly as an Orker reads it.
		// Signatures: nil — LocalTaskInput leaves image_policy unset, so the
		// dispatcher's image-policy step is never entered.
	})
	dispatcher := acts.Dispatcher()
	dispatcher.LogSinkObserver = r.t.line

	validation := dispatcher.Validate(r.in)
	r.printHeader(validation)
	if validation != nil {
		return r.finishFailure(validation, nil)
	}

	// STEP 9. Supplied bytes into the local store, then the run's bytes.
	if err := r.putSupplied(); err != nil {
		return err
	}
	if err := r.fetch(); err != nil {
		return err
	}
	if r.ctx.Err() != nil {
		return r.finish(taskRunCanceled, "", nil, nil)
	}

	// A pin's shared directory, seeded when --shared asks.
	if pin := r.in.GetPin(); pin != nil {
		defer func() {
			if r.opts.keep {
				if pinWS, acqErr := orker.AcquirePin(workRoot, r.localID, pin.GetName(), pin.GetInstanceId(), pin.GetTimeoutSeconds()); acqErr == nil {
					if skipped, _ := copyTree(pinWS.Shared, filepath.Join(r.session, "shared"), nil); len(skipped) > 0 {
						for _, note := range skipped {
							r.t.println("kept shared: " + note)
						}
					}
				}
			}
			_, _ = orker.ReleasePinDirectory(workRoot, r.localID, pin.GetName(), pin.GetInstanceId())
		}()
		if r.parsed.shared != "" {
			pinWS, acqErr := orker.AcquirePin(workRoot, r.localID, pin.GetName(), pin.GetInstanceId(), pin.GetTimeoutSeconds())
			if acqErr != nil {
				return fmt.Errorf("--shared: acquire the pin's shared directory: %w", acqErr)
			}
			if _, copyErr := copyTree(r.parsed.shared, pinWS.Shared, nil); copyErr != nil {
				return fmt.Errorf("--shared: seed %s: %w", r.opts.shared, copyErr)
			}
		}
	}

	// STEP 10. Execute.
	if !r.opts.renderOnly && !r.g.json {
		r.t.println(taskRunSideEffects)
	}
	result, execErr := dispatcher.Execute(r.ctx, r.in)

	// STEP 11. Report.
	return r.outcome(result, execErr)
}

// onSealed is called by observedRuntime.Create once the sealed input tree is
// snapshotted: config.json is printed before the container starts, and under
// --render-only task.json after it.
func (r *taskRunner) onSealed(snapshot string) {
	r.t.printFile("config.json", filepath.Join(snapshot, "config.json"))
	if raw, err := os.ReadFile(filepath.Join(snapshot, "config.json")); err == nil && json.Valid(raw) {
		r.report.Config = json.RawMessage(raw)
	}
	if raw, err := os.ReadFile(filepath.Join(snapshot, "task.json")); err == nil && json.Valid(raw) {
		r.report.TaskJSON = json.RawMessage(raw)
		var doc struct {
			Image struct {
				PlatformDigest string `json:"platformDigest"`
			} `json:"image"`
		}
		if json.Unmarshal(raw, &doc) == nil {
			r.report.PlatformDigest = doc.Image.PlatformDigest
		}
	}
	if r.opts.renderOnly {
		r.t.printFile("task.json", filepath.Join(snapshot, "task.json"))
	}
}

// putSupplied writes every supplied producer's values and artifacts into the
// session's local store and threads their references into the input, in
// closure order.
func (r *taskRunner) putSupplied() error {
	refs := map[string]*orkstackv1.UpstreamRef{}
	for _, up := range r.in.GetUpstream() {
		refs[up.GetTaskId()] = up
	}
	for _, source := range r.sources {
		if source.supplied == nil {
			continue
		}
		prov := artifacts.Provenance{Orkflow: r.wf.GetName(), TaskID: source.taskID, RunID: r.localID}
		valuesRef, err := r.store.Put(r.ctx, "values.json", strings.NewReader(string(source.supplied.values)),
			artifacts.PutOptions{ContentType: "application/json", Metadata: prov.Metadata(nil)})
		if err != nil {
			return fmt.Errorf("store the supplied values of %s: %w", source.taskID, err)
		}
		up := &orkstackv1.UpstreamRef{TaskId: source.taskID, Values: protoRef(valuesRef)}
		// A failure handler's FAILED need contributes its files and no
		// values, exactly as the interpreter's failed-need ref does.
		if r.in.GetJoin() != nil {
			switch r.parsed.needStatus[source.taskID] {
			case orkstackv1.Status_STATUS_FAILED:
				up = &orkstackv1.UpstreamRef{TaskId: source.taskID}
			case orkstackv1.Status_STATUS_SKIPPED, orkstackv1.Status_STATUS_CANCELED:
				// A need that did not run contributes nothing.
				continue
			}
		}
		for _, a := range source.supplied.artifacts {
			file, err := os.Open(a.path)
			if err != nil {
				return fmt.Errorf("invalid --artifact %q: %w", a.flag, err)
			}
			ref, err := r.store.Put(r.ctx, a.name, file, artifacts.PutOptions{Metadata: prov.Metadata(nil)})
			_ = file.Close()
			if err != nil {
				return fmt.Errorf("store --artifact %q: %w", a.flag, err)
			}
			up.Artifacts = append(up.Artifacts, protoRef(ref))
		}
		source.values = up.GetValues()
		source.outputs = up.GetArtifacts()
		refs[source.taskID] = up
	}
	ordered := make([]*orkstackv1.UpstreamRef, 0, len(refs))
	for _, id := range r.closure {
		if up, ok := refs[id]; ok {
			ordered = append(ordered, up)
		}
	}
	r.in.Upstream = ordered
	return nil
}

func protoRef(ref artifacts.ArtifactRef) *orkstackv1.ArtifactRef {
	return &orkstackv1.ArtifactRef{
		Name: ref.Name, Uri: ref.URI, ContentType: ref.ContentType,
		SizeBytes: ref.SizeBytes, Sha256: ref.SHA256, Metadata: ref.Metadata,
	}
}

// fetch reads every member whose source is the run: each
// recorded reference is read through ReadArtifact, checked against the
// reference GetRun returned, hashed, and served to the dispatcher under the
// recorded URI.
func (r *taskRunner) fetch() error {
	n := 0
	for _, source := range r.sources {
		if source.from != "run" {
			continue
		}
		type item struct {
			ref  *orkstackv1.ArtifactRef
			kind orkstackv1.ArtifactKind
		}
		var items []item
		if source.values.GetUri() != "" {
			items = append(items, item{source.values, orkstackv1.ArtifactKind_ARTIFACT_KIND_VALUES})
		}
		for _, ref := range source.outputs {
			items = append(items, item{ref, orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT})
		}
		for _, it := range items {
			n++
			path := filepath.Join(r.session, ".fetched", fmt.Sprintf("%04d", n))
			if err := r.fetchOne(source.taskID, it.kind, it.ref, path); err != nil {
				if r.ctx.Err() != nil {
					return r.finish(taskRunCanceled, "", nil, nil)
				}
				supply := fmt.Sprintf("--artifact %s/%s=PATH", source.taskID, it.ref.GetName())
				if it.kind == orkstackv1.ArtifactKind_ARTIFACT_KIND_VALUES {
					supply = fmt.Sprintf("--output %s=…", source.taskID)
				}
				return fmt.Errorf("fetch %s/%s from run %s: %v — the bytes may have been reclaimed; supply them with %s",
					source.taskID, it.ref.GetName(), r.run.GetId(), err, supply)
			}
			r.store.serve(it.ref.GetUri(), path)
			r.t.field("fetched", fmt.Sprintf("%s/%s  %d  %s  ok", source.taskID, it.ref.GetName(),
				it.ref.GetSizeBytes(), it.ref.GetSha256()))
		}
	}
	return nil
}

func (r *taskRunner) fetchOne(taskID string, kind orkstackv1.ArtifactKind, want *orkstackv1.ArtifactRef, path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	ctx, cancel := r.streamContext()
	defer cancel()
	got, readErr := readRecordedArtifact(ctx, r.g.runClient(), &orkstackv1.ReadArtifactRequest{
		RunId: r.run.GetId(), Namespace: r.g.namespace, TaskId: taskID, Kind: kind, Name: want.GetName(),
	}, file)
	if closeErr := file.Close(); readErr == nil && closeErr != nil {
		readErr = closeErr
	}
	if readErr != nil {
		return readErr
	}
	if got == nil || got.GetUri() != want.GetUri() || got.GetSha256() != want.GetSha256() {
		return errors.New("the server's reference does not match the run's record")
	}
	sum, size, err := orker.HashFile(path)
	if err != nil {
		return err
	}
	if size != want.GetSizeBytes() || (want.GetSha256() != "" && sum != strings.TrimPrefix(want.GetSha256(), "sha256:")) {
		return fmt.Errorf("the bytes hash to %s (%d bytes), the run recorded %s (%d bytes)",
			sum, size, want.GetSha256(), want.GetSizeBytes())
	}
	return nil
}

// outcome turns what Execute returned into the report.
func (r *taskRunner) outcome(result *orkstackv1.ExecuteTaskResult, execErr error) error {
	seen := r.observed.observed()
	if execErr == nil {
		var exit *int32
		code := result.GetExitCode()
		exit = &code
		return r.finish(taskRunSucceeded, "", nil, &finishFacts{result: result, exitCode: exit, seen: seen})
	}
	if r.opts.renderOnly && seen.stopped {
		return r.finish(taskRunRendered, "", nil, &finishFacts{seen: seen})
	}
	if errors.Is(execErr, context.Canceled) || r.ctx.Err() != nil {
		if _, _, ok := decodeDispatchFailure(execErr); !ok {
			return r.finish(taskRunCanceled, "", nil, &finishFacts{seen: seen})
		}
	}
	detail, category, ok := decodeDispatchFailure(execErr)
	if !ok {
		return fmt.Errorf("run the task: %w", execErr)
	}
	return r.finishFailure(&orker.Failure{
		Reason: detail.GetReasonCode(), Message: dispatchMessage(execErr, detail.GetReasonCode()), Detail: detail,
	}, &finishFacts{seen: seen, category: category})
}

// dispatchMessage is the Orker's human message for a failure, without the
// "REASON: " prefix Failure.Error() puts in front of it.
func dispatchMessage(err error, reason string) string {
	message := err.Error()
	var failure *orker.Failure
	if errors.As(err, &failure) {
		message = failure.Message
	} else if app := applicationMessage(err); app != "" {
		message = app
	}
	return strings.TrimPrefix(message, reason+": ")
}

// finishFacts is what the dispatcher left behind for the result block.
type finishFacts struct {
	result   *orkstackv1.ExecuteTaskResult
	exitCode *int32
	seen     observation
	category string
}

// finishFailure reports a dispatcher Failure (a Validate refusal or what
// Execute returned) with its own code and category.
func (r *taskRunner) finishFailure(failure *orker.Failure, facts *finishFacts) error {
	category := failure.Category()
	if facts != nil && facts.category != "" {
		category = facts.category
	}
	if facts == nil {
		facts = &finishFacts{}
	}
	if facts.seen.ranContainer && facts.seen.waited {
		code := failure.Detail.GetExitCode()
		facts.exitCode = &code
	}
	return r.finish(taskRunFailed, category, &taskRunError{
		Code:      failure.Reason,
		Category:  shortCategory(category),
		Message:   failure.Message,
		ScanNotes: failure.Detail.GetScanNotes(),
	}, facts, failure.Detail)
}
