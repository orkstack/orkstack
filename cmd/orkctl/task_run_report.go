package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
)

// Report outcomes. They are the words the human result block starts with and
// the --json report's outcome.
const (
	taskRunSucceeded = "SUCCEEDED"
	taskRunFailed    = "FAILED"
	taskRunCanceled  = "CANCELED"
	taskRunRendered  = "RENDERED"
)

// taskRunNothingWritten is the transcript's last line on every path that
// reached the dispatcher: the promise the command keeps by construction.
const taskRunNothingWritten = "Nothing was written to the deployment: no run, no commit, no attempt record, no log."

// taskRunSideEffects is printed before the container starts, in the manner of
// `orkctl run again`'s warning: a local run performs its side effects for real.
const taskRunSideEffects = "This starts the task's container on this machine; any external side effect it performs is real."

// taskRunConfigPrintLimit bounds how much of config.json (and, under
// --render-only, task.json) the transcript prints.
const taskRunConfigPrintLimit = 65536

// taskRunValuesPrintLimit bounds the values line of a succeeded run.
const taskRunValuesPrintLimit = 4096

// taskRunReport is the --json report: a local result with no wire contract,
// so a plain Go type rendered by writeGoJSON, the reason `activity test`
// gives for not inventing a proto message. Enum-valued strings use the short
// dialect of docs/JSON.md.
type taskRunReport struct {
	Task           string            `json:"task"`
	Namespace      string            `json:"namespace"`
	Orkflow        taskRunDefinition `json:"orkflow"`
	SourceRun      *taskRunSource    `json:"sourceRun,omitempty"`
	LocalRunID     string            `json:"localRunId"`
	IdempotencyKey string            `json:"idempotencyKey"`
	Image          string            `json:"image"`
	PlatformDigest string            `json:"platformDigest,omitempty"`
	Input          json.RawMessage   `json:"input"`
	Upstream       []taskRunUpstream `json:"upstream"`
	Secrets        []taskRunSecret   `json:"secrets"`
	Limits         taskRunLimits     `json:"limits"`
	RenderOnly     bool              `json:"renderOnly"`
	Outcome        string            `json:"outcome"`
	Error          *taskRunError     `json:"error,omitempty"`
	ExitCode       *int32            `json:"exitCode,omitempty"`
	ContainerMs    int64             `json:"containerMs"`
	TotalMs        int64             `json:"totalMs"`
	Config         json.RawMessage   `json:"config,omitempty"`
	TaskJSON       json.RawMessage   `json:"taskJson,omitempty"`
	Values         json.RawMessage   `json:"values,omitempty"`
	Artifacts      []taskRunArtifact `json:"artifacts"`
	Workspace      string            `json:"workspace,omitempty"`
}

type taskRunDefinition struct {
	Name             string `json:"name"`
	Version          string `json:"version"`
	DefinitionDigest string `json:"definitionDigest"`
	Source           string `json:"source"` // "file" | "run"
}

type taskRunSource struct {
	ID               string `json:"id"`
	OrkflowVersion   string `json:"orkflowVersion"`
	DefinitionDigest string `json:"definitionDigest,omitempty"`
	TaskStatus       string `json:"taskStatus,omitempty"`
	TaskError        string `json:"taskError,omitempty"`
}

type taskRunUpstream struct {
	TaskID    string            `json:"taskId"`
	Status    string            `json:"status,omitempty"`
	From      string            `json:"from"` // "run" | "supplied" | "none"
	Values    *taskRunArtifact  `json:"values,omitempty"`
	Artifacts []taskRunArtifact `json:"artifacts"`
}

// taskRunArtifact is {name, sizeBytes, sha256} — never a URI: a local store
// URI is a path on this machine and a recorded one is a bucket location,
// neither of which the reader needs.
type taskRunArtifact struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"sizeBytes"`
	SHA256    string `json:"sha256"`
}

type taskRunSecret struct {
	Name   string `json:"name"`
	Staged bool   `json:"staged"`
	Scheme string `json:"scheme"` // "file" | "env" | ""
}

type taskRunLimits struct {
	ValuesMaxBytes   int64  `json:"valuesMaxBytes"`
	ArtifactMaxCount int32  `json:"artifactMaxCount"`
	ArtifactMaxBytes int64  `json:"artifactMaxBytes"`
	LogMaxBytes      int64  `json:"logMaxBytes"`
	CommitMaxBytes   int64  `json:"commitMaxBytes"`
	Source           string `json:"source"` // "contract-defaults"
}

type taskRunError struct {
	Code      string   `json:"code"`
	Category  string   `json:"category"`
	Message   string   `json:"message"`
	ScanNotes []string `json:"scanNotes,omitempty"`
}

func artifactOf(ref *orkstackv1.ArtifactRef) taskRunArtifact {
	return taskRunArtifact{Name: ref.GetName(), SizeBytes: ref.GetSizeBytes(), SHA256: ref.GetSha256()}
}

func artifactsOf(refs []*orkstackv1.ArtifactRef) []taskRunArtifact {
	out := make([]taskRunArtifact, 0, len(refs))
	for _, ref := range refs {
		out = append(out, artifactOf(ref))
	}
	return out
}

// shortCategory renders an §8.1 category string in the short enum dialect:
// "ActivityFailed" -> "ACTIVITY_FAILED".
func shortCategory(category string) string {
	return strings.TrimPrefix(orker.CategoryEnum(category).String(), "FAILURE_CATEGORY_")
}

// ---------------------------------------------------------------- transcript

// taskRunPhaseLines is the fixed table the PhaseObserver prints through. An
// empty line prints nothing; a phase absent from the table (a later
// proposal's) prints verbatim.
var taskRunPhaseLines = map[string]string{
	"start":        "",
	"commit-check": "",
	"workspace":    "",
	"create":       "",
	"exit":         "",
	"stage-inputs": "staging inputs",
	"image":        "pulling image",
	"render":       "rendering",
	"run":          "running",
	"validate":     "validating outputs",
	"commit":       "collecting outputs (into this machine's session directory)",
	"timeout":      "timeout: SIGTERM, then SIGKILL after the grace period",
	"cancel":       "interrupted: SIGTERM, then SIGKILL after the grace period",
}

// transcript is the human account of one invocation. It is also the
// dispatcher's PhaseObserver and its LogSinkObserver, so progress and the
// container's streams are printed by the same writer, in the order they
// happen. Its writer is stdout in human mode and stderr under --json, so a
// person watching sees the same lines either way.
type transcript struct {
	mu     sync.Mutex
	out    io.Writer
	stream *os.File // session/logs/stream.log, once the session exists
}

func (t *transcript) printf(format string, args ...any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fmt.Fprintf(t.out, format, args...)
}

func (t *transcript) println(line string) { t.printf("%s\n", line) }

// field prints one "label        value" line, the label padded to 13
// columns; a multi-line value continues under the value column.
func (t *transcript) field(label, value string) {
	lines := strings.Split(value, "\n")
	t.printf("%-13s%s\n", label, lines[0])
	for _, more := range lines[1:] {
		t.printf("%-13s%s\n", "", more)
	}
}

// line is the LogSinkObserver: every framed line the dispatcher emits, with
// its [stderr] marker, printed as "  │ " plus the line and appended to
// logs/stream.log.
func (t *transcript) line(_ *orkstackv1.ExecuteTaskInput, line string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fmt.Fprintf(t.out, "  │ %s\n", line)
	if t.stream != nil {
		_, _ = fmt.Fprintln(t.stream, line)
	}
}

// Begin implements orker.PhaseObserver.
func (t *transcript) Begin(ctx context.Context, _ *orkstackv1.ExecuteTaskInput, _ int32) (context.Context, orker.AttemptObserver) {
	return ctx, t
}

// Phase implements orker.AttemptObserver.
func (t *transcript) Phase(name string) {
	text, known := taskRunPhaseLines[name]
	if !known {
		text = name
	}
	if text != "" {
		t.println("› " + text)
	}
}

// End implements orker.AttemptObserver. The outcome is reported from what
// Execute returns, so End adds nothing.
func (t *transcript) End(orker.AttemptOutcome) {}

// printFile prints one sealed input file: its size, then the file as written
// (two-space indented JSON) indented two more spaces, stopping at
// taskRunConfigPrintLimit bytes.
func (t *transcript) printFile(label, path string) {
	raw, err := os.ReadFile(path)
	if err != nil {
		t.field(label, fmt.Sprintf("(could not be read: %v)", err))
		return
	}
	t.field(label, fmt.Sprintf("(%d bytes)", len(raw)))
	shown := raw[:runeCut(raw, taskRunConfigPrintLimit)]
	scanner := bufio.NewScanner(bytes.NewReader(shown))
	scanner.Buffer(make([]byte, 0, 64*1024), taskRunConfigPrintLimit+1)
	for scanner.Scan() {
		t.println("  " + scanner.Text())
	}
	if len(shown) < len(raw) {
		t.printf("  … (%d more bytes; --keep keeps input/%s)\n", len(raw)-len(shown), label)
	}
}

// renderArtifactRows renders "<producer>/<name>  SIZE  SHA256" rows, aligned
// the way `run artifacts` aligns its table.
func renderArtifactRows(rows [][3]string) []string {
	var buf bytes.Buffer
	table := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
	for _, row := range rows {
		fmt.Fprintf(table, "%s\t%s\t%s\n", row[0], row[1], row[2])
	}
	_ = table.Flush()
	return strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
}

// canonicalValues renders a values.json document on one line, truncated at
// taskRunValuesPrintLimit bytes.
func canonicalValues(raw []byte) string {
	var decoded map[string]any
	line := strings.TrimSpace(string(raw))
	if json.Unmarshal(raw, &decoded) == nil {
		if data, err := structFromMap(decoded); err == nil {
			if canonical, err := dsl.CanonicalInputLine(data); err == nil {
				line = canonical
			}
		}
	}
	if len(line) > taskRunValuesPrintLimit {
		return fmt.Sprintf("%s… (%d bytes; --json or --keep has every byte)", line[:runeCut([]byte(line), taskRunValuesPrintLimit)], len(line))
	}
	return line
}

// runeCut returns the largest length at most limit that does not split a
// UTF-8 sequence of b, so a truncated line never ends in half a character.
func runeCut(b []byte, limit int) int {
	if len(b) <= limit {
		return len(b)
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(b[cut]) {
		cut--
	}
	return cut
}

// formatSeconds renders a duration the transcript's one way: seconds with
// one decimal.
func formatSeconds(d time.Duration) string {
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// ---------------------------------------------------------------- the runner's report

func structFromMap(m map[string]any) (*structpb.Struct, error) { return structpb.NewStruct(m) }

// applicationMessage is a Temporal ApplicationError's own message, without
// the cause Error() appends.
func applicationMessage(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		return app.Message()
	}
	return ""
}

// taskErrorLine is errorLine without saying the code twice: the engine's
// message for an Orker failure already starts with its reason code.
func taskErrorLine(err *orkstackv1.Error) string {
	if code := err.GetCode(); code != "" && strings.HasPrefix(err.GetMessage(), code+": ") {
		return err.GetMessage()
	}
	return errorLine(err)
}

// newReport fills every field the invocation knows before the dispatcher
// runs.
func (r *taskRunner) newReport() *taskRunReport {
	report := &taskRunReport{
		Task:      r.opts.task,
		Namespace: r.namespace(),
		Orkflow: taskRunDefinition{
			Name: r.wf.GetName(), Version: r.wf.GetVersion(),
			DefinitionDigest: r.wf.GetDefinitionDigest(), Source: "run",
		},
		LocalRunID:     r.localID,
		IdempotencyKey: task.IdempotencyKeyFor(r.wf.GetNamespace(), r.localID, r.opts.task),
		Image:          r.spec.GetImage().GetPinned(),
		Upstream:       []taskRunUpstream{},
		Secrets:        []taskRunSecret{},
		RenderOnly:     r.opts.renderOnly,
		Artifacts:      []taskRunArtifact{},
	}
	if r.opts.file != "" {
		report.Orkflow.Source = "file"
	}
	if r.run != nil {
		source := &taskRunSource{
			ID: r.run.GetId(), OrkflowVersion: r.run.GetOrkflowVersion(),
			DefinitionDigest: r.run.GetOrkflowDefinitionDigest(),
		}
		if tr := r.taskRunOf(r.opts.task); tr != nil {
			source.TaskStatus = statusLabel(tr.GetStatus())
			if tr.GetError() != nil {
				source.TaskError = taskErrorLine(tr.GetError())
			}
		}
		report.SourceRun = source
	}
	if line, err := dsl.CanonicalInputLine(r.input.GetData()); err == nil {
		report.Input = json.RawMessage(line)
	}
	limits := engine.DefaultLimits()
	report.Limits = taskRunLimits{
		ValuesMaxBytes: limits.ValuesMaxBytes, ArtifactMaxCount: limits.ArtifactMaxCount,
		ArtifactMaxBytes: limits.ArtifactMaxBytes, LogMaxBytes: limits.LogMaxBytes,
		CommitMaxBytes: limits.CommitMaxBytes, Source: "contract-defaults",
	}
	for _, secret := range r.spec.GetSecrets() {
		name := secret.GetName()
		entry := taskRunSecret{Name: name}
		if source, ok := r.parsed.secrets[name]; ok && !r.opts.renderOnly {
			entry.Staged = true
			entry.Scheme, _ = orker.SplitSecretSource(source)
		}
		report.Secrets = append(report.Secrets, entry)
	}
	return report
}

// printHeader prints everything the transcript states before any byte is
// fetched. validation is the dispatcher's pure Validate verdict, nil when it
// passed (and when the input could not even be composed).
func (r *taskRunner) printHeader(validation *orker.Failure) {
	t := r.t
	t.printf("TASK RUN  %s  %s@%s  namespace %s\n", r.opts.task, r.wf.GetName(), r.wf.GetVersion(), r.namespace())
	definition := r.wf.GetDefinitionDigest() + "  "
	switch {
	case r.opts.file != "":
		definition += "from " + r.opts.file + ", not applied"
	case r.run.GetOrkflowDefinitionDigest() == "":
		definition += "the run's own (the run recorded no definition digest)"
	default:
		definition += "the run's own"
	}
	t.field("definition", definition)
	if r.run != nil {
		digest := r.run.GetOrkflowDefinitionDigest()
		if digest == "" {
			digest = "no definition digest recorded"
		}
		there := r.opts.task + " there: not in this run"
		if tr := r.taskRunOf(r.opts.task); tr != nil {
			there = r.opts.task + " there: " + taskStatusLabel(tr)
			if tr.GetError() != nil {
				there += "  " + taskErrorLine(tr.GetError())
			}
		}
		t.field("source run", fmt.Sprintf("%s  %s@%s  %s\n%s", r.run.GetId(), r.run.GetOrkflowName(),
			r.run.GetOrkflowVersion(), digest, there))
	}
	t.field("local run", fmt.Sprintf("%s  (a new logical activity; idempotency key %s)", r.localID, r.report.IdempotencyKey))
	t.field("image", r.spec.GetImage().GetPinned())
	input, err := dsl.CanonicalInputLine(r.input.GetData())
	if err != nil {
		input = "(could not be rendered: " + err.Error() + ")"
	}
	t.field("input", input)
	if condition := r.spec.GetCondition(); condition != "" {
		t.field("condition", fmt.Sprintf("%q — not evaluated here; orkctl run plan answers whether it would run", condition))
	}
	if attempts := r.spec.GetRetry().GetMaximumAttempts(); attempts > 1 {
		t.field("retry", fmt.Sprintf("maximum %d attempts — not applied here: a local run is one attempt", attempts))
	}
	if pin := r.spec.GetPin(); pin != "" {
		if r.opts.shared != "" {
			t.field("pin", fmt.Sprintf("%s — /orkstack/shared starts with the contents of %s (--shared)", pin, r.opts.shared))
		} else {
			t.field("pin", pin+" — /orkstack/shared starts EMPTY: the pin host's directory is gone and is never reconstructed")
		}
	}
	for _, issue := range r.warnings {
		if issue.GetField() != "" {
			t.printf("warning: %s: %s\n", issue.GetField(), issue.GetMessage())
		} else {
			t.printf("warning: %s\n", issue.GetMessage())
		}
	}
	t.println("")

	// The upstream table, one row per closure member in closure order.
	if len(r.sources) == 0 {
		t.field("upstream", "none")
	} else {
		var buf bytes.Buffer
		table := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
		fmt.Fprintln(table, "UPSTREAM\tSTATUS\tFROM\tVALUES\tARTIFACTS")
		for _, source := range r.sources {
			status := dash
			if source.taskRun != nil {
				status = taskStatusLabel(source.taskRun)
			}
			from, values, count := dash, dash, dash
			switch source.from {
			case "run":
				from = r.run.GetId()
				if source.values != nil {
					values = fmt.Sprint(source.values.GetSizeBytes())
				}
				count = fmt.Sprint(len(source.outputs))
			case "supplied":
				from = "supplied"
				values = fmt.Sprint(len(source.supplied.values))
				count = fmt.Sprint(len(source.supplied.artifacts))
			}
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n", source.taskID, status, from, values, count)
		}
		_ = table.Flush()
		for _, line := range strings.Split(strings.TrimRight(buf.String(), "\n"), "\n") {
			t.println(strings.TrimRight(line, " "))
		}
		rows := [][3]string{{"ARTIFACT", "SIZE", "SHA256"}}
		for _, source := range r.sources {
			switch source.from {
			case "run":
				for _, ref := range source.outputs {
					rows = append(rows, [3]string{source.taskID + "/" + ref.GetName(), fmt.Sprint(ref.GetSizeBytes()), ref.GetSha256()})
				}
			case "supplied":
				for i, a := range source.supplied.artifacts {
					rows = append(rows, [3]string{source.taskID + "/" + a.name, fmt.Sprint(source.supplied.sizes[i]), source.supplied.hashes[i]})
				}
			}
		}
		if len(rows) > 1 {
			t.println("")
			for _, line := range renderArtifactRows(rows) {
				t.println(strings.TrimRight(line, " "))
			}
		}
	}
	t.println("")

	// Secrets: names and whether they are staged — never a source or value.
	if len(r.spec.GetSecrets()) == 0 {
		t.field("secrets", "none declared")
	} else {
		var lines []string
		for _, secret := range r.spec.GetSecrets() {
			name := secret.GetName()
			switch {
			case r.opts.renderOnly:
				lines = append(lines, name+"  not staged (--render-only)")
			case r.parsed.omitted[name]:
				lines = append(lines, name+"  omitted (--omit-secret)")
			default:
				scheme, _ := orker.SplitSecretSource(r.parsed.secrets[name])
				lines = append(lines, fmt.Sprintf("%s  supplied (%s)", name, scheme))
			}
		}
		t.field("secrets", strings.Join(lines, "\n"))
	}
	allow := strings.TrimSpace(os.Getenv(orker.EnvAllowlistVar))
	if allow == "" {
		t.field("env helper", orker.EnvAllowlistVar+" is empty: the env helper reads no variable")
	} else {
		t.field("env helper", orker.EnvAllowlistVar+" allows "+allow+" (from this shell)")
	}
	if cpu, memory, failure := r.parsed.policy.ResolveResources(r.spec.GetResources()); failure == nil {
		t.field("resources", fmt.Sprintf("cpu %s, memory %s (this machine's ORK_ORKER_* policy; the pool's Orkers apply their own)", cpu, memory))
	} else {
		t.field("resources", "refused by this machine's ORK_ORKER_* policy (below)")
	}
	if network, failure := r.parsed.policy.ResolveNetwork(r.spec.GetNetwork()); failure == nil {
		if network == orker.NetworkNone {
			t.field("network", "none (no network)")
		} else {
			t.field("network", network+" (this machine's container network, not the pool's)")
		}
	} else {
		t.field("network", "refused by this machine's ORK_ORKER_* policy (below)")
	}
	limits := r.report.Limits
	t.field("limits", fmt.Sprintf("contract defaults: values %d, %d artifacts of at most %d, logs %d per stream\n"+
		"(a deployment may configure others; they are engine configuration no client reads)",
		limits.ValuesMaxBytes, limits.ArtifactMaxCount, limits.ArtifactMaxBytes, limits.LogMaxBytes))
	t.println("")
	_ = validation
}

// finish completes the report, prints the result block, and maps the outcome
// to the command's exit status.
func (r *taskRunner) finish(
	outcome, category string,
	failure *taskRunError,
	facts *finishFacts,
	detail ...*orkstackv1.ActivityFailureDetail,
) error {
	if facts == nil {
		facts = &finishFacts{}
	}
	report := r.report
	report.Outcome = outcome
	report.Error = failure
	report.ExitCode = facts.exitCode
	report.ContainerMs = facts.seen.ran.Milliseconds()
	report.TotalMs = time.Since(r.start).Milliseconds()
	for _, source := range r.sources {
		row := taskRunUpstream{TaskID: source.taskID, From: source.from, Artifacts: []taskRunArtifact{}}
		if source.taskRun != nil {
			row.Status = statusLabel(source.taskRun.GetStatus())
		}
		switch source.from {
		case "run", "supplied":
			if source.values != nil {
				values := artifactOf(source.values)
				row.Values = &values
			}
			row.Artifacts = artifactsOf(source.outputs)
		}
		report.Upstream = append(report.Upstream, row)
	}
	var produced, system []*orkstackv1.ArtifactRef
	if facts.result != nil {
		produced = facts.result.GetArtifacts()
		system = facts.result.GetSystemArtifacts()
		report.Values = r.valuesOf(facts.result)
		if image := facts.result.GetImage(); image.GetPlatformDigest() != "" {
			report.PlatformDigest = image.GetPlatformDigest()
		}
	}
	if len(detail) > 0 && detail[0] != nil {
		produced = detail[0].GetArtifacts()
		system = detail[0].GetSystemArtifacts()
	}
	report.Artifacts = artifactsOf(produced)

	t := r.t
	t.println("")
	switch outcome {
	case taskRunSucceeded:
		t.println(taskRunSucceeded)
	case taskRunRendered:
		t.println(taskRunRendered + "  no container was started")
	case taskRunCanceled:
		t.println(taskRunCanceled)
	default:
		t.printf("%s  %s (%s)\n", taskRunFailed, failure.Code, category)
		t.println("  " + failure.Message)
		for _, note := range failure.ScanNotes {
			t.println("  " + note)
		}
	}
	if facts.exitCode != nil && facts.seen.ranContainer {
		t.field("exit code", fmt.Sprintf("%d after %s", *facts.exitCode, formatSeconds(facts.seen.ran)))
	}
	switch outcome {
	case taskRunSucceeded:
		t.field("values", canonicalValues(report.Values))
	case taskRunRendered:
		t.field("values", "none: no container was started")
	case taskRunCanceled:
		t.field("values", "none: an interrupted attempt writes no values.json anywhere")
	default:
		t.field("values", "none: a failed attempt writes no values.json anywhere")
	}
	if len(produced) == 0 {
		t.field("artifacts", "none")
	} else {
		var rows [][3]string
		for _, ref := range produced {
			rows = append(rows, [3]string{ref.GetName(), fmt.Sprint(ref.GetSizeBytes()), ref.GetSha256()})
		}
		lines := renderArtifactRows(rows)
		for i := range lines {
			lines[i] = strings.TrimRight(lines[i], " ")
		}
		t.field("artifacts", strings.Join(lines, "\n"))
	}
	for _, note := range facts.seen.keptNotes {
		t.println("kept output: " + note)
	}

	if r.session != "" {
		if r.opts.keep {
			report.Workspace = r.session
			r.keepLogs(system)
		}
		if file, err := os.Create(filepath.Join(r.session, "report.json")); err == nil {
			_ = writeGoJSON(file, report)
			_ = file.Close()
		}
	}
	t.println(taskRunNothingWritten)
	if r.session != "" && r.opts.keep {
		t.println("workspace kept at " + r.session)
	}
	if r.g.json {
		if err := writeGoJSON(r.stdout, report); err != nil {
			return err
		}
	}
	code := taskRunExitCode(outcome, category)
	switch code {
	case exitOK:
		return nil
	case exitRunCanceled:
		return &exitCodeError{code: code, err: fmt.Errorf("task %s was interrupted", r.opts.task)}
	default:
		return &exitCodeError{code: code, err: fmt.Errorf("task %s %s: %s", r.opts.task, strings.ToLower(outcome), failure.Code)}
	}
}

// valuesOf is a succeeded attempt's values.json: inline when the dispatcher
// inlined it, else read back from the session's local store.
func (r *taskRunner) valuesOf(result *orkstackv1.ExecuteTaskResult) json.RawMessage {
	raw := result.GetValuesJson()
	if !result.GetValuesInlined() && result.GetValuesRef().GetUri() != "" && r.store != nil {
		if reader, err := r.store.Get(r.ctx, result.GetValuesRef().GetUri()); err == nil {
			raw, _ = io.ReadAll(reader)
			_ = reader.Close()
		}
	}
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return nil
	}
	return json.RawMessage(compact.Bytes())
}

// keepLogs copies the byte-exact captured streams the dispatcher stored into
// session/logs/.
func (r *taskRunner) keepLogs(system []*orkstackv1.ArtifactRef) {
	for _, ref := range system {
		switch ref.GetName() {
		case orker.StdoutLogName, orker.StderrLogName:
		default:
			continue
		}
		reader, err := r.store.Get(context.WithoutCancel(r.ctx), ref.GetUri())
		if err != nil {
			continue
		}
		if file, err := os.Create(filepath.Join(r.session, "logs", ref.GetName())); err == nil {
			_, _ = io.Copy(file, reader)
			_ = file.Close()
		}
		_ = reader.Close()
	}
}

// cleanup removes the session directory on every path unless --keep, with
// the unseal-then-remove walk the Orker uses for a sealed tree. A file the
// container created as another uid that this user cannot remove is reported
// with its path, never ignored.
func (r *taskRunner) cleanup() {
	if r.session == "" || r.opts.keep {
		return
	}
	if err := (&orker.Workspace{Root: r.session}).Remove(); err != nil {
		r.t.printf("warning: the session directory %s could not be removed: %v\n", r.session, err)
	}
}
