package shellplugin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/plugin"
)

func execContext(t *testing.T, config map[string]any) *plugin.ExecContext {
	t.Helper()
	return &plugin.ExecContext{
		TaskID:  "transform",
		Config:  config,
		WorkDir: t.TempDir(),
		Log:     func(string, ...any) {},
	}
}

func TestShellPlugin_RunsCommandInWorkDirAndReportsStreams(t *testing.T) {
	ec := execContext(t, map[string]any{
		"command": "pwd; echo to-stdout; echo to-stderr 1>&2",
	})
	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.Output["exit_code"] != float64(0) {
		t.Errorf("exit_code = %v, want 0", result.Output["exit_code"])
	}
	stdout, _ := result.Output["stdout"].(string)
	if !strings.Contains(stdout, "to-stdout") {
		t.Errorf("stdout = %q, want it to contain to-stdout", stdout)
	}
	// The shell reports the workdir through pwd; macOS resolves /var to
	// /private/var, so compare the final component.
	if !strings.Contains(stdout, filepath.Base(ec.WorkDir)) {
		t.Errorf("stdout = %q, want the command to run in %q", stdout, ec.WorkDir)
	}
	if stderr, _ := result.Output["stderr"].(string); !strings.Contains(stderr, "to-stderr") {
		t.Errorf("stderr = %q, want it to contain to-stderr", stderr)
	}
}

func TestShellPlugin_NonZeroExitFailsTheTask(t *testing.T) {
	_, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"command": "echo failing 1>&2; exit 3",
	}))
	if err == nil {
		t.Fatal("Execute succeeded, want a failure for a non-zero exit")
	}
	if !strings.Contains(err.Error(), "exited with code 3") {
		t.Errorf("error %q does not report the exit code", err)
	}
	if !strings.Contains(err.Error(), "failing") {
		t.Errorf("error %q does not include the stderr tail", err)
	}
	var retryable *plugin.RetryableError
	if !errors.As(err, &retryable) || retryable.Retryable {
		t.Errorf("error %v is not marked permanent", err)
	}
}

func TestShellPlugin_CapturesStdoutAndStderrAsArtifacts(t *testing.T) {
	result, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"command":           "printf 'out-bytes'; printf 'err-bytes' 1>&2",
		"capture_stdout_as": "title",
		"capture_stderr_as": "errors",
	}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Artifacts) != 2 {
		t.Fatalf("artifacts = %#v, want stdout and stderr", result.Artifacts)
	}
	want := map[string]string{"title": "out-bytes", "errors": "err-bytes"}
	for _, artifact := range result.Artifacts {
		contents, err := os.ReadFile(artifact.LocalPath)
		if err != nil {
			t.Fatalf("read artifact %q: %v", artifact.Name, err)
		}
		if string(contents) != want[artifact.Name] {
			t.Errorf("artifact %q = %q, want %q", artifact.Name, contents, want[artifact.Name])
		}
	}
}

func TestShellPlugin_CapturePathsGlobsBecomeArtifacts(t *testing.T) {
	result, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"command":       "mkdir -p out && printf 'a' > out/a.txt && printf 'b' > out/b.txt && printf 'c' > skip.md",
		"capture_paths": []any{"out/*.txt"},
	}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	names := make([]string, 0, len(result.Artifacts))
	for _, artifact := range result.Artifacts {
		names = append(names, artifact.Name)
	}
	if len(names) != 2 || names[0] != "out/a.txt" || names[1] != "out/b.txt" {
		t.Fatalf("captured artifacts = %v, want out/a.txt and out/b.txt", names)
	}
	contents, err := os.ReadFile(result.Artifacts[0].LocalPath)
	if err != nil {
		t.Fatalf("read captured file: %v", err)
	}
	if string(contents) != "a" {
		t.Errorf("out/a.txt = %q, want a", contents)
	}
}

func TestShellPlugin_CapturePathsRejectsEscapingPatterns(t *testing.T) {
	for _, pattern := range []string{"../*", "/etc/*"} {
		_, err := New().Execute(context.Background(), execContext(t, map[string]any{
			"command":       "true",
			"capture_paths": []any{pattern},
		}))
		if err == nil {
			t.Fatalf("capture_paths %q succeeded, want a rejection", pattern)
		}
		if !strings.Contains(err.Error(), "work directory") {
			t.Errorf("error %q does not explain the containment rule", err)
		}
	}
}

func TestShellPlugin_PassesConfiguredEnvironmentWithoutInheritingTheOrkerEnv(t *testing.T) {
	t.Setenv("ORK_SHELL_TEST_LEAK", "leaked")

	result, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"command": "echo \"greeting=$GREETING leak=$ORK_SHELL_TEST_LEAK\"",
		"env":     map[string]any{"GREETING": "hello"},
	}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	stdout, _ := result.Output["stdout"].(string)
	if !strings.Contains(stdout, "greeting=hello") {
		t.Errorf("stdout = %q, want the configured env var", stdout)
	}
	if !strings.Contains(stdout, "leak=\n") && !strings.HasSuffix(strings.TrimSpace(stdout), "leak=") {
		t.Errorf("stdout = %q, want the orker's own environment absent", stdout)
	}
}

func TestShellPlugin_TruncatesOutputPayloadButNotTheArtifact(t *testing.T) {
	result, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"command":           "for i in $(seq 1 5000); do printf '0123456789'; done",
		"capture_stdout_as": "full",
	}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	stdout, _ := result.Output["stdout"].(string)
	if len(stdout) != MaxCapturedOutput {
		t.Fatalf("len(stdout) = %d, want the payload truncated to %d", len(stdout), MaxCapturedOutput)
	}
	contents, err := os.ReadFile(result.Artifacts[0].LocalPath)
	if err != nil {
		t.Fatalf("read stdout artifact: %v", err)
	}
	if len(contents) != 50000 {
		t.Fatalf("artifact holds %d bytes, want the full 50000", len(contents))
	}
}

func TestShellPlugin_ConsumesAMaterializedUpstreamArtifactPath(t *testing.T) {
	ec := execContext(t, nil)
	upstream := filepath.Join(ec.WorkDir, ".artifacts", "fetch")
	if err := os.MkdirAll(upstream, 0o755); err != nil {
		t.Fatalf("prepare materialized artifact: %v", err)
	}
	path := filepath.Join(upstream, "body")
	if err := os.WriteFile(path, []byte("{\"title\":\"Sample\"}\n"), 0o644); err != nil {
		t.Fatalf("write materialized artifact: %v", err)
	}
	ec.Config = map[string]any{
		"command":           "cat \"" + path + "\" | tr -d '\\n'",
		"capture_stdout_as": "title",
	}

	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["stdout"] != "{\"title\":\"Sample\"}" {
		t.Fatalf("stdout = %q, want the materialized artifact contents", result.Output["stdout"])
	}
	if result.Artifacts[0].Name != "title" {
		t.Fatalf("artifact name = %q, want title", result.Artifacts[0].Name)
	}
}

func TestShellPlugin_RejectsMissingCommand(t *testing.T) {
	_, err := New().Execute(context.Background(), execContext(t, map[string]any{"command": "  "}))
	if err == nil {
		t.Fatal("Execute succeeded without a command, want an error")
	}
	if !strings.Contains(err.Error(), "`command` is required") {
		t.Errorf("error %q does not explain the missing command", err)
	}
}

// TestShellPlugin_AcceptsEveryDocumentedOp holds the plugin to the op set it
// advertises: exec, eval and script are all accepted, and each behaves
// differently enough to be worth naming.
func TestShellPlugin_AcceptsEveryDocumentedOp(t *testing.T) {
	properties, _ := New().Describe().InputSchema["properties"].(map[string]any)
	opSchema, _ := properties["op"].(map[string]any)
	enum, _ := opSchema["enum"].([]any)
	advertised := make([]string, 0, len(enum))
	for _, value := range enum {
		advertised = append(advertised, fmt.Sprintf("%v", value))
	}
	if got := strings.Join(advertised, ","); got != "exec,eval,script" {
		t.Fatalf("advertised ops = %q, want exec,eval,script", got)
	}
	if opSchema["default"] != "exec" {
		t.Errorf("default op = %v, want exec", opSchema["default"])
	}

	configs := map[string]map[string]any{
		"exec":   {"op": "exec", "command": "echo ran-exec"},
		"eval":   {"op": "eval", "command": "echo ran-eval"},
		"script": {"op": "script", "script": "printf 'ran-%s\\n' script\n"},
	}
	for _, op := range advertised {
		config, ok := configs[op]
		if !ok {
			t.Fatalf("op %q is advertised but this test does not exercise it", op)
		}
		result, err := New().Execute(context.Background(), execContext(t, config))
		if err != nil {
			t.Fatalf("Execute(op=%s): %v", op, err)
		}
		stdout, _ := result.Output["stdout"].(string)
		if !strings.Contains(stdout, "ran-"+op) {
			t.Errorf("op %s stdout = %q, want it to contain ran-%s", op, stdout, op)
		}
		_, hasValue := result.Output["value"]
		_, hasScriptPath := result.Output["script_path"]
		if hasValue != (op == "eval") {
			t.Errorf("op %s: output.value present = %v, want %v", op, hasValue, op == "eval")
		}
		if hasScriptPath != (op == "script") {
			t.Errorf("op %s: output.script_path present = %v, want %v", op, hasScriptPath, op == "script")
		}
	}
}

// TestShellPlugin_OmittedOpDefaultsToExec pins the documented default: no `op`
// is exactly `op: exec`, including its non-zero-exit failure.
func TestShellPlugin_OmittedOpDefaultsToExec(t *testing.T) {
	explicit, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"op":      "exec",
		"command": "echo hello",
	}))
	if err != nil {
		t.Fatalf("Execute(op=exec): %v", err)
	}
	implicit, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"command": "echo hello",
	}))
	if err != nil {
		t.Fatalf("Execute(no op): %v", err)
	}
	if explicit.Output["stdout"] != implicit.Output["stdout"] {
		t.Errorf("stdout differs between op=exec (%v) and no op (%v)",
			explicit.Output["stdout"], implicit.Output["stdout"])
	}
	if _, ok := implicit.Output["value"]; ok {
		t.Error("exec produced a `value` field, which belongs to eval only")
	}
	// The default is exec, not eval: a non-zero exit still fails the task.
	if _, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"command": "exit 5",
	})); err == nil {
		t.Error("Execute(no op) succeeded on exit 5, want the exec default to fail")
	}
}

// TestShellPlugin_EvalReportsTheTrimmedValue treats the command as a question
// whose answer lands on output.value.
func TestShellPlugin_EvalReportsTheTrimmedValue(t *testing.T) {
	result, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"op":      "eval",
		"command": "echo '  42  '",
	}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["value"] != "42" {
		t.Errorf("value = %q, want the trimmed stdout 42", result.Output["value"])
	}
	if result.Output["exit_code"] != float64(0) {
		t.Errorf("exit_code = %v, want 0", result.Output["exit_code"])
	}
}

// TestShellPlugin_EvalReportsANonZeroExitAsDataNotFailure is the behavioral
// difference between eval and the other two ops: exec and script fail the
// task, eval answers it.
func TestShellPlugin_EvalReportsANonZeroExitAsDataNotFailure(t *testing.T) {
	result, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"op":                "eval",
		"command":           "echo missing; exit 7",
		"capture_stdout_as": "answer",
	}))
	if err != nil {
		t.Fatalf("Execute(op=eval) failed on a non-zero exit: %v", err)
	}
	if result.Output["exit_code"] != float64(7) {
		t.Errorf("exit_code = %v, want 7 reported as data", result.Output["exit_code"])
	}
	if result.Output["value"] != "missing" {
		t.Errorf("value = %q, want missing", result.Output["value"])
	}
	if len(result.Artifacts) != 1 || result.Artifacts[0].Name != "answer" {
		t.Errorf("artifacts = %#v, want the captured stdout even after a non-zero exit", result.Artifacts)
	}

	// The same command under exec and under script still fails the task.
	if _, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"op":      "exec",
		"command": "echo missing; exit 7",
	})); err == nil {
		t.Error("Execute(op=exec) succeeded on exit 7, want a failure")
	}
	_, err = New().Execute(context.Background(), execContext(t, map[string]any{
		"op":     "script",
		"script": "echo missing\nexit 7\n",
	}))
	if err == nil {
		t.Fatal("Execute(op=script) succeeded on exit 7, want a failure")
	}
	if !strings.Contains(err.Error(), "exited with code 7") {
		t.Errorf("script error %q does not report the exit code", err)
	}
}

// TestShellPlugin_ScriptRunsAMultiLineProgramFromItsOwnFile is what separates
// script from exec: the body is a program on disk, so it keeps its own $0 and
// its path is reported on the output payload.
func TestShellPlugin_ScriptRunsAMultiLineProgramFromItsOwnFile(t *testing.T) {
	ec := execContext(t, map[string]any{
		"op": "script",
		"script": "set -e\n" +
			"greeting=hello\n" +
			"printf '%s from %s\\n' \"$greeting\" \"$(basename \"$0\")\"\n",
		"capture_stdout_as": "log",
	})
	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute(op=script): %v", err)
	}
	stdout, _ := result.Output["stdout"].(string)
	if !strings.Contains(stdout, "hello from script") {
		t.Errorf("stdout = %q, want the script to run from a file named script", stdout)
	}
	if result.Output["script_path"] != ".ork-shell/script" {
		t.Errorf("script_path = %v, want .ork-shell/script", result.Output["script_path"])
	}
	onDisk, err := os.ReadFile(filepath.Join(ec.WorkDir, ".ork-shell", "script"))
	if err != nil {
		t.Fatalf("read the written script: %v", err)
	}
	if !strings.Contains(string(onDisk), "greeting=hello") {
		t.Errorf("written script = %q, want the configured body", onDisk)
	}
	if len(result.Artifacts) != 1 || result.Artifacts[0].Name != "log" {
		t.Errorf("artifacts = %#v, want the captured stdout", result.Artifacts)
	}
}

// TestShellPlugin_ScriptHonorsItsShebang proves the file is executed, not
// piped through `sh -c`: under `sh` the body below would be a command-not-found
// failure, but its shebang hands it to /bin/cat, which prints it verbatim.
func TestShellPlugin_ScriptHonorsItsShebang(t *testing.T) {
	if _, err := os.Stat("/bin/cat"); err != nil {
		t.Skip("/bin/cat is absent; a shebang needs an interpreter that exists")
	}
	result, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"op":     "script",
		"script": "#!/bin/cat\nthis-line-is-data-not-a-command\n",
	}))
	if err != nil {
		t.Fatalf("Execute(op=script) with a shebang: %v", err)
	}
	stdout, _ := result.Output["stdout"].(string)
	if !strings.Contains(stdout, "#!/bin/cat") || !strings.Contains(stdout, "this-line-is-data-not-a-command") {
		t.Errorf("stdout = %q, want the shebang interpreter to have printed the file", stdout)
	}
}

// TestShellPlugin_ScriptUsesTheConfiguredInterpreter: `interpreter` overrides
// both `sh` and any shebang.
func TestShellPlugin_ScriptUsesTheConfiguredInterpreter(t *testing.T) {
	result, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"op":          "script",
		"interpreter": "cat",
		"script":      "plain text, not a command\n",
	}))
	if err != nil {
		t.Fatalf("Execute(op=script, interpreter=cat): %v", err)
	}
	if stdout, _ := result.Output["stdout"].(string); stdout != "plain text, not a command\n" {
		t.Errorf("stdout = %q, want the body printed by cat", stdout)
	}
}

// TestShellPlugin_ScriptRequiresAScriptBody: script takes `script`, not
// `command`, and says so instead of running something else.
func TestShellPlugin_ScriptRequiresAScriptBody(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	_, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"op":      "script",
		"command": "touch " + marker,
	}))
	if err == nil {
		t.Fatal("Execute(op=script) without a script succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "`script` is required for op script") {
		t.Errorf("error %q does not explain the missing script body", err)
	}
	if _, statErr := os.Stat(marker); statErr == nil {
		t.Error("op=script ran the `command` field instead of failing")
	}
	var retryable *plugin.RetryableError
	if !errors.As(err, &retryable) || retryable.Retryable {
		t.Errorf("error %v is not marked permanent", err)
	}
}

// TestShellPlugin_RejectsUnknownOp is the graceful-unknown-op contract: an
// unrecognized op fails permanently and names the supported ops instead of
// being silently ignored. Every documented op is accepted; only a value that
// is not an op at all is refused.
func TestShellPlugin_RejectsUnknownOp(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	ec := execContext(t, map[string]any{
		"op":      "frobnicate",
		"command": "touch " + marker,
	})
	_, err := New().Execute(context.Background(), ec)
	if err == nil {
		t.Fatal("Execute(op=frobnicate) succeeded, want a permanent error")
	}
	if !strings.Contains(err.Error(), `unknown op "frobnicate"`) {
		t.Errorf("error %q does not name the rejected op", err)
	}
	for _, op := range []string{"exec", "eval", "script"} {
		if !strings.Contains(err.Error(), op) {
			t.Errorf("error %q does not list the supported op %q", err, op)
		}
	}
	if _, statErr := os.Stat(marker); statErr == nil {
		t.Error("the command ran despite an unknown op")
	}
	var retryable *plugin.RetryableError
	if !errors.As(err, &retryable) || retryable.Retryable {
		t.Errorf("error %v is not marked permanent", err)
	}

	// The rejection is narrow: the documented ops still run.
	accepted := filepath.Join(t.TempDir(), "ran")
	if _, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"op":      "exec",
		"command": "touch " + accepted,
	})); err != nil {
		t.Fatalf("Execute(op=exec): %v, want the documented default op to run", err)
	}
	if _, statErr := os.Stat(accepted); statErr != nil {
		t.Errorf("op=exec did not run the command: %v", statErr)
	}
}

func TestShellPlugin_DescribeNamesThePlugin(t *testing.T) {
	p := New()
	if p.Name() != "shell" || p.Describe().Name != "shell" {
		t.Fatalf("plugin name = %q/%q, want shell", p.Name(), p.Describe().Name)
	}
}
