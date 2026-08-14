package shellplugin

import (
	"context"
	"errors"
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

func TestShellPlugin_DescribeNamesThePlugin(t *testing.T) {
	p := New()
	if p.Name() != "shell" || p.Describe().Name != "shell" {
		t.Fatalf("plugin name = %q/%q, want shell", p.Name(), p.Describe().Name)
	}
}
