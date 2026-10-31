package main

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cfgfile "github.com/orkstack/orkstack/internal/config"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/telemetry"
)

// writeTempConfig writes content to a temp file under t.TempDir() and
// returns its path, mirroring internal/config's own writeTemp test helper.
func writeTempConfig(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// t.Setenv-backed setenv, so a value applyConfigFile's Apply call sets is
// automatically unset again when the test ends — the file this test loads
// must never leak an environment variable into any other test in this
// package, and loadConfig() below reads the REAL environment (os.LookupEnv),
// so the value has to land there for that assertion to mean anything.
func setenvT(t *testing.T) func(string, string) error {
	t.Helper()
	return func(key, value string) error {
		t.Setenv(key, value)
		return nil
	}
}

// unsetenvT removes key for the rest of the test and restores it afterwards.
// A test that asserts the FILE supplies a value must not inherit that value
// from the shell running it: AGENTS.md §11.1 has parallel integration runs
// export a unique ORK_TEMPORAL_NAMESPACE, and the environment wins over the
// file by design.
func unsetenvT(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "") // registers the restore of the original value
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unset %s: %v", key, err)
	}
}

func TestApplyConfigFile_FileValueReachesLoadConfig(t *testing.T) {
	unsetenvT(t, "ORK_TEMPORAL_NAMESPACE")
	path := writeTempConfig(t, "cfg.yaml", "temporal:\n  namespace: from-file\n")

	doc, gotPath, check, set, err := applyConfigFile(
		[]string{"--config", path}, cfgfile.BinaryOrker, os.LookupEnv, setenvT(t))
	if err != nil {
		t.Fatalf("applyConfigFile: %v", err)
	}
	if doc == nil || gotPath != path || check {
		t.Fatalf("applyConfigFile = (doc=%v, path=%q, check=%v), want a document, %q, false", doc, gotPath, check, path)
	}
	if len(set) != 1 || set[0] != "ORK_TEMPORAL_NAMESPACE" {
		t.Fatalf("applyConfigFile set = %v, want [ORK_TEMPORAL_NAMESPACE]", set)
	}

	cfg := loadConfig()
	if cfg.temporalNamespace != "from-file" {
		t.Errorf("loadConfig().temporalNamespace = %q, want from-file", cfg.temporalNamespace)
	}
}

func TestApplyConfigFile_EnvironmentWinsOverFile(t *testing.T) {
	t.Setenv("ORK_TEMPORAL_NAMESPACE", "from-env")
	path := writeTempConfig(t, "cfg.yaml", "temporal:\n  namespace: from-file\n")

	_, _, _, set, err := applyConfigFile(
		[]string{"--config", path}, cfgfile.BinaryOrker, os.LookupEnv, setenvT(t))
	if err != nil {
		t.Fatalf("applyConfigFile: %v", err)
	}
	for _, e := range set {
		if e == "ORK_TEMPORAL_NAMESPACE" {
			t.Errorf("applyConfigFile set = %v, ORK_TEMPORAL_NAMESPACE was already in the environment", set)
		}
	}

	cfg := loadConfig()
	if cfg.temporalNamespace != "from-env" {
		t.Errorf("loadConfig().temporalNamespace = %q, want from-env (the pre-set environment value)", cfg.temporalNamespace)
	}
}

func TestApplyConfigFile_PerBinaryAdminAddr(t *testing.T) {
	path := writeTempConfig(t, "cfg.yaml",
		"telemetry:\n  admin_addr: {ork: \"127.0.0.1:9464\", orker: \"127.0.0.1:9465\"}\n")

	_, _, _, _, err := applyConfigFile(
		[]string{"--config", path}, cfgfile.BinaryOrker, os.LookupEnv, setenvT(t))
	if err != nil {
		t.Fatalf("applyConfigFile: %v", err)
	}
	if got, ok := os.LookupEnv("ORK_ADMIN_ADDR"); !ok || got != "127.0.0.1:9465" {
		t.Errorf("ORK_ADMIN_ADDR = %q, %v; want 127.0.0.1:9465, true (this binary's own side of the pair)", got, ok)
	}
}

func TestApplyConfigFile_Check(t *testing.T) {
	t.Setenv("ORK_LOG_FORMAT", "json") // guarantees at least one origin=env line
	path := writeTempConfig(t, "cfg.yaml",
		"storage:\n  s3:\n    secret_key: super-secret\n")

	doc, _, check, set, err := applyConfigFile(
		[]string{"--config", path, "--check"}, cfgfile.BinaryOrker, os.LookupEnv, setenvT(t))
	if err != nil {
		t.Fatalf("applyConfigFile: %v", err)
	}
	if !check || doc == nil {
		t.Fatalf("applyConfigFile = (doc=%v, check=%v), want a document and check=true", doc, check)
	}
	if set != nil {
		t.Errorf("applyConfigFile set = %v, --check must not apply anything", set)
	}

	out := doc.Effective(cfgfile.BinaryOrker, os.LookupEnv)
	if strings.Contains(out, "super-secret") {
		t.Errorf("--check output leaks the secret value:\n%s", out)
	}
	for _, want := range []string{"***", "origin=file", "origin=env", "origin=default"} {
		if !strings.Contains(out, want) {
			t.Errorf("--check output missing %q:\n%s", want, out)
		}
	}
}

func TestApplyConfigFile_CheckWithoutConfigIsAUsageError(t *testing.T) {
	doc, path, check, _, err := applyConfigFile(
		[]string{"--check"}, cfgfile.BinaryOrker, func(string) (string, bool) { return "", false }, setenvT(t))
	if err == nil {
		t.Fatal("applyConfigFile: want an error for --check with no --config/ORK_CONFIG_FILE")
	}
	if doc != nil || path != "" || !check {
		t.Errorf("applyConfigFile = (doc=%v, path=%q, check=%v), want (nil, \"\", true)", doc, path, check)
	}
}

func TestApplyConfigFile_BadFileNamesTheKey(t *testing.T) {
	path := writeTempConfig(t, "cfg.yaml", "not_a_real_key: 1\n")

	_, _, _, _, err := applyConfigFile(
		[]string{"--config", path}, cfgfile.BinaryOrker, os.LookupEnv, setenvT(t))
	if err == nil {
		t.Fatal("applyConfigFile: want an error for an unknown key")
	}
	if !strings.Contains(err.Error(), "not_a_real_key") {
		t.Errorf("applyConfigFile error = %q, want it to name not_a_real_key", err.Error())
	}
}

func TestApplyConfigFile_NoConfigIsTheZeroConfigPath(t *testing.T) {
	doc, path, check, set, err := applyConfigFile(
		nil, cfgfile.BinaryOrker, func(string) (string, bool) { return "", false }, setenvT(t))
	if err != nil || doc != nil || path != "" || check || set != nil {
		t.Errorf("applyConfigFile(nil args) = (doc=%v, path=%q, check=%v, set=%v, err=%v), want the zero-config path", doc, path, check, set, err)
	}
}

// TestOrkerOptions_TheConfigurationsFilesAreTheOrkersOwn: the file this Orker
// was configured from, every {file:} that file read, and every telemetry
// sink header's file and variable are the Orker's own, so its secret floor
// admits none of them as a task's source, whatever ORK_ORKER_SECRET_ALLOW
// says. The Orker is booted by start, main's whole body, given the arguments
// main is given, and judged by the options built from what start serves, so
// the files are the Orker's own on the path a real boot takes; --check stops
// start before it serves anything.
func TestOrkerOptions_TheConfigurationsFilesAreTheOrkersOwn(t *testing.T) {
	clearTelemetryEnv(t)
	unsetenvT(t, "ORK_S3_SECRET_KEY")
	dir := t.TempDir()
	secret := filepath.Join(dir, "s3-secret")
	sinkKey := filepath.Join(dir, "sink-key")
	sibling := filepath.Join(dir, "task-secret")
	for _, path := range []string{secret, sinkKey, sibling} {
		if err := os.WriteFile(path, []byte("x\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	path := writeTempConfig(t, "cfg.yaml", "storage:\n  s3:\n    secret_key: {file: "+secret+"}\n")
	t.Setenv(telemetry.SinksVar, "sinks:\n  vendor:\n    endpoint: http://127.0.0.1:1\n    headers:\n"+
		"      X-Key: { source: \"file:"+sinkKey+"\" }\n"+
		"      X-Team: { source: \"env:VENDOR_TEAM_KEY\" }\n")
	t.Setenv("VENDOR_TEAM_KEY", "x")
	var opts orker.Options
	served := 0
	serve := func(logger *slog.Logger, tcfg telemetry.Config, sinks *telemetry.Sinks, cfg config) error {
		served++
		opts = orkerOptions(cfg, tcfg, sinks, nil, orker.LogOptions{}, nil, nil, nil, logger)
		return nil
	}
	var stderr strings.Builder
	if exit := start([]string{"--config", path, "--check"}, os.LookupEnv, setenvT(t), io.Discard, &stderr, serve); exit != 0 || served != 0 {
		t.Fatalf("start(--check) = %d having served %d times, want 0 and nothing served: %s", exit, served, stderr.String())
	}
	if exit := start([]string{"--config", path}, os.LookupEnv, setenvT(t), io.Discard, &stderr, serve); exit != 0 || served != 1 {
		t.Fatalf("start = %d having served %d times, want 0 having served once: %s", exit, served, stderr.String())
	}
	if opts.Policy == nil {
		t.Fatal("orkerOptions.Policy = nil, want the Orker's policy with its own files")
	}
	for _, own := range []string{path, secret, sinkKey} {
		var refusal *orker.OwnSourceRefusal
		if err := opts.Policy.AllowsSecretSource("file:" + own); !errors.As(err, &refusal) {
			t.Errorf("AllowsSecretSource(file:%s) = %v, want it refused as the Orker's own", own, err)
		}
	}
	var refusal *orker.OwnSourceRefusal
	if err := opts.Policy.AllowsSecretSource("env:VENDOR_TEAM_KEY"); !errors.As(err, &refusal) {
		t.Errorf("AllowsSecretSource(env:VENDOR_TEAM_KEY) = %v, want the sink's header variable refused as the Orker's own", err)
	}
	if err := opts.Policy.AllowsSecretSource("file:" + sibling); err != nil {
		t.Errorf("AllowsSecretSource(file:%s) = %v, want a file beside them permitted", sibling, err)
	}
}
