package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	cfgfile "github.com/orkstack/orkstack/internal/config"
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

func TestApplyConfigFile_FileValueReachesLoadConfig(t *testing.T) {
	// The file's value only lands when the variable is unset, so a caller's
	// own ORK_TEMPORAL_NAMESPACE (a proof run's private namespace, AGENTS.md
	// §11.1) is hidden for this test rather than silently winning.
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

// unsetenvT removes key from the environment for the rest of the test and
// restores its previous value afterwards (t.Setenv records the restore).
func unsetenvT(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, os.Getenv(key))
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unset %s: %v", key, err)
	}
}
