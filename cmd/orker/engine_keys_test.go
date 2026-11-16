package main

import (
	"bytes"
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
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestSchema_EveryFileSettingIsAnOrkersOwn: every setting the schema marks
// as naming a file, whichever binary reads it, is the Orker's own when its
// environment names the file, and an engine's key settings are too when
// the configuration file it was given names them: no task's secret can
// name one, whatever ORK_ORKER_SECRET_ALLOW says (docs/SECURITY_MODEL.md,
// "The Orker's own sources").
func TestSchema_EveryFileSettingIsAnOrkersOwn(t *testing.T) {
	clearTelemetryEnv(t)
	dir := t.TempDir()
	var fromEnv []string
	for _, v := range cfgfile.Schema {
		if !v.File {
			continue
		}
		path := filepath.Join(dir, strings.ReplaceAll(v.Path, ".", "-"))
		fromEnv = append(fromEnv, path)
		switch v.Kind {
		case cfgfile.KindList:
			second := path + "-2"
			fromEnv = append(fromEnv, second)
			t.Setenv(v.Env, path+","+second)
		case cfgfile.KindDocument:
			t.Setenv(v.Env, "issuers:\n  dev:\n    issuer: dev\n    "+v.FileField+": "+path+"\n")
		default:
			t.Setenv(v.Env, path)
		}
	}
	if len(fromEnv) < 15 {
		t.Fatalf("only %d marked files; the schema lost its marks", len(fromEnv))
	}
	// Settings the environment does not name, named by the file instead.
	glass, keyA, jwks := filepath.Join(dir, "file-glass.pem"), filepath.Join(dir, "file-key.pem"), filepath.Join(dir, "file-jwks.json")
	cfgPath := writeTempConfig(t, "cfg.yaml", "auth:\n  break_glass_key_file: "+glass+"\n  issuers:\n    dev:\n      issuer: dev\n      jwks_file: "+jwks+
		"\nidentity:\n  signing:\n    key_files:\n      - "+keyA+"\n")
	// The file's engine settings must not be overridden by the environment
	// for this half of the test.
	for _, env := range []string{"ORK_AUTH_BREAK_GLASS_KEY_FILE", "ORK_AUTH_ISSUERS", "ORK_IDENTITY_SIGNING_KEY_FILES"} {
		v, _ := os.LookupEnv(env)
		t.Setenv(env, v)
	}
	var opts orker.Options
	serve := func(logger *slog.Logger, tcfg telemetry.Config, sinks *telemetry.Sinks, cfg config) error {
		opts = orkerOptions(cfg, tcfg, sinks, nil, orker.LogOptions{}, nil, nil, nil, logger)
		return nil
	}
	var stderr strings.Builder
	unsetenvT(t, orker.SecretAllowVar) // an empty floor: only the own-source rule refuses
	if exit := start([]string{"--config", cfgPath}, os.LookupEnv, setenvT(t), io.Discard, &stderr, serve); exit != 0 {
		t.Fatalf("start = %d: %s", exit, stderr.String())
	}
	for _, own := range append(fromEnv, glass, keyA, jwks) {
		var refusal *orker.OwnSourceRefusal
		if err := opts.Policy.AllowsSecretSource("file:" + own); !errors.As(err, &refusal) {
			t.Errorf("AllowsSecretSource(file:%s) = %v, want it refused as the Orker's own", own, err)
		}
	}
	if err := opts.Policy.AllowsSecretSource("file:" + filepath.Join(dir, "a-task-secret")); err != nil {
		t.Errorf("a file beside them was refused: %v", err)
	}
}

// TestCheckEngineKeyFiles: an engine key file this Orker can open is an
// error in its log, and refuses the boot with
// orker.refuse_readable_engine_keys; one it cannot open, and a file of a
// setting the Orker itself reads, say nothing.
func TestCheckEngineKeyFiles(t *testing.T) {
	dir := t.TempDir()
	readable := filepath.Join(dir, "engine.pem")
	must.OK(t, os.WriteFile(readable, []byte("x"), 0o600))
	files := []cfgfile.FileSetting{
		{Setting: "identity.signing.key_files", Env: "ORK_IDENTITY_SIGNING_KEY_FILES", Path: readable, EngineOnly: true},
		{Setting: "auth.break_glass_key_file", Env: "ORK_AUTH_BREAK_GLASS_KEY_FILE", Path: filepath.Join(dir, "missing.pem"), EngineOnly: true},
		{Setting: "temporal.tls.key_file", Env: "ORK_TEMPORAL_TLS_KEY_FILE", Path: readable},
	}
	var log bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&log, nil))
	if err := checkEngineKeyFiles(files, false, logger); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(log.String(), "level=ERROR") || !strings.Contains(log.String(), "identity.signing.key_files") ||
		strings.Contains(log.String(), "break_glass") || strings.Contains(log.String(), "temporal.tls") {
		t.Fatalf("log: %s", log.String())
	}
	err := checkEngineKeyFiles(files, true, logger)
	if err == nil || !strings.Contains(err.Error(), "identity.signing.key_files") || !strings.Contains(err.Error(), orker.RefuseReadableEngineKeysVar) {
		t.Fatalf("with refuse on: %v", err)
	}
	if err := checkEngineKeyFiles(files[1:], true, logger); err != nil {
		t.Fatalf("nothing readable: %v", err)
	}
	unresolved := []cfgfile.FileSetting{{Setting: "auth.issuers", Env: "ORK_AUTH_ISSUERS", EngineOnly: true, Unresolved: "ORK_AUTH_ISSUERS does not parse"}}
	log.Reset()
	if err := checkEngineKeyFiles(unresolved, false, logger); err != nil || !strings.Contains(log.String(), "cannot tell") {
		t.Fatalf("an unresolved setting: %v, %s", err, log.String())
	}
	if err := checkEngineKeyFiles(unresolved, true, logger); err == nil || !strings.Contains(err.Error(), "auth.issuers") {
		t.Fatalf("an unresolved setting with refuse on: %v", err)
	}
}
