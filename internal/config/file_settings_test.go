package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestSchema_EverySettingThatNamesAFileIsMarked: a key whose last segment
// names a file (…_file, …_files, a certificate) is marked File, so the
// Orker can hold every file a setting names as its own; a document that
// names files in a field says which.
func TestSchema_EverySettingThatNamesAFileIsMarked(t *testing.T) {
	for _, v := range Schema {
		last := v.Path[strings.LastIndex(v.Path, ".")+1:]
		names := strings.HasSuffix(last, "_file") || strings.HasSuffix(last, "_files") || last == "certificate"
		if names && !v.File {
			t.Errorf("%s names a file and is not marked File", v.Path)
		}
		if v.File && v.Kind != KindString && v.Kind != KindList && v.Kind != KindDocument {
			t.Errorf("%s is marked File with kind %v", v.Path, v.Kind)
		}
		if v.Kind == KindDocument && v.File && v.FileField == "" {
			t.Errorf("%s is a document marked File with no FileField", v.Path)
		}
	}
	issuers, ok := Lookup("ORK_AUTH_ISSUERS")
	if !ok || !issuers.File || issuers.FileField != "jwks_file" {
		t.Fatalf("auth.issuers = %+v", issuers)
	}
}

// TestFileSettings_FromTheEnvironment: every marked setting set in the
// environment answers its file or files, named by setting.
func TestFileSettings_FromTheEnvironment(t *testing.T) {
	env := map[string]string{
		"ORK_AUTH_BREAK_GLASS_KEY_FILE":  "/k/glass.pem",
		"ORK_IDENTITY_SIGNING_KEY_FILES": "/k/a.pem, /k/b.pem",
		"ORK_AUTH_ISSUERS":               "issuers:\n  dev:\n    issuer: dev\n    jwks_file: /k/dev.json\n  corp:\n    issuer: https://c\n",
		"ORK_TEMPORAL_TLS_KEY_FILE":      " /k/temporal.key ",
		"ORK_S3_BUCKET":                  "not-a-file",
	}
	got := FileSettings(func(k string) string { return env[k] })
	var paths []string
	for _, f := range got {
		paths = append(paths, f.Setting+"="+f.Path)
	}
	slices.Sort(paths)
	want := []string{"auth.break_glass_key_file=/k/glass.pem", "auth.issuers=/k/dev.json",
		"identity.signing.key_files=/k/a.pem", "identity.signing.key_files=/k/b.pem", "temporal.tls.key_file=/k/temporal.key"}
	if !slices.Equal(paths, want) {
		t.Fatalf("FileSettings = %v, want %v", paths, want)
	}
	for _, f := range got {
		if f.Setting == "auth.break_glass_key_file" && (!f.EngineOnly || f.Env != "ORK_AUTH_BREAK_GLASS_KEY_FILE") {
			t.Errorf("break-glass: %+v", f)
		}
		if f.Setting == "temporal.tls.key_file" && f.EngineOnly {
			t.Errorf("a setting both binaries read is engine-only: %+v", f)
		}
	}
}

// TestLoad_TheOtherBinarysFilesAreNamed: an Orker given a configuration file
// that holds the engine's key settings is told which files they name,
// though it never parses them as its own.
func TestLoad_TheOtherBinarysFilesAreNamed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(path, []byte(`auth:
  break_glass_key_file: /k/glass.pem
  issuers:
    dev:
      issuer: dev
      jwks_file: /k/dev.json
identity:
  signing:
    key_files:
      - /k/a.pem
      - /k/b.pem
`), 0o600); err != nil {
		t.Fatal(err)
	}
	doc, err := Load(path, BinaryOrker)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, f := range doc.ForeignFiles() {
		if !f.EngineOnly {
			t.Errorf("%+v is not marked engine-only", f)
		}
		paths = append(paths, f.Setting+"="+f.Path)
	}
	slices.Sort(paths)
	want := []string{"auth.break_glass_key_file=/k/glass.pem", "auth.issuers=/k/dev.json",
		"identity.signing.key_files=/k/a.pem", "identity.signing.key_files=/k/b.pem"}
	if !slices.Equal(paths, want) {
		t.Fatalf("ForeignFiles = %v, want %v", paths, want)
	}
	engine, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatal(err)
	}
	if len(engine.ForeignFiles()) != 0 {
		t.Fatalf("the engine's own settings read as foreign: %v", engine.ForeignFiles())
	}
}

// TestForeignFiles_EverySpellingIsResolvedOrSaidUnresolved: the engine's
// key settings in each form the loader accepts name their files to an
// Orker, and a form it cannot resolve is reported, never dropped.
func TestForeignFiles_EverySpellingIsResolvedOrSaidUnresolved(t *testing.T) {
	dir := t.TempDir()
	listFile := filepath.Join(dir, "keys.txt")
	if err := os.WriteFile(listFile, []byte("/k/from-file-a.pem,/k/from-file-b.pem\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path2 := filepath.Join(dir, "cfg2.yaml")
	if err := os.WriteFile(path2, []byte(`auth:
  break_glass_key_file: {ork: /k/glass.pem, orker: /o/ignored.pem}
  issuers:
    base: &base
      issuer: dev
      jwks_file: /k/base.json
    dev:
      <<: *base
    odd:
      issuer: o
      jwks_file: [not, a, path]
identity:
  signing:
    key_files: {file: keys.txt}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	doc, err := Load(path2, BinaryOrker)
	if err != nil {
		t.Fatal(err)
	}
	var paths, unresolved []string
	for _, f := range doc.ForeignFiles() {
		if f.Unresolved != "" {
			unresolved = append(unresolved, f.Setting)
		}
		if f.Path != "" {
			paths = append(paths, f.Setting+"="+f.Path)
		}
	}
	slices.Sort(paths)
	for _, want := range []string{"auth.break_glass_key_file=/k/glass.pem", "auth.issuers=/k/base.json",
		"identity.signing.key_files=" + listFile, "identity.signing.key_files=/k/from-file-a.pem", "identity.signing.key_files=/k/from-file-b.pem"} {
		if !slices.Contains(paths, want) {
			t.Errorf("missing %s in %v", want, paths)
		}
	}
	if slices.Contains(paths, "auth.break_glass_key_file=/o/ignored.pem") {
		t.Error("the Orker's own spelling was taken for the engine's")
	}
	if !slices.Contains(unresolved, "auth.issuers") {
		t.Errorf("a jwks_file that is not a path was not reported: %v", unresolved)
	}
}

// TestFileSettings_AnUnreadableValueIsSaidUnresolved: an environment value
// that does not parse, and a relative path, are reported, not dropped.
func TestFileSettings_AnUnreadableValueIsSaidUnresolved(t *testing.T) {
	env := map[string]string{
		"ORK_AUTH_ISSUERS":              "issuers: [",
		"ORK_AUTH_BREAK_GLASS_KEY_FILE": "keys/glass.pem",
	}
	got := FileSettings(func(k string) string { return env[k] })
	reasons := map[string]string{}
	for _, f := range got {
		reasons[f.Setting] = f.Unresolved
	}
	if !strings.Contains(reasons["auth.issuers"], "parse") {
		t.Errorf("an unparsable document: %q", reasons["auth.issuers"])
	}
	if !strings.Contains(reasons["auth.break_glass_key_file"], "relative") {
		t.Errorf("a relative path: %q", reasons["auth.break_glass_key_file"])
	}
}
