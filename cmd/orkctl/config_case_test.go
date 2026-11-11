package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadConfig_RefusesAKeyInAnotherCase: a hand-edited config whose key
// differs from the documented one only in case once set the field; it is
// refused, naming the key and the one it nearly matched, rather than ignored.
func TestLoadConfig_RefusesAKeyInAnotherCase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"Namespace":"prod"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(configEnvVar, path)
	_, err := loadConfig()
	if err == nil || !strings.Contains(err.Error(), `the key "Namespace", which is not one it takes: did you mean "namespace"?`) {
		t.Fatalf("loadConfig = %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"namespace":"prod","namespace":"dev"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), `the key "namespace" appears twice`) {
		t.Fatalf("loadConfig = %v", err)
	}
}
