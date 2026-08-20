package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// cliConfig is the small persisted state orkctl keeps between invocations.
//
// It exists for one reason: a namespace is typed on every command, and an
// operator working in `payments` all day should not retype it or export a
// variable in every shell. Only settings a user explicitly asks to persist are
// written — nothing is saved as a side effect of an ordinary command, so the
// file never acquires a value nobody chose.
type cliConfig struct {
	// Namespace is the default `-n` when neither the flag nor the environment
	// names one. Empty means the built-in default namespace.
	Namespace string `json:"namespace,omitempty"`
	// Server is reserved for the same treatment; it is round-tripped rather
	// than dropped so a hand-written file is not silently rewritten smaller.
	Server string `json:"server,omitempty"`
}

// configEnvVar overrides where the config file lives. It exists for tests and
// for a user who keeps their configuration somewhere unusual; the default is
// the platform's own config directory.
const configEnvVar = "ORK_CONFIG"

// namespaceEnvVar is the environment fallback for --namespace.
const namespaceEnvVar = "ORK_NAMESPACE"

// configPath resolves the config file's location.
//
// os.UserConfigDir is the platform's answer (~/.config on Linux,
// ~/Library/Application Support on macOS, %AppData% on Windows), so orkctl
// puts its file where the operating system says configuration goes rather than
// inventing a dotfile.
func configPath() (string, error) {
	if override := os.Getenv(configEnvVar); override != "" {
		return override, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("locate the user config directory: %w", err)
	}
	return filepath.Join(dir, "orkctl", "config.json"), nil
}

// loadConfig reads the config file.
//
// A missing file is the zero config, not an error: orkctl works on a machine
// that has never written one, which is every machine before the first
// `orkctl namespace use`. A file that exists and cannot be parsed IS an error,
// because silently ignoring it would run commands against a namespace the user
// believes they set.
func loadConfig() (cliConfig, error) {
	path, err := configPath()
	if err != nil {
		return cliConfig{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cliConfig{}, nil
		}
		return cliConfig{}, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg cliConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cliConfig{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return cfg, nil
}

// saveConfig writes the config file, creating its directory.
//
// It is written 0600: it is per-user state under the user's own config
// directory, and a future `server` entry may carry an address an operator does
// not want world-readable.
func saveConfig(cfg cliConfig) (string, error) {
	path, err := configPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("create %s: %w", filepath.Dir(path), err)
	}
	encoded, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode config: %w", err)
	}
	if err := os.WriteFile(path, append(encoded, '\n'), 0o600); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return path, nil
}
