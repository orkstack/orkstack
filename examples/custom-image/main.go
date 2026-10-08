// Command report is examples/custom-image: a user's own activity image, with
// no catalog name and nothing of OrkStack's imported. It reads the files the
// platform writes, writes the files it reads (docs/OCI_ACTIVITY_CONTRACT.md),
// and exits with an ordinary Unix status. examples/custom-image-shell is the
// same activity in POSIX shell: one configuration, byte-identical reports.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
)

const (
	image                  = "custom-image"
	workspace              = "/orkstack"
	supportedSchemaVersion = 1
	defaultArtifactName    = "report.txt"
	maxArtifactNameBytes   = 128
)

// taskContext is the part of task.json this image reads; an unknown key is
// ignored.
type taskContext struct {
	SchemaVersion  int    `json:"schemaVersion"`
	IdempotencyKey string `json:"idempotencyKey"`
	Task           struct {
		ID      string `json:"id"`
		Attempt int    `json:"attempt"`
	} `json:"task"`
	Each *eachContext `json:"each"`
}

// eachContext is task.json's `each` object, present only for one item of a
// fan-out. Item is kept as its exact JSON text.
type eachContext struct {
	Index int             `json:"index"`
	Count int             `json:"count"`
	Item  json.RawMessage `json:"item"`
}

// config is the task's rendered `with:` block. There is no `op`: that is a
// convention of the first-party images, and this image has one job.
type config struct {
	Label string `json:"label"`
	Src   string `json:"src"`
	As    string `json:"as"`
}

type textStats struct {
	Lines int `json:"lines"`
	Words int `json:"words"`
	Bytes int `json:"bytes"`
}

type values struct {
	IdempotencyKey string       `json:"idempotencyKey"`
	Attempt        int          `json:"attempt"`
	TaskID         string       `json:"taskId"`
	Argv           []string     `json:"argv"`
	Label          string       `json:"label,omitempty"`
	Source         string       `json:"source,omitempty"`
	Artifact       string       `json:"artifact,omitempty"`
	Stats          *textStats   `json:"stats,omitempty"`
	Each           *eachContext `json:"each,omitempty"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", image, err)
		os.Exit(1)
	}
}

func run() error {
	// The Orker may run as another user: everything written must be readable.
	syscall.Umask(0o022)

	var task taskContext
	if err := readJSON(workspace+"/input/task.json", &task); err != nil {
		return err
	}
	if task.SchemaVersion != supportedSchemaVersion {
		return fmt.Errorf("task.json has schemaVersion %d; this image understands %d", task.SchemaVersion, supportedSchemaVersion)
	}
	if task.IdempotencyKey == "" || task.Task.ID == "" {
		return errors.New("task.json has no idempotencyKey or task.id")
	}
	var cfg config
	if err := readJSON(workspace+"/input/config.json", &cfg); err != nil {
		return err
	}

	// The task's command:, which the Orker passes verbatim and never renders.
	argv := os.Args[1:]
	if len(argv) == 0 {
		argv = []string{}
	}
	out := values{
		IdempotencyKey: task.IdempotencyKey,
		Attempt:        task.Task.Attempt,
		TaskID:         task.Task.ID,
		Argv:           argv,
		Label:          labelOf(cfg, task.Each),
		Each:           task.Each,
	}

	if cfg.Src != "" {
		source, err := resolveRead(cfg.Src, workspace)
		if err != nil {
			return err
		}
		name := cfg.As
		if name == "" {
			name = defaultArtifactName
		}
		if err := checkArtifactName(name); err != nil {
			return err
		}
		data, err := os.ReadFile(source)
		if err != nil {
			return fmt.Errorf("read %s: %w", source, err)
		}
		stats := countText(data)
		argvJSON, err := compactJSON(argv)
		if err != nil {
			return err
		}
		report := fmt.Sprintf("activity report\nlabel:   %s\nsource:  %s\nargv:    %s\nlines:   %d\nwords:   %d\nbytes:   %d\n",
			out.Label, source, argvJSON, stats.Lines, stats.Words, stats.Bytes)
		if err := os.WriteFile(workspace+"/output/artifacts/"+name, []byte(report), 0o644); err != nil {
			return fmt.Errorf("write artifact %s: %w", name, err)
		}
		out.Source, out.Artifact, out.Stats = source, name, &stats
	}

	encoded, err := compactJSON(out)
	if err != nil {
		return err
	}
	if err := os.WriteFile(workspace+"/output/values.json", []byte(encoded+"\n"), 0o644); err != nil {
		return fmt.Errorf("write values.json: %w", err)
	}
	fmt.Printf("%s: task %s attempt %d wrote values.json\n", image, out.TaskID, out.Attempt)
	return nil
}

func readJSON(path string, into any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

// compactJSON encodes v on one line without HTML escaping, so `<`, `>` and
// `&` in an argument read as themselves, as jq -c prints them.
func compactJSON(v any) (string, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(v); err != nil {
		return "", fmt.Errorf("encode JSON: %w", err)
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
}

// labelOf is `with.label` when set; otherwise a fan-out item that is a JSON
// string; otherwise none.
func labelOf(cfg config, each *eachContext) string {
	if cfg.Label != "" {
		return cfg.Label
	}
	if each != nil {
		var s string
		if err := json.Unmarshal(each.Item, &s); err == nil {
			return s
		}
	}
	return ""
}

// resolveRead applies activity-file's path rules: a relative path resolves
// under <workspace>/input, an absolute one must be inside the workspace, a
// `..` component is refused (never normalized), and the result must be a
// regular file — a symbolic link is refused, not followed.
func resolveRead(raw, workspace string) (string, error) {
	if raw == "" {
		return "", errors.New("empty source path")
	}
	resolved := raw
	if !strings.HasPrefix(raw, "/") {
		resolved = workspace + "/input/" + raw
	}
	for _, segment := range strings.Split(resolved, "/") {
		if segment == ".." {
			return "", fmt.Errorf("source path may not contain '..': %s", raw)
		}
	}
	if !strings.HasPrefix(resolved, workspace+"/") {
		return "", fmt.Errorf("source path must be inside %s: %s", workspace, raw)
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		return "", fmt.Errorf("source path %s: %w", raw, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("not a regular file: %s", raw)
	}
	return resolved, nil
}

// checkArtifactName enforces the contract's artifact-name rules.
func checkArtifactName(name string) error {
	switch {
	case name == "" || name == "." || name == "..":
		return fmt.Errorf("artifact name may not be %q", name)
	case strings.Contains(name, "/"):
		return fmt.Errorf("artifact name may not contain '/': %s", name)
	case len(name) > maxArtifactNameBytes:
		return fmt.Errorf("artifact name is %d bytes, limit is %d", len(name), maxArtifactNameBytes)
	}
	return nil
}

// countText counts what a reader counts: every `\n`, plus one for a last line
// without one; words as runs of bytes that are not ASCII whitespace.
func countText(data []byte) textStats {
	stats := textStats{Bytes: len(data), Lines: bytes.Count(data, []byte("\n"))}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		stats.Lines++
	}
	inWord := false
	for _, b := range data {
		space := b == ' ' || b == '\t' || b == '\n' || b == '\v' || b == '\f' || b == '\r'
		if !space && !inWord {
			stats.Words++
		}
		inWord = !space
	}
	return stats
}
