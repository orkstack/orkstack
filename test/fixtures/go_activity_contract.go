//go:build ignore

// go_activity_contract.go — a complete OrkStack activity, in Go, with no SDK.
//
// This is a REFERENCE IMPLEMENTATION and an optional companion to the
// mandatory Bash fixture beside it (§10.3). It exists to show that the ABI is
// the same in a compiled language as it is in a shell: read files, write
// files, exit. Nothing here imports OrkStack, and nothing here could — the
// module it would import does not exist.
//
// Build it into any image you like:
//
//	go build -o /usr/local/bin/activity go_activity_contract.go
//
// Run it under the contract suite:
//
//	orkctl activity test <your-image>
//
// THE WHOLE ABI, in the order it is used:
//
//	/orkstack/input/task.json     who you are, what your limits are (§3.2)
//	/orkstack/input/config.json   your rendered `with:` block   (§3.3)
//	/orkstack/output/values.json  your structured result        (§3.4)
//	/orkstack/output/artifacts/   any regular file you write    (§3.5)
//	stdout / stderr               two independent logs          (§3.6)
//	exit status                   0 succeeded, anything else failed
//
// There is no callback, no registration, no heartbeat to implement and no
// environment variable to read. Cancellation is an ordinary SIGTERM.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

const (
	inputDir  = "/orkstack/input"
	outputDir = "/orkstack/output"
)

// task is the subset of task.json this fixture reads. It is a SUBSET on
// purpose: an activity should decode the fields it uses and ignore the rest,
// so a platform that adds a field does not break an image that never wanted it.
type task struct {
	SchemaVersion  int    `json:"schemaVersion"`
	IdempotencyKey string `json:"idempotencyKey"`
	Run            struct {
		ID        string `json:"id"`
		Namespace string `json:"namespace"`
	} `json:"run"`
	Task struct {
		ID      string `json:"id"`
		Attempt int32  `json:"attempt"`
	} `json:"task"`
	Limits struct {
		ValuesMaxBytes                 int64 `json:"valuesMaxBytes"`
		CancellationGracePeriodSeconds int32 `json:"cancellationGracePeriodSeconds"`
	} `json:"limits"`
}

func main() {
	// Cancellation is an ordinary Unix signal. Handling it is optional; if you
	// do nothing at all, the Orker's SIGTERM -> grace -> SIGKILL sequence still
	// stops you correctly. What you must NOT do is exit before writing
	// values.json if you intend to report a result.
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, syscall.SIGTERM, syscall.SIGINT)

	meta, err := readTask()
	if err != nil {
		// stderr is a log, not a protocol. Plain text is enough.
		fmt.Fprintf(os.Stderr, "activity: %v\n", err)
		os.Exit(1)
	}
	config, err := readConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "activity: %v\n", err)
		os.Exit(1)
	}

	// stdout reaches the live stream AND the durable stdout.log. No logging
	// SDK, no structured format required.
	fmt.Printf("activity: run=%s task=%s attempt=%d\n",
		meta.Run.ID, meta.Task.ID, meta.Task.Attempt)

	// The work. Everything above and below is the contract; this is the part
	// that would differ in a real image.
	message, _ := config["message"].(string)
	if message == "" {
		message = "hello from a Go activity"
	}

	select {
	case sig := <-terminate:
		// Cleanly abandoning the work is a valid response to cancellation.
		fmt.Printf("activity: received %v, stopping\n", sig)
	case <-time.After(10 * time.Millisecond):
	}

	// An artifact is any REGULAR FILE written here. The name is the file name,
	// exactly. Directories, symlinks, devices, sockets and FIFOs are refused
	// at the scan (§3.5) — archive a hierarchy yourself as a .tar.gz.
	artifact := filepath.Join(outputDir, "artifacts", "message.txt")
	if err := os.WriteFile(artifact, []byte(message+"\n"), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "activity: write artifact: %v\n", err)
		os.Exit(1)
	}

	// values.json is the compact structured result. It must be a JSON OBJECT
	// and must fit inside limits.valuesMaxBytes. Large data belongs in an
	// artifact; this is metadata about what happened.
	if err := writeValues(map[string]any{
		"message":        message,
		"idempotencyKey": meta.IdempotencyKey,
		"bytes":          len(message) + 1,
	}, meta.Limits.ValuesMaxBytes); err != nil {
		fmt.Fprintf(os.Stderr, "activity: %v\n", err)
		os.Exit(1)
	}

	// Exit 0 means the computation succeeded. The Orker then validates and
	// commits the outputs. There are no magic exit codes: any non-zero status
	// fails the activity and Temporal's retry policy decides what follows.
	os.Exit(0)
}

func readTask() (*task, error) {
	raw, err := os.ReadFile(filepath.Join(inputDir, "task.json"))
	if err != nil {
		return nil, fmt.Errorf("read task.json: %w", err)
	}
	var meta task
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, fmt.Errorf("decode task.json: %w", err)
	}
	return &meta, nil
}

// readConfig reads the rendered `with:` block. An absent file is an empty
// config rather than an error: a task with no `with:` is perfectly ordinary.
func readConfig() (map[string]any, error) {
	raw, err := os.ReadFile(filepath.Join(inputDir, "config.json"))
	if os.IsNotExist(err) {
		return map[string]any{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read config.json: %w", err)
	}
	var config map[string]any
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil, fmt.Errorf("decode config.json: %w", err)
	}
	return config, nil
}

// writeValues writes values.json, checking the limit BEFORE writing.
//
// Checking first matters: a values.json over the limit is a
// ContractViolation, which does not retry. Discovering that after writing a
// truncated file would turn a clear refusal into a corrupt result.
func writeValues(values map[string]any, maxBytes int64) error {
	encoded, err := json.Marshal(values)
	if err != nil {
		return fmt.Errorf("encode values: %w", err)
	}
	if maxBytes > 0 && int64(len(encoded)) > maxBytes {
		return fmt.Errorf("values.json is %d bytes, over the %d-byte limit; "+
			"put large data in an artifact instead", len(encoded), maxBytes)
	}
	return os.WriteFile(filepath.Join(outputDir, "values.json"), encoded, 0o644)
}
