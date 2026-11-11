//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const threeAwaitsOrkflow = `apiVersion: orkstack/v1
kind: Orkflow
name: values-in-run-get
version: 1.0.0
tasks:
  - id: short
    await:
      timeout_seconds: 300
  - id: long
    await:
      timeout_seconds: 300
  - id: large
    await:
      timeout_seconds: 300
`

// savedByCommand finds the command a task's VALUES row names.
var savedByCommand = regexp.MustCompile(`orkctl (.*) saves them`)

// TestIntegration_RunGetShowsValuesAndTheCommandsThatSaveThem: a real engine's
// run reads in the human output, and the command each row prints is exact: run
// as printed, it saves the bytes the task was resolved with.
func TestIntegration_RunGetShowsValuesAndTheCommandsThatSaveThem(t *testing.T) {
	c := newAwaitCLI(t, 1, false)
	document := filepath.Join(t.TempDir(), "values.ork.yaml")
	if err := os.WriteFile(document, []byte(threeAwaitsOrkflow), 0o644); err != nil {
		t.Fatal(err)
	}
	c.must(0, "orkflow", "create", document)
	var submitted submitJSON
	if err := json.Unmarshal([]byte(c.must(0, "run", "submit", "values-in-run-get", "--json")), &submitted); err != nil {
		t.Fatalf("decode submit: %v", err)
	}
	runID := submitted.Run.ID

	bodies := map[string]string{
		"short": `{"approved":true}`,
		"long":  `{"note":"` + strings.Repeat("n", 200) + `"}`,
		"large": `{"note":"` + strings.Repeat("l", 5000) + `"}`,
	}
	for task, body := range bodies {
		c.waitAwaiting(0, runID, task)
		c.must(0, "run", "resolve", runID, "--task", task, "--values", body)
	}
	if _, stderr, code := c.on(0, "run", "watch", runID); code != 0 {
		t.Fatalf("run watch exit %d: %s", code, stderr)
	}

	rows := map[string]string{}
	human := c.must(0, "run", "get", runID)
	table := human[strings.LastIndex(human, "\nTASK "):]
	for _, line := range strings.Split(table, "\n") {
		if task, cell, ok := strings.Cut(line, " "); ok {
			rows[task] = strings.TrimSpace(cell)
		}
	}
	if rows["short"] != bodies["short"] {
		t.Fatalf("short's row = %q, want %q, in\n%s", rows["short"], bodies["short"], human)
	}
	if want := `{"note":"` + strings.Repeat("n", 55) + "…"; !strings.HasPrefix(rows["long"], want) {
		t.Fatalf("long's row = %q, want the first 64 characters, in\n%s", rows["long"], human)
	}
	if !strings.HasPrefix(rows["large"], "not shown (5011 bytes; ") {
		t.Fatalf("large's row = %q, want it not fetched, in\n%s", rows["large"], human)
	}

	for _, task := range []string{"long", "large"} {
		match := savedByCommand.FindStringSubmatch(rows[task])
		if match == nil {
			t.Fatalf("%s's row names no command: %q", task, rows[task])
		}
		args := strings.Fields(match[1])
		// Only the destination changes, to a directory the test owns.
		saved := filepath.Join(t.TempDir(), args[len(args)-1])
		c.must(0, append(args[:len(args)-1], saved)...)
		got, err := os.ReadFile(saved)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, []byte(bodies[task])) {
			t.Fatalf("%s saved %d bytes, want the %d it was resolved with", task, len(got), len(bodies[task]))
		}
	}
}

// valuesChildOrkflow returns one long value from a real container.
func valuesChildOrkflow(name string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: 1.0.0
tasks:
  - id: sign
    uses: json
    network: none
    with:
      op: query
      nullInput: true
      filter: '"%s"'
`, name, strings.Repeat("n", 200))
}

// valuesParentOrkflow composes that child beside a fan-out whose items each
// return their part.
func valuesParentOrkflow(name, child string) string {
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: 1.0.0
inputs:
  parts:
    type: array
tasks:
  - id: release
    uses: orkflow:%s@1.0.0
  - id: convert
    uses: json
    network: none
    each:
      over: '{{ .inputs.parts }}'
      as: part
      max_items: 10
    with:
      op: query
      nullInput: true
      filter: $vars.part
      vars:
        part: '{{ .each.part.name }}'
`, name, child)
}

// savedResult is the `result` of the values.json a command saved.
func savedResult(t *testing.T, saved []byte) string {
	t.Helper()
	var values struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(saved, &values); err != nil {
		t.Fatalf("saved values %q: %v", saved, err)
	}
	return values.Result
}

// runSavedCommand runs the command a row names, as printed but for where it
// saves, and returns what it saved.
func runSavedCommand(t *testing.T, s *timingStack, row string) []byte {
	t.Helper()
	match := savedByCommand.FindStringSubmatch(row)
	if match == nil {
		t.Fatalf("the row names no command: %q", row)
	}
	args := strings.Fields(match[1])
	saved := filepath.Join(t.TempDir(), args[len(args)-1])
	s.mustCLI(0, append(args[:len(args)-1], saved)...)
	got, err := os.ReadFile(saved)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// rowOf finds the row of a VALUES table that begins with name, after header.
func rowOf(t *testing.T, output, header, name string) string {
	t.Helper()
	_, table, found := strings.Cut(output, header)
	if !found {
		t.Fatalf("no %q table in\n%s", header, output)
	}
	for _, line := range strings.Split(table, "\n") {
		if cell, ok := strings.CutPrefix(strings.TrimLeft(line, " "), name+" "); ok {
			return strings.TrimSpace(cell)
		}
	}
	t.Fatalf("no row %q under %q in\n%s", name, header, output)
	return ""
}

// TestIntegration_ExpandAndTreeShowValues: real containers' values read under
// --expand, from the store, and under --tree, from the child run; and each
// printed command, run as printed, saves the bytes the task wrote.
func TestIntegration_ExpandAndTreeShowValues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 1)
	s.startOrker()
	stamp := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	child, parent := "values-child-"+stamp, "values-parent-"+stamp
	s.apply(valuesChildOrkflow(child))
	s.apply(valuesParentOrkflow(parent, child))

	longPart := strings.Repeat("p", 100)
	input := fmt.Sprintf(`{"parts":[{"name":"a"},{"name":%q}]}`, longPart)
	runID := s.submit(parent, "--input-json", input)
	if run := awaitTerminalOnB(ctx, t, s.replicas[0], runID); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s finished %s: %v", runID, run.GetStatus(), run.GetError())
	}

	expanded := s.mustCLI(0, "run", "get", runID, "--expand", "convert")
	if got := rowOf(t, expanded, "ITEM  VALUES", "0"); got != `{"result":"a"}` {
		t.Fatalf("item 0's row = %q, in\n%s", got, expanded)
	}
	longRow := rowOf(t, expanded, "ITEM  VALUES", "1")
	if want := `{"result":"` + strings.Repeat("p", 53) + "…"; !strings.HasPrefix(longRow, want) {
		t.Fatalf("item 1's row = %q, want the first 64 characters", longRow)
	}
	if !strings.Contains(longRow, "--item 1") {
		t.Fatalf("item 1's row names no item: %q", longRow)
	}
	if got := savedResult(t, runSavedCommand(t, s, longRow)); got != longPart {
		t.Fatalf("item 1 saved %q, want %q", got, longPart)
	}

	plain := s.mustCLI(0, "run", "get", runID)
	if strings.Contains(plain, "ITEM  VALUES") {
		t.Fatalf("run get with no --expand printed item values:\n%s", plain)
	}

	tree := s.mustCLI(0, "run", "get", runID, "--tree")
	_, childPart, found := strings.Cut(tree, "release → ")
	if !found {
		t.Fatalf("no child in\n%s", tree)
	}
	signRow := rowOf(t, childPart, "TASK  VALUES", "sign")
	childID := strings.Fields(childPart)[0]
	if !strings.Contains(signRow, " "+childID+" ") {
		t.Fatalf("sign's row names no child run %s: %q", childID, signRow)
	}
	if got, want := savedResult(t, runSavedCommand(t, s, signRow)), strings.Repeat("n", 200); got != want {
		t.Fatalf("sign saved %q, want %q", got, want)
	}
}
