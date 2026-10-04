package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// childLabel is a composed task's pinned child, "name@version".
func childLabel(child *orkstackv1.TaskChild) string {
	return child.GetOrkflowName() + "@" + child.GetOrkflowVersion()
}

// composedLine is the one line `run get` prints under the task table for a
// composed task: the child run and the command that opens it, or why there is
// none.
func composedLine(namespace string, taskRun *orkstackv1.TaskRun) string {
	child := taskRun.GetChild()
	id := taskRun.GetTaskId()
	if runID := child.GetRunId(); runID != "" {
		return fmt.Sprintf("%s: child run %s (%s). Open it with: %s",
			id, runID, childLabel(child), orkctlCommand(namespace, "run", "get", runID))
	}
	switch taskRun.GetStatus() {
	case orkstackv1.Status_STATUS_SKIPPED:
		return fmt.Sprintf("%s: skipped; no child run was started", id)
	case orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_CANCELED:
		return fmt.Sprintf("%s: no child run was started (%s)", id, orDash(taskRun.GetError().GetCode()))
	default:
		return fmt.Sprintf("%s: will run %s as a child run", id, childLabel(child))
	}
}

// childReader reads one child run in the parent's namespace.
type childReader func(runID, namespace string) (*orkstackv1.Run, error)

// renderRunTree appends, depth first in declaration order, one block per child
// run of run: a heading and the child's task table, every line indented two
// spaces per level, then that child's own children. maxDepth 0 is the whole
// tree. A child that cannot be read prints one line saying so and the walk
// continues.
func renderRunTree(out io.Writer, run *orkstackv1.Run, read childReader, level, maxDepth int) error {
	if maxDepth > 0 && level > maxDepth {
		return nil
	}
	heading := strings.Repeat("  ", level-1)
	for _, taskRun := range run.GetTaskRuns() {
		child := taskRun.GetChild()
		if child.GetRunId() == "" {
			continue
		}
		fmt.Fprintln(out)
		childRun, err := read(child.GetRunId(), run.GetNamespace())
		if err != nil {
			code, message := "unknown", err.Error()
			var connectErr *connect.Error
			if errors.As(err, &connectErr) {
				code, message = connectErr.Code().String(), connectErr.Message()
			}
			fmt.Fprintf(out, "%s%s → %s: unavailable (%s: %s)\n",
				heading, taskRun.GetTaskId(), child.GetRunId(), code, message)
			continue
		}
		fmt.Fprintf(out, "%s%s → %s  %s  %s\n", heading, taskRun.GetTaskId(), child.GetRunId(),
			childLabel(child), statusLabel(childRun.GetStatus()))
		var table bytes.Buffer
		if err := renderTaskRunTable(&table, childRun); err != nil {
			return err
		}
		indent := strings.Repeat("  ", level)
		for _, line := range strings.SplitAfter(table.String(), "\n") {
			if line == "" {
				continue
			}
			fmt.Fprint(out, indent+line)
		}
		if err := renderRunTree(out, childRun, read, level+1, maxDepth); err != nil {
			return err
		}
	}
	return nil
}

// composedRow is the run's row for taskID when it is a composed task.
func composedRow(run *orkstackv1.Run, taskID string) *orkstackv1.TaskRun {
	for _, taskRun := range run.GetTaskRuns() {
		if taskRun.GetTaskId() == taskID && taskRun.GetChild() != nil {
			return taskRun
		}
	}
	return nil
}

// composedHint prints, in place of a composed task's log stream, where its
// logs are: the child run's. It reports whether it printed, so a follower
// looks again until the child has started.
func (w *logWriter) composedHint(namespace string, task *orkstackv1.TaskRun, final bool) bool {
	child := task.GetChild()
	if child.GetRunId() == "" && !final {
		return false
	}
	if w.json {
		return true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if child.GetRunId() == "" {
		_ = w.write(fmt.Sprintf("%s: runs as a child run that has not started\n", task.GetTaskId()))
		return true
	}
	_ = w.write(fmt.Sprintf("%s: runs as child run %s; read its logs with %s\n",
		task.GetTaskId(), child.GetRunId(), orkctlCommand(namespace, "run", "logs", child.GetRunId())))
	return true
}
