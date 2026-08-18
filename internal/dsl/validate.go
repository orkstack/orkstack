package dsl

import (
	"fmt"
	"sort"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// validate enforces the six documented DSL rules (docs/DSL.md).
func validate(wf *orkstackv1.Orkflow, opts Options) error {
	known := opts.KnownPlugins
	if len(known) == 0 {
		known = KnownPlugins
	}
	knownSet := make(map[string]struct{}, len(known))
	for _, name := range known {
		knownSet[name] = struct{}{}
	}

	// Rule 1: orkflow name present and slug-formatted.
	if wf.GetName() == "" {
		return fmt.Errorf("orkflow name is required")
	}
	if !slugPattern.MatchString(wf.GetName()) {
		return fmt.Errorf("orkflow name %q is not a slug (%s)", wf.GetName(), slugPattern.String())
	}
	// Rule 7: an explicit namespace is a slug. Absent is legal and means the
	// built-in "default" namespace, so an empty value is not checked here —
	// the store resolves it where the name becomes a key.
	if ns := wf.GetNamespace(); ns != "" && !slugPattern.MatchString(ns) {
		return fmt.Errorf("namespace %q is not a slug (%s)", ns, slugPattern.String())
	}
	if len(wf.GetTasks()) == 0 {
		return fmt.Errorf("orkflow %q declares no tasks", wf.GetName())
	}

	// Rule 2: task ids unique and slug-formatted.
	byID := make(map[string]*orkstackv1.Task, len(wf.GetTasks()))
	for _, task := range wf.GetTasks() {
		if task.GetId() == "" {
			return fmt.Errorf("task id is required")
		}
		if !slugPattern.MatchString(task.GetId()) {
			return fmt.Errorf("task id %q is not a slug (%s)", task.GetId(), slugPattern.String())
		}
		if _, dup := byID[task.GetId()]; dup {
			return fmt.Errorf("duplicate task id %q", task.GetId())
		}
		byID[task.GetId()] = task
	}

	for _, task := range wf.GetTasks() {
		// Rule 3: every needs entry references an existing task id.
		for _, need := range task.GetNeeds() {
			if _, ok := byID[need]; !ok {
				return fmt.Errorf("task %q needs unknown task %q", task.GetId(), need)
			}
			if need == task.GetId() {
				return fmt.Errorf("task %q needs itself (cycle)", task.GetId())
			}
		}
		// Rule 5: plugin is a non-empty known plugin name.
		if task.GetPlugin() == "" {
			return fmt.Errorf("task %q: plugin is required", task.GetId())
		}
		if _, ok := knownSet[task.GetPlugin()]; !ok {
			return fmt.Errorf("task %q: unknown plugin %q (known: %s)",
				task.GetId(), task.GetPlugin(), strings.Join(sortedKeys(knownSet), ", "))
		}
		// Rule 7: an explicit pool is a slug. The compiler checks the shape
		// only; whether this namespace may dispatch onto that pool is the
		// server's call, on a path a hand-written request cannot skip.
		if pool := task.GetPool(); pool != "" && !slugPattern.MatchString(pool) {
			return fmt.Errorf("task %q: pool %q is not a slug (%s)",
				task.GetId(), pool, slugPattern.String())
		}
	}

	// Rule 4: the needs graph is acyclic.
	if err := checkAcyclic(wf.GetTasks()); err != nil {
		return err
	}

	// Rule 6: templates parse and reference only declared inputs and
	// (transitively) upstream tasks.
	inputs := map[string]struct{}{}
	for name := range wf.GetInputs().GetFields() {
		inputs[name] = struct{}{}
	}
	closures := transitiveNeeds(byID)
	for _, task := range wf.GetTasks() {
		checker := &templateChecker{
			taskID:   task.GetId(),
			inputs:   inputs,
			tasks:    byID,
			upstream: closures[task.GetId()],
		}
		if err := checker.checkString("condition", task.GetCondition()); err != nil {
			return err
		}
		if err := checker.checkValue("with", task.GetWith().AsMap()); err != nil {
			return err
		}
	}
	return nil
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// checkAcyclic reports a cycle in the needs graph using an iterative DFS over
// lexically sorted ids so the reported cycle is stable.
func checkAcyclic(tasks []*orkstackv1.Task) error {
	needs := make(map[string][]string, len(tasks))
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		sorted := append([]string(nil), task.GetNeeds()...)
		sort.Strings(sorted)
		needs[task.GetId()] = sorted
		ids = append(ids, task.GetId())
	}
	sort.Strings(ids)

	const (
		white = 0
		grey  = 1
		black = 2
	)
	color := make(map[string]int, len(ids))
	var path []string

	var visit func(id string) error
	visit = func(id string) error {
		switch color[id] {
		case black:
			return nil
		case grey:
			cycle := append(append([]string(nil), path...), id)
			return fmt.Errorf("cycle in needs graph: %s", strings.Join(trimCycle(cycle, id), " -> "))
		}
		color[id] = grey
		path = append(path, id)
		for _, dep := range needs[id] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		color[id] = black
		return nil
	}

	for _, id := range ids {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

func trimCycle(path []string, start string) []string {
	for i, id := range path {
		if id == start {
			return path[i:]
		}
	}
	return path
}

// transitiveNeeds returns, per task id, the set of task ids reachable through
// needs edges (the tasks whose outputs/artifacts it may reference).
func transitiveNeeds(byID map[string]*orkstackv1.Task) map[string]map[string]struct{} {
	out := make(map[string]map[string]struct{}, len(byID))
	var resolve func(id string, seen map[string]struct{}) map[string]struct{}
	resolve = func(id string, seen map[string]struct{}) map[string]struct{} {
		if cached, ok := out[id]; ok {
			return cached
		}
		if _, looping := seen[id]; looping {
			return map[string]struct{}{}
		}
		seen[id] = struct{}{}
		closure := map[string]struct{}{}
		for _, dep := range byID[id].GetNeeds() {
			closure[dep] = struct{}{}
			for parent := range resolve(dep, seen) {
				closure[parent] = struct{}{}
			}
		}
		delete(seen, id)
		out[id] = closure
		return closure
	}
	for id := range byID {
		resolve(id, map[string]struct{}{})
	}
	return out
}
