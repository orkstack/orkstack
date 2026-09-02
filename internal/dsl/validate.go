package dsl

import (
	"fmt"
	"sort"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// validate enforces the eight documented DSL rules (docs/DSL.md).
func validate(wf *orkstackv1.Orkflow, opts Options) error {
	// Rule 6 checks that `uses:` is present and resolvable through the
	// CATALOG, not that it names something this binary was compiled to know
	// about.
	catalog := opts.catalog()

	// Rule 1: an explicit namespace is a slug. Absent is legal and means the
	// built-in "default" namespace, so an empty value is not checked here —
	// the store resolves it where the name becomes a key.
	if ns := wf.GetNamespace(); ns != "" {
		if err := checkSlug("namespace", ns); err != nil {
			return err
		}
	}

	// Rule 2: orkflow name present and slug-formatted.
	if wf.GetName() == "" {
		return fmt.Errorf("orkflow name is required")
	}
	if err := checkSlug("orkflow name", wf.GetName()); err != nil {
		return err
	}
	if len(wf.GetTasks()) == 0 {
		return fmt.Errorf("orkflow %q declares no tasks", wf.GetName())
	}

	// Rule 3: task ids unique and slug-formatted.
	byID := make(map[string]*orkstackv1.Task, len(wf.GetTasks()))
	for _, task := range wf.GetTasks() {
		if task.GetId() == "" {
			return fmt.Errorf("task id is required")
		}
		if err := checkSlug("task id", task.GetId()); err != nil {
			return err
		}
		if _, dup := byID[task.GetId()]; dup {
			return fmt.Errorf("duplicate task id %q", task.GetId())
		}
		byID[task.GetId()] = task
	}

	for _, task := range wf.GetTasks() {
		// Rule 4: every needs entry references an existing task id.
		for _, need := range task.GetNeeds() {
			if _, ok := byID[need]; !ok {
				return fmt.Errorf("task %q needs unknown task %q", task.GetId(), need)
			}
			if need == task.GetId() {
				return fmt.Errorf("task %q needs itself (cycle)", task.GetId())
			}
		}
		// Rule 6: `uses:` names the activity, and it is required.
		//
		// It is checked against the CATALOG, not against anything compiled
		// into this binary. §4.2's four rules decide what
		// a value means: a template can never be pinned (rule 1); a slug that
		// is a catalog key is that entry (rule 2); anything carrying `/`, `:`
		// or `@` is a reference and is taken as itself (rule 3); anything
		// else names nothing (rule 4).
		//
		// Rule 3 is what keeps "any OCI image can become a durable production
		// activity" true: a third-party reference needs no catalog entry, and
		// the compiler never asks whether it exists — that is the registry's
		// answer, at APPLY (§4.3), not a hardcoded list's.
		//
		// This runs in the compiler AND again in internal/api before Put, as
		// §4.2 specifies. The compiler's copy is the one an author sees while
		// editing; the applier's is the one nothing can skip.
		if task.GetUses() == "" {
			return fmt.Errorf("task %q: `uses:` is required — it names the activity, "+
				"either a catalog name (\"shell\") or a registry reference "+
				"(\"ghcr.io/acme/migrate:v3\")", task.GetId())
		}
		if _, err := catalog.ResolveUses(task.GetId(), task.GetUses()); err != nil {
			return err
		}
		// Rule 6b: the network mode is one of exactly two.
		switch task.GetNetwork() {
		case orkstackv1.NetworkMode_NETWORK_MODE_UNSPECIFIED,
			orkstackv1.NetworkMode_NETWORK_MODE_DEFAULT,
			orkstackv1.NetworkMode_NETWORK_MODE_NONE:
		default:
			return fmt.Errorf("task %q: `network:` must be \"default\" or \"none\"", task.GetId())
		}
		// Rule 6c: a secret names a file and a source, and the name is
		// validated rather than sanitized so it can never escape the secrets
		// directory.
		for _, secret := range task.GetSecrets() {
			if !validSecretName(secret.GetName()) {
				return fmt.Errorf("task %q: secret name %q must match ^[A-Za-z0-9._-]{1,128}$",
					task.GetId(), secret.GetName())
			}
			if secret.GetSource() == "" {
				return fmt.Errorf("task %q: secret %q needs a `source:` (e.g. \"env:DB_PASSWORD\")",
					task.GetId(), secret.GetName())
			}
		}
		// Rule 7: an explicit pool is a slug. The compiler checks the shape
		// only; whether this namespace may dispatch onto that pool is the
		// server's call, on a path a hand-written request cannot skip.
		if pool := task.GetPool(); pool != "" {
			if err := checkSlug(fmt.Sprintf("task %q: pool", task.GetId()), pool); err != nil {
				return err
			}
		}
	}

	// Rule 5: the needs graph is acyclic.
	if err := checkAcyclic(wf.GetTasks()); err != nil {
		return err
	}

	// Rule 8: templates parse and reference only declared inputs and
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

// validSecretName holds a secret name to the §3.2 grammar,
// ^[A-Za-z0-9._-]{1,128}$.
//
// It is VALIDATED, never sanitized — the same discipline every other name in
// this file gets. A sanitized name is a name the author did not write, and
// this one becomes a path segment under /orkstack/input/secrets.
func validSecretName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.' || r == '_' || r == '-':
		default:
			return false
		}
	}
	return name != "." && name != ".."
}
