package dsl

import (
	"fmt"
	"sort"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	// taskpool is imported for ONE constant: the name of the default pool.
	// Rule 10 has to know that an absent pool and "main" are one pool, and
	// QueueForPool (internal/task/pool.go:82) is where that is already true.
	// Reading the name from there rather than writing "main" here is what
	// keeps the compiler's idea of "the same pool" from drifting from the
	// router's.
	taskpool "github.com/orkstack/orkstack/internal/task"
)

// validate enforces the documented DSL rules (docs/DSL.md) except rules 14,
// 31 and 32, which need a resolved image or the orkflow store and are checked
// by internal/api.
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
			return validationError("namespace", err)
		}
	}

	// Rule 2: orkflow name present and slug-formatted.
	if wf.GetName() == "" {
		return validationError("name", fmt.Errorf("orkflow name is required"))
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

	// Rule 9 (declarations) and rule 13: every declared pin is named once, by
	// a slug, and carries a timeout that is not negative.
	//
	// A pin name is slug-bounded for a reason the other slugs do not have: it
	// becomes a directory component on the pin host
	// (<workdir>/ork-pin-<run>-<pin>-<instance>, §8.2), and a slug is exactly
	// what safeComponent would not have to rewrite. A name the Orker would
	// have to sanitize is a name the author did not write.
	//
	// The duplicate check cannot fire for a compiled document — document.Pins
	// is a Go map, so YAML cannot hand us the same key twice — and it is here
	// because validate also guards a HAND-BUILT Orkflow arriving at
	// internal/api, which never met the YAML decoder. Rule 3 makes the same
	// bet about task ids and for the same reason.
	byPin := make(map[string]*orkstackv1.Pin, len(wf.GetPins()))
	for pinIndex, pin := range wf.GetPins() {
		field := fmt.Sprintf("pins[%d]", pinIndex)
		if err := checkSlug("pin name", pin.GetName()); err != nil {
			return validationError(field, err)
		}
		if _, dup := byPin[pin.GetName()]; dup {
			return validationError(field, fmt.Errorf("duplicate pin %q", pin.GetName()))
		}
		// Rule 13: reject a NEGATIVE timeout only.
		//
		// docs/DSL.md phrases the rule as "when present, at least 1", and 0 is
		// how "not present" arrives here: Pin.timeout_seconds is a plain
		// proto int32, so by the time validate sees it, an author who wrote
		// `timeout_seconds: 0` and an author who wrote nothing are one value —
		// and the contract gives that value one meaning, DERIVED: the sum of
		// every member's budget (docs/DSL.md "Pins", rule 13).
		// Enforcing ">= 1 if the author typed something" would need the
		// *int32 that only pinDocument has, i.e. a rule the YAML front end
		// could apply and a hand-built request could skip — two answers to
		// one question, in the class validate exists to close. So 0 is
		// accepted as "derived" however it was written, and the only value
		// that names no lifetime at all is refused.
		if pin.GetTimeoutSeconds() < 0 {
			return validationError(field+".timeout_seconds", fmt.Errorf(
				"pin %q: timeout_seconds %d is negative — omit it (or 0) for the derived "+
					"default, which is the sum of the pin's tasks' own budgets",
				pin.GetName(), pin.GetTimeoutSeconds()))
		}
		byPin[pin.GetName()] = pin
	}

	for taskIndex, task := range wf.GetTasks() {
		// Rule 4: every needs entry references an existing task id.
		for needIndex, need := range task.GetNeeds() {
			if _, ok := byID[need]; !ok {
				return validationError(fmt.Sprintf("tasks[%d].needs[%d]", taskIndex, needIndex), fmt.Errorf("task %q needs unknown task %q", task.GetId(), need))
			}
			if need == task.GetId() {
				return validationError(fmt.Sprintf("tasks[%d].needs[%d]", taskIndex, needIndex), fmt.Errorf("task %q needs itself (cycle)", task.GetId()))
			}
		}
		// Rule 26: a compiled orkflow reference belongs only to a `uses:`
		// that names an Orkflow.
		composed := strings.HasPrefix(task.GetUses(), OrkflowUsesPrefix)
		if task.GetOrkflow() != nil && !composed {
			return validationError(fmt.Sprintf("tasks[%d].orkflow", taskIndex), fmt.Errorf(
				"task %q: orkflow %s is set, but uses: %q does not name an orkflow — a composed task's "+
					"uses: is %s", task.GetId(), ComposedLabel(task.GetOrkflow()), task.GetUses(), orkflowUsesShape))
		}
		// Rule 6: a task has exactly one of `uses:` and `await:`. An awaited
		// task runs no image, so it skips the catalog check, network (6b) and
		// secrets (6c), and meets rules 15–17 instead.
		if task.GetAwait() != nil {
			if task.GetUses() != "" {
				return validationError(fmt.Sprintf("tasks[%d].uses", taskIndex), awaitUsesRefusal(task.GetId()))
			}
			if err := validateAwaitTask(taskIndex, task); err != nil {
				return err
			}
			continue
		}
		// Rule 6, composed: a `uses:` beginning with orkflow: is a composed
		// task (rules 26–27). It skips the catalog check, 6b and 6c — it
		// would otherwise be taken for a registry reference because it
		// contains `:` and `@`.
		if composed {
			if err := validateComposedTask(taskIndex, task); err != nil {
				return err
			}
			continue
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
		// Rule 9 (references): a task's pin names a DECLARED pin.
		//
		// This is rule 4's rule for a different reference: `needs: [ghost]` is
		// refused rather than ignored, and so is `pin: buidl`. It is the whole
		// argument for declaring pins at the top level (§4.2) — an implicit
		// group is a reference that cannot be found to be missing, so a typo
		// would quietly run one task alone on its own host instead of with
		// the group its author meant.
		if pin := task.GetPin(); pin != "" {
			if err := checkSlug(fmt.Sprintf("task %q: pin", task.GetId()), pin); err != nil {
				return validationError(fmt.Sprintf("tasks[%d].pin", taskIndex), err)
			}
			if _, ok := byPin[pin]; !ok {
				return validationError(fmt.Sprintf("tasks[%d].pin", taskIndex),
					fmt.Errorf("task %q pins to unknown pin %q", task.GetId(), pin))
			}
		}
		// Rules 34–36: a cached task's TTL, scope, and no pin.
		if err := validateCache(taskIndex, task); err != nil {
			return err
		}
	}

	// Rule 5: the needs graph is acyclic.
	if err := checkAcyclic(wf.GetTasks()); err != nil {
		return err
	}

	// Rules 37–42: join policies. After rule 5 (SureAncestors and rule 40
	// walk the needs graph) and before rule 8, so a refused join is named
	// before any template rule reads it.
	if err := validateJoins(wf, byID); err != nil {
		return err
	}

	// Rule 8: templates parse and reference only declared inputs and
	// (transitively) upstream tasks.
	inputs := map[string]struct{}{}
	for name := range wf.GetInputs().GetFields() {
		inputs[name] = struct{}{}
	}
	closures := transitiveNeeds(byID)

	// Rules 19–24 (the fan-out's own declaration): before the template
	// rules, so a malformed `each:` is named before anything that reads it.
	if err := validateEach(wf, byID, closures); err != nil {
		return err
	}

	for taskIndex, task := range wf.GetTasks() {
		checker := &templateChecker{
			taskID:   task.GetId(),
			inputs:   inputs,
			tasks:    byID,
			upstream: closures[task.GetId()],
			anchor:   fmt.Sprintf("tasks[%d]", taskIndex),
		}
		// Rule 25: `.each` is legal only in the `with:` of a fan-out, so the
		// condition is checked with no item name bound.
		if err := checker.checkString("condition", task.GetCondition()); err != nil {
			return err
		}
		checker.each = task.GetEach().GetAs()
		checker.fanOut = task.GetEach() != nil
		// Rule 28: a composed task's with: is rendered by the ENGINE into the
		// child run's input.
		if task.GetOrkflow() != nil {
			checker.engine = true
			checker.engineWhat = "a composed task's with: becomes the child run's input, which " +
				"carries values, not files"
		}
		if err := checker.checkValue("with", task.GetWith().AsMap()); err != nil {
			return err
		}
	}

	// Rule 29: outputs: is a flat map of names to engine-rendered templates
	// that may read any task but a fan-out — they are rendered after every
	// task is terminal, so no needs closure applies.
	if err := validateOutputs(wf, inputs, byID); err != nil {
		return err
	}

	// Rules 10, 11 and 12: what one pin's members must agree on, and that
	// every declaration has members. A cross-task pass, like rules 5 and 8
	// above, because each answer is a property of the GROUP and no single task
	// carries it.
	//
	// The comparison is against the FIRST member in declaration order, so the
	// error always names the same two tasks for the same document.
	firstOf := make(map[string]*orkstackv1.Task, len(byPin))
	for taskIndex, task := range wf.GetTasks() {
		pin := task.GetPin()
		if pin == "" {
			continue
		}
		first, seen := firstOf[pin]
		if !seen {
			firstOf[pin] = task
			continue
		}
		// Rule 10: one pin, one pool — necessarily, because a pin is one
		// process answering from one pool's queue, so a pin spanning two
		// pools is not implementable rather than merely unwise (§4.3).
		//
		// Compared NORMALIZED: an absent pool and "main" are one pool here
		// because they are one route in QueueForPool, so a document that
		// spells the default two ways is not refused for a difference that
		// does not exist. This is the compile-time check on the LITERAL
		// values only. The same rule is checked again in
		// PoolResolver.Plan (internal/engine/pools.go) after namespace-default
		// resolution, on the resolved route's pool — two tasks whose literal
		// pools agree can still resolve apart, and a hand-built request skips
		// this file entirely. Rule 7 is split between compiler and server the
		// same way, for the same reason.
		if normalizedPool(first.GetPool()) != normalizedPool(task.GetPool()) {
			return validationError(fmt.Sprintf("tasks[%d].pool", taskIndex), fmt.Errorf(
				"pin %q spans two pools: task %q names pool %q and task %q names pool %q — "+
					"a pin is ONE Orker out of one pool's set, so every task of a pin must name "+
					"the same pool (an absent pool and %q are the same pool)",
				pin, first.GetId(), first.GetPool(), task.GetId(), task.GetPool(), taskpool.DefaultPool))
		}
		// Rule 11: one pin, one run_as_orker_user.
		//
		// /orkstack/shared is 1777 with the sticky bit, like output/, so two
		// members running as different uids can each create files the other
		// cannot delete or overwrite — the classic sticky-bit failure, and the
		// most common way for a pin to break on its SECOND task. Agreement
		// does not remove the difference between two images' baked-in USERs
		// when the flag is false; that residue is what internal/api's
		// all-false WARNING is for (§4.4).
		if first.GetRunAsOrkerUser() != task.GetRunAsOrkerUser() {
			return validationError(fmt.Sprintf("tasks[%d].run_as_orker_user", taskIndex), fmt.Errorf(
				"pin %q disagrees on run_as_orker_user: task %q has %t and task %q has %t — "+
					"/orkstack/shared is sticky-bit 1777, so members running as different users "+
					"can each create files the other cannot replace",
				pin, first.GetId(), first.GetRunAsOrkerUser(), task.GetId(), task.GetRunAsOrkerUser()))
		}
	}
	// Rule 12: a declaration nothing references is a typo, and refusing it
	// costs less than a pin that would open a session for nobody. It cannot
	// open one — opening is lazy, on the first member that is ready — but the
	// document is still wrong, and the author who wrote `pins: {buidl: {}}`
	// alongside `pin: build` is told which half is unreferenced.
	for _, pin := range wf.GetPins() {
		if _, used := firstOf[pin.GetName()]; !used {
			return validationError(fmt.Sprintf("pins.%s", pin.GetName()),
				fmt.Errorf("pin %q is declared but no task references it", pin.GetName()))
		}
	}
	return nil
}

// normalizedPool collapses the two spellings of the default pool into one, the
// way QueueForPool does when it routes them (internal/task/pool.go:82). An
// absent pool and "main" are one set of Orkers, so a rule about "the same
// pool" must not see two.
func normalizedPool(pool string) string {
	if pool == "" {
		return taskpool.DefaultPool
	}
	return pool
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
