package api

import (
	"context"
	"fmt"
	"slices"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
)

// holdsIdentity is identity.TaskHoldsIdentity.
var holdsIdentity = identity.TaskHoldsIdentity

// imageOf is what a task runs: its pinned digest once resolved.
func imageOf(t *orkstackv1.Task) string {
	if pinned := t.GetImage().GetPinned(); pinned != "" {
		return pinned
	}
	return t.GetImage().GetReference()
}

// identityIssues are the create-time warnings of task identity
// (workload-identity §8, §10), never an error:
//   - a cached task that holds an identity: a memo hit runs no container and
//     mints nothing, so a hit is not checked against the current policy;
//   - a pin whose members run different images where any holds an
//     identity: a pin is one trust domain, sharing a writable directory;
//   - a task holding an identity whose pool more than one namespace may
//     use: a stolen Orker of that pool takes the identity tasks of each;
//   - a task holding an identity with an approval or composed task upstream
//     it does not follow, or directly behind a sleeping task.
//
// The last reads the namespace registry; a registry that cannot be read is
// a warning that the check was not made, never silence.
func (s *Service) identityIssues(ctx context.Context, ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow) []*orkstackv1.ValidationIssue {
	warning := orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING
	var issues []*orkstackv1.ValidationIssue
	pools := map[string]int{} // pool → first task index holding an identity there
	pins := map[string][]int{}
	for i, t := range wf.GetTasks() {
		if pin := t.GetPin(); pin != "" {
			pins[pin] = append(pins[pin], i)
		}
		if !holdsIdentity(t) {
			continue
		}
		if t.GetCache() != nil {
			issues = append(issues, composeIssue(fmt.Sprintf("tasks[%d].cache", i), fmt.Sprintf(
				"task %q holds an identity and is cached: a memo hit runs no container and mints nothing, so it is not checked "+
					"against the namespace's identity policy as it is then; removing a grant does not stop a result cached under it from being reused",
				t.GetId()), warning))
		}
		pool := engine.PoolFor(ns, t)
		if _, seen := pools[pool]; !seen {
			pools[pool] = i
		}
	}
	// A barrier the author may expect and the run does not hold: an
	// approval or a composed task upstream that the task does not follow,
	// and a sleep right before it (docs/TASK_IDENTITY.md, "What a task
	// follows").
	for _, u := range identity.UnguardedIdentities(wf) {
		if len(u.Unfollowed) > 0 {
			why := "through a need that may be skipped"
			if u.Join == orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED || u.Join == orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE {
				why = "under its join " + dsl.JoinWord(u.Join)
			}
			issues = append(issues, composeIssue(fmt.Sprintf("tasks[%d].needs", u.Index), fmt.Sprintf(
				"task %q holds an identity and does not follow %s upstream of it, %s: "+
					"it may start, and hold its identity, whatever became of them",
				u.Task, strings.Join(u.Unfollowed, ", "), why), warning))
		}
		if len(u.BehindSleep) > 0 {
			issues = append(issues, composeIssue(fmt.Sprintf("tasks[%d].needs", u.Index), fmt.Sprintf(
				"task %q holds an identity directly behind sleeping task %s: a sleep has no record, so it does not hold "+
					"the identity back against a party that answers the run's workflow tasks",
				u.Task, strings.Join(u.BehindSleep, ", ")), warning))
		}
	}
	for _, pin := range sortedKeys(pins) {
		members := pins[pin]
		holds, images := false, map[string]bool{}
		var names []string
		for _, i := range members {
			t := wf.GetTasks()[i]
			holds = holds || holdsIdentity(t)
			images[imageOf(t)] = true
			names = append(names, t.GetId())
		}
		if holds && len(images) > 1 {
			issues = append(issues, composeIssue(fmt.Sprintf("tasks[%d].pin", members[0]), fmt.Sprintf(
				"pin %q's members %s run different images and one holds an identity: a pin is one trust domain, whose members share "+
					"a writable directory, so any of them can leave a token there for another", pin, strings.Join(names, ", ")), warning))
		}
	}
	if len(pools) == 0 || s.namespaces == nil {
		return issues
	}
	all, err := s.namespaces.List(ctx)
	if err != nil {
		return append(issues, composeIssue("tasks", fmt.Sprintf(
			"how many namespaces may use the pools of its identity tasks could not be checked: %v", err), warning))
	}
	for _, pool := range sortedKeys(pools) {
		n := 0
		for _, other := range all {
			if allowed := other.GetAllowedPools(); len(allowed) == 0 || slices.Contains(allowed, pool) {
				n++
			}
		}
		if n > 1 {
			i := pools[pool]
			issues = append(issues, composeIssue(fmt.Sprintf("tasks[%d].pool", i), fmt.Sprintf(
				"task %q holds an identity on pool %q, which %d namespaces may use: an Orker of that pool taken over takes the "+
					"identity tasks of each; restrict the pool with allowed_pools", wf.GetTasks()[i].GetId(), pool, n), warning))
		}
	}
	return issues
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
