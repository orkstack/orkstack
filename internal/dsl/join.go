package dsl

import (
	"fmt"
	"sort"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// MaxHandlerNeeds bounds the needs of a failure handler that runs a container
// (docs/DSL.md rule 42): its input records how every need ended, and that
// record is bounded. Raising it later breaks nothing; lowering it would.
const MaxHandlerNeeds = 32

// joinWords maps the DSL's words to the compiled policy. all_succeeded is the
// default and compiles to UNSPECIFIED — the only encoding of the default — so
// a document that writes it keeps the digest of one that writes nothing.
var joinWords = map[string]orkstackv1.JoinPolicy{
	"all_succeeded": orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED,
	"none_failed":   orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED,
	"any_failed":    orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED,
	"all_done":      orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE,
}

// joinWordList is rule 37's vocabulary, in the order the sentences name it.
const joinWordList = "all_succeeded, none_failed, any_failed or all_done"

// compileJoin maps a task's `join:` word to its policy (rule 37). Absent and
// all_succeeded both compile to UNSPECIFIED. The word is validated, never
// sanitised.
func compileJoin(taskIndex int, td taskDocument) (orkstackv1.JoinPolicy, error) {
	if td.Join == "" {
		return orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED, nil
	}
	policy, ok := joinWords[td.Join]
	if !ok {
		return 0, validationError(fmt.Sprintf("tasks[%d].join", taskIndex), fmt.Errorf(
			"task %q: join %q is not a join policy; it is one of %s", td.ID, td.Join, joinWordList))
	}
	return policy, nil
}

// JoinWord renders a policy as the DSL writes it: all_succeeded for
// UNSPECIFIED. An undefined value renders as its number, so a sentence about
// a hand-built message still names what it carried. Pure.
func JoinWord(p orkstackv1.JoinPolicy) string {
	switch p {
	case orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED:
		return "all_succeeded"
	case orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED:
		return "none_failed"
	case orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED:
		return "any_failed"
	case orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE:
		return "all_done"
	}
	return fmt.Sprintf("JoinPolicy(%d)", int32(p))
}

// IsFailureHandler reports whether t is a failure handler: its join is
// any_failed or all_done. It is the only kind of task that starts in a
// failing run (docs/DSL.md "Joins and failure handlers"). Pure.
func IsFailureHandler(t *orkstackv1.Task) bool {
	switch t.GetJoin() {
	case orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED, orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE:
		return true
	}
	return false
}

// validateJoins enforces rules 37 (the compiled half), 38, 39, 40, 41 and 42
// on the compiled message, so a hand-built definition meets the same rules a
// document does. Called after rule 5, so the needs graph is acyclic.
func validateJoins(wf *orkstackv1.Orkflow, byID map[string]*orkstackv1.Task) error {
	for taskIndex, task := range wf.GetTasks() {
		id := task.GetId()
		policy := task.GetJoin()
		at := func(field string) string { return fmt.Sprintf("tasks[%d].%s", taskIndex, field) }
		// Rule 37: a compiled join is one of JoinPolicy's defined values.
		if _, defined := orkstackv1.JoinPolicy_name[int32(policy)]; !defined {
			return validationError(at("join"), fmt.Errorf(
				"task %q: join %s is not a defined join policy; it is one of %s",
				id, JoinWord(policy), joinWordList))
		}
		// Rule 38: a join decides on its needs' outcomes.
		if policy != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED && len(task.GetNeeds()) == 0 {
			return validationError(at("join"), fmt.Errorf(
				"task %q: join %s decides on how its needs ended, and it names no need — add needs:, "+
					"or remove join:", id, JoinWord(policy)))
		}
		handler := IsFailureHandler(task)
		// Rule 39: a failure handler is not pinned, fanned out or cached.
		if handler {
			if task.GetPin() != "" {
				return validationError(at("pin"), fmt.Errorf(
					"task %q (join %s) is a failure handler and cannot join a pin — fail-fast ends "+
						"every pin with the run's work, so the /orkstack/shared it would read is gone "+
						"when it starts", id, JoinWord(policy)))
			}
			if task.GetEach() != nil {
				return validationError(at("each"), fmt.Errorf(
					"task %q (join %s) is a failure handler and cannot be a fan-out — a fan-out's "+
						"first failed item fails it and the run's fail-fast stops its siblings, and a "+
						"failing run has no fail-fast left to stop them", id, JoinWord(policy)))
			}
			if task.GetCache() != nil {
				return validationError(at("cache"), fmt.Errorf(
					"task %q (join %s) is a failure handler and cannot be cached — its input "+
						"includes how its needs ended, which no memo key covers, and a handler "+
						"exists for its effect, which a memo hit would silently skip", id, JoinWord(policy)))
			}
		}
		// Rule 40: an ordinary or none_failed task may not need an
		// any_failed task — it could never run.
		if !handler {
			for needIndex, need := range task.GetNeeds() {
				if byID[need].GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED {
					continue
				}
				return validationError(fmt.Sprintf("tasks[%d].needs[%d]", taskIndex, needIndex), fmt.Errorf(
					"task %q (join %s) needs %q, whose join is any_failed — in a run that has not "+
						"failed that task is always skipped, and in a failing run only failure "+
						"handlers start, so %q could never run; use join: all_done (run after it, "+
						"whatever it did) or join: any_failed (run if it fails)",
					id, JoinWord(policy), need, id))
			}
		}
		// Rule 42: a container failure handler names at most 32 needs.
		if handler && task.GetAwait() == nil && !IsComposedTask(task) && len(task.GetNeeds()) > MaxHandlerNeeds {
			return validationError(at("needs"), fmt.Errorf(
				"task %q (join %s) names %d needs; a failure handler names at most %d, because its "+
					"input records how each one ended — split it into handlers of at most %d needs each",
				id, JoinWord(policy), len(task.GetNeeds()), MaxHandlerNeeds, MaxHandlerNeeds))
		}
	}
	// Rule 41: outputs: may not read an any_failed task. A template rule 29
	// will refuse (a dynamic index, a parse error) is left to rule 29.
	for _, key := range sortedStringKeys(wf.GetOutputs()) {
		refs, err := ReferencedUpstreamIn(wf.GetOutputs()[key])
		if err != nil {
			continue
		}
		for _, ref := range refs {
			if byID[ref].GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED {
				continue
			}
			return validationError("outputs."+key, fmt.Errorf(
				"outputs.%s reads task %q, whose join is any_failed — outputs are rendered only "+
					"when a run succeeds, and in a run that succeeds that task is always skipped",
				key, ref))
		}
	}
	return nil
}

func sortedStringKeys(m map[string]string) []string {
	set := make(map[string]struct{}, len(m))
	for k := range m {
		set[k] = struct{}{}
	}
	return sortedKeys(set)
}

// SureAncestors returns, per task id, the set of ancestors CERTAIN to have
// SUCCEEDED whenever that task runs (docs/DSL.md "Joins and failure
// handlers", the guard idiom):
//
//	all_succeeded  ⋃ over needs n of ({n} ∪ sure(n))
//	none_failed    ⋂ over needs n of ({n} ∪ sure(n))
//	any_failed     ⋂ over needs n of sure(n)
//	all_done       ∅
//
// For a definition with no join it is every task's whole needs closure.
// Memoised over the needs graph; wf must be acyclic (rule 5). Pure.
func SureAncestors(wf *orkstackv1.Orkflow) map[string]map[string]bool {
	byID := make(map[string]*orkstackv1.Task, len(wf.GetTasks()))
	for _, t := range wf.GetTasks() {
		byID[t.GetId()] = t
	}
	out := make(map[string]map[string]bool, len(byID))
	visiting := map[string]bool{}
	var sure func(id string) map[string]bool
	sure = func(id string) map[string]bool {
		if cached, ok := out[id]; ok {
			return cached
		}
		set := map[string]bool{}
		t := byID[id]
		if t == nil || visiting[id] {
			return set
		}
		visiting[id] = true
		defer delete(visiting, id)
		withNeed := func(n string) map[string]bool {
			s := map[string]bool{n: true}
			for a := range sure(n) {
				s[a] = true
			}
			return s
		}
		switch t.GetJoin() {
		case orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE:
			// ∅: a need may have been cancelled before it ever ran.
		case orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED:
			set = intersectAll(t.GetNeeds(), withNeed)
		case orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED:
			set = intersectAll(t.GetNeeds(), sure)
		default:
			for _, n := range t.GetNeeds() {
				for a := range withNeed(n) {
					set[a] = true
				}
			}
		}
		out[id] = set
		return set
	}
	for _, t := range wf.GetTasks() {
		sure(t.GetId())
	}
	return out
}

// intersectAll is the intersection of of(n) over every n of needs; empty for
// no needs.
func intersectAll(needs []string, of func(string) map[string]bool) map[string]bool {
	out := map[string]bool{}
	for i, n := range needs {
		s := of(n)
		if i == 0 {
			for a := range s {
				out[a] = true
			}
			continue
		}
		for a := range out {
			if !s[a] {
				delete(out, a)
			}
		}
	}
	return out
}

// TaskReadKind is how a template reads a task.
type TaskReadKind string

const (
	// TaskReadField is the field spelling: .tasks.<id>… or $.tasks.<id>….
	TaskReadField TaskReadKind = "field"
	// TaskReadIndex is the index spelling with at least one key after the
	// id: index .tasks "<id>" "output" …. A bare index .tasks "<id>" is an
	// existence test, not a read.
	TaskReadIndex TaskReadKind = "index"
	// TaskReadArtifact is the helper artifact "<id>/<name>" with a literal
	// argument.
	TaskReadArtifact TaskReadKind = "artifact"
)

// TaskRead is one read of a task by a template.
type TaskRead struct {
	TaskID string
	Kind   TaskReadKind
	// Path is the template's field below the task ("condition",
	// "with.command[2]"); "" for TaskReads.
	Path string
	// Guarded is true when the read lies in the body — never the else — of
	// an {{ if }} or {{ with }} whose pipeline is exactly index .tasks "<id>"
	// (or index $.tasks "<id>") for the same id.
	Guarded bool
	// Name is, for an artifact read, the artifact's name after "<id>/";
	// empty otherwise.
	Name string
}

// TaskReads lists every read of a task in text, in walk order, with whether
// each is guarded. A collector: it never refuses what rule 8 would. A parse
// error is returned as is. Pure.
func TaskReads(text string) ([]TaskRead, error) {
	return taskReadsAt("", text)
}

func taskReadsAt(path, text string) ([]TaskRead, error) {
	if !strings.Contains(text, "{{") {
		return nil, nil
	}
	tmpl, err := Parse("reference", text)
	if err != nil {
		return nil, err
	}
	var reads []TaskRead
	c := &templateChecker{refs: map[string]struct{}{}, reads: &reads}
	if err := c.walkTemplate(path, tmpl); err != nil {
		return nil, err
	}
	return reads, nil
}

// taskReadsIn is TaskReads over every string leaf of value, walked as rule 8
// walks a task's with: (map keys sorted, list items in order), each read's
// Path the leaf's field below the task.
func taskReadsIn(path string, value any) ([]TaskRead, error) {
	switch typed := value.(type) {
	case string:
		return taskReadsAt(path, typed)
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var out []TaskRead
		for _, k := range keys {
			reads, err := taskReadsIn(path+"."+k, typed[k])
			if err != nil {
				return nil, err
			}
			out = append(out, reads...)
		}
		return out, nil
	case []any:
		var out []TaskRead
		for i, item := range typed {
			reads, err := taskReadsIn(fmt.Sprintf("%s[%d]", path, i), item)
			if err != nil {
				return nil, err
			}
			out = append(out, reads...)
		}
		return out, nil
	}
	return nil, nil
}

// JoinWarnings is W1: one WARNING per (task, ancestor) for an unguarded read,
// in a task's condition: or with:, of an ancestor that is not certain to have
// SUCCEEDED when the task runs (SureAncestors). An artifact read by a failure
// handler counts as unguarded whatever encloses it: a failed need may be
// present with some artifacts and not others. A definition with no join gets
// none. Pure; never an ERROR.
func JoinWarnings(wf *orkstackv1.Orkflow) []*orkstackv1.ValidationIssue {
	hasJoin := false
	byID := make(map[string]*orkstackv1.Task, len(wf.GetTasks()))
	for _, t := range wf.GetTasks() {
		byID[t.GetId()] = t
		if t.GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED {
			hasJoin = true
		}
	}
	if !hasJoin {
		return nil
	}
	closures := transitiveNeeds(byID)
	sure := SureAncestors(wf)
	var issues []*orkstackv1.ValidationIssue
	for taskIndex, t := range wf.GetTasks() {
		id := t.GetId()
		handler := IsFailureHandler(t)
		reads, err := taskReadsAt("condition", t.GetCondition())
		if err != nil {
			continue
		}
		if t.GetWith() != nil {
			withReads, err := taskReadsIn("with", t.GetWith().AsMap())
			if err != nil {
				continue
			}
			reads = append(reads, withReads...)
		}
		warned := map[string]bool{}
		for _, r := range reads {
			if warned[r.TaskID] {
				continue
			}
			if _, inClosure := closures[id][r.TaskID]; !inClosure || sure[id][r.TaskID] {
				continue
			}
			unguarded := !r.Guarded || (r.Kind == TaskReadArtifact && handler)
			if !unguarded {
				continue
			}
			warned[r.TaskID] = true
			issues = append(issues, &orkstackv1.ValidationIssue{
				Field:    fmt.Sprintf("tasks[%d].%s", taskIndex, r.Path),
				Message:  joinWarningMessage(id, handler, r),
				Severity: orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING,
			})
		}
	}
	return issues
}

// joinWarningMessage is W1's sentence for one read.
func joinWarningMessage(taskID string, handler bool, r TaskRead) string {
	if r.Kind == TaskReadArtifact {
		msg := fmt.Sprintf("task %q reads artifact %q, which is not certain to exist when %q runs; "+
			"a missing artifact fails the render. Read it from task.json's inputs.artifacts instead, "+
			"where a missing file is a missing entry", taskID, r.TaskID+"/"+r.Name, taskID)
		if !handler {
			msg += fmt.Sprintf(", or guard it: {{ with index .tasks %q }}…{{ end }}", r.TaskID)
		}
		return msg
	}
	why := "(a join lies between them)"
	if handler {
		why = fmt.Sprintf("(%q is a failure handler)", taskID)
	}
	return fmt.Sprintf("task %q reads .tasks.%s, which is not certain to have succeeded when %q runs "+
		"%s; reading a task that did not succeed fails the task. Guard the read: "+
		"{{ with index .tasks %q }}…{{ end }}", taskID, r.TaskID, taskID, why, r.TaskID)
}
