package dsl

import (
	"errors"
	"fmt"
	"math/bits"
	"path"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// ErrAncestryTooLarge is rule 44 (docs/DSL.md "Validation rules"): a task
// whose required ancestors' smallest possible upstream references alone
// exceed one activity input (task.MaxActivityInputBytes) could never be
// dispatched, so the definition is refused at validate and apply, naming the
// task and its required-ancestor count.
var ErrAncestryTooLarge = errors.New("a task's required ancestors cannot fit one activity input")

// ancestryError is rule 44's refusal: its own sentence, unwrapping to
// ErrAncestryTooLarge.
type ancestryError struct{ message string }

func (e *ancestryError) Error() string { return e.message }
func (e *ancestryError) Unwrap() error { return ErrAncestryTooLarge }

// minimalRunID is the shortest run id the engine's minter can print
// (ork-%d-%d with both numbers zero): every real run id is at least as long,
// so a commit uri built from it is a lower bound on a real one.
const minimalRunID = "ork-0-0"

// minimalNamespace stands in for an undeclared namespace: apply may file the
// definition in any namespace, and a slug is at least one byte.
const minimalNamespace = "a"

// minimalSHA256 is a values reference's digest: 64 hexadecimal characters,
// whatever the bytes. The only field every values reference is sure to carry
// whatever the store (the uri, name and type depend on it).
var minimalSHA256 = strings.Repeat("0", 64)

// RefKind is what kind of upstream reference an ancestor contributes, which
// decides its smallest possible size.
type RefKind int

const (
	// RefPlain is a container task: its task id, a commit uri and a values
	// reference.
	RefPlain RefKind = iota
	// RefNoCommit is an awaited or composed task: its task id and a values
	// reference, and no commit uri — an await's result has no commit.json
	// (the Signal in history is its commit) and a composed task's stored
	// result has none either.
	RefNoCommit
	// RefFanOut is a fan-out: its task id and an empty fan-out marker, and
	// only on the input of a direct dependant.
	RefFanOut
)

// refKindOf classifies t.
func refKindOf(t *orkstackv1.Task) RefKind {
	switch {
	case t.GetEach() != nil:
		return RefFanOut
	case t.GetAwait() != nil, t.GetOrkflow() != nil:
		return RefNoCommit
	}
	return RefPlain
}

// MinimalUpstreamRef is the smallest UpstreamRef the engine can hand a
// dependant for the ancestor id of the given kind, in namespace (empty: the
// shortest a namespace can be): a lower bound on a real reference's size,
// since a real one always carries at least these fields with at least these
// values. A plain task's commit uri is built with an empty store root and
// the shortest run id, exactly as the engine composes it (store root + "/" +
// metadata/commits/{namespace}/{run}/{task}/commit.json). Pure.
func MinimalUpstreamRef(kind RefKind, namespace, id string) *orkstackv1.UpstreamRef {
	switch kind {
	case RefFanOut:
		return &orkstackv1.UpstreamRef{TaskId: id, Each: &orkstackv1.EachUpstream{}}
	case RefNoCommit:
		return &orkstackv1.UpstreamRef{TaskId: id, Values: &orkstackv1.ArtifactRef{Sha256: minimalSHA256}}
	}
	if namespace == "" {
		namespace = minimalNamespace
	}
	return &orkstackv1.UpstreamRef{
		TaskId:    id,
		CommitUri: "/" + path.Join("metadata/commits", namespace, minimalRunID, id, "commit.json"),
		Values:    &orkstackv1.ArtifactRef{Sha256: minimalSHA256},
	}
}

// refMarginal is what one more reference adds to an activity input that
// already carries at least one: task.PayloadSize's bound on the input with
// the reference twice, less with it once (the reference, its whitespace
// sites, and the separator between two).
func refMarginal(ref *orkstackv1.UpstreamRef) (int64, error) {
	once, err := task.PayloadSize(&orkstackv1.ExecuteTaskInput{Upstream: []*orkstackv1.UpstreamRef{ref}})
	if err != nil {
		return 0, err
	}
	twice, err := task.PayloadSize(&orkstackv1.ExecuteTaskInput{Upstream: []*orkstackv1.UpstreamRef{ref, ref}})
	if err != nil {
		return 0, err
	}
	return twice - once, nil
}

// RequiredAncestors returns, per task id, the ancestors whose success is
// implied by that task running, and so whose references its input always
// carries (rule 44):
//
//   - a need of a task with the default join (all_succeeded) is required:
//     such a task runs only when every need succeeded;
//   - the walk continues through a need's own needs only when that need has
//     the default join too; a task with a join policy can run with needs
//     skipped or failed, so a task with one — a failure handler included —
//     requires nothing of its needs. Such a task, as a required need, is
//     itself required.
//
// A fan-out is in the set wherever it is required; MinimalInputBytes counts
// it only as a direct need. The needs graph must be acyclic (rule 5). Pure;
// for tests and tools, O(T²) for a chain.
func RequiredAncestors(wf *orkstackv1.Orkflow) map[string][]string {
	g := newAncestryGraph(wf)
	out := make(map[string][]string, len(g.tasks))
	for i := range g.tasks {
		set := g.required(i)
		var ids []string
		for w, word := range set {
			for word != 0 {
				b := bits.TrailingZeros64(word)
				word &^= 1 << b
				ids = append(ids, g.tasks[w*64+b].GetId())
			}
		}
		out[g.tasks[i].GetId()] = ids
	}
	return out
}

// checkAncestry is rule 44: refuse a task whose required ancestors' minimal
// references alone (MinimalUpstreamRef) exceed task.MaxActivityInputBytes in
// one activity input, naming the first such task in declaration order and
// its required-ancestor count. It refuses only definitions that cannot
// succeed: whenever such a task runs, every required ancestor succeeded and
// contributed at least its minimal reference. Called after rules 5 and 37–42.
func checkAncestry(wf *orkstackv1.Orkflow) error {
	g := newAncestryGraph(wf)
	if err := g.weigh(wf.GetNamespace()); err != nil {
		return err
	}
	// The largest any input could carry: every reference at once. Below the
	// bound, no task can cross it, and nothing more is computed.
	var all int64
	for i := range g.tasks {
		all += g.weight[i]
	}
	if g.maxBase+all <= task.MaxActivityInputBytes {
		return nil
	}
	for i := range g.tasks {
		size, count, err := g.minimalInputBytes(i)
		if err != nil {
			return err
		}
		if size <= task.MaxActivityInputBytes {
			continue
		}
		t := g.tasks[i]
		return validationError(fmt.Sprintf("tasks[%d].needs", i), &ancestryError{message: fmt.Sprintf(
			"task %q has %d required ancestors (its needs through default joins), whose smallest possible "+
				"upstream references alone serialize to %d bytes, over the %d-byte activity input bound: "+
				"it could never be dispatched. Split the pipeline into composed Orkflows",
			t.GetId(), count, size, task.MaxActivityInputBytes)})
	}
	return nil
}

// MinimalInputBytes is rule 44's measure for task id of wf: task.PayloadSize
// of an ExecuteTaskInput carrying only its task id and the minimal reference
// of every required ancestor (a fan-out only as a direct need), and the
// number of required ancestors. Pure; for tests and tools.
func MinimalInputBytes(wf *orkstackv1.Orkflow, id string) (size int64, required int, err error) {
	g := newAncestryGraph(wf)
	if err := g.weigh(wf.GetNamespace()); err != nil {
		return 0, 0, err
	}
	i, ok := g.index[id]
	if !ok {
		return 0, 0, fmt.Errorf("no task %q", id)
	}
	return g.minimalInputBytes(i)
}

// ancestryGraph is the needs graph by declaration index, with each task's
// required set as a bitset, computed on demand and memoised.
type ancestryGraph struct {
	tasks []*orkstackv1.Task
	index map[string]int
	needs [][]int
	// req[i] is i's required ancestors as a bitset; nil until computed.
	req   [][]uint64
	done  []bool
	words int
	// weight[i] is i's minimal reference's marginal size, and kinds[i] its
	// kind; base[i] is the input with i's task id and one reference
	// (weigh).
	weight, base []int64
	kinds        []RefKind
	maxBase      int64
}

func newAncestryGraph(wf *orkstackv1.Orkflow) *ancestryGraph {
	tasks := wf.GetTasks()
	g := &ancestryGraph{
		tasks: tasks,
		index: make(map[string]int, len(tasks)),
		needs: make([][]int, len(tasks)),
		req:   make([][]uint64, len(tasks)),
		done:  make([]bool, len(tasks)),
		words: (len(tasks) + 63) / 64,
		kinds: make([]RefKind, len(tasks)),
	}
	for i, t := range tasks {
		g.index[t.GetId()] = i
		g.kinds[i] = refKindOf(t)
	}
	for i, t := range tasks {
		for _, need := range t.GetNeeds() {
			if j, ok := g.index[need]; ok && j != i {
				g.needs[i] = append(g.needs[i], j)
			}
		}
	}
	return g
}

// weigh measures every task's minimal reference once.
func (g *ancestryGraph) weigh(namespace string) error {
	g.weight = make([]int64, len(g.tasks))
	g.base = make([]int64, len(g.tasks))
	for i, t := range g.tasks {
		ref := MinimalUpstreamRef(g.kinds[i], namespace, t.GetId())
		marginal, err := refMarginal(ref)
		if err != nil {
			return fmt.Errorf("task %q: measure its upstream reference: %w", t.GetId(), err)
		}
		g.weight[i] = marginal
		// The input of a task whose id is t's own, with one reference: the
		// largest such base bounds every task's own id plus the list's frame.
		base, err := task.PayloadSize(&orkstackv1.ExecuteTaskInput{TaskId: t.GetId(),
			Upstream: []*orkstackv1.UpstreamRef{ref}})
		if err != nil {
			return fmt.Errorf("task %q: measure its activity input: %w", t.GetId(), err)
		}
		g.base[i] = base - marginal
		g.maxBase = max(g.maxBase, g.base[i])
	}
	return nil
}

// required is i's required-ancestor bitset (see RequiredAncestors). The graph
// is acyclic (rule 5), so the recursion ends; its depth is the longest needs
// chain.
func (g *ancestryGraph) required(i int) []uint64 {
	if g.done[i] {
		return g.req[i]
	}
	g.done[i] = true
	if g.tasks[i].GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED || len(g.needs[i]) == 0 {
		return nil
	}
	set := make([]uint64, g.words)
	for _, n := range g.needs[i] {
		set[n/64] |= 1 << (n % 64)
		for w, word := range g.required(n) {
			set[w] |= word
		}
	}
	g.req[i] = set
	return set
}

// minimalInputBytes is MinimalInputBytes by index: the input of task i with
// its own id and the minimal reference of each required ancestor, a fan-out
// counted only as a direct need. Exact: the list's first reference is in
// the base the second and later ones add their marginal to.
func (g *ancestryGraph) minimalInputBytes(i int) (size int64, required int, err error) {
	set := g.required(i)
	direct := map[int]bool{}
	for _, n := range g.needs[i] {
		direct[n] = true
	}
	refs := 0
	var sum int64
	for w, word := range set {
		for word != 0 {
			b := bits.TrailingZeros64(word)
			word &^= 1 << b
			a := w*64 + b
			required++
			if g.kinds[a] == RefFanOut && !direct[a] {
				continue
			}
			refs++
			sum += g.weight[a]
		}
	}
	own, err := task.PayloadSize(&orkstackv1.ExecuteTaskInput{TaskId: g.tasks[i].GetId()})
	if err != nil {
		return 0, 0, err
	}
	if refs == 0 {
		return own, required, nil
	}
	// The frame of a non-empty list beside the task id: what one reference
	// adds beyond its marginal. It does not depend on which reference.
	frame, err := g.listFrame(i)
	if err != nil {
		return 0, 0, err
	}
	return own + frame + sum, required, nil
}

// listFrame is what a non-empty upstream list adds to task i's input beyond
// its references' marginals: the field name, brackets and separators, less
// the one separator a single reference does not have.
func (g *ancestryGraph) listFrame(i int) (int64, error) {
	ref := MinimalUpstreamRef(RefFanOut, "", g.tasks[i].GetId())
	marginal, err := refMarginal(ref)
	if err != nil {
		return 0, err
	}
	own, err := task.PayloadSize(&orkstackv1.ExecuteTaskInput{TaskId: g.tasks[i].GetId()})
	if err != nil {
		return 0, err
	}
	one, err := task.PayloadSize(&orkstackv1.ExecuteTaskInput{TaskId: g.tasks[i].GetId(),
		Upstream: []*orkstackv1.UpstreamRef{ref}})
	if err != nil {
		return 0, err
	}
	return one - own - marginal, nil
}
