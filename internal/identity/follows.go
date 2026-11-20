package identity

import (
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// What a task follows (docs/TASK_IDENTITY.md, "What a task follows"): the
// tasks of its own run whose record of success must be there before it
// may start, hold an identity or be recorded, computed once from the
// pinned definition. Only a task that records its result has a record: a
// container task that is not a fan-out, an awaited task, a composed task.
// A sleeping task and a fan-out have none, so a task follows through them
// what they follow. A record stands for the chain behind it, so a task
// names its direct needs only.

// RecordKind is the kind of record t's success leaves (FollowContainer,
// FollowAwaited or FollowComposed), or "" for a task that leaves none: a
// sleeping task, or a fan-out, whose items are recorded instead.
func RecordKind(t *orkstackv1.Task) string {
	switch {
	case t.GetSleep() != nil:
		return ""
	case t.GetAwait() != nil:
		return artifacts.FollowAwaited
	case t.GetOrkflow() != nil:
		return artifacts.FollowComposed
	case t.GetEach() != nil:
		return ""
	default:
		return artifacts.FollowContainer
	}
}

// Follows is what each task of wf that runs a container, is awaited or is
// composed follows, in the order its needs are declared, naming tasks of
// this run only; a task that follows nothing of its run follows what the
// run's start follows, and has no entry. A fan-out's entry is what each of
// its items follows. Completes is the run's completion list: every task
// that always runs and leaves a record, in declaration order.
func Follows(wf *orkstackv1.Orkflow) (follows map[string][]artifacts.FollowRef, completes []string) {
	f := newFollower(wf)
	follows = map[string][]artifacts.FollowRef{}
	for _, t := range wf.GetTasks() {
		if t.GetSleep() != nil {
			continue
		}
		if refs := f.of(t.GetId()); len(refs) > 0 {
			follows[t.GetId()] = refs
		}
		if RecordKind(t) != "" && f.alwaysRuns(t.GetId()) {
			completes = append(completes, t.GetId())
		}
	}
	if len(follows) == 0 {
		follows = nil
	}
	return follows, completes
}

type follower struct {
	tasks  map[string]*orkstackv1.Task
	always map[string]bool
	memo   map[string][]artifacts.FollowRef
}

func newFollower(wf *orkstackv1.Orkflow) *follower {
	f := &follower{tasks: map[string]*orkstackv1.Task{}, always: map[string]bool{}, memo: map[string][]artifacts.FollowRef{}}
	for _, t := range wf.GetTasks() {
		f.tasks[t.GetId()] = t
	}
	return f
}

// alwaysRuns is a task that is never skipped in a run that runs at all: no
// condition, the default join, and every need always runs. A definition is
// a DAG (validated at create), so the recursion ends.
func (f *follower) alwaysRuns(id string) bool {
	if v, ok := f.always[id]; ok {
		return v
	}
	t := f.tasks[id]
	v := t != nil && t.GetCondition() == "" && t.GetJoin() == orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED
	for _, need := range t.GetNeeds() {
		v = v && f.alwaysRuns(need)
	}
	f.always[id] = v
	return v
}

// of is what task id follows, by its own join.
func (f *follower) of(id string) []artifacts.FollowRef {
	if refs, ok := f.memo[id]; ok {
		return refs
	}
	t := f.tasks[id]
	var out []artifacts.FollowRef
	seen := map[string]bool{}
	add := func(r artifacts.FollowRef) {
		if !seen[r.TaskID] {
			seen[r.TaskID] = true
			out = append(out, r)
		}
	}
	switch t.GetJoin() {
	case orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED, orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE:
		// These joins start the task whatever became of its needs.
	default:
		noneFailed := t.GetJoin() == orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED
		for _, need := range t.GetNeeds() {
			n := f.tasks[need]
			// Under none_failed a need that may be skipped constrains
			// nothing, and neither does what it follows: the task may start
			// with it skipped.
			if n == nil || (noneFailed && !f.alwaysRuns(need)) {
				continue
			}
			if kind := RecordKind(n); kind != "" {
				add(artifacts.FollowRef{TaskID: need, Kind: kind})
				continue
			}
			for _, r := range f.of(need) {
				add(r)
			}
		}
	}
	f.memo[id] = out
	return out
}

// FollowedTasks is refs as task ids, for a plan to show.
func FollowedTasks(refs []artifacts.FollowRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.TaskID)
	}
	return out
}

// UnguardedIdentity is a task that holds an identity and may start without
// a barrier its author may expect: Unfollowed are the awaited and composed
// tasks upstream of it that it does not follow, under its join or through a
// need that may be skipped; BehindSleep are the sleeping tasks it needs
// directly, which have no record, so a hold-off there does not hold against
// a party that answers the run's workflow tasks.
type UnguardedIdentity struct {
	Index       int
	Task        string
	Join        orkstackv1.JoinPolicy
	Unfollowed  []string
	BehindSleep []string
}

// UnguardedIdentities is wf's tasks that hold an identity and are
// UnguardedIdentity, in declaration order: what create and validate warn of.
func UnguardedIdentities(wf *orkstackv1.Orkflow) []UnguardedIdentity {
	f := newFollower(wf)
	var out []UnguardedIdentity
	for i, t := range wf.GetTasks() {
		if !TaskHoldsIdentity(t) {
			continue
		}
		guarded := f.chain(t.GetId())
		u := UnguardedIdentity{Index: i, Task: t.GetId(), Join: t.GetJoin()}
		for _, up := range f.upstream(t.GetId()) {
			if kind := RecordKind(f.tasks[up]); (kind == artifacts.FollowAwaited || kind == artifacts.FollowComposed) && !guarded[up] {
				u.Unfollowed = append(u.Unfollowed, up)
			}
		}
		for _, need := range t.GetNeeds() {
			if f.tasks[need].GetSleep() != nil {
				u.BehindSleep = append(u.BehindSleep, need)
			}
		}
		if len(u.Unfollowed) > 0 || len(u.BehindSleep) > 0 {
			out = append(out, u)
		}
	}
	return out
}

// chain is every task whose record id's start stands on: what it follows,
// and what each of those followed, since a record exists only once what its
// task followed was on record.
func (f *follower) chain(id string) map[string]bool {
	seen := map[string]bool{}
	var walk func(string)
	walk = func(id string) {
		for _, r := range f.of(id) {
			if !seen[r.TaskID] {
				seen[r.TaskID] = true
				walk(r.TaskID)
			}
		}
	}
	walk(id)
	return seen
}

// upstream is every task id needs, directly or not, in the order a walk of
// its needs first meets them.
func (f *follower) upstream(id string) []string {
	seen := map[string]bool{}
	var out []string
	var walk func(string)
	walk = func(id string) {
		for _, need := range f.tasks[id].GetNeeds() {
			if !seen[need] && f.tasks[need] != nil {
				seen[need] = true
				out = append(out, need)
				walk(need)
			}
		}
	}
	walk(id)
	return out
}
