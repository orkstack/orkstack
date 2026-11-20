package identity

import (
	"reflect"
	"slices"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func container(id string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Needs: needs}
}

func awaited(id string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Needs: needs, Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}}
}

func composed(id string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Needs: needs, Orkflow: &orkstackv1.OrkflowRef{Name: "child", Version: "1"}}
}

func sleeping(id string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Needs: needs, Sleep: &orkstackv1.SleepSpec{}}
}

func fanOut(id string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Needs: needs, Each: &orkstackv1.EachSpec{Over: "{{ .inputs.xs }}", MaxItems: 3}}
}

func joined(t *orkstackv1.Task, join orkstackv1.JoinPolicy) *orkstackv1.Task { t.Join = join; return t }

func conditioned(t *orkstackv1.Task) *orkstackv1.Task { t.Condition = "{{ .inputs.go }}"; return t }

func ref(task, kind string) artifacts.FollowRef { return artifacts.FollowRef{TaskID: task, Kind: kind} }

// TestFollows is the table of "What a task follows": each join, each kind
// of need, through a need that has no record, and the completion list.
func TestFollows(t *testing.T) {
	const (
		c = artifacts.FollowContainer
		a = artifacts.FollowAwaited
		k = artifacts.FollowComposed
	)
	for _, tc := range []struct {
		name      string
		tasks     []*orkstackv1.Task
		follows   map[string][]artifacts.FollowRef
		completes []string
	}{
		{"a chain names its direct needs only",
			[]*orkstackv1.Task{container("build"), awaited("approve", "build"), container("deploy", "approve")},
			map[string][]artifacts.FollowRef{"approve": {ref("build", c)}, "deploy": {ref("approve", a)}},
			[]string{"build", "approve", "deploy"}},
		{"a composed need, and a task with no need follows the run's start",
			[]*orkstackv1.Task{composed("ship"), container("report", "ship"), container("alone")},
			map[string][]artifacts.FollowRef{"report": {ref("ship", k)}},
			[]string{"ship", "report", "alone"}},
		{"through a sleeping task to what it follows",
			[]*orkstackv1.Task{awaited("approve"), sleeping("hold", "approve"), container("deploy", "hold")},
			map[string][]artifacts.FollowRef{"deploy": {ref("approve", a)}},
			[]string{"approve", "deploy"}},
		{"through a fan-out, whose own entry is what each item follows",
			[]*orkstackv1.Task{awaited("approve"), fanOut("spread", "approve"), container("gather", "spread")},
			map[string][]artifacts.FollowRef{"spread": {ref("approve", a)}, "gather": {ref("approve", a)}},
			[]string{"approve", "gather"}},
		{"through two needs that share what they follow, named once",
			[]*orkstackv1.Task{awaited("approve"), sleeping("s1", "approve"), sleeping("s2", "approve"), container("deploy", "s1", "s2")},
			map[string][]artifacts.FollowRef{"deploy": {ref("approve", a)}},
			[]string{"approve", "deploy"}},
		{"none_failed follows only needs that always run",
			[]*orkstackv1.Task{awaited("approve"), conditioned(container("maybe")),
				joined(container("deploy", "approve", "maybe"), orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED)},
			map[string][]artifacts.FollowRef{"deploy": {ref("approve", a)}},
			[]string{"approve"}},
		{"any_failed and all_done follow nothing",
			[]*orkstackv1.Task{awaited("approve"),
				joined(container("on-fail", "approve"), orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED),
				joined(container("always", "approve"), orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE)},
			nil,
			[]string{"approve"}},
		{"a task behind one that may be skipped is not on the completion list",
			[]*orkstackv1.Task{conditioned(awaited("approve")), container("deploy", "approve")},
			map[string][]artifacts.FollowRef{"deploy": {ref("approve", a)}},
			nil},
	} {
		follows, completes := Follows(&orkstackv1.Orkflow{Tasks: tc.tasks})
		if !reflect.DeepEqual(follows, tc.follows) {
			t.Errorf("%s: follows %v; want %v", tc.name, follows, tc.follows)
		}
		if !slices.Equal(completes, tc.completes) {
			t.Errorf("%s: completes %v; want %v", tc.name, completes, tc.completes)
		}
	}
}

// TestUnguardedIdentities: a task that holds an identity is reported with
// each approval or composed task upstream it does not follow, and each
// sleeping task right before it; one whose barriers are all followed, by
// a chain of records, is not.
func TestUnguardedIdentities(t *testing.T) {
	holds := func(task *orkstackv1.Task) *orkstackv1.Task {
		task.Identities = []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts"}}
		return task
	}
	for _, tc := range []struct {
		name  string
		tasks []*orkstackv1.Task
		want  []UnguardedIdentity
	}{
		{"an approval three tasks upstream is followed through the chain",
			[]*orkstackv1.Task{awaited("approve"), container("a", "approve"), container("b", "a"), holds(container("deploy", "b"))}, nil},
		{"all_done does not follow the approval",
			[]*orkstackv1.Task{awaited("approve"), holds(joined(container("deploy", "approve"), orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE))},
			[]UnguardedIdentity{{Index: 1, Task: "deploy", Join: orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE, Unfollowed: []string{"approve"}}}},
		{"none_failed through a need that may be skipped",
			[]*orkstackv1.Task{composed("ship"), conditioned(container("maybe", "ship")), holds(joined(container("deploy", "maybe"), orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED))},
			[]UnguardedIdentity{{Index: 2, Task: "deploy", Join: orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED, Unfollowed: []string{"ship"}}}},
		{"directly behind a sleeping task, whose approval is still followed",
			[]*orkstackv1.Task{awaited("approve"), sleeping("hold", "approve"), holds(container("deploy", "hold"))},
			[]UnguardedIdentity{{Index: 2, Task: "deploy", BehindSleep: []string{"hold"}}}},
		{"a task that holds no identity is not reported",
			[]*orkstackv1.Task{awaited("approve"), joined(container("deploy", "approve"), orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE)}, nil},
	} {
		if got := UnguardedIdentities(&orkstackv1.Orkflow{Tasks: tc.tasks}); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: %+v; want %+v", tc.name, got, tc.want)
		}
	}
}
