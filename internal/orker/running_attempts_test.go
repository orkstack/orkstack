package orker

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func attemptRef(task string, n int32) artifacts.AttemptRef {
	return artifacts.AttemptRef{RunID: "ork-r", TaskID: task, Attempt: n}
}

// TestRunningAttempts: an Orker lists an attempt from its claim until it has
// ended, and one whose container it still owes a removal for as long as it
// does; the list is bounded, and says when it was cut.
func TestRunningAttempts(t *testing.T) {
	owed := map[ContainerID]bool{}
	list := func(r *runningAttempts) ([]string, bool) {
		rows, cut := r.describe(func(id ContainerID) bool { return owed[id] })
		var out []string
		for _, a := range rows {
			s := fmt.Sprintf("%s/%s.%d", a.GetRunId(), a.GetTaskId(), a.GetOrdinal())
			if a.Item != nil {
				s = fmt.Sprintf("%s/%s[%d].%d", a.GetRunId(), a.GetTaskId(), a.GetItem(), a.GetOrdinal())
			}
			out = append(out, s)
		}
		return out, cut
	}
	is := func(t *testing.T, r *runningAttempts, want ...string) {
		t.Helper()
		if got, cut := list(r); fmt.Sprint(got) != fmt.Sprint(want) || cut {
			t.Fatalf("running = %v (cut %v), want %v", got, cut, want)
		}
	}
	var r runningAttempts
	is(t, &r)
	item := artifacts.AttemptRef{RunID: "ork-r", TaskID: "fan", Item: artifacts.ItemAt(0), Attempt: 2}
	r.begin(attemptRef("b", 1))
	r.begin(item)
	is(t, &r, "ork-r/b.1", "ork-r/fan[0].2")

	// Ended with nothing left behind.
	r.container(item, "c-fan")
	r.end(item)
	is(t, &r, "ork-r/b.1")

	// Ended, its container not removed yet.
	r.container(attemptRef("b", 1), "c-b")
	owed["c-b"] = true
	r.end(attemptRef("b", 1))
	is(t, &r, "ork-r/b.1")
	delete(owed, "c-b")
	is(t, &r)
	owed["c-b"] = true // gone for good: never listed again
	is(t, &r)

	// A container of this Orker's previous life that its boot reap left.
	owed["c-old"] = true
	r.orphan(attemptRef("old", 3), "c-old")
	is(t, &r, "ork-r/old.3")
	delete(owed, "c-old")
	is(t, &r)

	// A previous life whose containers could not be named: the list may be
	// short of them for as long as this process lives.
	r.incomplete()
	if got, cut := list(&r); len(got) != 0 || !cut {
		t.Fatalf("running = %v (cut %v), want it said to be incomplete", got, cut)
	}
}

func TestRunningAttempts_Bounded(t *testing.T) {
	var r runningAttempts
	for n := range maxRunningAttempts {
		r.begin(attemptRef("t", int32(n+1)))
	}
	if rows, cut := r.describe(nil); len(rows) != maxRunningAttempts || cut {
		t.Fatalf("at the bound: %d rows, cut %v", len(rows), cut)
	}
	r.begin(attemptRef("t", maxRunningAttempts+1))
	rows, cut := r.describe(nil)
	if len(rows) != maxRunningAttempts || !cut {
		t.Fatalf("past the bound: %d rows, cut %v; want %d and said", len(rows), cut, maxRunningAttempts)
	}
	// The longest ids a run and a task can have keep the answer a payload.
	long := &orkstackv1.DescribeOrkerResult{}
	item := int32(1 << 30)
	for range maxRunningAttempts {
		long.Running = append(long.Running, &orkstackv1.RunningAttempt{RunId: string(make([]byte, 200)), TaskId: string(make([]byte, 200)), Item: &item, Ordinal: 1 << 30})
	}
	if size := proto.Size(long); size > 128<<10 {
		t.Fatalf("a full list is %d bytes, over half a payload", size)
	}
}

// TestDescribeRunning_OwedByEitherLedger: a container the Orker owes a removal
// is listed whichever ledger owes it, the capacity's or the plain one.
func TestDescribeRunning_OwedByEitherLedger(t *testing.T) {
	d := &OCIDispatcher{OrkerID: "orker-a"}
	for task, owe := range map[string]func(ContainerID){
		"plain":    func(id ContainerID) { d.unremoved.owe(id, time.Time{}) },
		"capacity": nil,
	} {
		ref := attemptRef(task, 1)
		d.attempts.begin(ref)
		d.attempts.container(ref, ContainerID("c-"+task))
		d.attempts.end(ref)
		if owe != nil {
			owe(ContainerID("c-" + task))
		}
	}
	c := newCapacity(amount{milli: 1000, bytes: 1 << 30}, amount{}, 1)
	c.seed("c-capacity")
	rows, cut := d.describeRunning(c)
	if len(rows) != 2 || cut || rows[0].GetTaskId() != "capacity" || rows[1].GetTaskId() != "plain" {
		t.Fatalf("running = %v, %v; want both", rows, cut)
	}
}

// TestRunningAttempts_AnAttemptWhoseContainerIsGoneLeavesNothing: the list
// holds nothing of an attempt that ended with its container removed, whether
// or not anyone asks for it.
func TestRunningAttempts_AnAttemptWhoseContainerIsGoneLeavesNothing(t *testing.T) {
	d := &OCIDispatcher{OrkerID: "orker-a", Claims: artifacts.NewMemoryAttemptStore()}
	o := d.newAttemptOrdinal(t.Context(), &orkstackv1.ExecuteTaskInput{RunId: "ork-r", TaskId: "b"}, 1)
	if f := o.Claim(t.Context()); f != nil {
		t.Fatal(f)
	}
	o.ContainerCreated()
	o.Container("c-b")
	if rows, _ := d.describeRunning(nil); len(rows) != 1 || rows[0].GetOrdinal() != 1 {
		t.Fatalf("running = %v, want the claimed attempt", rows)
	}
	o.ContainerRemoved(nil)
	o.Release(t.Context())
	if len(d.attempts.attempts) != 0 {
		t.Fatalf("left %v", d.attempts.attempts)
	}
	// One whose removal failed stands while it is owed.
	o = d.newAttemptOrdinal(t.Context(), &orkstackv1.ExecuteTaskInput{RunId: "ork-r", TaskId: "b"}, 1)
	if f := o.Claim(t.Context()); f != nil {
		t.Fatal(f)
	}
	o.ContainerCreated()
	o.Container("c-b2")
	d.unremoved.owe("c-b2", time.Time{})
	o.ContainerRemoved(errors.New("refused"))
	o.Release(t.Context())
	if rows, _ := d.describeRunning(nil); len(rows) != 1 || rows[0].GetOrdinal() != 2 {
		t.Fatalf("running = %v, want the attempt whose container is owed a removal", rows)
	}
}

// TestKeepReaped: the boot reap's leftovers are listed as running, and one
// that cannot be named makes the list say it may be short.
func TestKeepReaped(t *testing.T) {
	a := &Activities{dispatcher: &OCIDispatcher{OrkerID: "orker-a"}}
	a.keepReaped([]OwnContainer{{ID: "c1", RunID: "ork-r", TaskID: "b", Attempt: "4"}, {ID: "c2", RunID: "ork-r", TaskID: "c", Attempt: "1"}},
		false, []ContainerID{"c1"})
	a.dispatcher.unremoved.owe("c1", time.Time{})
	if rows, cut := a.dispatcher.describeRunning(nil); len(rows) != 1 || rows[0].GetTaskId() != "b" || rows[0].GetOrdinal() != 4 || cut {
		t.Fatalf("running = %v, %v; want the one that stayed", rows, cut)
	}
	// A container of no attempt (a probe's) holds no attempt.
	a.keepReaped([]OwnContainer{{ID: "c3"}}, false, []ContainerID{"c3"})
	if _, cut := a.dispatcher.describeRunning(nil); cut {
		t.Fatal("a container that ran no attempt made the list incomplete")
	}
	for _, unnamed := range []OwnContainer{{ID: "c4", RunID: "ork-r", TaskID: "b", Attempt: "?"}, {ID: "c4", TaskID: "b"}, {ID: "c4", Attempt: "2"}} {
		other := &Activities{dispatcher: &OCIDispatcher{OrkerID: "orker-a"}}
		other.keepReaped([]OwnContainer{unnamed}, false, []ContainerID{"c4"})
		if _, cut := other.dispatcher.describeRunning(nil); !cut {
			t.Fatalf("an attempt's container whose labels do not name it (%+v) left the list whole", unnamed)
		}
	}
	a.keepReaped(nil, true, []ContainerID{"c9"})
	if _, cut := a.dispatcher.describeRunning(nil); !cut {
		t.Fatal("a container that cannot be named left the list whole")
	}
}
