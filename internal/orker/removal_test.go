package orker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestUnremovedContainers_RetrySettlesOnlyWhatIsGone(t *testing.T) {
	var owed unremovedContainers
	owed.owe("refused", time.Time{})
	owed.owe("gone", time.Time{})
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }

	settled, failed := owed.retry(context.Background(), clock, func(_ context.Context, id ContainerID) error {
		if id == "refused" {
			return errors.New("removal refused")
		}
		return nil
	})
	if settled != 1 || len(failed) != 1 || owed.pending() != 1 {
		t.Fatalf("settled %d, failed %v, pending %d; want 1, one failure naming it, 1", settled, failed, owed.pending())
	}
	if !strings.Contains(failed[0].Error(), "refused") {
		t.Errorf("the failure does not name its container: %v", failed[0])
	}

	settled, failed = owed.retry(context.Background(), clock, func(context.Context, ContainerID) error { return nil })
	if settled != 1 || len(failed) != 0 || owed.pending() != 0 {
		t.Fatalf("settled %d, failed %v, pending %d after the daemon relented", settled, failed, owed.pending())
	}
}

// "No such container" says nothing about a create the daemon has not finished,
// so an unanswered create stays owed for the window whatever a removal says, and
// is settled by the first one after.
func TestUnremovedContainers_AnUnansweredCreateStaysOwedUntilItSettles(t *testing.T) {
	var owed unremovedContainers
	start := time.Unix(1000, 0)
	now := start
	clock := func() time.Time { return now }
	owed.owe("ork-late", start.Add(createSettleWindow))
	removes := 0
	remove := func(context.Context, ContainerID) error { removes++; return nil }

	if settled, _ := owed.retry(context.Background(), clock, remove); settled != 0 || owed.pending() != 1 {
		t.Fatalf("settled %d, pending %d inside the window", settled, owed.pending())
	}
	now = start.Add(createSettleWindow - time.Second)
	if settled, _ := owed.retry(context.Background(), clock, remove); settled != 0 || owed.pending() != 1 {
		t.Fatalf("settled %d, pending %d just inside the window", settled, owed.pending())
	}
	now = start.Add(createSettleWindow)
	if settled, _ := owed.retry(context.Background(), clock, remove); settled != 1 || owed.pending() != 0 {
		t.Fatalf("settled %d, pending %d at the window's end", settled, owed.pending())
	}
	if removes != 3 {
		t.Errorf("removes = %d, want one on every pass", removes)
	}
}

func TestUnremovedContainers_OneWedgedRemovalDoesNotStarveTheRest(t *testing.T) {
	var owed unremovedContainers
	owed.owe("a", time.Time{})
	owed.owe("b", time.Time{})
	var deadlines []bool
	settled, _ := owed.retry(context.Background(), time.Now, func(ctx context.Context, _ ContainerID) error {
		_, has := ctx.Deadline()
		deadlines = append(deadlines, has)
		return nil
	})
	if settled != 2 {
		t.Fatalf("settled %d, want 2", settled)
	}
	for _, has := range deadlines {
		if !has {
			t.Error("a removal ran with no budget of its own")
		}
	}
}

func TestUnremovedContainers_AStoppedRetryLeavesTheRestOwed(t *testing.T) {
	var owed unremovedContainers
	owed.owe("a", time.Time{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	settled, failed := owed.retry(ctx, time.Now, func(context.Context, ContainerID) error {
		t.Error("a retry on a stopped context asked the runtime")
		return nil
	})
	if settled != 0 || len(failed) != 0 || owed.pending() != 1 {
		t.Fatalf("settled %d, failed %v, pending %d", settled, failed, owed.pending())
	}
}

// removalRecorder is a runtime that fails the first removals it is asked for.
type removalRecorder struct {
	retryingRuntime
	mu        sync.Mutex
	failFirst int
	asked     []ContainerID
	createID  ContainerID
	createErr error
}

func (r *removalRecorder) Create(context.Context, ContainerSpec) (ContainerID, error) {
	return r.createID, r.createErr
}

func (r *removalRecorder) Remove(_ context.Context, id ContainerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.asked = append(r.asked, id)
	if r.failFirst > 0 {
		r.failFirst--
		return errors.New("removal refused")
	}
	return nil
}

func (r *removalRecorder) askedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.asked)
}

func removalDispatcher(t *testing.T, runtime ContainerRuntime) *OCIDispatcher {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return &OCIDispatcher{
		Runtime: runtime, Store: store, Refs: artifacts.MemoryRefPredictor{},
		Policy: DefaultSecurityPolicy(), WorkRoot: t.TempDir(), OrkerID: "orker-removal-test", BuildID: "test",
	}
}

// A create that fails hands back what it may have made, and the attempt owes
// exactly what the failure leaves unknown.
func TestExecute_AFailedCreateIsRemovedUnderTheNameItAskedFor(t *testing.T) {
	refused := errors.New("create container: docker exited 125: refused")
	unanswered := ErrCreateUnanswered
	for _, tt := range []struct {
		name      string
		id        ContainerID
		createErr error
		failFirst int
		wantAsked int
		wantOwed  int
	}{
		{name: "a refusal the removal confirms is settled", id: "ork-x", createErr: refused, wantAsked: 1},
		{name: "a refusal whose removal failed stays owed", id: "ork-x", createErr: refused, failFirst: 1, wantAsked: 1, wantOwed: 1},
		{name: "an unanswered create is only owed: a removal now proves nothing", id: "ork-x", createErr: unanswered, wantOwed: 1},
		{name: "a failure before any name was chosen owes nothing", createErr: refused},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runtime := &removalRecorder{createID: tt.id, createErr: tt.createErr, failFirst: tt.failFirst}
			d := removalDispatcher(t, runtime)
			if _, err := d.Execute(context.Background(), baseExportInput("create-fails")); err == nil {
				t.Fatal("Execute succeeded on a failed create")
			}
			if got := runtime.askedCount(); got != tt.wantAsked {
				t.Errorf("removals asked = %d, want %d", got, tt.wantAsked)
			}
			if got := d.unremoved.pending(); got != tt.wantOwed {
				t.Errorf("removals owed = %d, want %d", got, tt.wantOwed)
			}
		})
	}
}

// With no capacity declared, the retry still runs, and asks nothing while
// nothing is owed.
func TestActivities_RetriesARefusedRemovalWithNoCapacityDeclared(t *testing.T) {
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runtime := &removalRecorder{failFirst: 2}
	a := NewActivitiesWithOptions(store, Options{Runtime: runtime, WorkRoot: t.TempDir(), OrkerID: "orker-removal-retry"})
	if a.capacity != nil {
		t.Fatal("the case needs an Orker that declares no capacity")
	}
	a.removalRetryEvery = time.Millisecond
	a.startRemovalRetry()
	defer a.Close()
	a.dispatcher.unremoved.owe("refused", time.Time{})

	deadline := time.Now().Add(30 * time.Second)
	for a.dispatcher.unremoved.pending() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the removal is still owed after %d tries", runtime.askedCount())
		}
		time.Sleep(time.Millisecond)
	}
	if got := runtime.askedCount(); got != 3 {
		t.Errorf("tries = %d, want two refused and the one that worked", got)
	}
}

// A declared capacity keeps retrying its own: the container stays reserved until
// it is gone, and is not tried a second time by the other ledger.
func TestExecute_ADeclaredCapacityAloneOwesARefusedRemoval(t *testing.T) {
	limit := amount{milli: 4000, bytes: 1 << 32}
	capacity := newCapacity(limit, amount{milli: 1000, bytes: 1 << 30}, 2)
	admission, err := capacity.admit(false, amount{milli: 1000, bytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &removalRecorder{failFirst: 1}
	runtime.createID = "c1"
	d := removalDispatcher(t, &exitingRuntime{removalRecorder: runtime})
	ctx := withAdmission(context.Background(), admission)
	in := baseExportInput("capacity-keeps")
	_, _ = d.Execute(ctx, in)
	if got := d.unremoved.pending(); got != 0 {
		t.Errorf("the dispatcher's own ledger owes %d; a capacity retries its own", got)
	}
	if got := capacity.pendingRemovals(); got != 1 {
		t.Errorf("the capacity owes %d, want the one container it holds a reservation for", got)
	}
}

// exitingRuntime is a removalRecorder whose container exits 1 at once.
type exitingRuntime struct{ *removalRecorder }

func (r *exitingRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return ExitStatus{Code: 1}, nil
}

// The real CLI's answers map to what Create promises: a refusal names the
// container it may have left, and a CLI stopped before it answered says so.
func TestDockerRuntime_CreateFailuresNameTheContainer(t *testing.T) {
	spec := validSpec()
	fake := newCountingDocker(t)
	fake.set("createfail", "")
	id, err := (&DockerRuntime{Bin: fake.bin}).Create(context.Background(), spec)
	if err == nil || errors.Is(err, ErrCreateUnanswered) {
		t.Fatalf("Create = %v, want a refusal that the daemon answered", err)
	}
	if !strings.HasPrefix(string(id), "ork-") {
		t.Errorf("the failed create returned id %q, want the name it asked for", id)
	}

	fake = newCountingDocker(t)
	fake.set("createhang", "")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	id, err = (&DockerRuntime{Bin: fake.bin}).Create(ctx, spec)
	if !errors.Is(err, ErrCreateUnanswered) {
		t.Fatalf("Create past its deadline = %v, want ErrCreateUnanswered", err)
	}
	if !strings.HasPrefix(string(id), "ork-") {
		t.Errorf("the unanswered create returned id %q, want the name it asked for", id)
	}
}
