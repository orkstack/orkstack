package engine

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// rowStatuses is a snapshot's task rows as "id=STATUS ...", in order: the
// part of a snapshot a cancellation changes.
func rowStatuses(run *orkstackv1.Run) string {
	parts := make([]string, 0, len(run.GetTaskRuns()))
	for _, row := range run.GetTaskRuns() {
		parts = append(parts, row.GetTaskId()+"="+row.GetStatus().String())
	}
	return strings.Join(parts, " ")
}

func loanRun() *orkstackv1.Run {
	return &orkstackv1.Run{Id: "ork-1-1", Namespace: "default", TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "a", Status: orkstackv1.Status_STATUS_RUNNING},
		{TaskId: "b", Status: orkstackv1.Status_STATUS_PENDING},
	}}
}

// TestRowsLoan_TakeBackWaitsForTheReadAndRefusesTheNext: the workflow takes
// its rows back only once the activity's read has ended, and an attempt that
// asks afterwards is refused — never handed rows the workflow is about to
// change.
func TestRowsLoan_TakeBackWaitsForTheReadAndRefusesTheNext(t *testing.T) {
	run := loanRun()
	loan := lendRows(run)

	release, ok := borrowRows(run)
	if !ok {
		t.Fatal("the first borrow of a lent snapshot was refused")
	}
	asked := make(chan struct{})
	rowsTakeBackProbeForTest.Store(ptrTo(func() { close(asked) }))
	defer rowsTakeBackProbeForTest.Store(nil)
	taken := make(chan struct{})
	go func() {
		loan.takeBack()
		close(taken)
	}()
	// The take-back has begun (it was observed asking), and a bounded window
	// shows it still waiting for the read.
	select {
	case <-asked:
	case <-time.After(30 * time.Second):
		t.Fatal("takeBack never began")
	}
	select {
	case <-taken:
		t.Fatal("takeBack returned while the activity was still reading the rows")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case <-taken:
	case <-time.After(30 * time.Second):
		t.Fatal("takeBack did not return after the read ended")
	}
	if _, ok := borrowRows(run); ok {
		t.Fatal("a borrow after takeBack was allowed")
	}
}

// TestRowsLoan_NoReaderIsInsideOnceTakeBackReturns: with attempts racing to
// read, none is inside its read when takeBack returns and none enters after
// it, however the goroutines interleave; the rows' "change" after takeBack is
// a plain write the race detector would flag against any straggler.
func TestRowsLoan_NoReaderIsInsideOnceTakeBackReturns(t *testing.T) {
	for range 200 {
		run := loanRun()
		loan := lendRows(run)
		var inside, entered atomic.Int64
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					release, ok := borrowRows(run)
					if !ok {
						return
					}
					inside.Add(1)
					entered.Add(1)
					_ = run.GetTaskRuns()[0].GetStatus() // a read of a row
					inside.Add(-1)
					release()
				}
			}()
		}
		for entered.Load() == 0 {
			runtime.Gosched()
		}
		loan.takeBack()
		if n := inside.Load(); n != 0 {
			t.Fatalf("%d readers were inside their read when takeBack returned", n)
		}
		stopped := entered.Load()
		run.TaskRuns[0].Status = orkstackv1.Status_STATUS_CANCELED // the failure path's write
		wg.Wait()
		if entered.Load() != stopped {
			t.Fatalf("a reader entered after takeBack returned (%d, then %d)", stopped, entered.Load())
		}
	}
}

// TestRowsLoan_ASnapshotNobodyLentIsBorrowedFreely: callers that hand the
// activities a snapshot of their own (a test, a tool) are not put through a
// loan they never made.
func TestRowsLoan_ASnapshotNobodyLentIsBorrowedFreely(t *testing.T) {
	run := loanRun()
	for range 2 {
		release, ok := borrowRows(run)
		if !ok {
			t.Fatal("a snapshot nobody lent was refused")
		}
		release()
	}
}

// TestRowsLoan_TheEntryIsRetiredWithTheSnapshot: a loan outlives takeBack —
// an orphaned attempt may still ask — so what retires it is the snapshot
// being collected, not the workflow, and a run's many snapshots leave nothing
// behind.
func TestRowsLoan_TheEntryIsRetiredWithTheSnapshot(t *testing.T) {
	count := func() int {
		n := 0
		lentRows.Range(func(_, _ any) bool { n++; return true })
		return n
	}
	before := count()
	func() {
		for range 50 {
			run := loanRun()
			lendRows(run).takeBack()
		}
	}()
	deadline := time.Now().Add(30 * time.Second)
	for count() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d entries are still listed after their snapshots were dropped (%d before)", count(), before)
		}
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSnapshotActivities_RefuseARunTheWorkflowTookBack: an attempt that
// reaches the activity after the workflow took the rows back writes nothing
// and says why, as an error Temporal will not retry.
func TestSnapshotActivities_RefuseARunTheWorkflowTookBack(t *testing.T) {
	store := newMemSnapshotStore()
	a := NewSnapshotActivities(store)
	run := loanRun()
	lendRows(run).takeBack()

	_, err := a.WriteSnapshot(context.Background(), run, 3)
	if !errors.Is(err, errRowsTakenBack) {
		t.Fatalf("WriteSnapshot = %v, want errRowsTakenBack", err)
	}
	_, err = a.WriteSnapshotOrOverlay(context.Background(), run, 4, SnapshotBase{}, DefaultOverlayPolicy())
	if !errors.Is(err, errRowsTakenBack) {
		t.Fatalf("WriteSnapshotOrOverlay = %v, want errRowsTakenBack", err)
	}
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) || !applicationErr.NonRetryable() {
		t.Fatalf("error %v is not a non-retryable application error", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.objects) != 0 {
		t.Fatalf("a refused attempt wrote %d objects", len(store.objects))
	}
}

// TestSnapshotWrite_ACancelDuringTheEncodeNeverChangesTheRowsBeingEncoded: a
// cancel reaches the workflow while the snapshot activity is encoding the
// run's live rows. Get returns at once, and the failure path would record
// the cancellation on those very rows — mid-encode, a data race and a
// snapshot that can mix both states. The workflow instead waits at the end of
// the call for the encode to finish: the rows stay exactly as they were for
// as long as the activity reads them, and the object it wrote shows the
// state at the event.
//
// The activity holds its read open until the workflow is observed waiting
// (its take-back probe), or until the rows are seen to change.
func TestSnapshotWrite_ACancelDuringTheEncodeNeverChangesTheRowsBeingEncoded(t *testing.T) {
	store := newMemSnapshotStore()
	h := newRefusalHarnessOver(t, store)

	reached := make(chan struct{})
	var reachedOnce, cancelOnce sync.Once
	rowsTakeBackProbeForTest.Store(ptrTo(func() { reachedOnce.Do(func() { close(reached) }) }))
	defer rowsTakeBackProbeForTest.Store(nil)

	var (
		mu       sync.Mutex
		before   string
		changed  string
		timedOut bool
		hookSeq  int64
	)
	snapshotReadHookForTest.Store(ptrTo(func(run *orkstackv1.Run, sequence int64) {
		if !strings.Contains(rowStatuses(run), "STATUS_RUNNING") {
			return
		}
		cancelOnce.Do(func() {
			mu.Lock()
			before, hookSeq = rowStatuses(run), sequence
			mu.Unlock()
			h.env.CancelWorkflow()
			// Keep the read open until the workflow is seen at its take-back,
			// then a little longer: a workflow that went on instead of waiting
			// would have recorded the cancellation on the rows by now.
			deadline := time.NewTimer(30 * time.Second)
			defer deadline.Stop()
			var held <-chan time.Time
			waiting := reached
			for {
				select {
				case <-waiting:
					waiting, held = nil, time.After(150*time.Millisecond)
				case <-held:
					return
				case <-deadline.C:
					mu.Lock()
					timedOut = true
					mu.Unlock()
					return
				case <-time.After(2 * time.Millisecond):
					if now := rowStatuses(run); now != before {
						mu.Lock()
						changed = now
						mu.Unlock()
						return
					}
				}
			}
		})
	}))
	defer snapshotReadHookForTest.Store(nil)

	h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, guardCompile(t, loanDocument),
		(*orkstackv1.Payload)(nil), guardPlan(t, DefaultHistoryMaxEvents, DefaultHistoryMaxBytes),
		(*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the run did not complete")
	}

	mu.Lock()
	defer mu.Unlock()
	if before == "" {
		t.Fatal("no snapshot with a running task was written, so no cancel was sent")
	}
	if changed != "" {
		t.Fatalf("the rows changed from %q to %q while the snapshot write was reading them", before, changed)
	}
	if timedOut {
		t.Fatal("the workflow never reached its take-back: it did not wait for the write")
	}
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run ended without the cancel being acted on")
	}

	// The object the abandoned write left is the state at the event, whole.
	var written []byte
	deadline := time.Now().Add(30 * time.Second)
	for written == nil {
		store.mu.Lock()
		for uri, data := range store.objects {
			if strings.HasSuffix(uri, "/"+strconv.FormatInt(hookSeq, 10)+".pb") {
				written = data
			}
		}
		store.mu.Unlock()
		if written == nil {
			if time.Now().After(deadline) {
				t.Fatalf("snapshot %d was never written", hookSeq)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	got := &orkstackv1.Run{}
	if err := proto.Unmarshal(written, got); err != nil {
		t.Fatal(err)
	}
	if rowStatuses(got) != before {
		t.Fatalf("the snapshot written for event %d shows %q, want the state at the event, %q", hookSeq, rowStatuses(got), before)
	}
}

const loanDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: loan
tasks:
  - id: first
    uses: shell
    with: {op: exec}
  - id: second
    uses: shell
    needs: [first]
    with: {op: exec}
`

func ptrTo[T any](v T) *T { return &v }
