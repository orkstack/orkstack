package engine

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"weak"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A snapshot's task rows are the workflow's live rows: s.snapshot() builds a
// new Run, but its rows are the very messages RunWorkflow goes on changing.
// Copying them on every emit would cost O(rows) of workflow time per event,
// in a replay as much as in a run, so the local activity that encodes a
// snapshot reads them in place. That is safe only while the workflow is
// blocked on the activity, and the SDK does not promise that: when a cancel
// (or an attempt's deadline) reaches a local activity, Get returns at once
// while the activity's goroutine runs on. fail() then records the
// cancellation on the rows the activity is still encoding — a data race, and
// an object that can mix the rows before and after the cancel.
//
// A rowsLoan closes that without a copy and without a command: the workflow
// lends the snapshot's rows to the activity (lendRows), and once its Get has
// returned — whatever it returned — it takes them back (takeBack), which
// waits for the activity's read, if one is under way, and refuses every read
// after it. The activity reads only inside borrowRows. The wait is on an
// in-process encode that touches nothing else, so it can only delay the
// workflow, never change which commands it issues or what they say: a replay,
// where no activity runs, passes through it untouched, and the run is as
// deterministic as it was.
type rowsLoan struct {
	mu      sync.Mutex
	revoked bool
}

// errRowsTakenBack is what an activity attempt reports when the workflow
// moved on before the attempt read the rows. Nobody is waiting for the
// result any more, and a retry could only fail the same way.
var errRowsTakenBack = temporal.NewNonRetryableApplicationError(
	"the run moved on before its snapshot was written", "ROWS_TAKEN_BACK", errors.New("rows taken back"))

// lentRows maps a snapshot to its loan. Keys are weak: a snapshot nobody
// holds is collected with its rows, and runtime.AddCleanup then drops its
// entry. The entry has to outlive takeBack — an orphaned attempt can still
// be about to ask for it, and an unlisted snapshot reads as the caller's own —
// so it is the garbage collector, not the workflow, that retires it.
var lentRows sync.Map // weak.Pointer[orkstackv1.Run] -> *rowsLoan

// lendRows lends run's rows to the local activity about to be scheduled
// with it.
func lendRows(run *orkstackv1.Run) *rowsLoan {
	loan := &rowsLoan{}
	key := weak.Make(run)
	lentRows.Store(key, loan)
	runtime.AddCleanup(run, func(k weak.Pointer[orkstackv1.Run]) { lentRows.Delete(k) }, key)
	return loan
}

// rowsTakeBackProbeForTest, when set, is called by takeBack just before it
// waits: how a test knows the workflow has reached the wait. TEST-ONLY;
// atomic because an attempt the workflow gave up on can outlive the test.
var rowsTakeBackProbeForTest atomic.Pointer[func()]

// takeBack returns the rows to the workflow. It blocks until the activity is
// not reading them, and no read begins after it returns.
func (l *rowsLoan) takeBack() {
	if probe := rowsTakeBackProbeForTest.Load(); probe != nil {
		(*probe)()
	}
	l.mu.Lock()
	l.revoked = true
	l.mu.Unlock()
}

// borrowRows is the activity's way in to a lent snapshot's rows: it returns
// the release to call when its reading is done, or false when the workflow
// has taken them back. A snapshot nobody lent is the caller's own, and is
// borrowed freely.
//
// That default cannot hide a shared run. A Run shares its rows with another
// goroutine only if it is the very pointer that goroutine holds: a protobuf
// message is not copied by value (go vet's copylocks refuses it), and every
// copy the SDK, a data converter or an interceptor could make of a local
// activity's arguments, a clone or an encode and decode, is deep. The SDK
// passes the arguments in-process as the pointers the workflow gave it, which
// is what the loan relies on; TestSnapshotWrite_ACancelDuringTheEncode...
// fails when the pointer the activity reads is not the one lent.
func borrowRows(run *orkstackv1.Run) (release func(), ok bool) {
	value, lent := lentRows.Load(weak.Make(run))
	if !lent {
		return func() {}, true
	}
	loan := value.(*rowsLoan)
	loan.mu.Lock()
	if loan.revoked {
		loan.mu.Unlock()
		return nil, false
	}
	return loan.mu.Unlock, true
}

// snapshotReadHookForTest, when set, is called with the lent rows inside the
// activity's read, before it encodes them. TEST-ONLY; never set in
// production.
var snapshotReadHookForTest atomic.Pointer[func(run *orkstackv1.Run, sequence int64)]

// readLent runs read, which reads run's rows, while they are on loan to this
// activity. Everything that touches the rows happens inside read, and nothing
// that waits on the network does: the workflow may be waiting for this read to
// end.
func readLent[T any](run *orkstackv1.Run, sequence int64, read func() (T, error)) (T, error) {
	release, ok := borrowRows(run)
	if !ok {
		var zero T
		return zero, errRowsTakenBack
	}
	defer release()
	if hook := snapshotReadHookForTest.Load(); hook != nil {
		(*hook)(run, sequence)
	}
	return read()
}
