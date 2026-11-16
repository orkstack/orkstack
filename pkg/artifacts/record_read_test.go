package artifacts

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/smithy-go"
)

// scriptedBlobs answers getStored from a script, one step per call, then
// repeats the last step. With end set, the call that reaches endAfter ends
// the attempt, so a read's deadline is the script's and never the
// runner's clock.
type scriptedBlobs struct {
	blobs
	mu       sync.Mutex
	steps    []error // nil: the object is there
	calls    int
	end      func()
	endAfter int
}

func (s *scriptedBlobs) getStored(ctx context.Context, key string, max int64) ([]byte, time.Time, error) {
	s.mu.Lock()
	i := min(s.calls, len(s.steps)-1)
	s.calls++
	step := s.steps[i]
	if s.end != nil && s.calls == s.endAfter {
		s.end()
	}
	s.mu.Unlock()
	if step != nil {
		return nil, time.Time{}, step
	}
	return []byte("{}"), time.Unix(1700000000, 0).UTC(), nil
}

// attemptCtx is an attempt whose deadline is an hour away, so no wait is
// cut short by a slow runner, and which ends, as at its deadline, when end
// is called.
type attemptCtx struct {
	context.Context
	deadline time.Time
	done     chan struct{}
	once     sync.Once
}

func newAttemptCtx() *attemptCtx {
	return &attemptCtx{Context: context.Background(), deadline: time.Now().Add(time.Hour), done: make(chan struct{})}
}

func (c *attemptCtx) Deadline() (time.Time, bool) { return c.deadline, true }
func (c *attemptCtx) Done() <-chan struct{}       { return c.done }
func (c *attemptCtx) end()                        { c.once.Do(func() { close(c.done) }) }
func (c *attemptCtx) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

var fast = RecordSpacings{MissConfirm: time.Millisecond, ReaskInitial: time.Millisecond, ReaskMax: 2 * time.Millisecond, AttemptMargin: 5 * time.Millisecond}

// TestReadObject_Answers: one row per answer of a read: found, a miss
// confirmed, a miss then found, a miss then an error, an error then found,
// errors to the attempt's end with their class, denied never reported as a
// timeout, and a cancellation as a cancellation.
func TestReadObject_Answers(t *testing.T) {
	refused := errors.New("connection refused")
	denied := &smithy.GenericAPIError{Code: "AccessDenied"}
	tests := []struct {
		name      string
		steps     []error
		wantFound bool
		wantClass string
		wantCalls int
	}{
		{"found", []error{nil}, true, "", 1},
		{"a miss, confirmed", []error{errBlobNotFound, errBlobNotFound}, false, "", 2},
		{"a miss, then found", []error{errBlobNotFound, nil}, true, "", 2},
		{"a miss, then an error", []error{errBlobNotFound, refused}, false, UnansweredUnavailable, -1},
		{"an error, then found", []error{refused, nil}, true, "", 2},
		{"errors to the end", []error{refused}, false, UnansweredUnavailable, -1},
		{"denied to the end", []error{denied}, false, UnansweredDenied, -1},
		{"denied, then a deadline", []error{denied, context.DeadlineExceeded}, false, UnansweredDenied, -1},
	}
	for _, tt := range tests {
		ctx := newAttemptCtx()
		// The attempt ends at the third ask: a read that asks again until
		// its attempt's end has asked more than once by then.
		b := &scriptedBlobs{steps: tt.steps, end: ctx.end, endAfter: 3}
		r := &recordReader{b: b, slots: newRecordSlots(4, 1), spacing: fast}
		body, _, found, err := r.readObject(ctx, SlotRun, "k", 1<<10)
		var un *RecordStoreUnanswered
		switch {
		case tt.wantClass != "":
			if !errors.As(err, &un) || un.Class != tt.wantClass {
				t.Errorf("%s: %v, want unanswered %s", tt.name, err, tt.wantClass)
			}
		case err != nil || found != tt.wantFound || (found && string(body) != "{}"):
			t.Errorf("%s: found %v, %q, %v", tt.name, found, body, err)
		}
		if tt.wantCalls > 0 && b.calls != tt.wantCalls {
			t.Errorf("%s: %d calls, want %d", tt.name, b.calls, tt.wantCalls)
		}
		if tt.wantCalls < 0 && tt.name != "a miss, then an error" && b.calls < 3 {
			t.Errorf("%s: %d calls; a read that ends with its attempt asks until then", tt.name, b.calls)
		}
	}
	b := &scriptedBlobs{steps: []error{context.Canceled}}
	r := &recordReader{b: b, slots: newRecordSlots(4, 1), spacing: fast}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := r.readObject(ctx, SlotRun, "k", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancellation = %v", err)
	}
}

// TestReadObject_DeniedOutlivesAnUnconfirmedMiss: a store that refused the
// credential and then answered a miss the attempt left no time to confirm
// is reported denied, never timeout; a store whose context ended for any
// reason is reported canceled when it was canceled.
func TestReadObject_DeniedOutlivesAnUnconfirmedMiss(t *testing.T) {
	denied := &smithy.GenericAPIError{Code: "AccessDenied"}
	slow := RecordSpacings{MissConfirm: 2 * time.Hour, ReaskInitial: time.Millisecond, ReaskMax: time.Millisecond, AttemptMargin: time.Millisecond}
	r := &recordReader{b: &scriptedBlobs{steps: []error{denied, errBlobNotFound}}, slots: newRecordSlots(4, 1), spacing: slow}
	// The miss's confirmation is longer than the whole attempt, whatever
	// the runner's clock does.
	ctx := newAttemptCtx()
	var un *RecordStoreUnanswered
	if _, _, _, err := r.readObject(ctx, SlotRun, "k", 1); !errors.As(err, &un) || un.Class != UnansweredDenied {
		t.Fatalf("denied then an unconfirmed miss = %v", err)
	}
	r = &recordReader{b: &scriptedBlobs{steps: []error{errors.New("transport closed")}}, slots: newRecordSlots(4, 1), spacing: fast}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, _, _, err := r.readObject(canceled, SlotRun, "k", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("an error after a cancellation = %v", err)
	}
}

// TestRecordReads_RefuseWhatCannotBeUsed: a slot class that is none, and
// spacings that would ask again without pausing, are refused, never a
// panic or a busy loop.
func TestRecordReads_RefuseWhatCannotBeUsed(t *testing.T) {
	s := newRecordSlots(1, 1)
	for _, c := range []SlotClass{0, 4, -1} {
		if _, err := s.acquire(context.Background(), c); err == nil {
			t.Errorf("class %d was given a slot", c)
		}
	}
	for _, sp := range []RecordSpacings{{}, {MissConfirm: time.Second, ReaskInitial: 0, ReaskMax: time.Second},
		{MissConfirm: time.Second, ReaskInitial: time.Second, ReaskMax: time.Millisecond}, {MissConfirm: -1, ReaskInitial: 1, ReaskMax: 1}} {
		if err := sp.check(); err == nil {
			t.Errorf("spacings %+v accepted", sp)
		}
	}
	if err := DefaultRecordSpacings.check(); err != nil {
		t.Fatal(err)
	}
}
