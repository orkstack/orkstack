package api

import (
	"container/list"
	"maps"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
)

const (
	// recordedPerCaller is how many calls one caller has at once; the bucket
	// beside it gives one a second.
	recordedPerCaller = 4
	// recordedRunReads is how many reads a minute one run's rows cost over
	// every caller, and recordedReplicaReads how many a second this replica
	// makes for these calls; what its cache answers costs neither.
	recordedRunReads     = 400
	recordedReplicaReads = 50
	maxRecordedRows      = 4096
)

// recordedReads bounds, in this replica's memory, what these calls cost the
// store, and keeps what they found: a record never changes.
type recordedReads struct {
	callers *identity.MintLimits
	replica *rate.Limiter
	now     func() time.Time

	mu     sync.Mutex
	inCall map[string]int
	runs   map[string]*rate.Limiter
	order  *list.List
	byKey  map[string]*list.Element
}

type recordedRow struct {
	key string
	row *orkstackv1.RecordedResult
}

func newRecordedReads(now func() time.Time) *recordedReads {
	if now == nil {
		now = time.Now
	}
	return &recordedReads{callers: identity.NewReadLimits(now), replica: rate.NewLimiter(recordedReplicaReads, recordedReplicaReads), now: now,
		inCall: map[string]int{}, runs: map[string]*rate.Limiter{}, order: list.New(), byKey: map[string]*list.Element{}}
}

// enter takes one of caller's calls, and answers what gives it back.
func (l *recordedReads) enter(caller string) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inCall[caller] >= recordedPerCaller {
		return nil, false
	}
	if _, ok := l.callers.TakePrincipal(caller); !ok {
		return nil, false
	}
	l.inCall[caller]++
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.inCall[caller]--; l.inCall[caller] == 0 {
			delete(l.inCall, caller)
		}
	}, true
}

// spend takes n of run's reads of this minute.
func (l *recordedReads) spend(run string, n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.runs[run]
	if !ok {
		if len(l.runs) >= maxRecordedRuns {
			maps.DeleteFunc(l.runs, func(_ string, r *rate.Limiter) bool { return r.TokensAt(l.now()) >= recordedRunReads })
		}
		if len(l.runs) >= maxRecordedRuns {
			return false
		}
		lim = rate.NewLimiter(rate.Every(time.Minute/recordedRunReads), recordedRunReads)
		l.runs[run] = lim
	}
	return lim.AllowN(l.now(), n)
}

func (l *recordedReads) kept(key string) (*orkstackv1.RecordedResult, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.byKey[key]
	if !ok {
		return nil, false
	}
	l.order.MoveToFront(e)
	return proto.Clone(e.Value.(*recordedRow).row).(*orkstackv1.RecordedResult), true
}

func (l *recordedReads) keep(key string, row *orkstackv1.RecordedResult) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.byKey[key]; ok {
		return
	}
	l.byKey[key] = l.order.PushFront(&recordedRow{key, proto.Clone(row).(*orkstackv1.RecordedResult)})
	for l.order.Len() > maxRecordedRows {
		oldest := l.order.Back()
		l.order.Remove(oldest)
		delete(l.byKey, oldest.Value.(*recordedRow).key)
	}
}
