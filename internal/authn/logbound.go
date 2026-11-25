package authn

import (
	"sync"
	"time"
)

// LogBound bounds the lines an unauthenticated caller can make an engine
// log: one a second for each kind, a word of the code's own, with how many
// were dropped since the last. golang.org/x/time/rate's Sometimes was weighed:
// it does not say what it dropped, and an operator must see that there was
// more than one.
type LogBound struct {
	mu    sync.Mutex
	kinds map[string]*boundKind
	now   func() time.Time // nil is time.Now; a test sets it
}

type boundKind struct {
	last    time.Time
	dropped int
}

const logBoundInterval = time.Second

// Do calls log, unless one of kind was logged within the last second. It is
// called outside the bound's lock, so a slow log delays nobody else.
func (b *LogBound) Do(kind string, log func(dropped int)) {
	now := time.Now
	if b.now != nil {
		now = b.now
	}
	b.mu.Lock()
	k := b.kinds[kind]
	if k == nil {
		if b.kinds == nil {
			b.kinds = map[string]*boundKind{}
		}
		k = &boundKind{}
		b.kinds[kind] = k
	}
	at := now()
	if !k.last.IsZero() && at.Sub(k.last) < logBoundInterval {
		k.dropped++
		b.mu.Unlock()
		return
	}
	dropped := k.dropped
	k.last, k.dropped = at, 0
	b.mu.Unlock()
	log(dropped)
}

// LogText is a caller's own text as a log line may hold it: at most max
// bytes, and said to be cut.
func LogText(s string, max int) string {
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

// The bounds of what a caller chose in a log line.
const (
	LogTextMax  = 128
	LogErrorMax = 512
)

func logError(err error) string {
	if err == nil {
		return ""
	}
	return LogText(err.Error(), LogErrorMax)
}
