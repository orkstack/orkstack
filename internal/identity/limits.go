package identity

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// The mint's bounds (workload-identity §3, "Bounds on the mint itself"):
// each calling principal a bucket of 64 refilled 8 a second, and each
// attempt 4 granted mints a minute. A mint that is refused or answered
// unavailable spends neither.
const (
	principalBurst  = 64
	principalRate   = 8
	attemptBudget   = 4
	attemptWindow   = time.Minute
	maxLimiterKeys  = 65536
	limiterIdleDrop = 10 * time.Minute

	// The commit's bounds (workload-identity §3.4): an Orker commits as
	// fast as its tasks finish, so each principal's bucket is 512 refilled
	// 128 a second; each attempt is granted 4 calls a minute, as a mint.
	commitPrincipalBurst = 512
	commitPrincipalRate  = 128
)

// MintLimits holds the buckets in this replica's memory: nothing of it is
// stored or shared. Both maps are bounded; an idle bucket is dropped first,
// and one dropped gives its caller nothing a fresh bucket would not.
type MintLimits struct {
	mu         sync.Mutex
	principals map[string]*limiterEntry
	attempts   map[string]*limiterEntry
	now        func() time.Time
	// The principal bucket's refill and size.
	principalRate  rate.Limit
	principalBurst int
}

type limiterEntry struct {
	l    *rate.Limiter
	seen time.Time
}

// NewMintLimits builds the mint's buckets on clock now (nil is time.Now).
func NewMintLimits(now func() time.Time) *MintLimits {
	if now == nil {
		now = time.Now
	}
	return &MintLimits{principals: map[string]*limiterEntry{}, attempts: map[string]*limiterEntry{}, now: now,
		principalRate: rate.Limit(principalRate), principalBurst: principalBurst}
}

// NewCommitLimits builds CommitResult's buckets on clock now (nil is
// time.Now): the mint's attempt budget and a wider principal bucket.
func NewCommitLimits(now func() time.Time) *MintLimits {
	l := NewMintLimits(now)
	l.principalRate, l.principalBurst = rate.Limit(commitPrincipalRate), commitPrincipalBurst
	return l
}

// Spend is a reservation of a mint's tokens: Refund it when the mint is
// refused or unavailable, so only a granted mint spends.
type Spend struct {
	reservations []*rate.Reservation
	at           time.Time
}

// Refund gives the reservation back.
func (s *Spend) Refund() {
	for _, r := range s.reservations {
		r.CancelAt(s.at)
	}
}

// TakePrincipal reserves one of principal's tokens; false is
// resource_exhausted, and nothing is spent.
func (m *MintLimits) TakePrincipal(principal string) (*Spend, bool) {
	return m.take(m.principals, principal, m.principalRate, m.principalBurst)
}

// TakeAttempt reserves one of the attempt's granted mints; false is
// resource_exhausted, and nothing is spent.
func (m *MintLimits) TakeAttempt(attempt string) (*Spend, bool) {
	return m.take(m.attempts, attempt, rate.Every(attemptWindow/attemptBudget), attemptBudget)
}

func (m *MintLimits) take(set map[string]*limiterEntry, key string, limit rate.Limit, burst int) (*Spend, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	e, ok := set[key]
	if !ok {
		if len(set) >= maxLimiterKeys {
			m.drop(set, now)
		}
		e = &limiterEntry{l: rate.NewLimiter(limit, burst)}
		set[key] = e
	}
	e.seen = now
	r := e.l.ReserveN(now, 1)
	if !r.OK() || r.DelayFrom(now) > 0 {
		r.CancelAt(now)
		return nil, false
	}
	return &Spend{reservations: []*rate.Reservation{r}, at: now}, true
}

// drop removes the buckets idle longest, and at least one, so the map
// never grows past its bound.
func (m *MintLimits) drop(set map[string]*limiterEntry, now time.Time) {
	var oldestKey string
	var oldest time.Time
	for k, e := range set {
		if now.Sub(e.seen) > limiterIdleDrop {
			delete(set, k)
			continue
		}
		if oldestKey == "" || e.seen.Before(oldest) {
			oldestKey, oldest = k, e.seen
		}
	}
	if len(set) >= maxLimiterKeys && oldestKey != "" {
		delete(set, oldestKey)
	}
}
