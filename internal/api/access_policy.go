package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The access document's cache bounds (docs/ACCESS.md, "One rule for every
// cache"): fresh for the refresh interval, served stale for the stale limit
// when the store cannot be read, then closed.
const (
	DefaultPolicyRefresh    = 10 * time.Second
	DefaultPolicyStaleLimit = 5 * time.Minute
	// policyRetryFloor keeps a store that does not answer from being asked
	// by every request: a failed read is not tried again inline sooner.
	policyRetryFloor = time.Second
	// DefaultPolicyReadTimeout bounds one read of the document: a store that
	// accepts a read and never answers is a store that did not answer.
	DefaultPolicyReadTimeout = 5 * time.Second
	// maxChainWalk bounds how far back a replica follows revisions to find
	// the one it last saw before calling a head "written around the API".
	maxChainWalk = 32
)

// ErrPolicyUnavailable is a replica whose copy of the access document is
// past its stale limit: every request but configuration's principals is
// refused, unavailable.
var ErrPolicyUnavailable = errors.New("the access document could not be read within its stale limit")

// PolicyCache holds one replica's compiled access document. It is a
// disposable in-memory cache of an immutable revision (AGENTS.md §13): it
// answers no run read, and every decision is made against one revision,
// which the audit record names.
type PolicyCache struct {
	store      *artifacts.AccessStore
	configured []authz.Binding
	refresh    time.Duration
	staleLimit time.Duration
	readTime   time.Duration
	now        func() time.Time
	logger     *slog.Logger

	// refreshMu serialises reads of the store; mu guards the state below.
	refreshMu sync.Mutex
	mu        sync.Mutex
	policy    *authz.Policy
	etag      string
	sha       string
	readAt    time.Time // when the last good read was sent
	failedAt  time.Time
	lastErr   error
	// aroundPending is a head written around the API (or gone) that no
	// admin has acknowledged; aroundRevision is its revision.
	aroundPending  bool
	aroundRevision int64
	onRead         func(outcome string)
	onUnexpected   func()
	subs           map[chan struct{}]struct{}
}

// PolicyCacheOptions configures a cache. Zero durations take the defaults;
// a negative StaleLimit closes at once.
type PolicyCacheOptions struct {
	Configured []authz.Binding
	Refresh    time.Duration
	StaleLimit time.Duration
	// OnRead is told each read's outcome (unchanged, changed, failed), and
	// OnUnexpected each head found written around the API or gone.
	OnRead       func(outcome string)
	OnUnexpected func()
	// ReadTimeout bounds one read of the store; zero takes
	// DefaultPolicyReadTimeout.
	ReadTimeout time.Duration
	Now         func() time.Time
	Logger      *slog.Logger
}

// NewPolicyCache builds a cache over store. Nothing is read until the first
// decision or the first Run tick.
func NewPolicyCache(store *artifacts.AccessStore, opts PolicyCacheOptions) *PolicyCache {
	c := &PolicyCache{store: store, configured: opts.Configured, refresh: opts.Refresh, staleLimit: opts.StaleLimit,
		readTime: opts.ReadTimeout, now: opts.Now, logger: opts.Logger, subs: map[chan struct{}]struct{}{},
		onRead: opts.OnRead, onUnexpected: opts.OnUnexpected}
	if c.readTime <= 0 {
		c.readTime = DefaultPolicyReadTimeout
	}
	if c.refresh <= 0 {
		c.refresh = DefaultPolicyRefresh
	}
	if c.staleLimit == 0 {
		c.staleLimit = DefaultPolicyStaleLimit
	}
	if c.staleLimit < 0 {
		c.staleLimit = 0
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	return c
}

// PolicyState is what the Access page's banners and the age metric read.
type PolicyState struct {
	Revision         int64
	ReadAt           time.Time
	Age              time.Duration
	Stale            bool // past its refresh bound, served because the store did not answer
	Unavailable      bool // past its stale limit
	WrittenAroundAPI bool
}

// State reports the cache's state now, without reading the store.
func (c *PolicyCache) State() PolicyState {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	s := PolicyState{ReadAt: c.readAt, WrittenAroundAPI: c.aroundPending}
	if c.policy != nil {
		s.Revision = c.policy.Revision
		s.Age = now.Sub(c.readAt)
		s.Stale = s.Age >= c.refresh
		s.Unavailable = s.Age >= c.refresh+c.staleLimit
	} else {
		s.Unavailable = true
	}
	return s
}

// Policy answers the policy a decision is made against: the cached copy
// while fresh; else it revalidates with one conditional read; a copy past
// its refresh bound whose store does not answer is served until the stale
// limit, then ErrPolicyUnavailable. A decision never waits behind a read
// another caller has in flight: it is answered from the copy.
func (c *PolicyCache) Policy(ctx context.Context) (*authz.Policy, error) {
	if p, ok := c.fresh(); ok {
		return p, nil
	}
	if !c.refreshMu.TryLock() {
		c.mu.Lock()
		first := c.policy == nil
		c.mu.Unlock()
		if !first {
			return c.servable()
		}
		// The first read is in flight: wait for it, bounded by its timeout.
		c.refreshMu.Lock()
	}
	if p, ok := c.fresh(); ok {
		c.refreshMu.Unlock()
		return p, nil
	}
	c.mu.Lock()
	recentFailure := !c.failedAt.IsZero() && c.now().Sub(c.failedAt) < policyRetryFloor
	c.mu.Unlock()
	if !recentFailure {
		c.revalidate(ctx)
	}
	c.refreshMu.Unlock()
	return c.servable()
}

// PolicyReadAfter answers a policy from a read sent at or after t, or an
// error: a refusal that must rest on a document read now asks for one.
func (c *PolicyCache) PolicyReadAfter(ctx context.Context, t time.Time) (*authz.Policy, error) {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.mu.Lock()
	have := c.policy != nil && !c.readAt.Before(t)
	c.mu.Unlock()
	if !have {
		c.revalidate(ctx)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.policy == nil || c.readAt.Before(t) {
		if c.lastErr != nil {
			return nil, fmt.Errorf("%w: %w", ErrPolicyUnavailable, c.lastErr)
		}
		return nil, ErrPolicyUnavailable
	}
	return c.policy, nil
}

// Last is the last copy this replica compiled, however old; nil before the
// first read.
func (c *PolicyCache) Last() *authz.Policy {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.policy
}

func (c *PolicyCache) fresh() (*authz.Policy, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.policy != nil && c.now().Sub(c.readAt) < c.refresh {
		return c.policy, true
	}
	return nil, false
}

func (c *PolicyCache) servable() (*authz.Policy, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.policy != nil && c.now().Sub(c.readAt) < c.refresh+c.staleLimit {
		return c.policy, nil
	}
	if c.lastErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrPolicyUnavailable, c.lastErr)
	}
	return nil, ErrPolicyUnavailable
}

// Refresh revalidates now, whatever the copy's age: Run's tick and a test.
func (c *PolicyCache) Refresh(ctx context.Context) {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.revalidate(ctx)
}

// Run revalidates every refresh interval until ctx ends, so an open stream
// is checked again at each change and the age metric stays true.
func (c *PolicyCache) Run(ctx context.Context) {
	t := time.NewTicker(c.refresh)
	defer t.Stop()
	c.Refresh(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.Refresh(ctx)
		}
	}
}

// revalidate is one conditional read and, on a change, one compile. The
// caller holds refreshMu.
func (c *PolicyCache) revalidate(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.readTime)
	defer cancel()
	sent := c.now()
	c.mu.Lock()
	etag := c.etag
	if c.policy == nil {
		etag = ""
	}
	c.mu.Unlock()
	head, changed, err := c.store.Revalidate(ctx, etag)
	if errors.Is(err, artifacts.ErrNotFound) {
		// A deployment that never wrote a document: only configuration grants.
		// A head this replica saw that is now gone was removed around the API.
		c.mu.Lock()
		seen := c.sha != ""
		c.mu.Unlock()
		if seen {
			c.logger.Error("the access document this replica read is gone: it was removed around the API; only configuration grants now",
				"store", c.store.Location())
		}
		c.read("changed")
		// Compiling no document cannot fail: configuration was validated at boot.
		p, _ := authz.Compile(nil, c.configured, sent)
		c.adopt(p, "", "", sent, seen)
		return
	}
	if err != nil {
		c.read("failed")
		c.fail(err)
		return
	}
	if changed {
		c.read("changed")
	} else {
		c.read("unchanged")
	}
	if !changed {
		c.mu.Lock()
		c.readAt, c.failedAt, c.lastErr = sent, time.Time{}, nil
		c.mu.Unlock()
		return
	}
	p, err := authz.Compile(head.Document, c.configured, sent)
	if err != nil {
		c.logger.Error("access document does not compile; deciding from the last good copy until its stale limit",
			"store", c.store.Location(), "revision", head.Meta.Revision, "error", err)
		c.fail(err)
		return
	}
	around := c.writtenAround(ctx, head)
	c.adopt(p, head.ETag, head.Meta.SHA256, sent, around)
}

// writtenAround reports a head that has no revision, or whose chain does
// not lead back to the head this replica last saw.
func (c *PolicyCache) writtenAround(ctx context.Context, head *artifacts.AccessHead) bool {
	ok, err := c.store.HasRevision(ctx, head.Meta)
	if err != nil {
		return false // unknown, not alarmed: the next change looks again
	}
	if !ok {
		c.logger.Error("access document has no revision: it was written around the API",
			"store", c.store.Location(), "revision", head.Meta.Revision, "sha256", head.Meta.SHA256)
		return true
	}
	c.mu.Lock()
	last := c.sha
	c.mu.Unlock()
	if last == "" {
		return false
	}
	meta := head.Meta
	for i := 0; i < maxChainWalk && meta.PreviousSHA256 != ""; i++ {
		if meta.PreviousSHA256 == last {
			return false
		}
		prev, err := c.store.GetRevision(ctx, meta.Revision-1, meta.PreviousSHA256)
		if errors.Is(err, artifacts.ErrNotFound) {
			break // the chain ends here: the head does not lead back
		}
		if err != nil {
			c.logger.Warn("access document's chain could not be followed now; looked at again at the next change",
				"store", c.store.Location(), "error", err)
			return false // unknown, not alarmed
		}
		meta = prev.Meta
	}
	c.logger.Error("access document's chain does not lead back to the revision this replica saw: it was written around the API",
		"store", c.store.Location(), "revision", head.Meta.Revision, "seen_sha256", last)
	return true
}

func (c *PolicyCache) adopt(p *authz.Policy, etag, sha string, sent time.Time, around bool) {
	c.mu.Lock()
	changed := c.policy == nil || c.etag != etag || c.policy.Revision != p.Revision
	c.policy, c.etag, c.readAt, c.failedAt, c.lastErr = p, etag, sent, time.Time{}, nil
	if sha != "" {
		c.sha = sha
	}
	if around {
		c.aroundPending, c.aroundRevision = true, p.Revision
	}
	subs := make([]chan struct{}, 0, len(c.subs))
	for ch := range c.subs {
		subs = append(subs, ch)
	}
	c.mu.Unlock()
	if around && c.onUnexpected != nil {
		c.onUnexpected()
	}
	if changed {
		notify(subs)
	}
}

func (c *PolicyCache) fail(err error) {
	c.mu.Lock()
	c.failedAt, c.lastErr = c.now(), err
	expired := c.policy != nil && c.now().Sub(c.readAt) >= c.refresh+c.staleLimit
	subs := make([]chan struct{}, 0, len(c.subs))
	for ch := range c.subs {
		subs = append(subs, ch)
	}
	c.mu.Unlock()
	c.logger.Warn("access document could not be read", "store", c.store.Location(), "error", err)
	if expired {
		notify(subs)
	}
}

// read reports one read's outcome.
func (c *PolicyCache) read(outcome string) {
	if c.onRead != nil {
		c.onRead(outcome)
	}
}

// Acknowledge clears the written-around banner up to revision.
func (c *PolicyCache) Acknowledge(revision int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aroundPending && revision >= c.aroundRevision {
		c.aroundPending = false
	}
}

// Subscribe returns a channel told of every change of revision and of the
// copy passing its stale limit, and a function that stops it. A send never
// blocks: a subscriber that has not read the last notice gets no second.
func (c *PolicyCache) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	c.mu.Lock()
	c.subs[ch] = struct{}{}
	c.mu.Unlock()
	return ch, func() {
		c.mu.Lock()
		delete(c.subs, ch)
		c.mu.Unlock()
	}
}

func notify(subs []chan struct{}) {
	for _, ch := range subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
