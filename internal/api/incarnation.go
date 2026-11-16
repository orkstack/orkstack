package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// IncarnationStore keeps the immutable ids of namespaces and Orkflows
// (artifacts.IncarnationStore; docs/ACCESS.md, "Pinned bindings"). Each id belongs to one incarnation: a namespace's to its
// document's created_at, an Orkflow's to its head's since. A read never
// writes one; an Ensure mints one on first need.
type IncarnationStore interface {
	NamespaceID(ctx context.Context, namespace string, createdAt time.Time) (string, bool, error)
	EnsureNamespaceID(ctx context.Context, namespace string, createdAt time.Time) (string, error)
	DeleteNamespaceID(ctx context.Context, namespace string, createdAt time.Time) error
	OrkflowID(ctx context.Context, namespace, name string, since time.Time) (string, bool, error)
	EnsureOrkflowID(ctx context.Context, namespace, name string, since time.Time) (string, error)
	DeleteOrkflowID(ctx context.Context, namespace, name string, since time.Time) error
}

var _ IncarnationStore = (*artifacts.IncarnationStore)(nil)

// incarnationCacheTTL is how long one replica reuses an id it read for an
// access decision. It is the policy cache's own window (auth.policy_refresh's
// default): a namespace deleted and made again through another replica may
// be decided against its old id for that long, as a binding removed there
// may still be honoured here for that long. A delete or create through this
// replica forgets its entries at once.
const incarnationCacheTTL = 10 * time.Second

// incarnationCacheMax bounds the cache; past it the cache starts over.
const incarnationCacheMax = 4096

// incarnationCache holds ids an access decision read, keyed by namespace and
// namespace/orkflow, both resolved. An empty id is cached too: "nothing to
// match a pin". A read records the generation it began in and is kept only
// if nothing was forgotten meanwhile, so a read that raced a create or a
// delete through this replica never outlives it.
type incarnationCache struct {
	mu      sync.Mutex
	entries map[string]incarnationEntry
	gen     uint64
	now     func() time.Time
}

type incarnationEntry struct {
	id string
	at time.Time
}

func newIncarnationCache() *incarnationCache {
	return &incarnationCache{entries: map[string]incarnationEntry{}, now: time.Now}
}

func incarnationKey(namespace, name string) string {
	namespace = artifacts.ResolveNamespace(namespace)
	if name == "" {
		return namespace
	}
	return namespace + "/" + name
}

// get answers a fresh entry, and the generation a read to fill it begins in.
func (c *incarnationCache) get(key string) (id string, ok bool, gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, found := c.entries[key]
	if !found || c.now().Sub(e.at) >= incarnationCacheTTL {
		return "", false, c.gen
	}
	return e.id, true, c.gen
}

// fill keeps what a read that began in gen found, unless something was
// forgotten since.
func (c *incarnationCache) fill(key, id string, gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen == gen {
		c.setLocked(key, id)
	}
}

// set keeps what this replica just wrote.
func (c *incarnationCache) set(key, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setLocked(key, id)
}

func (c *incarnationCache) setLocked(key, id string) {
	if len(c.entries) >= incarnationCacheMax {
		clear(c.entries)
	}
	c.entries[key] = incarnationEntry{id: id, at: c.now()}
}

// forget drops one Orkflow's entry when name is set, and otherwise the
// namespace's and every one of its Orkflows', and voids every read in
// flight.
func (c *incarnationCache) forget(namespace, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gen++
	key := incarnationKey(namespace, name)
	if name != "" {
		delete(c.entries, key)
		return
	}
	for k := range c.entries {
		if k == key || strings.HasPrefix(k, key+"/") {
			delete(c.entries, k)
		}
	}
}

// idError maps a failed read or write of an id. A request it serves is
// never decided as if no id existed: an outage is unavailable, a lost race
// aborted, a record that cannot be read an internal fault, a name that
// addresses nothing the caller's, and a canceled request canceled. What the
// caller is told names what failed and never the store: the object, the
// bucket and the store's own words go to the log alone, because a decision
// reads ids before it knows who may see them.
func (s *Service) idError(what string, err error) error {
	code, said := idErrorCode(err)
	if code == connect.CodeUnavailable || code == connect.CodeInternal {
		s.logger.Warn("an id of a namespace or an Orkflow could not be read or written", "what", what, "error", err)
	}
	return connect.NewError(code, fmt.Errorf("%s: %s", what, said))
}

func idErrorCode(err error) (connect.Code, string) {
	switch {
	case errors.Is(err, context.Canceled):
		return connect.CodeCanceled, "the request was canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return connect.CodeDeadlineExceeded, "the request ran out of time"
	case errors.Is(err, artifacts.ErrInvalidNamespace), errors.Is(err, artifacts.ErrInvalidOrkflow):
		return connect.CodeInvalidArgument, err.Error()
	case errors.Is(err, artifacts.ErrIncarnationContended):
		return connect.CodeAborted, "it is being written by another engine; try again"
	case errors.Is(err, artifacts.ErrIncarnationUnreadable):
		return connect.CodeInternal, "its stored record is unreadable; the engine's log names the object, which an " +
			"operator removes so that the next save of a binding pins to a new id (bindings pinned to the old one " +
			"are then stale until `orkctl admin binding update NAME --repin`)"
	}
	return connect.CodeUnavailable, "the object store did not answer; try again"
}

// errOrkflowStoreTellsNoIncarnation is a deployment whose Orkflow store
// cannot say which incarnation it holds while ids are kept: every pin of an
// Orkflow would be unreadable, so it is said, never answered as "no id".
var errOrkflowStoreTellsNoIncarnation = connect.NewError(connect.CodeInternal,
	errors.New("this engine's Orkflow store cannot say which incarnation of an Orkflow it holds, so no Orkflow has an id"))

// namespaceID is the id of the namespace's current incarnation, "" when the
// namespace does not exist or no id is stored for it (and ensure is false).
// With ensure set it mints one on first need: a write, never a read's side
// effect. No store wired is "" always.
func (s *Service) namespaceID(ctx context.Context, namespace string, ensure bool) (string, error) {
	namespace = artifacts.ResolveNamespace(namespace)
	if s.incarnations == nil {
		return "", nil
	}
	if namespace == artifacts.DefaultNamespace {
		return artifacts.DefaultNamespaceID, nil
	}
	ns, err := s.pools.Namespace(ctx, namespace)
	if errors.Is(err, artifacts.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", s.idError(fmt.Sprintf("read namespace %q", namespace), err)
	}
	createdAt := ns.GetCreatedAt().AsTime()
	var id string
	if ensure {
		id, err = s.incarnations.EnsureNamespaceID(ctx, namespace, createdAt)
	} else {
		id, _, err = s.incarnations.NamespaceID(ctx, namespace, createdAt)
	}
	if err != nil {
		return "", s.idError(fmt.Sprintf("the id of namespace %q", namespace), err)
	}
	if ensure {
		s.ids.set(incarnationKey(namespace, ""), id)
	}
	return id, nil
}

// orkflowID is the id of the Orkflow's current incarnation, "" when it does
// not exist, has no settled incarnation yet, or no id is stored for it (and
// ensure is false).
func (s *Service) orkflowID(ctx context.Context, namespace, name string, ensure bool) (string, error) {
	namespace = artifacts.ResolveNamespace(namespace)
	if s.incarnations == nil || name == "" {
		return "", nil
	}
	heads, ok := s.orkflows.(engine.OrkflowIncarnations)
	if !ok {
		return "", errOrkflowStoreTellsNoIncarnation
	}
	since, ok, err := heads.Incarnation(ctx, namespace, name)
	if err != nil {
		return "", s.idError(fmt.Sprintf("the head of orkflow %s/%s", namespace, name), err)
	}
	if !ok {
		return "", nil
	}
	var id string
	if ensure {
		id, err = s.incarnations.EnsureOrkflowID(ctx, namespace, name, since)
	} else {
		id, _, err = s.incarnations.OrkflowID(ctx, namespace, name, since)
	}
	if err != nil {
		return "", s.idError(fmt.Sprintf("the id of orkflow %s/%s", namespace, name), err)
	}
	if ensure {
		s.ids.set(incarnationKey(namespace, name), id)
	}
	return id, nil
}

// withIDs fills the ids of the incarnation a resource names, when the
// policy pins any binding inside its namespace; otherwise no binding can
// tell one incarnation from another and nothing is read. A read that fails
// is unavailable: a pinned binding is never decided against an unknown id.
func (s *Service) withIDs(ctx context.Context, policy *authz.Policy, r authz.Resource) (authz.Resource, error) {
	if s.incarnations == nil || r.Namespace == "" || r.Pool != "" || !policy.PinsWithin(r.Namespace) {
		return r, nil
	}
	if r.NamespaceID == "" {
		id, err := s.cachedID(incarnationKey(r.Namespace, ""), func() (string, error) { return s.namespaceID(ctx, r.Namespace, false) })
		if err != nil {
			return r, err
		}
		r.NamespaceID = id
	}
	if r.Orkflow != "" && r.OrkflowID == "" && !strings.Contains(r.Orkflow, "/") {
		// A shared Orkflow ("owner/name") is another namespace's; no binding
		// of this namespace is pinned to it.
		id, err := s.cachedID(incarnationKey(r.Namespace, r.Orkflow), func() (string, error) { return s.orkflowID(ctx, r.Namespace, r.Orkflow, false) })
		if err != nil {
			return r, err
		}
		r.OrkflowID = id
	}
	return r, nil
}

func (s *Service) cachedID(key string, read func() (string, error)) (string, error) {
	id, ok, gen := s.ids.get(key)
	if ok {
		return id, nil
	}
	id, err := read()
	if err != nil {
		return "", err
	}
	s.ids.fill(key, id, gen)
	return id, nil
}

// mintNamespaceID gives a namespace just created its id. It is best effort:
// a namespace whose id could not be written gets one on first need (a
// binding pinned to it, the admission of a run that holds an identity), and
// that request is unavailable while the write keeps failing.
func (s *Service) mintNamespaceID(ctx context.Context, ns *orkstackv1.Namespace) {
	s.ids.forget(ns.GetName(), "")
	if _, err := s.namespaceID(ctx, ns.GetName(), true); err != nil {
		s.logger.Warn("the namespace was created, but its id was not written; it is made on first need",
			"namespace", ns.GetName(), "error", err)
	}
}

// mintOrkflowID gives an Orkflow whose version was just created the id of
// its incarnation, when it has none yet; best effort, as mintNamespaceID.
func (s *Service) mintOrkflowID(ctx context.Context, namespace, name string) {
	s.ids.forget(namespace, name)
	if _, err := s.orkflowID(ctx, namespace, name, true); err != nil {
		s.logger.Warn("the Orkflow version was stored, but its id was not written; it is made on first need",
			"namespace", namespace, "orkflow", name, "error", err)
	}
}

// forgetNamespaceID removes a deleted namespace's id record, the one bound
// to createdAt, the incarnation that was deleted. The document is
// deleted first: a record left behind is bound to the incarnation that is
// gone and is replaced when the name is made again, while a record removed
// before a delete that then failed would give the surviving namespace a new
// id and leave every binding pinned to it stale.
func (s *Service) forgetNamespaceID(ctx context.Context, namespace string, createdAt time.Time) {
	s.ids.forget(namespace, "")
	if s.incarnations == nil {
		return
	}
	if err := s.incarnations.DeleteNamespaceID(ctx, namespace, createdAt); err != nil {
		s.logger.Warn("the namespace was deleted, but its id record was not; it is ignored and replaced if the name is made again",
			"namespace", namespace, "error", err)
	}
}

// forgetOrkflowID removes a deleted Orkflow's id record, after the Orkflow,
// for the reason forgetNamespaceID gives. since is the incarnation that was
// deleted, read before the delete; known is false when it could not be.
func (s *Service) forgetOrkflowID(ctx context.Context, namespace, name string, since time.Time, known bool) {
	s.ids.forget(namespace, name)
	if s.incarnations == nil || !known {
		return
	}
	if err := s.incarnations.DeleteOrkflowID(ctx, namespace, name, since); err != nil {
		s.logger.Warn("the Orkflow was deleted, but its id record was not; it is ignored and replaced if the name is made again",
			"namespace", namespace, "orkflow", name, "error", err)
	}
}

// authorizeWithIDs is authz.Authorize with the ids of the incarnation the
// resource names filled first (withIDs): what a handler asks outside the
// interceptor. Its error is a connect error when the ids cannot be read.
func (s *Service) authorizeWithIDs(ctx context.Context, req authz.Request) (authz.Decision, error) {
	res, err := s.withIDs(ctx, req.Policy, req.Resource)
	if err != nil {
		return authz.Decision{}, err
	}
	req.Resource = res
	return authz.Authorize(req)
}

// mayWithIDs is authorizeWithIDs for a handler that shows or hides
// something: whether the caller may, and an error only when the ids of what
// it names cannot be read, which the handler answers as its own failure (a
// list never silently drops what it could not decide).
func (s *Service) mayWithIDs(ctx context.Context, req authz.Request) (bool, error) {
	res, err := s.withIDs(ctx, req.Policy, req.Resource)
	if err != nil {
		return false, err
	}
	req.Resource = res
	_, err = authz.Authorize(req)
	return err == nil, nil
}

// orkflowIncarnation is when the Orkflow's current incarnation began, read
// before a delete so the delete removes that incarnation's id record only.
// known is false when it cannot be read: the record is then left, bound to
// an incarnation that is gone, and replaced when the name is made again.
func (s *Service) orkflowIncarnation(ctx context.Context, namespace, name string) (time.Time, bool) {
	if s.incarnations == nil {
		return time.Time{}, false
	}
	heads, ok := s.orkflows.(engine.OrkflowIncarnations)
	if !ok {
		s.logger.Warn("the Orkflow's id record is left: this engine's Orkflow store cannot say which incarnation it holds",
			"namespace", namespace, "orkflow", name)
		return time.Time{}, false
	}
	since, ok, err := heads.Incarnation(ctx, namespace, name)
	if err != nil {
		s.logger.Warn("the Orkflow's incarnation could not be read before its delete; its id record is left",
			"namespace", namespace, "orkflow", name, "error", err)
		return time.Time{}, false
	}
	return since, ok
}

// withoutNamespaceID is ns with its output-only id cleared: what a create or
// an update stores, and what a policy comparison compares. The id lives in
// its own record, never in the namespace document, which an engine of an
// earlier release rewrites with unknown fields dropped.
func withoutNamespaceID(ns *orkstackv1.Namespace) *orkstackv1.Namespace {
	if ns.GetIdentity().GetNamespaceId() == "" {
		return ns
	}
	out := proto.Clone(ns).(*orkstackv1.Namespace)
	out.Identity.NamespaceId = ""
	if proto.Size(out.Identity) == 0 {
		out.Identity = nil
	}
	return out
}
