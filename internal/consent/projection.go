package consent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// Resolver reads a composed Orkflow at a pinned version from its owner
// namespace. A version that does not exist is (nil, nil); an error is an
// outage, never "asks for nothing".
type Resolver func(ctx context.Context, owner, name, version string) (*orkstackv1.Orkflow, error)

// ErrChildMissing is a composed task whose pinned Orkflow does not exist:
// what it asks for cannot be known, so neither can the consent.
var ErrChildMissing = errors.New("a composed Orkflow it pins does not exist")

// ErrTooDeep is a chain of composed Orkflows longer than any a definition
// may hold.
var ErrTooDeep = errors.New("composed Orkflows nest deeper than a consent can follow")

// maxDepth bounds the walk. Validation bounds composition far lower; this
// only keeps a hand-built cycle from recursing for ever.
const maxDepth = 32

// Asks is what a resolved definition asks for under projection 1: its
// digest and the short list a person reads.
type Asks struct {
	// Digest is "c1:" and the SHA-256 of the canonical table.
	Digest string
	// Audiences are the distinct token audiences asked for, sorted.
	Audiences []string
	// Grants are the distinct grant names asked for, sorted.
	Grants []string

	canonical []byte
}

// Empty reports that the definition asks for nothing.
func (a Asks) Empty() bool { return a.Digest == empty }

// The canonical table of projection 1. Members are declared in key order, so
// the encoding is the same as a map's; every slice is non-nil, so an absent
// list is [] and never null. These shapes are frozen: a member a later
// release covers is projection 2, "c2:".
type (
	table struct {
		Tasks []roleTask `json:"tasks"`
		V     int        `json:"v"`
	}
	roleTask struct {
		Credentials []credential  `json:"credentials"`
		Grants      []secretGrant `json:"grants"`
		ID          string        `json:"id"`
		Identities  []identity    `json:"identities"`
		Join        int64         `json:"join"`
		Kind        string        `json:"kind"`
		Path        []string      `json:"path"`
		Pool        string        `json:"pool"`
		Upstream    []upstream    `json:"upstream"`
	}
	identity struct {
		Audience   string `json:"audience"`
		Name       string `json:"name"`
		Refresh    int64  `json:"refresh"`
		TTLSeconds int64  `json:"ttl_seconds"`
	}
	// credential is a task's credential as the spec projection lists it:
	// its name, grant, path, params and refresh policy. CredentialRef holds
	// none of them until P15 lands its fields, and the DSL refuses
	// `credentials:` until then, so no definition can carry one yet.
	credential struct {
		Grant   string            `json:"grant"`
		Name    string            `json:"name"`
		Params  map[string]string `json:"params"`
		Path    string            `json:"path"`
		Refresh int64             `json:"refresh"`
	}
	secretGrant struct {
		Grant string `json:"grant"`
		Name  string `json:"name"`
	}
	upstream struct {
		Await *await   `json:"await"`
		ID    string   `json:"id"`
		Join  int64    `json:"join"`
		Kind  string   `json:"kind"`
		Needs []string `json:"needs"`
		Path  []string `json:"path"`
		Uses  *uses    `json:"uses"`
	}
	await struct {
		Callback       bool  `json:"callback"`
		TimeoutSeconds int64 `json:"timeout_seconds"`
	}
	uses struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
		Version   string `json:"version"`
	}
)

// Compute is consent projection 1 of def, written in namespace ns, with
// every Orkflow it composes read through resolve at the version it pins
// (workload-identity §3.2, "What consent covers"). For every task that would
// hold an identity — an identity, a credential or a secret's grant — the
// table holds the task's id, kind, pool and join, what it asks for, and every
// task upstream of it: in its own definition through needs and, for a task
// of a composed Orkflow, the composing task and all it reaches in its parent.
func Compute(ctx context.Context, ns string, def *orkstackv1.Orkflow, resolve Resolver) (Asks, error) {
	w := walker{resolve: resolve, audiences: map[string]bool{}, grants: map[string]bool{}}
	if err := w.walk(ctx, def, ns, []string{}, nil, 0); err != nil {
		return Asks{}, err
	}
	slices.SortFunc(w.tasks, func(a, b roleTask) int {
		if c := slices.Compare(a.Path, b.Path); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	tbl := table{Tasks: w.tasks, V: 1}
	if tbl.Tasks == nil {
		tbl.Tasks = []roleTask{}
	}
	b, err := stablejson.Marshal(tbl)
	if err != nil {
		return Asks{}, fmt.Errorf("consent: encode the table: %w", err)
	}
	sum := sha256.Sum256(b)
	return Asks{
		Digest:    Prefix1 + hex.EncodeToString(sum[:]),
		Audiences: sortedSet(w.audiences),
		Grants:    sortedSet(w.grants),
		canonical: b,
	}, nil
}

type walker struct {
	resolve   Resolver
	tasks     []roleTask
	audiences map[string]bool
	grants    map[string]bool
}

func (w *walker) walk(ctx context.Context, def *orkstackv1.Orkflow, owner string, path []string, outer []upstream, depth int) error {
	if depth > maxDepth {
		return ErrTooDeep
	}
	byID := make(map[string]*orkstackv1.Task, len(def.GetTasks()))
	for _, t := range def.GetTasks() {
		byID[t.GetId()] = t
	}
	for _, t := range def.GetTasks() {
		inner := upstreamOf(t, byID, path, owner)
		if isRole(t) {
			w.addRole(t, path, append(slices.Clone(inner), outer...))
		}
		if !isComposed(t) {
			continue
		}
		ref := t.GetOrkflow()
		if ref == nil {
			return fmt.Errorf("consent: task %q names an Orkflow with no compiled reference: %w", t.GetId(), ErrChildMissing)
		}
		childOwner := refOwner(owner, ref)
		child, err := w.resolve(ctx, childOwner, ref.GetName(), ref.GetVersion())
		if err != nil {
			return err
		}
		if child == nil {
			return fmt.Errorf("consent: task %q: %s/%s@%s: %w", t.GetId(), childOwner, ref.GetName(), ref.GetVersion(), ErrChildMissing)
		}
		childOuter := append(append([]upstream{entryOf(t, path, owner)}, inner...), outer...)
		if err := w.walk(ctx, child, childOwner, append(slices.Clone(path), t.GetId()), childOuter, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func (w *walker) addRole(t *orkstackv1.Task, path []string, ups []upstream) {
	ids := make([]identity, 0, len(t.GetIdentities()))
	for _, id := range t.GetIdentities() {
		ids = append(ids, identity{Audience: id.GetAudience(), Name: id.GetName(),
			Refresh: int64(id.GetRefresh()), TTLSeconds: int64(id.GetTtlSeconds())})
		w.audiences[id.GetAudience()] = true
	}
	// Validation keeps names unique in a task; the full key still orders
	// two equal names, so the digest never depends on declaration order.
	slices.SortFunc(ids, func(a, b identity) int {
		return cmpAll(strings.Compare(a.Name, b.Name), strings.Compare(a.Audience, b.Audience),
			cmpInt(a.TTLSeconds, b.TTLSeconds), cmpInt(a.Refresh, b.Refresh))
	})
	creds := make([]credential, 0, len(t.GetCredentials()))
	for range t.GetCredentials() {
		creds = append(creds, credential{Params: map[string]string{}})
	}
	grants := []secretGrant{}
	for _, s := range t.GetSecrets() {
		if s.GetGrant() == "" {
			continue
		}
		grants = append(grants, secretGrant{Grant: s.GetGrant(), Name: s.GetName()})
		w.grants[s.GetGrant()] = true
	}
	slices.SortFunc(grants, func(a, b secretGrant) int {
		return cmpAll(strings.Compare(a.Name, b.Name), strings.Compare(a.Grant, b.Grant))
	})
	sortUpstream(ups)
	w.tasks = append(w.tasks, roleTask{
		Credentials: creds, Grants: grants, ID: t.GetId(), Identities: ids,
		Join: int64(t.GetJoin()), Kind: kindOf(t), Path: slices.Clone(path), Pool: t.GetPool(), Upstream: ups,
	})
}

// upstreamOf is every task t reaches through needs in its own definition.
func upstreamOf(t *orkstackv1.Task, byID map[string]*orkstackv1.Task, path []string, owner string) []upstream {
	seen := map[string]bool{t.GetId(): true}
	queue := slices.Clone(t.GetNeeds())
	out := []upstream{}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		u, ok := byID[id]
		if !ok {
			continue // validation refuses a need that names no task
		}
		out = append(out, entryOf(u, path, owner))
		queue = append(queue, u.GetNeeds()...)
	}
	return out
}

func entryOf(t *orkstackv1.Task, path []string, owner string) upstream {
	needs := slices.Clone(t.GetNeeds())
	if needs == nil {
		needs = []string{}
	}
	slices.Sort(needs)
	e := upstream{ID: t.GetId(), Join: int64(t.GetJoin()), Kind: kindOf(t), Needs: needs, Path: slices.Clone(path)}
	if a := t.GetAwait(); a != nil {
		e.Await = &await{Callback: a.GetCallback(), TimeoutSeconds: int64(a.GetTimeoutSeconds())}
	}
	if ref := t.GetOrkflow(); ref != nil {
		e.Uses = &uses{Name: ref.GetName(), Namespace: refOwner(owner, ref), Version: ref.GetVersion()}
	}
	return e
}

func sortUpstream(ups []upstream) {
	slices.SortFunc(ups, func(a, b upstream) int {
		if c := slices.Compare(a.Path, b.Path); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
}

func isRole(t *orkstackv1.Task) bool {
	if len(t.GetIdentities()) > 0 || len(t.GetCredentials()) > 0 {
		return true
	}
	return slices.ContainsFunc(t.GetSecrets(), func(s *orkstackv1.SecretRef) bool { return s.GetGrant() != "" })
}

func isComposed(t *orkstackv1.Task) bool {
	return t.GetOrkflow() != nil || strings.HasPrefix(t.GetUses(), "orkflow:")
}

func kindOf(t *orkstackv1.Task) string {
	switch {
	case isComposed(t):
		return "composed"
	case t.GetAwait() != nil:
		return "awaited"
	case t.GetSleep() != nil:
		return "sleeping"
	case t.GetEach() != nil:
		return "fan-out"
	default:
		return "container"
	}
}

// refOwner is the namespace a reference resolves in: its own, or the
// namespace of the definition that wrote it.
func refOwner(composer string, ref *orkstackv1.OrkflowRef) string {
	if ns := ref.GetNamespace(); ns != "" {
		return ns
	}
	return composer
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// Difference says, for a person, what changed between the consent given
// and what the Orkflow asks for now. Only the short list is stored with a
// consent, so a change of the tasks behind the same names is said as such.
func Difference(given *orkstackv1.ConsentView, now Asks) string {
	if given.GetDigest() == "" {
		return fmt.Sprintf("no consent was given; it asks for audiences %s and grants %s",
			listOrNone(now.Audiences), listOrNone(now.Grants))
	}
	var parts []string
	parts = append(parts, changes("audience", given.GetAudiences(), now.Audiences)...)
	parts = append(parts, changes("grant", given.GetGrants(), now.Grants)...)
	if len(parts) == 0 {
		parts = append(parts, "it asks for the same audiences and grants, but the tasks that hold them, or what runs before those tasks, changed")
	}
	return strings.Join(parts, "; ")
}

func changes(what string, was, now []string) []string {
	var out []string
	for _, n := range now {
		if !slices.Contains(was, n) {
			out = append(out, fmt.Sprintf("now also asks for %s %s", what, n))
		}
	}
	for _, w := range was {
		if !slices.Contains(now, w) {
			out = append(out, fmt.Sprintf("no longer asks for %s %s", what, w))
		}
	}
	return out
}

func listOrNone(s []string) string {
	if len(s) == 0 {
		return "(none)"
	}
	return strings.Join(s, ", ")
}

// cmpAll is the first non-zero comparison.
func cmpAll(cs ...int) int {
	for _, c := range cs {
		if c != 0 {
			return c
		}
	}
	return 0
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
