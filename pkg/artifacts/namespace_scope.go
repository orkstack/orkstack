package artifacts

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ErrNotNamespaced reports a key that is not a namespace-first key this
// package renders: not a commit, memo or attested key, or one no renderer
// of this package would write.
var ErrNotNamespaced = errors.New("not a namespace-first key")

// NamespaceOf returns the namespace a namespace-first key names: a commit
// key (CommitKey, ItemCommitKey), a memo key (MemoKey) or an attested key
// (AttestedCommitKey, RecordKey, OutputsRecordKey). They are the only keys an
// Orker reads or writes that carry a namespace; artifacts, committed values,
// logs and attempt records carry none, so no key says whose they are.
//
// It is no second parser: a key is accepted only when this package's own
// renderer, given the coordinates read back from it, writes the same key.
// So every segment passed that renderer's rule, none is "", "." or "..", and
// no filesystem reading of the key leaves the namespace it names.
func NamespaceOf(key string) (string, error) {
	var ns string
	var err error
	switch {
	case strings.HasPrefix(key, CommitPrefix+"/"):
		ns, err = commitNamespace(key)
	case strings.HasPrefix(key, MemoPrefix+"/"):
		ref, ok := ParseMemoKey(key)
		if !ok {
			err = errors.New("not a memo key")
		}
		ns = ref.Namespace
	case strings.HasPrefix(key, AttestedPrefix+"/"):
		ns, err = attestedNamespace(key)
	default:
		err = errors.New("not a commit, memo or attested key")
	}
	if err != nil {
		return "", fmt.Errorf("artifacts: %q: %w: %w", key, ErrNotNamespaced, err)
	}
	return ns, nil
}

// commitNamespace reads a commit key. AttestedCommitKey holds every segment
// of one to its renderer's rule, so its acceptance is the check.
func commitNamespace(key string) (string, error) {
	if _, err := AttestedCommitKey(key); err != nil {
		return "", err
	}
	ns, _, _ := strings.Cut(strings.TrimPrefix(key, CommitPrefix+"/"), "/")
	return ns, nil
}

// attestedNamespace reads an attested key: a commit's twin, a task's record
// or a run's outputs, each accepted only when its renderer writes it back.
func attestedNamespace(key string) (string, error) {
	rest := strings.TrimPrefix(key, AttestedPrefix+"/")
	segs := strings.Split(rest, "/")
	if len(segs) < 3 {
		return "", errors.New("not an attested key")
	}
	ns := segs[0]
	if segs[len(segs)-1] == CommitObjectName {
		twin, err := AttestedCommitKey(CommitPrefix + "/" + rest)
		if err != nil {
			return "", err
		}
		if twin != key {
			return "", errors.New("not an attested commit key")
		}
		return ns, nil
	}
	if err := validNamespaceName(ns); err != nil {
		return "", err
	}
	run, ok := decodeArchiveSegment(segs[1])
	if !ok {
		return "", errors.New("run id segment")
	}
	var rendered string
	var err error
	switch {
	case len(segs) == 3 && segs[2] == outputsObjectName:
		rendered, err = OutputsRecordKey(ns, run)
	case len(segs) == 4 || len(segs) == 5:
		kind, ok := recordKindOf(segs[len(segs)-1])
		if !ok {
			return "", errors.New("not a record key")
		}
		var scope uint64
		if len(segs) == 5 {
			if scope, ok = parseScopeSegment(segs[3]); !ok || scope == 0 {
				return "", errors.New("scope segment")
			}
		}
		rendered, err = RecordKey(kind, ns, run, segs[2], scope)
	default:
		return "", errors.New("not an attested key")
	}
	if err != nil {
		return "", err
	}
	if rendered != key {
		return "", errors.New("not a key this package renders")
	}
	return ns, nil
}

func recordKindOf(leaf string) (RecordKind, bool) {
	switch leaf {
	case resolutionObjectName:
		return RecordResolution, true
	case composedObjectName:
		return RecordComposed, true
	}
	return 0, false
}

// ErrNamespaceNotServed reports a read or write of an object of a namespace
// the Orker does not serve (ORK_ORKER_NAMESPACES), refused before the store
// is asked. It is no fault of the store: a retry is refused the same way.
var ErrNamespaceNotServed = errors.New("namespace not served")

// Namespaces is the set of namespaces an Orker serves. The zero value, and a
// set parsed from an empty list, serves every namespace.
type Namespaces struct {
	names []string // sorted, unique; nil serves every namespace
}

// ParseNamespaces reads an Orker's namespace list. Each name is held to the
// namespace rule; an empty list serves every namespace.
func ParseNamespaces(list []string) (Namespaces, error) {
	var names []string
	for _, name := range list {
		if err := validNamespaceName(name); err != nil {
			return Namespaces{}, err
		}
		names = append(names, name)
	}
	slices.Sort(names)
	return Namespaces{names: slices.Compact(names)}, nil
}

// Every reports whether the set serves every namespace.
func (n Namespaces) Every() bool { return len(n.names) == 0 }

// Serves reports whether the set serves ns; "" is the default namespace.
func (n Namespaces) Serves(ns string) bool {
	if n.Every() {
		return true
	}
	_, found := slices.BinarySearch(n.names, ResolveNamespace(ns))
	return found
}

// Names is the set's names, sorted; nil when it serves every namespace.
func (n Namespaces) Names() []string { return slices.Clone(n.names) }

// NamespaceOfURI returns the namespace a uri composed as root + "/" + key
// names, for a namespace-first key (NamespaceOf). A uri under any other root,
// or whose key is not namespace-first, is ErrNotNamespaced.
func NamespaceOfURI(root, uri string) (string, error) {
	key, ok := strings.CutPrefix(uri, root+"/")
	if !ok || root == "" {
		return "", fmt.Errorf("artifacts: %q is not under %q: %w", uri, root, ErrNotNamespaced)
	}
	return NamespaceOf(key)
}

// ScopeCommits returns a CommitStore that asks inner only for a uri under
// root whose key is a commit of a namespace served names; any other uri is
// ErrNamespaceNotServed, before inner is asked. For a set that serves every
// namespace it returns inner itself.
func ScopeCommits(inner CommitStore, root string, served Namespaces) CommitStore {
	if served.Every() {
		return inner
	}
	return scopedCommits{inner: inner, root: root, served: served}
}

type scopedCommits struct {
	inner  CommitStore
	root   string
	served Namespaces
}

func (s scopedCommits) check(uri string) error {
	ns, err := NamespaceOfURI(s.root, uri)
	if err != nil {
		return fmt.Errorf("artifacts: commit %q names no namespace this Orker serves: %w: %w", uri, ErrNamespaceNotServed, err)
	}
	if !s.served.Serves(ns) {
		return fmt.Errorf("artifacts: commit %q is of namespace %q, which this Orker does not serve: %w", uri, ns, ErrNamespaceNotServed)
	}
	return nil
}

func (s scopedCommits) Create(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (CommitRecord, error) {
	if err := s.check(uri); err != nil {
		return CommitRecord{}, err
	}
	return s.inner.Create(ctx, uri, c)
}

func (s scopedCommits) Get(ctx context.Context, uri string) (CommitRecord, error) {
	if err := s.check(uri); err != nil {
		return CommitRecord{}, err
	}
	return s.inner.Get(ctx, uri)
}

// ScopeMemos returns a MemoStore that asks inner only about memos of a
// namespace served names; any other is ErrNamespaceNotServed, before inner
// is asked. For a set that serves every namespace it returns inner itself.
func ScopeMemos(inner MemoStore, served Namespaces) MemoStore {
	if served.Every() {
		return inner
	}
	return scopedMemos{inner: inner, served: served}
}

type scopedMemos struct {
	inner  MemoStore
	served Namespaces
}

func (s scopedMemos) check(ns string) error {
	if !s.served.Serves(ns) {
		return fmt.Errorf("artifacts: memo of namespace %q, which this Orker does not serve: %w", ResolveNamespace(ns), ErrNamespaceNotServed)
	}
	return nil
}

func (s scopedMemos) GetMemo(ctx context.Context, ref MemoRef) (*orkstackv1.CacheMemo, error) {
	if err := s.check(ref.Namespace); err != nil {
		return nil, err
	}
	return s.inner.GetMemo(ctx, ref)
}

func (s scopedMemos) PutMemo(ctx context.Context, memo *orkstackv1.CacheMemo) error {
	if err := s.check(memo.GetNamespace()); err != nil {
		return err
	}
	return s.inner.PutMemo(ctx, memo)
}

func (s scopedMemos) ListMemos(ctx context.Context, scope MemoScopeRef, after string, limit int) ([]MemoEntry, string, error) {
	if err := s.check(scope.Namespace); err != nil {
		return nil, "", err
	}
	return s.inner.ListMemos(ctx, scope, after, limit)
}

func (s scopedMemos) DeleteMemo(ctx context.Context, ref MemoRef) error {
	if err := s.check(ref.Namespace); err != nil {
		return err
	}
	return s.inner.DeleteMemo(ctx, ref)
}
