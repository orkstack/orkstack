package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// registry returns the durable namespace registry, or the refusal a process
// without one owes its caller.
//
// There is no in-memory fallback, deliberately. A namespace this process
// invented would be gone at the next restart and absent from every other
// replica, so a create that "succeeded" would be followed by a submit saying
// the namespace does not exist. FailedPrecondition names the deployment as the
// thing to fix.
func (s *Service) registry() (engine.NamespaceRegistry, error) {
	if s.namespaces == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("this engine has no namespace registry configured, so namespaces "+
				"cannot be created, changed or removed"))
	}
	return s.namespaces, nil
}

// validNamespaceName holds a client-supplied name to the registry's own rule
// before anything is written.
//
// The rule is the store's (artifacts.NamespaceKey validates what it addresses),
// asked here so the refusal arrives as InvalidArgument naming the field rather
// than as whatever a backend makes of an unaddressable key.
func validNamespaceName(name string) error {
	if name == "" {
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("namespace name is required"))
	}
	if _, err := artifacts.NamespaceKey(name); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return nil
}

// validNamespaceSelector is the same rule for a request that SELECTS a
// namespace rather than naming one to write: empty is the default namespace,
// which is the contract of every `namespace` field on the wire.
//
// It is asked on every path, not only the writing ones. A read used to hand
// the raw name to the registry and answer with whatever came back, so
// `Payments` was NotFound against an in-memory registry — "does not exist;
// these do: default", which reads as an empty namespace and sends the caller
// to create it — and Unavailable against a store that validates its own keys,
// which blames the deployment for a typo. Neither is true: the name cannot be
// a namespace anywhere, and that is a bad request.
func validNamespaceSelector(name string) error {
	if name == "" {
		return nil
	}
	return validNamespaceName(name)
}

// CreateNamespace registers a namespace, idempotently for an identical spec.
//
// The write is conditional on the object being absent, so two replicas racing
// the same name produce one winner and one refusal rather than two writes
// where the second silently replaced the first. The refusal is then compared
// against what is stored: an identical spec is the same request arriving twice
// — a re-run provisioning script — and is answered with the existing document;
// a different spec is a collision and says which field differs.
func (s *Service) CreateNamespace(
	ctx context.Context,
	req *connect.Request[orkstackv1.CreateNamespaceRequest],
) (*connect.Response[orkstackv1.CreateNamespaceResponse], error) {
	registry, err := s.registry()
	if err != nil {
		return nil, err
	}
	spec := req.Msg.GetNamespace()
	if err := validNamespaceName(spec.GetName()); err != nil {
		return nil, err
	}
	if artifacts.ResolveNamespace(spec.GetName()) == artifacts.DefaultNamespace {
		// Wanting the default namespace is not an error; it is already there.
		// An identical spec is the no-op it looks like, and a different one is
		// refused as any other collision is.
		return s.createConflict(ctx, spec)
	}

	stored, err := registry.Put(ctx, proto.Clone(spec).(*orkstackv1.Namespace),
		artifacts.PutConditions{IfAbsent: true})
	switch {
	case err == nil:
		return connect.NewResponse(&orkstackv1.CreateNamespaceResponse{
			Namespace: stored,
			Created:   true,
		}), nil
	case errors.Is(err, artifacts.ErrPreconditionFailed):
		return s.createConflict(ctx, spec)
	default:
		return nil, namespaceStoreError(err, "create namespace %q", spec.GetName())
	}
}

// createConflict answers a create whose name is already taken: the same spec is
// an idempotent repeat, a different one is a collision.
func (s *Service) createConflict(
	ctx context.Context,
	spec *orkstackv1.Namespace,
) (*connect.Response[orkstackv1.CreateNamespaceResponse], error) {
	existing, err := s.namespaces.Get(ctx, spec.GetName())
	if err != nil {
		return nil, namespaceStoreError(err, "read namespace %q", spec.GetName())
	}
	if difference := specDifference(existing, spec); difference != "" {
		return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf(
			"namespace %q already exists and differs: %s", spec.GetName(), difference))
	}
	return connect.NewResponse(&orkstackv1.CreateNamespaceResponse{
		Namespace: existing,
		Created:   false,
	}), nil
}

// specDifference names the first field in which two namespace specs disagree,
// or "" when they are the same namespace said twice.
//
// Only the fields a client sets are compared. created_at, updated_at and the
// etag are the registry's and always differ between a request and a stored
// document; comparing them would make every repeat a collision, which is the
// opposite of idempotent.
//
// Two fields are compared through the meaning of their value rather than its
// spelling, because a spec is identical when the system would act on it
// identically. allowed_pools is a set: a list reordered is the same
// allow-list, and refusing a reformatted script would be a collision that is
// not one. default_pool is compared through engine.DefaultPoolFor — the
// resolution rule itself — because an unset value MEANS main: `orkctl
// namespace list` prints main for a stored "", and `--default-pool`
// advertises main as its default, so a user who reads back what the CLI told
// them and re-runs their create is sending the same spec. Comparing the raw
// strings refused exactly that, with `default_pool is "main", not "main"` — a
// difference report whose two sides print identically.
func specDifference(stored, requested *orkstackv1.Namespace) string {
	if got, want := stored.GetDescription(), requested.GetDescription(); got != want {
		return fmt.Sprintf("description is %q, not %q", got, want)
	}
	if got, want := engine.DefaultPoolFor(stored), engine.DefaultPoolFor(requested); got != want {
		return fmt.Sprintf("default_pool is %q, not %q", got, want)
	}
	if got, want := sortedCopy(stored.GetAllowedPools()),
		sortedCopy(requested.GetAllowedPools()); !equalStrings(got, want) {
		return fmt.Sprintf("allowed_pools are [%s], not [%s]",
			strings.Join(got, " "), strings.Join(want, " "))
	}
	if got, want := stored.GetLabels(), requested.GetLabels(); !equalLabels(got, want) {
		return fmt.Sprintf("labels are %s, not %s", renderLabels(got), renderLabels(want))
	}
	return ""
}

func sortedCopy(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalLabels(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}

func renderLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, key+"="+labels[key])
	}
	return "{" + strings.Join(pairs, " ") + "}"
}

// GetNamespace reads one namespace document. The default namespace always
// resolves, whether or not anyone created it.
func (s *Service) GetNamespace(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetNamespaceRequest],
) (*connect.Response[orkstackv1.GetNamespaceResponse], error) {
	ns, err := s.namespace(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.GetNamespaceResponse{Namespace: ns}), nil
}

// ListNamespaces returns every registered namespace, ordered by name, with the
// built-in default always among them.
func (s *Service) ListNamespaces(
	ctx context.Context,
	_ *connect.Request[orkstackv1.ListNamespacesRequest],
) (*connect.Response[orkstackv1.ListNamespacesResponse], error) {
	if s.namespaces == nil {
		// A process with no registry still has the namespace every orkflow
		// that names none is in. An empty list would say the deployment has no
		// namespaces, which is never true.
		return connect.NewResponse(&orkstackv1.ListNamespacesResponse{
			Namespaces: []*orkstackv1.Namespace{artifacts.DefaultNamespaceDocument()},
		}), nil
	}
	registered, err := s.namespaces.List(ctx)
	if err != nil {
		return nil, namespaceStoreError(err, "list namespaces")
	}
	return connect.NewResponse(&orkstackv1.ListNamespacesResponse{Namespaces: registered}), nil
}

// UpdateNamespace replaces a namespace's mutable fields, compare-and-swapping
// on the etag the caller read.
//
// The etag travels inside the document rather than in a field of its own
// because it belongs to the document: a client reads a namespace, edits it and
// sends it back. Every update is conditional, and the etag says on WHAT: a
// token names the stored version it replaces, and no token says "the version I
// read had no stored bytes at all", which is written IfAbsent.
//
// There is deliberately no unconditional escape hatch. It used to be the
// meaning of an empty etag, and `default` is why that could not stand: until
// somebody writes metadata/namespaces/default.json a read hands back a
// SYNTHESIZED document, whose etag is empty because there is nothing stored to
// name — not because the caller asked for anything. Read as an escape hatch,
// the first two edits of `default` were both unconditional writes and one of
// them vanished with no error, which is exactly what Namespace.etag promises
// cannot happen. A caller who genuinely means "whatever is there now" reads
// first and sends that token back; that round trip IS the compare-and-swap.
func (s *Service) UpdateNamespace(
	ctx context.Context,
	req *connect.Request[orkstackv1.UpdateNamespaceRequest],
) (*connect.Response[orkstackv1.UpdateNamespaceResponse], error) {
	registry, err := s.registry()
	if err != nil {
		return nil, err
	}
	spec := req.Msg.GetNamespace()
	if err := validNamespaceName(spec.GetName()); err != nil {
		return nil, err
	}
	// An update is not a create: a name nobody registered is NotFound, naming
	// the ones that do exist, rather than a namespace quietly brought into
	// being by a typo.
	if _, err := s.namespace(ctx, spec.GetName()); err != nil {
		return nil, err
	}
	cond := artifacts.PutConditions{IfMatch: spec.GetEtag()}
	if spec.GetEtag() == "" {
		cond = artifacts.PutConditions{IfAbsent: true}
	}
	stored, err := registry.Put(ctx, proto.Clone(spec).(*orkstackv1.Namespace), cond)
	switch {
	case err == nil:
		return connect.NewResponse(&orkstackv1.UpdateNamespaceResponse{Namespace: stored}), nil
	case errors.Is(err, artifacts.ErrPreconditionFailed) && cond.IfAbsent:
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"namespace %q was written by somebody else since it was read "+
				"(it had no stored version then and has one now); "+
				"read it again and re-apply the change", spec.GetName()))
	case errors.Is(err, artifacts.ErrPreconditionFailed):
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"namespace %q changed since it was read (etag %q is stale); "+
				"read it again and re-apply the change", spec.GetName(), spec.GetEtag()))
	default:
		return nil, namespaceStoreError(err, "update namespace %q", spec.GetName())
	}
}

// DeleteNamespace removes a namespace that holds no orkflows.
//
// The catalog is checked here and the names of what is in the way are
// reported: the registry's own refusal knows a count, and a count is not a next
// step. The store refuses too, and that refusal is the one no client can
// bypass — this one is the one that can say what to delete first.
func (s *Service) DeleteNamespace(
	ctx context.Context,
	req *connect.Request[orkstackv1.DeleteNamespaceRequest],
) (*connect.Response[orkstackv1.DeleteNamespaceResponse], error) {
	registry, err := s.registry()
	if err != nil {
		return nil, err
	}
	name := artifacts.ResolveNamespace(req.Msg.GetName())
	if name == artifacts.DefaultNamespace {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"namespace %q cannot be deleted: every orkflow that names no namespace is in it",
			name))
	}
	// A name nobody registered is NotFound, and says which names exist.
	if _, err := s.namespace(ctx, name); err != nil {
		return nil, err
	}
	held, err := s.orkflows.List(ctx, name)
	if err != nil {
		return nil, orkflowStoreError(err, "list orkflows in namespace %q", name)
	}
	if len(held) > 0 {
		names := make([]string, 0, len(held))
		for _, wf := range held {
			names = append(names, wf.GetName())
		}
		sort.Strings(names)
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"namespace %q still holds %d orkflow(s) (%s); delete them first",
			name, len(names), strings.Join(names, ", ")))
	}
	if err := registry.Delete(ctx, name); err != nil {
		return nil, namespaceStoreError(err, "delete namespace %q", name)
	}
	return connect.NewResponse(&orkstackv1.DeleteNamespaceResponse{}), nil
}

// namespaceStoreError maps a registry failure onto a Connect code.
//
// A name the store cannot address is the caller's (InvalidArgument), a missing
// document is NotFound, a refused delete or a lost race is FailedPrecondition,
// and anything else means the registry could not be reached — Unavailable,
// never answered from memory.
func namespaceStoreError(err error, format string, args ...any) error {
	subject := fmt.Sprintf(format, args...)
	switch {
	case errors.Is(err, artifacts.ErrInvalidNamespace):
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%s: %w", subject, err))
	case errors.Is(err, artifacts.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("%s: not found", subject))
	case errors.Is(err, artifacts.ErrNamespaceNotEmpty),
		errors.Is(err, artifacts.ErrPreconditionFailed):
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s: %w", subject, err))
	default:
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("%s: %w", subject, err))
	}
}
