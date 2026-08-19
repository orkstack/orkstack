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
// opposite of idempotent. allowed_pools is compared as a set, because a list
// reordered is the same allow-list and refusing a reformatted script would be
// a collision that is not one.
func specDifference(stored, requested *orkstackv1.Namespace) string {
	if got, want := stored.GetDescription(), requested.GetDescription(); got != want {
		return fmt.Sprintf("description is %q, not %q", got, want)
	}
	if got, want := stored.GetDefaultPool(), requested.GetDefaultPool(); got != want {
		return fmt.Sprintf("default_pool is %q, not %q", orDefaultPool(got), orDefaultPool(want))
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

// orDefaultPool renders an unset default pool as the pool it means, so a
// difference report never compares "" against the name of the same thing.
func orDefaultPool(pool string) string {
	if pool == "" {
		return artifacts.DefaultPool
	}
	return pool
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
// sends it back. An empty etag is an unconditional write — the escape hatch
// for a caller who genuinely means "whatever is there now", asked for by
// clearing a field rather than by forgetting one.
//
// The default namespace needs no special case: when nobody wrote its document
// the read hands back a synthesized one whose etag is empty, so the first
// update writes it unconditionally, which is the same rule as any other empty
// etag.
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
	stored, err := registry.Put(ctx, proto.Clone(spec).(*orkstackv1.Namespace),
		artifacts.PutConditions{IfMatch: spec.GetEtag()})
	switch {
	case err == nil:
		return connect.NewResponse(&orkstackv1.UpdateNamespaceResponse{Namespace: stored}), nil
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
