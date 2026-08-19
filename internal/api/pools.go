package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// namespace resolves a namespace name to its registered document.
//
// An empty name is the built-in default. A name nothing registered is
// NotFound; a registry that cannot be reached is Unavailable, and the two are
// kept apart because they need opposite reactions — fix the request, or fix
// the deployment.
//
// A NotFound also lists the namespaces that DO exist. "namespace not found" is
// true and useless: the caller mistyped one of a handful of names, and the
// names are one List away on a server that has already decided to answer with
// an error. Making them run a second command to learn what the first should
// have said is the difference between an error and an answer.
func (s *Service) namespace(ctx context.Context, name string) (*orkstackv1.Namespace, error) {
	ns, err := s.pools.Namespace(ctx, name)
	switch {
	case err == nil:
		return ns, nil
	case errors.Is(err, artifacts.ErrNotFound):
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf(
			"namespace %q does not exist%s", artifacts.ResolveNamespace(name),
			s.existingNamespaces(ctx)))
	case errors.Is(err, engine.ErrNoNamespaceRegistry):
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	default:
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("read namespace %q: %w", artifacts.ResolveNamespace(name), err))
	}
}

// existingNamespaces renders the registered names as a suffix for a NotFound.
//
// It is best-effort by construction: it is decorating an error that has
// already been decided, so a registry that cannot be listed adds nothing
// rather than replacing "you mistyped the namespace" with "the registry is
// unwell". The caller still gets the accurate half.
func (s *Service) existingNamespaces(ctx context.Context) string {
	if s.namespaces == nil {
		return ""
	}
	registered, err := s.namespaces.List(ctx)
	if err != nil || len(registered) == 0 {
		return ""
	}
	names := make([]string, 0, len(registered))
	for _, ns := range registered {
		names = append(names, ns.GetName())
	}
	sort.Strings(names)
	return "; these do: " + strings.Join(names, ", ")
}

// ListPools reports the pools a namespace may dispatch onto, with the Orkers
// serving each right now and the plugins those Orkers run.
//
// Everything in the answer comes from the cluster: the Orker count from what
// is polling for the pool's work, the plugin set from the Orkers themselves.
// Nothing is read from this process's memory, so any replica answers — one
// that started nothing gives the same reply as the one that started
// everything, which is the property AGENTS.md §1.7 asks of every read.
func (s *Service) ListPools(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListPoolsRequest],
) (*connect.Response[orkstackv1.ListPoolsResponse], error) {
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	binding, err := s.binding(ns.GetName())
	if err != nil {
		return nil, err
	}

	// An unrestricted namespace has no list to enumerate, so the pools its own
	// orkflows name are what "the pools in scope" can honestly mean. A
	// namespace with an allow-list uses that instead, and the resolver ignores
	// this.
	named, err := s.poolsNamedByOrkflows(ctx, ns)
	if err != nil {
		return nil, err
	}

	resolver := s.resolverFor(binding)
	pools, err := resolver.Pools(ctx, ns, named)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("list pools for namespace %q: %w", ns.GetName(), err))
	}
	// The default pool comes from the resolver, not from the document: it is
	// where THIS deployment sends a task that names none, which is the only
	// answer a client can act on.
	return connect.NewResponse(&orkstackv1.ListPoolsResponse{
		Pools:       pools,
		DefaultPool: resolver.DefaultPool(ns),
	}), nil
}

// poolsNamedByOrkflows collects every pool the namespace's stored definitions
// mention. It is a report, so an unreachable store is not fatal: the answer
// falls back to the pools the namespace itself defines.
func (s *Service) poolsNamedByOrkflows(
	ctx context.Context,
	ns *orkstackv1.Namespace,
) ([]string, error) {
	if len(ns.GetAllowedPools()) > 0 {
		return nil, nil
	}
	orkflows, err := s.orkflows.List(ctx, ns.GetName())
	if err != nil {
		if errors.Is(err, artifacts.ErrNotFound) {
			return nil, nil
		}
		return nil, orkflowStoreError(err, "list orkflows in namespace %q", ns.GetName())
	}
	seen := map[string]struct{}{}
	for _, wf := range orkflows {
		for _, t := range wf.GetTasks() {
			if pool := t.GetPool(); pool != "" {
				seen[pool] = struct{}{}
			}
		}
	}
	named := make([]string, 0, len(seen))
	for pool := range seen {
		named = append(named, pool)
	}
	sort.Strings(named)
	return named, nil
}
