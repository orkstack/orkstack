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
// An empty name is the built-in default. A name the rule forbids is
// InvalidArgument and never reaches the registry — one clear answer instead of
// a NotFound that reads as an empty namespace. A name nothing registered is
// NotFound; a registry that cannot be reached is Unavailable, and the two are
// kept apart because they need opposite reactions — fix the request, or fix
// the deployment.
//
// A NotFound also lists the namespaces that DO exist. "namespace not found" is
// true and useless: the caller mistyped one of a handful of names, and the
// names are one List away on a server that has already decided to answer with
// an error. Making them run a second command to learn what the first should
// have said is the difference between an error and an answer.
//
// This is the one seam every namespace-carrying request passes through —
// ListOrkflows, GetOrkflow, ApplyOrkflow, SubmitRun, ListRuns, the run-id
// paths, ListPools and the namespace RPCs themselves — which is why the rule
// is checked here rather than restated at each caller.
func (s *Service) namespace(ctx context.Context, name string) (*orkstackv1.Namespace, error) {
	if err := validNamespaceSelector(name); err != nil {
		return nil, err
	}
	ns, err := s.pools.Namespace(ctx, name)
	switch {
	case err == nil:
		return ns, nil
	case errors.Is(err, artifacts.ErrInvalidNamespace):
		// The boundary above refuses every name this store would, so reaching
		// here means a store with a stricter rule of its own. It is still the
		// request that is wrong, not the deployment.
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
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
// Liveness and plugins come from the cluster: the Orker count from what is
// polling for the pool's work, the plugin names from the Orkers themselves.
// Nothing is read from this process's memory, so any replica answers — one
// that started nothing gives the same reply as the one that started
// everything, which is the property AGENTS.md §1.7 asks of every read.
//
// It is the SAME report the dashboard renders (StatsHandler.pools), through
// the same resolver, degrading the same way. Two paths to one question drift,
// and the drift shows up as an Admin page and a dashboard disagreeing about
// whether a pool is healthy.
//
// It NARROWS and does not refuse. Every source is best-effort and each one's
// loss costs exactly what that source knew: a pool the cluster did not answer
// for keeps its row and carries Pool.unavailable_reason; a pool whose Orkers
// did not answer keeps its count and loses its plugin list; an unreadable
// orkflow store narrows the scope to the pools the namespace document itself
// names. The one error left is the one that leaves nothing to report — a
// namespace that cannot be resolved, or a default pool this deployment cannot
// route, under which no task of this namespace could run at all.
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
	//
	// A store that cannot be read narrows this report rather than replacing it
	// with an error. The answer has two halves from two sources — which pools
	// are in scope (the namespace document plus this) and who is serving them
	// (the cluster) — and only this half needs the store. Pool health during a
	// bucket wobble is precisely when an operator is looking, so what remains
	// is the pools the namespace itself defines.
	//
	// A narrowed answer that says nothing is indistinguishable from a complete
	// one, so the degradation is logged with the namespace and the failure.
	// There is no field on the wire for THIS half: which pools are in scope is
	// a set, and a set has no room to say "and possibly others". The other
	// half — a pool that could not be probed — does have one, because a row
	// exists to carry it (Pool.unavailable_reason).
	named, err := s.poolsNamedByOrkflows(ctx, ns)
	if err != nil {
		s.logger.Warn("list pools: the orkflow store could not be read, so only the "+
			"namespace's own pools are reported",
			"namespace", ns.GetName(), "error", err)
		named = nil
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
// mention, across every stored version. A namespace with no definitions has no
// extra pools, which is not a failure.
//
// What a store failure MEANS is the caller's decision, not this one's: the
// caller degrades, and this reports.
func (s *Service) poolsNamedByOrkflows(
	ctx context.Context,
	ns *orkstackv1.Namespace,
) ([]string, error) {
	// List returns one resolved version per orkflow name. That is not enough:
	// an explicit-version submit can still run any stored version, so every
	// version catalog must be scanned before the pool set is complete.
	orkflows, err := s.orkflows.List(ctx, ns.GetName())
	if err != nil {
		if errors.Is(err, artifacts.ErrNotFound) {
			return nil, nil
		}
		return nil, orkflowStoreError(err, "list orkflows in namespace %q", ns.GetName())
	}
	seen := map[string]struct{}{}
	add := func(wf *orkstackv1.Orkflow) {
		for _, t := range wf.GetTasks() {
			if pool := t.GetPool(); pool != "" {
				seen[pool] = struct{}{}
			}
		}
	}
	for _, head := range orkflows {
		catalog, err := s.orkflows.Versions(ctx, ns.GetName(), head.GetName())
		if err != nil {
			if errors.Is(err, artifacts.ErrNotFound) {
				add(head)
				continue
			}
			return nil, orkflowStoreError(err,
				"list versions of orkflow %q in namespace %q",
				head.GetName(), ns.GetName())
		}
		if len(catalog.All) == 0 {
			add(head)
			continue
		}
		for _, wf := range catalog.All {
			add(wf)
		}
	}
	named := make([]string, 0, len(seen))
	for pool := range seen {
		named = append(named, pool)
	}
	sort.Strings(named)
	return named, nil
}
