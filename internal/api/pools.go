package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
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
// The pool scope comes from the durable namespace and orkflow stores. Live
// counts and plugin names come from the cluster; this handler never answers
// those parts from process memory, so a replica that started nothing can read
// the same view.
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

	// A pool list is a complete answer only when the durable orkflow catalog
	// was readable. Every stored version is a possible explicit-version submit
	// input, so looking only at the current version can hide a durable route;
	// the submit path applies the namespace policy again before dispatch.
	named, err := s.poolsNamedByOrkflows(ctx, ns)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("list pools for namespace %q: %w", ns.GetName(), err))
	}

	pools, err := s.listPoolsFromCluster(ctx, binding, ns, named)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("list pools for namespace %q: %w", ns.GetName(), err))
	}
	return connect.NewResponse(&orkstackv1.ListPoolsResponse{
		Pools:       pools,
		DefaultPool: s.pools.DefaultPool(ns),
	}), nil
}

// listPoolsFromCluster builds the admin health view directly from the pool
// probe. The engine resolver is the submit path; its report used to discard a
// stale observation's LastSeen. Keeping this read here lets a dead pool retain
// its last observed time while still reporting zero live Orkers.
func (s *Service) listPoolsFromCluster(
	ctx context.Context,
	binding engine.Binding,
	ns *orkstackv1.Namespace,
	extra []string,
) ([]*orkstackv1.Pool, error) {
	if binding.Pools == nil {
		return nil, fmt.Errorf("pool liveness is not configured")
	}
	candidates := []string{engine.DefaultPoolFor(ns)}
	candidates = append(candidates, ns.GetAllowedPools()...)
	// Explicit-version submissions can still run any stored version, so named
	// pools stay in scope even when the namespace also has an allow-list. The
	// submit path re-checks that policy; this read must not hide a durable route
	// merely because the current policy changed after it was stored.
	candidates = append(candidates, extra...)

	routes := map[string]task.Route{}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		route, err := task.RouteForPool(candidate, s.rawQueue)
		if err != nil {
			return nil, fmt.Errorf("route pool %q: %w", candidate, err)
		}
		routes[route.Queue] = route
	}
	ordered := make([]task.Route, 0, len(routes))
	for _, route := range routes {
		ordered = append(ordered, route)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Pool < ordered[j].Pool })

	defaultPool := s.pools.DefaultPool(ns)
	out := make([]*orkstackv1.Pool, 0, len(ordered))
	for _, route := range ordered {
		observed, err := binding.Pools.ObservePool(ctx, route.Queue)
		if err != nil {
			return nil, &engine.PoolUnreachableError{Pool: route.Pool, Err: err}
		}
		pool := &orkstackv1.Pool{
			Name:      route.Pool,
			IsDefault: route.Pool == defaultPool,
		}
		if !observed.LastSeen.IsZero() {
			pool.LastSeen = timestamppb.New(observed.LastSeen.UTC())
		}
		if !observed.Fresh(s.now(), engine.DefaultPollerFreshness) {
			out = append(out, pool)
			continue
		}
		pool.LiveOrkerCount = int32(observed.Orkers)
		if observed.Orkers > 0 {
			plugins, err := s.listPoolPluginNames(ctx, route, observed.Orkers)
			if err != nil {
				return nil, &engine.PoolUnreachableError{Pool: route.Pool, Err: err}
			}
			pool.Plugins = plugins
		}
		out = append(out, pool)
	}
	return out, nil
}

func (s *Service) listPoolPluginNames(
	ctx context.Context,
	route task.Route,
	orkers int,
) ([]string, error) {
	if orkers > 1 && s.poolInventory == nil {
		return nil, fmt.Errorf(
			"pool %q has %d live Orkers but complete plugin inventory is not configured",
			route.Pool, orkers)
	}
	if s.poolInventory != nil {
		descriptors, err := s.poolInventory.PluginsOnPool(ctx, route.Queue)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(descriptors))
		for _, descriptor := range descriptors {
			names = append(names, descriptor.Name)
		}
		sort.Strings(names)
		return names, nil
	}
	if s.pools == nil || s.pools.Plugins == nil {
		return nil, fmt.Errorf("pool %q has live Orkers but plugin discovery is not configured", route.Pool)
	}
	names, err := s.pools.Plugins.PluginsOnPool(ctx, route.Queue)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// poolsNamedByOrkflows collects every pool the namespace's stored definitions
// mention, across every stored version. A namespace with no definitions has no
// extra pools, which is not a failure.
//
// A store failure is returned to the caller. A complete-looking pool list must
// never silently omit a pool because the durable catalog was unavailable.
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
