package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// RunReader is the system of record for live runs, as the router hands it out.
//
// It is declared here as well as in internal/api because a Binding cannot name
// the API's type without a cycle: internal/api imports internal/engine. The
// two are the same method set, so one implementation satisfies both and the
// API accepts what the router returns.
//
// It embeds ExecutionReader — the aggregate half, which reads the same index
// for a whole namespace instead of for one run. The two are one interface
// because they must be one cluster: a dashboard whose numbers came from
// somewhere else than the runs it links to would be a report about a
// deployment nobody is running.
type RunReader interface {
	ExecutionReader

	GetRun(ctx context.Context, runID string) (*orkstackv1.Run, error)
	ListRuns(ctx context.Context, q RunQuery) ([]*orkstackv1.Run, error)
	StreamEvents(
		ctx context.Context,
		runID string,
		fromSequence int64,
		send func(*orkstackv1.Event) error,
	) error
}

// RunStarter starts and cancels runs, as the router hands it out. Same reason
// as RunReader for living here.
type RunStarter interface {
	StartRun(
		ctx context.Context,
		runID string,
		wf *orkstackv1.Orkflow,
		input *orkstackv1.Payload,
		plan RunPlan,
	) error
	CancelRun(ctx context.Context, runID, reason string) error
}

// ErrNoBinding reports a namespace no binding serves.
var ErrNoBinding = errors.New("no binding serves this namespace")

// ErrMultiBindingUnsupported reports a router configured with more than one
// binding. Holding half of the multi-binding path would be worse than holding
// none: a Locate that guessed would answer for the wrong cluster.
var ErrMultiBindingUnsupported = errors.New("multi-binding routing is not configured")

// Binding is one cluster, and the ork namespaces it serves.
//
// Everything a request needs from Temporal comes from a Binding: the reader
// that answers for a run, the starter that begins one, and the probe that says
// whether a pool has Orkers. They travel together because they must agree —
// reading a run from one cluster while starting it on another is not a
// degraded mode, it is a wrong answer.
type Binding struct {
	// Namespaces are the ork namespaces this binding serves. Empty means all
	// of them, which is the default deployment: one cluster, every namespace.
	Namespaces []string

	Reader  RunReader
	Starter RunStarter
	Pools   PoolProbe
}

// serves reports whether this binding answers for a namespace.
func (b Binding) serves(namespace string) bool {
	if len(b.Namespaces) == 0 {
		return true
	}
	for _, name := range b.Namespaces {
		if name == namespace {
			return true
		}
	}
	return false
}

// NamespaceRouter resolves an ork namespace to the cluster serving it.
//
// It holds exactly one binding by default and no user-facing surface names it.
// The point is what it makes cheap later: hard per-namespace partitioning — a
// separate Temporal namespace, or a separate cluster, per ork namespace —
// becomes engine CONFIGURATION rather than a proto change, because every read
// and write path already asks the router which cluster to use. No request
// carries a Temporal coordinate today and none would then.
//
// GetRun(run_id) keeps working with no namespace argument because Locate is
// total for a single binding: it hands back the one it has, without inspecting
// the run id. With more than one binding that stops being free — Locate would
// have to fan out (GetRun on each, first hit wins) or consult a run→binding
// index. Neither is built, and both are additive; see SingleRouter.
type NamespaceRouter interface {
	// For returns the binding serving one ork namespace.
	For(namespace string) (Binding, error)
	// Locate returns the binding owning a run id, for the reads that carry no
	// namespace: GetRun, CancelRun, StreamEvents.
	Locate(ctx context.Context, runID string) (Binding, error)
	// Bindings returns every configured binding.
	Bindings() []Binding
}

// SingleRouter is the one-binding router: every namespace, one cluster.
//
// It is the whole of the default deployment and it asserts its own shape.
// NewRouter refuses more than one binding rather than silently serving the
// first, so nobody ships half of the multi-binding path by accident.
type SingleRouter struct {
	binding Binding
}

var _ NamespaceRouter = (*SingleRouter)(nil)

// NewSingleRouter returns a router over one binding.
func NewSingleRouter(binding Binding) *SingleRouter {
	return &SingleRouter{binding: binding}
}

// NewRouter builds the router for a set of bindings. One binding is the
// supported configuration; zero and more than one are refused, each for its
// own reason.
func NewRouter(bindings ...Binding) (NamespaceRouter, error) {
	switch len(bindings) {
	case 0:
		return nil, fmt.Errorf("%w: no bindings were configured", ErrNoBinding)
	case 1:
		return NewSingleRouter(bindings[0]), nil
	default:
		return nil, fmt.Errorf("%w: %d bindings were configured", ErrMultiBindingUnsupported, len(bindings))
	}
}

// For returns the single binding, checking that it serves the namespace asked
// for. A binding that names no namespaces serves all of them.
func (r *SingleRouter) For(namespace string) (Binding, error) {
	if namespace == "" {
		namespace = artifacts.DefaultNamespace
	}
	if !r.binding.serves(namespace) {
		return Binding{}, fmt.Errorf("%w: namespace %q (this engine serves: %s)",
			ErrNoBinding, namespace, strings.Join(sorted(r.binding.Namespaces), ", "))
	}
	return r.binding, nil
}

// Locate returns the binding owning a run id.
//
// It never inspects the run id, and that is the property GetRun rests on: with
// one binding the answer is total by construction, so a read that carries no
// namespace stays exactly as cheap as it was before the router existed. It is
// also why a run id is never parsed for meaning (AGENTS.md §7) — the seam
// that would have been tempted to is this one, and it declines.
func (r *SingleRouter) Locate(context.Context, string) (Binding, error) {
	return r.binding, nil
}

// Bindings returns the one binding.
func (r *SingleRouter) Bindings() []Binding { return []Binding{r.binding} }

func sorted(values []string) []string {
	if len(values) == 0 {
		return []string{"every namespace"}
	}
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
