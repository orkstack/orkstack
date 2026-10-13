package engine_test

import (
	"context"
	"errors"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// TestSingleRouter_LocatesARunWithoutParsingItsID is the property GetRun rests
// on, and the reason the seam can exist at all today.
//
// A read that carries no namespace — GetRun, CancelRun, StreamEvents — has
// only a run id to go on. With one binding the answer is total by
// construction: the router hands back its one binding without looking at the
// id. So the seam costs the read path nothing, and no run id is ever parsed
// for meaning (AGENTS.md §7), which is the temptation a router of this shape
// would otherwise create.
func TestSingleRouter_LocatesARunWithoutParsingItsID(t *testing.T) {
	binding := engine.Binding{Reader: nopReader{}}
	router := engine.NewSingleRouter(binding)

	for _, runID := range []string{
		"ork-1787000000000000000-4242",
		"", // not a run id at all: Locate still answers, because it never asks
		"anything",
	} {
		located, err := router.Locate(context.Background(), runID)
		if err != nil {
			t.Fatalf("Locate(%q): %v", runID, err)
		}
		if located.Reader != binding.Reader {
			t.Errorf("Locate(%q) returned a different binding", runID)
		}
	}
}

// TestSingleRouter_ServesEveryNamespaceUntilOneIsNamed covers the default
// deployment (one cluster, every namespace) and the configured one (a binding
// that names the namespaces it serves).
func TestSingleRouter_ServesEveryNamespaceUntilOneIsNamed(t *testing.T) {
	open := engine.NewSingleRouter(engine.Binding{Reader: nopReader{}})
	for _, name := range []string{"", "default", "payments", "anything"} {
		if _, err := open.For(name); err != nil {
			t.Errorf("For(%q) on an unrestricted binding: %v", name, err)
		}
	}

	scoped := engine.NewSingleRouter(engine.Binding{
		Namespaces: []string{"payments"},
		Reader:     nopReader{},
	})
	if _, err := scoped.For("payments"); err != nil {
		t.Errorf("For(payments) on the binding that serves it: %v", err)
	}
	_, err := scoped.For("ops")
	if !errors.Is(err, engine.ErrNoBinding) {
		t.Errorf("For(ops) = %v, want ErrNoBinding", err)
	}
}

// TestNewRouter_RefusesToShipHalfOfMultiBindingRouting is the honest half.
//
// With more than one binding, Locate stops being free: it becomes a fan-out
// across clusters or needs a run→binding index, and neither is built. Refusing
// the configuration is what keeps someone from shipping a router that answers
// for the wrong cluster and calling it partitioning. The point of the seam is
// that adding it later is a configuration change rather than a proto change —
// no request carries a Temporal coordinate today, and none would then.
func TestNewRouter_RefusesToShipHalfOfMultiBindingRouting(t *testing.T) {
	if _, err := engine.NewRouter(); !errors.Is(err, engine.ErrNoBinding) {
		t.Errorf("NewRouter() with no bindings = %v, want ErrNoBinding", err)
	}
	router, err := engine.NewRouter(engine.Binding{Reader: nopReader{}})
	if err != nil || router == nil {
		t.Fatalf("NewRouter with one binding = %v, %v", router, err)
	}
	if got := len(router.Bindings()); got != 1 {
		t.Errorf("router holds %d bindings, want exactly 1 by default", got)
	}
	_, err = engine.NewRouter(
		engine.Binding{Namespaces: []string{"a"}},
		engine.Binding{Namespaces: []string{"b"}},
	)
	if !errors.Is(err, engine.ErrMultiBindingUnsupported) {
		t.Errorf("NewRouter with two bindings = %v, want ErrMultiBindingUnsupported", err)
	}
}

// nopReader satisfies the reader half of a binding without a cluster.
type nopReader struct{}

func (nopReader) GetRun(context.Context, string) (*orkstackv1.Run, error) { return nil, nil }
func (nopReader) ListRuns(context.Context, engine.RunQuery) ([]*orkstackv1.Run, error) {
	return nil, nil
}
func (nopReader) StreamEvents(
	context.Context, string, int64, func(*orkstackv1.Event) error,
) error {
	return nil
}
func (nopReader) Executions(context.Context, engine.StatsWindow) (engine.Executions, error) {
	return engine.Executions{}, nil
}

func (nopReader) RunningCount(context.Context, string, int64) (int64, error) { return 0, nil }

func (nopReader) RunStart(context.Context, string) (*engine.RunStartRecord, error) {
	return nil, engine.ErrRunNotFound
}
