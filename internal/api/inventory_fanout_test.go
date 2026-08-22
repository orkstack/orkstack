package api_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// TestListPlugins_ScansTheDurableCatalogueWithoutARoundTripApieceInSeries is
// the regression for the defect that made `orkctl plugin list` time out
// against a real deployment.
//
// The inventory learns which non-default pools exist by reading durable state
// whose size is the deployment's whole history: every namespace's orkflow
// catalog, then every version catalog of every orkflow in it. Issued one after
// another, that is a round-trip per stored object, so the read's latency grew
// with everything anyone had ever stored. Against a bucket holding 90
// namespaces and 628 orkflows it spent its entire 30s client deadline inside
// the scan and answered nothing — every pool it had not yet reached was logged
// "context canceled". The live form of this is
// cmd/orkctl/admin_surface_integration_test.go.
//
// So the property under test is HOW the scan is issued, not how fast this
// machine happens to be. The store below refuses to answer its first caller
// until several callers are inside it at once, which a serial scan can never
// satisfy. And the overlap must stay BOUNDED: a round-trip per stored object
// all at once is the same read failing again, as store throttling instead of
// as a deadline.
func TestListPlugins_ScansTheDurableCatalogueWithoutARoundTripApieceInSeries(t *testing.T) {
	const (
		namespaces   = 12
		perNamespace = 20
		width        = 8
	)
	stored := namespaces * perNamespace

	witness := newParallelWitness(width, 5*time.Second)
	heads := map[string][]*orkstackv1.Orkflow{}
	nss := registry{"default": {Name: "default"}}
	for n := 0; n < namespaces; n++ {
		name := fmt.Sprintf("tenant-%02d", n)
		nss[name] = &orkstackv1.Namespace{Name: name}
		for f := 0; f < perNamespace; f++ {
			heads[name] = append(heads[name], &orkstackv1.Orkflow{
				Name:    fmt.Sprintf("flow-%02d", f),
				Version: "0.1.0",
				Tasks:   []*orkstackv1.Task{{Id: "run", Pool: "main"}},
			})
		}
	}

	svc := api.NewService(api.Config{
		Namespaces: nss,
		Orkflows:   rendezvousCatalogue{heads: heads, witness: witness},
		Pools:      livePools{task.DefaultQueue: 1},
		Lister: describedPlugins{descriptors: []plugin.Descriptor{
			{Name: "shell", Version: "0.1.0"},
		}},
		PoolPlugins: poolPlugins{task.DefaultQueue: {"shell"}},
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	resp, err := svc.ListPlugins(ctx,
		connect.NewRequest(&orkstackv1.ListPluginsRequest{}))
	if err != nil {
		t.Fatalf("ListPlugins over %d stored orkflows: %v", stored, err)
	}
	// The answer is still the answer: a faster scan that reported a different
	// inventory would be a different bug.
	if got := fmt.Sprint(pluginNames(resp.Msg.GetPlugins())); got != "[shell]" {
		t.Errorf("inventory = %s, want [shell]: the deployment's own Orkers", got)
	}

	peak, reached := witness.observed()
	if !reached {
		t.Fatalf("the catalogue scan never had %d round-trips in flight at once "+
			"(peak %d, over %d stored orkflows): it is issuing one per stored "+
			"object in series, which is what spent the client's whole deadline "+
			"on a real bucket", width, peak, stored)
	}
	if peak > 4*width {
		t.Errorf("the scan had %d round-trips in flight at once over %d stored "+
			"orkflows: it is unbounded, and a deployment's whole catalogue at "+
			"once is the same read failing as store throttling", peak, stored)
	}
}

// parallelWitness records the most round-trips a single read had in flight at
// once, and makes the observation a fact rather than a race: a caller blocks
// until `width` of them are inside together.
//
// A serial scan is what the rendezvous is built to catch, so it must not cost
// one timeout per stored object to catch it — the first caller to give up
// releases every later one, and the run fails once.
type parallelWitness struct {
	width  int
	giveUp time.Duration

	mu       sync.Mutex
	inFlight int
	peak     int
	reached  bool

	gate chan struct{}
	once sync.Once
}

func newParallelWitness(width int, giveUp time.Duration) *parallelWitness {
	return &parallelWitness{
		width:  width,
		giveUp: giveUp,
		gate:   make(chan struct{}),
	}
}

func (w *parallelWitness) enter() {
	w.mu.Lock()
	w.inFlight++
	if w.inFlight > w.peak {
		w.peak = w.inFlight
	}
	full := w.inFlight >= w.width
	if full {
		w.reached = true
	}
	w.mu.Unlock()

	if full {
		w.open()
		return
	}
	select {
	case <-w.gate:
	case <-time.After(w.giveUp):
		w.open()
	}
}

func (w *parallelWitness) leave() {
	w.mu.Lock()
	w.inFlight--
	w.mu.Unlock()
}

func (w *parallelWitness) open() { w.once.Do(func() { close(w.gate) }) }

func (w *parallelWitness) observed() (peak int, reached bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.peak, w.reached
}

// rendezvousCatalogue is an orkflow store that answers correctly and reports
// how the caller asked. Only Versions is instrumented: it is the per-object
// round-trip, the one the scan makes once per stored orkflow.
type rendezvousCatalogue struct {
	heads   map[string][]*orkstackv1.Orkflow
	witness *parallelWitness
}

func (r rendezvousCatalogue) Put(
	context.Context, *orkstackv1.Orkflow, artifacts.HeadExpectation,
) (*orkstackv1.Orkflow, bool, error) {
	return nil, false, artifacts.ErrNotFound
}

func (r rendezvousCatalogue) Get(
	context.Context, string, string, string,
) (*orkstackv1.Orkflow, error) {
	return nil, artifacts.ErrNotFound
}

func (r rendezvousCatalogue) List(
	_ context.Context, namespace string,
) ([]*orkstackv1.Orkflow, error) {
	return append([]*orkstackv1.Orkflow(nil), r.heads[namespace]...), nil
}

func (r rendezvousCatalogue) Versions(
	_ context.Context, namespace, name string,
) (artifacts.OrkflowVersions, error) {
	r.witness.enter()
	defer r.witness.leave()
	for _, head := range r.heads[namespace] {
		if head.GetName() == name {
			return artifacts.OrkflowVersions{All: []*orkstackv1.Orkflow{head}}, nil
		}
	}
	return artifacts.OrkflowVersions{}, artifacts.ErrNotFound
}

func (r rendezvousCatalogue) SetCurrentVersion(
	context.Context, string, string, string,
) (string, error) {
	return "", artifacts.ErrNotFound
}
