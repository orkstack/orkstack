package main

import (
	"context"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// oneGate is a cluster whose namespaces hold one limit, as ConcurrencyService
// reads it; the engine's own reader needs a Temporal.
type oneGate struct {
	engine.ConcurrencyAdmissionReader
	view engine.GateView
}

func (g oneGate) Gate(_ context.Context, id string) (engine.GateView, bool, error) {
	return g.view, id == g.view.GateID, nil
}

func (g oneGate) Gates(context.Context, string, int, []byte) ([]engine.GateView, []byte, error) {
	return []engine.GateView{g.view}, nil, nil
}

// TestConcurrency_ReadsAnOrkflowsOwnKey: the key `concurrency list` prints
// for an Orkflow that declares none is the one `get` and `list -F key=` take,
// through the engine's own service.
func TestConcurrency_ReadsAnOrkflowsOwnKey(t *testing.T) {
	key := engine.DefaultRunConcurrencyKey("default", "migrate")
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.ConcurrencyReader = oneGate{view: engine.GateView{GateID: engine.GateID("default", "", key), Namespace: "default", Key: key,
			Answering: true, Rebuild: engine.GateRebuildNone, Holders: []engine.GateEntryView{{Run: "ork-holder", Limit: 1}}}}
	})
	listed := f.run("concurrency", "list")
	if listed.code != exitOK {
		t.Fatalf("concurrency list exited %d: %s", listed.code, listed.stderr)
	}
	printed := ""
	for _, line := range strings.Split(listed.stdout, "\n") {
		if strings.HasPrefix(line, "ork:") {
			printed = strings.Fields(line)[0]
		}
	}
	if printed != key {
		t.Fatalf("concurrency list printed %q; want %q in\n%s", printed, key, listed.stdout)
	}
	got := f.run("concurrency", "get", printed)
	if got.code != exitOK || !strings.Contains(got.stdout, key) || !strings.Contains(got.stdout, "ork-holder") {
		t.Errorf("concurrency get %s exited %d:\n%s%s", printed, got.code, got.stdout, got.stderr)
	}
	one := f.run("concurrency", "list", "-F", "key="+printed)
	if one.code != exitOK || !strings.Contains(one.stdout, key) {
		t.Errorf("concurrency list -F key=%s exited %d:\n%s%s", printed, one.code, one.stdout, one.stderr)
	}
	other := f.run("concurrency", "get", engine.DefaultRunConcurrencyKey("payments", "migrate"))
	if other.code == exitOK || !strings.Contains(other.stderr, "no run holds or waits for concurrency key") {
		t.Errorf("another namespace's own key exited %d: %s", other.code, other.stderr)
	}
}
