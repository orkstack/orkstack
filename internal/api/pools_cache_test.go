package api_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// poolScopeClock is a settable clock for the pool-scope cache's TTL.
type poolScopeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *poolScopeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *poolScopeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// flakyVersionsStore fails Versions while failing is set.
type flakyVersionsStore struct {
	*countingOrkflowStore
	mu      sync.Mutex
	failing bool
}

func (f *flakyVersionsStore) Versions(ctx context.Context, namespace, name string) (artifacts.OrkflowVersions, error) {
	f.mu.Lock()
	failing := f.failing
	f.mu.Unlock()
	if failing {
		return artifacts.OrkflowVersions{}, errors.New("503 SlowDown")
	}
	return f.countingOrkflowStore.Versions(ctx, namespace, name)
}

func (c *countingOrkflowStore) versionCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.versions
}

func poolScopeService(t *testing.T, store engine.OrkflowStore, clock *poolScopeClock) *api.Service {
	t.Helper()
	gpu := poolQueue(t, "gpu")
	return api.NewService(api.Config{
		Orkflows:   store,
		Namespaces: registry{"default": {Name: "default"}},
		Pools:      livePools{task.DefaultQueue: 1, gpu: 1},
		PoolOrkers: poolOrkers{task.DefaultQueue: {}, gpu: {}},
		Now:        clock.Now,
	})
}

func listedPools(t *testing.T, svc *api.Service) string {
	t.Helper()
	resp, err := svc.ListPools(context.Background(), connect.NewRequest(&orkstackv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatalf("ListPools: %v", err)
	}
	return strings.Join(poolNames(resp.Msg.GetPools()), ",")
}

func applyPoolScopeYAML(t *testing.T, svc *api.Service, yaml string) {
	t.Helper()
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: yaml})); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

const mainOnlyOrkflow = `
apiVersion: orkstack/v1
kind: Orkflow
name: main-job
version: 0.1.0
tasks:
  - id: run
    uses: shell
    with: {command: "echo run"}
`

func TestPoolScope_IsComputedOncePerTTLAndAgainAfterIt(t *testing.T) {
	clock := &poolScopeClock{now: time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)}
	store := &countingOrkflowStore{MemoryOrkflowStore: engine.NewMemoryOrkflowStore()}
	svc := poolScopeService(t, store, clock)
	applyPoolScopeYAML(t, svc, gpuOrkflow)
	applyPoolScopeYAML(t, svc, mainOnlyOrkflow)

	before := store.versionCalls()
	if got := listedPools(t, svc); got != "gpu,main" {
		t.Fatalf("pools = %q, want gpu,main", got)
	}
	wave := store.versionCalls() - before
	if wave != 2 {
		t.Fatalf("one computation made %d Versions calls, want one per stored orkflow (2)", wave)
	}
	clock.Advance(api.PoolScopeCacheTTL - time.Second)
	if got := listedPools(t, svc); got != "gpu,main" {
		t.Fatalf("pools = %q, want gpu,main", got)
	}
	if calls := store.versionCalls() - before; calls != wave {
		t.Errorf("a second call within the TTL made %d more Versions calls, want 0", calls-wave)
	}
	clock.Advance(2 * time.Second)
	listedPools(t, svc)
	if calls := store.versionCalls() - before; calls != 2*wave {
		t.Errorf("a call past the TTL made %d Versions calls in all, want %d", calls, 2*wave)
	}
}

func TestPoolScope_AnApplyOnThisReplicaIsSeenAtOnce(t *testing.T) {
	clock := &poolScopeClock{now: time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)}
	store := &countingOrkflowStore{MemoryOrkflowStore: engine.NewMemoryOrkflowStore()}
	svc := poolScopeService(t, store, clock)
	applyPoolScopeYAML(t, svc, mainOnlyOrkflow)
	if got := listedPools(t, svc); got != "main" {
		t.Fatalf("pools = %q, want main", got)
	}
	applyPoolScopeYAML(t, svc, gpuOrkflow)
	if got := listedPools(t, svc); got != "gpu,main" {
		t.Fatalf("after a local apply, pools = %q, want gpu,main without waiting for the TTL", got)
	}
	if _, err := svc.DeleteOrkflow(context.Background(), connect.NewRequest(&orkstackv1.DeleteOrkflowRequest{Name: "gpu-job"})); err != nil {
		t.Fatalf("DeleteOrkflow: %v", err)
	}
	if got := listedPools(t, svc); got != "main" {
		t.Fatalf("after a local delete, pools = %q, want main without waiting for the TTL", got)
	}
}

func TestPoolScope_AFailureIsNotRememberedAndIsAskedAgain(t *testing.T) {
	clock := &poolScopeClock{now: time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)}
	store := &flakyVersionsStore{countingOrkflowStore: &countingOrkflowStore{MemoryOrkflowStore: engine.NewMemoryOrkflowStore()}}
	svc := poolScopeService(t, store, clock)
	applyPoolScopeYAML(t, svc, gpuOrkflow)

	store.mu.Lock()
	store.failing = true
	store.mu.Unlock()
	if got := listedPools(t, svc); got != "main" {
		t.Fatalf("a failing catalog: pools = %q, want the degraded main", got)
	}
	store.mu.Lock()
	store.failing = false
	store.mu.Unlock()
	if got := listedPools(t, svc); got != "gpu,main" {
		t.Fatalf("the next call within the TTL: pools = %q, want gpu,main (a failure is never cached)", got)
	}
}
