package engine_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// eachSupportPools answers DescribeOrker per channel with each_supported as
// given, and records every channel it was asked about.
type eachSupportPools struct {
	mu        sync.Mutex
	supported map[string]bool
	asked     []string
	fail      bool
}

func (p *eachSupportPools) OrkerOnPool(_ context.Context, queue string) (*orkstackv1.DescribeOrkerResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.asked = append(p.asked, queue)
	if p.fail {
		return nil, errors.New("describe timed out")
	}
	return &orkstackv1.DescribeOrkerResult{
		OrkerId: "gpu-3", BuildId: "2026.09.01", EachSupported: p.supported[queue],
	}, nil
}

const eachPoolsDocument = `
name: each-pools
tasks:
  - id: list
    uses: shell
    pool: cpu
    with: {op: exec}
  - id: convert
    uses: shell
    pool: gpu
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 10}
    with: {op: exec}
  - id: merge
    uses: shell
    pool: gpu
    needs: [convert]
    with: {op: exec}
  - id: report
    uses: shell
    pool: io
    needs: [merge]
    with: {op: exec}
`

func eachPoolsResolver(p *eachSupportPools, logger *slog.Logger) *engine.PoolResolver {
	cpu, _ := task.QueueForPool("cpu", "")
	gpu, _ := task.QueueForPool("gpu", "")
	io, _ := task.QueueForPool("io", "")
	return &engine.PoolResolver{
		Probe:  livePools{cpu: 1, gpu: 1, io: 1},
		Orkers: p,
		Logger: logger,
	}
}

func TestPoolResolverPlan_RefusesAPoolWhoseOrkerPredatesFanOut(t *testing.T) {
	p := &eachSupportPools{supported: map[string]bool{}}
	_, _, err := eachPoolsResolver(p, nil).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, eachPoolsDocument), false)
	var refused *engine.PoolNoEachSupportError
	if !errors.As(err, &refused) {
		t.Fatalf("Plan = %v, want PoolNoEachSupportError", err)
	}
	if refused.Pool != "gpu" || fmt.Sprint(refused.Tasks) != "[convert merge]" {
		t.Fatalf("refusal = %+v", refused)
	}
	for _, want := range []string{
		`pool "gpu" (namespace "data", tasks: convert, merge)`,
		`Orker "gpu-3" (build "2026.09.01")`, "predates fan-out (each:) support",
		"give merge no items to read", "Upgrade the pool's Orkers",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("message %q lacks %q", err.Error(), want)
		}
	}
	gpu, _ := task.QueueForPool("gpu", "")
	p.supported[gpu] = true
	if _, _, err := eachPoolsResolver(p, nil).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, eachPoolsDocument), false); err != nil {
		t.Fatalf("a pool whose Orker supports fan-out = %v", err)
	}
}

func TestPoolResolverPlan_IgnoresADescribeFailure(t *testing.T) {
	var logged strings.Builder
	p := &eachSupportPools{fail: true}
	if _, _, err := eachPoolsResolver(p, slog.New(slog.NewTextHandler(&logged, nil))).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, eachPoolsDocument), false); err != nil {
		t.Fatalf("a describe failure refused the submit: %v", err)
	}
	if !strings.Contains(logged.String(), "whether it supports fan-out is unknown") {
		t.Fatalf("the unmeasured probe was not logged: %s", logged.String())
	}
}

func TestPoolResolverPlan_AllowEmptyPoolDoesNotDowngradeIt(t *testing.T) {
	p := &eachSupportPools{supported: map[string]bool{}}
	_, _, err := eachPoolsResolver(p, nil).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, eachPoolsDocument), true)
	var refused *engine.PoolNoEachSupportError
	if !errors.As(err, &refused) {
		t.Fatalf("Plan with allow_empty_pool = %v, want the refusal to stand", err)
	}
}

func TestPoolResolverPlan_AsksOnlyForPoolsOfFanOutsAndTheirDirectDependents(t *testing.T) {
	p := &eachSupportPools{supported: map[string]bool{}}
	gpu, _ := task.QueueForPool("gpu", "")
	p.supported[gpu] = true
	if _, _, err := eachPoolsResolver(p, nil).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, eachPoolsDocument), false); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if fmt.Sprint(p.asked) != fmt.Sprint([]string{gpu}) {
		t.Fatalf("asked %v, want only gpu (cpu holds the source, io a task two hops down)", p.asked)
	}
	// An Orkflow with no fan-out asks nothing.
	none := &eachSupportPools{}
	resolver := &engine.PoolResolver{Probe: everyPoolLive{}, Orkers: none}
	if _, _, err := resolver.Plan(context.Background(),
		&orkstackv1.Namespace{Name: "data"}, compile(t, twoPoolDocument), false); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(none.asked) != 0 {
		t.Fatalf("an Orkflow with no fan-out asked %v", none.asked)
	}
}
