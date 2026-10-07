package engine_test

import (
	"context"
	"fmt"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// joinSupportPools answers DescribeOrker per channel with join_supported as
// given (each and cache always supported), and records every channel asked.
type joinSupportPools struct {
	eachSupportPools
	joinSupported map[string]bool
}

func (p *joinSupportPools) OrkerOnPool(ctx context.Context, queue string) (*orkstackv1.DescribeOrkerResult, error) {
	result, err := p.eachSupportPools.OrkerOnPool(ctx, queue)
	if err != nil {
		return nil, err
	}
	result.EachSupported, result.CacheSupported = true, true
	result.JoinSupported = p.joinSupported[queue]
	return result, nil
}

func joinPoolsResolver(p *joinSupportPools) *engine.PoolResolver {
	cpu, _ := task.QueueForPool("cpu", "")
	gpu, _ := task.QueueForPool("gpu", "")
	return &engine.PoolResolver{Probe: livePools{cpu: 1, gpu: 1}, Orkers: p}
}

const joinPoolsDocument = `
name: join-pools
tasks:
  - id: deploy
    uses: shell
    pool: cpu
    with: {op: exec}
  - id: verify
    uses: shell
    pool: cpu
    needs: [deploy]
    join: none_failed
    with: {op: exec}
  - id: rollback
    uses: shell
    pool: gpu
    needs: [deploy]
    join: any_failed
    with: {op: exec}
  - id: notify
    uses: shell
    pool: gpu
    needs: [deploy]
    join: all_done
    with: {op: exec}
  - id: approve
    needs: [deploy]
    join: any_failed
    await: {timeout_seconds: 60}
`

func TestPoolResolverPlan_RefusesAPoolWhoseOrkerPredatesJoins(t *testing.T) {
	p := &joinSupportPools{joinSupported: map[string]bool{}}
	_, _, err := joinPoolsResolver(p).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, joinPoolsDocument), false)
	refused, ok := err.(*engine.PoolNoJoinSupportError)
	if !ok {
		t.Fatalf("Plan = %v, want PoolNoJoinSupportError", err)
	}
	if refused.Pool != "gpu" || fmt.Sprint(refused.Tasks) != "[rollback notify]" {
		t.Fatalf("refusal = %+v", refused)
	}
	want := `pool "gpu" (namespace "ml", tasks: rollback, notify) is served by Orker "gpu-3" (build "2026.09.01"), ` +
		`which predates join policies (join:): it cannot read a failure handler's input and would fail it. ` +
		`Upgrade the pool's Orkers before submitting this Orkflow.`
	if err.Error() != want {
		t.Fatalf("message = %q\nwant %q", err.Error(), want)
	}
	gpu, _ := task.QueueForPool("gpu", "")
	p.joinSupported[gpu] = true
	if _, _, err := joinPoolsResolver(p).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, joinPoolsDocument), false); err != nil {
		t.Fatalf("a pool whose Orker reads join policies = %v", err)
	}
}

func TestPoolResolverPlan_JoinIgnoresADescribeFailure(t *testing.T) {
	p := &joinSupportPools{}
	p.fail = true
	if _, _, err := joinPoolsResolver(p).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, joinPoolsDocument), false); err != nil {
		t.Fatalf("a describe failure refused the submit: %v", err)
	}
}

func TestPoolResolverPlan_JoinAllowEmptyPoolDoesNotDowngradeIt(t *testing.T) {
	p := &joinSupportPools{joinSupported: map[string]bool{}}
	_, _, err := joinPoolsResolver(p).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, joinPoolsDocument), true)
	if _, ok := err.(*engine.PoolNoJoinSupportError); !ok {
		t.Fatalf("Plan with allow_empty_pool = %v, want the refusal to stand", err)
	}
}

func TestPoolResolverPlan_AsksOnlyForPoolsOfContainerFailureHandlers(t *testing.T) {
	gpu, _ := task.QueueForPool("gpu", "")
	p := &joinSupportPools{joinSupported: map[string]bool{gpu: true}}
	if _, _, err := joinPoolsResolver(p).Plan(context.Background(),
		&orkstackv1.Namespace{Name: "ml"}, compile(t, joinPoolsDocument), false); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	// cpu hosts only a none_failed task; the awaited handler is never routed.
	if fmt.Sprint(p.asked) != fmt.Sprint([]string{gpu}) {
		t.Fatalf("asked %v, want only gpu", p.asked)
	}
	// A definition with no join, and one whose only handlers are awaited or
	// composed, cause no describe call at all.
	for _, doc := range []string{`
name: no-join
tasks:
  - id: a
    uses: shell
    pool: gpu
    with: {op: exec}
`, `
name: await-handler
tasks:
  - id: a
    uses: shell
    pool: gpu
    with: {op: exec}
  - id: b
    uses: shell
    pool: gpu
    needs: [a]
    join: none_failed
    with: {op: exec}
  - id: approve
    needs: [a]
    join: all_done
    await: {timeout_seconds: 60}
  - id: compensate
    needs: [a]
    join: any_failed
    uses: orkflow:release-block@2.1.0
`} {
		q := &joinSupportPools{}
		if _, _, err := joinPoolsResolver(q).Plan(context.Background(),
			&orkstackv1.Namespace{Name: "ml"}, compile(t, doc), false); err != nil {
			t.Fatalf("Plan: %v", err)
		}
		if len(q.asked) != 0 {
			t.Fatalf("asked %v for a definition with no container failure handler", q.asked)
		}
	}
}
