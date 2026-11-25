package engine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// TestCheckIdentitySupport: a run that holds an identity is refused on the
// first pool one of whose Orkers does not serve task identity, naming the
// Orker and its build; a pool whose every Orker does passes; a pool none of
// whose Orkers answers is not refused.
func TestCheckIdentitySupport(t *testing.T) {
	planned := &orkstackv1.Orkflow{Name: "deploy", Tasks: []*orkstackv1.Task{
		{Id: "build", Pool: "cpu"}, {Id: "approve", Pool: "cpu", Await: &orkstackv1.AwaitSpec{}}, {Id: "push", Pool: "gpu"},
	}}
	ns := &orkstackv1.Namespace{Name: "ml"}
	fleet := func(gpuServes bool) *fakeFleet {
		cpu := currentOrker("cpu-1", "cpu", 0)
		cpu.IdentitySupported = true
		gpu := currentOrker("gpu-3", "gpu", 0)
		gpu.BuildId, gpu.IdentitySupported = "2026.11.18", gpuServes
		if !gpuServes {
			gpu.IdentityUnsupportedReason = "this Orker's storage credential is not confirmed scoped"
		}
		return fleetOf(cpu, gpu)
	}
	if err := cachePoolsResolver(fleet(true)).CheckIdentitySupport(context.Background(), ns, planned, nil); err != nil {
		t.Fatalf("every Orker serves it: %v", err)
	}
	err := cachePoolsResolver(fleet(false)).CheckIdentitySupport(context.Background(), ns, planned, nil)
	var refused *engine.PoolNoIdentitySupportError
	if !errors.As(err, &refused) || refused.Pool != "gpu" || refused.OrkerID != "gpu-3" || refused.BuildID != "2026.11.18" ||
		!strings.Contains(err.Error(), engine.ReasonPoolNoIdentitySupport) || !strings.Contains(err.Error(), "not confirmed scoped") {
		t.Fatalf("err = %v", err)
	}
	if err := cachePoolsResolver(&fakeFleet{silent: []string{"cpu-1", "gpu-3"}}).CheckIdentitySupport(context.Background(), ns, planned, nil); err != nil {
		t.Fatalf("no Orker answering is refused: %v", err)
	}
}

// TestCheckIdentitySupport_EveryOrkerDeclaresTheProvider: a task that logs in
// through a grant is refused on a pool one of whose Orkers does not declare
// the grant's provider, or declares it as another type; a pool no such task
// runs on is not asked.
func TestCheckIdentitySupport_EveryOrkerDeclaresTheProvider(t *testing.T) {
	planned := &orkstackv1.Orkflow{Name: "deploy", Tasks: []*orkstackv1.Task{
		{Id: "build", Pool: "cpu"},
		{Id: "push", Pool: "gpu", Credentials: []*orkstackv1.CredentialRef{{Name: "c", Grant: "deployer"}}},
		{Id: "read", Pool: "gpu", Secrets: []*orkstackv1.SecretRef{{Name: "s", Source: "vault:secret/data/db", Grant: "reader"}}},
	}}
	ns := &orkstackv1.Namespace{Name: "ml"}
	needs := map[string]engine.ProviderNeed{
		"deployer": {Name: "aws", Type: "aws"}, "reader": {Name: "vault", Type: "vault"}, "unused": {Name: "other", Type: "vault"},
	}
	orkers := func(second ...*orkstackv1.ProviderInfo) *fakeFleet {
		cpu, one, two := currentOrker("cpu-1", "cpu", 0), currentOrker("gpu-1", "gpu", 0), currentOrker("gpu-2", "gpu", 0)
		one.Providers = []*orkstackv1.ProviderInfo{{Name: "aws", Type: "aws"}, {Name: "vault", Type: "vault"}}
		two.Providers, two.BuildId = second, "2026.11.25"
		cpu.IdentitySupported, one.IdentitySupported, two.IdentitySupported = true, true, true
		return fleetOf(cpu, one, two)
	}
	check := func(fleet *fakeFleet) error {
		return cachePoolsResolver(fleet).CheckIdentitySupport(context.Background(), ns, planned, needs)
	}
	aws, vault := &orkstackv1.ProviderInfo{Name: "aws", Type: "aws"}, &orkstackv1.ProviderInfo{Name: "vault", Type: "vault"}
	if err := check(orkers(vault, aws)); err != nil {
		t.Fatalf("every Orker declares both: %v", err)
	}
	for name, tc := range map[string]struct {
		second   []*orkstackv1.ProviderInfo
		provider string
		task     string
		says     string
	}{
		"one is missing":  {[]*orkstackv1.ProviderInfo{aws}, "vault", "read", "does not declare"},
		"none":            {nil, "aws", "push", "does not declare"},
		"of another type": {[]*orkstackv1.ProviderInfo{{Name: "aws", Type: "vault"}, vault}, "aws", "push", `as type "vault"`},
	} {
		t.Run(name, func(t *testing.T) {
			err := check(orkers(tc.second...))
			var refused *engine.PoolNoProviderError
			if !errors.As(err, &refused) {
				t.Fatalf("err = %v", err)
			}
			if refused.Pool != "gpu" || refused.OrkerID != "gpu-2" || refused.BuildID != "2026.11.25" || refused.Provider != tc.provider ||
				len(refused.Tasks) != 1 || refused.Tasks[0] != tc.task {
				t.Errorf("refused = %+v", refused)
			}
			if msg := err.Error(); !strings.HasPrefix(msg, engine.ReasonPoolNoProvider+":") || !strings.Contains(msg, tc.says) {
				t.Errorf("message = %q", msg)
			}
		})
	}
}

// pollingPools is a pool probe that also names who polls each channel, as
// the cluster's poller records do.
type pollingPools struct {
	livePools
	polling map[string][]string
	err     error
}

func (p pollingPools) PollingOrkers(_ context.Context, queue string) ([]engine.OrkerPresence, error) {
	if p.err != nil {
		return nil, p.err
	}
	var out []engine.OrkerPresence
	for _, id := range p.polling[queue] {
		out = append(out, engine.OrkerPresence{Identity: id})
	}
	return out, nil
}

// TestCheckIdentitySupport_AnOrkerThatDoesNotAnswer: where another Orker
// of the pool answers, one that polls the pool's channel and does not
// answer on its own is refused, named, "did not answer". One whose pool
// cannot be told (it polls no channel of the run, or the poller records
// cannot be read) is logged and not refused, as a pool none of whose
// Orkers answers is (AGENTS.md §7.2); the Orker refuses at dispatch.
func TestCheckIdentitySupport_AnOrkerThatDoesNotAnswer(t *testing.T) {
	cpuQueue, _ := task.QueueForPool("cpu", "")
	gpuQueue, _ := task.QueueForPool("gpu", "")
	planned := &orkstackv1.Orkflow{Name: "deploy", Tasks: []*orkstackv1.Task{{Id: "build", Pool: "cpu"}}}
	ns := &orkstackv1.Namespace{Name: "ml"}
	serving := currentOrker("cpu-1", "cpu", 0)
	serving.IdentitySupported = true
	fleet := &fakeFleet{orkers: []*orkstackv1.DescribeOrkerResult{serving}, silent: []string{"cpu-2"}}
	live := livePools{cpuQueue: 2, gpuQueue: 1}

	resolver := &engine.PoolResolver{Fleet: fleet, Probe: pollingPools{livePools: live, polling: map[string][]string{cpuQueue: {"cpu-1", "cpu-2"}}}}
	err := resolver.CheckIdentitySupport(context.Background(), ns, planned, nil)
	var refused *engine.PoolNoIdentitySupportError
	if !errors.As(err, &refused) || refused.Pool != "cpu" || refused.OrkerID != "cpu-2" || !refused.Unanswered ||
		!strings.Contains(err.Error(), "did not answer") || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("a silent Orker of the pool: %v", err)
	}

	for name, probe := range map[string]engine.PoolProbe{
		"it polls another pool":         pollingPools{livePools: live, polling: map[string][]string{cpuQueue: {"cpu-1"}, gpuQueue: {"cpu-2"}}},
		"the poller records are unread": pollingPools{livePools: live, err: errors.New("frontend down")},
		"the probe names no pollers":    live,
	} {
		r := &engine.PoolResolver{Fleet: &fakeFleet{orkers: []*orkstackv1.DescribeOrkerResult{serving}, silent: []string{"cpu-2"}}, Probe: probe}
		if err := r.CheckIdentitySupport(context.Background(), ns, planned, nil); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestPoolNoIdentitySupportError_QuotesAndBoundsTheReason: the Orker's own
// reason reaches the message quoted, and cut at a bound (512 bytes before
// quoting; ASCII here, so the quoted reason stays near that).
func TestPoolNoIdentitySupportError_QuotesAndBoundsTheReason(t *testing.T) {
	msg := (&engine.PoolNoIdentitySupportError{Pool: "cpu", OrkerID: "o", Reason: "line\n" + strings.Repeat("x", 4096)}).Error()
	if strings.Contains(msg, "\n") || len(msg) > 2048 || !strings.Contains(msg, `"line\n`) {
		t.Fatalf("message (%d bytes) = %.200q", len(msg), msg)
	}
	// An Orker that sends the words of a non-answer is quoted as any reason.
	spoof := (&engine.PoolNoIdentitySupportError{Pool: "cpu", OrkerID: "o", Reason: "it did not answer on its own channel"}).Error()
	if !strings.Contains(spoof, `it says "it did not answer`) || strings.Contains(spoof, "is unknown") {
		t.Fatalf("a reason that reads as a non-answer: %s", spoof)
	}
}
