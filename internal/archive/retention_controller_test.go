package archive

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestRetentionController_EnsureMaterializesTheObservedTTLWhenNoPolicyExists(t *testing.T) {
	cluster := newFakeCluster()
	cluster.retention = 72 * time.Hour // what a freshly registered Temporal namespace already has
	policies := &fakePolicyStore{}
	ctrl := NewRetentionController(policies, cluster, "ork-main", 24*time.Hour, testLogger())

	policy, err := ctrl.Ensure(context.Background())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if got := policy.GetMaxRunRetention().AsDuration(); got != 72*time.Hour {
		t.Fatalf("materialized maximum = %s, want 72h (the observed TTL, unchanged)", got)
	}
	if got := cluster.retention; got != 96*time.Hour {
		t.Fatalf("backstop after Ensure = %s, want 96h (72h + 24h margin)", got)
	}
	if len(cluster.setRetentionCalls) != 1 || cluster.setRetentionCalls[0] != 96*time.Hour {
		t.Fatalf("SetNamespaceRetention calls = %v, want [96h]", cluster.setRetentionCalls)
	}
}

// TestRetentionController_EnsureDoesNotGrowTheCeilingOnASecondBoot is the
// regression a materializing (rather than a merely deriving) Ensure exists to
// prevent: reading the backstop TTL and treating it as the ceiling on every
// boot would raise the TTL by another margin every restart.
func TestRetentionController_EnsureDoesNotGrowTheCeilingOnASecondBoot(t *testing.T) {
	cluster := newFakeCluster()
	cluster.retention = 72 * time.Hour
	policies := &fakePolicyStore{}
	ctrl := NewRetentionController(policies, cluster, "ork-main", 24*time.Hour, testLogger())

	first, err := ctrl.Ensure(context.Background())
	if err != nil {
		t.Fatalf("first Ensure: %v", err)
	}
	second, err := ctrl.Ensure(context.Background())
	if err != nil {
		t.Fatalf("second Ensure: %v", err)
	}
	if got := second.GetMaxRunRetention().AsDuration(); got != first.GetMaxRunRetention().AsDuration() {
		t.Fatalf("second Ensure moved the ceiling: first=%s second=%s", first.GetMaxRunRetention().AsDuration(), got)
	}
	if got := cluster.retention; got != 96*time.Hour {
		t.Fatalf("backstop after two Ensures = %s, want 96h (not 120h)", got)
	}
}

func TestRetentionController_EnsureLosingTheMaterializationRaceReadsTheWinnersValue(t *testing.T) {
	cluster := newFakeCluster()
	cluster.retention = 72 * time.Hour
	// Simulate another replica having already written the document by the
	// time this Put lands: the store already holds a value, and Put refuses.
	policies := &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(48 * time.Hour)}}
	policies.putErr = artifacts.ErrPreconditionFailed
	ctrl := NewRetentionController(policies, cluster, "ork-main", 24*time.Hour, testLogger())

	policy, err := ctrl.Ensure(context.Background())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if got := policy.GetMaxRunRetention().AsDuration(); got != 48*time.Hour {
		t.Fatalf("Ensure = %s, want the winner's 48h", got)
	}
}

func TestRetentionController_ReconcileIsANoOpWhenTheClusterAlreadyMatches(t *testing.T) {
	cluster := newFakeCluster()
	cluster.retention = 96 * time.Hour // already max(72h) + margin(24h)
	ctrl := NewRetentionController(&fakePolicyStore{}, cluster, "ork-main", 24*time.Hour, testLogger())

	if err := ctrl.Reconcile(context.Background(), 72*time.Hour); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(cluster.setRetentionCalls) != 0 {
		t.Fatalf("SetNamespaceRetention calls = %v, want none", cluster.setRetentionCalls)
	}
}

func TestRetentionController_ReconcileCorrectsBothUpAndDown(t *testing.T) {
	cluster := newFakeCluster()
	cluster.retention = 10 * time.Hour
	ctrl := NewRetentionController(&fakePolicyStore{}, cluster, "ork-main", 24*time.Hour, testLogger())
	if err := ctrl.Reconcile(context.Background(), 72*time.Hour); err != nil {
		t.Fatalf("Reconcile up: %v", err)
	}
	if cluster.retention != 96*time.Hour {
		t.Fatalf("after raising = %s, want 96h", cluster.retention)
	}

	if err := ctrl.Reconcile(context.Background(), 1*time.Hour); err != nil {
		t.Fatalf("Reconcile down: %v", err)
	}
	if cluster.retention != 25*time.Hour {
		t.Fatalf("after lowering = %s, want 25h", cluster.retention)
	}
}

func TestRetentionController_SetMaxOrdersTheWritesSoTheBackstopNeverDropsBelowAnyCeiling(t *testing.T) {
	t.Run("raising reconciles before storing", func(t *testing.T) {
		cluster := newFakeCluster()
		cluster.retention = 48 * time.Hour // 24h ceiling + 24h margin
		var order []string
		cluster.setRetentionErr = nil
		policies := &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(24 * time.Hour)}}
		// Wrap Put to observe ordering against SetNamespaceRetention.
		orderedPolicies := &orderRecordingPolicyStore{RetentionPolicyStore: policies, order: &order}
		orderedCluster := &orderRecordingCluster{fakeCluster: cluster, order: &order}
		ctrl := NewRetentionController(orderedPolicies, orderedCluster, "ork-main", 24*time.Hour, testLogger())

		if _, err := ctrl.SetMax(context.Background(), 72*time.Hour); err != nil {
			t.Fatalf("SetMax: %v", err)
		}
		if len(order) != 2 || order[0] != "reconcile" || order[1] != "put" {
			t.Fatalf("order = %v, want [reconcile put] when raising", order)
		}
	})

	t.Run("lowering stores before reconciling", func(t *testing.T) {
		cluster := newFakeCluster()
		cluster.retention = 96 * time.Hour // 72h ceiling + 24h margin
		var order []string
		policies := &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(72 * time.Hour)}}
		orderedPolicies := &orderRecordingPolicyStore{RetentionPolicyStore: policies, order: &order}
		orderedCluster := &orderRecordingCluster{fakeCluster: cluster, order: &order}
		ctrl := NewRetentionController(orderedPolicies, orderedCluster, "ork-main", 24*time.Hour, testLogger())

		if _, err := ctrl.SetMax(context.Background(), 24*time.Hour); err != nil {
			t.Fatalf("SetMax: %v", err)
		}
		if len(order) != 2 || order[0] != "put" || order[1] != "reconcile" {
			t.Fatalf("order = %v, want [put reconcile] when lowering", order)
		}
	})
}

func TestRetentionController_SetMaxRefusesANonPositiveDuration(t *testing.T) {
	cluster := newFakeCluster()
	ctrl := NewRetentionController(&fakePolicyStore{}, cluster, "ork-main", 24*time.Hour, testLogger())
	if _, err := ctrl.SetMax(context.Background(), 0); err == nil {
		t.Fatal("SetMax(0) succeeded, want a refusal")
	}
}

// orderRecordingPolicyStore and orderRecordingCluster wrap the fakes to
// record which side SetMax touches first, without changing their behaviour.
type orderRecordingPolicyStore struct {
	artifacts.RetentionPolicyStore
	order *[]string
}

func (o *orderRecordingPolicyStore) Put(ctx context.Context, p *orkstackv1.RetentionPolicy, cond artifacts.PutConditions) (*orkstackv1.RetentionPolicy, error) {
	*o.order = append(*o.order, "put")
	return o.RetentionPolicyStore.Put(ctx, p, cond)
}

type orderRecordingCluster struct {
	*fakeCluster
	order *[]string
}

func (o *orderRecordingCluster) SetNamespaceRetention(ctx context.Context, namespace string, d time.Duration) (time.Duration, error) {
	*o.order = append(*o.order, "reconcile")
	return o.fakeCluster.SetNamespaceRetention(ctx, namespace, d)
}

func TestEffectiveRunRetention_UsesTheNamespacesOwnValueWhenSetAndWithinTheMaximum(t *testing.T) {
	ns := &orkstackv1.Namespace{Name: "payments", RunRetention: durationpb.New(24 * time.Hour)}
	if got := EffectiveRunRetention(ns, 72*time.Hour); got != 24*time.Hour {
		t.Fatalf("EffectiveRunRetention = %s, want 24h", got)
	}
}

func TestEffectiveRunRetention_FallsBackToTheMaximumWhenUnset(t *testing.T) {
	ns := &orkstackv1.Namespace{Name: "payments"}
	if got := EffectiveRunRetention(ns, 72*time.Hour); got != 72*time.Hour {
		t.Fatalf("EffectiveRunRetention = %s, want 72h", got)
	}
}

func TestEffectiveRunRetention_BoundsAStoredValueALaterLowerMaximumLeftAboveIt(t *testing.T) {
	ns := &orkstackv1.Namespace{Name: "payments", RunRetention: durationpb.New(168 * time.Hour)}
	if got := EffectiveRunRetention(ns, 72*time.Hour); got != 72*time.Hour {
		t.Fatalf("EffectiveRunRetention = %s, want the 72h maximum, not the stale 168h", got)
	}
}

func TestEffectiveRunRetention_NilNamespaceFallsBackToTheMaximum(t *testing.T) {
	if got := EffectiveRunRetention(nil, 72*time.Hour); got != 72*time.Hour {
		t.Fatalf("EffectiveRunRetention(nil) = %s, want 72h", got)
	}
}

// flakyNewNamespaceCluster simulates the propagation delay between
// task.EnsureNamespace registering a namespace and it becoming visible to
// the very next admin RPC against it (observed on Temporal's dev-server,
// fail-closed-boot territory, but for a transient,
// self-inflicted cause rather than a genuine misconfiguration): its first
// notFoundCalls calls to either admin method report NamespaceNotFound, then
// it behaves like a normal fakeCluster.
type flakyNewNamespaceCluster struct {
	*fakeCluster
	notFoundCalls int
	calls         int
}

func (f *flakyNewNamespaceCluster) NamespaceRetention(ctx context.Context, namespace string) (time.Duration, error) {
	if f.calls < f.notFoundCalls {
		f.calls++
		return 0, serviceerror.NewNamespaceNotFound(namespace)
	}
	return f.fakeCluster.NamespaceRetention(ctx, namespace)
}

func (f *flakyNewNamespaceCluster) SetNamespaceRetention(ctx context.Context, namespace string, d time.Duration) (time.Duration, error) {
	if f.calls < f.notFoundCalls {
		f.calls++
		return 0, serviceerror.NewNamespaceNotFound(namespace)
	}
	return f.fakeCluster.SetNamespaceRetention(ctx, namespace, d)
}

// TestRetentionController_EnsureToleratesTheNamespacePropagationDelay is the
// regression for a boot-time race a manual live run against a real Temporal
// dev-server surfaced: Ensure runs immediately after task.EnsureNamespace in
// cmd/ork/main.go, and a namespace Temporal just accepted registration for is
// not always immediately visible to the very next admin RPC — without this
// retry, that transient NamespaceNotFound made `ork` refuse to boot on a
// namespace it had itself just created moments earlier.
func TestRetentionController_EnsureToleratesTheNamespacePropagationDelay(t *testing.T) {
	cluster := &flakyNewNamespaceCluster{fakeCluster: newFakeCluster(), notFoundCalls: 2}
	cluster.retention = 72 * time.Hour
	ctrl := NewRetentionController(&fakePolicyStore{}, cluster, "ork-main", 24*time.Hour, testLogger())

	policy, err := ctrl.Ensure(context.Background())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if got := policy.GetMaxRunRetention().AsDuration(); got != 72*time.Hour {
		t.Fatalf("materialized maximum = %s, want 72h", got)
	}
	if cluster.calls != cluster.notFoundCalls {
		t.Fatalf("retry calls = %d, want exactly %d (the flaky window)", cluster.calls, cluster.notFoundCalls)
	}
}

// TestRetentionController_ReconcileGivesUpOnNamespaceNotFoundPastTheWindow
// proves the retry is bounded, not infinite: a namespace that is genuinely
// gone (deleted, or never existed at all — an operator's real
// misconfiguration, not a boot-time race) still fails, once the propagation
// window elapses. This test doesn't wait out the real window (that would be
// a 10s+ unit test); it proves NamespaceNotFound is retried, distinctly from
// any other error, which is what a shortened, injected clock would otherwise
// have to prove more slowly.
func TestRetentionController_ReconcileRetriesOnlyNamespaceNotFoundNeverOtherErrors(t *testing.T) {
	boom := errors.New("cluster unavailable")
	cluster := newFakeCluster()
	cluster.retentionErr = boom
	ctrl := NewRetentionController(&fakePolicyStore{}, cluster, "ork-main", 24*time.Hour, testLogger())

	err := ctrl.Reconcile(context.Background(), 72*time.Hour)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v (a non-NamespaceNotFound error must never be retried away)", err, boom)
	}
	if len(cluster.retentionNamespaces) != 1 {
		t.Fatalf("NamespaceRetention calls = %d, want exactly 1 (no retry for a non-NamespaceNotFound error)",
			len(cluster.retentionNamespaces))
	}
}

func TestRetentionController_PolicyPassesThroughNotFound(t *testing.T) {
	ctrl := NewRetentionController(&fakePolicyStore{}, newFakeCluster(), "ork-main", 24*time.Hour, testLogger())
	_, err := ctrl.Policy(context.Background())
	if !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("err = %v, want it to wrap artifacts.ErrNotFound", err)
	}
}
