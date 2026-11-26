package archive

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests drive fakeCluster with setRetentionErr: a cluster whose
// operator refused the engine's credential the right to update the
// namespace. A real cluster refuses only when its authorizer is configured to,
// which no test can ask of one on demand, so the refusal is faked here.

// refusals counts what the controller reported to its observer.
type refusals struct{ covered, short int }

func (r *refusals) ObserveBackstopRefused(_ context.Context, covered bool) {
	if covered {
		r.covered++
	} else {
		r.short++
	}
}

func refusingCluster(retention time.Duration, err error) *fakeCluster {
	c := newFakeCluster()
	c.retention = retention
	c.setRetentionErr = err
	return c
}

var permissionDenied = serviceerror.NewPermissionDenied("not allowed", "")

// A cluster that refuses the update and already keeps runs at least as long
// as the backstop asks: the engine boots, changes nothing, warns and counts.
func TestRetentionController_ReconcileRefusedButCoveredBoots(t *testing.T) {
	for name, err := range map[string]error{
		"permission denied": permissionDenied,
		"unimplemented":     serviceerror.NewUnimplemented("no namespace updates here"),
	} {
		t.Run(name, func(t *testing.T) {
			cluster := refusingCluster(120*time.Hour, err)
			seen := &refusals{}
			ctrl := NewRetentionController(&fakePolicyStore{}, cluster, "ork-main", 24*time.Hour, testLogger()).WithObserver(seen)
			must.OKf(t, ctrl.Reconcile(context.Background(), 72*time.Hour), "Reconcile")
			if cluster.retention != 120*time.Hour {
				t.Fatalf("retention = %s, want the cluster's 120h untouched", cluster.retention)
			}
			if seen.covered != 1 || seen.short != 0 {
				t.Fatalf("observed %+v, want one covered refusal", *seen)
			}
		})
	}
}

// A cluster that refuses the update and keeps runs shorter than the backstop
// asks: the engine refuses, naming the retention to set and how.
func TestRetentionController_ReconcileRefusedAndShortFailsNamingTheFix(t *testing.T) {
	cluster := refusingCluster(48*time.Hour, permissionDenied)
	seen := &refusals{}
	ctrl := NewRetentionController(&fakePolicyStore{}, cluster, "ork-main", 24*time.Hour, testLogger()).WithObserver(seen)
	err := ctrl.Reconcile(context.Background(), 72*time.Hour)
	var short *BackstopRefusedError
	if !errors.As(err, &short) {
		t.Fatalf("Reconcile = %v, want a *BackstopRefusedError", err)
	}
	if short.Current != 48*time.Hour || short.Want != 96*time.Hour {
		t.Fatalf("error carries current %s, want %s; expected 48h and 96h", short.Current, short.Want)
	}
	for _, part := range []string{"ork-main", "96h", "temporal operator namespace update --namespace ork-main --retention 96h"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not name %q", err, part)
		}
	}
	if seen.short != 1 || seen.covered != 0 {
		t.Fatalf("observed %+v, want one short refusal", *seen)
	}
}

// Any other failure to update is not a refusal of rights and fails as before.
func TestRetentionController_ReconcileOtherUpdateFailureStillFails(t *testing.T) {
	cluster := refusingCluster(120*time.Hour, serviceerror.NewUnavailable("frontend down"))
	seen := &refusals{}
	ctrl := NewRetentionController(&fakePolicyStore{}, cluster, "ork-main", 24*time.Hour, testLogger()).WithObserver(seen)
	err := ctrl.Reconcile(context.Background(), 72*time.Hour)
	var short *BackstopRefusedError
	if err == nil || errors.As(err, &short) {
		t.Fatalf("Reconcile = %v, want the plain update failure", err)
	}
	if seen.covered+seen.short != 0 {
		t.Fatalf("observed %+v for a failure that is not a refusal", *seen)
	}
}

// First boot without the right to update: the maximum is derived from what
// the cluster keeps, so the backstop it implies is exactly the cluster's TTL.
func TestRetentionController_EnsureFirstBootWithoutRightsDerivesTheMaximum(t *testing.T) {
	cluster := refusingCluster(96*time.Hour, permissionDenied)
	policies := &fakePolicyStore{}
	ctrl := NewRetentionController(policies, cluster, "ork-main", 24*time.Hour, testLogger()).WithObserver(&refusals{})
	policy, err := ctrl.Ensure(context.Background())
	must.OKf(t, err, "Ensure")
	if got := policy.GetMaxRunRetention().AsDuration(); got != 72*time.Hour {
		t.Fatalf("materialized maximum = %s, want 72h (96h kept by the cluster less the 24h margin)", got)
	}
	if got := policies.policy.GetMaxRunRetention().AsDuration(); got != 72*time.Hour {
		t.Fatalf("stored maximum = %s, want 72h", got)
	}
	if cluster.retention != 96*time.Hour {
		t.Fatalf("retention = %s, want 96h untouched", cluster.retention)
	}
}

// First boot without rights on a cluster that keeps runs no longer than the
// margin: nothing can be derived, and the engine refuses, naming the fix.
func TestRetentionController_EnsureFirstBootWithoutRightsAndTooShortRefuses(t *testing.T) {
	cluster := refusingCluster(24*time.Hour, permissionDenied)
	policies := &fakePolicyStore{}
	ctrl := NewRetentionController(policies, cluster, "ork-main", 24*time.Hour, testLogger())
	_, err := ctrl.Ensure(context.Background())
	var short *BackstopRefusedError
	if !errors.As(err, &short) {
		t.Fatalf("Ensure = %v, want a *BackstopRefusedError", err)
	}
	if policies.policy != nil {
		t.Fatalf("a refused first boot wrote a policy: %v", policies.policy)
	}
}

// The derived maximum must still cover an idempotency key's life; when it
// cannot, the engine refuses rather than let the cluster delete a run a
// retry would look for.
func TestRetentionController_EnsureFirstBootWithoutRightsHonoursTheKeyLife(t *testing.T) {
	cluster := refusingCluster(96*time.Hour, permissionDenied)
	policies := &fakePolicyStore{}
	ctrl := NewRetentionController(policies, cluster, "ork-main", 24*time.Hour, testLogger()).WithKeyLife(80 * time.Hour)
	_, err := ctrl.Ensure(context.Background())
	var short *BackstopRefusedError
	if !errors.As(err, &short) || short.Want != 104*time.Hour {
		t.Fatalf("Ensure = %v, want a refusal asking for 104h (80h key life + 24h)", err)
	}
	if policies.policy != nil {
		t.Fatalf("a refused first boot wrote a policy: %v", policies.policy)
	}
}

// A later boot without rights, on a cluster that keeps runs long enough,
// boots on the stored maximum; one that keeps them too short refuses.
func TestRetentionController_EnsureLaterBootWithoutRights(t *testing.T) {
	stored := func() *fakePolicyStore {
		return &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(72 * time.Hour)}}
	}
	ctrl := NewRetentionController(stored(), refusingCluster(100*time.Hour, permissionDenied), "ork-main", 24*time.Hour, testLogger())
	_, err := ctrl.Ensure(context.Background())
	must.OKf(t, err, "Ensure on a covering cluster")

	ctrl = NewRetentionController(stored(), refusingCluster(90*time.Hour, permissionDenied), "ork-main", 24*time.Hour, testLogger())
	_, err = ctrl.Ensure(context.Background())
	var short *BackstopRefusedError
	if !errors.As(err, &short) {
		t.Fatalf("Ensure on a short cluster = %v, want a *BackstopRefusedError", err)
	}
}

// The life the backstop is kept above is the reset window's where that is the
// longer (CR-1): a cluster that keeps executions longer than it asks, and that
// this engine may not change, boots with its warning and is left as it is; one
// that keeps them shorter refuses, naming what to set.
func TestRetentionController_EnsureWithoutRightsHonoursTheResetWindow(t *testing.T) {
	const life = 48*time.Hour + 5*time.Minute // a 48h window, above the 1h maximum
	stored := &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(time.Hour)}}
	for _, kept := range []time.Duration{life + 24*time.Hour, 30 * 24 * time.Hour} {
		cluster, seen := refusingCluster(kept, permissionDenied), &refusals{}
		_, err := NewRetentionController(stored, cluster, "ork-main", 24*time.Hour, testLogger()).WithKeyLife(life).WithObserver(seen).Ensure(context.Background())
		must.OKf(t, err, "Ensure on a cluster that keeps %s", kept)
		if cluster.retention != kept || seen.short != 0 || (kept > life+24*time.Hour && seen.covered != 1) {
			t.Errorf("kept %s: retention now %s, observed %+v; want it untouched and covered", kept, cluster.retention, *seen)
		}
	}
	_, err := NewRetentionController(stored, refusingCluster(life+24*time.Hour-time.Second, permissionDenied), "ork-main", 24*time.Hour, testLogger()).
		WithKeyLife(life).Ensure(context.Background())
	var short *BackstopRefusedError
	if !errors.As(err, &short) || short.Want != life+24*time.Hour {
		t.Fatalf("Ensure on a cluster that keeps less = %v; want a refusal asking for %s", err, life+24*time.Hour)
	}
}

// Another replica materializes the policy and raises the backstop right
// after this engine's probe: the probe does not undo it, and this engine
// adopts the winner's policy rather than write its own.
func TestRetentionController_EnsureProbeDoesNotUndoAConcurrentWriter(t *testing.T) {
	cluster := newFakeCluster()
	cluster.retention = 72 * time.Hour
	policies := &fakePolicyStore{}
	cluster.afterSet = func(f *fakeCluster) {
		if policies.policy == nil {
			policies.policy = &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(72 * time.Hour)}
			f.retention = 96 * time.Hour
		}
	}
	ctrl := NewRetentionController(policies, cluster, "ork-main", 24*time.Hour, testLogger())
	policy, err := ctrl.Ensure(context.Background())
	must.OKf(t, err, "Ensure")
	if got := policy.GetMaxRunRetention().AsDuration(); got != 72*time.Hour {
		t.Fatalf("Ensure = %s, want the winner's 72h", got)
	}
	if cluster.retention != 96*time.Hour {
		t.Fatalf("retention = %s, want the winner's 96h backstop, not undone", cluster.retention)
	}
}

// Without rights, a second replica that loses the conditional create adopts
// the winner's document and never overwrites it.
func TestRetentionController_EnsureWithoutRightsLosingTheRaceAdoptsTheWinner(t *testing.T) {
	cluster := refusingCluster(96*time.Hour, permissionDenied)
	winner := &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(48 * time.Hour)}
	policies := &fakePolicyStore{policy: winner, putErr: artifacts.ErrPreconditionFailed}
	// The store already holds the winner by the time Put lands; Get before it
	// still answers not found, as a race would.
	first := true
	store := &racingPolicyStore{fakePolicyStore: policies, notFoundOnce: &first}
	ctrl := NewRetentionController(store, cluster, "ork-main", 24*time.Hour, testLogger())
	policy, err := ctrl.Ensure(context.Background())
	must.OKf(t, err, "Ensure")
	if got := policy.GetMaxRunRetention().AsDuration(); got != 48*time.Hour {
		t.Fatalf("Ensure = %s, want the winner's 48h", got)
	}
	if policies.policy != winner {
		t.Fatalf("the winner's document was overwritten: %v", policies.policy)
	}
}

// racingPolicyStore answers the first Get not found, as a store does just
// before another replica's create lands.
type racingPolicyStore struct {
	*fakePolicyStore
	notFoundOnce *bool
}

func (r *racingPolicyStore) Get(ctx context.Context) (*orkstackv1.RetentionPolicy, error) {
	if *r.notFoundOnce {
		*r.notFoundOnce = false
		return nil, artifacts.ErrNotFound
	}
	return r.fakePolicyStore.Get(ctx)
}

// A writer that changes the retention right after the probe, before any
// policy exists, is seen by the re-read: the maximum is derived again from
// what it left, never from the value read before it.
func TestRetentionController_EnsureRederivesWhenTheRetentionMovesAroundTheProbe(t *testing.T) {
	cluster := newFakeCluster()
	cluster.retention = 72 * time.Hour
	moved := false
	cluster.afterSet = func(f *fakeCluster) {
		if !moved {
			moved = true
			f.retention = 100 * time.Hour
		}
	}
	policies := &fakePolicyStore{}
	ctrl := NewRetentionController(policies, cluster, "ork-main", 24*time.Hour, testLogger())
	policy, err := ctrl.Ensure(context.Background())
	must.OKf(t, err, "Ensure")
	if got := policy.GetMaxRunRetention().AsDuration(); got != 100*time.Hour {
		t.Fatalf("materialized maximum = %s, want 100h, derived again after the retention moved", got)
	}
	if cluster.retention != 124*time.Hour {
		t.Fatalf("retention = %s, want 124h (100h + 24h)", cluster.retention)
	}
}

// A first boot without rights counts its refusal once, and a covered
// refusal that repeats every cleanup pass is logged at WARN once while every
// one is still counted.
func TestRetentionController_RefusalsAreCountedEachTimeAndWarnedOnce(t *testing.T) {
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	cluster := refusingCluster(96*time.Hour, permissionDenied)
	seen := &refusals{}
	ctrl := NewRetentionController(&fakePolicyStore{}, cluster, "ork-main", 24*time.Hour, logger).WithObserver(seen)
	_, err := ctrl.Ensure(context.Background())
	must.OKf(t, err, "Ensure")
	if seen.covered != 1 {
		t.Fatalf("a first boot without rights counted %d covered refusals, want 1", seen.covered)
	}
	for range 3 {
		must.OKf(t, ctrl.Reconcile(context.Background(), 48*time.Hour), "Reconcile")
	}
	if seen.covered != 4 {
		t.Fatalf("counted %d covered refusals after three passes, want 4", seen.covered)
	}
	if n := strings.Count(logs.String(), "level=WARN"); n != 1 {
		t.Fatalf("logged %d WARN lines, want 1:\n%s", n, logs.String())
	}
}
