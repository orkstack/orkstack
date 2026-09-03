package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// retentionAdmin stands in for engine.TemporalRunReader's admin seam — the
// underlying Temporal cluster's own backstop TTL, with no cluster in this
// process. A successful write moves `current`, so RetentionController's
// reconcile can be observed without a live cluster.
type retentionAdmin struct {
	current         time.Duration
	writeErr        error
	requestedNames  []string
	requestedValues []time.Duration
}

func (r *retentionAdmin) NamespaceRetention(_ context.Context, namespace string) (time.Duration, error) {
	r.requestedNames = append(r.requestedNames, namespace)
	return r.current, nil
}

func (r *retentionAdmin) SetNamespaceRetention(
	_ context.Context,
	namespace string,
	retention time.Duration,
) (time.Duration, error) {
	r.requestedNames = append(r.requestedNames, namespace)
	r.requestedValues = append(r.requestedValues, retention)
	if r.writeErr != nil {
		return 0, r.writeErr
	}
	r.current = retention
	return retention, nil
}

// retentionPolicyStore is an in-memory artifacts.RetentionPolicyStore.
type retentionPolicyStore struct {
	policy *orkstackv1.RetentionPolicy
}

func (s *retentionPolicyStore) Get(context.Context) (*orkstackv1.RetentionPolicy, error) {
	if s.policy == nil {
		return nil, artifacts.ErrNotFound
	}
	return s.policy, nil
}

func (s *retentionPolicyStore) Put(_ context.Context, p *orkstackv1.RetentionPolicy, _ artifacts.PutConditions) (*orkstackv1.RetentionPolicy, error) {
	s.policy = p
	return p, nil
}

// retentionFixture wires Config.Retention exactly as `ork` wires it in
// cmd/ork/main.go: a RetentionController over the admin seam that answers
// for the cluster's backstop TTL and a policy store pre-set to
// maxRunRetention. admin.current starts at the already-reconciled backstop
// (maxRunRetention + the controller's margin) so a get/set round trip is not
// itself confused for a reconcile.
func retentionFixture(t *testing.T, admin *retentionAdmin, maxRunRetention time.Duration) *fixture {
	t.Helper()
	admin.current = maxRunRetention + 24*time.Hour
	policies := &retentionPolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(maxRunRetention)}}
	ctrl := archive.NewRetentionController(policies, admin, "ork-main", 24*time.Hour, nil)
	return newFixtureWith(t, func(cfg *api.Config) {
		cfg.Retention = ctrl
	})
}

// TestAdminRetentionGet_GoldenHumanAndJSON pins both renderings of the
// deployment's maximum run retention, and that it names namespace-level
// enforcement rather than any Temporal concept.
func TestAdminRetentionGet_GoldenHumanAndJSON(t *testing.T) {
	f := retentionFixture(t, &retentionAdmin{}, 72*time.Hour)

	human := f.run("admin", "retention", "get").requireExit(t, exitOK)
	requireGolden(t, "admin-retention-get.golden", human.stdout)
	if strings.Contains(human.stdout, "Temporal") || strings.Contains(human.stdout, "temporal") {
		t.Errorf("human output names Temporal, which is meant to stay abstracted from the user:\n%s", human.stdout)
	}
	if !strings.Contains(human.stdout, "maximum run retention") {
		t.Errorf("human output does not name the maximum run retention:\n%s", human.stdout)
	}

	machine := f.run("--json", "admin", "retention", "get").requireExit(t, exitOK)
	requireGolden(t, "admin-retention-get.json.golden", machine.stdout)
}

// TestAdminRetentionSet_ChangesTheMaximumAndGetReflectsIt is the CLI-level
// round trip acceptance asks for: get, set, get again, and the second get
// shows the new value, proving the write actually took. It also proves the
// underlying backstop is reconciled to the new maximum plus the enforcement
// margin, never the raw requested value.
func TestAdminRetentionSet_ChangesTheMaximumAndGetReflectsIt(t *testing.T) {
	admin := &retentionAdmin{}
	f := retentionFixture(t, admin, 72*time.Hour)

	before := f.run("admin", "retention", "get").requireExit(t, exitOK)
	if !strings.Contains(before.stdout, "72h0m0s") {
		t.Fatalf("initial retention = %q, want it to show 72h0m0s", before.stdout)
	}

	set := f.run("admin", "retention", "set", "168h").requireExit(t, exitOK)
	requireGolden(t, "admin-retention-set.golden", set.stdout)
	if len(admin.requestedValues) != 1 || admin.requestedValues[0] != 192*time.Hour {
		t.Fatalf("backstop reconcile requests = %v, want [192h] (168h + 24h margin)", admin.requestedValues)
	}
	if admin.requestedNames[len(admin.requestedNames)-1] != "ork-main" {
		t.Fatalf("SetNamespaceRetention namespace = %q, want ork-main", admin.requestedNames[len(admin.requestedNames)-1])
	}

	after := f.run("admin", "retention", "get").requireExit(t, exitOK)
	if !strings.Contains(after.stdout, "168h0m0s") {
		t.Errorf("retention after set = %q, want it to show the new value 168h0m0s", after.stdout)
	}
}

// TestAdminRetentionSet_ParsesGoDurationSyntax proves the flag accepts the
// same duration flavour as ORK_*_TTL/ORK_*_INTERVAL — including a form with
// more than one unit — and rejects garbage locally, before any RPC.
func TestAdminRetentionSet_ParsesGoDurationSyntax(t *testing.T) {
	admin := &retentionAdmin{}
	f := retentionFixture(t, admin, 72*time.Hour)

	f.run("admin", "retention", "set", "24h30m").requireExit(t, exitOK)
	if got := admin.requestedValues[len(admin.requestedValues)-1]; got != 24*time.Hour+30*time.Minute+24*time.Hour {
		t.Errorf("reconciled backstop duration = %s, want 24h30m + the 24h margin", got)
	}

	garbage := f.run("admin", "retention", "set", "not-a-duration")
	if garbage.code == exitOK {
		t.Fatalf("admin retention set not-a-duration succeeded, want a parse failure")
	}
	if !strings.Contains(garbage.stderr, "not-a-duration") {
		t.Errorf("stderr does not name the bad value:\n%s", garbage.stderr)
	}
}

// TestAdminRetentionSet_RefusesANonPositiveDurationLocally never reaches the
// engine: 0h and a negative duration are refused before any RPC leaves the
// process. Both checks matter and are asserted separately: the fake's
// SetNamespaceRetention is never called (the handler didn't act on a
// request), AND — the stronger claim — the fixture's server never received
// any HTTP request at all (no request crossed the wire in the first place).
// A CLI that validated only in the handler after already sending the RPC
// would pass the first assertion and fail the second.
func TestAdminRetentionSet_RefusesANonPositiveDurationLocally(t *testing.T) {
	for _, raw := range []string{"0h", "-1h"} {
		admin := &retentionAdmin{}
		f := retentionFixture(t, admin, 72*time.Hour)

		result := f.run("admin", "retention", "set", raw)
		if result.code == exitOK {
			t.Errorf("admin retention set %s succeeded, want a refusal", raw)
		}
		if len(admin.requestedValues) != 0 {
			t.Errorf("admin retention set %s called the engine, want no call for a non-positive duration", raw)
		}
		if reqs := f.requests(); len(reqs) != 0 {
			t.Errorf("admin retention set %s sent %d HTTP request(s) to the server (%v), want zero: "+
				"a non-positive duration must be refused before any request leaves the process",
				raw, len(reqs), reqs)
		}
	}
}

// TestAdminRetentionSet_SurfacesARejectedWrite proves a write the underlying
// cluster itself refuses — its own server-side minimum-retention rejection —
// reaches the operator's terminal verbatim rather than being swallowed into
// a silent success. The request RAISES the maximum (72h -> 240h), so the
// backstop is reconciled BEFORE the new ceiling is stored: a rejection here
// leaves the stored maximum genuinely unchanged, which is what this test
// proves — a lowering request commits its new ceiling before reconciling and
// is covered separately by the RetentionController tests.
func TestAdminRetentionSet_SurfacesARejectedWrite(t *testing.T) {
	rejection := serviceerror.NewInvalidArgument("retention period is below the cluster's configured minimum")
	admin := &retentionAdmin{writeErr: rejection}
	f := retentionFixture(t, admin, 72*time.Hour)

	result := f.run("admin", "retention", "set", "240h")
	if result.code == exitOK {
		t.Fatalf("admin retention set succeeded despite the engine's rejection:\n%s", result.stdout)
	}
	if !strings.Contains(result.stderr, "configured minimum") {
		t.Errorf("stderr does not carry the underlying refusal message:\n%s", result.stderr)
	}
	// The stored maximum never moved.
	after := f.run("admin", "retention", "get").requireExit(t, exitOK)
	if !strings.Contains(after.stdout, "72h0m0s") {
		t.Errorf("retention after a rejected write = %q, want it unchanged at 72h0m0s", after.stdout)
	}
}
