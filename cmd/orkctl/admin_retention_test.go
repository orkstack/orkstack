package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"

	"github.com/orkstack/orkstack/internal/api"
)

// retentionAdmin stands in for engine.TemporalRunReader's retention seam —
// the deployment-wide Temporal cluster setting, with no cluster in this
// process. A successful write moves `current`, so `admin retention get`
// after `admin retention set` proves the change stuck, the same shape the
// proof transcript exercises against a real cluster.
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

// retentionFixture wires Config.Retention and Config.TemporalNamespace
// exactly as `ork` wires them in cmd/ork/main.go: one Temporal namespace name
// alongside the admin seam that answers for it.
func retentionFixture(t *testing.T, admin *retentionAdmin) *fixture {
	t.Helper()
	return newFixtureWith(t, func(cfg *api.Config) {
		cfg.Retention = admin
		cfg.TemporalNamespace = "ork-main"
	})
}

// TestAdminRetentionGet_GoldenHumanAndJSON pins both renderings of the
// deployment-wide read, and that it says in words this is not per-namespace.
func TestAdminRetentionGet_GoldenHumanAndJSON(t *testing.T) {
	f := retentionFixture(t, &retentionAdmin{current: 72 * time.Hour})

	human := f.run("admin", "retention", "get").requireExit(t, exitOK)
	requireGolden(t, "admin-retention-get.golden", human.stdout)
	if !strings.Contains(human.stdout, "ork-main") {
		t.Errorf("human output does not name the Temporal namespace:\n%s", human.stdout)
	}
	if !strings.Contains(human.stdout, "whole deployment") {
		t.Errorf("human output does not say this is deployment-wide, not per-namespace:\n%s", human.stdout)
	}

	machine := f.run("--json", "admin", "retention", "get").requireExit(t, exitOK)
	requireGolden(t, "admin-retention-get.json.golden", machine.stdout)
}

// TestAdminRetentionSet_ChangesTheClusterAndGetReflectsIt is the CLI-level
// round trip acceptance asks for: get, set, get again, and the second get
// shows the new value, proving the write actually took (against the fake
// here; the integration test and proof transcript prove it against a real
// cluster with an INDEPENDENT DescribeNamespace call).
func TestAdminRetentionSet_ChangesTheClusterAndGetReflectsIt(t *testing.T) {
	admin := &retentionAdmin{current: 72 * time.Hour}
	f := retentionFixture(t, admin)

	before := f.run("admin", "retention", "get").requireExit(t, exitOK)
	if !strings.Contains(before.stdout, "72h0m0s") {
		t.Fatalf("initial retention = %q, want it to show 72h0m0s", before.stdout)
	}

	set := f.run("admin", "retention", "set", "168h").requireExit(t, exitOK)
	requireGolden(t, "admin-retention-set.golden", set.stdout)
	if len(admin.requestedValues) != 1 || admin.requestedValues[0] != 168*time.Hour {
		t.Fatalf("SetNamespaceRetention requests = %v, want [168h]", admin.requestedValues)
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
	admin := &retentionAdmin{current: 72 * time.Hour}
	f := retentionFixture(t, admin)

	f.run("admin", "retention", "set", "24h30m").requireExit(t, exitOK)
	if got := admin.requestedValues[len(admin.requestedValues)-1]; got != 24*time.Hour+30*time.Minute {
		t.Errorf("parsed duration = %s, want 24h30m", got)
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
		admin := &retentionAdmin{current: 72 * time.Hour}
		f := retentionFixture(t, admin)

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

// TestAdminRetentionSet_SurfacesARejectedWrite proves a write Temporal itself
// refuses — the server-side minimum-retention rejection — reaches the
// operator's terminal verbatim rather than being swallowed into a silent
// success.
func TestAdminRetentionSet_SurfacesARejectedWrite(t *testing.T) {
	rejection := serviceerror.NewInvalidArgument("retention period is below the cluster's configured minimum")
	admin := &retentionAdmin{current: 72 * time.Hour, writeErr: rejection}
	f := retentionFixture(t, admin)

	result := f.run("admin", "retention", "set", "1m")
	if result.code == exitOK {
		t.Fatalf("admin retention set succeeded despite the engine's rejection:\n%s", result.stdout)
	}
	if !strings.Contains(result.stderr, "configured minimum") {
		t.Errorf("stderr does not carry Temporal's own refusal message:\n%s", result.stderr)
	}
	// The cluster-side state (here, the fake's `current`) never moved.
	after := f.run("admin", "retention", "get").requireExit(t, exitOK)
	if !strings.Contains(after.stdout, "72h0m0s") {
		t.Errorf("retention after a rejected write = %q, want it unchanged at 72h0m0s", after.stdout)
	}
}
