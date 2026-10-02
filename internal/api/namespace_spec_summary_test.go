package api

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestNamespaceSpecSummary_RendersHoldAndMaxRunning pins the audit detail of a
// namespace write: hold="{reason}" only when a hold is requested, in
// strconv.Quote form, max_running=N always, both after run_retention= and
// before telemetry.sink=, and nothing else of the summary moved.
func TestNamespaceSpecSummary_RendersHoldAndMaxRunning(t *testing.T) {
	base := &orkstackv1.Namespace{
		Name:         "payments",
		DefaultPool:  "main",
		AllowedPools: []string{"main", "gpu"},
		RunRetention: durationpb.New(168 * time.Hour),
		Telemetry:    &orkstackv1.TelemetryPolicy{Sink: "default"},
		Labels:       map[string]string{"team": "payments"},
	}
	// before is the summary as it rendered before the two fields existed,
	// the reference every case below must still contain around the new keys.
	const before = "default_pool=main allowed_pools=[main gpu] run_retention=168h0m0s telemetry.sink=default labels={team=payments}"
	const head = "default_pool=main allowed_pools=[main gpu] run_retention=168h0m0s"
	const tail = "telemetry.sink=default labels={team=payments}"

	longReason := strings.Repeat("r", 512)
	cases := []struct {
		name   string
		hold   *orkstackv1.NamespaceHold
		cap    int32
		middle string
	}{
		{name: "accepting and unlimited", middle: "max_running=0"},
		{
			name:   "held with a cap",
			hold:   &orkstackv1.NamespaceHold{Reason: "partner CI loop, INC-4412"},
			cap:    20,
			middle: `hold="partner CI loop, INC-4412" max_running=20`,
		},
		{
			name:   "a quote and a backslash cannot end the value early",
			hold:   &orkstackv1.NamespaceHold{Reason: `say "stop" \ now`},
			cap:    1,
			middle: "hold=" + strconv.Quote(`say "stop" \ now`) + " max_running=1",
		},
		{
			name:   "a 512-byte reason is rendered whole",
			hold:   &orkstackv1.NamespaceHold{Reason: longReason},
			middle: "hold=" + strconv.Quote(longReason) + " max_running=0",
		},
		{
			name:   "since is never rendered",
			hold:   &orkstackv1.NamespaceHold{Reason: "x", Since: timestamppb.Now()},
			middle: `hold="x" max_running=0`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ns := proto.Clone(base).(*orkstackv1.Namespace)
			ns.Hold = tc.hold
			ns.MaxRunning = tc.cap
			got := namespaceSpecSummary(ns)
			want := head + " " + tc.middle + " " + tail
			if got != want {
				t.Fatalf("summary =\n  %s\nwant\n  %s", got, want)
			}
			if tc.hold == nil && strings.Contains(got, "hold=") {
				t.Errorf("summary %q names a hold nobody requested", got)
			}
			if stripped := strings.Replace(got, " "+tc.middle, "", 1); stripped != before {
				t.Errorf("rest of the summary = %q, want it byte-identical to %q", stripped, before)
			}
		})
	}
}

// TestNamespaceSpecSummary_RendersAllowedSecretsOnlyWhenSet: allowed_secrets=
// is rendered only for a non-empty list, in stored order, immediately after
// allowed_pools= and before run_retention=; an unrestricted namespace's
// summary is byte-identical to what it rendered before the field existed.
func TestNamespaceSpecSummary_RendersAllowedSecretsOnlyWhenSet(t *testing.T) {
	ns := &orkstackv1.Namespace{
		Name:         "sandbox",
		DefaultPool:  "main",
		AllowedPools: []string{"main"},
		RunRetention: durationpb.New(168 * time.Hour),
		Labels:       map[string]string{"team": "contractors"},
	}
	const before = "default_pool=main allowed_pools=[main] run_retention=168h0m0s max_running=0 telemetry.sink=inherited labels={team=contractors}"
	if got := namespaceSpecSummary(ns); got != before {
		t.Fatalf("unrestricted summary =\n  %s\nwant\n  %s", got, before)
	}
	ns.AllowedSecrets = []string{}
	if got := namespaceSpecSummary(ns); got != before {
		t.Fatalf("empty-list summary =\n  %s\nwant\n  %s", got, before)
	}
	ns.AllowedSecrets = []string{"env:SANDBOX_*", "file:/etc/orkstack/secrets/sandbox/*"}
	want := "default_pool=main allowed_pools=[main] allowed_secrets=[env:SANDBOX_* file:/etc/orkstack/secrets/sandbox/*] run_retention=168h0m0s max_running=0 telemetry.sink=inherited labels={team=contractors}"
	if got := namespaceSpecSummary(ns); got != want {
		t.Fatalf("summary =\n  %s\nwant\n  %s", got, want)
	}
}

// TestNamespaceSpecSummary_RendersBudgetOnlyWhenSet: budget={…} appears after
// max_running= and before telemetry.sink= only when a budget is set, so a
// namespace with none summarizes byte-identically to before the field.
func TestNamespaceSpecSummary_RendersBudgetOnlyWhenSet(t *testing.T) {
	ns := &orkstackv1.Namespace{
		Name:         "analytics",
		DefaultPool:  "main",
		AllowedPools: []string{"main", "gpu"},
		RunRetention: durationpb.New(168 * time.Hour),
		MaxRunning:   20,
		Telemetry:    &orkstackv1.TelemetryPolicy{Sink: "default"},
		Labels:       map[string]string{"team": "analytics"},
	}
	const before = "default_pool=main allowed_pools=[main gpu] run_retention=168h0m0s max_running=20 telemetry.sink=default labels={team=analytics}"
	if got := namespaceSpecSummary(ns); got != before {
		t.Fatalf("no budget: summary =\n  %s\nwant\n  %s", got, before)
	}
	with := proto.Clone(ns).(*orkstackv1.Namespace)
	with.Budget = &orkstackv1.Budget{CpuSeconds: 5000000, Period: durationpb.New(720 * time.Hour), Enforce: true}
	const want = "default_pool=main allowed_pools=[main gpu] run_retention=168h0m0s max_running=20 " +
		"budget={cpu_seconds=5000000 gib_seconds=0 period=720h0m0s enforce=true} telemetry.sink=default labels={team=analytics}"
	if got := namespaceSpecSummary(with); got != want {
		t.Fatalf("budget: summary =\n  %s\nwant\n  %s", got, want)
	}
}

// TestNamespaceSpecSummary_RendersRequireSignedByOnlyWhenSet:
// require_signed_by= is rendered only when set, immediately after
// run_retention=; an unrestricted namespace's summary is byte-identical to
// what it rendered before the field existed.
func TestNamespaceSpecSummary_RendersRequireSignedByOnlyWhenSet(t *testing.T) {
	ns := &orkstackv1.Namespace{
		Name:         "prod-payments",
		DefaultPool:  "main",
		AllowedPools: []string{"main"},
		RunRetention: durationpb.New(168 * time.Hour),
		Labels:       map[string]string{"team": "payments"},
	}
	const before = "default_pool=main allowed_pools=[main] run_retention=168h0m0s max_running=0 telemetry.sink=inherited labels={team=payments}"
	if got := namespaceSpecSummary(ns); got != before {
		t.Fatalf("unrestricted summary =\n  %s\nwant\n  %s", got, before)
	}
	ns.RequireSignedBy = "cosign:acme-release"
	want := "default_pool=main allowed_pools=[main] run_retention=168h0m0s require_signed_by=cosign:acme-release max_running=0 telemetry.sink=inherited labels={team=payments}"
	if got := namespaceSpecSummary(ns); got != want {
		t.Fatalf("summary =\n  %s\nwant\n  %s", got, want)
	}
}
