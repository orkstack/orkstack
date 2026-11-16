package telemetry

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func resourceValue(t *testing.T, res *resource.Resource, key string) string {
	t.Helper()
	v, ok := res.Set().Value(attribute.Key(key))
	if !ok {
		t.Fatalf("resource has no %s: %v", key, res.Attributes())
	}
	return v.String()
}

// TestResource_HasInstanceIDAndComponent: every process carries one
// service.instance.id, a UUIDv7 minted once per process, and its component.
func TestResource_HasInstanceIDAndComponent(t *testing.T) {
	for _, tc := range []struct {
		service, component string
	}{
		{ServiceEngine, ComponentEngine},
		{ServiceOrker, ComponentOrker},
	} {
		t.Run(tc.service, func(t *testing.T) {
			cfg, err := configFrom(tc.service, func(string) (string, bool) { return "", false })
			must.OK(t, err)
			res, conflicts, err := newResource(context.Background(), cfg, ResourceAttrs("v1", "prod", "", "ork-main"))
			must.OK(t, err)
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %v; want none", conflicts)
			}
			id := resourceValue(t, res, AttrInstanceID)
			parsed, err := uuid.Parse(id)
			must.OKf(t, err, "service.instance.id %q", id)
			if parsed.Version() != 7 {
				t.Fatalf("service.instance.id %q is version %d; want 7", id, parsed.Version())
			}
			if got := resourceValue(t, res, AttrComponent); got != tc.component {
				t.Fatalf("%s = %q; want %q", AttrComponent, got, tc.component)
			}
			if got := resourceValue(t, res, AttrDeployment); got != "prod" {
				t.Fatalf("%s = %q; want prod", AttrDeployment, got)
			}
			again, _, err := newResource(context.Background(), cfg, nil)
			must.OK(t, err)
			if got := resourceValue(t, again, AttrInstanceID); got != id {
				t.Fatalf("a second resource has instance %q; want the process's one %q", got, id)
			}
		})
	}
}

// TestResource_EnvCannotOverrideIdentity: OTEL_RESOURCE_ATTRIBUTES may add
// attributes and override the binary's other ones, but never the process
// identity; each key it tried to override is reported once, at Warn.
func TestResource_EnvCannotOverrideIdentity(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES",
		"service.instance.id=spoofed,ork.component=engine,ork.deployment=other,ork.orker_id=x,service.version=env,team=blue")
	cfg, err := configFrom(ServiceOrker, func(string) (string, bool) { return "", false })
	must.OK(t, err)
	extra := append(ResourceAttrs("v1", "prod", "", "ork-main"), attribute.String(AttrOrkerID, "orker-a"))
	res, conflicts, err := newResource(context.Background(), cfg, extra)
	must.OK(t, err)
	for key, want := range map[string]string{
		AttrInstanceID:    InstanceID(),
		AttrComponent:     ComponentOrker,
		AttrDeployment:    "prod",
		AttrOrkerID:       "orker-a",
		"service.version": "env",
		"team":            "blue",
	} {
		if got := resourceValue(t, res, key); got != want {
			t.Errorf("%s = %q; want %q", key, got, want)
		}
	}
	want := []string{AttrInstanceID, AttrComponent, AttrDeployment, AttrOrkerID}
	if strings.Join(conflicts, ",") != strings.Join(want, ",") {
		t.Fatalf("conflicts = %v; want %v", conflicts, want)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	s := &Sinks{identityConflicts: conflicts}
	s.WithLogger(logger)
	s.WithLogger(logger)
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 {
		t.Fatalf("Warn lines = %d; want 1:\n%s", n, buf.String())
	}
	for _, key := range want {
		if !strings.Contains(buf.String(), key) {
			t.Fatalf("Warn does not name %s:\n%s", key, buf.String())
		}
	}
}

// TestResource_EnvCannotSetAnIdentityKeyTheBinaryLeavesUnset: an engine has
// no Orker id and a test binary no component; the environment cannot give
// either one, and is reported for trying.
func TestResource_EnvCannotSetAnIdentityKeyTheBinaryLeavesUnset(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "ork.orker_id=x,ork.component=orker")
	res, conflicts, err := newResource(context.Background(), Config{ServiceName: "test"}, nil)
	must.OK(t, err)
	for _, key := range []string{AttrOrkerID, AttrComponent} {
		if v, ok := res.Set().Value(attribute.Key(key)); ok {
			t.Errorf("resource has %s=%q from the environment; want none", key, v.String())
		}
	}
	if got := strings.Join(conflicts, ","); got != AttrComponent+","+AttrOrkerID {
		t.Fatalf("conflicts = %q; want %s,%s", got, AttrComponent, AttrOrkerID)
	}
}

// TestBuildInfo_ObservedEveryCollection: ork.build.info is reported at every
// collection, even under delta temporality, where a gauge recorded once at
// boot would vanish after the first export.
func TestBuildInfo_ObservedEveryCollection(t *testing.T) {
	reader := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
		func(sdkmetric.InstrumentKind) metricdata.Temporality { return metricdata.DeltaTemporality }))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	must.OK(t, registerBuildInfo(provider, []attribute.KeyValue{attribute.String("service.version", "v9")}))
	for i := range 3 {
		var rm metricdata.ResourceMetrics
		must.OK(t, reader.Collect(context.Background(), &rm))
		found := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name != "ork.build.info" {
					continue
				}
				g, ok := m.Data.(metricdata.Gauge[int64])
				if !ok || len(g.DataPoints) != 1 || g.DataPoints[0].Value != 1 {
					t.Fatalf("collection %d: ork.build.info = %#v; want one point of 1", i, m.Data)
				}
				if v, _ := g.DataPoints[0].Attributes.Value("service.version"); v.String() != "v9" {
					t.Fatalf("collection %d: attributes %v; want service.version=v9", i, g.DataPoints[0].Attributes)
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("collection %d has no ork.build.info", i)
		}
	}
}
