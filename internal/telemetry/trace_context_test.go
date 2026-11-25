package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	callerTrace   = "0af7651916cd43dd8448eb211c80319c"
	callerSpan    = "b7ad6b7169203331"
	callerSampled = "00-" + callerTrace + "-" + callerSpan + "-01"
)

// TestBoundedPropagator_CapsTracestateDropsBaggage: what a caller sends is
// read only within bounds, and its baggage only from a trusted caller.
func TestBoundedPropagator_CapsTracestateDropsBaggage(t *testing.T) {
	members := func(n int) string {
		all := make([]string, n)
		for i := range all {
			all[i] = "k" + strconv.Itoa(i) + "=v"
		}
		return strings.Join(all, ",")
	}
	for _, tc := range []struct {
		name, parent, state, wantTrace, wantState string
	}{
		{"within bounds", callerSampled, "vendor=1", callerTrace, "vendor=1"},
		{"32 members", callerSampled, members(32), callerTrace, members(32)},
		{"33 members", callerSampled, members(33), callerTrace, ""},
		{"over 512 bytes", callerSampled, "vendor=" + strings.Repeat("x", 250) + ",other=" + strings.Repeat("y", 256), callerTrace, ""},
		{"malformed state", callerSampled, "=,=", callerTrace, ""},
		{"malformed parent", "00-zz", "vendor=1", "", ""},
		{"no parent", "", "vendor=1", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, trust := range []bool{false, true} {
				carrier := propagation.HeaderCarrier(http.Header{})
				carrier.Set("traceparent", tc.parent)
				carrier.Set("tracestate", tc.state)
				carrier.Set("baggage", "user=dana")
				ctx := InboundPropagator(trust).Extract(context.Background(), carrier)
				sc := trace.SpanContextFromContext(ctx)
				gotTrace := ""
				if sc.IsValid() {
					gotTrace = sc.TraceID().String()
				}
				if gotTrace != tc.wantTrace || sc.TraceState().String() != tc.wantState {
					t.Errorf("trust %v: trace %q state %q, want %q %q", trust, gotTrace, sc.TraceState().String(), tc.wantTrace, tc.wantState)
				}
				if has := baggage.FromContext(ctx).Len() > 0; has != trust {
					t.Errorf("trust %v: baggage read = %v", trust, has)
				}
			}
		})
	}
}

// handled serves one request carrying header through the API's inbound
// options and answers the span it made and what RunTrace pins of it.
func handled(t *testing.T, trust bool, sampler sdktrace.Sampler, header map[string]string) (tracetest.SpanStubs, string, string) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sampler))
	defer provider.Shutdown(context.Background())
	p := noopProviders(Config{TrustInboundTrace: trust, TracesExporter: ExporterOTLP})
	p.Tracer = provider
	var parent, state string
	h := otelhttp.NewHandler(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		parent, state = RunTrace(r.Context())
	}), "orkstack.api", InboundHTTPOptions(p)...)
	req := httptest.NewRequest(http.MethodPost, "/orkstack.v1.RunService/SubmitRun", nil)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	h.ServeHTTP(httptest.NewRecorder(), req)
	return exporter.GetSpans(), parent, state
}

// TestRunTrace_PublicEndpointLinksCaller: an untrusted caller neither chooses
// the run's trace nor whether it is sampled; its context is a link.
func TestRunTrace_PublicEndpointLinksCaller(t *testing.T) {
	for _, flags := range []string{"01", "00"} {
		spans, parent, state := handled(t, false, sdktrace.ParentBased(sdktrace.AlwaysSample()),
			map[string]string{"traceparent": "00-" + callerTrace + "-" + callerSpan + "-" + flags, "tracestate": "vendor=1"})
		if len(spans) != 1 || spans[0].Parent.IsValid() || spans[0].SpanContext.TraceID().String() == callerTrace {
			t.Fatalf("flags %s: spans = %+v; want one root of its own trace", flags, spans)
		}
		if len(spans[0].Links) != 1 || spans[0].Links[0].SpanContext.TraceID().String() != callerTrace {
			t.Fatalf("flags %s: links = %+v; want the caller's", flags, spans[0].Links)
		}
		want := "00-" + spans[0].SpanContext.TraceID().String() + "-" + spans[0].SpanContext.SpanID().String() + "-01"
		if parent != want || state != "" {
			t.Fatalf("flags %s: pinned %q %q, want %q and no state", flags, parent, state, want)
		}
	}
}

// TestRunTrace_TrustHonoursParentAndSampling: a trusted caller's trace, its
// state and its sampling decision are the run's.
func TestRunTrace_TrustHonoursParentAndSampling(t *testing.T) {
	spans, parent, state := handled(t, true, sdktrace.ParentBased(sdktrace.AlwaysSample()),
		map[string]string{"traceparent": callerSampled, "tracestate": "vendor=1"})
	if len(spans) != 1 || spans[0].Parent.SpanID().String() != callerSpan || len(spans[0].Links) != 0 {
		t.Fatalf("spans = %+v; want the caller's child", spans)
	}
	if !strings.HasPrefix(parent, "00-"+callerTrace+"-") || !strings.HasSuffix(parent, "-01") || state != "vendor=1" {
		t.Fatalf("pinned %q %q", parent, state)
	}
	spans, parent, _ = handled(t, true, sdktrace.ParentBased(sdktrace.AlwaysSample()),
		map[string]string{"traceparent": "00-" + callerTrace + "-" + callerSpan + "-00"})
	if len(spans) != 0 || !strings.HasPrefix(parent, "00-"+callerTrace+"-") || !strings.HasSuffix(parent, "-00") {
		t.Fatalf("an unsampled caller: %d spans, pinned %q", len(spans), parent)
	}
}

// TestRunTrace_ADeploymentThatDoesNotTracePinsNothing: with metrics only, the
// no-op tracer passes a caller's context through; a run never takes it.
func TestRunTrace_ADeploymentThatDoesNotTracePinsNothing(t *testing.T) {
	for _, trust := range []bool{false, true} {
		p := noopProviders(Config{TrustInboundTrace: trust, MetricsExporters: []string{ExporterOTLP}})
		var parent string
		served := false
		h := otelhttp.NewHandler(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			served = true
			parent, _ = RunTrace(r.Context())
		}), "orkstack.api", InboundHTTPOptions(p)...)
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("traceparent", callerSampled)
		h.ServeHTTP(httptest.NewRecorder(), req)
		if !served || parent != "" {
			t.Errorf("trust %v: served %v, pinned %q; want none", trust, served, parent)
		}
	}
}

func TestRunTrace_NoSpanIsNone(t *testing.T) {
	if parent, state := RunTrace(context.Background()); parent != "" || state != "" {
		t.Fatalf("RunTrace = %q %q, want none", parent, state)
	}
}

func TestTrustInboundTrace_ReadsItsSetting(t *testing.T) {
	for raw, want := range map[string]bool{"": DefaultTrustInboundTrace, "true": true, "false": false} {
		cfg, err := configFrom(ServiceEngine, func(key string) (string, bool) { return raw, key == TrustInboundTraceVar })
		if err != nil || cfg.TrustInboundTrace != want {
			t.Errorf("%q = %v, %v; want %v", raw, cfg.TrustInboundTrace, err, want)
		}
	}
	if _, err := configFrom(ServiceEngine, func(key string) (string, bool) { return "maybe", key == TrustInboundTraceVar }); err == nil {
		t.Error("a malformed value was accepted")
	}
}

// FuzzBoundedTraceContextExtract: no header panics, a state read back is
// within its bounds and parses, and a parent is kept or dropped whole.
func FuzzBoundedTraceContextExtract(f *testing.F) {
	f.Add(callerSampled, "vendor=1")
	f.Add("", "")
	f.Add("00-zz", strings.Repeat("a=b,", 40))
	f.Add(callerSampled, "k="+strings.Repeat("x", 600))
	f.Fuzz(func(t *testing.T, parent, state string) {
		carrier := propagation.MapCarrier{"traceparent": parent, "tracestate": state}
		sc := trace.SpanContextFromContext(InboundPropagator(false).Extract(context.Background(), carrier))
		got := sc.TraceState().String()
		if len(got) > maxTracestateBytes || sc.TraceState().Len() > maxTracestateMembers {
			t.Fatalf("state of %q = %q", state, got)
		}
		if _, err := trace.ParseTraceState(got); err != nil {
			t.Fatalf("state of %q = %q: %v", state, got, err)
		}
		if sc.IsValid() && !strings.Contains(parent, sc.TraceID().String()+"-"+sc.SpanID().String()) {
			t.Fatalf("%q read as %s-%s", parent, sc.TraceID(), sc.SpanID())
		}
		if !sc.IsValid() && sc.TraceState().Len() != 0 {
			t.Fatalf("a state with no parent: %q", got)
		}
	})
}
