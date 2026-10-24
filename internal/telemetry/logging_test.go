package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// recordingExporter collects every log.Record Export receives, so a test can
// inspect what NewLogger's OTLP bridge actually sent — without a real
// collector.
type recordingExporter struct {
	records []sdklog.Record
}

func (e *recordingExporter) Export(_ context.Context, records []sdklog.Record) error {
	for _, r := range records {
		e.records = append(e.records, r.Clone())
	}
	return nil
}
func (e *recordingExporter) Shutdown(context.Context) error   { return nil }
func (e *recordingExporter) ForceFlush(context.Context) error { return nil }

func TestNewLoggerJSONFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(Config{LogFormat: LogFormatJSON, LogLevel: slog.LevelInfo}, nil, &buf)
	logger.Info("hello world")

	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("output is not JSON: %v (%q)", err, buf.String())
	}
	if _, ok := parsed["level"]; !ok {
		t.Errorf("JSON output missing %q: %v", "level", parsed)
	}
	if _, ok := parsed["msg"]; !ok {
		t.Errorf("JSON output missing %q: %v", "msg", parsed)
	}
}

// stripTime removes the leading time=... token slog's text handler writes,
// since two calls a moment apart would otherwise never compare equal.
func stripTime(s string) string {
	return regexp.MustCompile(`time=\S+ `).ReplaceAllString(s, "")
}

func TestNewLoggerTextMatchesPlainSlog(t *testing.T) {
	var gotBuf, wantBuf bytes.Buffer
	logger := NewLogger(Config{}, nil, &gotBuf)
	logger.Info("hello", "key", "value")

	want := slog.New(slog.NewTextHandler(&wantBuf, nil))
	want.Info("hello", "key", "value")

	if stripTime(gotBuf.String()) != stripTime(wantBuf.String()) {
		t.Errorf("NewLogger text output = %q, want shape of %q", gotBuf.String(), wantBuf.String())
	}
}

func TestNewLoggerLevelFloor(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(Config{LogLevel: slog.LevelWarn}, nil, &buf)
	logger.Debug("should not appear")
	logger.Info("should not appear either")
	if buf.Len() != 0 {
		t.Fatalf("expected nothing below warn, got %q", buf.String())
	}
	logger.Warn("this one should appear")
	if !strings.Contains(buf.String(), "this one should appear") {
		t.Errorf("warn line missing: %q", buf.String())
	}
}

func TestNewLoggerBridgesToOTLP(t *testing.T) {
	exporter := &recordingExporter{}
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)))
	defer provider.Shutdown(context.Background())

	providers := &Providers{Logs: provider, cfg: Config{LogsExporter: ExporterOTLP}}
	var buf bytes.Buffer
	logger := NewLogger(Config{LogsExporter: ExporterOTLP}, providers, &buf)
	logger.Info("hello", "run_id", "r1")

	if len(exporter.records) != 1 {
		t.Fatalf("got %d exported records, want 1", len(exporter.records))
	}
	rec := exporter.records[0]
	if rec.Body().AsString() != "hello" {
		t.Errorf("body = %q, want %q", rec.Body().AsString(), "hello")
	}
	found := false
	rec.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == "run_id" && kv.Value.AsString() == "r1" {
			found = true
		}
		return true
	})
	if !found {
		t.Errorf("record missing attribute run_id=r1")
	}
}

func TestTraceAttrsNoSpan(t *testing.T) {
	if attrs := TraceAttrs(context.Background()); attrs != nil {
		t.Errorf("TraceAttrs with no span = %v, want nil", attrs)
	}
}

func TestTraceAttrsWithSpan(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(context.Background())
	ctx, span := tp.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	attrs := TraceAttrs(ctx)
	if len(attrs) != 2 {
		t.Fatalf("TraceAttrs = %v, want 2 attrs", attrs)
	}
}
