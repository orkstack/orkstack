package metriccatalog

import (
	"sync"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	"go.opentelemetry.io/otel/metric/noop"
)

// Recorder is a metric.MeterProvider that records every instrument the code
// creates through it, by name, kind and unit, as the code itself spells
// them, and otherwise does nothing. A test hands it to internal/telemetry's
// own constructors to learn the instruments they build through helpers and
// loops, which Instruments cannot read from the source. Its Instruments
// feed PrometheusNames, so a renamed instrument, a changed unit or kind
// changes the exported name the test holds a rule to.
type Recorder struct {
	embedded.MeterProvider
	mu  sync.Mutex
	out map[string]Instrument
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder { return &Recorder{out: map[string]Instrument{}} }

// Meter returns a meter that records into r.
func (r *Recorder) Meter(string, ...metric.MeterOption) metric.Meter { return recordingMeter{r: r} }

// Instruments is every instrument recorded, once per name.
func (r *Recorder) Instruments() []Instrument {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Instrument, 0, len(r.out))
	for _, in := range r.out {
		out = append(out, in)
	}
	return out
}

func (r *Recorder) add(name, method, unit string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.out[name] = Instrument{Name: name, Method: method, Unit: unit}
}

// recordingMeter records each creation and returns noop's instrument.
type recordingMeter struct {
	noop.Meter
	r *Recorder
}

func (m recordingMeter) Int64Counter(n string, o ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	m.r.add(n, "Int64Counter", metric.NewInt64CounterConfig(o...).Unit())
	return m.Meter.Int64Counter(n, o...)
}

func (m recordingMeter) Int64UpDownCounter(n string, o ...metric.Int64UpDownCounterOption) (metric.Int64UpDownCounter, error) {
	m.r.add(n, "Int64UpDownCounter", metric.NewInt64UpDownCounterConfig(o...).Unit())
	return m.Meter.Int64UpDownCounter(n, o...)
}

func (m recordingMeter) Int64Histogram(n string, o ...metric.Int64HistogramOption) (metric.Int64Histogram, error) {
	m.r.add(n, "Int64Histogram", metric.NewInt64HistogramConfig(o...).Unit())
	return m.Meter.Int64Histogram(n, o...)
}

func (m recordingMeter) Int64Gauge(n string, o ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	m.r.add(n, "Int64Gauge", metric.NewInt64GaugeConfig(o...).Unit())
	return m.Meter.Int64Gauge(n, o...)
}

func (m recordingMeter) Int64ObservableCounter(n string, o ...metric.Int64ObservableCounterOption) (metric.Int64ObservableCounter, error) {
	m.r.add(n, "Int64ObservableCounter", metric.NewInt64ObservableCounterConfig(o...).Unit())
	return m.Meter.Int64ObservableCounter(n, o...)
}

func (m recordingMeter) Int64ObservableUpDownCounter(n string, o ...metric.Int64ObservableUpDownCounterOption) (metric.Int64ObservableUpDownCounter, error) {
	m.r.add(n, "Int64ObservableUpDownCounter", metric.NewInt64ObservableUpDownCounterConfig(o...).Unit())
	return m.Meter.Int64ObservableUpDownCounter(n, o...)
}

func (m recordingMeter) Int64ObservableGauge(n string, o ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	m.r.add(n, "Int64ObservableGauge", metric.NewInt64ObservableGaugeConfig(o...).Unit())
	return m.Meter.Int64ObservableGauge(n, o...)
}

func (m recordingMeter) Float64Counter(n string, o ...metric.Float64CounterOption) (metric.Float64Counter, error) {
	m.r.add(n, "Float64Counter", metric.NewFloat64CounterConfig(o...).Unit())
	return m.Meter.Float64Counter(n, o...)
}

func (m recordingMeter) Float64UpDownCounter(n string, o ...metric.Float64UpDownCounterOption) (metric.Float64UpDownCounter, error) {
	m.r.add(n, "Float64UpDownCounter", metric.NewFloat64UpDownCounterConfig(o...).Unit())
	return m.Meter.Float64UpDownCounter(n, o...)
}

func (m recordingMeter) Float64Histogram(n string, o ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	m.r.add(n, "Float64Histogram", metric.NewFloat64HistogramConfig(o...).Unit())
	return m.Meter.Float64Histogram(n, o...)
}

func (m recordingMeter) Float64Gauge(n string, o ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	m.r.add(n, "Float64Gauge", metric.NewFloat64GaugeConfig(o...).Unit())
	return m.Meter.Float64Gauge(n, o...)
}

func (m recordingMeter) Float64ObservableCounter(n string, o ...metric.Float64ObservableCounterOption) (metric.Float64ObservableCounter, error) {
	m.r.add(n, "Float64ObservableCounter", metric.NewFloat64ObservableCounterConfig(o...).Unit())
	return m.Meter.Float64ObservableCounter(n, o...)
}

func (m recordingMeter) Float64ObservableUpDownCounter(n string, o ...metric.Float64ObservableUpDownCounterOption) (metric.Float64ObservableUpDownCounter, error) {
	m.r.add(n, "Float64ObservableUpDownCounter", metric.NewFloat64ObservableUpDownCounterConfig(o...).Unit())
	return m.Meter.Float64ObservableUpDownCounter(n, o...)
}

func (m recordingMeter) Float64ObservableGauge(n string, o ...metric.Float64ObservableGaugeOption) (metric.Float64ObservableGauge, error) {
	m.r.add(n, "Float64ObservableGauge", metric.NewFloat64ObservableGaugeConfig(o...).Unit())
	return m.Meter.Float64ObservableGauge(n, o...)
}
