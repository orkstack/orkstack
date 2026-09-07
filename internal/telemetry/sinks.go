package telemetry

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
	"gopkg.in/yaml.v3"
)

// SinksVar names every OTLP destination an operator has, once, so a
// namespace can later pick one by name instead of every deployment carrying
// its own copy of the standard OTEL_* environment (that per-namespace
// selection is a separate concern from this package). With SinksVar unset,
// or set to a document with no `default:` entry, the OTEL_* environment IS
// the default sink — nothing here changes today's behavior.
const SinksVar = "ORK_TELEMETRY_SINKS"

// DefaultSink is the name Setup and SetupSinks fall back to. A table that
// defines it overrides the OTEL_* environment for the default destination;
// a table that does not still leaves the environment in charge.
const DefaultSink = "default"

// sinkNamePattern is the slug every sink name must match — the same shape
// ORK_ACTIVITY_CATALOG requires of an activity name, chosen for the same
// reason: a sink name becomes a label, a file path segment and, later, a
// namespace-facing selector, none of which forgive arbitrary punctuation.
var sinkNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// HeaderSource is one OTLP header value: either a literal or a reference the
// process resolves at boot. Exactly one of Value or Source may be set.
//
// Source uses the same "scheme:locator" shape SplitSecretSource already
// parses for activity secrets (internal/orker) — env:NAME or file:/path —
// so an operator learns one syntax for "where does a secret come from" across
// the whole system.
type HeaderSource struct {
	Value  string `yaml:"value"`
	Source string `yaml:"source"`
}

// SinkSpec is one named sink's destinations. Endpoint is the base URL used
// for any signal that has no more specific *_endpoint; at least one of the
// two must exist for every signal Config has switched on, checked at
// SetupSinks time (a sink is only as complete as the deployment that uses
// it needs it to be).
type SinkSpec struct {
	Endpoint        string `yaml:"endpoint"`
	LogsEndpoint    string `yaml:"logs_endpoint"`
	TracesEndpoint  string `yaml:"traces_endpoint"`
	MetricsEndpoint string `yaml:"metrics_endpoint"`

	// Headers are attached to every OTLP export this sink makes. A value
	// resolved from `source` never leaves this struct: it lives only in the
	// unexported `resolved` map, built once by Resolve.
	Headers map[string]HeaderSource `yaml:"headers"`

	// Insecure allows plain http. It is otherwise inferred from the
	// endpoint's own scheme, so this only matters for an http endpoint that,
	// for some reason, should be refused — which is not offered — or for
	// documentation clarity in the YAML; in practice leave it unset and let
	// the scheme decide.
	Insecure bool `yaml:"insecure"`

	// Timeout bounds one export request. Zero keeps the exporter's default.
	Timeout time.Duration `yaml:"timeout"`

	// resolved holds Headers with every `source` replaced by its resolved
	// value, keyed by header name. Built by Resolve; nil until then.
	resolved map[string]string
}

// sinksDocument is the shape of ORK_TELEMETRY_SINKS: one mapping from sink
// name to spec, decoded strictly so a misspelled key is refused rather than
// silently dropped (the same reasoning as internal/dsl.CompileWithOptions).
type sinksDocument struct {
	Sinks map[string]SinkSpec `yaml:"sinks"`
}

// SinkTable is every sink an operator declared. The zero value is empty and
// valid: Len() is 0, Has and For(anything) report absence, and Resolve is a
// no-op.
type SinkTable struct {
	sinks map[string]SinkSpec
}

// ParseSinks decodes one ORK_TELEMETRY_SINKS document. Empty or all-blank
// input is not an error — it is the zero-config path — and returns an empty
// table.
func ParseSinks(raw []byte) (SinkTable, error) {
	if strings.TrimSpace(string(raw)) == "" {
		return SinkTable{}, nil
	}
	var doc sinksDocument
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil && !errors.Is(err, io.EOF) {
		return SinkTable{}, fmt.Errorf("%s: parse yaml: %w", SinksVar, err)
	}

	sinks := make(map[string]SinkSpec, len(doc.Sinks))
	for name, spec := range doc.Sinks {
		if !sinkNamePattern.MatchString(name) {
			return SinkTable{}, fmt.Errorf("%s: sink name %q is not a slug (%s)",
				SinksVar, name, sinkNamePattern.String())
		}
		for header, source := range spec.Headers {
			hasValue := source.Value != ""
			hasSource := source.Source != ""
			if hasValue && hasSource {
				return SinkTable{}, fmt.Errorf("%s: sink %q header %q has both value and source; give exactly one",
					SinksVar, name, header)
			}
			if !hasValue && !hasSource {
				return SinkTable{}, fmt.Errorf("%s: sink %q header %q has neither value nor source; give exactly one",
					SinksVar, name, header)
			}
			if hasSource {
				scheme, _ := orker.SplitSecretSource(source.Source)
				if scheme != "env" && scheme != "file" {
					return SinkTable{}, fmt.Errorf(
						"%s: sink %q header %q: source %q has unsupported scheme %q (accepted: env, file)",
						SinksVar, name, header, source.Source, scheme)
				}
			}
		}
		sinks[name] = spec
	}
	return SinkTable{sinks: sinks}, nil
}

// Names lists every sink, sorted, so a log line or an error is deterministic
// regardless of Go's map iteration order.
func (t SinkTable) Names() []string {
	names := make([]string, 0, len(t.sinks))
	for name := range t.sinks {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Has reports whether a sink of that name was declared.
func (t SinkTable) Has(name string) bool {
	_, ok := t.sinks[name]
	return ok
}

// Len is the number of declared sinks.
func (t SinkTable) Len() int { return len(t.sinks) }

// Resolve turns every header's `source` into a value, once, at boot. lookupEnv
// and readFile are os.LookupEnv and os.ReadFile in production and fakes in
// tests; a file's contents are trimmed of exactly one trailing newline, the
// shape `echo secret > file` and most secret managers produce.
//
// An unresolvable source — a missing environment variable, an unreadable
// file — is a boot error naming the sink and header, never a silently empty
// header: a collector that starts rejecting exports because DD-API-KEY came
// through blank is a harder bug to find than a refused start (§11, the same
// rule ORK_ACTIVITY_CATALOG follows).
func (t *SinkTable) Resolve(lookupEnv func(string) (string, bool), readFile func(string) ([]byte, error)) error {
	if t == nil || len(t.sinks) == 0 {
		return nil
	}
	var errs []error
	for _, name := range t.Names() {
		spec := t.sinks[name]
		if len(spec.Headers) == 0 {
			spec.resolved = map[string]string{}
			t.sinks[name] = spec
			continue
		}
		resolved := make(map[string]string, len(spec.Headers))
		for header, source := range spec.Headers {
			if source.Value != "" {
				resolved[header] = source.Value
				continue
			}
			scheme, locator := orker.SplitSecretSource(source.Source)
			switch scheme {
			case "env":
				value, ok := lookupEnv(locator)
				if !ok {
					errs = append(errs, fmt.Errorf(
						"sink %q header %q: environment variable %q is not set", name, header, locator))
					continue
				}
				resolved[header] = value
			case "file":
				content, err := readFile(locator)
				if err != nil {
					errs = append(errs, fmt.Errorf("sink %q header %q: read %s: %w", name, header, locator, err))
					continue
				}
				resolved[header] = strings.TrimSuffix(string(content), "\n")
			default:
				// ParseSinks already refused any other scheme, so this is
				// unreachable outside a table built by hand in a test.
				errs = append(errs, fmt.Errorf("sink %q header %q: source scheme %q is unknown", name, header, scheme))
			}
		}
		spec.resolved = resolved
		t.sinks[name] = spec
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// String lists every sink's name and endpoints for a boot log line. It names
// headers, never their values or their sources — a source names an
// environment variable or a file path, which is closer to a value than a
// label is comfortable with, and Resolve's whole point is that no caller
// ever needs to print one.
func (t SinkTable) String() string {
	names := t.Names()
	if len(names) == 0 {
		return "sinks: none"
	}
	parts := make([]string, 0, len(names))
	for _, name := range names {
		spec := t.sinks[name]
		var b strings.Builder
		fmt.Fprintf(&b, "%s(endpoint=%s", name, spec.Endpoint)
		if spec.LogsEndpoint != "" {
			fmt.Fprintf(&b, ", logs_endpoint=%s", spec.LogsEndpoint)
		}
		if spec.TracesEndpoint != "" {
			fmt.Fprintf(&b, ", traces_endpoint=%s", spec.TracesEndpoint)
		}
		if spec.MetricsEndpoint != "" {
			fmt.Fprintf(&b, ", metrics_endpoint=%s", spec.MetricsEndpoint)
		}
		if len(spec.Headers) > 0 {
			headerNames := make([]string, 0, len(spec.Headers))
			for header := range spec.Headers {
				headerNames = append(headerNames, header)
			}
			sort.Strings(headerNames)
			fmt.Fprintf(&b, ", headers=%s", strings.Join(headerNames, ","))
		}
		b.WriteString(")")
		parts = append(parts, b.String())
	}
	return "sinks: " + strings.Join(parts, "; ")
}

// hasAnyEndpoint reports whether s names a destination for at least one
// signal. A spec with none at all is a useless entry — SetupSinks refuses
// boot on it rather than silently building a sink that can never export
// anything.
func (s SinkSpec) hasAnyEndpoint() bool {
	return s.Endpoint != "" || s.LogsEndpoint != "" || s.TracesEndpoint != "" || s.MetricsEndpoint != ""
}

// endpointFor resolves the endpoint one signal uses: its own *_endpoint if
// set, else Endpoint with the signal's standard OTLP http/protobuf path
// appended. An empty result with a non-nil error means neither was given.
func (s SinkSpec) endpointFor(signal, defaultPath string) (string, error) {
	specific := ""
	switch signal {
	case "traces":
		specific = s.TracesEndpoint
	case "logs":
		specific = s.LogsEndpoint
	case "metrics":
		specific = s.MetricsEndpoint
	}
	if specific != "" {
		return specific, nil
	}
	if s.Endpoint == "" {
		return "", fmt.Errorf("no %s endpoint (set endpoint or %s_endpoint)", signal, signal)
	}
	return strings.TrimRight(s.Endpoint, "/") + defaultPath, nil
}

// otlpHeadersEnvVar is the generic W3C-baggage-style header list every OTLP
// exporter's autoconfiguration reads (OTEL_EXPORTER_OTLP_HEADERS); signalEnvVar
// names the per-signal variant one signal's exporter prefers over it.
const otlpHeadersEnvVar = "OTEL_EXPORTER_OTLP_HEADERS"

// signalEnvVar names the per-signal OTLP headers variable for signal
// ("traces", "logs" or "metrics"): OTEL_EXPORTER_OTLP_TRACES_HEADERS and so on.
func signalEnvVar(signal string) string {
	return "OTEL_EXPORTER_OTLP_" + strings.ToUpper(signal) + "_HEADERS"
}

// parseBaggageHeaders decodes one OTEL_EXPORTER_OTLP_*_HEADERS value: the
// W3C baggage-ish shape the OTel spec defines for it, "key1=value1,key2=value2",
// with every value percent-decoded (a header a secrets manager writes may
// itself contain "," or "=", which %-encoding is how the spec asks an
// operator to carry through this list). A pair with no "=" or an empty key is
// skipped rather than refused — this is read from the process environment at
// export time, long past ConfigFromEnv's chance to refuse a malformed
// variable.
func parseBaggageHeaders(raw string) map[string]string {
	headers := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, value, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		value = strings.TrimSpace(value)
		if decoded, err := url.QueryUnescape(value); err == nil {
			value = decoded
		}
		headers[key] = value
	}
	return headers
}

// mergedHeaders is the header set a spec-bound sink's signal exporter dials
// with: the environment's generic OTEL_EXPORTER_OTLP_HEADERS is the base
// layer, the signal-specific OTEL_EXPORTER_OTLP_<SIGNAL>_HEADERS overrides it
// key by key (the more specific variable wins, same as the OTel SDK's own
// autoconfiguration), and the sink's own `headers:` — already Resolve()d into
// spec.resolved — overrides both: an operator naming a sink explicitly is more
// specific still. lookup is os.LookupEnv in production and a fake in tests.
func mergedHeaders(spec *SinkSpec, signal string, lookup func(string) (string, bool)) map[string]string {
	merged := map[string]string{}
	if raw, ok := lookup(otlpHeadersEnvVar); ok {
		for k, v := range parseBaggageHeaders(raw) {
			merged[k] = v
		}
	}
	if raw, ok := lookup(signalEnvVar(signal)); ok {
		for k, v := range parseBaggageHeaders(raw) {
			merged[k] = v
		}
	}
	if spec != nil {
		for k, v := range spec.resolved {
			merged[k] = v
		}
	}
	return merged
}

// insecureFor reports whether the given endpoint should be dialed over plain
// http: either the spec said so explicitly, or the endpoint's own scheme is
// http (an https or unparseable endpoint is left to the exporter's default,
// which is secure).
func insecureFor(endpoint string, specInsecure bool) bool {
	if specInsecure {
		return true
	}
	u, err := url.Parse(endpoint)
	return err == nil && u.Scheme == "http"
}
