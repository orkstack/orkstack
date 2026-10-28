package config

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/telemetry"
)

// writeTemp writes content to a fresh file under t.TempDir() and returns its
// path.
func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// fullFixture is a configuration file exercising every section of Schema,
// for both binaries. It is used as the shared happy-path fixture below.
func fullFixture(t *testing.T) string {
	t.Helper()
	secretKeyFile := writeTemp(t, "s3-secret-key", "s3-secret-from-file\n")
	otlpHeaderFile := writeTemp(t, "otlp-header", "header-secret-value\n")
	sinkHeaderFile := writeTemp(t, "sink-header", "sink-secret-value\n")

	yaml := `
temporal:
  address: temporal.internal:7233
  namespace: ork-test
deployment:
  name: orkstack-test
  build_id: build-123
pool:
  name: main
  queue: main-queue
storage:
  backend: s3
  dir: /data/artifacts
  s3:
    endpoint: http://minio:9000
    region: us-east-1
    bucket: bucket1
    access_key: AKIA-EXAMPLE
    secret_key: {file: ` + secretKeyFile + `}
    session_token: session-tok
    path_style: true
engine:
  http_addr: :9090
  ws_allowed_origins:
    - https://a.example
    - https://b.example
  idempotency_ttl: 12h
  registry_auth_file: /etc/docker/config.json
  stats_cache_ttl: 10s
  limits:
    values_max_bytes: 1000
    artifact_max_count: 10
    artifact_max_bytes: 2000
    log_max_bytes: 3000
    commit_max_bytes: 4000
    transfer_budget_seconds: 500
    floor_throughput_bytes_per_sec: 999
  cleanup:
    enabled: true
    interval: 2h
    retention_buffer: 48h
    batch_size: 50
activities:
  shell: ghcr.io/orkstack/activity-shell:0.2
  custom: ghcr.io/acme/custom:1.0
orker:
  id: orker-1
  workdir: /var/ork/work
  image_layout: /var/ork/layout
  env_allowlist:
    - FOO
    - BAR
  secret_schemes:
    - env
    - file
  task_log_export: true
  log:
    flush_bytes: 65536
    flush_interval: 250ms
    max_bytes: 999999
  resources:
    default_cpu: "1"
    default_memory: 1Gi
    max_cpu: "4"
    max_memory: 8Gi
    pids_limit: 256
    tmpfs_bytes: 33554432
    force_user: false
  docker:
    bin: /usr/bin/docker
    pull_timeout: 5m
    bind_probe_timeout: 3m
telemetry:
  log_format: json
  log_level: debug
  admin_addr:
    ork: "127.0.0.1:9464"
    orker: "127.0.0.1:9465"
  pprof_enabled: true
  high_cardinality: false
  service_name: ork-svc
  resource_attributes:
    deployment.environment: prod
    team: platform
  exporters:
    traces: otlp
    metrics:
      - otlp
      - prometheus
    logs: otlp
  sampler:
    name: traceidratio
    arg: "0.1"
  otlp:
    endpoint: https://collector:4318
    protocol: http/protobuf
    headers:
      x-api-key: {file: ` + otlpHeaderFile + `}
      x-team: platform
    timeout: 5000
    certificate: /etc/ssl/otel.pem
    traces:
      endpoint: https://collector:4318/traces
      headers:
        x-trace: "yes"
    metrics:
      endpoint: https://collector:4318/metrics
      headers: {}
    logs:
      endpoint: https://collector:4318/logs
      headers: {}
  batch:
    metric_export_interval: 15000
  sinks:
    default:
      endpoint: https://sink1:4318
    grafana:
      endpoint: https://grafana:4318
      headers:
        authorization:
          source: "file:` + sinkHeaderFile + `"
`
	return writeTemp(t, "orkstack.yaml", yaml)
}

func TestLoad_HappyPath_Ork(t *testing.T) {
	path := fullFixture(t)
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := map[string]string{
		"ORK_TEMPORAL_ADDRESS":       "temporal.internal:7233",
		"ORK_TEMPORAL_NAMESPACE":     "ork-test",
		"ORK_DEPLOYMENT_NAME":        "orkstack-test",
		"ORK_HTTP_ADDR":              ":9090",
		"ORK_WS_ALLOWED_ORIGINS":     "https://a.example,https://b.example",
		"ORK_IDEMPOTENCY_TTL":        "12h",
		"ORK_VALUES_MAX_BYTES":       "1000",
		"ORK_CLEANUP_ENABLED":        "true",
		"ORK_S3_SECRET_KEY":          "s3-secret-from-file",
		"ORK_S3_PATH_STYLE":          "true",
		"ORK_ADMIN_ADDR":             "127.0.0.1:9464",
		"OTEL_METRICS_EXPORTER":      "otlp,prometheus",
		"OTEL_EXPORTER_OTLP_TIMEOUT": "5000",
	}
	for env, expected := range want {
		rv, ok := doc.values[env]
		if !ok {
			t.Errorf("%s: not set by Load", env)
			continue
		}
		if rv.value != expected {
			t.Errorf("%s = %q, want %q", env, rv.value, expected)
		}
	}

	// The Orker-only section must not have been read for the ork binary:
	// its keys are simply absent from the document.
	for _, env := range []string{"ORK_ORKER_ID", "ORK_ORKER_WORKDIR", "ORK_DOCKER_BIN"} {
		if _, ok := doc.values[env]; ok {
			t.Errorf("%s: set by Load(binary=ork), but that key belongs only to orker", env)
		}
	}

	// The OTLP header secret was resolved from its file.
	if rv := doc.values["OTEL_EXPORTER_OTLP_HEADERS"]; !strings.Contains(rv.value, "header-secret-value") {
		t.Errorf("OTEL_EXPORTER_OTLP_HEADERS = %q, want it to contain the file's contents", rv.value)
	}
	if rv := doc.values["OTEL_EXPORTER_OTLP_HEADERS"]; !strings.Contains(rv.value, "x-team=platform") {
		t.Errorf("OTEL_EXPORTER_OTLP_HEADERS = %q, want x-team=platform", rv.value)
	}

	// activities re-serializes to a document dsl.ParseCatalog accepts.
	catalog, err := dsl.ParseCatalog([]byte(doc.values["ORK_ACTIVITY_CATALOG"].value))
	if err != nil {
		t.Fatalf("dsl.ParseCatalog(re-serialized activities): %v", err)
	}
	if catalog["shell"] != "ghcr.io/orkstack/activity-shell:0.2" {
		t.Errorf("catalog[shell] = %q", catalog["shell"])
	}
	if catalog["custom"] != "ghcr.io/acme/custom:1.0" {
		t.Errorf("catalog[custom] = %q", catalog["custom"])
	}

	// telemetry.sinks re-serializes to a document telemetry.ParseSinks
	// accepts, including its {source: file:...} header.
	sinks, err := telemetry.ParseSinks([]byte(doc.values["ORK_TELEMETRY_SINKS"].value))
	if err != nil {
		t.Fatalf("telemetry.ParseSinks(re-serialized sinks): %v", err)
	}
	if !sinks.Has("default") || !sinks.Has("grafana") {
		t.Errorf("sinks: want default and grafana, got names %v", sinks.Names())
	}
}

func TestLoad_HappyPath_Orker(t *testing.T) {
	path := fullFixture(t)
	doc, err := Load(path, BinaryOrker)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := map[string]string{
		"ORK_ORKER_ID":                  "orker-1",
		"ORK_ORKER_WORKDIR":             "/var/ork/work",
		"ORK_ORKER_ENV_ALLOWLIST":       "FOO,BAR",
		"ORK_ORKER_LOG_FLUSH_BYTES":     "65536",
		"ORK_ORKER_DEFAULT_CPU":         "1",
		"ORK_DOCKER_BIN":                "/usr/bin/docker",
		"ORK_DOCKER_PULL_TIMEOUT":       "5m",
		"ORK_DOCKER_BIND_PROBE_TIMEOUT": "3m",
		"ORK_ADMIN_ADDR":                "127.0.0.1:9465", // the orker side of the per-binary leaf
	}
	for env, expected := range want {
		rv, ok := doc.values[env]
		if !ok {
			t.Errorf("%s: not set by Load", env)
			continue
		}
		if rv.value != expected {
			t.Errorf("%s = %q, want %q", env, rv.value, expected)
		}
	}

	// The engine (ork-only) section must not have been read for orker.
	for _, env := range []string{"ORK_HTTP_ADDR", "ORK_VALUES_MAX_BYTES", "ORK_ACTIVITY_CATALOG"} {
		if _, ok := doc.values[env]; ok {
			t.Errorf("%s: set by Load(binary=orker), but that key belongs only to ork", env)
		}
	}
}

// An unknown key is refused by its full dotted path, nested or top-level.
func TestLoad_UnknownKey_NamesItsPath(t *testing.T) {
	for name, tc := range map[string]struct{ doc, path string }{
		"nested":    {doc: "storage:\n  bogus_key: 1\n", path: "storage.bogus_key"},
		"top-level": {doc: "not_a_real_section:\n  x: 1\n", path: "not_a_real_section"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeTemp(t, "bad.yaml", tc.doc), BinaryOrk)
			if err == nil {
				t.Fatal("Load: want an error for an unknown key")
			}
			if !strings.Contains(err.Error(), tc.path) {
				t.Errorf("Load error = %q, want it to name %q", err, tc.path)
			}
		})
	}
}

func TestLoad_WrongType_NamesKey(t *testing.T) {
	cases := []struct {
		name, yaml, wantKey string
	}{
		{"bool for duration", "engine:\n  idempotency_ttl: true\n", "engine.idempotency_ttl"},
		{"string for int", "engine:\n  limits:\n    values_max_bytes: not-a-number\n", "engine.limits.values_max_bytes"},
		{"string for bool", "storage:\n  s3:\n    path_style: maybe\n", "storage.s3.path_style"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemp(t, "bad.yaml", tc.yaml)
			_, err := Load(path, BinaryOrk)
			if err == nil {
				t.Fatal("Load: want a type error")
			}
			if !strings.Contains(err.Error(), tc.wantKey) {
				t.Errorf("Load error = %q, want it to name %q", err.Error(), tc.wantKey)
			}
		})
	}
}

// A typed value reaches a daemon's own reader, and not every reader trims: a
// padded duration would silently become the default there, and a padded
// integer would stop the daemon. Load trims an integer, boolean or duration
// and applies the bare value, so every reader takes what the file meant; one
// that is not of its kind once trimmed is refused, naming the key.
func TestLoad_ATypedValueIsAppliedTrimmed(t *testing.T) {
	cases := []struct {
		name, yaml, env, want string
	}{
		{"padded duration", "engine:\n  idempotency_ttl: ' 5m'\n", "ORK_IDEMPOTENCY_TTL", "5m"},
		{"duration with a trailing space", "engine:\n  idempotency_ttl: '5m '\n", "ORK_IDEMPOTENCY_TTL", "5m"},
		{"duration from a block scalar", "engine:\n  idempotency_ttl: |\n    5m\n", "ORK_IDEMPOTENCY_TTL", "5m"},
		{"padded integer", "engine:\n  each_max_items: ' 7 '\n", "ORK_EACH_MAX_ITEMS", "7"},
		{"padded boolean", "storage:\n  s3:\n    path_style: 'true '\n", "ORK_S3_PATH_STYLE", "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemp(t, "padded.yaml", tc.yaml)
			doc, err := Load(path, BinaryOrk)
			if err != nil {
				t.Fatalf("Load refused a padded value: %v", err)
			}
			if got := doc.values[tc.env].value; got != tc.want {
				t.Errorf("%s = %q, want the bare %q", tc.env, got, tc.want)
			}
		})
	}
	for _, tc := range []struct{ name, yaml, wantKey string }{
		{"duration", "engine:\n  idempotency_ttl: ' 5 minutes'\n", "engine.idempotency_ttl"},
		{"integer", "engine:\n  each_max_items: ' 7.0 '\n", "engine.each_max_items"},
		{"boolean", "storage:\n  s3:\n    path_style: ' yes '\n", "storage.s3.path_style"},
	} {
		t.Run("not a "+tc.name, func(t *testing.T) {
			path := writeTemp(t, "wrong.yaml", tc.yaml)
			doc, err := Load(path, BinaryOrk)
			if err == nil {
				t.Fatalf("Load accepted it and set %v", doc.values)
			}
			if !strings.Contains(err.Error(), tc.wantKey) {
				t.Errorf("Load error = %q, want it to name %q", err.Error(), tc.wantKey)
			}
		})
	}
}

// An environment variable cannot hold a NUL byte, and a double-quoted YAML
// scalar can spell one ("\0"). The file is refused at load, naming the key,
// rather than failing partway through applying it at boot.
func TestLoad_RefusesAValueTheEnvironmentCannotHold(t *testing.T) {
	cases := []struct {
		name, yaml, wantKey string
	}{
		{"a string", "temporal:\n  address: \"a\\0b\"\n", "temporal.address"},
		{"a list item", "engine:\n  ws_allowed_origins: [\"a\", \"b\\0\"]\n", "engine.ws_allowed_origins"},
		{"a map value", "telemetry:\n  resource_attributes:\n    k: \"v\\0\"\n", "telemetry.resource_attributes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemp(t, "nul.yaml", tc.yaml)
			doc, err := Load(path, BinaryOrk)
			if err == nil {
				t.Fatalf("Load accepted a NUL byte and set %q", doc.values)
			}
			if !strings.Contains(err.Error(), tc.wantKey) || !strings.Contains(err.Error(), "NUL") {
				t.Errorf("Load error = %q, want it to name %q and NUL", err.Error(), tc.wantKey)
			}
		})
	}
}

func TestLoad_PerBinaryLeaf(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", "telemetry:\n  admin_addr:\n    ork: \":9464\"\n    orker: \":9465\"\n")

	orkDoc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load(ork): %v", err)
	}
	if got := orkDoc.values["ORK_ADMIN_ADDR"].value; got != ":9464" {
		t.Errorf("ork ORK_ADMIN_ADDR = %q, want :9464", got)
	}

	orkerDoc, err := Load(path, BinaryOrker)
	if err != nil {
		t.Fatalf("Load(orker): %v", err)
	}
	if got := orkerDoc.values["ORK_ADMIN_ADDR"].value; got != ":9465" {
		t.Errorf("orker ORK_ADMIN_ADDR = %q, want :9465", got)
	}
}

func TestLoad_ScalarLeafAppliesToBothBinaries(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", "telemetry:\n  log_level: debug\n")
	for _, b := range []Binary{BinaryOrk, BinaryOrker} {
		doc, err := Load(path, b)
		if err != nil {
			t.Fatalf("Load(%s): %v", b, err)
		}
		if got := doc.values["ORK_LOG_LEVEL"].value; got != "debug" {
			t.Errorf("Load(%s) ORK_LOG_LEVEL = %q, want debug", b, got)
		}
	}
}

func TestLoad_SecretFile_MissingFile(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", "storage:\n  s3:\n    secret_key: {file: /no/such/file/exists}\n")
	_, err := Load(path, BinaryOrk)
	if err == nil {
		t.Fatal("Load: want an error for a missing secret file")
	}
	if !strings.Contains(err.Error(), "storage.s3.secret_key") {
		t.Errorf("Load error = %q, want it to name storage.s3.secret_key", err.Error())
	}
	if !strings.Contains(err.Error(), "/no/such/file/exists") {
		t.Errorf("Load error = %q, want it to name the missing path", err.Error())
	}
}

// A secret file loses exactly one trailing line ending, LF or CRLF, and
// nothing more: a second newline is part of the secret.
func TestLoad_SecretFile_TrimsOneTrailingLineEnding(t *testing.T) {
	for name, tc := range map[string]struct{ file, want string }{
		"one of two newlines": {file: "topsecret\n\n", want: "topsecret\n"},
		"a CRLF":              {file: "topsecret\r\n", want: "topsecret"},
	} {
		t.Run(name, func(t *testing.T) {
			secretFile := writeTemp(t, "secret", tc.file)
			path := writeTemp(t, "cfg.yaml", "storage:\n  s3:\n    secret_key: {file: "+secretFile+"}\n")
			doc, err := Load(path, BinaryOrk)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got := doc.values["ORK_S3_SECRET_KEY"].value; got != tc.want {
				t.Errorf("secret_key from %q = %q, want %q", tc.file, got, tc.want)
			}
		})
	}
}

func TestLoad_List_CommaJoined(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", "orker:\n  secret_schemes:\n    - env\n    - file\n    - vault\n")
	doc, err := Load(path, BinaryOrker)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := doc.values["ORK_ORKER_SECRET_SCHEMES"].value; got != "env,file,vault" {
		t.Errorf("secret_schemes = %q, want env,file,vault", got)
	}
}

func TestLoad_Map_PercentEncoding(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", `telemetry:
  resource_attributes:
    plain: value
    withcomma: "a,b"
    withequals: "a=b"
    withpercent: "50%"
`)
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := doc.values["OTEL_RESOURCE_ATTRIBUTES"].value
	for _, want := range []string{"plain=value", "withcomma=a%2Cb", "withequals=a%3Db", "withpercent=50%25"} {
		if !strings.Contains(got, want) {
			t.Errorf("OTEL_RESOURCE_ATTRIBUTES = %q, want it to contain %q", got, want)
		}
	}
}

func TestLoad_EmptyFile(t *testing.T) {
	path := writeTemp(t, "empty.yaml", "")
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load(empty file): %v", err)
	}
	if len(doc.values) != 0 {
		t.Errorf("Load(empty file): values = %v, want none", doc.values)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"), BinaryOrk)
	if err == nil {
		t.Fatal("Load: want an error for a missing file")
	}
}

// TestPercentEncode_RoundTripsWithQueryUnescape proves percentEncode stays
// compatible with internal/telemetry.parseBaggageHeaders, which decodes
// with url.QueryUnescape — form-decoding semantics that turn an unescaped
// '+' into a literal space, so '+' must be escaped exactly like '%', ',',
// and '=' or a header value containing one would come back corrupted.
func TestPercentEncode_RoundTripsWithQueryUnescape(t *testing.T) {
	cases := []string{
		"plain",
		"a+b",
		"50%",
		"a,b",
		"a=b",
		"a+b%c,d=e f",
		"+",
		"has space",
	}
	for _, want := range cases {
		encoded := percentEncode(want)
		got, err := url.QueryUnescape(encoded)
		if err != nil {
			t.Fatalf("percentEncode(%q) = %q, QueryUnescape: %v", want, encoded, err)
		}
		if got != want {
			t.Errorf("QueryUnescape(percentEncode(%q)) = %q, want %q (encoded: %q)", want, got, want, encoded)
		}
	}
}

func TestLoad_Map_PlusEncodedAsPercent2B(t *testing.T) {
	path := writeTemp(t, "cfg.yaml", "telemetry:\n  resource_attributes:\n    withplus: \"a+b\"\n")
	doc, err := Load(path, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := doc.values["OTEL_RESOURCE_ATTRIBUTES"].value
	if !strings.Contains(got, "withplus=a%2Bb") {
		t.Errorf("OTEL_RESOURCE_ATTRIBUTES = %q, want it to contain withplus=a%%2Bb", got)
	}
}

// TestLoad_SecretFile_RelativeToConfigDir proves a relative {file: path}
// resolves against the directory holding the config file itself, never the
// process's current working directory — otherwise the same config file
// would read a different secret (or none) depending on where a binary
// happens to be launched from.
func TestLoad_SecretFile_RelativeToConfigDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "s3-secret"), []byte("relative-secret\n"), 0o600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	configPath := filepath.Join(dir, "orkstack.yaml")
	if err := os.WriteFile(configPath, []byte("storage:\n  s3:\n    secret_key: {file: s3-secret}\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// Run from an unrelated working directory so a bug that resolved the
	// path against the process cwd instead of the config file's directory
	// would fail loudly rather than by accident matching.
	elsewhere := t.TempDir()
	origWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(elsewhere); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	defer os.Chdir(origWD)

	doc, err := Load(configPath, BinaryOrk)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := doc.values["ORK_S3_SECRET_KEY"].value; got != "relative-secret" {
		t.Errorf("ORK_S3_SECRET_KEY = %q, want relative-secret", got)
	}
}

// TestLoad_SecretFile_MissingFile_NamesResolvedPath proves the error names
// the resolved (joined) path, not the relative string the file wrote.
func TestLoad_SecretFile_MissingFile_NamesResolvedPath(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "orkstack.yaml")
	if err := os.WriteFile(configPath, []byte("storage:\n  s3:\n    secret_key: {file: no-such-secret}\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	_, err := Load(configPath, BinaryOrk)
	if err == nil {
		t.Fatal("Load: want an error for a missing relative secret file")
	}
	want := filepath.Join(dir, "no-such-secret")
	if !strings.Contains(err.Error(), want) {
		t.Errorf("Load error = %q, want it to name the resolved path %q", err.Error(), want)
	}
}

// TestLoad_NullScalar_MeansAbsent proves a YAML null (any of its plain
// spellings) leaves a leaf unset — as if the key were never written — so
// Load falls through to the environment or default rather than recording
// the literal text "null".
func TestLoad_NullScalar_MeansAbsent(t *testing.T) {
	for _, spelling := range []string{"null", "~", "Null", "NULL"} {
		t.Run(spelling, func(t *testing.T) {
			path := writeTemp(t, "cfg.yaml", "storage:\n  s3:\n    session_token: "+spelling+"\n")
			doc, err := Load(path, BinaryOrk)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if rv, ok := doc.values["ORK_S3_SESSION_TOKEN"]; ok {
				t.Errorf("ORK_S3_SESSION_TOKEN = %+v, want absent (fall through to env/default)", rv)
			}
		})
	}
}

// TestLoad_ExplicitEmpty_RefusedAgainstNonEmptyDefault proves Load refuses
// an explicit empty value (a blank scalar, an empty list, or an empty map)
// for a leaf whose documented default is a real, non-empty value — because
// every *FromEnv reader in this codebase treats an empty environment
// variable as unset, so such a file could never actually take effect; it
// would silently produce the default instead.
func TestLoad_ExplicitEmpty_RefusedAgainstNonEmptyDefault(t *testing.T) {
	cases := []struct {
		name, yaml, wantEnv, wantDefault string
	}{
		{"empty scalar", "telemetry:\n  log_level: \"\"\n", "ORK_LOG_LEVEL", "info"},
		{"empty list", "orker:\n  secret_schemes: []\n", "ORK_ORKER_SECRET_SCHEMES", "env,file"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			binary := BinaryOrk
			if tc.wantEnv != "ORK_LOG_LEVEL" {
				binary = BinaryOrker
			}
			path := writeTemp(t, "cfg.yaml", tc.yaml)
			_, err := Load(path, binary)
			if err == nil {
				t.Fatal("Load: want an error for an explicit empty value against a non-empty default")
			}
			if !strings.Contains(err.Error(), tc.wantEnv) {
				t.Errorf("Load error = %q, want it to name %s", err.Error(), tc.wantEnv)
			}
			if !strings.Contains(err.Error(), tc.wantDefault) {
				t.Errorf("Load error = %q, want it to name the default %q", err.Error(), tc.wantDefault)
			}
		})
	}
}

// TestLoad_ExplicitEmpty_AcceptedAgainstPlaceholderDefault proves the same
// explicit-empty shapes are accepted when Var.Default is itself a
// placeholder note ("(empty)", ...) rather than a real fixed value — there,
// an empty file value and "unset" are indistinguishable regardless, so
// there is nothing for Load to protect against.
func TestLoad_ExplicitEmpty_AcceptedAgainstPlaceholderDefault(t *testing.T) {
	cases := []struct {
		name, yaml, wantEnv string
	}{
		{"empty scalar", "deployment:\n  build_id: \"\"\n", "ORK_BUILD_ID"},
		{"empty list", "orker:\n  env_allowlist: []\n", "ORK_ORKER_ENV_ALLOWLIST"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemp(t, "cfg.yaml", tc.yaml)
			doc, err := Load(path, BinaryOrker)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			rv, ok := doc.values[tc.wantEnv]
			if !ok {
				t.Fatalf("%s: not set by Load", tc.wantEnv)
			}
			if rv.value != "" {
				t.Errorf("%s = %q, want empty", tc.wantEnv, rv.value)
			}
		})
	}
}

// TestLoad_DuplicateKey_Refused proves walkNode refuses a mapping that
// repeats a key, at any depth, naming the duplicated key's full path — a
// YAML decode into map[string]any would have silently kept only one of the
// two, hiding whichever the author meant to win.
func TestLoad_DuplicateKey_Refused(t *testing.T) {
	cases := []struct {
		name, yaml, wantSubstr string
	}{
		{"top level", "temporal:\n  address: a:1\ntemporal:\n  address: b:1\n", "duplicate key \"temporal\""},
		{"nested", "storage:\n  dir: /a\n  dir: /b\n", "storage.dir: duplicate key \"dir\""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemp(t, "cfg.yaml", tc.yaml)
			_, err := Load(path, BinaryOrk)
			if err == nil {
				t.Fatal("Load: want an error for a duplicate key")
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("Load error = %q, want it to contain %q", err.Error(), tc.wantSubstr)
			}
		})
	}
}
