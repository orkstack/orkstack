// Package httpplugin implements the builtin http plugin.
//
// `with`:
//
//	op                get | post | put | patch | delete | head (default get)
//	method            legacy spelling of `op`; any HTTP method verb
//	url               request URL (required)
//	headers           map of header name -> value
//	body              request body string
//	capture_body_as   artifact name for the exact response bytes
//	timeout_seconds   per-request timeout (number or numeric string)
//
// `op` is the generic operation selector every OrkStack plugin shares (see
// docs/PLUGIN_DESIGN.md). An unknown `op` fails the task permanently. `method`
// stays supported for orkflows written before `op` existed and, unlike `op`,
// accepts any verb (OPTIONS, TRACE, custom). Setting both to different verbs
// is an error.
//
// `output`: status, headers (name -> list of values), json (present only when
// the response body parses as JSON), duration_ms.
package httpplugin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/orkstack/orkstack/pkg/plugin"
)

// maxJSONBytes bounds the body size the plugin attempts to parse as JSON. The
// exact bytes always remain available as an artifact.
const maxJSONBytes = 8 << 20

// Plugin performs an HTTP request.
type Plugin struct {
	// Client overrides the HTTP client. Nil uses a per-request client honoring
	// timeout_seconds.
	Client *http.Client
}

// New creates the http plugin.
func New() *Plugin { return &Plugin{} }

// Name is the DSL `plugin:` identifier.
func (p *Plugin) Name() string { return "http" }

// Describe returns discovery metadata.
func (p *Plugin) Describe() plugin.Descriptor {
	return plugin.Descriptor{
		Name:        "http",
		Version:     "0.1.0",
		Description: "Perform an HTTP request and optionally capture the response body as an artifact.",
		InputSchema: plugin.SchemaDoc{
			"type": "object",
			"properties": map[string]any{
				"op":              map[string]any{"type": "string", "enum": []any{"get", "post", "put", "patch", "delete", "head"}, "default": "get"},
				"method":          map[string]any{"type": "string", "default": http.MethodGet},
				"url":             map[string]any{"type": "string"},
				"headers":         map[string]any{"type": "object"},
				"body":            map[string]any{"type": "string"},
				"capture_body_as": map[string]any{"type": "string"},
				"timeout_seconds": map[string]any{"type": "number"},
			},
			"required": []any{"url"},
		},
		OutputSchema: plugin.SchemaDoc{
			"type": "object",
			"properties": map[string]any{
				"status":      map[string]any{"type": "number"},
				"headers":     map[string]any{"type": "object"},
				"json":        map[string]any{},
				"duration_ms": map[string]any{"type": "number"},
			},
		},
	}
}

// Execute performs the request. The response body is streamed to a file in the
// workdir so the captured artifact holds the exact bytes.
func (p *Plugin) Execute(ctx context.Context, ec *plugin.ExecContext) (*plugin.Result, error) {
	url := strings.TrimSpace(configString(ec.Config, "url"))
	if url == "" {
		return nil, plugin.Permanent(fmt.Errorf("http: `url` is required"))
	}
	method, err := resolveMethod(ec.Config)
	if err != nil {
		return nil, plugin.Permanent(err)
	}

	timeout, err := configDuration(ec.Config, "timeout_seconds")
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("http: %w", err))
	}
	headers, err := configStringMap(ec.Config, "headers")
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("http: %w", err))
	}

	reqCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var body io.Reader
	if raw := configString(ec.Config, "body"); raw != "" {
		body = strings.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(reqCtx, method, url, body)
	if err != nil {
		return nil, plugin.Permanent(fmt.Errorf("http: build %s %s: %w", method, url, err))
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}

	client := p.Client
	if client == nil {
		client = &http.Client{}
	}

	if ec.Log != nil {
		ec.Log("http %s %s", method, url)
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http: %s %s: %w", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	file, err := os.CreateTemp(ec.WorkDir, "http-body-*")
	if err != nil {
		return nil, fmt.Errorf("http: create body file: %w", err)
	}
	bodyPath := file.Name()
	size, copyErr := io.Copy(file, resp.Body)
	closeErr := file.Close()
	elapsed := time.Since(start)
	if copyErr != nil {
		_ = os.Remove(bodyPath)
		return nil, fmt.Errorf("http: read body of %s %s: %w", method, url, copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(bodyPath)
		return nil, fmt.Errorf("http: write body file: %w", closeErr)
	}

	output := plugin.Payload{
		"status":      float64(resp.StatusCode),
		"headers":     headerPayload(resp.Header),
		"duration_ms": float64(elapsed.Microseconds()) / 1000,
	}
	if parsed, ok := parseJSON(bodyPath, size); ok {
		output["json"] = parsed
	}

	captureAs := strings.TrimSpace(configString(ec.Config, "capture_body_as"))
	var produced []plugin.ProducedArtifact
	if captureAs != "" {
		produced = append(produced, plugin.ProducedArtifact{
			Name:        captureAs,
			LocalPath:   bodyPath,
			ContentType: resp.Header.Get("Content-Type"),
		})
	} else {
		_ = os.Remove(bodyPath)
	}

	if ec.Log != nil {
		ec.Log("http %s %s -> %d (%d bytes in %s)", method, url, resp.StatusCode, size, elapsed)
	}
	return &plugin.Result{Output: output, Artifacts: produced}, nil
}

// knownOps are the operations `op` selects. They map one-to-one onto HTTP
// method verbs, so `op` is just the portable spelling of `method`.
var knownOps = []string{"get", "post", "put", "patch", "delete", "head"}

// resolveMethod turns `op` (preferred) or `method` (legacy) into the verb to
// send. An unrecognized `op` is a permanent user error rather than a request
// the server would reject anyway.
func resolveMethod(config map[string]any) (string, error) {
	op := strings.TrimSpace(configString(config, "op"))
	method := strings.TrimSpace(configString(config, "method"))

	if op != "" && method != "" && !strings.EqualFold(op, method) {
		return "", fmt.Errorf("http: `op` %q and `method` %q disagree; set one of them", op, method)
	}
	if op != "" {
		if !slices.Contains(knownOps, strings.ToLower(op)) {
			return "", fmt.Errorf("http: unknown op %q (want %s)", op, strings.Join(knownOps, ", "))
		}
		return strings.ToUpper(op), nil
	}
	if method == "" {
		return http.MethodGet, nil
	}
	return strings.ToUpper(method), nil
}

// parseJSON decodes the body when it is valid JSON and small enough to hold in
// memory. A non-JSON body is not an error: the bytes stay in the artifact.
func parseJSON(path string, size int64) (any, bool) {
	if size == 0 || size > maxJSONBytes {
		return nil, false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, false
	}
	return value, true
}

// headerPayload keeps every header value rather than collapsing repeats.
func headerPayload(header http.Header) map[string]any {
	out := make(map[string]any, len(header))
	for name, values := range header {
		items := make([]any, 0, len(values))
		for _, value := range values {
			items = append(items, value)
		}
		out[name] = items
	}
	return out
}

func configString(config map[string]any, key string) string {
	value, ok := config[key]
	if !ok || value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", value)
}

func configStringMap(config map[string]any, key string) (map[string]string, error) {
	value, ok := config[key]
	if !ok || value == nil {
		return nil, nil
	}
	raw, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("`%s` must be a map, got %T", key, value)
	}
	out := make(map[string]string, len(raw))
	for name, item := range raw {
		if item == nil {
			out[name] = ""
			continue
		}
		if s, ok := item.(string); ok {
			out[name] = s
			continue
		}
		out[name] = fmt.Sprintf("%v", item)
	}
	return out, nil
}

// configDuration accepts the float64 a proto struct yields, a plain int, and a
// numeric string.
func configDuration(config map[string]any, key string) (time.Duration, error) {
	value, ok := config[key]
	if !ok || value == nil {
		return 0, nil
	}
	var seconds float64
	switch typed := value.(type) {
	case float64:
		seconds = typed
	case float32:
		seconds = float64(typed)
	case int:
		seconds = float64(typed)
	case int32:
		seconds = float64(typed)
	case int64:
		seconds = float64(typed)
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, nil
		}
		parsed, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0, fmt.Errorf("`%s` must be a number, got %q", key, typed)
		}
		seconds = parsed
	default:
		return 0, fmt.Errorf("`%s` must be a number, got %T", key, value)
	}
	if seconds <= 0 {
		return 0, nil
	}
	return time.Duration(seconds * float64(time.Second)), nil
}
