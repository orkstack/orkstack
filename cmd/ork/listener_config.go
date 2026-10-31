package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/api"
)

// maxRequestMaxBytes is the largest ORK_REQUEST_MAX_BYTES the engine
// accepts while the task ceiling needs less: 1 GiB. A Connect message is
// held in memory whole, and again once decompressed, so a cap above that
// bounds nothing a process can afford. A ceiling that needs more gets what
// it needs, warned at boot, and may be given up to twice that.
const maxRequestMaxBytes = 1 << 30

// listenerConfig is the API listener's own settings (listenerFromEnv):
// ORK_HTTP_IDLE_TIMEOUT (engine.http_idle_timeout), ORK_REQUEST_MAX_BYTES
// (engine.request_max_bytes) and ORK_ALLOWED_HOSTS (engine.allowed_hosts),
// read into the Host policy with ORK_HTTP_ADDR. The listener's fixed bounds
// are constants of internal/api.
type listenerConfig struct {
	idleTimeout     time.Duration
	requestMaxBytes int64
	allowedHosts    []string
	hosts           api.HostPolicy
	// warnings are logged at boot, Warn, one line each.
	warnings []string
}

// listenerFromEnv reads the API listener's settings for an engine whose
// task ceiling is maxTasks. Unset, the read cap is what an apply at that
// ceiling needs (api.RequestMaxBytesFor). A malformed value, an idle timeout
// under a second, a read cap under that need or over 1 GiB (twice the need
// when the need is more, which is warned), and an allowed host that is not a name or a pattern are refused at boot:
// each would bound something other than what the operator wrote, and a cap
// under the need would turn a transport bound into a definition limit.
func listenerFromEnv(maxTasks int32) (listenerConfig, error) {
	need := api.RequestMaxBytesFor(maxTasks)
	cfg := listenerConfig{idleTimeout: api.DefaultIdleTimeout, requestMaxBytes: need}
	for _, entry := range strings.Split(env("ORK_ALLOWED_HOSTS", ""), ",") {
		if entry = strings.TrimSpace(entry); entry != "" {
			cfg.allowedHosts = append(cfg.allowedHosts, entry)
		}
	}
	hosts, err := api.NewHostPolicy(env("ORK_HTTP_ADDR", api.DefaultHTTPAddr), cfg.allowedHosts)
	if err != nil {
		return cfg, err
	}
	cfg.hosts = hosts
	if raw := env("ORK_HTTP_IDLE_TIMEOUT", ""); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return cfg, fmt.Errorf("ORK_HTTP_IDLE_TIMEOUT=%q is not a duration (e.g. 15m): %w", raw, err)
		}
		if parsed < time.Second {
			return cfg, fmt.Errorf("ORK_HTTP_IDLE_TIMEOUT=%s must be at least 1s", raw)
		}
		cfg.idleTimeout = parsed
	}
	requestMaxBytes, err := envInt64("ORK_REQUEST_MAX_BYTES", need)
	if err != nil {
		return cfg, err
	}
	if requestMaxBytes < need {
		return cfg, fmt.Errorf("ORK_REQUEST_MAX_BYTES=%d (engine.request_max_bytes) is under %d, what an apply at "+
			"engine.orkflow_max_tasks (ORK_ORKFLOW_MAX_TASKS=%d) may need (%d bytes and %d a task): raise it, "+
			"or unset it to take that need", requestMaxBytes, need, maxTasks, api.RequestBaseBytes, api.RequestBytesPerTask)
	}
	limit := int64(maxRequestMaxBytes)
	if need > maxRequestMaxBytes {
		limit = 2 * need
		cfg.warnings = append(cfg.warnings, fmt.Sprintf("engine.orkflow_max_tasks (ORK_ORKFLOW_MAX_TASKS=%d) needs a "+
			"read cap of %d bytes, over 1 GiB: the engine holds one such request in memory, and again once "+
			"decompressed, per request in flight", maxTasks, need))
	}
	if requestMaxBytes > limit {
		return cfg, fmt.Errorf("ORK_REQUEST_MAX_BYTES=%d (engine.request_max_bytes) is over %d: a read cap above "+
			"1 GiB, or above twice what engine.orkflow_max_tasks (ORK_ORKFLOW_MAX_TASKS=%d) needs when that is "+
			"more, bounds nothing a process can afford", requestMaxBytes, limit, maxTasks)
	}
	cfg.requestMaxBytes = requestMaxBytes
	return cfg, nil
}
