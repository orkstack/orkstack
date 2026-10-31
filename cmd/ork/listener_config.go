package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/api"
)

// maxRequestMaxBytes is the largest ORK_REQUEST_MAX_BYTES the engine
// accepts: 1 GiB. A Connect message is held in memory whole, and again once
// decompressed, so a cap above that bounds nothing a process can afford.
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
}

// listenerFromEnv reads the API listener's settings. A malformed value, an
// idle timeout under a second, a read cap outside 1 MiB … 1 GiB and an
// allowed host that is not a name or a pattern are refused at boot: each
// would bound something other than what the operator wrote.
func listenerFromEnv() (listenerConfig, error) {
	cfg := listenerConfig{idleTimeout: api.DefaultIdleTimeout, requestMaxBytes: api.DefaultRequestMaxBytes}
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
	requestMaxBytes, err := envInt64("ORK_REQUEST_MAX_BYTES", api.DefaultRequestMaxBytes)
	if err != nil {
		return cfg, err
	}
	if requestMaxBytes < api.MinRequestMaxBytes || requestMaxBytes > maxRequestMaxBytes {
		return cfg, fmt.Errorf("ORK_REQUEST_MAX_BYTES=%d must be between %d (1 MiB) and %d (1 GiB)",
			requestMaxBytes, api.MinRequestMaxBytes, maxRequestMaxBytes)
	}
	cfg.requestMaxBytes = requestMaxBytes
	return cfg, nil
}
