package main

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/webui"
)

// uiConfig is engine.ui.* (uiFromEnv): whether this engine serves the web UI
// at / on its API listener (ORK_UI_ENABLED), and from where — the build
// compiled into the binary, or the one in ORK_UI_DIR.
type uiConfig struct {
	enabled bool
	dir     string
}

// uiFromEnv reads ORK_UI_ENABLED and ORK_UI_DIR. A value that is not a
// boolean, and a directory named for a UI that is switched off, are refused
// at boot: each says the operator meant something the engine would not do.
func uiFromEnv() (uiConfig, error) {
	cfg := uiConfig{enabled: api.DefaultUIEnabled, dir: env("ORK_UI_DIR", "")}
	if raw := env("ORK_UI_ENABLED", ""); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			return cfg, fmt.Errorf("ORK_UI_ENABLED=%q must be true or false: %w", raw, err)
		}
		cfg.enabled = enabled
	}
	if !cfg.enabled && cfg.dir != "" {
		return cfg, fmt.Errorf("ORK_UI_DIR=%q names a UI build, but ORK_UI_ENABLED=false switches the UI off; unset one of them", cfg.dir)
	}
	return cfg, nil
}

// uiHandler builds what NewMux mounts at / (nil when the UI is off) and names
// its source for the boot log: "embedded", "directory", "placeholder" (a
// binary compiled without the UI's build) or "off".
func uiHandler(cfg uiConfig) (http.Handler, string, error) {
	switch {
	case !cfg.enabled:
		return nil, "off", nil
	case cfg.dir != "":
		build, err := webui.Dir(cfg.dir)
		if err != nil {
			return nil, "", fmt.Errorf("ORK_UI_DIR: %w", err)
		}
		return webui.Handler(build), "directory", nil
	}
	if build, ok := webui.Embedded(); ok {
		return webui.Handler(build), "embedded", nil
	}
	return webui.Handler(webui.Placeholder()), "placeholder", nil
}
