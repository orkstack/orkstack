package main

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// eventSourceCheckLines are what `ork --check` says about event sources
// beside the settings it prints: whether they are on, what the opt-in lets
// through, and that no source consumes anything in this release.
func eventSourceCheckLines(s eventsources.Settings) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# event sources: enabled=%t allow_asserted=%t\n", s.Enabled, s.AllowAsserted)
	if s.AllowAsserted {
		b.WriteString("# event sources: allow_asserted lets any caller the gateway lets through write, read and evaluate sources;" +
			" no source's instance runs until authentication is enforced\n")
	}
	b.WriteString("# event sources: this release runs no source; a source is written, read and evaluated, and consumes nothing\n")
	return b.String()
}

// warnEventSourceOptIn logs the opt-in at every boot that sets it.
func warnEventSourceOptIn(logger *slog.Logger, s eventsources.Settings) {
	if s.AllowAsserted {
		logger.Warn("event_sources.allow_asserted is set: the event-source API answers callers that are not authenticated; " +
			"no source's instance runs until authentication is enforced")
	}
}

// eventSourceSettingsFor reads the settings as --check sees them: the
// environment first, then what the file would set, without setting
// anything in the process.
func eventSourceSettingsFor(apply func(lookup func(string) (string, bool), setenv func(string, string) error) ([]string, error),
	lookup func(string) (string, bool)) (eventsources.Settings, error) {
	values := map[string]string{}
	if apply != nil {
		if _, err := apply(lookup, func(k, v string) error { values[k] = v; return nil }); err != nil {
			return eventsources.Settings{}, err
		}
	}
	return eventsources.SettingsFromEnv(func(k string) (string, bool) {
		if v, ok := lookup(k); ok {
			return v, true
		}
		v, ok := values[k]
		return v, ok
	})
}

// sourcesRun reports whether this deployment runs event sources: the feature
// is on, it runs as a release, and its callers are authenticated. Elsewhere a
// source is stored, and only the workflow of one being deleted is started.
func sourcesRun(s eventsources.Settings, c config) bool {
	return s.Enabled && c.deployment.BuildID != "" && c.auth.mode == authn.ModeEnforce
}
