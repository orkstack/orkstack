package eventsources

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// The deployment's event_sources.* settings (docs/DEPLOY.md §1): each
// variable, and the default internal/config binds its key to.
const (
	EnvEnabled            = "ORK_EVENT_SOURCES_ENABLED"
	EnvAllowAsserted      = "ORK_EVENT_SOURCES_ALLOW_ASSERTED"
	EnvMaxMessageBytes    = "ORK_EVENT_SOURCE_MAX_MESSAGE_BYTES"
	EnvMaxInputBytes      = "ORK_EVENT_SOURCE_MAX_INPUT_BYTES"
	EnvMaxRate            = "ORK_EVENT_SOURCE_MAX_RATE"
	EnvNamespaceRate      = "ORK_EVENT_SOURCE_NAMESPACE_RATE"
	EnvMaxSources         = "ORK_EVENT_SOURCE_MAX_SOURCES"
	EnvMaxPerNamespace    = "ORK_EVENT_SOURCE_MAX_PER_NAMESPACE"
	EnvMaxInstances       = "ORK_EVENT_SOURCE_MAX_INSTANCES"
	EnvNamespaceInstances = "ORK_EVENT_SOURCE_NAMESPACE_INSTANCES"
	EnvReconcile          = "ORK_EVENT_SOURCE_RECONCILE"
	EnvReadmit            = "ORK_EVENT_SOURCE_READMIT"
	EnvInstanceMaxAge     = "ORK_EVENT_SOURCE_INSTANCE_MAX_AGE"
	EnvRestartMaxInterval = "ORK_EVENT_SOURCE_RESTART_MAX_INTERVAL"

	DefaultEnabled            = false
	DefaultAllowAsserted      = false
	DefaultMaxRate            = 100
	DefaultNamespaceRate      = 200
	DefaultMaxSources         = 200
	DefaultMaxPerNamespace    = 20
	DefaultMaxInstances       = 4
	DefaultNamespaceInstances = 20
	DefaultReconcile          = time.Minute
	DefaultReadmit            = 5 * time.Minute
	DefaultInstanceMaxAge     = 7 * 24 * time.Hour
	DefaultRestartMaxInterval = 5 * time.Minute

	// MaxMessageBytesCeiling and MaxInputBytesCeiling are the most the two
	// bounds may be set to; a boot with more is refused.
	MaxMessageBytesCeiling = 64 << 20
	MaxInputBytesCeiling   = 256 << 10
)

// Settings are the deployment's event-source settings as one engine read
// them.
type Settings struct {
	// Enabled turns event sources on; off, every write is refused
	// EVENT_SOURCES_DISABLED.
	Enabled bool
	// AllowAsserted lets the user-facing service answer where callers are
	// not authenticated. It never lets an instance run.
	AllowAsserted bool
	Limits        Limits
	// Reconcile is how often an engine's pass over the sources begins.
	Reconcile time.Duration
	// Readmit is how often a running instance is judged again, and how long a
	// revoke that was observed is held before it is checked.
	Readmit time.Duration
	// InstanceMaxAge is the age an instance is replaced at, and
	// RestartMaxInterval the longest wait between two starts of a failing one.
	InstanceMaxAge, RestartMaxInterval time.Duration
}

// DefaultSettings are the settings of a deployment that sets none.
func DefaultSettings() Settings {
	return Settings{Enabled: DefaultEnabled, AllowAsserted: DefaultAllowAsserted, Limits: Limits{
		MaxMessageBytes: DefaultMaxMessageBytes, MaxInputBytes: DefaultMaxInputBytes,
		MaxRate: DefaultMaxRate, NamespaceRate: DefaultNamespaceRate, MaxSources: DefaultMaxSources,
		MaxPerNamespace: DefaultMaxPerNamespace, MaxInstances: DefaultMaxInstances, NamespaceInstances: DefaultNamespaceInstances,
	}, Reconcile: DefaultReconcile, Readmit: DefaultReadmit, InstanceMaxAge: DefaultInstanceMaxAge, RestartMaxInterval: DefaultRestartMaxInterval}
}

// SettingsFromEnv reads the settings through lookup, refusing a value that
// does not parse or is out of its range, naming the variable: a boot with
// one does not start.
func SettingsFromEnv(lookup func(string) (string, bool)) (Settings, error) {
	s := DefaultSettings()
	boolean := func(env string, into *bool) error {
		v, ok := lookup(env)
		if !ok || strings.TrimSpace(v) == "" {
			return nil
		}
		b, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return fmt.Errorf("%s=%q: want true or false", env, v)
		}
		*into = b
		return nil
	}
	integer := func(env string, low, high int64, into func(int64)) error {
		v, ok := lookup(env)
		if !ok || strings.TrimSpace(v) == "" {
			return nil
		}
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil || n < low || n > high {
			return fmt.Errorf("%s=%q: want an integer from %d to %d", env, v, low, high)
		}
		into(n)
		return nil
	}
	span := func(env string, low, high time.Duration, into *time.Duration) error {
		v, ok := lookup(env)
		if !ok || strings.TrimSpace(v) == "" {
			return nil
		}
		d, err := time.ParseDuration(strings.TrimSpace(v))
		if err != nil || d < low || d > high {
			return fmt.Errorf("%s=%q: want a duration from %s to %s", env, v, low, high)
		}
		*into = d
		return nil
	}
	l := &s.Limits
	for _, err := range []error{
		boolean(EnvEnabled, &s.Enabled),
		boolean(EnvAllowAsserted, &s.AllowAsserted),
		integer(EnvMaxMessageBytes, 1, MaxMessageBytesCeiling, func(n int64) { l.MaxMessageBytes = n }),
		integer(EnvMaxInputBytes, 1, MaxInputBytesCeiling, func(n int64) { l.MaxInputBytes = int(n) }),
		integer(EnvMaxRate, 1, 1<<20, func(n int64) { l.MaxRate = int32(n) }),
		integer(EnvNamespaceRate, 1, 1<<30, func(n int64) { l.NamespaceRate = int(n) }),
		integer(EnvMaxSources, 1, 1<<20, func(n int64) { l.MaxSources = int(n) }),
		integer(EnvMaxPerNamespace, 1, 1<<20, func(n int64) { l.MaxPerNamespace = int(n) }),
		integer(EnvMaxInstances, 1, 1<<10, func(n int64) { l.MaxInstances = int32(n) }),
		integer(EnvNamespaceInstances, 1, 1<<20, func(n int64) { l.NamespaceInstances = int(n) }),
		span(EnvReconcile, 15*time.Second, 10*time.Minute, &s.Reconcile),
		span(EnvReadmit, time.Minute, 30*time.Minute, &s.Readmit),
		span(EnvInstanceMaxAge, 2*time.Hour, 30*24*time.Hour, &s.InstanceMaxAge),
		span(EnvRestartMaxInterval, time.Second, time.Hour, &s.RestartMaxInterval),
	} {
		if err != nil {
			return Settings{}, fmt.Errorf("event sources: %w", err)
		}
	}
	return s, nil
}
