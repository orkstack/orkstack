package orker

import (
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

// The settings of an Orker's task identity (docs/DEPLOY.md §1).
const (
	// IdentityDirVar is orker.identity.dir: auto, workroot or an absolute
	// path, where identity token and credential files are made.
	IdentityDirVar = "ORK_ORKER_IDENTITY_DIR"
	// IdentitySetupBudgetVar is orker.identity.setup_budget: how long an
	// attempt asks for its first tokens before it fails.
	IdentitySetupBudgetVar = "ORK_ORKER_IDENTITY_SETUP_BUDGET"
	// IdentityRetryMinVar is orker.identity.retry_min: the first retry's
	// least delay under the one retry rule.
	IdentityRetryMinVar = "ORK_ORKER_IDENTITY_RETRY_MIN"
	// IdentityMaxConcurrentVar is orker.identity.max_concurrent: how many
	// mints this Orker has in flight at once.
	IdentityMaxConcurrentVar = "ORK_ORKER_IDENTITY_MAX_CONCURRENT"
	// ProvidersCallTimeoutVar is orker.providers.call_timeout: how long one
	// call to a provider may take.
	ProvidersCallTimeoutVar = "ORK_ORKER_PROVIDERS_CALL_TIMEOUT"
	// ProvidersMaxConcurrentVar is orker.providers.max_concurrent: how many
	// calls this Orker has in flight to one provider.
	ProvidersMaxConcurrentVar = "ORK_ORKER_PROVIDERS_MAX_CONCURRENT"
	// IdentityMaxClockSkewVar is orker.identity.max_clock_skew: how far
	// apart the engine's issued_at and this Orker's clock may be.
	IdentityMaxClockSkewVar = "ORK_ORKER_IDENTITY_MAX_CLOCK_SKEW"
	// LogMaskGraceVar is orker.log.mask_grace: how long a replaced value
	// stays masked after it has expired.
	LogMaskGraceVar = "ORK_ORKER_LOG_MASK_GRACE"
)

// The settings' defaults.
const (
	DefaultIdentityDir           = "auto"
	DefaultIdentitySetupBudget   = 2 * time.Minute
	DefaultIdentityRetryMin      = 5 * time.Second
	DefaultIdentityMaxConcurrent = 8
	DefaultIdentityMaxClockSkew  = 2 * time.Minute
	DefaultLogMaskGrace          = 5 * time.Minute
)

// IdentityConfig is an Orker's task identity settings.
type IdentityConfig struct {
	Dir           string
	SetupBudget   time.Duration
	RetryMin      time.Duration
	MaxConcurrent int
	MaxClockSkew  time.Duration
	MaskGrace     time.Duration
	// ProviderCallTimeout bounds one call to a provider, and
	// ProviderMaxConcurrent the calls this Orker has in flight to one.
	ProviderCallTimeout   time.Duration
	ProviderMaxConcurrent int
}

// DefaultIdentityConfig is every setting at its default.
func DefaultIdentityConfig() IdentityConfig {
	return IdentityConfig{
		Dir: DefaultIdentityDir, SetupBudget: DefaultIdentitySetupBudget, RetryMin: DefaultIdentityRetryMin,
		MaxConcurrent: DefaultIdentityMaxConcurrent, MaxClockSkew: DefaultIdentityMaxClockSkew, MaskGrace: DefaultLogMaskGrace,
		ProviderCallTimeout: provider.DefaultCallTimeout, ProviderMaxConcurrent: provider.DefaultMaxConcurrent,
	}
}

// Validate refuses a setting outside its range, naming it, at boot.
func (c IdentityConfig) Validate() error {
	switch {
	case c.Dir != "auto" && c.Dir != "workroot" && !filepath.IsAbs(c.Dir):
		return fmt.Errorf("%s=%q: want auto, workroot or an absolute path", IdentityDirVar, c.Dir)
	case c.RetryMin < 100*time.Millisecond || c.RetryMin > identityRetryCap:
		return fmt.Errorf("%s=%s: want 100ms to %s", IdentityRetryMinVar, c.RetryMin, identityRetryCap)
	case c.SetupBudget < c.RetryMin || c.SetupBudget > time.Hour:
		return fmt.Errorf("%s=%s: want at least %s (%s) and at most 1h", IdentitySetupBudgetVar, c.SetupBudget, IdentityRetryMinVar, c.RetryMin)
	case c.MaxConcurrent < 1 || c.MaxConcurrent > 256:
		return fmt.Errorf("%s=%d: want 1 to 256", IdentityMaxConcurrentVar, c.MaxConcurrent)
	case c.MaxClockSkew < time.Second || c.MaxClockSkew > time.Hour:
		return fmt.Errorf("%s=%s: want 1s to 1h", IdentityMaxClockSkewVar, c.MaxClockSkew)
	case c.MaskGrace < 0 || c.MaskGrace > 24*time.Hour:
		return fmt.Errorf("%s=%s: want 0 to 24h", LogMaskGraceVar, c.MaskGrace)
	case c.ProviderCallTimeout < time.Second || c.ProviderCallTimeout > 5*time.Minute:
		return fmt.Errorf("%s=%s: want 1s to 5m", ProvidersCallTimeoutVar, c.ProviderCallTimeout)
	case c.ProviderMaxConcurrent < 1 || c.ProviderMaxConcurrent > 256:
		return fmt.Errorf("%s=%d: want 1 to 256", ProvidersMaxConcurrentVar, c.ProviderMaxConcurrent)
	}
	return nil
}

// IdentityConfigFromEnv reads the settings from getenv: unset is each one's
// default, and a value that is not its kind or out of its range is refused,
// naming the setting.
func IdentityConfigFromEnv(getenv func(string) string) (IdentityConfig, error) {
	c := DefaultIdentityConfig()
	if v := getenv(IdentityDirVar); v != "" {
		c.Dir = v
	}
	for _, d := range []struct {
		name string
		to   *time.Duration
	}{
		{IdentitySetupBudgetVar, &c.SetupBudget}, {IdentityRetryMinVar, &c.RetryMin},
		{IdentityMaxClockSkewVar, &c.MaxClockSkew}, {LogMaskGraceVar, &c.MaskGrace},
		{ProvidersCallTimeoutVar, &c.ProviderCallTimeout},
	} {
		if v := getenv(d.name); v != "" {
			parsed, err := time.ParseDuration(v)
			if err != nil {
				return c, fmt.Errorf("%s=%q is not a duration (want e.g. 2m, 5s)", d.name, v)
			}
			*d.to = parsed
		}
	}
	for _, d := range []struct {
		name string
		to   *int
	}{{IdentityMaxConcurrentVar, &c.MaxConcurrent}, {ProvidersMaxConcurrentVar, &c.ProviderMaxConcurrent}} {
		if v := getenv(d.name); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return c, fmt.Errorf("%s=%q is not a whole number", d.name, v)
			}
			*d.to = n
		}
	}
	return c, c.Validate()
}
