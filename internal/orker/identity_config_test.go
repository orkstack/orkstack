package orker

import (
	"strings"
	"testing"
	"time"
)

// TestIdentityConfig_Validate: the defaults hold, and a value outside a
// setting's range, or a dir that is neither auto, workroot nor absolute, is
// refused naming the setting.
func TestIdentityConfig_Validate(t *testing.T) {
	if err := DefaultIdentityConfig().Validate(); err != nil {
		t.Fatalf("the defaults: %v", err)
	}
	d := DefaultIdentityConfig()
	if d.Dir != "auto" || d.SetupBudget != 2*time.Minute || d.RetryMin != 5*time.Second || d.MaxConcurrent != 8 ||
		d.MaxClockSkew != 2*time.Minute || d.MaskGrace != 5*time.Minute {
		t.Fatalf("defaults %+v", d)
	}
	for _, tc := range []struct {
		name string
		edit func(*IdentityConfig)
		want string
	}{
		{"a relative dir", func(c *IdentityConfig) { c.Dir = "ids" }, IdentityDirVar},
		{"an empty dir", func(c *IdentityConfig) { c.Dir = "" }, IdentityDirVar},
		{"setup budget too short", func(c *IdentityConfig) { c.SetupBudget = 0 }, IdentitySetupBudgetVar},
		{"retry_min above the cap", func(c *IdentityConfig) { c.RetryMin = 2 * time.Minute }, IdentityRetryMinVar},
		{"retry_min too short", func(c *IdentityConfig) { c.RetryMin = time.Millisecond }, IdentityRetryMinVar},
		{"setup budget below retry_min", func(c *IdentityConfig) { c.SetupBudget = time.Second; c.RetryMin = 5 * time.Second }, IdentitySetupBudgetVar},
		{"no concurrency", func(c *IdentityConfig) { c.MaxConcurrent = 0 }, IdentityMaxConcurrentVar},
		{"no skew", func(c *IdentityConfig) { c.MaxClockSkew = 0 }, IdentityMaxClockSkewVar},
		{"a negative grace", func(c *IdentityConfig) { c.MaskGrace = -time.Second }, LogMaskGraceVar},
	} {
		c := DefaultIdentityConfig()
		tc.edit(&c)
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want it to name %s", tc.name, err, tc.want)
		}
	}
	for _, dir := range []string{"workroot", "/run/orkstack"} {
		c := DefaultIdentityConfig()
		c.Dir = dir
		if err := c.Validate(); err != nil {
			t.Errorf("dir %q: %v", dir, err)
		}
	}
}

// TestIdentityConfigFromEnv: unset is the defaults; each setting is read
// from its variable; one that is not its kind is refused naming it.
func TestIdentityConfigFromEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	c, err := IdentityConfigFromEnv(env(nil))
	if err != nil || c != DefaultIdentityConfig() {
		t.Fatalf("unset: %+v, %v", c, err)
	}
	c, err = IdentityConfigFromEnv(env(map[string]string{
		IdentityDirVar: "workroot", IdentitySetupBudgetVar: "5s", IdentityRetryMinVar: "200ms",
		IdentityMaxConcurrentVar: "2", IdentityMaxClockSkewVar: "30s", LogMaskGraceVar: "0s",
	}))
	want := IdentityConfig{Dir: "workroot", SetupBudget: 5 * time.Second, RetryMin: 200 * time.Millisecond,
		MaxConcurrent: 2, MaxClockSkew: 30 * time.Second, MaskGrace: 0}
	if err != nil || c != want {
		t.Fatalf("set: %+v, %v", c, err)
	}
	for k, v := range map[string]string{IdentitySetupBudgetVar: "two minutes", IdentityMaxConcurrentVar: "eight", IdentityRetryMinVar: "1h"} {
		if _, err := IdentityConfigFromEnv(env(map[string]string{k: v})); err == nil || !strings.Contains(err.Error(), k) {
			t.Errorf("%s=%s: %v", k, v, err)
		}
	}
}
