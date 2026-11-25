package provider_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/provider"
)

// TestFromEnv_RefusesBootNamingTheSetting: an unset variable declares none;
// a document that declares a type this build lacks refuses boot, naming the
// setting and its variable.
func TestFromEnv_RefusesBootNamingTheSetting(t *testing.T) {
	set, err := provider.FromEnv(func(string) (string, bool) { return "", false }, testTypes)
	if err != nil || set.Len() != 0 {
		t.Fatalf("unset = %d providers, %v", set.Len(), err)
	}
	_, err = provider.FromEnv(func(key string) (string, bool) {
		if key == provider.Var {
			return "providers:\n  aws:\n    type: aws\n", true
		}
		return "", false
	}, testTypes)
	if err == nil || !strings.Contains(err.Error(), "providers (ORK_PROVIDERS)") || !strings.Contains(err.Error(), `type "aws" is not one this build has`) {
		t.Fatalf("FromEnv = %v; want a refusal naming the setting and the type", err)
	}
}
