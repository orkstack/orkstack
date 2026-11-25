package main

import (
	"strings"
	"testing"
)

// TestLoadConfig_SessionTagsAreTheProvidersOwn: the engine reads the audiences
// to tag from the providers it declares, and two providers of one audience
// that disagree refuse its boot, naming the setting and both.
func TestLoadConfig_SessionTagsAreTheProvidersOwn(t *testing.T) {
	const one = "providers:\n  aws:\n    type: aws\n    region: eu-west-1\n    transitive_tags: [ork_task]\n"
	t.Setenv("ORK_PROVIDERS", one)
	cfg := loadConfig()
	if cfg.providersErr != nil || len(cfg.sessionTags) != 1 || len(cfg.sessionTags["sts.amazonaws.com"].Transitive) != 1 {
		t.Fatalf("session tags = %v, %v", cfg.sessionTags, cfg.providersErr)
	}
	t.Setenv("ORK_PROVIDERS", one+"  other:\n    type: aws\n    region: us-east-1\n    session_tags: false\n")
	err := loadConfig().providersErr
	for _, want := range []string{"ORK_PROVIDERS", "providers aws and other", "sts.amazonaws.com"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("providersErr = %v, want it to name %q", err, want)
		}
	}
	t.Setenv("ORK_PROVIDERS", "")
	if cfg := loadConfig(); cfg.providersErr != nil || len(cfg.sessionTags) != 0 {
		t.Errorf("with none declared: %v, %v", cfg.sessionTags, cfg.providersErr)
	}
}
