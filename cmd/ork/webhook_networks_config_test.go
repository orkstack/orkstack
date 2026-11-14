package main

import (
	"net/netip"
	"slices"
	"strings"
	"testing"
)

// webhooks.allowed_networks is read with the rest of the configuration, split
// on commas and spaces, and a malformed entry is held for the boot to refuse.
func TestLoadConfig_ReadsWebhookAllowedNetworks(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		want      []netip.Prefix
		wantErr   string
	}{
		{"unset", "", []netip.Prefix{}, ""},
		{"commas and spaces", "10.0.0.0/8, fd00::/8 192.168.1.0/24", []netip.Prefix{
			netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("fd00::/8"), netip.MustParsePrefix("192.168.1.0/24"),
		}, ""},
		{"malformed", "10.0.0.0/8,not-a-network", nil, "ORK_WEBHOOK_ALLOWED_NETWORKS (webhooks.allowed_networks)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ORK_WEBHOOK_ALLOWED_NETWORKS", tc.raw)
			cfg := loadConfig()
			if tc.wantErr != "" {
				if cfg.webhookNetworksErr == nil || !strings.Contains(cfg.webhookNetworksErr.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want one naming %s", cfg.webhookNetworksErr, tc.wantErr)
				}
				return
			}
			if cfg.webhookNetworksErr != nil {
				t.Fatalf("error = %v", cfg.webhookNetworksErr)
			}
			if !slices.Equal(cfg.webhookNetworks, tc.want) {
				t.Fatalf("networks = %v, want %v", cfg.webhookNetworks, tc.want)
			}
		})
	}
}
