package main

import (
	"net/netip"
	"os"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

// The suites register and deliver to httptest servers on loopback, so this
// package's tests run with loopback allowed (webhooks.allowed_networks).
// The variable carries the same allowance to engines started as subprocesses.
func init() {
	_ = os.Setenv("ORK_WEBHOOK_ALLOWED_NETWORKS", "127.0.0.0/8")
	webhooks.SetAllowedNetworks([]netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128")})
}
