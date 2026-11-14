// Package webhooktest holds the webhook destination policy the suites use:
// they register and deliver to httptest servers on loopback, which the
// production default refuses.
package webhooktest

import (
	"net/netip"
	"os"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

// LoopbackNetworks is the allowance the suites run with.
const LoopbackNetworks = "127.0.0.0/8,::1/128"

// LoopbackPolicy admits public addresses and loopback, for a registry or a
// dispatcher built in a test.
func LoopbackPolicy() webhooks.DestinationPolicy {
	return webhooks.NewDestinationPolicy([]netip.Prefix{
		netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128"),
	})
}

// AllowLoopbackInEngines sets webhooks.allowed_networks to loopback in this
// process's environment, which every engine a suite boots reads: one started
// as a subprocess inherits it, and one booted in process builds its policy
// from it. Call it from a package's init.
func AllowLoopbackInEngines() {
	if err := os.Setenv("ORK_WEBHOOK_ALLOWED_NETWORKS", LoopbackNetworks); err != nil {
		panic("webhooktest: cannot allow loopback in engines: " + err.Error())
	}
}
