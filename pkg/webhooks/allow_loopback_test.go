package webhooks_test

import (
	"net/netip"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

// The suites deliver to httptest servers on loopback, so the package's tests
// run with loopback allowed; destination_test.go replaces it where a test
// proves the refusal.
func init() {
	webhooks.SetAllowedNetworks([]netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128")})
}
