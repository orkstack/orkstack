package webhooks

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// ReasonDestinationBlocked is the machine-readable reason of a webhook
// destination OrkStack refuses to call: an address in a loopback, link-local
// (cloud metadata), private (RFC 1918, ULA), shared, multicast or otherwise
// non-public range that the operator has not allowed
// (webhooks.allowed_networks). It prefixes the message of a refused
// registration and of a refused delivery attempt.
const ReasonDestinationBlocked = "WEBHOOK_DESTINATION_BLOCKED"

// ErrDestinationBlocked is wrapped by every refusal that carries
// ReasonDestinationBlocked.
var ErrDestinationBlocked = errors.New(ReasonDestinationBlocked)

// allowed is the operator's allow-list of networks that may be called though
// they are not public. It is process-wide: it is set once at boot, before any
// delivery, and read at every dial.
var allowed atomic.Pointer[[]netip.Prefix]

// SetAllowedNetworks replaces the operator's allow-list and returns the
// previous one. An empty list allows nothing beyond public addresses.
func SetAllowedNetworks(prefixes []netip.Prefix) []netip.Prefix {
	cleaned := make([]netip.Prefix, 0, len(prefixes))
	for _, p := range prefixes {
		cleaned = append(cleaned, netip.PrefixFrom(p.Addr().Unmap(), unmappedBits(p)).Masked())
	}
	var previous []netip.Prefix
	if old := allowed.Swap(&cleaned); old != nil {
		previous = *old
	}
	return previous
}

// unmappedBits is p's length when p is an IPv4-mapped prefix, so one rule
// covers an address in either spelling.
func unmappedBits(p netip.Prefix) int {
	if p.Addr().Is4In6() && p.Bits() >= 96 {
		return p.Bits() - 96
	}
	return p.Bits()
}

// ParseNetworks parses CIDR strings, each a network address with its length
// (10.0.0.0/8). A host address with bits set below the length is refused so
// that a typo does not silently widen the allowance.
func ParseNetworks(cidrs []string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, raw := range cidrs {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("webhooks: invalid network %q: %w", raw, err)
		}
		if prefix.Addr().Is4In6() && prefix.Bits() < 96 {
			return nil, fmt.Errorf("webhooks: invalid network %q: an IPv4-mapped network is at least /96; write the IPv4 network", raw)
		}
		if prefix != prefix.Masked() {
			return nil, fmt.Errorf("webhooks: invalid network %q: host bits are set; write %s", raw, prefix.Masked())
		}
		out = append(out, prefix)
	}
	return out, nil
}

// WholeFamilyNetworks returns the entries of prefixes that admit a whole
// address family (0.0.0.0/0, ::/0, ::ffff:0:0/96), which turns the guard off
// for that family; the engine warns of each at boot.
func WholeFamilyNetworks(prefixes []netip.Prefix) []netip.Prefix {
	var out []netip.Prefix
	for _, p := range prefixes {
		if p.Bits() == 0 || (p.Addr().Is4In6() && p.Bits() == 96) {
			out = append(out, p)
		}
	}
	return out
}

// blockedRanges are the non-public ranges the address classes do not cover.
var blockedRanges = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), // shared address space (CGNAT)
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),  // reserved, and the broadcast address
	netip.MustParsePrefix("64:ff9b::/96"), // NAT64: embeds an IPv4 address
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2002::/16"),       // 6to4: embeds an IPv4 address
	netip.MustParsePrefix("2001::/32"),       // Teredo
	netip.MustParsePrefix("2001:db8::/32"),   // documentation
	netip.MustParsePrefix("fec0::/10"),       // deprecated site-local
	netip.MustParsePrefix("100::/64"),        // discard-only
	netip.MustParsePrefix("::ffff:0:0:0/96"), // SIIT: embeds an IPv4 address
	netip.MustParsePrefix("2001::/23"),       // IETF protocol assignments (PCP, AMT, AS112, Teredo)
	netip.MustParsePrefix("2001:10::/28"),
	netip.MustParsePrefix("2001:20::/28"),
	netip.MustParsePrefix("3fff::/20"),      // documentation
	netip.MustParsePrefix("5f00::/16"),      // SRv6
	netip.MustParsePrefix("::/96"),          // IPv4-compatible
	netip.MustParsePrefix("192.88.99.0/24"), // 6to4 relay
}

// CheckAddr reports whether a delivery may connect to addr: it must be public,
// or inside a network the operator allowed. The error wraps
// ErrDestinationBlocked.
func CheckAddr(addr netip.Addr) error {
	addr = addr.Unmap().WithZone("")
	if p := allowed.Load(); p != nil {
		for _, prefix := range *p {
			if prefix.Contains(addr) {
				return nil
			}
		}
	}
	if blocked(addr) {
		return fmt.Errorf("%w: %s is not a public address; allow its network with webhooks.allowed_networks (ORK_WEBHOOK_ALLOWED_NETWORKS)",
			ErrDestinationBlocked, addr)
	}
	return nil
}

func blocked(addr netip.Addr) bool {
	if !addr.IsValid() || addr.IsUnspecified() || addr.IsLoopback() || addr.IsPrivate() || addr.IsMulticast() ||
		addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsInterfaceLocalMulticast() {
		return true
	}
	for _, prefix := range blockedRanges {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// checkHost refuses a URL host that names a blocked address outright: an IP
// literal (in any spelling inet_aton reads) or localhost. A name is resolved,
// and judged, only when it is dialled.
func checkHost(host string) error {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		// localhost resolves to either loopback address; one allowed is enough,
		// and the dial judges the one actually used.
		if CheckAddr(netip.MustParseAddr("127.0.0.1")) != nil && CheckAddr(netip.MustParseAddr("::1")) != nil {
			return fmt.Errorf("%w: %s is a loopback name; allow loopback with webhooks.allowed_networks (ORK_WEBHOOK_ALLOWED_NETWORKS)",
				ErrDestinationBlocked, host)
		}
		return nil
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		return CheckAddr(addr)
	}
	if addr, ok := parseLegacyIPv4(host); ok {
		return CheckAddr(addr)
	}
	return nil
}

// parseLegacyIPv4 reads the inet_aton spellings a system resolver accepts:
// one to four dot-separated numbers, decimal, octal (leading 0) or hex (0x),
// the last filling the remaining bytes (2130706433, 0x7f.1, 127.1).
func parseLegacyIPv4(host string) (netip.Addr, bool) {
	parts := strings.Split(host, ".")
	if len(parts) > 4 {
		return netip.Addr{}, false
	}
	nums := make([]uint64, len(parts))
	for i, part := range parts {
		if part == "" {
			return netip.Addr{}, false
		}
		n, err := strconv.ParseUint(part, 0, 32)
		if err != nil {
			return netip.Addr{}, false
		}
		nums[i] = n
	}
	var value uint64
	for i, n := range nums[:len(nums)-1] {
		if n > 255 {
			return netip.Addr{}, false
		}
		value |= n << (24 - 8*uint(i))
	}
	last := nums[len(nums)-1]
	if last >= 1<<(8*uint(5-len(nums))) {
		return netip.Addr{}, false
	}
	value |= last
	return netip.AddrFrom4([4]byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}), true
}

// guardedDialer judges the address of every connection at the moment it is
// made, after name resolution, so a name that resolves to an internal address
// (DNS rebinding) and a redirect to one are refused like a literal.
func guardedDialer() *net.Dialer {
	return &net.Dialer{
		Timeout: 10 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			ap, err := netip.ParseAddrPort(address)
			if err != nil {
				return fmt.Errorf("%w: cannot read the destination address %q", ErrDestinationBlocked, address)
			}
			return CheckAddr(ap.Addr())
		},
	}
}

// guardedTransport is the default transport of a Dispatcher: the standard
// one with every connection judged at dial. It uses no proxy, because a proxy
// would hide the destination from that check.
func guardedTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = guardedDialer().DialContext
	return transport
}

// checkRedirect refuses a redirect to a blocked literal before it is dialled,
// and ends a chain of more than ten.
func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	return checkHost(req.URL.Hostname())
}
