package webhooks

import (
	"errors"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// FuzzValidateDestination holds the destination guard to what it claims for
// any URL a caller sends: it never panics; it accepts only what ValidateURL
// accepts; every refusal of a valid URL carries ErrDestinationBlocked; and a
// host spelled as an address, in netip's form or any inet_aton form a
// resolver reads, is refused exactly when that address is not public.
func FuzzValidateDestination(f *testing.F) {
	for _, seed := range []string{
		"http://127.0.0.1/hook", "http://localhost:8080/hook", "https://LOCALHOST./x", "http://a.localhost/x",
		"http://[::1]:9000/", "http://169.254.169.254/latest/meta-data/", "http://10.0.0.5/", "http://192.168.0.1:81/",
		"http://[fd00::1]/", "http://[::ffff:127.0.0.1]/", "http://0.0.0.0/", "http://2130706433/", "http://0x7f.1/",
		"http://017700000001/", "http://127.1/", "https://example.com/hook", "http://8.8.8.8/x",
		"http://0x08.0x08.0x08.0x08/", "http://134744072/", "http://8.526344/", "http://255.255.255.255/",
		"http://4294967296/", "http://1.2.3.4.5/", "http://08.1/", "http://[fe80::1%25eth0]/", "ftp://8.8.8.8/",
		"", "http://", "http://user@127.0.0.1/", "http://127.0.0.1.:80/",
	} {
		f.Add(seed)
	}
	var policy DestinationPolicy // public addresses only
	f.Fuzz(func(t *testing.T, raw string) {
		_, err := policy.ValidateDestination(raw)
		_, syntaxErr := ValidateURL(raw)
		if syntaxErr != nil {
			if err == nil {
				t.Fatalf("ValidateDestination(%q) accepted a URL ValidateURL refuses: %v", raw, syntaxErr)
			}
			return
		}
		if err != nil && !errors.Is(err, ErrDestinationBlocked) {
			t.Fatalf("ValidateDestination(%q) refused a valid URL without ErrDestinationBlocked: %v", raw, err)
		}
		parsed, perr := url.Parse(strings.TrimSpace(raw))
		if perr != nil {
			t.Fatalf("url.Parse(%q) failed after ValidateURL accepted it: %v", raw, perr)
		}
		host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
		if host == "localhost" || strings.HasSuffix(host, ".localhost") {
			if err == nil {
				t.Fatalf("ValidateDestination(%q) accepted a loopback name", raw)
			}
			return
		}
		addr, ok := netip.Addr{}, false
		if a, aerr := netip.ParseAddr(host); aerr == nil {
			addr, ok = a, true
		} else if a, legacy := parseLegacyIPv4(host); legacy {
			addr, ok = a, true
		}
		if !ok {
			if err != nil {
				t.Fatalf("ValidateDestination(%q) refused a name, which is judged only when dialled: %v", raw, err)
			}
			return
		}
		if wantBlocked := blocked(addr.Unmap().WithZone("")); wantBlocked != (err != nil) {
			t.Fatalf("ValidateDestination(%q): host %s reads as %s; refused = %v, want %v", raw, host, addr, err != nil, wantBlocked)
		}
	})
}

// FuzzParseLegacyIPv4 holds the hand-rolled inet_aton reader to the
// arithmetic it implements: an accepted spelling of one to four parts is the
// address whose leading parts are bytes and whose last part fills the rest.
func FuzzParseLegacyIPv4(f *testing.F) {
	for _, seed := range []string{"127.0.0.1", "2130706433", "0x7f.1", "017700000001", "127.1", "1.2.3", "08", "0x", "1..2", "256.1", "1.16777216"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, host string) {
		addr, ok := parseLegacyIPv4(host)
		if !ok {
			return
		}
		if !addr.Is4() {
			t.Fatalf("parseLegacyIPv4(%q) = %s, not an IPv4 address", host, addr)
		}
		parts := strings.Split(host, ".")
		if len(parts) < 1 || len(parts) > 4 {
			t.Fatalf("parseLegacyIPv4(%q) accepted %d parts", host, len(parts))
		}
		b := addr.As4()
		value := uint64(b[0])<<24 | uint64(b[1])<<16 | uint64(b[2])<<8 | uint64(b[3])
		// The value, recomputed from the parts by place: each leading part is
		// one byte from the top, the last is the rest.
		var want uint64
		for i, part := range parts {
			n, err := strconv.ParseUint(part, 0, 64)
			if err != nil {
				t.Fatalf("parseLegacyIPv4(%q) accepted a part strconv cannot read: %q", host, part)
			}
			if i < len(parts)-1 {
				want += n * (1 << (8 * (3 - i)))
			} else {
				want += n
			}
		}
		if want != value {
			t.Fatalf("parseLegacyIPv4(%q) = %s (%d), but its parts give %d", host, addr, value, want)
		}
		// A strict dotted quad reads as netip reads it.
		if std, err := netip.ParseAddr(host); err == nil && std != addr {
			t.Fatalf("parseLegacyIPv4(%q) = %s, netip reads %s", host, addr, std)
		}
		if again, ok := parseLegacyIPv4(addr.String()); !ok || again != addr {
			t.Fatalf("parseLegacyIPv4(%q) = %s, which does not read back as itself", host, addr)
		}
		if decimal, ok := parseLegacyIPv4(strconv.FormatUint(value, 10)); !ok || decimal != addr {
			t.Fatalf("parseLegacyIPv4(%q) = %s, but its one-part spelling %d reads as %s", host, addr, value, decimal)
		}
	})
}

// Every range the guard names is refused at its first and its last address,
// with the zero policy and as a URL literal.
func TestBlockedRanges_RefuseTheirEdges(t *testing.T) {
	var p DestinationPolicy
	for _, prefix := range blockedRanges {
		first := prefix.Masked().Addr()
		last := first
		bytes := last.AsSlice()
		for i := prefix.Bits(); i < len(bytes)*8; i++ {
			bytes[i/8] |= 1 << (7 - i%8)
		}
		last, _ = netip.AddrFromSlice(bytes)
		for _, addr := range []netip.Addr{first, last} {
			if !errors.Is(p.CheckAddr(addr), ErrDestinationBlocked) {
				t.Errorf("%s (edge of %s) is not refused", addr, prefix)
			}
			host := addr.String()
			if addr.Is6() {
				host = "[" + host + "]"
			}
			if _, err := p.ValidateDestination("http://" + host + "/"); !errors.Is(err, ErrDestinationBlocked) {
				t.Errorf("http://%s/ (edge of %s) = %v, want ErrDestinationBlocked", host, prefix, err)
			}
		}
	}
}
