// Package hostpolicy is the Host allowlist every OrkStack listener applies
// (the API listener's ORK_ALLOWED_HOSTS, the admin listener's
// ORK_ADMIN_ALLOWED_HOSTS): which names a request may give in its Host
// header. It stops DNS rebinding through a public name. A page on a domain
// its author controls, whose name is then made to resolve to the listener,
// reaches the listener with that domain as its Host and the listener's own
// origin as its Origin, so no origin check can tell it apart from a page
// the listener serves; only its Host can, and the policy refuses it.
package hostpolicy

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// Listener names the settings of the listener a Policy guards, so a refusal
// tells the operator which to change.
type Listener struct {
	// Name is how a refusal speaks of the listener: "engine", "admin
	// listener".
	Name string
	// AddrVar is the variable that sets the listener's address
	// (ORK_HTTP_ADDR).
	AddrVar string
	// AllowedVar and AllowedKey are its allowlist's variable and
	// configuration key (ORK_ALLOWED_HOSTS, engine.allowed_hosts).
	AllowedVar, AllowedKey string
}

// Policy is one listener's Host allowlist. Whatever its setting says, it
// admits the names a rebinding page cannot give:
//
//   - an IP address, v4 or v6: a rebinding page's Host is a name;
//   - localhost and every name under it, which no public DNS answers for
//     (RFC 6761);
//   - a name with no dot, written without a trailing one: a quickstart
//     service (ork), a Kubernetes Service in its own namespace, a host's
//     short name. Such a name resolves only on the local network, while a
//     name a page's author can make resolve where they like is one they
//     registered in public DNS, and every such name has a dot. A dotless
//     name a browser reads as an IPv4 address (2130706433, 0x7f000001) is
//     not one: a browser sends that address dotted, so only a client that
//     is not a browser sends it, and it is refused;
//   - the host the listener's address names, when it names one;
//   - an empty Host, which no browser sends.
//
// The setting adds to them: a name, *.suffix for every name under a suffix
// of two labels or more (never the suffix itself), or * for any Host. Names
// compare without their port, their case or a trailing dot.
type Policy struct {
	listener Listener
	any      bool
	names    map[string]bool
	suffixes []string
	// clock is the time Guard's refusal log is throttled by; nil is
	// time.Now. Only a test sets it.
	clock func() time.Time
}

// New builds the policy of the listener l on listenAddr with the further
// entries allowed. An entry that is neither a name nor a pattern, a pattern
// over a top-level name (*.com), and a listen address that is not host:port
// are refused, naming the setting.
func New(l Listener, listenAddr string, allowed []string) (Policy, error) {
	policy := Policy{listener: l, names: map[string]bool{"localhost": true}, suffixes: []string{".localhost"}}
	if listenAddr != "" {
		listenHost, _, err := net.SplitHostPort(listenAddr)
		if err != nil {
			return Policy{}, fmt.Errorf("%s=%q is not host:port: %w", l.AddrVar, listenAddr, err)
		}
		if name, _, _ := hostName(listenHost); name != "" && !isAddress(name) {
			policy.names[name] = true
		}
	}
	for _, raw := range allowed {
		entry := asciiLower(strings.TrimSpace(raw))
		switch {
		case entry == "":
		case entry == "*":
			policy.any = true
		case isAddress(entry) || strings.HasPrefix(entry, "[") && strings.HasSuffix(entry, "]") && isAddress(entry[1:len(entry)-1]):
			// An address is admitted already; listing one changes nothing.
		case strings.HasPrefix(entry, "*."):
			parent := strings.TrimSuffix(entry[len("*."):], ".")
			switch {
			case !validName(parent):
				return Policy{}, l.refusedEntry(raw)
			case parent == "localhost":
				// Every name under localhost is admitted already.
			case !strings.Contains(parent, "."):
				return Policy{}, fmt.Errorf("%s entry %q admits every name under the top-level name %q, "+
					"any of which a page's author may register: list each name, or a suffix of two labels "+
					"or more, e.g. *.example.com", l.AllowedVar, raw, parent)
			default:
				policy.suffixes = append(policy.suffixes, "."+parent)
			}
		default:
			name := strings.TrimSuffix(entry, ".")
			if !validName(name) {
				return Policy{}, l.refusedEntry(raw)
			}
			policy.names[name] = true
		}
	}
	return policy, nil
}

func (l Listener) refusedEntry(entry string) error {
	return fmt.Errorf("%s entry %q is not a host name, *.suffix or *: "+
		"list each name without a scheme, a port or a path, e.g. orkstack.example.com or *.example.com", l.AllowedVar, entry)
}

// validName reports whether name is a DNS name: dot-separated labels of
// ASCII letters, digits, hyphens and underscores, none empty and none
// opening or closing with a hyphen.
func validName(name string) bool {
	if name == "" || len(name) > 253 {
		return false
	}
	for label := range strings.SplitSeq(name, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := range len(label) {
			c := label[i]
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
				return false
			}
		}
	}
	return true
}

// isNumber reports whether a dotless name is one a browser's URL parser
// reads as an IPv4 address (WHATWG URL, "IPv4 number parser"): decimal or
// octal digits, or 0x and hexadecimal digits.
func isNumber(name string) bool {
	digits, hex := name, strings.HasPrefix(name, "0x")
	if hex {
		digits = name[len("0x"):]
	} else if name == "" {
		return false
	}
	for i := range len(digits) {
		c := digits[i]
		if !(c >= '0' && c <= '9' || hex && c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// asciiLower folds the ASCII letters of s to lower case and leaves every
// other rune as it is. A string with no upper-case ASCII letter is returned
// as it is, without a copy.
func asciiLower(s string) string {
	for i := range len(s) {
		if s[i] >= 'A' && s[i] <= 'Z' {
			return strings.Map(func(r rune) rune {
				if r >= 'A' && r <= 'Z' {
					return r + ('a' - 'A')
				}
				return r
			}, s)
		}
	}
	return s
}

// isAddress reports whether host is an IP address, a zone allowed. A name
// that cannot be one (no colon, and not four dotted numbers) is answered
// without parsing, so judging it allocates no error.
func isAddress(host string) bool {
	if !strings.Contains(host, ":") && (strings.Trim(host, "0123456789.") != "" || strings.Count(host, ".") != 3) {
		return false
	}
	_, err := netip.ParseAddr(host)
	return err == nil
}

// hostName is a Host header's name: without its port or the brackets of an
// IPv6 address, in lower case, without a trailing dot. qualified reports
// whether it had one, and ok whether the port, if any, is a number. Case is
// folded in ASCII only, as DNS compares names (RFC 4343); a browser sends a
// name outside ASCII as punycode.
func hostName(host string) (name string, qualified, ok bool) {
	// Only a Host that can carry a port is split: a failed split allocates
	// its error.
	canSplit := strings.Count(host, ":") == 1
	if strings.HasPrefix(host, "[") {
		canSplit = strings.Contains(host, "]:")
	}
	split := false
	if canSplit {
		if h, port, err := net.SplitHostPort(host); err == nil {
			for i := range len(port) {
				if port[i] < '0' || port[i] > '9' {
					return "", false, false
				}
			}
			host, split = h, true
		}
	}
	if !split && strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	host = asciiLower(host)
	if strings.HasSuffix(host, ".") {
		return strings.TrimSuffix(host, "."), true, true
	}
	return host, false, true
}

// Allows reports whether the policy admits a request whose Host header is
// host. A Host whose name is neither an address nor a DNS name is refused
// unless the policy admits any.
func (p Policy) Allows(host string) bool {
	if p.any {
		return true
	}
	name, qualified, ok := hostName(host)
	switch {
	case host == "":
		return true
	case !ok:
		return false
	case isAddress(name):
		return true
	case !validName(name):
		return false
	case p.names[name]:
		return true
	case !strings.Contains(name, ".") && !qualified:
		return !isNumber(name)
	}
	for _, suffix := range p.suffixes {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

const (
	// maxLoggedHost bounds how much of a refused Host is logged and answered.
	maxLoggedHost = 255
	// refusalLogInterval is the least time between two refusal log lines: a
	// page or a misdirected probe that loops is one line per interval, each
	// counting the refusals it did not log.
	refusalLogInterval = 10 * time.Second
	// RefusalPolicy is the content security policy of a refusal, the one
	// every response but the UI's page carries (internal/webui FilePolicy):
	// it loads and runs nothing, and nothing may frame it.
	RefusalPolicy = "default-src 'none'; frame-ancestors 'none'"
)

// Guard serves next only for a request whose Host the policy admits. Any
// other is refused 403 before a route sees it, answered with the Host and the
// setting that admits it — as a Connect error on a Connect path, so a client
// reads the reason like any other error, and as plain text anywhere else, the
// UI and the sockets included — under the headers every other error carries,
// and logged, at most once a refusalLogInterval.
func (p Policy) Guard(next http.Handler, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	l := p.listener
	now := time.Now
	if p.clock != nil {
		now = p.clock
	}
	var (
		mu         sync.Mutex
		lastLogged time.Time
		unlogged   int
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.Allows(r.Host) {
			next.ServeHTTP(w, r)
			return
		}
		host := r.Host
		if len(host) > maxLoggedHost {
			host = host[:maxLoggedHost] + "…"
		}
		message := fmt.Sprintf("this %s does not answer to Host %q: add the name it is reached by to "+
			"%s (%s); without it, it answers an IP address, localhost and *.localhost, a name without a dot, "+
			"and the host %s names", l.Name, host, l.AllowedVar, l.AllowedKey, l.AddrVar)
		mu.Lock()
		at := now()
		logIt, skipped := at.Sub(lastLogged) >= refusalLogInterval, unlogged
		if logIt {
			lastLogged, unlogged = at, 0
		} else {
			unlogged++
		}
		mu.Unlock()
		if logIt {
			logger.Warn("request refused: Host not allowed",
				"listener", l.Name, "host", host, "remote", r.RemoteAddr, "refused_unlogged", skipped,
				"hint", "a name with a dot the "+l.Name+" is reached by (a proxy that keeps the browser's Host, "+
					"a Kubernetes Service name, an FQDN) must be listed in "+l.AllowedVar)
		}
		header := w.Header()
		header.Set("Cache-Control", "no-store")
		header.Set("X-Content-Type-Options", "nosniff")
		header.Set("Content-Security-Policy", RefusalPolicy)
		if strings.HasPrefix(r.URL.Path, "/orkstack.") {
			header.Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "permission_denied", "message": message})
			return
		}
		http.Error(w, message, http.StatusForbidden)
	})
}
