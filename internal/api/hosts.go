package api

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

// HostPolicy is the API listener's Host allowlist (ORK_ALLOWED_HOSTS,
// engine.allowed_hosts): which names a request may give in its Host header.
// It stops DNS rebinding through a public name. A page on a domain its
// author controls, whose
// name is then made to resolve to the engine, reaches the engine with that
// domain as its Host and the engine's own origin as its Origin, so no
// origin check can tell it apart from the UI; only its Host can, and the
// policy refuses it.
//
// Whatever the setting says, the policy admits the names such a page cannot
// give:
//
//   - an IP address, v4 or v6: a rebinding page's Host is a name;
//   - localhost and every name under it, which no public DNS answers for
//     (RFC 6761);
//   - a name with no dot, written without a trailing one: a quickstart
//     service (ork), a Kubernetes Service in its own namespace, a host's
//     short name. Such a name resolves only on the local network, while a
//     name a page's author can make resolve where they like is one they
//     registered in public DNS, and every such name has a dot;
//   - the host ORK_HTTP_ADDR names, when it names one;
//   - an empty Host, which no browser sends.
//
// The setting adds to them: a name, *.suffix for every name under suffix
// (never suffix itself), or * for any Host. Names compare without their port,
// their case or a trailing dot.
type HostPolicy struct {
	any      bool
	names    map[string]bool
	suffixes []string
}

// NewHostPolicy builds the policy for an engine listening on listenAddr
// (ORK_HTTP_ADDR) with the further entries allowed (ORK_ALLOWED_HOSTS). An
// entry that is neither a name nor a pattern is refused, naming it.
func NewHostPolicy(listenAddr string, allowed []string) (HostPolicy, error) {
	policy := HostPolicy{names: map[string]bool{"localhost": true}, suffixes: []string{".localhost"}}
	if listenAddr == "" {
		listenAddr = DefaultHTTPAddr
	}
	listenHost, _, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return HostPolicy{}, fmt.Errorf("ORK_HTTP_ADDR=%q is not host:port: %w", listenAddr, err)
	}
	if name, _, _ := hostName(listenHost); name != "" && !isAddress(name) {
		policy.names[name] = true
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
			if !validName(parent) {
				return HostPolicy{}, refusedHostEntry(raw)
			}
			policy.suffixes = append(policy.suffixes, "."+parent)
		default:
			name := strings.TrimSuffix(entry, ".")
			if !validName(name) {
				return HostPolicy{}, refusedHostEntry(raw)
			}
			policy.names[name] = true
		}
	}
	return policy, nil
}

func refusedHostEntry(entry string) error {
	return fmt.Errorf("ORK_ALLOWED_HOSTS entry %q is not a host name, *.suffix or *: "+
		"list each name without a scheme, a port or a path, e.g. orkstack.example.com or *.example.com", entry)
}

// validName reports whether name is a DNS name: dot-separated labels of
// ASCII letters, digits, hyphens and underscores, none empty and none
// opening or closing with a hyphen.
func validName(name string) bool {
	if name == "" || len(name) > 253 {
		return false
	}
	for _, label := range strings.Split(name, ".") {
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

// asciiLower folds the ASCII letters of s to lower case and leaves every
// other rune as it is.
func asciiLower(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, s)
}

// isAddress reports whether host is an IP address, a zone allowed.
func isAddress(host string) bool {
	_, err := netip.ParseAddr(host)
	return err == nil
}

// hostName is a Host header's name: without its port or the brackets of an
// IPv6 address, in lower case, without a trailing dot. qualified reports
// whether it had one, and ok whether the port, if any, is a number. Case is
// folded in ASCII only, as DNS compares names (RFC 4343); a browser sends a
// name outside ASCII as punycode.
func hostName(host string) (name string, qualified, ok bool) {
	if h, port, err := net.SplitHostPort(host); err == nil {
		for i := range len(port) {
			if port[i] < '0' || port[i] > '9' {
				return "", false, false
			}
		}
		host = h
	} else if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
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
func (p HostPolicy) Allows(host string) bool {
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
		return true
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
)

// Guard serves next only for a request whose Host the policy admits. Any
// other is refused 403 before a route sees it, answered with the Host and the
// setting that admits it — as a Connect error on a Connect path, so a client
// reads the reason like any other error, and as plain text anywhere else, the
// UI and the sockets included — and logged, at most once a
// refusalLogInterval.
func (p HostPolicy) Guard(next http.Handler, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
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
		message := fmt.Sprintf("this engine does not answer to Host %q: add the name it is reached by to "+
			"ORK_ALLOWED_HOSTS (engine.allowed_hosts); without it, it answers an IP address, localhost "+
			"and *.localhost, a name without a dot, and the host ORK_HTTP_ADDR names", host)
		mu.Lock()
		now := time.Now()
		logIt, skipped := now.Sub(lastLogged) >= refusalLogInterval, unlogged
		if logIt {
			lastLogged, unlogged = now, 0
		} else {
			unlogged++
		}
		mu.Unlock()
		if logIt {
			logger.Warn("request refused: Host not allowed",
				"host", host, "remote", r.RemoteAddr, "refused_unlogged", skipped,
				"hint", "a name with a dot the engine is reached by (a proxy that keeps the browser's Host, a Kubernetes Service name, an FQDN) must be listed in ORK_ALLOWED_HOSTS")
		}
		header := w.Header()
		header.Set("Cache-Control", "no-store")
		header.Set("X-Content-Type-Options", "nosniff")
		if strings.HasPrefix(r.URL.Path, "/orkstack.") {
			header.Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "permission_denied", "message": message})
			return
		}
		http.Error(w, message, http.StatusForbidden)
	})
}
