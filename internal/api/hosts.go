package api

import "github.com/orkstack/orkstack/internal/hostpolicy"

// HostListener names the API listener's settings for its Host policy
// (internal/hostpolicy): ORK_HTTP_ADDR, and ORK_ALLOWED_HOSTS
// (engine.allowed_hosts).
var HostListener = hostpolicy.Listener{
	Name: "engine", AddrVar: "ORK_HTTP_ADDR", AllowedVar: "ORK_ALLOWED_HOSTS", AllowedKey: "engine.allowed_hosts",
}

// NewHostPolicy is the API listener's Host policy for an engine listening on
// listenAddr (ORK_HTTP_ADDR, DefaultHTTPAddr when empty) with the further
// entries allowed (ORK_ALLOWED_HOSTS).
func NewHostPolicy(listenAddr string, allowed []string) (hostpolicy.Policy, error) {
	if listenAddr == "" {
		listenAddr = DefaultHTTPAddr
	}
	return hostpolicy.New(HostListener, listenAddr, allowed)
}
