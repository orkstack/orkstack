package dsl

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// OCI reference parsing, to exactly the depth §4 needs and no further.
//
// This is not a general reference library. It answers three questions — which
// registry, which repository, which tag or digest — because those are the
// three the resolver needs to build `GET /v2/<repository>/manifests/<ref>`.

// DefaultRegistry is the registry a reference with no host component means.
const DefaultRegistry = "docker.io"

// DefaultRegistryAPIHost is where docker.io's distribution API actually lives.
// The name in a reference and the host serving it are not the same string, and
// only this file may know that.
const DefaultRegistryAPIHost = "registry-1.docker.io"

// DefaultTag is the tag a reference with neither tag nor digest means.
const DefaultTag = "latest"

// digestPattern is the only digest spelling this contract uses. A reference
// carrying anything else is not pinned as far as OrkStack is concerned.
var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// ValidDigest reports whether s is a well-formed "sha256:<64 lowercase hex>".
func ValidDigest(s string) bool { return digestPattern.MatchString(s) }

// The reference grammar, from the OCI distribution specification's reference
// rules. A reference an Orkflow names reaches a registry as a URL and the
// container runtime as an argument, so ParseReference accepts exactly this and
// refuses the rest by name, never by repairing it: a registry that is a host
// name (or a bracketed IP literal) with an optional port, lower-case path
// components, a tag of word characters, dots and dashes that does not start
// with a dot or a dash, and a digest. Nothing it accepts can begin like a
// runtime option, carry a space or a control character, or change a registry
// URL's path, query or fragment.
var (
	domainComponent = `(?:[a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])`
	registryPattern = regexp.MustCompile(`^(?:` + domainComponent + `(?:\.` + domainComponent + `)*|\[[0-9a-fA-F:.]+\])(?::([0-9]{1,5}))?$`)
	pathComponent   = regexp.MustCompile(`^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$`)
	tagPattern      = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$`)
)

// maxReferencePart bounds a registry host and a repository path, the limits
// the distribution specification and Docker both hold names to.
const maxReferencePart = 255

// Reference is a parsed OCI reference.
type Reference struct {
	// Original is the string as written, verbatim.
	Original string
	// Registry is the resolved host as it appears in a reference, e.g.
	// "ghcr.io" or "docker.io".
	Registry string
	// Repository is the resolved path, e.g. "acme/migrate" or
	// "library/alpine".
	Repository string
	// Tag is the tag, empty when the reference is pinned by digest.
	Tag string
	// Digest is the pinned digest, empty when the reference names a tag.
	Digest string
}

// Target is the tag-or-digest that goes into the manifest URL.
func (r Reference) Target() string {
	if r.Digest != "" {
		return r.Digest
	}
	return r.Tag
}

// Pinned renders the only string that may be pulled or run:
// "{registry}/{repository}@{digest}" (§2.2 ImageRef.pinned).
func (r Reference) Pinned(digest string) string {
	return r.Registry + "/" + r.Repository + "@" + digest
}

// APIHost is the host to open a connection to, which is not always the host
// the reference names.
func (r Reference) APIHost() string {
	if r.Registry == DefaultRegistry {
		return DefaultRegistryAPIHost
	}
	return r.Registry
}

// ParseReference splits an OCI reference into its parts.
//
// The one genuinely subtle rule is telling a registry host from the first path
// component of a Docker Hub repository: `alpine:3.20` is Docker Hub's
// library/alpine, while `localhost:5001/x:3.20` is a host and a port. The test
// is the conventional one — a first component containing "." or ":", or equal
// to "localhost", is a host — and it is the same test every OCI client uses,
// which matters because disagreeing with them would mean resolving a different
// image than the runtime pulls.
func ParseReference(reference string) (Reference, error) {
	raw := strings.TrimSpace(reference)
	if raw == "" {
		return Reference{}, fmt.Errorf("empty image reference")
	}
	parsed := Reference{Original: reference}

	remainder := raw
	// A digest is separated by the LAST '@'; a tag may not contain one.
	if at := strings.LastIndex(remainder, "@"); at >= 0 {
		parsed.Digest = remainder[at+1:]
		remainder = remainder[:at]
		if !ValidDigest(parsed.Digest) {
			return Reference{}, fmt.Errorf("image reference %q: digest %q is not "+
				"\"sha256:<64 lowercase hex>\"", reference, parsed.Digest)
		}
	}

	// Split the host off before looking for a tag, because a host may carry a
	// port and a port looks exactly like a tag.
	name := remainder
	if slash := strings.Index(remainder, "/"); slash >= 0 {
		first := remainder[:slash]
		if first == "localhost" || strings.ContainsAny(first, ".:") {
			parsed.Registry = first
			name = remainder[slash+1:]
		}
	}
	if parsed.Registry == "" {
		parsed.Registry = DefaultRegistry
	}
	if err := checkRegistry(parsed.Registry); err != nil {
		return Reference{}, fmt.Errorf("image reference %q: %w", reference, err)
	}

	// Now a ':' can only be a tag separator.
	if colon := strings.LastIndex(name, ":"); colon >= 0 {
		parsed.Tag = name[colon+1:]
		name = name[:colon]
		if parsed.Tag == "" {
			return Reference{}, fmt.Errorf("image reference %q: empty tag", reference)
		}
		if !tagPattern.MatchString(parsed.Tag) {
			return Reference{}, fmt.Errorf("image reference %q: tag %q must match %s", reference, parsed.Tag, tagPattern)
		}
	}

	if name == "" {
		return Reference{}, fmt.Errorf("image reference %q: no repository", reference)
	}
	// Docker Hub's official images live under library/.
	if parsed.Registry == DefaultRegistry && !strings.Contains(name, "/") {
		name = "library/" + name
	}
	if err := checkRepository(name); err != nil {
		return Reference{}, fmt.Errorf("image reference %q: %w", reference, err)
	}
	parsed.Repository = name

	// Neither tag nor digest means the default tag. It is spelled out here
	// rather than left implicit, because the resolver puts this exact string
	// in a URL and "the registry will figure it out" is not true of `/v2/`.
	if parsed.Tag == "" && parsed.Digest == "" {
		parsed.Tag = DefaultTag
	}
	return parsed, nil
}

// checkRegistry holds a registry to a host name or bracketed IP literal with
// an optional port from 1 to 65535.
func checkRegistry(registry string) error {
	match := registryPattern.FindStringSubmatch(registry)
	if match == nil || len(registry) > maxReferencePart {
		return fmt.Errorf("registry %q must be a host name or a bracketed IP address, with an optional :port", registry)
	}
	if port := match[1]; port != "" {
		if n, _ := strconv.Atoi(port); n < 1 || n > 65535 {
			return fmt.Errorf("registry %q has the port %s, which is not from 1 to 65535", registry, port)
		}
	}
	return nil
}

// checkRepository holds a repository to lower-case path components.
func checkRepository(repository string) error {
	if len(repository) > maxReferencePart {
		return fmt.Errorf("repository of %d bytes exceeds the %d-byte limit", len(repository), maxReferencePart)
	}
	for _, component := range strings.Split(repository, "/") {
		if !pathComponent.MatchString(component) {
			return fmt.Errorf("repository %q: %q must be lower-case letters and digits joined by single separators (., _, __ or dashes) (%s)",
				repository, component, pathComponent)
		}
	}
	return nil
}

// IsLoopback reports whether the API host is a loopback address, which is the
// one case this resolver speaks plain HTTP to. A local `registry:3` from
// deploy/docker-compose.yml has no TLS, and requiring one would mean either a
// certificate ceremony in the dev stack (`make up`) or an "insecure registries" list that
// could be pointed at anything. Loopback is the narrow, non-configurable
// exception: nothing off this host can be reached through it.
func (r Reference) IsLoopback() bool {
	host := r.APIHost()
	if colon := strings.LastIndex(host, ":"); colon >= 0 {
		// Strip a port, but not the colons of a bare IPv6 literal.
		if !strings.Contains(host[colon+1:], "]") {
			host = host[:colon]
		}
	}
	host = strings.Trim(host, "[]")
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

// Scheme is the URL scheme to use for this reference's registry.
func (r Reference) Scheme() string {
	if r.IsLoopback() {
		return "http"
	}
	return "https"
}
