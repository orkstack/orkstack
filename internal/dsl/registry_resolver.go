package dsl

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Apply-time image resolution (§4.3).
//
// WHERE THIS RUNS. Not here. This file defines the resolver; the only thing
// that calls it is internal/api.ApplyOrkflow, between Compile and Put. Compile
// stays pure — no network, no clock — because every hermetic compiler test and
// TestCompile_AcceptsEveryShippedExample depend on it. A resolver reachable
// from Compile would make compiling a document a network operation, and
// `orkctl orkflow validate` would need a registry.
//
// THERE IS EXACTLY ONE RESOLVER. `docker image inspect .RepoDigests` reports
// whatever the daemon negotiated for the platform it pulled, and `docker
// manifest inspect --verbose` does not reliably surface a parent index digest,
// so the daemon is not the resolver. Neither is `docker buildx imagetools
// inspect`: a second resolver can disagree with the first about `platforms`
// and therefore about the definition digest, and §4.4's whole argument is that
// one definition has one digest. Resolution goes to the registry over the
// distribution API and nowhere else.

// Reason codes for a resolution that failed. They are Error.code strings and
// reach the author verbatim.
const (
	ReasonImageNotFound                  = "IMAGE_NOT_FOUND"
	ReasonImageResolveUnauthorized       = "IMAGE_RESOLVE_UNAUTHORIZED"
	ReasonImageResolveUnavailable        = "IMAGE_RESOLVE_UNAVAILABLE"
	ReasonImageDigestMismatch            = "IMAGE_DIGEST_MISMATCH"
	ReasonImageResolveUnsupported        = "IMAGE_RESOLVE_UNSUPPORTED"
	defaultResolveTimeout                = 30 * time.Second
	maxManifestBytes               int64 = 4 << 20
)

// ResolveError is a resolution that failed, carrying its reason code.
type ResolveError struct {
	Reference string
	Code      string
	Detail    string
	Err       error
}

func (e *ResolveError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("resolve image %q: %s: %s: %v", e.Reference, e.Code, e.Detail, e.Err)
	}
	return fmt.Sprintf("resolve image %q: %s: %s", e.Reference, e.Code, e.Detail)
}

func (e *ResolveError) Unwrap() error { return e.Err }

// Media types. The Accept set is a CONTRACT, not a convenience: OCI
// distribution content negotiation means a different Accept set can select a
// different object, so resolving with one set and pulling with another can pin
// a digest the runtime never sees. These are ordered index-first so a
// multi-platform tag resolves to its index rather than to one platform's
// manifest.
const (
	MediaTypeOCIIndex           = "application/vnd.oci.image.index.v1+json"
	MediaTypeDockerManifestList = "application/vnd.docker.distribution.manifest.list.v2+json"
	MediaTypeOCIManifest        = "application/vnd.oci.image.manifest.v1+json"
	MediaTypeDockerManifest     = "application/vnd.docker.distribution.manifest.v2+json"
)

// ManifestAccept is the exact Accept header value used at resolve time.
var ManifestAccept = strings.Join([]string{
	MediaTypeOCIIndex,
	MediaTypeDockerManifestList,
	MediaTypeOCIManifest,
	MediaTypeDockerManifest,
}, ", ")

// Resolver turns a registry reference into a pinned ImageRef.
type Resolver struct {
	// Client is the HTTP client. Nil uses a client with a bounded timeout.
	Client *http.Client
	// AuthFile is ORK_REGISTRY_AUTH_FILE: a Docker config.json, used ONLY
	// for manifest and config-blob GETs (§4.5). It grants no push and no
	// execution.
	AuthFile string

	// authMu guards the lazily read auth file: a Resolver is shared by
	// every concurrent apply and, as a SignatureVerifier's client, by every
	// concurrent Orker attempt.
	authMu   sync.Mutex
	auth     map[string]dockerAuth
	authRead bool
}

// NewResolver builds a resolver reading credentials from authFile.
func NewResolver(authFile string) *Resolver {
	return &Resolver{
		Client:   &http.Client{Timeout: defaultResolveTimeout},
		AuthFile: authFile,
	}
}

func (r *Resolver) client() *http.Client {
	if r.Client != nil {
		return r.Client
	}
	return &http.Client{Timeout: defaultResolveTimeout}
}

// Resolution is one reference, resolved: the pinned ImageRef and what the
// image declares about its with: block.
type Resolution struct {
	Image *orkstackv1.ImageRef
	// Interface is the parsed label, or nil when the image carries none or
	// InterfaceError says why it could not be read.
	Interface *orkstackv1.ActivityInterface
	// InterfaceError is ActivityInfo.interface_error, verbatim.
	InterfaceError string
}

// Resolutions is ResolveOrkflow's answer, keyed by the task's `uses:` —
// the same key ResolveOrkflow already de-duplicates the manifest GET on.
type Resolutions map[string]*Resolution

// ResolveOrkflow resolves every distinct `uses:` in wf, stamps Task.image,
// and returns what each image declares about its with: block (§4.3 step 7).
//
// Distinct, because a document with twelve `uses: shell` tasks is one image
// and should be one GET. Sharing the resolved message across tasks is safe:
// the ImageRef is written once and never mutated, and proto.Clone would only
// add allocations and a way for two tasks to drift.
func (r *Resolver) ResolveOrkflow(ctx context.Context, catalog Catalog, wf *orkstackv1.Orkflow) (Resolutions, error) {
	resolved := make(Resolutions)
	for _, t := range wf.GetTasks() {
		// An awaited task has no `uses:` and gets no Task.image: it runs no
		// container (docs/DSL.md rules 6 and 15).
		if t.GetAwait() != nil {
			continue
		}
		// A composed task names an Orkflow, not an image: its child's own
		// tasks were resolved when the child was applied (rule 26). No
		// manifest GET for `orkflow:…`.
		if IsComposedTask(t) {
			continue
		}
		uses := t.GetUses()
		if res, ok := resolved[uses]; ok {
			t.Image = res.Image
			continue
		}
		reference, err := catalog.ResolveUses(t.GetId(), uses)
		if err != nil {
			return nil, err
		}
		res, err := r.Inspect(ctx, uses, reference)
		if err != nil {
			return nil, err
		}
		resolved[uses] = res
		t.Image = res.Image
	}
	return resolved, nil
}

// Resolve is Inspect's ImageRef. Kept for the callers that want only the pin.
func (r *Resolver) Resolve(ctx context.Context, uses, reference string) (*orkstackv1.ImageRef, error) {
	res, err := r.Inspect(ctx, uses, reference)
	if err != nil {
		return nil, err
	}
	return res.Image, nil
}

// Inspect issues the manifest GET (§4.3 steps 2–6) and then reads the
// image's interface label from its config blob (step 7).
//
// `uses` is what the author wrote and becomes ImageRef.reference verbatim;
// `reference` is what the catalog turned it into and is what gets fetched.
// Keeping both is what makes `uses: shell` explainable after the fact.
func (r *Resolver) Inspect(ctx context.Context, uses, reference string) (*Resolution, error) {
	parsed, err := ParseReference(reference)
	if err != nil {
		return nil, &ResolveError{Reference: reference, Code: ReasonImageResolveUnsupported,
			Detail: "cannot be parsed as an OCI reference", Err: err}
	}

	// One credential per Inspect: the token the manifest GET earns is reused
	// for the platform-manifest and config-blob GETs of step 7.
	var authorization string
	body, contentType, headerDigest, err := r.getManifest(ctx, parsed, parsed.Target(), &authorization)
	if err != nil {
		return nil, err
	}

	// Step 3: the digest is computed over the EXACT response bytes, and
	// checked against Docker-Content-Digest when the registry sent one. The
	// distribution spec makes that verification mandatory, and it is the only
	// thing standing between a pinned digest and a proxy that rewrote the
	// body on the way through.
	sum := sha256.Sum256(body)
	computed := "sha256:" + hex.EncodeToString(sum[:])
	if headerDigest != "" && !strings.EqualFold(headerDigest, computed) {
		return nil, &ResolveError{Reference: reference, Code: ReasonImageDigestMismatch,
			Detail: fmt.Sprintf("registry sent Docker-Content-Digest %s but the response body hashes to %s",
				headerDigest, computed)}
	}
	// Step 5: an already-pinned reference still gets the GET, and the body
	// must hash to the pin. Apply MUST NOT succeed with an unpullable pin —
	// a digest nobody can fetch is not a pin, it is a promise.
	if parsed.Digest != "" && !strings.EqualFold(parsed.Digest, computed) {
		return nil, &ResolveError{Reference: reference, Code: ReasonImageDigestMismatch,
			Detail: fmt.Sprintf("reference pins %s but the registry served a body hashing to %s",
				parsed.Digest, computed)}
	}

	platforms, err := indexPlatforms(body, contentType)
	if err != nil {
		return nil, &ResolveError{Reference: reference, Code: ReasonImageResolveUnsupported,
			Detail: "manifest body is not valid JSON for its content type", Err: err}
	}

	res := &Resolution{Image: &orkstackv1.ImageRef{
		Reference:   uses,
		Registry:    parsed.Registry,
		Repository:  parsed.Repository,
		IndexDigest: computed,
		Pinned:      parsed.Pinned(computed),
		MediaType:   contentType,
		Platforms:   platforms,
	}}
	if err := r.readInterface(ctx, parsed, body, contentType, &authorization, res); err != nil {
		return nil, err
	}
	return res, nil
}

// errNoPlatformManifest is InterfaceError for an index step 7a cannot read a
// config through.
const errNoPlatformManifest = "index advertises no platform manifest to read a config from"

// readInterface is §4.3 step 7: locate the image manifest, read its config
// blob, and parse the interface label from config.Labels.
//
// A failure to FETCH (7a–7d) fails resolution: a registry that serves a
// manifest but not its config cannot serve a pull, and apply must not pin a
// digest nobody can pull (step 5). A label that cannot be PARSED never fails
// resolution (7e) — the image is still pinned and runnable, and only the
// apply-time check is lost, which InterfaceError reports.
func (r *Resolver) readInterface(ctx context.Context, ref Reference, body []byte, contentType string,
	authorization *string, res *Resolution,
) error {
	manifest := body
	switch mediaTypeOf(contentType) {
	case MediaTypeOCIIndex, MediaTypeDockerManifestList:
		digest, ok := selectPlatformManifest(body)
		if !ok {
			res.InterfaceError = errNoPlatformManifest
			return nil
		}
		platform, platformType, _, err := r.getManifest(ctx, ref, digest, authorization)
		if err != nil {
			return err
		}
		if got := digestOf(platform); !strings.EqualFold(got, digest) {
			return &ResolveError{Reference: ref.Original, Code: ReasonImageDigestMismatch,
				Detail: fmt.Sprintf("index names platform manifest %s but the registry served a body hashing to %s",
					digest, got)}
		}
		switch mediaTypeOf(platformType) {
		case MediaTypeOCIIndex, MediaTypeDockerManifestList:
			res.InterfaceError = errNoPlatformManifest
			return nil
		}
		manifest = platform
	}

	configDigest, err := configDescriptor(manifest)
	if err != nil {
		return &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnsupported,
			Detail: "manifest has no readable config descriptor", Err: err}
	}
	config, err := r.getBlob(ctx, ref, configDigest, authorization)
	if err != nil {
		return err
	}
	if got := digestOf(config); !strings.EqualFold(got, configDigest) {
		return &ResolveError{Reference: ref.Original, Code: ReasonImageDigestMismatch,
			Detail: fmt.Sprintf("manifest names config %s but the registry served a blob hashing to %s",
				configDigest, got)}
	}
	value, present, err := labelOf(config, InterfaceLabel)
	if err != nil {
		return &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnsupported,
			Detail: "config blob is not valid JSON", Err: err}
	}
	if !present {
		return nil
	}
	iface, err := ParseInterfaceLabel(value)
	if err != nil {
		res.InterfaceError = err.Error()
		return nil
	}
	res.Interface = iface
	return nil
}

func digestOf(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// sha256DigestPattern is a content digest this resolver can verify.
var sha256DigestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

// selectPlatformManifest picks the FIRST entry, in index order, whose
// platform.os is present and is not "unknown" — buildx attestation manifests
// carry os: unknown and are skipped. It reports false when no entry
// qualifies, when the selected entry is itself an index, or when it names no
// sha256 digest this resolver could verify.
func selectPlatformManifest(index []byte) (string, bool) {
	var parsed struct {
		Manifests []struct {
			MediaType string `json:"mediaType"`
			Digest    string `json:"digest"`
			Platform  *struct {
				OS string `json:"os"`
			} `json:"platform"`
		} `json:"manifests"`
	}
	if err := json.Unmarshal(index, &parsed); err != nil {
		return "", false
	}
	for _, m := range parsed.Manifests {
		if m.Platform == nil || m.Platform.OS == "" || m.Platform.OS == "unknown" {
			continue
		}
		switch mediaTypeOf(m.MediaType) {
		case MediaTypeOCIIndex, MediaTypeDockerManifestList:
			return "", false
		}
		if !sha256DigestPattern.MatchString(m.Digest) {
			return "", false
		}
		return m.Digest, true
	}
	return "", false
}

// configDescriptor reads config.digest from an image manifest.
func configDescriptor(manifest []byte) (string, error) {
	var parsed struct {
		Config *struct {
			Digest string `json:"digest"`
		} `json:"config"`
	}
	if err := json.Unmarshal(manifest, &parsed); err != nil {
		return "", err
	}
	if parsed.Config == nil {
		return "", errors.New("the manifest has no config")
	}
	if !sha256DigestPattern.MatchString(parsed.Config.Digest) {
		return "", fmt.Errorf("config.digest %q is not sha256:<64 hex>", parsed.Config.Digest)
	}
	return parsed.Config.Digest, nil
}

// labelOf reads config.Labels[name] from an OCI image config (Docker's
// config uses the same path).
func labelOf(config []byte, name string) (string, bool, error) {
	var parsed struct {
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"config"`
	}
	if err := json.Unmarshal(config, &parsed); err != nil {
		return "", false, err
	}
	value, present := parsed.Config.Labels[name]
	return value, present, nil
}

// indexPlatforms reads manifests[].platform for an index, and returns nothing
// for a single manifest (§4.3 step 4).
//
// The order is the INDEX'S OWN, not sorted. §4.4 hashes this slice into the
// definition digest, so the order is part of the contract; index order is what
// the registry returned and needs no second rule to agree on.
func indexPlatforms(body []byte, contentType string) ([]string, error) {
	switch mediaTypeOf(contentType) {
	case MediaTypeOCIIndex, MediaTypeDockerManifestList:
	default:
		// Any single-manifest media type: platforms stays empty, as §2.2
		// already specifies. This is not a failure — a single-arch image is
		// an ordinary thing to pin.
		return nil, nil
	}
	var index struct {
		Manifests []struct {
			Platform struct {
				OS           string `json:"os"`
				Architecture string `json:"architecture"`
				Variant      string `json:"variant"`
			} `json:"platform"`
		} `json:"manifests"`
	}
	if err := json.Unmarshal(body, &index); err != nil {
		return nil, err
	}
	var platforms []string
	for _, m := range index.Manifests {
		p := m.Platform
		if p.OS == "" && p.Architecture == "" {
			continue
		}
		rendered := p.OS + "/" + p.Architecture
		if p.Variant != "" {
			rendered += "/" + p.Variant
		}
		platforms = append(platforms, rendered)
	}
	return platforms, nil
}

// mediaTypeOf strips any parameters from a Content-Type.
func mediaTypeOf(contentType string) string {
	if semi := strings.Index(contentType, ";"); semi >= 0 {
		contentType = contentType[:semi]
	}
	return strings.TrimSpace(strings.ToLower(contentType))
}

// getManifest performs the GET, including one bearer-token round trip.
//
// It must be a GET and not a HEAD, and this is not a preference. §4.4 hashes
// ImageRef.platforms into the definition digest, and platforms comes from the
// index's manifests[].platform array — which lives in the response BODY. HTTP
// forbids content on a HEAD response, and the OCI spec's HEAD requires only
// Docker-Content-Digest and Content-Length. A HEAD-only resolver would leave
// platforms empty where a GET-based one fills it, and the two would mint
// DIFFERENT definition digests for the same index.
func (r *Resolver) getManifest(ctx context.Context, ref Reference, target string, authorization *string) ([]byte, string, string, error) {
	endpoint := fmt.Sprintf("%s://%s/v2/%s/manifests/%s",
		ref.Scheme(), ref.APIHost(), ref.Repository, target)

	resp, body, err := r.authorizedGet(ctx, ref, endpoint, authorization)
	if err != nil {
		return nil, "", "", err
	}

	switch {
	case resp.StatusCode == http.StatusOK:
		if int64(len(body)) > maxManifestBytes {
			// Kept at the bound as before: a manifest past it hashes to
			// something the registry did not serve and is refused as a
			// mismatch or as unparseable, exactly as it always was.
			body = body[:maxManifestBytes]
		}
		return body, resp.Header.Get("Content-Type"), resp.Header.Get("Docker-Content-Digest"), nil
	case resp.StatusCode == http.StatusNotFound:
		return nil, "", "", &ResolveError{Reference: ref.Original, Code: ReasonImageNotFound,
			Detail: fmt.Sprintf("registry %s has no manifest %s for %s",
				ref.APIHost(), target, ref.Repository)}
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		return nil, "", "", &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnauthorized,
			Detail: fmt.Sprintf("registry %s refused the manifest read with %d; "+
				"set ORK_REGISTRY_AUTH_FILE to a docker config.json with credentials for %s",
				ref.APIHost(), resp.StatusCode, ref.Registry)}
	case resp.StatusCode >= 500:
		return nil, "", "", &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnavailable,
			Detail: fmt.Sprintf("registry %s answered %d", ref.APIHost(), resp.StatusCode)}
	default:
		return nil, "", "", &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnsupported,
			Detail: fmt.Sprintf("registry %s answered %d: %s",
				ref.APIHost(), resp.StatusCode, strings.TrimSpace(string(body)))}
	}
}

// getBlob reads one blob — the image config of §4.3 step 7c — with the
// credential the manifest GET earned. Registries commonly redirect a blob to
// a CDN; the client follows, and drops Authorization on a cross-host hop.
func (r *Resolver) getBlob(ctx context.Context, ref Reference, digest string, authorization *string) ([]byte, error) {
	endpoint := fmt.Sprintf("%s://%s/v2/%s/blobs/%s",
		ref.Scheme(), ref.APIHost(), ref.Repository, digest)

	resp, body, err := r.authorizedGet(ctx, ref, endpoint, authorization)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		if int64(len(body)) > maxManifestBytes {
			return nil, &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnsupported,
				Detail: fmt.Sprintf("config blob %s is larger than %d bytes", digest, maxManifestBytes)}
		}
		return body, nil
	case resp.StatusCode == http.StatusNotFound:
		return nil, &ResolveError{Reference: ref.Original, Code: ReasonImageNotFound,
			Detail: fmt.Sprintf("registry %s has no blob %s for %s", ref.APIHost(), digest, ref.Repository)}
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		return nil, &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnauthorized,
			Detail: fmt.Sprintf("registry %s refused the config blob read with %d; "+
				"set ORK_REGISTRY_AUTH_FILE to a docker config.json with credentials for %s",
				ref.APIHost(), resp.StatusCode, ref.Registry)}
	case resp.StatusCode >= 500:
		return nil, &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnavailable,
			Detail: fmt.Sprintf("registry %s answered %d for blob %s", ref.APIHost(), resp.StatusCode, digest)}
	default:
		return nil, &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnsupported,
			Detail: fmt.Sprintf("registry %s answered %d for blob %s: %s",
				ref.APIHost(), resp.StatusCode, digest, strings.TrimSpace(string(body)))}
	}
}

// authorizedGet performs one GET with *authorization and, on a 401 carrying
// a Bearer challenge, exactly one token handshake and retry. The token it
// earns is written back through authorization, so one Inspect performs the
// handshake once and every later GET reuses it.
func (r *Resolver) authorizedGet(ctx context.Context, ref Reference, endpoint string, authorization *string) (*http.Response, []byte, error) {
	resp, body, err := r.do(ctx, endpoint, *authorization)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		// The registry told us how to authenticate. Anonymous pulls from
		// Docker Hub and ghcr.io both arrive here on the first request, so
		// this path is the common case rather than an error path.
		if challenge := parseBearerChallenge(resp.Header.Get("WWW-Authenticate")); challenge != nil {
			token, tokenErr := r.fetchToken(ctx, ref, challenge)
			if tokenErr != nil {
				return nil, nil, tokenErr
			}
			if token != "" {
				*authorization = "Bearer " + token
				resp, body, err = r.do(ctx, endpoint, *authorization)
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}
	return resp, body, nil
}

// do issues one GET and reads a bounded body.
func (r *Resolver) do(ctx context.Context, endpoint, authorization string) (*http.Response, []byte, error) {
	// One byte past the bound, so a caller can tell "at the bound" from
	// "over it" (a config blob over it is refused, not truncated).
	return r.doWith(ctx, r.client(), endpoint, authorization, ManifestAccept, maxManifestBytes+1)
}

// doWith is do over an explicit client, Accept header and body bound, for
// the blob GET signature verification makes (getSignatureBlob).
func (r *Resolver) doWith(
	ctx context.Context, client *http.Client, endpoint, authorization, accept string, limit int64,
) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, nil, &ResolveError{Reference: endpoint, Code: ReasonImageResolveUnsupported,
			Detail: "cannot build the manifest request", Err: err}
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	} else if basic := r.basicFor(endpoint); basic != "" {
		req.Header.Set("Authorization", basic)
	}

	resp, err := client.Do(req)
	if err != nil {
		// DNS failure, connection refused, TLS failure and timeout all land
		// here, and all of them are "try again later" rather than "this
		// document is wrong".
		return nil, nil, &ResolveError{Reference: endpoint, Code: ReasonImageResolveUnavailable,
			Detail: "the registry could not be reached", Err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return nil, nil, &ResolveError{Reference: endpoint, Code: ReasonImageResolveUnavailable,
			Detail: "the manifest response could not be read", Err: err}
	}
	return resp, body, nil
}

// bearerChallenge is the parsed WWW-Authenticate of a token-auth registry.
type bearerChallenge struct{ Realm, Service, Scope string }

func parseBearerChallenge(header string) *bearerChallenge {
	if !strings.HasPrefix(strings.ToLower(header), "bearer ") {
		return nil
	}
	challenge := &bearerChallenge{}
	for _, part := range splitChallenge(header[len("bearer "):]) {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		value = strings.Trim(strings.TrimSpace(value), `"`)
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "realm":
			challenge.Realm = value
		case "service":
			challenge.Service = value
		case "scope":
			challenge.Scope = value
		}
	}
	if challenge.Realm == "" {
		return nil
	}
	return challenge
}

// splitChallenge splits on commas that are not inside a quoted string. A
// scope value legitimately contains commas ("repository:a:pull,push"), so a
// plain strings.Split loses half of it and the token comes back without push.
func splitChallenge(in string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false
	for _, r := range in {
		switch {
		case r == '"':
			inQuotes = !inQuotes
			current.WriteRune(r)
		case r == ',' && !inQuotes:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

// fetchToken performs the token handshake. Credentials, when configured, are
// sent to the token endpoint the registry named — and only for a pull scope.
func (r *Resolver) fetchToken(ctx context.Context, ref Reference, challenge *bearerChallenge) (string, error) {
	endpoint, err := url.Parse(challenge.Realm)
	if err != nil {
		return "", &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnauthorized,
			Detail: "the registry named an unparseable token realm", Err: err}
	}
	query := endpoint.Query()
	if challenge.Service != "" {
		query.Set("service", challenge.Service)
	}
	scope := challenge.Scope
	if scope == "" {
		scope = "repository:" + ref.Repository + ":pull"
	}
	query.Set("scope", scope)
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return "", &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnauthorized,
			Detail: "cannot build the token request", Err: err}
	}
	if basic := r.basicForHost(ref.Registry); basic != "" {
		req.Header.Set("Authorization", basic)
	}
	resp, err := r.client().Do(req)
	if err != nil {
		return "", &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnavailable,
			Detail: "the registry's token endpoint could not be reached", Err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxManifestBytes))
	if err != nil {
		return "", &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnavailable,
			Detail: "the token response could not be read", Err: err}
	}
	if resp.StatusCode != http.StatusOK {
		return "", &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnauthorized,
			Detail: fmt.Sprintf("the registry's token endpoint answered %d", resp.StatusCode)}
	}
	var token struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &token); err != nil {
		return "", &ResolveError{Reference: ref.Original, Code: ReasonImageResolveUnauthorized,
			Detail: "the token response is not JSON", Err: err}
	}
	if token.Token != "" {
		return token.Token, nil
	}
	return token.AccessToken, nil
}

// dockerAuth is one entry of a Docker config.json `auths` map.
type dockerAuth struct {
	Auth     string `json:"auth"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// basicFor returns the Authorization header for an endpoint URL, if any.
func (r *Resolver) basicFor(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	return r.basicForHost(parsed.Host)
}

// basicForHost looks a host up in the configured auth file.
//
// The lookup tries the host as written and then the two spellings Docker
// itself stores for Docker Hub, because a config.json written by `docker
// login` records the index URL rather than the API host.
func (r *Resolver) basicForHost(host string) string {
	auths := r.loadAuth()
	if len(auths) == 0 {
		return ""
	}
	candidates := []string{host}
	if host == DefaultRegistry || host == DefaultRegistryAPIHost {
		candidates = append(candidates,
			"https://index.docker.io/v1/", "index.docker.io", DefaultRegistryAPIHost, DefaultRegistry)
	}
	for _, candidate := range candidates {
		entry, ok := auths[candidate]
		if !ok {
			continue
		}
		if entry.Auth != "" {
			return "Basic " + entry.Auth
		}
		if entry.Username != "" {
			return "Basic " + base64.StdEncoding.EncodeToString(
				[]byte(entry.Username+":"+entry.Password))
		}
	}
	return ""
}

// loadAuth reads ORK_REGISTRY_AUTH_FILE once.
//
// A missing file is not an error: the overwhelmingly common deployment has no
// registry credentials at all, because public images need none. An unreadable
// or malformed file is also not fatal here — it degrades to anonymous, and the
// resolution then fails with IMAGE_RESOLVE_UNAUTHORIZED naming the file, which
// is a better message than a boot failure for a document that never needed it.
func (r *Resolver) loadAuth() map[string]dockerAuth {
	r.authMu.Lock()
	defer r.authMu.Unlock()
	if r.authRead {
		return r.auth
	}
	r.authRead = true
	if r.AuthFile == "" {
		return nil
	}
	raw, err := os.ReadFile(r.AuthFile)
	if err != nil {
		return nil
	}
	var config struct {
		Auths map[string]dockerAuth `json:"auths"`
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil
	}
	r.auth = config.Auths
	return r.auth
}

// ResolveCode reports the reason code of a resolution or `uses:` failure,
// and "" for anything else. It exists so the apply path can attach the code
// without re-deriving it from the message text.
func ResolveCode(err error) string {
	var resolve *ResolveError
	if errors.As(err, &resolve) {
		return resolve.Code
	}
	var uses *UsesError
	if errors.As(err, &uses) {
		return uses.Code
	}
	return ""
}
