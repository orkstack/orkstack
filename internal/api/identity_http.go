package api

import (
	"errors"
	"net/http"

	"github.com/orkstack/orkstack/internal/identity"
)

// The task identity issuer's documents, served by every engine with
// identity.serve_discovery for relying parties inside the network
// (docs/TASK_IDENTITY.md). What they hold is public, so they need no
// credential in any mode. The discovery document is the one the export
// writes: its jwks_uri is the issuer's own jwks.json, so a relying party
// that reads keys from an engine names IssuerKeysPath itself.
const (
	IssuerDiscoveryPath = "/.well-known/openid-configuration"
	IssuerKeysPath      = "/v1/identity/jwks"
	IssuerBundlePath    = "/v1/identity/spiffe-bundle"
	// issuerCacheControl lets a relying party keep a document for as long
	// as a replica keeps its own copy of the keys.
	issuerCacheControl = "public, max-age=60"
)

// NewIssuerHandler serves the issuer's three documents.
func NewIssuerHandler(issuer *identity.Issuer) http.Handler {
	mux := http.NewServeMux()
	serve := func(path string, doc func() ([]byte, error)) {
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
			body, err := doc()
			if errors.Is(err, identity.ErrNoTrustDomain) {
				http.NotFound(w, r)
				return
			}
			if err != nil {
				http.Error(w, "the issuer's keys are not served by this engine now", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", issuerCacheControl)
			_, _ = w.Write(body)
		})
	}
	serve(IssuerDiscoveryPath, issuer.Discovery)
	serve(IssuerKeysPath, issuer.JWKS)
	serve(IssuerBundlePath, issuer.Bundle)
	return mux
}
