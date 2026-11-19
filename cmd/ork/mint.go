package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/typeid"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// newMinter builds the mint's steps 2 and 4 to 8 for an engine that
// issues task identity, and the record store that answers what it issued;
// nil for both when it issues none, which the API answers
// IDENTITY_ENGINE_REFUSED and unimplemented.
func newMinter(ctx context.Context, cfg config, issuer *identity.Issuer, temporal client.Client,
	namespaces engine.NamespaceRegistry, policies *artifacts.IncarnationStore) (*identity.Minter, *artifacts.RecordStore, error) {
	if !cfg.identity.enabled() || issuer == nil || temporal == nil {
		return nil, nil, nil
	}
	records, err := artifacts.NewRecordStore(ctx, artifacts.ConfigFromEnv())
	if err != nil {
		return nil, nil, fmt.Errorf("the identity mint's record store: %w", err)
	}
	starts := identity.ClusterStarts{RecordStore: records, Temporal: temporal}
	return &identity.Minter{
		Stores: identity.NewAdmissionCache(records, int64(cfg.identity.admissionCacheBytes)),
		Starts: starts,
		Describe: func(ctx context.Context, runID string) (identity.ExecutionFacts, error) {
			return starts.Describe(ctx, runID, "")
		},
		Audiences: func(ctx context.Context, namespace, namespaceID string) ([]string, error) {
			return namespaceAudiences(ctx, namespaces, policies, namespace, namespaceID)
		},
		Signer:         issuer.Signer,
		Issuer:         cfg.identity.issuer,
		Deployment:     cfg.deployment.Name,
		TTL:            cfg.identity.tokenTTL,
		Skew:           cfg.auth.clockSkew,
		RecordIssuance: cfg.identity.issuanceRecords != "off",
		Now:            time.Now,
		NewJTI:         typeid.New,
	}, records, nil
}

// namespaceAudiences reads the audiences a namespace's identity policy
// allows now, for the mint's step 7, for the incarnation whose id the run
// was admitted with. A namespace or a policy that cannot be read is an
// error, never "allows nothing"; one that is gone or made again is
// identity.ErrNamespaceGone.
func namespaceAudiences(ctx context.Context, namespaces engine.NamespaceReader, policies *artifacts.IncarnationStore, name, admittedID string) ([]string, error) {
	ns, err := namespaces.Get(ctx, name)
	if errors.Is(err, artifacts.ErrNotFound) {
		return nil, identity.ErrNamespaceGone
	}
	if err != nil {
		return nil, fmt.Errorf("read namespace %q: %w", name, err)
	}
	// The run was admitted in one incarnation of the namespace; a namespace
	// deleted and made again under its name is another, and its policy
	// grants nothing to the old one's runs.
	id, found, err := policies.NamespaceID(ctx, name, createdAt(ns))
	if err != nil {
		return nil, fmt.Errorf("read namespace %q's id: %w", name, err)
	}
	if !found || id != admittedID {
		return nil, identity.ErrNamespaceGone
	}
	p, _, err := policies.NamespacePolicy(ctx, name, createdAt(ns))
	if err != nil {
		return nil, err
	}
	return p.Audiences, nil
}

// createdAt is the incarnation a namespace's policy is bound to, read
// exactly as the API reads and writes it (namespace_identity.go), so the
// mint and the API name one record, default's included.
func createdAt(ns *orkstackv1.Namespace) time.Time { return ns.GetCreatedAt().AsTime() }
