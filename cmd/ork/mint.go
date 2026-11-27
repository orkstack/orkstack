package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
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
	namespaces engine.NamespaceRegistry, policies *artifacts.IncarnationStore) (*identity.Minter, *engine.IdentityChecks, *artifacts.RecordStore, error) {
	if !cfg.identity.enabled() || issuer == nil || temporal == nil {
		return nil, nil, nil, nil
	}
	records, err := artifacts.NewRecordStore(ctx, artifacts.ConfigFromEnv())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("the identity mint's record store: %w", err)
	}
	checks := engine.NewIdentityChecks(records, temporal, int64(cfg.identity.admissionCacheBytes))
	return &identity.Minter{
		Stores:   checks.Admissions,
		Starts:   checks.Starts,
		Follows:  checks.Follows,
		Describe: checks.Describe,
		Policy: func(ctx context.Context, namespace, namespaceID string) (artifacts.NamespacePolicy, error) {
			return namespacePolicy(ctx, namespaces, policies, namespace, namespaceID)
		},
		Signer:         issuer.Signer,
		Issuer:         cfg.identity.issuer,
		Deployment:     cfg.deployment.Name,
		TTL:            cfg.identity.tokenTTL,
		Skew:           cfg.auth.clockSkew,
		RecordIssuance: cfg.identity.issuanceRecords != "off",
		Now:            time.Now,
		NewJTI:         typeid.New,
	}, &checks, records, nil
}

// awaitRecords is what the await doors read and write for a run whose
// results the engine records: admissions through the mint's cache, every
// other record from the record store. nil when the engine issues no task
// identity, so such a run's await answers unavailable.
func awaitRecords(minter *identity.Minter, records *artifacts.RecordStore) api.AwaitRecords {
	if minter == nil || records == nil {
		return nil
	}
	return awaitStore{RecordStore: records, admissions: minter.Stores}
}

// resolutionReader is what a run reads an await's record with; nil when the
// engine issues no task identity.
func resolutionReader(records *artifacts.RecordStore) engine.ResolutionReader {
	if records == nil {
		return nil
	}
	return records
}

// recordedResults is what a listing of recorded results reads; nil when the
// engine issues no task identity.
func recordedResults(records *artifacts.RecordStore) api.RecordedResults {
	if records == nil {
		return nil
	}
	return records
}

type awaitStore struct {
	*artifacts.RecordStore
	admissions identity.MintStores
}

func (s awaitStore) ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (artifacts.RecordRead[artifacts.Admission], error) {
	return s.admissions.ReadAdmission(ctx, namespace, runID, sha256Hex)
}

// awaitChecks is the mint's own check of what a task follows.
func awaitChecks(minter *identity.Minter) api.AwaitChecks {
	if minter == nil {
		return nil
	}
	if checks, ok := minter.Follows.(api.AwaitChecks); ok {
		return checks
	}
	return nil
}

// namespacePolicy reads a namespace's identity policy as it is now, for the
// mint's step 7, for the incarnation whose id the run was admitted with. A
// namespace or a policy that cannot be read is an error, never "allows
// nothing"; one that is gone or made again is identity.ErrNamespaceGone.
func namespacePolicy(ctx context.Context, namespaces engine.NamespaceReader, policies *artifacts.IncarnationStore, name, admittedID string) (artifacts.NamespacePolicy, error) {
	ns, err := namespaces.Get(ctx, name)
	if errors.Is(err, artifacts.ErrNotFound) {
		return artifacts.NamespacePolicy{}, identity.ErrNamespaceGone
	}
	if err != nil {
		return artifacts.NamespacePolicy{}, fmt.Errorf("read namespace %q: %w", name, err)
	}
	// The run was admitted in one incarnation of the namespace; a namespace
	// deleted and made again under its name is another, and its policy
	// grants nothing to the old one's runs.
	id, found, err := policies.NamespaceID(ctx, name, createdAt(ns))
	if err != nil {
		return artifacts.NamespacePolicy{}, fmt.Errorf("read namespace %q's id: %w", name, err)
	}
	if !found || id != admittedID {
		return artifacts.NamespacePolicy{}, identity.ErrNamespaceGone
	}
	p, _, err := policies.NamespacePolicy(ctx, name, createdAt(ns))
	return p, err
}

// createdAt is the incarnation a namespace's policy is bound to, read
// exactly as the API reads and writes it (namespace_identity.go), so the
// mint and the API name one record, default's included.
func createdAt(ns *orkstackv1.Namespace) time.Time { return ns.GetCreatedAt().AsTime() }

// chainCompleter is the switch's completion of a root run's chain record:
// identity.CompleteRootChain on the cluster and the records.
func chainCompleter(records *artifacts.RecordStore, temporal client.Client) engine.ChainCompleter {
	starts := engine.NewClusterStarts(records, temporal)
	return func(ctx context.Context, namespace, runID, admissionSHA256 string) error {
		if answer := identity.CompleteRootChain(ctx, starts, namespace, runID, admissionSHA256); answer.Verdict != identity.StartAdmitted {
			return errors.New(answer.Reason)
		}
		return nil
	}
}

// composedRecords is what a resume records a composed task's values from its
// child's outputs with; nil on an engine that keeps no identity records.
func composedRecords(rec engine.ValuesRecorder) api.ComposedRecords {
	if rec == nil {
		return nil
	}
	return rec
}
