package api

import (
	"context"
	"fmt"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// ImageResolver is the apply-time resolution seam (§4.3).
//
// It is an interface for one reason: ApplyOrkflow must be testable without a
// registry. It is NOT an extension point — §4.3 says there is exactly one
// resolver, because a second one can disagree with the first about
// `platforms` and therefore about the definition digest, and §4.4's whole
// argument is that one definition has one digest.
type ImageResolver interface {
	// ResolveOrkflow resolves every distinct `uses:`, stamps Task.image,
	// and returns what each image declares (§4.3 step 7).
	ResolveOrkflow(ctx context.Context, catalog dsl.Catalog, wf *orkstackv1.Orkflow) (dsl.Resolutions, error)
	// Inspect resolves a single reference, for the catalog and inspect
	// read paths.
	Inspect(ctx context.Context, uses, reference string) (*dsl.Resolution, error)
}

// CatalogLister answers ActivityService.ListActivities from the configured
// catalog (§4.2), resolving each entry so an operator sees what a short name
// will actually run.
//
// It CACHES, and the cache is the whole reason this type exists rather than a
// closure. The Admin page polls this endpoint; resolving three references over
// the network on every poll would put a registry round trip on a UI refresh,
// and a rate-limited registry would make the page fail intermittently for a
// question whose answer changes when an operator edits configuration.
//
// A resolution that fails is not fatal to the listing. The catalog entry still
// exists — that is a fact about configuration, not about the registry — so it
// is reported with an empty digest rather than failing the whole read. An
// operator seeing a blank digest learns something true and actionable; an
// error page tells them only that something, somewhere, is down.
type CatalogLister struct {
	// Catalog is the deployment's table. Nil means the builtin one.
	Catalog dsl.Catalog
	// Resolver turns each entry into a digest. Nil lists the catalog
	// unresolved, which is what an engine with no network egress can honestly
	// report.
	Resolver ImageResolver
	// TTL bounds how long a resolution is reused. Zero means DefaultCatalogTTL.
	TTL time.Duration
	// Now is injectable for deterministic tests.
	Now func() time.Time

	mu     sync.Mutex
	cached []*orkstackv1.ActivityInfo
	at     time.Time
}

// DefaultCatalogTTL is how long a resolved catalog entry is reused.
const DefaultCatalogTTL = 5 * time.Minute

func (l *CatalogLister) now() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	return time.Now()
}

func (l *CatalogLister) ttl() time.Duration {
	if l.TTL <= 0 {
		return DefaultCatalogTTL
	}
	return l.TTL
}

func (l *CatalogLister) catalog() dsl.Catalog {
	if l.Catalog == nil {
		return dsl.BuiltinActivityCatalog()
	}
	return l.Catalog
}

// ListActivities returns the catalog, ordered by name.
//
// The order is produced here as well as re-sorted by the RPC, and that is
// deliberate rather than redundant: §2.3 makes the order part of the contract,
// and a seam that returned an arbitrary order would be relying on its caller
// to fix it.
func (l *CatalogLister) ListActivities(ctx context.Context) ([]*orkstackv1.ActivityInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.cached != nil && l.now().Sub(l.at) < l.ttl() {
		return l.cached, nil
	}

	catalog := l.catalog()
	activities := make([]*orkstackv1.ActivityInfo, 0, len(catalog))
	for _, name := range catalog.Names() {
		reference := catalog[name]
		info := &orkstackv1.ActivityInfo{Name: name, Reference: reference}
		if l.Resolver != nil {
			if res, err := l.Resolver.Inspect(ctx, name, reference); err == nil {
				fillActivityInfo(info, res, l.now())
			}
			// A failed resolution leaves the digest empty, on purpose: see
			// the type comment. The entry is configuration and still exists.
		}
		activities = append(activities, info)
	}

	l.cached = activities
	l.at = l.now()
	return activities, nil
}

// resolveImages is the §4.3 call ApplyOrkflow makes between Compile and Put.
//
// It is a method on Service rather than a free function so that a Service with
// no resolver configured has one documented behaviour: leave Task.image empty
// and apply anyway. That is what a unit test and an air-gapped `make up`
// both need, and it is honest — an engine that cannot reach a registry cannot
// pin a digest, and pretending otherwise would be worse than not pinning.
//
// It also returns what each resolved image declares about its with: block,
// which prepareOrkflow checks the document against. No resolver returns no
// resolutions: an engine that pins nothing checks nothing.
func (s *Service) resolveImages(ctx context.Context, wf *orkstackv1.Orkflow) (dsl.Resolutions, error) {
	if s.resolver == nil {
		return nil, nil
	}
	return s.resolver.ResolveOrkflow(ctx, s.activityCatalog(), wf)
}

// fillActivityInfo copies one resolution into a catalog or inspect answer.
// The interface is a READ-MODEL field, like resolved_at: derived from the
// digest-addressed image on every read and stored nowhere.
func fillActivityInfo(info *orkstackv1.ActivityInfo, res *dsl.Resolution, now time.Time) {
	image := res.Image
	info.IndexDigest = image.GetIndexDigest()
	info.MediaType = image.GetMediaType()
	info.Platforms = image.GetPlatforms()
	info.ResolvedAt = timestamppb.New(now)
	info.Interface = res.Interface
	info.InterfaceError = res.InterfaceError
}

func (s *Service) activityCatalog() dsl.Catalog {
	if s.catalog == nil {
		return dsl.BuiltinActivityCatalog()
	}
	return s.catalog
}

// applyResolveError turns a resolution failure into the Connect error an
// author sees, preserving the reason code (§4.3).
//
// The code is carried in the message rather than invented from the HTTP
// status, because the status is a transport fact and the code is the contract:
// "IMAGE_NOT_FOUND" means the same thing to a CLI, a UI and a log grep,
// whereas connect.CodeInvalidArgument does not distinguish a typo'd repository
// from a registry that is merely down.
func applyResolveError(err error) error {
	code := dsl.ResolveCode(err)
	if code == "" {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	if code == dsl.ReasonImageResolveUnavailable {
		// Retryable: nothing about the document is wrong.
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("%s: %w", code, err))
	}
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%s: %w", code, err))
}
