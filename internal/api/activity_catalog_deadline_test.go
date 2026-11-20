package api_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// hangingResolver answers every reference at once except hang, which it
// never answers: a registry that accepts the connection and says nothing.
type hangingResolver struct {
	api.ImageResolver
	hang string
}

func (r hangingResolver) Inspect(ctx context.Context, _, reference string) (*dsl.Resolution, error) {
	if reference == r.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &dsl.Resolution{Image: &orkstackv1.ImageRef{IndexDigest: "sha256:" + strings.Repeat("a", 64)}}, nil
}

// A catalog image whose registry never answers costs the listing its own
// deadline, not the call: ListActivities returns within the listing's bound,
// with every other image resolved and the unreadable one named, its
// resolve_error saying why. A page that asks for the catalog is never left
// holding a request open.
func TestCatalogLister_ARegistryThatNeverAnswersCostsOnlyItsImage(t *testing.T) {
	catalog := dsl.Catalog{"shell": "registry.example/activity-shell:1", "http": "registry.example/activity-http:1"}
	lister := &api.CatalogLister{
		Catalog:      catalog,
		Resolver:     hangingResolver{hang: "registry.example/activity-shell:1"},
		ImageTimeout: 200 * time.Millisecond,
		ListTimeout:  time.Second,
	}
	started := time.Now()
	activities, err := lister.ListActivities(context.Background())
	took := time.Since(started)
	must.OKf(t, err, "ListActivities")
	if took > 2*time.Second {
		t.Fatalf("ListActivities took %s; want it within its 1s bound", took)
	}
	byName := map[string]*orkstackv1.ActivityInfo{}
	for _, a := range activities {
		byName[a.GetName()] = a
	}
	if got := byName["http"]; got.GetIndexDigest() == "" || got.GetResolveError() != "" {
		t.Errorf("http = digest %q, resolve_error %q; want it resolved", got.GetIndexDigest(), got.GetResolveError())
	}
	shell := byName["shell"]
	if shell.GetIndexDigest() != "" || !strings.Contains(shell.GetResolveError(), "registry.example/activity-shell:1") {
		t.Errorf("shell = digest %q, resolve_error %q; want no digest and an error naming its image", shell.GetIndexDigest(), shell.GetResolveError())
	}
}

// The listing's own bound holds even when every image's deadline would be
// longer than it, and a listing that could not read an image is read again
// soon rather than served for the catalog's whole TTL.
func TestCatalogLister_TheListingsBoundHoldsAndAFailureIsNotKeptLong(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	lister := &api.CatalogLister{
		Catalog:      dsl.Catalog{"shell": "registry.example/activity-shell:1"},
		Resolver:     hangingResolver{hang: "registry.example/activity-shell:1"},
		ImageTimeout: time.Minute,
		ListTimeout:  300 * time.Millisecond,
		Now:          func() time.Time { return now },
	}
	started := time.Now()
	activities, err := lister.ListActivities(context.Background())
	must.OKf(t, err, "ListActivities")
	if took := time.Since(started); took > 2*time.Second {
		t.Fatalf("ListActivities took %s; want it within its 300ms listing bound", took)
	}
	if len(activities) != 1 || activities[0].GetResolveError() == "" {
		t.Fatalf("activities = %v; want shell named unreadable", activities)
	}
	now = now.Add(api.CatalogFailureTTL + time.Second)
	lister.Resolver = hangingResolver{}
	again, err := lister.ListActivities(context.Background())
	must.OKf(t, err, "ListActivities again")
	if again[0].GetResolveError() != "" || again[0].GetIndexDigest() == "" {
		t.Fatalf("after the failure's short TTL: %v; want it read again and resolved", again[0])
	}
}

// A caller that goes away while the listing reads does not end the listing:
// it is shared by every caller it is cached for, so it finishes under its own
// bound, and the next caller is served it resolved, never a listing of
// cancellations.
func TestCatalogLister_ACancelledCallerDoesNotPoisonTheCache(t *testing.T) {
	slow := &slowResolver{asked: make(chan struct{}), release: make(chan struct{})}
	lister := &api.CatalogLister{
		Catalog:  dsl.Catalog{"shell": "registry.example/activity-shell:1"},
		Resolver: slow,
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-slow.asked
		cancel()
		close(slow.release)
	}()
	first, err := lister.ListActivities(ctx)
	must.OKf(t, err, "ListActivities of the caller that went away")
	if first[0].GetResolveError() != "" {
		t.Fatalf("the cancelled caller's listing = %v; want it read in full", first[0])
	}
	again, err := lister.ListActivities(context.Background())
	must.OKf(t, err, "ListActivities")
	if again[0].GetResolveError() != "" || again[0].GetIndexDigest() == "" {
		t.Fatalf("the next caller was served %v; want the resolved entry", again[0])
	}
}

// slowResolver answers once release closes, saying when it was asked.
type slowResolver struct {
	api.ImageResolver
	asked   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *slowResolver) Inspect(ctx context.Context, _, _ string) (*dsl.Resolution, error) {
	r.once.Do(func() { close(r.asked) })
	select {
	case <-r.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &dsl.Resolution{Image: &orkstackv1.ImageRef{IndexDigest: "sha256:" + strings.Repeat("b", 64)}}, nil
}
