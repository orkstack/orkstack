package api_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
)

// countingResolver is the real resolver, counted, and able to stand in for
// a registry that has gone away.
type countingResolver struct {
	api.ImageResolver
	calls atomic.Int32
	down  atomic.Bool
}

func (c *countingResolver) Inspect(ctx context.Context, uses, reference string) (*dsl.Resolution, error) {
	c.calls.Add(1)
	if c.down.Load() {
		return nil, &dsl.ResolveError{Reference: reference, Code: dsl.ReasonImageResolveUnavailable, Detail: "the registry is down"}
	}
	return c.ImageResolver.Inspect(ctx, uses, reference)
}

// cachedInspectService serves a labelled image under a tag, and returns the
// service, its counted resolver, the tag and the reference pinning the
// digest the tag names.
func cachedInspectService(t *testing.T) (*api.Service, *countingResolver, string, string) {
	t.Helper()
	registry := newMovableRegistry(t)
	registry.setImage(t, map[string]string{
		dsl.InterfaceLabel: fileLabel(t),
		dsl.IconLabel:      `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M2 8h12" stroke="currentColor"/></svg>`,
		dsl.DocsLabel:      `{"schemaVersion":1,"description":"Copies files."}`,
	}, "linux/amd64")
	server := httptest.NewServer(registry)
	t.Cleanup(server.Close)
	host := strings.TrimPrefix(server.URL, "http://")
	tag := host + "/orkstack/activity-file:dev"
	resolver := &countingResolver{ImageResolver: dsl.NewResolver("")}
	digest, err := resolver.Inspect(context.Background(), "", tag)
	if err != nil {
		t.Fatal(err)
	}
	pinned := host + "/orkstack/activity-file@" + digest.Image.GetIndexDigest()
	svc := api.NewService(api.Config{
		Catalog:  dsl.Catalog{"file": tag, "pinned": pinned},
		Resolver: resolver,
	})
	resolver.calls.Store(0)
	return svc, resolver, tag, pinned
}

func inspect(svc *api.Service, reference string, allowCached bool) (*orkstackv1.ActivityInfo, error) {
	resp, err := svc.InspectActivity(context.Background(),
		connect.NewRequest(&orkstackv1.InspectActivityRequest{Reference: reference, AllowCached: allowCached}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetActivity(), nil
}

// TestInspectActivity_AllowCachedAnswersADigestFromMemory: a reference that
// pins a digest, asked with allow_cached, is read from the registry once;
// later answers — even with the registry gone — are what that read found,
// resolved_at included, as a fresh copy each time.
func TestInspectActivity_AllowCachedAnswersADigestFromMemory(t *testing.T) {
	svc, resolver, _, pinned := cachedInspectService(t)
	first, err := inspect(svc, pinned, true)
	if err != nil {
		t.Fatal(err)
	}
	if first.GetIcon() == nil || first.GetDocs().GetDescription() != "Copies files." || first.GetInterface() == nil {
		t.Fatalf("first answer = %v, want the interface, icon and docs", first)
	}
	resolver.down.Store(true)
	for range 3 {
		again, err := inspect(svc, pinned, true)
		if err != nil {
			t.Fatalf("a cached digest went to the registry: %v", err)
		}
		if !proto.Equal(again, first) {
			t.Fatalf("cached answer\n%v\nwant\n%v", again, first)
		}
		again.Interface = nil
		again.Icon.Elements[0].Name = "circle"
	}
	if got := resolver.calls.Load(); got != 1 {
		t.Fatalf("the registry was read %d times, want once", got)
	}
	if again, _ := inspect(svc, pinned, true); !proto.Equal(again, first) {
		t.Fatal("changing an answer changed the cached one")
	}
}

// TestInspectActivity_ResolvesAfreshUnlessAllowedAndPinned: without
// allow_cached, and for a tag whatever it says, InspectActivity reads the
// registry every time, so a gone image fails as apply would.
func TestInspectActivity_ResolvesAfreshUnlessAllowedAndPinned(t *testing.T) {
	svc, resolver, tag, pinned := cachedInspectService(t)
	for _, reference := range []string{pinned, tag} {
		if _, err := inspect(svc, reference, true); err != nil {
			t.Fatal(err)
		}
	}
	resolver.down.Store(true)
	for _, tc := range []struct {
		name        string
		reference   string
		allowCached bool
	}{
		{"a pinned digest without allow_cached", pinned, false},
		{"a tag with allow_cached", tag, true},
		{"a catalog name for a tag", "file", true},
	} {
		before := resolver.calls.Load()
		_, err := inspect(svc, tc.reference, tc.allowCached)
		if connect.CodeOf(err) != connect.CodeUnavailable {
			t.Errorf("%s: err = %v, want the registry's own failure", tc.name, err)
		}
		if resolver.calls.Load() != before+1 {
			t.Errorf("%s: answered without reading the registry", tc.name)
		}
	}
}

// TestInspectActivity_NeverKeepsAFailure: a digest the registry failed to
// serve is asked again next time.
func TestInspectActivity_NeverKeepsAFailure(t *testing.T) {
	svc, resolver, _, pinned := cachedInspectService(t)
	resolver.down.Store(true)
	if _, err := inspect(svc, pinned, true); err == nil {
		t.Fatal("inspect with the registry down succeeded")
	}
	resolver.down.Store(false)
	if _, err := inspect(svc, pinned, true); err != nil {
		t.Fatalf("a failure was kept: %v", err)
	}
	if got := resolver.calls.Load(); got != 2 {
		t.Fatalf("the registry was read %d times, want twice", got)
	}
}

// TestInspectActivity_NameAndReferenceAreTheRequests: what the cache keeps
// is the digest's; the catalog name and reference are each request's own.
func TestInspectActivity_NameAndReferenceAreTheRequests(t *testing.T) {
	svc, resolver, _, pinned := cachedInspectService(t)
	byName, err := inspect(svc, "pinned", true)
	if err != nil {
		t.Fatal(err)
	}
	byReference, err := inspect(svc, pinned, true)
	if err != nil {
		t.Fatal(err)
	}
	if byName.GetName() != "pinned" || byReference.GetName() != "" {
		t.Fatalf("names = %q / %q, want the catalog key and then none", byName.GetName(), byReference.GetName())
	}
	if byName.GetReference() != pinned || byReference.GetReference() != pinned {
		t.Fatalf("references = %q / %q, want %q", byName.GetReference(), byReference.GetReference(), pinned)
	}
	if got := resolver.calls.Load(); got != 1 {
		t.Fatalf("the registry was read %d times for one digest, want once", got)
	}
}

// syntheticResolver answers any reference that pins a digest without a
// registry, counting reads per pinned image, each answer carrying docs.
type syntheticResolver struct {
	mu    sync.Mutex
	reads map[string]int
	docs  *orkstackv1.ActivityDocs
	// sized, when set, gives one image docs of its own size.
	sized map[string]*orkstackv1.ActivityDocs
	// together, when set for an image, holds each read of it until all the
	// reads it counts have arrived, so concurrent asks surely miss together.
	together map[string]*sync.WaitGroup
}

func (r *syntheticResolver) Inspect(_ context.Context, uses, reference string) (*dsl.Resolution, error) {
	parsed, err := dsl.ParseReference(reference)
	if err != nil || parsed.Digest == "" {
		return nil, &dsl.ResolveError{Reference: reference, Code: dsl.ReasonImageNotFound, Detail: "a synthetic image is pinned"}
	}
	pinned := parsed.Pinned(parsed.Digest)
	r.mu.Lock()
	r.reads[pinned]++
	r.mu.Unlock()
	if arrived, ok := r.together[pinned]; ok {
		arrived.Done()
		arrived.Wait()
	}
	// A fresh message per read, as the real resolver parses one per read.
	source := r.docs
	if sized, ok := r.sized[pinned]; ok {
		source = sized
	}
	var docs *orkstackv1.ActivityDocs
	if source != nil {
		docs = proto.Clone(source).(*orkstackv1.ActivityDocs)
	}
	return &dsl.Resolution{Image: &orkstackv1.ImageRef{Reference: uses, Registry: parsed.Registry,
		Repository: parsed.Repository, IndexDigest: parsed.Digest, Pinned: pinned}, Docs: docs}, nil
}

func (r *syntheticResolver) ResolveOrkflow(context.Context, dsl.Catalog, *orkstackv1.Orkflow) (dsl.Resolutions, error) {
	return nil, errors.New("not used")
}

func (r *syntheticResolver) readsOf(pinned string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads[pinned]
}

// syntheticPin is the i-th distinct pinned image.
func syntheticPin(i int) string {
	return fmt.Sprintf("registry.test/acme/x@sha256:%064x", i)
}

func syntheticService(t *testing.T, docs *orkstackv1.ActivityDocs, catalog dsl.Catalog) (*api.Service, *syntheticResolver) {
	t.Helper()
	resolver := &syntheticResolver{reads: map[string]int{}, docs: docs}
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	return api.NewService(api.Config{Catalog: catalog, Resolver: resolver, Now: func() time.Time { return at }}), resolver
}

// TestInspectActivity_DoesNotKeepAnAnswerPastItsMemoryBound: an answer
// whose parsed size, charged at the heap a parsed message really holds,
// passes the memory bound is never kept, so the image is read again.
func TestInspectActivity_DoesNotKeepAnAnswerPastItsMemoryBound(t *testing.T) {
	// 450 KB of text: under 16 MiB as encoded, well over it as held.
	big := &orkstackv1.ActivityDocs{SchemaVersion: 1, Description: strings.Repeat("a", 450<<10)}
	svc, resolver := syntheticService(t, big, nil)
	for range 2 {
		if _, err := inspect(svc, syntheticPin(1), true); err != nil {
			t.Fatal(err)
		}
	}
	if got := resolver.readsOf(syntheticPin(1)); got != 2 {
		t.Fatalf("an answer past the memory bound was read %d times, want twice", got)
	}
}

// TestInspectActivity_AnswersConcurrentAsksAlike: many asks at once, about
// many images, hits and misses mixed, each get the answer for the image
// asked about — and what one caller does to its answer reaches no other.
func TestInspectActivity_AnswersConcurrentAsksAlike(t *testing.T) {
	svc, _ := syntheticService(t, &orkstackv1.ActivityDocs{SchemaVersion: 1, Description: "x"}, nil)
	want := func(i int) *orkstackv1.ActivityInfo {
		digest := fmt.Sprintf("sha256:%064x", i)
		return &orkstackv1.ActivityInfo{Reference: syntheticPin(i), IndexDigest: digest,
			ResolvedAt: timestamppb.New(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)),
			Docs:       &orkstackv1.ActivityDocs{SchemaVersion: 1, Description: "x"}}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16*64)
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range 64 {
				i := (g*7 + n) % 48
				info, err := inspect(svc, syntheticPin(i), true)
				switch {
				case err != nil:
					errs <- err
				case !proto.Equal(info, want(i)):
					errs <- fmt.Errorf("image %d: answer %v, want %v", i, info, want(i))
				default:
					info.Docs.Description = "changed by one caller"
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestInspectActivity_OnePinnedImageWhateverTagRidesWithIt: a reference that
// names a tag and a digest pins the same image as the digest alone.
func TestInspectActivity_OnePinnedImageWhateverTagRidesWithIt(t *testing.T) {
	svc, resolver := syntheticService(t, nil, nil)
	withTag := strings.Replace(syntheticPin(3), "@", ":v1@", 1)
	for _, reference := range []string{withTag, syntheticPin(3)} {
		if _, err := inspect(svc, reference, true); err != nil {
			t.Fatal(err)
		}
	}
	if got := resolver.readsOf(syntheticPin(3)); got != 1 {
		t.Fatalf("one image under two spellings was read %d times, want once", got)
	}
}

// TestInspectActivity_OneDigestInTwoRepositoriesIsTwoImages: an image is
// kept by its registry, repository and digest, so the same digest in
// another repository is read for itself, never answered from the first.
func TestInspectActivity_OneDigestInTwoRepositoriesIsTwoImages(t *testing.T) {
	svc, resolver := syntheticService(t, nil, nil)
	other := strings.Replace(syntheticPin(7), "acme/x", "other/y", 1)
	for range 2 {
		for _, reference := range []string{syntheticPin(7), other} {
			if _, err := inspect(svc, reference, true); err != nil {
				t.Fatal(err)
			}
		}
	}
	if first, second := resolver.readsOf(syntheticPin(7)), resolver.readsOf(other); first != 1 || second != 1 {
		t.Fatalf("one digest in two repositories was read %d and %d times, want once each", first, second)
	}
}

// TestInspectActivity_TwoMissesOnOneImageKeepItOnce: two asks that miss on
// one image at the same time each read it, and the second answer replaces
// the first instead of being kept beside it, so the image is charged once.
func TestInspectActivity_TwoMissesOnOneImageKeepItOnce(t *testing.T) {
	svc, resolver := syntheticService(t, docsHeld(3), nil)
	var arrived sync.WaitGroup
	arrived.Add(2)
	resolver.together = map[string]*sync.WaitGroup{syntheticPin(0): &arrived}
	var asks sync.WaitGroup
	for range 2 {
		asks.Go(func() {
			if _, err := inspect(svc, syntheticPin(0), true); err != nil {
				t.Error(err)
			}
		})
	}
	asks.Wait()
	// Five answers of about 3 MiB fit in 16 MiB. Kept twice, image 0 would
	// be charged twice, and the fourth other image would push it out.
	for _, i := range []int{1, 2, 3, 4, 0} {
		if _, err := inspect(svc, syntheticPin(i), true); err != nil {
			t.Fatal(err)
		}
	}
	if got := resolver.readsOf(syntheticPin(0)); got != 2 {
		t.Fatalf("the image asked about twice at once was read %d times, want twice: it was kept twice", got)
	}
}

// TestInspectActivity_ACachedAnswerKeepsTheCatalogName: an answer served
// from memory to a catalog name still says which name was asked.
func TestInspectActivity_ACachedAnswerKeepsTheCatalogName(t *testing.T) {
	svc, resolver := syntheticService(t, nil, dsl.Catalog{"pinned": syntheticPin(5)})
	for range 2 {
		info, err := inspect(svc, "pinned", true)
		if err != nil {
			t.Fatal(err)
		}
		if info.GetName() != "pinned" || info.GetReference() != syntheticPin(5) {
			t.Fatalf("answer names %q at %q, want the catalog name and its reference", info.GetName(), info.GetReference())
		}
	}
	if got := resolver.readsOf(syntheticPin(5)); got != 1 {
		t.Fatalf("read %d times, want once", got)
	}
}

// TestInspectActivity_KeepsTheMostRecentlyAskedPastItsEntryBound: asking
// about an image again makes it recent, so past the bound the image asked
// about least recently is the one read again, not the oldest one kept.
func TestInspectActivity_KeepsTheMostRecentlyAskedPastItsEntryBound(t *testing.T) {
	svc, resolver := syntheticService(t, nil, nil)
	const bound = 512
	for i := range bound {
		if _, err := inspect(svc, syntheticPin(i), true); err != nil {
			t.Fatal(err)
		}
	}
	for _, i := range []int{0, bound, 0, 1} {
		if _, err := inspect(svc, syntheticPin(i), true); err != nil {
			t.Fatal(err)
		}
	}
	if got := resolver.readsOf(syntheticPin(0)); got != 1 {
		t.Fatalf("the image asked about again was read %d times, want once", got)
	}
	if got := resolver.readsOf(syntheticPin(1)); got != 2 {
		t.Fatalf("the image asked about least recently was read %d times, want twice", got)
	}
}

// TestInspectActivity_ForgetsPastItsMemoryBoundAcrossAnswers: answers that
// each fit but together pass the memory bound push the oldest out.
func TestInspectActivity_ForgetsPastItsMemoryBoundAcrossAnswers(t *testing.T) {
	// About 4 MiB each as held: four fit in 16 MiB, five do not.
	docs := &orkstackv1.ActivityDocs{SchemaVersion: 1, Description: strings.Repeat("a", 100<<10)}
	svc, resolver := syntheticService(t, docs, nil)
	for _, i := range []int{0, 1, 2, 3, 4, 0, 4} {
		if _, err := inspect(svc, syntheticPin(i), true); err != nil {
			t.Fatal(err)
		}
	}
	if got := resolver.readsOf(syntheticPin(0)); got != 2 {
		t.Fatalf("the oldest answer was read %d times, want twice: five did not fit", got)
	}
	if got := resolver.readsOf(syntheticPin(4)); got != 1 {
		t.Fatalf("an answer that fits was read %d times, want once: it was not kept", got)
	}
}

// docsHeld is docs whose answer is held as about n MiB: its text is n/40 MiB.
func docsHeld(n int) *orkstackv1.ActivityDocs {
	return &orkstackv1.ActivityDocs{SchemaVersion: 1, Description: strings.Repeat("a", n<<20/40)}
}

// TestInspectActivity_MakesRoomForALargeAnswerByForgettingAsManyAsItNeeds:
// an answer that fits only once several kept ones are forgotten pushes out
// as many as it takes, oldest first, and is then kept itself.
func TestInspectActivity_MakesRoomForALargeAnswerByForgettingAsManyAsItNeeds(t *testing.T) {
	svc, resolver := syntheticService(t, docsHeld(3), nil)
	resolver.sized = map[string]*orkstackv1.ActivityDocs{syntheticPin(9): docsHeld(12)}
	// Four of about 3 MiB fit in 16 MiB; one of about 12 MiB then needs
	// three of them gone — 0, 1 and 2, the least recently asked — not one.
	for _, i := range []int{0, 1, 2, 3, 9, 9, 2} {
		if _, err := inspect(svc, syntheticPin(i), true); err != nil {
			t.Fatal(err)
		}
	}
	if got := resolver.readsOf(syntheticPin(9)); got != 1 {
		t.Fatalf("the large answer was read %d times, want once: it was not kept", got)
	}
	if got := resolver.readsOf(syntheticPin(2)); got != 2 {
		t.Fatalf("the third oldest answer was read %d times, want twice: too few were forgotten", got)
	}
}

// TestInspectActivity_AnAnswerTooLargeToKeepForgetsNothingElse: an answer
// past the whole memory bound is not kept, and does not push out what was.
func TestInspectActivity_AnAnswerTooLargeToKeepForgetsNothingElse(t *testing.T) {
	svc, resolver := syntheticService(t, docsHeld(1), nil)
	resolver.sized = map[string]*orkstackv1.ActivityDocs{syntheticPin(9): docsHeld(20)}
	for _, i := range []int{0, 9, 0} {
		if _, err := inspect(svc, syntheticPin(i), true); err != nil {
			t.Fatal(err)
		}
	}
	if got := resolver.readsOf(syntheticPin(0)); got != 1 {
		t.Fatalf("the small answer was read %d times, want once: the large one pushed it out", got)
	}
}
