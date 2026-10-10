package api_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// readCountingRegistry counts the document reads a listing makes, so a test can
// tell a page that read its own documents from one that read every document
// and threw most of them away — the 2,800-namespace page that took seconds.
type readCountingRegistry struct {
	*fakeRegistry
	mu    sync.Mutex
	gets  []string
	lists int
	// vanish names a namespace deleted between the name listing and its
	// read, as another replica's DeleteNamespace can do.
	vanish string
}

func (r *readCountingRegistry) Get(ctx context.Context, name string) (*orkstackv1.Namespace, error) {
	r.mu.Lock()
	r.gets = append(r.gets, name)
	vanish := r.vanish
	r.mu.Unlock()
	if name != "" && name == vanish {
		return nil, fmt.Errorf("namespace %q: %w", name, artifacts.ErrNotFound)
	}
	return r.fakeRegistry.Get(ctx, name)
}

func (r *readCountingRegistry) List(ctx context.Context) ([]*orkstackv1.Namespace, error) {
	r.mu.Lock()
	r.lists++
	r.mu.Unlock()
	return r.fakeRegistry.List(ctx)
}

// reads returns and resets what was read.
func (r *readCountingRegistry) reads() (gets []string, lists int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	gets, lists = append([]string(nil), r.gets...), r.lists
	r.gets, r.lists = nil, 0
	return gets, lists
}

func pagedServiceOf(t *testing.T, docs ...*orkstackv1.Namespace) (*api.Service, *readCountingRegistry) {
	t.Helper()
	reg := &readCountingRegistry{fakeRegistry: newFakeRegistry(docs...)}
	svc := api.NewService(api.Config{
		Orkflows:   engine.NewMemoryOrkflowStore(),
		Namespaces: reg,
		Runs:       engine.NewRunStore(),
		Bus:        engine.NewEventBus(),
		Starter:    &fakeStarter{},
		NewRunID:   func() string { return "run-001" },
	})
	return svc, reg
}

func pagedService(t *testing.T, names ...string) (*api.Service, *readCountingRegistry) {
	t.Helper()
	docs := make([]*orkstackv1.Namespace, 0, len(names))
	for _, name := range names {
		docs = append(docs, &orkstackv1.Namespace{Name: name, Description: "the " + name + " tenant"})
	}
	return pagedServiceOf(t, docs...)
}

// tenants is tenant-00 … tenant-(n-1); every tenth carries team=payments.
func tenants(n int) []*orkstackv1.Namespace {
	docs := make([]*orkstackv1.Namespace, 0, n)
	for i := range n {
		ns := &orkstackv1.Namespace{Name: fmt.Sprintf("tenant-%02d", i), Description: "a tenant"}
		if i%10 == 0 {
			ns.Labels = map[string]string{"team": "payments"}
		}
		docs = append(docs, ns)
	}
	return docs
}

func listNamespacePage(
	t *testing.T, svc *api.Service, size int32, token string, texts ...string,
) *orkstackv1.ListNamespacesResponse {
	t.Helper()
	resp, err := svc.ListNamespaces(context.Background(), connect.NewRequest(&orkstackv1.ListNamespacesRequest{
		PageSize: size, PageToken: token, Filters: filters(t, texts...),
	}))
	if err != nil {
		t.Fatalf("ListNamespaces(size=%d token=%q filters=%q): %v", size, token, texts, err)
	}
	return resp.Msg
}

// walkNamespacePages follows next_page_token to the end and returns each
// page's names.
func walkNamespacePages(t *testing.T, svc *api.Service, size int32, texts ...string) [][]string {
	t.Helper()
	var pages [][]string
	token := ""
	for range 100 {
		page := listNamespacePage(t, svc, size, token, texts...)
		pages = append(pages, namespaceNames(page.GetNamespaces()))
		if page.GetNextPageToken() == "" {
			return pages
		}
		token = page.GetNextPageToken()
	}
	t.Fatalf("listing with page_size %d did not end within 100 pages: %v", size, pages)
	return nil
}

// TestListNamespaces_PagesWalkEveryNameOnceInNameOrder: a positive page_size
// splits the same name-ordered set page_size 0 returns, every name exactly
// once, and the last page carries no token.
func TestListNamespaces_PagesWalkEveryNameOnceInNameOrder(t *testing.T) {
	svc, _ := pagedService(t, "payments", "analytics", "zeta", "billing")
	all := []string{"analytics", "billing", "default", "payments", "zeta"}

	cases := []struct {
		size int32
		want [][]string
	}{
		{0, [][]string{all}},
		{1, [][]string{{"analytics"}, {"billing"}, {"default"}, {"payments"}, {"zeta"}}},
		{2, [][]string{{"analytics", "billing"}, {"default", "payments"}, {"zeta"}}},
		{5, [][]string{all}},
		{50, [][]string{all}},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("size=%d", tc.size), func(t *testing.T) {
			got := walkNamespacePages(t, svc, tc.size)
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("pages = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestListNamespaces_NameOrderNotKeyOrder: `a-b.json` sorts before `a.json`
// in a bucket, but `a` sorts before `a-b` by name. Pages follow names, so a
// cursor on `a` must still yield `a-b` and `ab` next, never skip or repeat.
func TestListNamespaces_NameOrderNotKeyOrder(t *testing.T) {
	svc, _ := pagedService(t, "ab", "a-b", "a")

	got := walkNamespacePages(t, svc, 1, "name~a")
	want := [][]string{{"a"}, {"a-b"}, {"ab"}, {"default"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pages filtered by name~a = %v, want %v", got, want)
	}
	got = walkNamespacePages(t, svc, 1, "labels.none=")
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pages under a property filter = %v, want %v", got, want)
	}
}

// TestListNamespaces_NameFiltersReadOnlyThePagesDocuments is the cost the
// paging exists for: when every filter is on name, the key listing decides
// the page and only the page's documents are read — never every namespace.
func TestListNamespaces_NameFiltersReadOnlyThePagesDocuments(t *testing.T) {
	svc, reg := pagedServiceOf(t, tenants(40)...)

	first := listNamespacePage(t, svc, 3, "")
	if got := namespaceNames(first.GetNamespaces()); fmt.Sprint(got) != "[default tenant-00 tenant-01]" {
		t.Fatalf("first page = %v, want [default tenant-00 tenant-01]", got)
	}
	gets, lists := reg.reads()
	if lists != 0 || len(gets) != 3 {
		t.Fatalf("a page of 3 out of 41 made %d full listings and %d reads (%v), want 0 and 3", lists, len(gets), gets)
	}
	for _, ns := range first.GetNamespaces()[1:] {
		if ns.GetDescription() == "" {
			t.Errorf("page entry %q came back without its document", ns.GetName())
		}
	}

	page := listNamespacePage(t, svc, 3, "", "name^tenant-1", "name!=tenant-10")
	if got := namespaceNames(page.GetNamespaces()); fmt.Sprint(got) != "[tenant-11 tenant-12 tenant-13]" {
		t.Fatalf("name^tenant-1 page = %v", got)
	}
	if gets, _ := reg.reads(); fmt.Sprint(slices.Sorted(slices.Values(gets))) != "[tenant-11 tenant-12 tenant-13]" {
		t.Fatalf("a name-filtered page read %v, want only its three documents", gets)
	}
	if page.GetNextPageToken() == "" {
		t.Fatal("six more names match; the page carried no token")
	}
}

// TestListNamespaces_PropertyFilterReadsInNameOrderUntilThePageIsFull: a
// filter the key listing cannot decide reads documents in name order, a
// batch of 8 at a time, and stops at the batch that holds the match one past
// the page.
func TestListNamespaces_PropertyFilterReadsInNameOrderUntilThePageIsFull(t *testing.T) {
	svc, reg := pagedServiceOf(t, tenants(40)...)

	// Names: default (1), tenant-00 (2) … tenant-39 (41). Matches are
	// tenant-00, -10, -20, -30, at names 2, 12, 22, 32. A page of 2 needs a
	// third match, name 22, which is in the third batch (names 17–24).
	page := listNamespacePage(t, svc, 2, "", "labels.team=payments")
	if got := namespaceNames(page.GetNamespaces()); fmt.Sprint(got) != "[tenant-00 tenant-10]" {
		t.Fatalf("page = %v, want [tenant-00 tenant-10]", got)
	}
	gets, lists := reg.reads()
	if lists != 0 || len(gets) != 24 {
		t.Fatalf("read %d documents and %d full listings, want 24 and 0", len(gets), lists)
	}
	if page.GetNextPageToken() == "" {
		t.Fatal("two more namespaces match; the page carried no token")
	}
}

// TestListNamespaces_PropertyFilterLastPageHasNoToken: the page that reads
// to the end of the candidates without a match past it says there is no more.
func TestListNamespaces_PropertyFilterLastPageHasNoToken(t *testing.T) {
	svc, reg := pagedServiceOf(t, tenants(40)...)
	first := listNamespacePage(t, svc, 2, "", "labels.team=payments")
	reg.reads()
	last := listNamespacePage(t, svc, 2, first.GetNextPageToken(), "labels.team=payments")
	if got := namespaceNames(last.GetNamespaces()); fmt.Sprint(got) != "[tenant-20 tenant-30]" || last.GetNextPageToken() != "" {
		t.Fatalf("last page = %v (token %q), want [tenant-20 tenant-30] and no token", got, last.GetNextPageToken())
	}
	// Every candidate after the cursor (tenant-11 … tenant-39) was read.
	if gets, _ := reg.reads(); len(gets) != 29 {
		t.Fatalf("read %d documents (%v), want the 29 after the cursor", len(gets), gets)
	}
	pages := walkNamespacePages(t, svc, 3, "labels.team=payments")
	if fmt.Sprint(pages) != "[[tenant-00 tenant-10 tenant-20] [tenant-30]]" {
		t.Fatalf("pages of 3 = %v", pages)
	}
}

// TestListNamespaces_LabelsAndHoldFilters: a label is labels.KEY with an
// absent key reading empty, and an unset hold reads as an empty reason.
func TestListNamespaces_LabelsAndHoldFilters(t *testing.T) {
	svc, _ := pagedServiceOf(t,
		&orkstackv1.Namespace{Name: "alpha", Labels: map[string]string{"team": "payments", "tier": "gold"}},
		&orkstackv1.Namespace{Name: "beta", Labels: map[string]string{"team": "search"},
			Hold: &orkstackv1.NamespaceHold{Reason: "billing review"}},
		&orkstackv1.Namespace{Name: "gamma"},
	)
	for _, tc := range []struct {
		texts []string
		want  string
	}{
		{[]string{"labels.team=payments"}, "[alpha]"},
		{[]string{"labels.team=payments,search"}, "[alpha beta]"},
		{[]string{"labels.team="}, "[default gamma]"},
		{[]string{"labels.team!=", "labels.tier!=gold"}, "[beta]"},
		{[]string{"hold.reason!="}, "[beta]"},
		{[]string{"hold.reason="}, "[alpha default gamma]"},
		{[]string{"hold.reason~billing", "name^b"}, "[beta]"},
	} {
		for _, size := range []int32{0, 1, 25} {
			var got []string
			for _, page := range walkNamespacePages(t, svc, size, tc.texts...) {
				got = append(got, page...)
			}
			if fmt.Sprint(got) != tc.want {
				t.Errorf("%q size %d = %v, want %s", tc.texts, size, got, tc.want)
			}
		}
	}
}

// TestListNamespaces_TimeFilters: created_at and updated_at reach the rows
// ListNamespaces matches, so an at-or-after or before bound selects by them.
// The synthesized default carries neither stamp, and an unset time never
// matches.
func TestListNamespaces_TimeFilters(t *testing.T) {
	at := func(day int) *timestamppb.Timestamp {
		return timestamppb.New(time.Date(2026, time.March, day, 12, 0, 0, 0, time.UTC))
	}
	svc, _ := pagedServiceOf(t,
		&orkstackv1.Namespace{Name: "alpha", CreatedAt: at(1), UpdatedAt: at(20)},
		&orkstackv1.Namespace{Name: "beta", CreatedAt: at(10), UpdatedAt: at(10)},
		&orkstackv1.Namespace{Name: "gamma", CreatedAt: at(15), UpdatedAt: at(16)},
	)
	for _, tc := range []struct {
		texts []string
		want  string
	}{
		{[]string{"created_at>=2026-03-10T12:00:00Z"}, "[beta gamma]"},
		{[]string{"updated_at>=2026-03-16T00:00:00Z"}, "[alpha gamma]"},
		{[]string{"created_at<2026-03-10T12:00:00Z"}, "[alpha]"},
		{[]string{"created_at>=2026-03-01T00:00:00Z", "updated_at<2026-03-16T00:00:00Z"}, "[beta]"},
	} {
		for _, size := range []int32{0, 1, 25} {
			var got []string
			for _, page := range walkNamespacePages(t, svc, size, tc.texts...) {
				got = append(got, page...)
			}
			if fmt.Sprint(got) != tc.want {
				t.Errorf("%q size %d = %v, want %s", tc.texts, size, got, tc.want)
			}
		}
	}
}

// TestListNamespaces_UnpagedListsEveryMatch: page_size 0 answers every
// match in one response and never a token, filtered or not.
func TestListNamespaces_UnpagedListsEveryMatch(t *testing.T) {
	svc, _ := pagedServiceOf(t, tenants(40)...)
	page := listNamespacePage(t, svc, 0, "", "labels.team=payments")
	if got := namespaceNames(page.GetNamespaces()); fmt.Sprint(got) != "[tenant-00 tenant-10 tenant-20 tenant-30]" || page.GetNextPageToken() != "" {
		t.Fatalf("unpaged = %v (token %q)", got, page.GetNextPageToken())
	}
	page = listNamespacePage(t, svc, 0, "", "name~fault")
	if got := namespaceNames(page.GetNamespaces()); fmt.Sprint(got) != "[default]" {
		t.Fatalf("name~fault = %v, want [default]", got)
	}
}

// TestListNamespaces_PageSizeZeroIsTheSamePlanWithNoBound: the unpaged,
// unfiltered answer is exactly what the registry's own List holds, without
// calling it.
func TestListNamespaces_PageSizeZeroIsTheSamePlanWithNoBound(t *testing.T) {
	svc, reg := pagedServiceOf(t, tenants(20)...)
	page := listNamespacePage(t, svc, 0, "")
	gets, lists := reg.reads()
	want, err := reg.fakeRegistry.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.GetNamespaces()) != len(want) || page.GetNextPageToken() != "" {
		t.Fatalf("unpaged = %d rows (token %q), want %d and no token", len(page.GetNamespaces()), page.GetNextPageToken(), len(want))
	}
	for i := range want {
		if !proto.Equal(page.GetNamespaces()[i], want[i]) {
			t.Errorf("row %d = %v, want %v", i, page.GetNamespaces()[i], want[i])
		}
	}
	if lists != 0 || len(gets) != 21 {
		t.Fatalf("page_size 0 made %d List calls and %d reads, want 0 and 21 (one per name)", lists, len(gets))
	}
}

// TestListNamespaces_TokenBoundToTheFilterSet: a token continues the set it
// was issued for — in any order or spelling of the same set — and no other.
func TestListNamespaces_TokenBoundToTheFilterSet(t *testing.T) {
	svc, _ := pagedServiceOf(t, tenants(40)...)
	first := listNamespacePage(t, svc, 1, "", "labels.team=payments", "name^tenant")
	token := first.GetNextPageToken()
	if token == "" {
		t.Fatal("no token")
	}
	// The same set, reordered and with a redundant duplicate, continues.
	next := listNamespacePage(t, svc, 1, token, "name^tenant", "labels.team=payments", "name^tenant")
	if got := namespaceNames(next.GetNamespaces()); fmt.Sprint(got) != "[tenant-10]" {
		t.Fatalf("continued page = %v, want [tenant-10]", got)
	}
	for _, other := range [][]string{nil, {"labels.team=payments"}, {"labels.team=search", "name^tenant"}} {
		_, err := svc.ListNamespaces(context.Background(), connect.NewRequest(&orkstackv1.ListNamespacesRequest{
			PageSize: 1, PageToken: token, Filters: filters(t, other...),
		}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument ||
			!strings.Contains(err.Error(), "page_token was issued for other filters; drop it and list from the first page") {
			t.Errorf("token under %q: %v; want the other-filters refusal", other, err)
		}
	}
}

// TestListNamespaces_RefusesWhatItCannotAnswer: a negative size, a token
// with page_size 0, a token that is not one, an old-shaped token and an
// invalid filter are all INVALID_ARGUMENT — never a silent first page.
func TestListNamespaces_RefusesWhatItCannotAnswer(t *testing.T) {
	svc, reg := pagedService(t, "payments", "pay-later", "billing")
	token := listNamespacePage(t, svc, 1, "", "name~pay").GetNextPageToken()
	if token == "" {
		t.Fatal("a one-entry page of two matches carried no next_page_token")
	}
	reg.reads()

	cases := []struct {
		name     string
		req      *orkstackv1.ListNamespacesRequest
		mentions string
	}{
		{"negative size", &orkstackv1.ListNamespacesRequest{PageSize: -1}, "page_size"},
		{"token with page_size 0", &orkstackv1.ListNamespacesRequest{PageToken: token, Filters: filters(t, "name~pay")}, "page_size"},
		{"not a token", &orkstackv1.ListNamespacesRequest{PageSize: 1, PageToken: "%%%"}, "invalid page_token"},
		{"forged token version", &orkstackv1.ListNamespacesRequest{
			PageSize: 1, PageToken: forgeNamespaceCursor(t, 2, "", "billing")}, "invalid page_token"},
		{"unknown field", &orkstackv1.ListNamespacesRequest{PageSize: 1, Filters: filters(t, "nope=x")},
			`filters[0]: "nope" is not a filterable field of Namespace`},
		{"name rule", &orkstackv1.ListNamespacesRequest{Filters: filters(t, "name=Payments")},
			`filters[0]: name "Payments" is not a slug`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.ListNamespaces(context.Background(), connect.NewRequest(tc.req))
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("code = %v (%v), want InvalidArgument", connect.CodeOf(err), err)
			}
			if !strings.Contains(err.Error(), tc.mentions) {
				t.Errorf("error %q does not name %s", err, tc.mentions)
			}
		})
	}
	if gets, lists := reg.reads(); len(gets) != 0 || lists != 0 {
		t.Errorf("refused requests read %v and listed %d times; a refusal reads nothing", gets, lists)
	}

	// The token round-trips: the same filter continues where the page ended.
	next := listNamespacePage(t, svc, 1, token, "name~pay")
	if got := namespaceNames(next.GetNamespaces()); fmt.Sprint(got) != "[payments]" || next.GetNextPageToken() != "" {
		t.Fatalf("second page = %v (token %q), want [payments] and no token", got, next.GetNextPageToken())
	}
}

func forgeNamespaceCursor(t *testing.T, version int, filter, name string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"v": version, "f": filter, "name": name})
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// TestListNamespaces_SkipsANameDeletedBetweenListingAndRead: a namespace
// removed after the name listing is left out of its page, exactly as the
// full listing leaves it out, and the page after it still follows.
func TestListNamespaces_SkipsANameDeletedBetweenListingAndRead(t *testing.T) {
	svc, reg := pagedService(t, "alpha", "bravo", "charlie")
	reg.vanish = "bravo"

	first := listNamespacePage(t, svc, 2, "")
	if got := namespaceNames(first.GetNamespaces()); fmt.Sprint(got) != "[alpha]" {
		t.Fatalf("page with a vanished name = %v, want [alpha]", got)
	}
	rest := listNamespacePage(t, svc, 2, first.GetNextPageToken())
	if got := namespaceNames(rest.GetNamespaces()); fmt.Sprint(got) != "[charlie default]" {
		t.Fatalf("page after the vanished name = %v, want [charlie default]", got)
	}
	all := walkNamespacePages(t, svc, 1, "description~tenant")
	if fmt.Sprint(all) != "[[alpha] [charlie]]" {
		t.Fatalf("property-filtered pages with a vanished name = %v", all)
	}
}

// TestListNamespaces_NoRegistryPagesTheDefaultAlone: a process with no
// registry still lists `default`, by the same paging and filter rules.
func TestListNamespaces_NoRegistryPagesTheDefaultAlone(t *testing.T) {
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Runs:     engine.NewRunStore(),
		Bus:      engine.NewEventBus(),
		Starter:  &fakeStarter{},
		NewRunID: func() string { return "run-001" },
	})
	cases := []struct {
		size  int32
		texts []string
		want  string
	}{
		{0, nil, "[default]"},
		{1, nil, "[default]"},
		{25, []string{"name~fault"}, "[default]"},
		{25, []string{"name=payments"}, "[]"},
		{25, []string{"labels.team="}, "[default]"},
	}
	for _, tc := range cases {
		page := listNamespacePage(t, svc, tc.size, "", tc.texts...)
		if got := fmt.Sprint(namespaceNames(page.GetNamespaces())); got != tc.want || page.GetNextPageToken() != "" {
			t.Errorf("size %d filters %q = %s (token %q), want %s and no token",
				tc.size, tc.texts, got, page.GetNextPageToken(), tc.want)
		}
	}
	if _, err := svc.ListNamespaces(context.Background(), connect.NewRequest(
		&orkstackv1.ListNamespacesRequest{PageSize: -1})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("negative page_size without a registry: code %v, want InvalidArgument", connect.CodeOf(err))
	}
}

// TestListNamespaces_DefaultIsReadThroughGetAndSynthesizedWhenUnstored:
// default is a name like any other — it is asked of the registry's Get,
// which synthesizes it while nothing was ever written.
func TestListNamespaces_DefaultIsReadThroughGetAndSynthesizedWhenUnstored(t *testing.T) {
	svc, reg := pagedService(t, "alpha")
	page := listNamespacePage(t, svc, 1, "", "name=default")
	gets, _ := reg.reads()
	if fmt.Sprint(gets) != "[default]" {
		t.Fatalf("reads = %v, want default read through Get", gets)
	}
	if len(page.GetNamespaces()) != 1 || !proto.Equal(page.GetNamespaces()[0], artifacts.DefaultNamespaceDocument()) {
		t.Fatalf("default = %v, want the synthesized document", page.GetNamespaces())
	}
}

// TestListNamespaces_StoredDefaultIsReturnedAsStored: once an update wrote
// default, the listing returns the stored document, not a synthesized one.
func TestListNamespaces_StoredDefaultIsReturnedAsStored(t *testing.T) {
	svc, _ := pagedService(t, "alpha")
	_, err := svc.UpdateNamespace(context.Background(), connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{
		Namespace: &orkstackv1.Namespace{Name: "default", Description: "the stored default",
			Labels: map[string]string{"team": "platform"}},
	}))
	if err != nil {
		t.Fatalf("UpdateNamespace(default): %v", err)
	}
	for _, texts := range [][]string{nil, {"labels.team=platform"}, {"name=default"}} {
		var found *orkstackv1.Namespace
		for _, ns := range listNamespacePage(t, svc, 10, "", texts...).GetNamespaces() {
			if ns.GetName() == "default" {
				found = ns
			}
		}
		if found.GetDescription() != "the stored default" || found.GetEtag() == "" {
			t.Errorf("filters %q: default = %v, want the stored document", texts, found)
		}
	}
}
