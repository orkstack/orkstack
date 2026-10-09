package api_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"

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

func (r *readCountingRegistry) reads() (gets []string, lists int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.gets...), r.lists
}

func pagedService(t *testing.T, names ...string) (*api.Service, *readCountingRegistry) {
	t.Helper()
	docs := make([]*orkstackv1.Namespace, 0, len(names))
	for _, name := range names {
		docs = append(docs, &orkstackv1.Namespace{Name: name, Description: "the " + name + " tenant"})
	}
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

func listNamespacePage(
	t *testing.T, svc *api.Service, size int32, token, contains string,
) *orkstackv1.ListNamespacesResponse {
	t.Helper()
	resp, err := svc.ListNamespaces(context.Background(), connect.NewRequest(&orkstackv1.ListNamespacesRequest{
		PageSize: size, PageToken: token, NameContains: contains,
	}))
	if err != nil {
		t.Fatalf("ListNamespaces(size=%d token=%q contains=%q): %v", size, token, contains, err)
	}
	return resp.Msg
}

// walkNamespacePages follows next_page_token to the end and returns each
// page's names.
func walkNamespacePages(t *testing.T, svc *api.Service, size int32, contains string) [][]string {
	t.Helper()
	var pages [][]string
	token := ""
	for range 100 {
		page := listNamespacePage(t, svc, size, token, contains)
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
			got := walkNamespacePages(t, svc, tc.size, "")
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("pages = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestListNamespaces_APageReadsOnlyItsOwnDocuments is the cost the paging
// exists for: one name listing and page_size document reads, never a read of
// every namespace.
func TestListNamespaces_APageReadsOnlyItsOwnDocuments(t *testing.T) {
	names := make([]string, 0, 40)
	for i := range 40 {
		names = append(names, fmt.Sprintf("tenant-%02d", i))
	}
	svc, reg := pagedService(t, names...)

	first := listNamespacePage(t, svc, 3, "", "")
	if got := namespaceNames(first.GetNamespaces()); fmt.Sprint(got) != "[default tenant-00 tenant-01]" {
		t.Fatalf("first page = %v, want [default tenant-00 tenant-01]", got)
	}
	gets, lists := reg.reads()
	if lists != 0 || len(gets) != 3 {
		t.Fatalf("a page of 3 out of 41 made %d full listings and %d reads (%v), want 0 and 3",
			lists, len(gets), gets)
	}
	for _, ns := range first.GetNamespaces() {
		if ns.GetDescription() == "" {
			t.Errorf("page entry %q came back without its document", ns.GetName())
		}
	}
	t.Logf("page of 3 of 41: %d document reads, %d full listings", len(gets), lists)
}

// TestListNamespaces_NameOrderNotKeyOrder: `a-b.json` sorts before `a.json`
// in a bucket, but `a` sorts before `a-b` by name. Pages follow names, so a
// cursor on `a` must still yield `a-b` and `ab` next, never skip or repeat.
func TestListNamespaces_NameOrderNotKeyOrder(t *testing.T) {
	svc, _ := pagedService(t, "ab", "a-b", "a")

	got := walkNamespacePages(t, svc, 1, "a")
	want := [][]string{{"a"}, {"a-b"}, {"ab"}, {"default"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pages filtered by %q = %v, want %v", "a", got, want)
	}
}

// TestListNamespaces_NameContainsFilters: the filter is a substring of the
// name, `default` included or excluded by the same rule as any other name,
// and it applies with or without paging.
func TestListNamespaces_NameContainsFilters(t *testing.T) {
	svc, _ := pagedService(t, "payments", "pay-later", "billing", "fault-lab")

	cases := []struct {
		contains string
		want     []string
	}{
		{"pay", []string{"pay-later", "payments"}},
		{"fault", []string{"default", "fault-lab"}}, // default included
		{"bill", []string{"billing"}},               // default excluded
		{"default", []string{"default"}},
		{"nothing-matches", nil},
		{"Pay", nil}, // names are lowercase; matching is exact
	}
	for _, tc := range cases {
		for _, size := range []int32{0, 1, 10} {
			t.Run(fmt.Sprintf("%s/size=%d", tc.contains, size), func(t *testing.T) {
				var got []string
				for _, page := range walkNamespacePages(t, svc, size, tc.contains) {
					got = append(got, page...)
				}
				if fmt.Sprint(got) != fmt.Sprint(tc.want) {
					t.Fatalf("name_contains %q = %v, want %v", tc.contains, got, tc.want)
				}
			})
		}
	}
}

// TestListNamespaces_RefusesWhatItCannotAnswer: a negative size, a token from
// another filter, a token with page_size 0, a token that is not one, and a
// filter longer than any name are all INVALID_ARGUMENT — never a silent
// first page.
func TestListNamespaces_RefusesWhatItCannotAnswer(t *testing.T) {
	svc, _ := pagedService(t, "payments", "pay-later", "billing")
	token := listNamespacePage(t, svc, 1, "", "pay").GetNextPageToken()
	if token == "" {
		t.Fatal("a one-entry page of two matches carried no next_page_token")
	}
	unfiltered := listNamespacePage(t, svc, 1, "", "").GetNextPageToken()

	cases := []struct {
		name     string
		req      *orkstackv1.ListNamespacesRequest
		mentions string
	}{
		{"negative size", &orkstackv1.ListNamespacesRequest{PageSize: -1}, "page_size"},
		{"token from another filter", &orkstackv1.ListNamespacesRequest{
			PageSize: 1, PageToken: token, NameContains: "bill"}, "page_token"},
		{"filtered token, filter dropped", &orkstackv1.ListNamespacesRequest{
			PageSize: 1, PageToken: token}, "page_token"},
		{"unfiltered token, filter added", &orkstackv1.ListNamespacesRequest{
			PageSize: 1, PageToken: unfiltered, NameContains: "pay"}, "page_token"},
		{"token with page_size 0", &orkstackv1.ListNamespacesRequest{
			PageToken: token, NameContains: "pay"}, "page_size"},
		{"not a token", &orkstackv1.ListNamespacesRequest{PageSize: 1, PageToken: "%%%"}, "page_token"},
		{"forged token version", &orkstackv1.ListNamespacesRequest{
			PageSize: 1, PageToken: forgeNamespaceCursor(t, 2, "", "billing")}, "page_token"},
		{"filter longer than a name", &orkstackv1.ListNamespacesRequest{
			NameContains: strings.Repeat("a", 201)}, "name_contains"},
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

	// The token round-trips: the same filter continues where the page ended.
	next := listNamespacePage(t, svc, 1, token, "pay")
	if got := namespaceNames(next.GetNamespaces()); fmt.Sprint(got) != "[payments]" || next.GetNextPageToken() != "" {
		t.Fatalf("second page = %v (token %q), want [payments] and no token", got, next.GetNextPageToken())
	}
	// A 200-byte filter is the longest a name can be, so it is allowed.
	listNamespacePage(t, svc, 0, "", strings.Repeat("a", 200))
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

	first := listNamespacePage(t, svc, 2, "", "")
	if got := namespaceNames(first.GetNamespaces()); fmt.Sprint(got) != "[alpha]" {
		t.Fatalf("page with a vanished name = %v, want [alpha]", got)
	}
	rest := listNamespacePage(t, svc, 2, first.GetNextPageToken(), "")
	if got := namespaceNames(rest.GetNamespaces()); fmt.Sprint(got) != "[charlie default]" {
		t.Fatalf("page after the vanished name = %v, want [charlie default]", got)
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
		size     int32
		contains string
		want     string
	}{
		{0, "", "[default]"},
		{1, "", "[default]"},
		{25, "fault", "[default]"},
		{25, "payments", "[]"},
	}
	for _, tc := range cases {
		page := listNamespacePage(t, svc, tc.size, "", tc.contains)
		if got := fmt.Sprint(namespaceNames(page.GetNamespaces())); got != tc.want || page.GetNextPageToken() != "" {
			t.Errorf("size %d contains %q = %s (token %q), want %s and no token",
				tc.size, tc.contains, got, page.GetNextPageToken(), tc.want)
		}
	}
	if _, err := svc.ListNamespaces(context.Background(), connect.NewRequest(
		&orkstackv1.ListNamespacesRequest{PageSize: -1})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("negative page_size without a registry: code %v, want InvalidArgument", connect.CodeOf(err))
	}
}
