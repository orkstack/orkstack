package api

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// principalsFixture is an access handler whose audit trail holds sign-in
// events written at fixed instants before now.
type principalsFixture struct {
	s   *Service
	h   accessHandler
	now time.Time
}

func newPrincipalsFixture(t *testing.T) *principalsFixture {
	t.Helper()
	s, h := accessWriter(t)
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	return &principalsFixture{s: s, h: h, now: now}
}

// signin writes one audit record of action for principal, ago before now.
func (f *principalsFixture) signin(t *testing.T, ago time.Duration, action, principal, issuer string) {
	t.Helper()
	at := f.now.Add(-ago)
	rec := &orkstackv1.AuditRecord{SchemaVersion: artifacts.AuditSchemaVersionVerified, Id: artifacts.NewAuditID(at),
		At: timestamppb.New(at), Action: action, Principal: principal, PrincipalVerified: principal != "",
		PrincipalIssuer: issuer, Succeeded: action != "auth.signin.refused", Target: "-", Procedure: "sign-in"}
	if err := f.s.auditStore.(*artifacts.MemoryAuditStore).PutAuditRecord(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
}

func (f *principalsFixture) list(t *testing.T, req *orkstackv1.ListPrincipalsRequest) (*orkstackv1.ListPrincipalsResponse, error) {
	t.Helper()
	resp, err := f.h.ListPrincipals(asAlice(authn.ModeEnforce), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func seenIDs(ps []*orkstackv1.SeenPrincipal) string {
	var out []string
	for _, p := range ps {
		out = append(out, p.GetId())
	}
	return strings.Join(out, ",")
}

func principalFilters(t *testing.T, texts ...string) []*orkstackv1.FieldFilter {
	t.Helper()
	var out []*orkstackv1.FieldFilter
	for _, text := range texts {
		f, err := listfilter.Parse(text)
		if err != nil {
			t.Fatalf("parse %q: %v", text, err)
		}
		out = append(out, f)
	}
	return out
}

func TestListPrincipals_FromTheWindowsSigninEvents(t *testing.T) {
	f := newPrincipalsFixture(t)
	f.signin(t, 72*time.Hour, "auth.signin.created", "user:corp:bob", "corp")
	f.signin(t, 2*time.Hour, "auth.signin.refreshed", "user:corp:bob", "corp")
	f.signin(t, 30*time.Minute, "auth.signin.ended", "user:corp:bob", "corp") // a sign-out is not a sighting
	f.signin(t, 5*time.Hour, "auth.signin.created", "user:people:vera", "people")
	f.signin(t, 1*time.Hour, "namespace.update", "user:corp:alice", "corp")      // not a sign-in
	f.signin(t, 3*time.Hour, "auth.signin.refused", "", "corp")                  // names nobody
	f.signin(t, 4*time.Hour, "auth.signin.refused", "user:corp:mallory", "corp") // did not sign in
	f.signin(t, 40*24*time.Hour, "auth.signin.created", "user:corp:old", "corp")

	got, err := f.list(t, &orkstackv1.ListPrincipalsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	// Newest sign-in first; the default window is thirty days.
	if seenIDs(got.GetPrincipals()) != "user:corp:bob,user:people:vera" {
		t.Fatalf("principals = %s", seenIDs(got.GetPrincipals()))
	}
	bob := got.GetPrincipals()[0]
	if bob.GetIssuer() != "corp" || !bob.GetFirstSeen().AsTime().Equal(f.now.Add(-72*time.Hour)) || !bob.GetLastSeen().AsTime().Equal(f.now.Add(-2*time.Hour)) {
		t.Errorf("bob = %v", bob)
	}
	if !got.GetWindowStart().AsTime().Equal(f.now.Add(-DefaultPrincipalsWindow)) {
		t.Errorf("window_start = %v", got.GetWindowStart().AsTime())
	}
	if got.GetNextPageToken() != "" {
		t.Errorf("next_page_token = %q, want empty", got.GetNextPageToken())
	}

	// A narrower window drops what came before it.
	narrow, err := f.list(t, &orkstackv1.ListPrincipalsRequest{Window: durationpb.New(4 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if seenIDs(narrow.GetPrincipals()) != "user:corp:bob" || !narrow.GetPrincipals()[0].GetFirstSeen().AsTime().Equal(f.now.Add(-2*time.Hour)) {
		t.Errorf("4h window = %v", narrow.GetPrincipals())
	}
}

func TestListPrincipals_MarksADisabledPrincipal(t *testing.T) {
	f := newPrincipalsFixture(t)
	ctx := asAlice(authn.ModeEnforce)
	etag, _ := etagOf(t, f.h)
	if _, err := f.h.SetPrincipalDisabled(ctx, connect.NewRequest(&orkstackv1.SetPrincipalDisabledRequest{PrincipalId: "user:corp:mallory", Disabled: true, Etag: etag})); err != nil {
		t.Fatal(err)
	}
	f.signin(t, time.Hour, "auth.signin.created", "user:corp:mallory", "corp")
	f.signin(t, 2*time.Hour, "auth.signin.created", "user:corp:bob", "corp")
	got, err := f.list(t, &orkstackv1.ListPrincipalsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if seenIDs(got.GetPrincipals()) != "user:corp:mallory,user:corp:bob" || !got.GetPrincipals()[0].GetDisabled() || got.GetPrincipals()[1].GetDisabled() {
		t.Fatalf("principals = %v", got.GetPrincipals())
	}
}

func TestListPrincipals_FiltersAndPages(t *testing.T) {
	f := newPrincipalsFixture(t)
	f.signin(t, 1*time.Hour, "auth.signin.created", "user:corp:a", "corp")
	f.signin(t, 2*time.Hour, "auth.signin.created", "user:people:b", "people")
	f.signin(t, 3*time.Hour, "auth.signin.created", "user:corp:c", "corp")
	f.signin(t, 3*time.Hour, "auth.signin.created", "user:corp:d", "corp") // same instant: by id

	for _, tc := range []struct {
		filters []string
		want    string
	}{
		{[]string{"issuer=corp"}, "user:corp:a,user:corp:c,user:corp:d"},
		{[]string{"id^user:people:"}, "user:people:b"},
		{[]string{"issuer!=corp"}, "user:people:b"},
	} {
		got, err := f.list(t, &orkstackv1.ListPrincipalsRequest{Filters: principalFilters(t, tc.filters...)})
		if err != nil {
			t.Fatalf("%q: %v", tc.filters, err)
		}
		if seenIDs(got.GetPrincipals()) != tc.want {
			t.Errorf("%q = %s, want %s", tc.filters, seenIDs(got.GetPrincipals()), tc.want)
		}
	}

	// Pages of one walk the whole order and end with no token.
	var walked []string
	req := &orkstackv1.ListPrincipalsRequest{PageSize: 1, Filters: principalFilters(t, "issuer=corp")}
	for range 5 {
		got, err := f.list(t, req)
		if err != nil {
			t.Fatal(err)
		}
		walked = append(walked, seenIDs(got.GetPrincipals()))
		if got.GetNextPageToken() == "" {
			break
		}
		req.PageToken = got.GetNextPageToken()
	}
	if strings.Join(walked, "|") != "user:corp:a|user:corp:c|user:corp:d" {
		t.Errorf("pages = %v", walked)
	}

	// A token is one listing's: other filters, or a mangled token, are refused.
	first, err := f.list(t, &orkstackv1.ListPrincipalsRequest{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.list(t, &orkstackv1.ListPrincipalsRequest{PageSize: 1, PageToken: first.GetNextPageToken(), Filters: principalFilters(t, "issuer=corp")}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("a token under other filters = %v, want invalid_argument", err)
	}
	if _, err := f.list(t, &orkstackv1.ListPrincipalsRequest{PageToken: "garbage"}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("a mangled token = %v, want invalid_argument", err)
	}
}

func TestListPrincipals_RefusesWhatItCannotAnswer(t *testing.T) {
	f := newPrincipalsFixture(t)
	for name, req := range map[string]*orkstackv1.ListPrincipalsRequest{
		"a negative window":     {Window: durationpb.New(-time.Hour)},
		"a page over the most":  {PageSize: MaxPrincipalsPageSize + 1},
		"a negative page size":  {PageSize: -1},
		"a filter not declared": {Filters: principalFilters(t, "groups=x")},
	} {
		if _, err := f.list(t, req); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s = %v, want invalid_argument", name, err)
		}
	}
	policies := f.s.access.Policies
	f.s.access.Policies = nil
	if _, err := f.list(t, &orkstackv1.ListPrincipalsRequest{}); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("no access document = %v, want failed_precondition: disabled is never guessed", err)
	}
	f.s.access.Policies = policies
	f.s.auditStore = nil
	if _, err := f.list(t, &orkstackv1.ListPrincipalsRequest{}); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("no audit store = %v, want failed_precondition", err)
	}
}

func TestSeenPrincipals_MergesLiveSignins(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	events := []*orkstackv1.AuditRecord{
		{Action: "auth.signin.created", Principal: "user:corp:bob", PrincipalIssuer: "corp", At: timestamppb.New(now.Add(-5 * time.Hour))},
	}
	live := []authn.SigninListing{
		{PrincipalID: "user:corp:bob", Display: "Bob", Issuer: "corp", Groups: []string{"eng"}, CreatedAt: now.Add(-time.Hour)},
		{PrincipalID: "user:corp:bob", Display: "Bob", Issuer: "corp", Groups: []string{"eng", "ops"}, CreatedAt: now.Add(-30 * time.Minute)},
		// A live sign-in whose events fell outside the window is still seen.
		{PrincipalID: "user:people:vera", Display: "vera@example.test", Issuer: "people", CreatedAt: now.Add(-2 * time.Hour)},
		// One created before the window is active, but widens nothing.
		{PrincipalID: "user:corp:old", Display: "Old", Issuer: "corp", CreatedAt: now.Add(-48 * time.Hour)},
	}
	got := seenPrincipals(events, live, now.Add(-24*time.Hour), now, func(id string) bool { return id == "user:people:vera" })
	if seenIDs(got) != "user:corp:bob,user:people:vera,user:corp:old" {
		t.Fatalf("seen = %s", seenIDs(got))
	}
	bob, vera := got[0], got[1]
	if bob.GetDisplay() != "Bob" || strings.Join(bob.GetGroups(), ",") != "eng,ops" || bob.GetActiveSignins() != 2 ||
		!bob.GetFirstSeen().AsTime().Equal(now.Add(-5*time.Hour)) || !bob.GetLastSeen().AsTime().Equal(now.Add(-30*time.Minute)) {
		t.Errorf("bob = %v", bob)
	}
	if !vera.GetDisabled() || vera.GetActiveSignins() != 1 || vera.GetIssuer() != "people" {
		t.Errorf("vera = %v", vera)
	}
	if old := got[2]; old.GetFirstSeen() != nil || old.GetLastSeen() != nil || old.GetActiveSignins() != 1 || old.GetDisplay() != "Old" {
		t.Errorf("a sign-in from before the window = %v, want active with no sighting", old)
	}
}

// FuzzDecodePrincipalsCursor holds the page token's parser to what it
// claims: any input is refused or accepted without a panic; an accepted
// token names instants that parse; and every token the handler mints reads
// back to what it carried, under the filter set it was minted for.
func FuzzDecodePrincipalsCursor(f *testing.F) {
	minted, err := encodePrincipalsCursor(principalsCursor{
		Filters: "", WindowSince: "2026-10-19T12:00:00Z", WindowUntil: "2026-11-18T12:00:00Z",
		AfterSeen: "2026-11-18T11:00:00Z", AfterID: "user:corp:a",
	})
	if err != nil {
		f.Fatal(err)
	}
	for _, seed := range []string{minted, "", "garbage", principalsCursorVersion, principalsCursorVersion + "e30", "v2:e30"} {
		f.Add(seed, "user:corp:a")
	}
	set, err := listfilter.SpecFor(&orkstackv1.ListPrincipalsRequest{})
	if err != nil {
		f.Fatal(err)
	}
	empty, err := set.Compile(nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, token, id string) {
		if c, err := decodePrincipalsCursor(token, empty); err == nil {
			if !c.until.After(c.since) || c.until.Sub(c.since) > MaxPrincipalsWindow || c.afterSeen.After(c.until) || c.afterID == "" {
				t.Fatalf("accepted %q naming a window no listing reads: %+v", token, c)
			}
		} else if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("refused %q with %v, want invalid_argument", token, err)
		}
		if !utf8.ValidString(id) {
			return // a principal id is valid UTF-8 (internal/authn/principal.go); JSON would replace the rest
		}
		want := principalsCursor{WindowSince: "2026-10-19T12:00:00Z", WindowUntil: "2026-11-18T12:00:00Z", AfterSeen: "2026-11-18T11:00:00Z", AfterID: id}
		tok, err := encodePrincipalsCursor(want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodePrincipalsCursor(tok, empty)
		if id == "" {
			if err == nil {
				t.Fatal("a token naming no last row was accepted")
			}
			return
		}
		if err != nil || got.afterID != id || got.since.Format(time.RFC3339) != want.WindowSince || got.until.Format(time.RFC3339) != want.WindowUntil {
			t.Fatalf("a minted token did not read back: %+v, %v", got, err)
		}
	})
}

// budgetStore is an audit store that refuses every listing for its scan
// budget: the fault a real store produces only after examining 5000 records
// of one day, which a unit test cannot write cheaply.
type budgetStore struct{ artifacts.AuditStore }

func (budgetStore) ListAuditRecords(context.Context, artifacts.AuditQuery) (artifacts.AuditPage, error) {
	return artifacts.AuditPage{}, fmt.Errorf("walk: %w", artifacts.ErrAuditScanBudget)
}

func TestListPrincipals_RefusesForgedTokensAndAnOverBudgetWindow(t *testing.T) {
	f := newPrincipalsFixture(t)
	mint := func(c principalsCursor) string {
		tok, err := encodePrincipalsCursor(c)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	until := f.now.Format(time.RFC3339Nano)
	hourAgo := f.now.Add(-time.Hour).Format(time.RFC3339Nano)
	for name, tok := range map[string]string{
		"a window that ends before it starts": mint(principalsCursor{WindowSince: until, WindowUntil: hourAgo, AfterSeen: hourAgo, AfterID: "a"}),
		"a window longer than the most":       mint(principalsCursor{WindowSince: "0001-01-01T00:00:00Z", WindowUntil: until, AfterSeen: until, AfterID: "a"}),
		"a last row after the window":         mint(principalsCursor{WindowSince: hourAgo, WindowUntil: until, AfterSeen: f.now.Add(time.Hour).Format(time.RFC3339Nano), AfterID: "a"}),
		"no last row":                         mint(principalsCursor{WindowSince: hourAgo, WindowUntil: until, AfterSeen: until}),
		"a window long past":                  mint(principalsCursor{WindowSince: f.now.Add(-200 * 24 * time.Hour).Format(time.RFC3339Nano), WindowUntil: f.now.Add(-199 * 24 * time.Hour).Format(time.RFC3339Nano), AfterSeen: f.now.Add(-199 * 24 * time.Hour).Format(time.RFC3339Nano), AfterID: "a"}),
		"a window ending ahead of now":        mint(principalsCursor{WindowSince: hourAgo, WindowUntil: f.now.Add(time.Hour).Format(time.RFC3339Nano), AfterSeen: until, AfterID: "a"}),
	} {
		if _, err := f.list(t, &orkstackv1.ListPrincipalsRequest{PageToken: tok}); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s = %v, want invalid_argument", name, err)
		}
	}
	// A token is read with its own window; another one asked for is refused.
	f.signin(t, time.Hour, "auth.signin.created", "user:corp:a", "corp")
	f.signin(t, 2*time.Hour, "auth.signin.created", "user:corp:b", "corp")
	first, err := f.list(t, &orkstackv1.ListPrincipalsRequest{PageSize: 1, Window: durationpb.New(48 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.list(t, &orkstackv1.ListPrincipalsRequest{PageSize: 1, PageToken: first.GetNextPageToken(), Window: durationpb.New(48 * time.Hour)}); err != nil {
		t.Errorf("the same window again = %v", err)
	}
	if _, err := f.list(t, &orkstackv1.ListPrincipalsRequest{PageSize: 1, PageToken: first.GetNextPageToken(), Window: durationpb.New(time.Hour)}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("another window = %v, want invalid_argument", err)
	}
	// A window longer than the most is cut to it, and window_start says so.
	long, err := f.list(t, &orkstackv1.ListPrincipalsRequest{Window: durationpb.New(1000 * 24 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if !long.GetWindowStart().AsTime().Equal(f.now.Add(-MaxPrincipalsWindow)) {
		t.Errorf("window_start = %v, want the most a window reads", long.GetWindowStart().AsTime())
	}

	f.s.auditStore = budgetStore{f.s.auditStore}
	_, err = f.list(t, &orkstackv1.ListPrincipalsRequest{})
	if connect.CodeOf(err) != connect.CodeResourceExhausted || !strings.Contains(err.Error(), "shorter window") {
		t.Errorf("an over-budget window = %v, want resource_exhausted asking for a shorter window", err)
	}
}
