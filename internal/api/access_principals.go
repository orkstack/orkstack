package api

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The People list (AccessService.ListPrincipals) is derived on read from the
// audit trail's sign-in events over a window and the sign-in records alive
// now. Nothing stores a list of principals: that would be an index, which
// AGENTS.md §7 does not allow. Each page reads the whole window again, so
// the window is bounded (MaxPrincipalsWindow), and memory is bounded by the
// principals seen, never by the events read.
const (
	// DefaultPrincipalsWindow is how far back sign-in events are read when the
	// request names no window.
	DefaultPrincipalsWindow = 30 * 24 * time.Hour
	// MaxPrincipalsWindow is the longest window read; a longer one is cut to it,
	// and window_start says where the answer began.
	MaxPrincipalsWindow = 90 * 24 * time.Hour
	// DefaultPrincipalsPageSize and MaxPrincipalsPageSize bound one page.
	DefaultPrincipalsPageSize = 100
	MaxPrincipalsPageSize     = 500
	// signinCreated and signinRefreshed are the events of a principal signing
	// in or keeping a sign-in alive: the only sightings. An ended sign-in (a
	// sign-out, an expiry, a revocation) and a refused one are not.
	signinCreated   = "auth.signin.created"
	signinRefreshed = "auth.signin.refreshed"
	// tokenSkew is how far a page token's window may sit from now and still be
	// a listing this engine began: its end no later than now plus the skew,
	// its start no earlier than MaxPrincipalsWindow plus a day before now.
	tokenSkew = 5 * time.Minute
	// principalsEventsPage is how many sign-in events one store call returns.
	principalsEventsPage    = 200
	principalsCursorVersion = "p1:"
)

// principalsCursor is what a ListPrincipals page token carries: the filter
// set and window it was issued under, and the last row delivered, so the
// next page continues after it in the list's order.
type principalsCursor struct {
	Filters     string `json:"filters"`
	WindowSince string `json:"window_since"`
	WindowUntil string `json:"window_until"`
	AfterSeen   string `json:"after_seen"`
	AfterID     string `json:"after_id"`
}

// readCursor is a decoded page token's instants.
type readCursor struct {
	since, until, afterSeen time.Time
	afterID                 string
}

// ListPrincipals answers the principals seen to sign in over a window, newest
// sign-in first.
func (h accessHandler) ListPrincipals(ctx context.Context, req *connect.Request[orkstackv1.ListPrincipalsRequest]) (*connect.Response[orkstackv1.ListPrincipalsResponse], error) {
	msg := req.Msg
	set, err := compileFilters(msg)
	if err != nil {
		return nil, err
	}
	pageSize := int(msg.GetPageSize())
	switch {
	case pageSize == 0:
		pageSize = DefaultPrincipalsPageSize
	case pageSize < 0 || pageSize > MaxPrincipalsPageSize:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_size %d is outside 1..%d", pageSize, MaxPrincipalsPageSize))
	}
	var window time.Duration
	if w := msg.GetWindow(); w != nil {
		if err := w.CheckValid(); err != nil || w.AsDuration() < 0 {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("window must be a duration of zero or more"))
		}
		window = w.AsDuration()
	}
	store := h.svc.auditStore
	if store == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("audit store is not configured: the people seen to sign in are read from it"))
	}

	var cur *readCursor
	if token := msg.GetPageToken(); token != "" {
		if cur, err = decodePrincipalsCursor(token, set); err != nil {
			return nil, err
		}
		// The token is unsigned: a window it carries must be one a listing
		// begun lately could have read, never history further back.
		now := h.svc.now().UTC()
		if cur.until.After(now.Add(tokenSkew)) || cur.since.Before(now.Add(-MaxPrincipalsWindow-24*time.Hour)) {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("page_token has expired; list from the first page"))
		}
		// A window asked for again must be the token's: the token is one listing's.
		if window > 0 && clampWindow(window) != cur.until.Sub(cur.since) {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("page_token was issued for another window; drop it and list from the first page"))
		}
	} else {
		until := h.svc.now().UTC()
		if window == 0 {
			window = DefaultPrincipalsWindow
		}
		cur = &readCursor{since: until.Add(-clampWindow(window)), until: until}
	}

	agg := newSeenSet()
	if err := readSigninEvents(ctx, store, cur.since, cur.until, agg); err != nil {
		if errors.Is(err, artifacts.ErrAuditScanBudget) {
			return nil, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf(
				"reading the sign-ins since %s examined more than %d audit records for one step of %d sign-ins; ask for a shorter window",
				cur.since.Format(time.RFC3339), artifacts.AuditScanMaxObjects, principalsEventsPage))
		}
		return nil, readError("read the sign-in events", err)
	}
	a := h.svc.access
	if a != nil && a.Signins != nil {
		live, err := a.Signins.List(ctx, "")
		if err != nil {
			return nil, readError("read the sign-ins", err)
		}
		agg.live(live, cur.since, cur.until)
	}
	// Whether a principal is disabled is never guessed: with no document to
	// read, the list is refused rather than shown with everyone enabled.
	if a == nil || a.Policies == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("access control is not configured: who is disabled cannot be told"))
	}
	policy, err := a.Policies.Policy(ctx)
	if err != nil {
		return nil, readError("read the access document", err)
	}
	disabled := policy.Disabled

	var rows []*orkstackv1.SeenPrincipal
	for _, p := range agg.rows(disabled) {
		if set.Match(p) {
			rows = append(rows, p)
		}
	}
	start := 0
	if cur.afterID != "" {
		start = len(rows)
		for i, p := range rows {
			if comparePrincipals(p.GetLastSeen().AsTime(), p.GetId(), cur.afterSeen, cur.afterID) > 0 {
				start = i
				break
			}
		}
	}
	end := min(start+pageSize, len(rows))
	out := &orkstackv1.ListPrincipalsResponse{Principals: rows[start:end], WindowStart: timestamppb.New(cur.since)}
	if end < len(rows) {
		last := rows[end-1]
		token, err := encodePrincipalsCursor(principalsCursor{
			Filters: set.Key(), WindowSince: cur.since.Format(time.RFC3339Nano), WindowUntil: cur.until.Format(time.RFC3339Nano),
			AfterSeen: last.GetLastSeen().AsTime().UTC().Format(time.RFC3339Nano), AfterID: last.GetId(),
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		out.NextPageToken = token
	}
	return connect.NewResponse(out), nil
}

// clampWindow cuts a window to MaxPrincipalsWindow.
func clampWindow(d time.Duration) time.Duration { return min(d, MaxPrincipalsWindow) }

// readError keeps a caller's cancellation and deadline as they are; anything
// else a read returned is the store's, and unavailable.
func readError(what string, err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	}
	return connect.NewError(connect.CodeUnavailable, fmt.Errorf("%s: %w", what, err))
}

// readSigninEvents folds every sign-in of the window into agg, page by page:
// a verified principal's sign-in created or refreshed. The store refuses a
// step that examines too many records (ErrAuditScanBudget).
func readSigninEvents(ctx context.Context, store artifacts.AuditStore, since, until time.Time, agg *seenSet) error {
	before := ""
	for {
		page, err := store.ListAuditRecords(ctx, artifacts.AuditQuery{
			Since: since, Until: until, BeforeKey: before, Limit: principalsEventsPage,
			Keep: func(rec *orkstackv1.AuditRecord) bool {
				action := rec.GetAction()
				return (action == signinCreated || action == signinRefreshed) && rec.GetPrincipal() != "" && rec.GetPrincipalVerified()
			},
		})
		if err != nil {
			return err
		}
		for _, rec := range page.Records {
			agg.see(rec.GetPrincipal(), rec.GetPrincipalIssuer(), rec.GetAt().AsTime())
		}
		if page.NextKey == "" || len(page.Records) == 0 {
			return nil
		}
		before = page.NextKey
	}
}

// seenSet is one row per principal, built as events and sign-ins are read.
type seenSet struct {
	byID  map[string]*orkstackv1.SeenPrincipal
	named map[string]time.Time // when the display and groups were taken
}

func newSeenSet() *seenSet {
	return &seenSet{byID: map[string]*orkstackv1.SeenPrincipal{}, named: map[string]time.Time{}}
}

// row is id's row, made on first sight.
func (s *seenSet) row(id string) *orkstackv1.SeenPrincipal {
	p := s.byID[id]
	if p == nil {
		p = &orkstackv1.SeenPrincipal{Id: id}
		s.byID[id] = p
	}
	return p
}

// see records that id signed in at at: it widens first and last seen, and
// the latest sighting names the issuer.
func (s *seenSet) see(id, issuer string, at time.Time) {
	p := s.row(id)
	if p.GetFirstSeen() == nil || at.Before(p.GetFirstSeen().AsTime()) {
		p.FirstSeen = timestamppb.New(at)
	}
	if p.GetLastSeen() == nil || at.After(p.GetLastSeen().AsTime()) {
		p.LastSeen = timestamppb.New(at)
		if issuer != "" {
			p.Issuer = issuer
		}
	}
	if p.GetIssuer() == "" {
		p.Issuer = issuer
	}
}

// live adds the sign-ins alive now: each counts as active and gives the
// principal's name and groups, which events do not carry (the most recently
// created sign-in's win). One created inside the window is a sighting too; one
// created outside it widens nothing, so first and last seen stay inside the
// window the answer names.
func (s *seenSet) live(listed []authn.SigninListing, since, until time.Time) {
	for _, l := range listed {
		if !l.CreatedAt.Before(since) && l.CreatedAt.Before(until) {
			s.see(l.PrincipalID, l.Issuer, l.CreatedAt)
		}
		p := s.row(l.PrincipalID)
		p.ActiveSignins++
		if p.GetIssuer() == "" {
			p.Issuer = l.Issuer
		}
		if t, ok := s.named[l.PrincipalID]; !ok || l.CreatedAt.After(t) {
			s.named[l.PrincipalID] = l.CreatedAt
			p.Display, p.Groups = l.Display, slices.Clone(l.Groups)
		}
	}
}

// rows are the principals, newest sign-in first, then by id; a principal
// with a live sign-in but no sighting in the window sorts last.
func (s *seenSet) rows(disabled func(string) bool) []*orkstackv1.SeenPrincipal {
	out := make([]*orkstackv1.SeenPrincipal, 0, len(s.byID))
	for _, p := range s.byID {
		p.Disabled = disabled(p.GetId())
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b *orkstackv1.SeenPrincipal) int {
		return comparePrincipals(a.GetLastSeen().AsTime(), a.GetId(), b.GetLastSeen().AsTime(), b.GetId())
	})
	return out
}

// seenPrincipals merges events and live sign-ins as one read does.
func seenPrincipals(events []*orkstackv1.AuditRecord, live []authn.SigninListing, since, until time.Time, disabled func(string) bool) []*orkstackv1.SeenPrincipal {
	s := newSeenSet()
	for _, rec := range events {
		s.see(rec.GetPrincipal(), rec.GetPrincipalIssuer(), rec.GetAt().AsTime())
	}
	s.live(live, since, until)
	return s.rows(disabled)
}

// comparePrincipals is the list's order: last seen, newest first, then id. A
// row never seen in the window (a nil last_seen reads as the epoch) is last.
func comparePrincipals(aSeen time.Time, aID string, bSeen time.Time, bID string) int {
	if c := bSeen.Compare(aSeen); c != 0 {
		return c
	}
	return cmp.Compare(aID, bID)
}

func encodePrincipalsCursor(c principalsCursor) (string, error) {
	raw, err := stablejson.Marshal(c)
	if err != nil {
		return "", err
	}
	return principalsCursorVersion + base64.RawURLEncoding.EncodeToString(raw), nil
}

// decodePrincipalsCursor reads a page token issued under set. The token is
// the client's to hand back, so everything it carries is checked: instants
// that parse, a window that starts before it ends and is no longer than
// MaxPrincipalsWindow, and a last row inside it or never seen in it.
func decodePrincipalsCursor(token string, set listfilter.Set) (*readCursor, error) {
	invalid := connect.NewError(connect.CodeInvalidArgument, errors.New("invalid page_token"))
	body, ok := strings.CutPrefix(token, principalsCursorVersion)
	if !ok {
		return nil, invalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return nil, invalid
	}
	var c principalsCursor
	if json.Unmarshal(raw, &c) != nil {
		return nil, invalid
	}
	since, err1 := time.Parse(time.RFC3339Nano, c.WindowSince)
	until, err2 := time.Parse(time.RFC3339Nano, c.WindowUntil)
	after, err3 := time.Parse(time.RFC3339Nano, c.AfterSeen)
	if err1 != nil || err2 != nil || err3 != nil || c.AfterID == "" {
		return nil, invalid
	}
	since, until, after = since.UTC(), until.UTC(), after.UTC()
	if !until.After(since) || until.Sub(since) > MaxPrincipalsWindow || after.After(until) {
		return nil, invalid
	}
	if err := checkFilterKey(c.Filters, set); err != nil {
		return nil, err
	}
	return &readCursor{since: since, until: until, afterSeen: after, afterID: c.AfterID}, nil
}
