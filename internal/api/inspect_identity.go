package api

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The bounds of the identity state one InspectRun reads: pages a task (or
// item) of records, and the time all of them may take.
const (
	maxIdentityPages  = 8
	identityReadBound = 2 * time.Second
)

// fillLiveIdentities gives each started attempt the state of the
// identities it was issued (LiveAttempt.identity): for each name, when it was
// last minted and when that expires, from the records of what the mint
// issued, FRESH until then and EXPIRED after. An Orker that reports its
// refreshes says more (RETRYING), once it does. The records of a task (or
// an item) are listed once, for all its attempts. An engine that keeps no
// records issues no token and fills nothing; records that cannot be read,
// cannot be trusted or run past the bound leave those attempts' identities
// unset, logged, never a partial or empty claim.
func (s *Service) fillLiveIdentities(ctx context.Context, run *orkstackv1.Run, attempts []*orkstackv1.LiveAttempt, now time.Time) {
	if s.issuedRecords == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, identityReadBound)
	defer cancel()
	type scope struct {
		task string
		item artifacts.Item
	}
	byScope := map[scope][]*orkstackv1.LiveAttempt{}
	var scopes []scope
	for _, a := range attempts {
		if a.GetAttemptStartedAt() == nil {
			continue
		}
		sc := scope{task: a.GetTaskId()}
		if a.Item != nil {
			sc.item = artifacts.Item{Index: a.GetItem(), Set: true}
		}
		if _, seen := byScope[sc]; !seen {
			scopes = append(scopes, sc)
		}
		byScope[sc] = append(byScope[sc], a)
	}
	for _, sc := range scopes {
		records, ok := s.readIssuedFor(ctx, run, sc.task, sc.item)
		if !ok {
			continue
		}
		for _, a := range byScope[sc] {
			latest := map[string]artifacts.IssuedEntry{}
			var order []string
			for _, r := range records {
				if r.Attempt != a.GetAttempt() {
					continue
				}
				for _, e := range r.Tokens {
					if _, seen := latest[e.Name]; !seen {
						order = append(order, e.Name)
					}
					if e.IssuedAtUnix >= latest[e.Name].IssuedAtUnix {
						latest[e.Name] = e
					}
				}
			}
			// What the Orker's beat said of an identity stands (it knows a
			// refresh is retrying); the records add only the identities the
			// beat did not name.
			said := map[string]bool{}
			for _, l := range a.GetIdentity() {
				said[l.GetName()] = true
			}
			for _, name := range order {
				if said[name] {
					continue
				}
				e := latest[name]
				state := orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_FRESH
				expires := time.Unix(e.ExpiresAtUnix, 0).UTC()
				if !now.Before(expires) {
					state = orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_EXPIRED
				}
				a.Identity = append(a.Identity, &orkstackv1.LiveIdentity{Name: name, Kind: liveIdentityKind,
					LastRefresh: timestamppb.New(time.Unix(e.IssuedAtUnix, 0).UTC()), ExpiresAt: timestamppb.New(expires), State: state})
			}
		}
	}
}

// liveIdentityKind is LiveIdentity.kind for a task's identity token.
const liveIdentityKind = "identity"

// readIssuedFor reads every record of what a task (or an item) was issued,
// within maxIdentityPages pages; ok is false, logged, when a page could not
// be read, a record cannot be trusted, or the records run past the bound.
func (s *Service) readIssuedFor(ctx context.Context, run *orkstackv1.Run, task string, item artifacts.Item) ([]artifacts.IssuedRecord, bool) {
	q := artifacts.IssuedQuery{Namespace: artifacts.ResolveNamespace(run.GetNamespace()), RunID: run.GetId(), TaskID: task,
		Item: item, Limit: artifacts.MaxIssuedPage}
	var out []artifacts.IssuedRecord
	for page := 0; page < maxIdentityPages; page++ {
		reads, next, err := s.issuedRecords.ListIssued(ctx, q)
		if err != nil {
			s.logger.Warn("an attempt's identity state could not be read; it is not shown", "run", run.GetId(), "task", task, "error", err)
			return nil, false
		}
		for _, r := range reads {
			if r.Answer != artifacts.RecordFound {
				s.logger.Warn("a record of what an attempt was issued cannot be trusted; its identity state is not shown",
					"run", run.GetId(), "task", task, "object", r.Object, "reason", r.Reason)
				return nil, false
			}
			out = append(out, r.Value)
		}
		if next == "" {
			return out, true
		}
		q.After = next
	}
	s.logger.Warn("an attempt's identity records run past what one inspect reads; its identity state is not shown",
		"run", run.GetId(), "task", task, "pages", maxIdentityPages)
	return nil, false
}
