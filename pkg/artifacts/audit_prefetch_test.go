package artifacts

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// serialWalkAudit is the walk as it was before bodies were prefetched: one
// body fetched and evaluated at a time. It is the oracle the prefetching walk
// must agree with, page for page, cursor for cursor and error for error.
func serialWalkAudit(ctx context.Context, src auditSource, q AuditQuery) (AuditPage, error) {
	since, until := q.Since.UTC(), q.Until.UTC()
	last := until.Add(-time.Nanosecond)
	if q.BeforeKey != "" {
		_, cursorDay, ok := ParseAuditKey(q.BeforeKey)
		if !ok {
			return AuditPage{}, ErrInvalidAuditRecord
		}
		if end := cursorDay.Add(24*time.Hour - time.Nanosecond); end.Before(last) {
			last = end
		}
	}
	examined := 0
	var page AuditPage
	err := walkDaysNewestFirst(ctx, last, since, func(day time.Time) (bool, error) {
		keys, err := src.listAuditDay(ctx, auditDayPrefix(day))
		if err != nil {
			return false, err
		}
		sort.Strings(keys)
		for i := len(keys) - 1; i >= 0; i-- {
			key := keys[i]
			if q.BeforeKey != "" && key >= q.BeforeKey {
				continue
			}
			id, _, ok := ParseAuditKey(key)
			if !ok {
				continue
			}
			stamp, _ := auditIDTime(id)
			if stamp.Before(since) {
				break
			}
			if !stamp.Before(until) {
				continue
			}
			if examined >= AuditScanMaxObjects {
				return false, ErrAuditScanBudget
			}
			examined++
			rec, ok, err := readAuditCandidate(ctx, src, key, day)
			if err != nil {
				return false, err
			}
			if !ok {
				continue
			}
			at := rec.GetAt().AsTime()
			if at.Before(since) || !at.Before(until) || !q.Matches(rec) {
				continue
			}
			page.Records = append(page.Records, rec)
			if len(page.Records) == q.Limit {
				page.NextKey = key
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return AuditPage{}, err
	}
	return page, nil
}

// latencySource is an auditSource with a per-fetch latency, a fetch counter, a
// high-water mark of concurrent fetches, and keys whose fetch fails.
type latencySource struct {
	flakySource
	latency  time.Duration
	failing  map[string]bool
	fetches  atomic.Int64
	inflight atomic.Int64
	peak     atomic.Int64
}

func (l *latencySource) fetchAudit(ctx context.Context, key string) ([]byte, bool, error) {
	l.fetches.Add(1)
	now := l.inflight.Add(1)
	defer l.inflight.Add(-1)
	for {
		peak := l.peak.Load()
		if now <= peak || l.peak.CompareAndSwap(peak, now) {
			break
		}
	}
	if l.latency > 0 {
		timer := time.NewTimer(l.latency)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, false, ctx.Err()
		}
	}
	if l.failing[key] {
		return nil, false, fmt.Errorf("503 SlowDown reading %s", key)
	}
	return l.flakySource.fetchAudit(ctx, key)
}

var (
	auditPrincipals = []string{"alice", "bob", "carol"}
	auditActions    = []string{"namespace.update", "namespace.delete", "orkflow.apply", "run.cancel"}
)

// seedAuditSource writes n records spread over days days ending at end, with
// a few vanished and undecodable bodies among them.
func seedAuditSource(t *testing.T, r *rand.Rand, n int, end time.Time, span time.Duration) *latencySource {
	t.Helper()
	src := &latencySource{flakySource: flakySource{bodies: map[string][]byte{}}, failing: map[string]bool{}}
	for i := 0; i < n; i++ {
		at := end.Add(-time.Duration(r.Int64N(int64(span))))
		rec := &orkstackv1.AuditRecord{
			SchemaVersion: AuditSchemaVersion,
			Id:            NewAuditID(at),
			At:            timestamppb.New(at),
			Principal:     auditPrincipals[r.IntN(len(auditPrincipals))],
			Action:        auditActions[r.IntN(len(auditActions))],
		}
		key, err := AuditKey(rec)
		if err != nil {
			t.Fatalf("AuditKey: %v", err)
		}
		body, err := marshalAuditExact(rec)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		switch r.IntN(40) {
		case 0:
			body = nil // vanished between listing and read
		case 1:
			body = []byte("{not json")
		}
		src.bodies[key] = body
	}
	return src
}

func samePages(a, b AuditPage) bool {
	if a.NextKey != b.NextKey || len(a.Records) != len(b.Records) {
		return false
	}
	for i := range a.Records {
		if !proto.Equal(a.Records[i], b.Records[i]) {
			return false
		}
	}
	return true
}

// Over random stores, filters, limits and windows, the prefetching walk
// returns exactly the serial walk's pages, cursors and errors.
func TestWalkAudit_PrefetchAnswersExactlyLikeTheSerialWalk(t *testing.T) {
	end := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	for seed := uint64(1); seed <= 60; seed++ {
		r := rand.New(rand.NewPCG(seed, seed*7919))
		src := seedAuditSource(t, r, 50+r.IntN(400), end, 72*time.Hour)
		if r.IntN(3) == 0 {
			// One failing fetch somewhere in the store.
			keys := make([]string, 0, len(src.bodies))
			for key := range src.bodies {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			src.failing[keys[r.IntN(len(keys))]] = true
		}
		principal := ""
		if r.IntN(2) == 0 {
			principal = auditPrincipals[r.IntN(len(auditPrincipals))]
		}
		prefix := ""
		if r.IntN(2) == 0 {
			prefix = strings.Split(auditActions[r.IntN(len(auditActions))], ".")[0] + "."
		}
		q := AuditQuery{
			Since: end.Add(-time.Duration(1+r.IntN(80)) * time.Hour),
			Until: end.Add(-time.Duration(r.IntN(10)) * time.Hour),
			Limit: 1 + r.IntN(25),
			Keep: func(rec *orkstackv1.AuditRecord) bool {
				return (principal == "" || rec.GetPrincipal() == principal) &&
					strings.HasPrefix(rec.GetAction(), prefix)
			},
		}
		if !q.Until.After(q.Since) {
			continue
		}
		for pageNo := 0; ; pageNo++ {
			want, wantErr := serialWalkAudit(context.Background(), src, q)
			got, gotErr := walkAudit(context.Background(), src, q)
			if (wantErr == nil) != (gotErr == nil) || (wantErr != nil && wantErr.Error() != gotErr.Error()) {
				t.Fatalf("seed %d page %d: error %v, serial walk %v", seed, pageNo, gotErr, wantErr)
			}
			if wantErr != nil {
				break
			}
			if !samePages(got, want) {
				t.Fatalf("seed %d page %d: %d records next %q, serial walk %d records next %q",
					seed, pageNo, len(got.Records), got.NextKey, len(want.Records), want.NextKey)
			}
			if want.NextKey == "" {
				break
			}
			q.BeforeKey = want.NextKey
		}
	}
}

// The budget counts EVALUATED keys, so it trips at exactly the serial walk's
// point, and a match inside the budget is found exactly as before.
func TestWalkAudit_PrefetchKeepsTheScanBudget(t *testing.T) {
	end := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	r := rand.New(rand.NewPCG(5, 5))
	src := seedAuditSource(t, r, AuditScanMaxObjects+13, end, 6*time.Hour)
	for key, body := range src.bodies {
		if body == nil {
			src.bodies[key] = []byte("{}")
		}
	}
	q := AuditQuery{Since: end.Add(-7 * time.Hour), Until: end.Add(time.Hour), Limit: 10,
		Keep: func(*orkstackv1.AuditRecord) bool { return false }}
	if _, err := walkAudit(context.Background(), src, q); !errors.Is(err, ErrAuditScanBudget) {
		t.Fatalf("a filter matching nothing over %d keys answered %v, want ErrAuditScanBudget", AuditScanMaxObjects+13, err)
	}
	if fetched := src.fetches.Load(); fetched != AuditScanMaxObjects {
		t.Errorf("fetched %d bodies before refusing, want exactly the budget (%d)", fetched, AuditScanMaxObjects)
	}
}

// A filtered scan over a slow store fetches eight bodies at a time.
func TestWalkAudit_AFilteredScanFetchesEightBodiesAtATime(t *testing.T) {
	end := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	r := rand.New(rand.NewPCG(9, 9))
	const n, latency = 160, 10 * time.Millisecond
	src := seedAuditSource(t, r, n, end, 2*time.Hour)
	src.latency = latency
	q := AuditQuery{Since: end.Add(-3 * time.Hour), Until: end.Add(time.Hour), Limit: 50,
		Keep: func(*orkstackv1.AuditRecord) bool { return false }}

	started := time.Now()
	if _, err := walkAudit(context.Background(), src, q); err != nil {
		t.Fatalf("walkAudit: %v", err)
	}
	elapsed := time.Since(started)
	if peak := src.peak.Load(); peak != auditPrefetchWindow {
		t.Errorf("peak concurrent fetches = %d, want %d", peak, auditPrefetchWindow)
	}
	if serial := time.Duration(n) * latency; elapsed > serial/3 {
		t.Errorf("a %d-key filtered scan took %s; one at a time it takes %s", n, elapsed, serial)
	}
}
