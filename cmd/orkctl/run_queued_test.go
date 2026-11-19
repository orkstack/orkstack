package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestQueuedLine: `run get`'s Queued line says, for each reason a run waits
// for its concurrency slot, where it stands, behind whom and until when, in
// the words the UI and docs/CLI.md use.
func TestQueuedLine(t *testing.T) {
	at := func(s string) *timestamppb.Timestamp {
		ts, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return timestamppb.New(ts)
	}
	base := func(cause orkstackv1.ConcurrencyQueueCause) *orkstackv1.ConcurrencyQueued {
		return &orkstackv1.ConcurrencyQueued{Key: "db-migrate:prod", Limit: 1, Position: 2, PositionKnown: true,
			HoldersAhead: []string{"ork-01"}, HoldersCount: 1, QueuedSince: at("2026-11-19T10:02:00Z"),
			TimesOutAt: at("2026-11-19T12:02:00Z"), Cause: cause}
	}
	cases := []struct {
		name string
		q    *orkstackv1.ConcurrencyQueued
		want string
	}{
		{"arrival", base(orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_ARRIVAL),
			"queued for key db-migrate:prod (limit 1, 2nd in line) behind ork-01; since 2026-11-19T10:02:00Z, times out 2026-11-19T12:02:00Z"},
		{"unpause", base(orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_UNPAUSE),
			"waiting again for key db-migrate:prod after an unpause (limit 1, 2nd in line) behind ork-01; since 2026-11-19T10:02:00Z, times out 2026-11-19T12:02:00Z"},
		{"resume", base(orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_RESUME),
			"queued for key db-migrate:prod after a resume (limit 1, 2nd in line) behind ork-01; since 2026-11-19T10:02:00Z, times out 2026-11-19T12:02:00Z"},
		{"failing", base(orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_FAILING),
			"waiting for its concurrency slot to run its failure handlers (key db-migrate:prod, limit 1, 2nd in line) behind ork-01; since 2026-11-19T10:02:00Z, times out 2026-11-19T12:02:00Z"},
		{"place unknown, with its note", func() *orkstackv1.ConcurrencyQueued {
			q := base(orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_ARRIVAL)
			q.PositionKnown, q.Position, q.NoteCode, q.Note = false, 0, "NOT_ANSWERING", "the limit is not answering; this run stays queued and asks again by 2026-11-19T10:17:00Z"
			return q
		}(), "queued for key db-migrate:prod (limit 1, place unknown) behind ork-01; since 2026-11-19T10:02:00Z, times out 2026-11-19T12:02:00Z; the limit is not answering; this run stays queued and asks again by 2026-11-19T10:17:00Z"},
		{"the limit reading its holders", func() *orkstackv1.ConcurrencyQueued {
			q := base(orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_ARRIVAL)
			q.PositionKnown, q.Position, q.HoldersAhead, q.HoldersCount = false, 0, nil, 0
			q.NoteCode, q.Note = "REBUILDING", "waiting while the limit on key db-migrate:prod reads which runs hold it (a first run at a key, or after its record was lost)"
			return q
		}(), "queued for key db-migrate:prod (limit 1, place unknown); since 2026-11-19T10:02:00Z, times out 2026-11-19T12:02:00Z; waiting while the limit on key db-migrate:prod reads which runs hold it (a first run at a key, or after its record was lost)"},
		{"more holders than named, as of arrival", func() *orkstackv1.ConcurrencyQueued {
			q := base(orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_ARRIVAL)
			q.Limit, q.Position, q.HoldersAhead, q.HoldersCount, q.HoldersAsOfArrival = 3, 1, []string{"ork-01", "ork-02"}, 3, true
			return q
		}(), "queued for key db-migrate:prod (limit 3, 1st in line) behind ork-01, ork-02 and 1 more (as of its arrival); since 2026-11-19T10:02:00Z, times out 2026-11-19T12:02:00Z"},
	}
	for _, c := range cases {
		if got := queuedLine(c.q); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.name, got, c.want)
		}
	}
}

// TestRenderRun_SaysWhyItWaitsForItsSlot: a queued run's detail carries its
// Queued line, and a run that does not wait carries none.
func TestRenderRun_SaysWhyItWaitsForItsSlot(t *testing.T) {
	run := &orkstackv1.Run{Id: "ork-x", Status: orkstackv1.Status_STATUS_PENDING,
		Queued: &orkstackv1.ConcurrencyQueued{Key: "k", Limit: 1, Position: 1, PositionKnown: true}}
	var b bytes.Buffer
	must.OK(t, renderRun(&b, run))
	if !strings.Contains(b.String(), "Queued:") || !strings.Contains(b.String(), "queued for key k (limit 1, 1st in line)") {
		t.Fatalf("output lacks the Queued line:\n%s", b.String())
	}
	b.Reset()
	run.Queued = nil
	must.OK(t, renderRun(&b, run))
	if strings.Contains(b.String(), "Queued:") {
		t.Fatalf("a run that does not wait printed a Queued line:\n%s", b.String())
	}
}
