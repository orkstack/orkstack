package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const (
	stRec    = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_ON_RECORD
	stNot    = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_APPLIED
	stGone   = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_NOT_ON_RECORD
	stUnread = orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_UNREAD
	budget   = orkstackv1.RecordedReadStop_RECORDED_READ_STOP_READ_BUDGET
	unread   = orkstackv1.RecordedReadStop_RECORDED_READ_STOP_ROWS_UNREAD
)

// TestListRecorded: every page, waiting where the engine says so; complete
// only when every page was, and never for ever.
func TestListRecorded(t *testing.T) {
	row := &orkstackv1.RecordedResult{TaskId: "build", State: stRec}
	page := func(token string, stop orkstackv1.RecordedReadStop, rows ...*orkstackv1.RecordedResult) *orkstackv1.ListRecordedResultsResponse {
		return &orkstackv1.ListRecordedResultsResponse{Results: rows, NextPageToken: token, IncompleteReason: stop, ReadComplete: stop == 0}
	}
	busy := connect.NewError(connect.CodeResourceExhausted, errors.New("ask again"))
	down := connect.NewError(connect.CodeUnavailable, errors.New("down"))
	for _, tc := range []struct {
		name     string
		pages    []any
		rows     int
		complete bool
		waited   time.Duration
		err      error
	}{
		{"two pages", []any{page("a", 0, row), page("", 0, row)}, 2, true, 0, nil},
		{"asked too often", []any{page("a", 0, row), busy, busy, page("", 0, row)}, 2, true, 2 * time.Second, nil},
		{"the run's reads are spent", []any{page("a", budget, row), page("", 0, row)}, 2, true, 10 * time.Second, nil},
		{"a page with a row that could not be read", []any{page("a", unread, row), page("", 0, row)}, 2, false, 0, nil},
		{"pages that bring nothing", []any{page("a", 0), page("a", 0), page("a", 0), page("a", 0)}, 0, false, 0, nil},
		{"an engine that does not answer", []any{page("a", 0, row), down}, 0, false, 0, down},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var waited time.Duration
			at := 0
			rows, complete, err := listRecorded(context.Background(), func(_ context.Context, d time.Duration) error { waited += d; return nil },
				func(context.Context, string) (*orkstackv1.ListRecordedResultsResponse, error) {
					next := tc.pages[at]
					at++
					if err, ok := next.(error); ok {
						return nil, err
					}
					return next.(*orkstackv1.ListRecordedResultsResponse), nil
				})
			if !errors.Is(err, tc.err) || len(rows) != tc.rows || complete != tc.complete || waited != tc.waited {
				t.Fatalf("%d rows, complete %v, waited %v, %v", len(rows), complete, waited, err)
			}
		})
	}
}

// TestRenderRecorded: what a person reads of each row, and what to do.
func TestRenderRecorded(t *testing.T) {
	zero := int32(0)
	var out bytes.Buffer
	err := renderRecorded(&out, "team-a", "ork-1", []*orkstackv1.RecordedResult{
		{TaskId: "fan", Item: &zero, Kind: orkstackv1.RecordedResultKind_RECORDED_RESULT_KIND_COMMIT, State: stRec, OrkerId: "o-1",
			ValuesSha256: strings.Repeat("c", 64), StoredAt: timestamppb.New(time.Date(2026, 11, 1, 2, 3, 4, 0, time.UTC))},
		{TaskId: "gate", Kind: orkstackv1.RecordedResultKind_RECORDED_RESULT_KIND_RESOLUTION, State: stNot, Principal: "alice", PrincipalVerified: true,
			TaskStatus: orkstackv1.Status_STATUS_AWAITING},
		{TaskId: "gate2", Kind: orkstackv1.RecordedResultKind_RECORDED_RESULT_KIND_RESOLUTION, State: stNot, Principal: "bob", Rejected: true,
			TaskStatus: orkstackv1.Status_STATUS_FAILED, InChildRun: true},
		{TaskId: "deploy", Kind: orkstackv1.RecordedResultKind_RECORDED_RESULT_KIND_COMMIT, State: stGone},
		{Kind: orkstackv1.RecordedResultKind_RECORDED_RESULT_KIND_OUTPUTS, State: stUnread},
		{TaskId: "later", State: 99},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"fan[0]", "sha256:cccccccccccc", "2026-11-01T02:03:04Z", "o-1", "the run has not taken it", "alice",
		"bob (asserted)", "rejection", "run the parent again", "NOT ON RECORD", "(outputs)", "could not be read", "unknown; update this client",
		"run resolve ork-1 --task gate --redeliver", "1 not on record"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("no %q in\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "--task gate2") {
		t.Errorf("offers to tell a run that stopped waiting\n%s", out.String())
	}
}
