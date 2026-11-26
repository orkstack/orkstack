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

// fakeNotAppliedDoor is the engine as the loop sees it: a fixed set of runs,
// their unapplied rows, and a door that answers each telling as scripted.
type fakeNotAppliedDoor struct {
	runs     []*orkstackv1.Run
	pageSize int
	rows     map[string][]*orkstackv1.RecordedResult
	partial  map[string]bool
	listErr  map[string]error
	answers  map[string]*orkstackv1.ResolveAwaitResponse
	refusals map[string]error
	parents  map[string]*orkstackv1.Run

	endedAsked []bool
	told       []string
}

func (f *fakeNotAppliedDoor) listRuns(_ context.Context, ended bool, token string) ([]*orkstackv1.Run, string, error) {
	f.endedAsked = append(f.endedAsked, ended)
	start := 0
	if token != "" {
		for i, r := range f.runs {
			if r.GetId() == token {
				start = i
			}
		}
	}
	size := f.pageSize
	if size == 0 {
		size = len(f.runs)
	}
	end := min(start+size, len(f.runs))
	next := ""
	if end < len(f.runs) {
		next = f.runs[end].GetId()
	}
	return f.runs[start:end], next, nil
}

func (f *fakeNotAppliedDoor) listUnapplied(_ context.Context, runID string) ([]*orkstackv1.RecordedResult, bool, error) {
	if err := f.listErr[runID]; err != nil {
		return nil, false, err
	}
	return f.rows[runID], !f.partial[runID], nil
}

func (f *fakeNotAppliedDoor) redeliver(_ context.Context, runID, taskID string) (*orkstackv1.ResolveAwaitResponse, error) {
	key := runID + "/" + taskID
	f.told = append(f.told, key)
	if err := f.refusals[key]; err != nil {
		return nil, err
	}
	return f.answers[key], nil
}

func (f *fakeNotAppliedDoor) getRun(_ context.Context, runID string) (*orkstackv1.Run, error) {
	if r, ok := f.parents[runID]; ok {
		return r, nil
	}
	return nil, connect.NewError(connect.CodeNotFound, errors.New("no such run"))
}

// TestNotApplied: each line the pass prints, what it tells, and when it exits
// non-zero (workload-identity.md, "After an outage").
func TestNotApplied(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	ts := func(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(now.Add(d)) }
	open := func(id string) *orkstackv1.Run {
		return &orkstackv1.Run{Id: id, Status: orkstackv1.Status_STATUS_RUNNING}
	}
	ended := func(id string, ago time.Duration) *orkstackv1.Run {
		return &orkstackv1.Run{Id: id, Status: orkstackv1.Status_STATUS_FAILED, FinishedAt: ts(-ago)}
	}
	child := func(id, parent string) *orkstackv1.Run {
		r := open(id)
		r.Origin = &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: parent}
		return r
	}
	awaiting := func(task string, deadline time.Duration) *orkstackv1.RecordedResult {
		return &orkstackv1.RecordedResult{TaskId: task, Kind: orkstackv1.RecordedResultKind_RECORDED_RESULT_KIND_RESOLUTION,
			State: stNot, TaskStatus: orkstackv1.Status_STATUS_AWAITING, Principal: "alice", PrincipalVerified: true,
			AskedAt: ts(-time.Hour), StoredAt: ts(-time.Hour), Deadline: ts(deadline)}
	}
	stopped := func(task string, inChild bool) *orkstackv1.RecordedResult {
		r := awaiting(task, -time.Minute)
		r.TaskStatus, r.InChildRun = orkstackv1.Status_STATUS_FAILED, inChild
		return r
	}
	toldAt := &orkstackv1.ResolveAwaitResponse{ToldAt: ts(0)}
	refused := connect.NewError(connect.CodeFailedPrecondition, errors.New("AWAIT_DEADLINE_PASSED: too late"))

	for _, tc := range []struct {
		name      string
		door      *fakeNotAppliedDoor
		opts      notAppliedOptions
		want      []string
		notWant   []string
		told      []string
		wantErr   bool
		wantEnded bool
	}{
		{
			name: "open runs only, nothing told without --redeliver",
			door: &fakeNotAppliedDoor{runs: []*orkstackv1.Run{open("ork-a"), ended("ork-old", time.Hour)},
				rows: map[string][]*orkstackv1.RecordedResult{
					"ork-a":   {awaiting("gate", time.Hour), awaiting("late", -time.Minute)},
					"ork-old": {stopped("gate", false)},
				}},
			opts: notAppliedOptions{maxRuns: 200},
			want: []string{"Reading at most 200 runs", "(open)", "ork-a", "gate", "alice", "tell the run again",
				"late", "past its deadline; the run decides"},
			notWant: []string{"ork-old", "Told is not taken"},
		},
		{
			name: "ended runs since T: resume a root, run the parent of a child",
			door: &fakeNotAppliedDoor{runs: []*orkstackv1.Run{ended("ork-root", time.Hour), ended("ork-kid", 2*time.Hour), ended("ork-ancient", 72*time.Hour)},
				rows: map[string][]*orkstackv1.RecordedResult{
					"ork-root":    {stopped("gate", false)},
					"ork-kid":     {stopped("gate", true)},
					"ork-ancient": {stopped("gate", false)},
				}},
			opts:      notAppliedOptions{maxRuns: 200, includeEnded: true, since: now.Add(-24 * time.Hour)},
			want:      []string{"ended since", "ork-root", "resume the run", "ork-kid", "run the parent again; this answer is not carried over"},
			notWant:   []string{"ork-ancient"},
			wantEnded: true,
		},
		{
			name: "--redeliver tells only what is before its deadline, and says each answer",
			door: &fakeNotAppliedDoor{runs: []*orkstackv1.Run{open("ork-a"), open("ork-b"), child("ork-c", "ork-gone"), open("ork-d")},
				rows: map[string][]*orkstackv1.RecordedResult{
					"ork-a": {awaiting("gate", time.Hour), awaiting("late", -time.Minute)},
					"ork-b": {awaiting("gate", time.Hour)},
					"ork-c": {func() *orkstackv1.RecordedResult { r := awaiting("gate", time.Hour); r.InChildRun = true; return r }()},
					"ork-d": {awaiting("gate", time.Hour)},
				},
				answers:  map[string]*orkstackv1.ResolveAwaitResponse{"ork-a/gate": toldAt, "ork-d/gate": {DeliveryPending: true}},
				refusals: map[string]error{"ork-b/gate": refused},
				parents:  map[string]*orkstackv1.Run{"ork-gone": ended("ork-gone", time.Minute)}},
			opts: notAppliedOptions{maxRuns: 200, redeliver: true},
			want: []string{"told at 2026-11-26T12:00:00Z", "refused: AWAIT_DEADLINE_PASSED", "pending",
				"skipped: its parent has ended", "skipped: past its deadline", "Told is not taken"},
			told:    []string{"ork-a/gate", "ork-b/gate", "ork-d/gate"},
			wantErr: true,
		},
		{
			name: "every telling heard exits zero",
			door: &fakeNotAppliedDoor{runs: []*orkstackv1.Run{open("ork-a")},
				rows:    map[string][]*orkstackv1.RecordedResult{"ork-a": {awaiting("gate", time.Hour)}},
				answers: map[string]*orkstackv1.ResolveAwaitResponse{"ork-a/gate": toldAt}},
			opts: notAppliedOptions{maxRuns: 200, redeliver: true},
			want: []string{"told at", "Told is not taken"},
			told: []string{"ork-a/gate"},
		},
		{
			name: "a run whose records could not all be read exits non-zero",
			door: &fakeNotAppliedDoor{runs: []*orkstackv1.Run{open("ork-a"), open("ork-b")},
				rows:    map[string][]*orkstackv1.RecordedResult{"ork-a": {{TaskId: "gate", State: stUnread}}},
				partial: map[string]bool{"ork-a": true},
				listErr: map[string]error{"ork-b": connect.NewError(connect.CodeUnavailable, errors.New("down"))}},
			opts:    notAppliedOptions{maxRuns: 200},
			want:    []string{"ork-a: some of its records could not be read", "could not be read; run this again", "ork-b:", "unavailable"},
			wantErr: true,
		},
		{
			name: "--max-runs stops the pass, says so and exits non-zero",
			door: &fakeNotAppliedDoor{runs: []*orkstackv1.Run{open("ork-a"), open("ork-b"), open("ork-c")}, pageSize: 2,
				rows: map[string][]*orkstackv1.RecordedResult{"ork-c": {awaiting("gate", time.Hour)}}},
			opts:    notAppliedOptions{maxRuns: 2},
			want:    []string{"Reading at most 2 runs", "No answer on record is waiting", "Stopped at --max-runs 2"},
			notWant: []string{"ork-c"},
			wantErr: true,
		},
		{
			name: "a record stored after its deadline is never told",
			door: &fakeNotAppliedDoor{runs: []*orkstackv1.Run{open("ork-a")},
				rows: map[string][]*orkstackv1.RecordedResult{"ork-a": {func() *orkstackv1.RecordedResult {
					r := awaiting("gate", -2*time.Hour)
					r.Deadline = ts(time.Hour)
					r.StoredAt = ts(2 * time.Hour)
					return r
				}()}}},
			opts: notAppliedOptions{maxRuns: 200, redeliver: true},
			want: []string{"stored after its deadline; not used"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.opts.now = now
			var out bytes.Buffer
			err := notApplied(context.Background(), tc.door, tc.opts, &out)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v\n%s", err, tc.wantErr, out.String())
			}
			for _, want := range tc.want {
				if !strings.Contains(out.String(), want) {
					t.Errorf("no %q in\n%s", want, out.String())
				}
			}
			for _, not := range tc.notWant {
				if strings.Contains(out.String(), not) {
					t.Errorf("%q in\n%s", not, out.String())
				}
			}
			if strings.Join(tc.door.told, ",") != strings.Join(tc.told, ",") {
				t.Errorf("told %v, want %v", tc.door.told, tc.told)
			}
			for _, asked := range tc.door.endedAsked {
				if asked != tc.wantEnded {
					t.Errorf("listed runs with ended=%v, want %v", asked, tc.wantEnded)
				}
			}
		})
	}
}

// TestRunRecordsCommand_NotAppliedFlags: --not-applied reads a namespace and
// names no run; its flags go with it alone.
func TestRunRecordsCommand_NotAppliedFlags(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--not-applied", "ork-1"}, "name no run"},
		{[]string{"--not-applied", "--task", "gate"}, "does not go with --not-applied"},
		{[]string{"ork-1", "--redeliver"}, "--redeliver goes with --not-applied"},
		{[]string{"ork-1", "--include-ended"}, "--include-ended goes with --not-applied"},
		{[]string{"--not-applied", "--since", "2026-11-01T00:00:00Z"}, "--since goes with --include-ended"},
		{[]string{"--not-applied", "--include-ended", "--since", "yesterday"}, "RFC 3339"},
		{[]string{"--not-applied", "--max-runs", "0"}, "at least 1"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			cmd := newRunRecordsCommand(&globalFlags{})
			cmd.SetArgs(tc.args)
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			err := cmd.ExecuteContext(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one naming %q", err, tc.want)
			}
		})
	}
}

// stuckDoor repeats its cursor; nilEnd lists an ended run with no end time.
type stuckDoor struct{ *fakeNotAppliedDoor }

func (s stuckDoor) listRuns(_ context.Context, _ bool, _ string) ([]*orkstackv1.Run, string, error) {
	return s.runs, "same", nil
}

// TestNotApplied_ReadsShortOnlyAloud: a listing that does not page on, more
// marked runs than a pass may list, and an engine string carrying a terminal
// escape; and an ended run with no end time is read, not left out unsaid.
func TestNotApplied_ReadsShortOnlyAloud(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	run := &orkstackv1.Run{Id: "ork-a", Status: orkstackv1.Status_STATUS_RUNNING}
	var out bytes.Buffer
	err := notApplied(context.Background(), stuckDoor{&fakeNotAppliedDoor{runs: []*orkstackv1.Run{run}}}, notAppliedOptions{maxRuns: 200, now: now}, &out)
	if err == nil || !strings.Contains(out.String(), "did not page on") {
		t.Fatalf("stuck cursor: %v\n%s", err, out.String())
	}

	many := &fakeNotAppliedDoor{}
	for i := range 25 {
		many.runs = append(many.runs, &orkstackv1.Run{Id: "ork-" + string(rune('a'+i)), Status: orkstackv1.Status_STATUS_FAILED, FinishedAt: timestamppb.New(now.Add(-72 * time.Hour))})
	}
	out.Reset()
	err = notApplied(context.Background(), many, notAppliedOptions{maxRuns: 2, includeEnded: true, since: now.Add(-time.Hour), now: now}, &out)
	if err == nil || !strings.Contains(out.String(), "Stopped after listing 20 marked runs") {
		t.Fatalf("scan bound: %v\n%s", err, out.String())
	}

	odd := &fakeNotAppliedDoor{runs: []*orkstackv1.Run{{Id: "ork-x", Status: orkstackv1.Status_STATUS_FAILED}},
		rows: map[string][]*orkstackv1.RecordedResult{"ork-x": {{TaskId: "gate", State: stNot, TaskStatus: orkstackv1.Status_STATUS_FAILED,
			Principal: "eve\x1b[2J"}}}}
	out.Reset()
	if err := notApplied(context.Background(), odd, notAppliedOptions{maxRuns: 200, includeEnded: true, since: now.Add(-time.Hour), now: now}, &out); err != nil {
		t.Fatalf("ended run with no end time: %v", err)
	}
	if !strings.Contains(out.String(), "ork-x") || strings.Contains(out.String(), "\x1b") {
		t.Fatalf("output:\n%q", out.String())
	}
}
