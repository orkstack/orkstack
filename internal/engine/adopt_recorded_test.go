package engine

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// unansweringCommits is a commit store that never answers: the outage no
// real store produces on demand.
type unansweringCommits struct{}

func (unansweringCommits) Create(context.Context, string, *orkstackv1.ActivityCommit) (artifacts.CommitRecord, error) {
	return artifacts.CommitRecord{}, errors.New("connection refused")
}
func (unansweringCommits) Get(context.Context, string) (artifacts.CommitRecord, error) {
	return artifacts.CommitRecord{}, errors.New("connection refused")
}

type adoptFixture struct {
	commits *artifacts.MemoryCommitStore
	values  *artifacts.LocalStore
	acts    *ResumeActivities
}

func newAdoptFixture(t *testing.T) *adoptFixture {
	t.Helper()
	values, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	commits := artifacts.NewMemoryCommitStore()
	return &adoptFixture{commits: commits, values: values, acts: NewResumeActivities(commits, values)}
}

var adoptExpected = &orkstackv1.ExpectedCommit{
	Namespace: "default", RunId: "ork-1", TaskId: "build", DefinitionDigest: "sha256:def",
	IdempotencyKey: "default/ork-1/build", ImageIndexDigest: "sha256:img",
}

// commit writes the commit of adoptExpected at uri, with values.
func (f *adoptFixture) commit(t *testing.T, uri, values string) {
	t.Helper()
	ref, err := f.values.Put(context.Background(), "values.json", strings.NewReader(values), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.commits.Create(context.Background(), uri, &orkstackv1.ActivityCommit{
		SchemaVersion: artifacts.CommitSchemaVersion, Namespace: "default", RunId: "ork-1", TaskId: "build",
		OrkflowDefinitionDigest: "sha256:def", IdempotencyKey: "default/ork-1/build",
		Image:   &orkstackv1.ImageRef{IndexDigest: "sha256:img"},
		Values:  &orkstackv1.ArtifactRef{Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes},
		Attempt: 2, OrkerId: "orker-a", StartedAt: timestamppb.New(time.Unix(100, 0)), FinishedAt: timestamppb.New(time.Unix(160, 0)),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func adoptInput(entries ...*orkstackv1.AdoptEntry) *orkstackv1.AdoptRecordedInput {
	return &orkstackv1.AdoptRecordedInput{Entries: entries, Concurrency: 16, GetTimeout: durationpb.New(time.Second), GetTries: 3, Budget: 128 << 10}
}

func entry(uri string, expected *orkstackv1.ExpectedCommit) *orkstackv1.AdoptEntry {
	return &orkstackv1.AdoptEntry{TaskId: "build", Item: -1, Kind: orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_COMMIT, CommitUri: uri, Expected: expected}
}

// A kept result reads back as the result an adopting Orker returns: the
// committing attempt's facts, its own Orker, and small values inline.
func TestAdoptRecorded_ReadsAKeptResultAsAnAdoptedAttempt(t *testing.T) {
	f := newAdoptFixture(t)
	f.commit(t, "mem://c/build", `{"ok":true}`)
	out, err := f.acts.AdoptRecorded(context.Background(), adoptInput(entry("mem://c/build", adoptExpected)))
	if err != nil {
		t.Fatal(err)
	}
	r := out.GetEntries()[0].GetResult()
	if !r.GetCommitAdopted() || r.GetAttempt() != 2 || r.GetOrkerId() != "orker-a" || string(r.GetValuesJson()) != `{"ok":true}` ||
		r.GetCommit().GetUri() != "mem://c/build" || r.GetCommit().GetSha256() == "" || r.GetFinishedAt().AsTime().Unix() != 160 {
		t.Fatalf("result = %v", r)
	}
}

func TestAdoptRecorded_FailsEachCaseWithItsReason(t *testing.T) {
	other := proto.Clone(adoptExpected).(*orkstackv1.ExpectedCommit)
	other.IdempotencyKey = "default/ork-1/build@r41"
	for _, tc := range []struct {
		name      string
		setup     func(*adoptFixture)
		entry     *orkstackv1.AdoptEntry
		reason    string
		retryable bool
	}{
		{"no commit", func(*adoptFixture) {}, entry("mem://c/build", adoptExpected), reasonCommitMissing, false},
		{"another activity's commit", nil, entry("mem://c/build", other), reasonCommitIdentity, false},
		{"undecodable", func(f *adoptFixture) { f.commits.PutRaw("mem://c/build", []byte("x")) }, entry("mem://c/build", adoptExpected), reasonCommitUndecodable, false},
		{"no commit uri", func(*adoptFixture) {}, entry("", adoptExpected), reasonCommitMissing, false},
		{"the store does not answer", func(f *adoptFixture) { f.acts.commits = unansweringCommits{} }, entry("mem://c/build", adoptExpected), reasonCommitReadFailed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAdoptFixture(t)
			if tc.setup == nil {
				f.commit(t, "mem://c/build", `{}`)
			} else {
				tc.setup(f)
			}
			_, err := f.acts.AdoptRecorded(context.Background(), adoptInput(tc.entry))
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || app.Type() != tc.reason || app.NonRetryable() == tc.retryable {
				t.Fatalf("error = %v, want %s (retryable %v)", err, tc.reason, tc.retryable)
			}
		})
	}
}

// One final failure among store errors is what the call answers: a retry
// cannot bring a missing commit back.
func TestAdoptRecorded_AFinalFailureWinsOverARetryableOne(t *testing.T) {
	f := newAdoptFixture(t)
	f.commit(t, "mem://c/build", `{}`)
	gone := entry("mem://c/gone", adoptExpected)
	_, err := f.acts.AdoptRecorded(context.Background(), adoptInput(entry("mem://c/build", adoptExpected), gone))
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != reasonCommitMissing {
		t.Fatalf("error = %v, want COMMIT_MISSING", err)
	}
}

func TestAdoptRecorded_RefusesAnAnswerOverItsBudget(t *testing.T) {
	f := newAdoptFixture(t)
	f.commit(t, "mem://c/build", `{}`)
	in := adoptInput(entry("mem://c/build", adoptExpected))
	in.Budget = 64
	_, err := f.acts.AdoptRecorded(context.Background(), in)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != reasonCommitReadFailed || !app.NonRetryable() {
		t.Fatalf("error = %v, want a final COMMIT_READ_FAILED", err)
	}
}

// A call's input crosses Temporal history, so its largest form, a full call
// of entries with the longest names, stays within the 256 KiB payload bound.
func TestAdoptRecordedInput_ItsLargestCallIsWithinThePayloadBound(t *testing.T) {
	tun := defaultReadbackTunables()
	long := strings.Repeat("t", 200) // the longest task id
	run := "ork-" + strings.Repeat("r", 26)
	in := &orkstackv1.AdoptRecordedInput{Concurrency: int32(tun.Concurrency), GetTimeout: durationpb.New(tun.GetTimeout), GetTries: int32(tun.GetTries), Budget: tun.Budget}
	for i := 0; i < tun.MaxEntries; i++ {
		key, err := artifacts.ScopedItemCommitKey(strings.Repeat("n", 200), run, long, artifacts.MaxItemIndex, 9007199254740991)
		if err != nil {
			t.Fatal(err)
		}
		in.Entries = append(in.Entries, &orkstackv1.AdoptEntry{
			TaskId: long, Item: artifacts.MaxItemIndex, Kind: orkstackv1.AdoptEntryKind_ADOPT_ENTRY_KIND_COMMIT,
			CommitUri: "s3://" + strings.Repeat("b", 63) + "/" + key, RecordScope: 9007199254740991,
			Expected: &orkstackv1.ExpectedCommit{
				Namespace: strings.Repeat("n", 200), RunId: run, TaskId: long, DefinitionDigest: "sha256:" + strings.Repeat("d", 64),
				IdempotencyKey: strings.Repeat("n", 200) + "/" + run + "/" + long + "@r9007199254740991/99999", ImageIndexDigest: "sha256:" + strings.Repeat("i", 64),
			},
		})
	}
	size, err := task.PayloadSize(in)
	if err != nil {
		t.Fatal(err)
	}
	if size > task.MaxActivityInputBytes {
		t.Fatalf("the largest read-back call encodes to %d bytes, over the %d-byte payload bound", size, task.MaxActivityInputBytes)
	}
	t.Logf("the largest read-back call: %d bytes of %d", size, task.MaxActivityInputBytes)
}

// failingValues is an artifact store that never answers: the outage no real
// store produces on demand.
type failingValues struct{}

func (failingValues) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("connection refused")
}

func TestAdoptRecorded_ReadsValuesOnlyWhenTheyAreWhatTheCommitRecords(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		setup     func(*testing.T, *adoptFixture)
		reason    string
		retryable bool
	}{
		{"values gone", func(t *testing.T, f *adoptFixture) {
			rec, err := f.commits.Get(ctx, "mem://c/build")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(strings.TrimPrefix(rec.Commit.GetValues().GetUri(), "file://")); err != nil {
				t.Fatal(err)
			}
		}, reasonValuesMissing, false},
		{"values that hash otherwise", func(t *testing.T, f *adoptFixture) {
			rec, _ := f.commits.Get(ctx, "mem://c/build")
			rec.Commit.Values.Sha256 = strings.Repeat("0", 64)
			f.commits = artifacts.NewMemoryCommitStore()
			f.acts.commits = f.commits
			if _, err := f.commits.Create(ctx, "mem://c/build", rec.Commit); err != nil {
				t.Fatal(err)
			}
		}, reasonValuesMissing, false},
		{"a values store that does not answer", func(_ *testing.T, f *adoptFixture) { f.acts.values = failingValues{} }, reasonCommitReadFailed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAdoptFixture(t)
			f.commit(t, "mem://c/build", `{"ok":true}`)
			tc.setup(t, f)
			_, err := f.acts.AdoptRecorded(ctx, adoptInput(entry("mem://c/build", adoptExpected)))
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || app.Type() != tc.reason || app.NonRetryable() == tc.retryable {
				t.Fatalf("error = %v, want %s (retryable %v)", err, tc.reason, tc.retryable)
			}
		})
	}
}

// An input without its bounds, or an entry without the identity to check
// its commit against, is never read: reading on would hide the fault.
func TestAdoptRecorded_RefusesAnInputWithoutItsBounds(t *testing.T) {
	f := newAdoptFixture(t)
	f.commit(t, "mem://c/build", `{}`)
	for name, edit := range map[string]func(*orkstackv1.AdoptRecordedInput){
		"no budget":      func(in *orkstackv1.AdoptRecordedInput) { in.Budget = 0 },
		"no concurrency": func(in *orkstackv1.AdoptRecordedInput) { in.Concurrency = 0 },
		"no tries":       func(in *orkstackv1.AdoptRecordedInput) { in.GetTries = 0 },
		"no read bound":  func(in *orkstackv1.AdoptRecordedInput) { in.GetTimeout = nil },
		"no identity":    func(in *orkstackv1.AdoptRecordedInput) { in.Entries[0].Expected = nil },
	} {
		t.Run(name, func(t *testing.T) {
			in := adoptInput(entry("mem://c/build", adoptExpected))
			edit(in)
			_, err := f.acts.AdoptRecorded(context.Background(), in)
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || !app.NonRetryable() {
				t.Fatalf("error = %v, want a final refusal", err)
			}
		})
	}
}

// A call whose context ends is a store that did not answer in time: the
// workflow's retry policy decides, so it is never final, and a sibling's
// final failure still wins.
func TestAdoptRecorded_ACanceledCallIsRetryable(t *testing.T) {
	f := newAdoptFixture(t)
	f.acts.commits = unansweringCommits{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var entries []*orkstackv1.AdoptEntry
	for i := 0; i < 40; i++ {
		entries = append(entries, entry("mem://c/build", adoptExpected))
	}
	_, err := f.acts.AdoptRecorded(ctx, adoptInput(entries...))
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.NonRetryable() || app.Type() != reasonCommitReadFailed {
		t.Fatalf("error = %v, want a retryable COMMIT_READ_FAILED", err)
	}
}
