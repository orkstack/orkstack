package api

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestFillLiveIdentities: a started attempt shows, for each identity it
// was issued, its latest mint and expiry, FRESH until then and EXPIRED
// after; another attempt's records and a not-started attempt show nothing.
func TestFillLiveIdentities(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewRecordStore(ctx, artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	write := func(attempt int32, issued time.Time, name string, life time.Duration, id string) {
		if _, err := store.WriteIssued(ctx, artifacts.IssuedRecord{Namespace: "default", RunID: "ork-1", TaskID: "apply", Attempt: attempt,
			Principal: "serviceaccount:o", OrkerID: "o", IssuedAt: issued, Tokens: []artifacts.IssuedEntry{{Name: name, Audience: "sts",
				Subject: "s", JTI: strings.Repeat("0", 15) + id, KID: "k", IssuedAtUnix: issued.Unix(), ExpiresAtUnix: issued.Add(life).Unix()}}}); err != nil {
			t.Fatal(err)
		}
	}
	write(1, at, "aws", time.Hour, "1")
	write(2, at, "aws", time.Hour, "2")
	write(2, at.Add(30*time.Minute), "aws", time.Hour, "3")
	write(2, at, "vault", 10*time.Minute, "4")
	s := &Service{issuedRecords: store, logger: slog.Default()}
	started := &orkstackv1.LiveAttempt{TaskId: "apply", Attempt: 2, AttemptStartedAt: timestamppb.New(at)}
	waiting := &orkstackv1.LiveAttempt{TaskId: "apply", Attempt: 3}
	s.fillLiveIdentities(ctx, &orkstackv1.Run{Id: "ork-1", Namespace: "default"}, []*orkstackv1.LiveAttempt{started, waiting}, at.Add(20*time.Minute))
	got := map[string]*orkstackv1.LiveIdentity{}
	for _, i := range started.GetIdentity() {
		got[i.GetName()] = i
	}
	if len(got) != 2 || len(waiting.GetIdentity()) != 0 {
		t.Fatalf("identities: started %v, waiting %v", started.GetIdentity(), waiting.GetIdentity())
	}
	if aws := got["aws"]; aws.GetState() != orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_FRESH ||
		!aws.GetLastRefresh().AsTime().Equal(at.Add(30*time.Minute)) {
		t.Errorf("aws = %v", aws)
	}
	if vault := got["vault"]; vault.GetState() != orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_EXPIRED {
		t.Errorf("vault = %v", vault)
	}
}

// endlessIssued answers every page with one record and another page to
// come: the fault of records past the bound, which no real store produces
// on demand.
type endlessIssued struct{}

func (endlessIssued) ListIssued(context.Context, artifacts.IssuedQuery) ([]artifacts.RecordRead[artifacts.IssuedRecord], string, error) {
	rec := artifacts.IssuedRecord{TaskID: "apply", Attempt: 1, Tokens: []artifacts.IssuedEntry{{Name: "aws", ExpiresAtUnix: 1 << 40}}}
	return []artifacts.RecordRead[artifacts.IssuedRecord]{{Answer: artifacts.RecordFound, Value: rec}}, "next", nil
}

// TestFillLiveIdentities_PastTheBoundShowsNothing: records past what one
// inspect reads leave the state unset, never a partial latest.
func TestFillLiveIdentities_PastTheBoundShowsNothing(t *testing.T) {
	a := &orkstackv1.LiveAttempt{TaskId: "apply", Attempt: 1, AttemptStartedAt: timestamppb.Now()}
	(&Service{issuedRecords: endlessIssued{}, logger: slog.Default()}).fillLiveIdentities(context.Background(),
		&orkstackv1.Run{Id: "ork-1"}, []*orkstackv1.LiveAttempt{a}, time.Now())
	if len(a.GetIdentity()) != 0 {
		t.Fatalf("a truncated read showed %v", a.GetIdentity())
	}
}

// TestFillLiveIdentities_TheOrkersWordStands: what an attempt's beat said
// of an identity stands; the records add only what the beat did not name.
func TestFillLiveIdentities_TheOrkersWordStands(t *testing.T) {
	retrying := &orkstackv1.LiveIdentity{Name: "aws", Kind: "identity", State: orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_RETRYING}
	a := &orkstackv1.LiveAttempt{TaskId: "apply", Attempt: 1, AttemptStartedAt: timestamppb.Now(), Identity: []*orkstackv1.LiveIdentity{retrying}}
	(&Service{issuedRecords: endlessIssued{}, logger: slog.Default()}).fillLiveIdentities(context.Background(),
		&orkstackv1.Run{Id: "ork-1"}, []*orkstackv1.LiveAttempt{a}, time.Now())
	if len(a.GetIdentity()) != 1 || a.GetIdentity()[0] != retrying {
		t.Fatalf("the Orker's word was changed: %v", a.GetIdentity())
	}
}
