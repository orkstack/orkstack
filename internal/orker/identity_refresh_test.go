package orker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// fakeIdentityClock is a wall clock and a boot clock that advance only when
// the refresher sleeps.
type fakeIdentityClock struct {
	mu    sync.Mutex
	wall  time.Time
	boot  time.Duration
	slept []time.Duration
}

func (c *fakeIdentityClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wall
}

func (c *fakeIdentityClock) Boot() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.boot
}

func (c *fakeIdentityClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slept = append(c.slept, d)
	c.wall = c.wall.Add(d)
	c.boot += d
	return nil
}

// recordingMasker records each holder it is given.
type recordingMasker struct {
	added   []string
	dropped []string
}

func (m *recordingMasker) Add(holder string, _ []byte) (bool, error) {
	m.added = append(m.added, holder)
	return true, nil
}
func (m *recordingMasker) Drop(holder string) { m.dropped = append(m.dropped, holder) }

type refreshRig struct {
	r       *identityRefresher
	clock   *fakeIdentityClock
	masker  *recordingMasker
	parent  string
	answers []func() (*orkstackv1.MintAttemptTokensResponse, error)
	asked   int
}

func newRefreshRig(t *testing.T, names ...string) *refreshRig {
	t.Helper()
	rig := &refreshRig{
		clock:  &fakeIdentityClock{wall: time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC), boot: time.Hour},
		masker: &recordingMasker{},
		parent: t.TempDir(),
	}
	files := map[string]*identityFiles{}
	for _, name := range names {
		f, err := newIdentityFiles(rig.parent, name)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Remove() })
		files[name] = f
	}
	rig.r = &identityRefresher{
		files:       files,
		masker:      rig.masker,
		clock:       rig.clock,
		draw:        func() float64 { return 0 },
		retryMin:    5 * time.Second,
		setupBudget: 2 * time.Minute,
		maxSkew:     2 * time.Minute,
		orkerBuild:  "b1",
		mint: func(context.Context) (*orkstackv1.MintAttemptTokensResponse, error) {
			i := rig.asked
			rig.asked++
			if i >= len(rig.answers) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("IDENTITY_UNAVAILABLE: ask again"))
			}
			return rig.answers[i]()
		},
	}
	return rig
}

// issued answers one token for each name, issued at the rig's wall clock
// plus skew.
func (rig *refreshRig) issued(skew time.Duration, names ...string) func() (*orkstackv1.MintAttemptTokensResponse, error) {
	return func() (*orkstackv1.MintAttemptTokensResponse, error) {
		resp := &orkstackv1.MintAttemptTokensResponse{}
		for _, name := range names {
			resp.Tokens = append(resp.Tokens, &orkstackv1.IssuedToken{Name: name, Audience: "sts", Token: "jwt-" + name,
				ExpiresInSeconds: 600, IssuedAt: timestamppb.New(rig.clock.wall.Add(skew)), Jti: "j-" + name})
		}
		return resp, nil
	}
}

func refused(code connect.Code, reason string) func() (*orkstackv1.MintAttemptTokensResponse, error) {
	return func() (*orkstackv1.MintAttemptTokensResponse, error) {
		return nil, connect.NewError(code, errors.New(reason+": the mint issued nothing"))
	}
}

// TestIdentityRefresher_Setup: the first mint writes every identity's
// files, each value masked.
func TestIdentityRefresher_Setup(t *testing.T) {
	rig := newRefreshRig(t, "aws", "gcp")
	rig.answers = append(rig.answers, rig.issued(0, "aws", "gcp"))
	if f := rig.r.Setup(context.Background()); f != nil {
		t.Fatalf("Setup = %v", f)
	}
	for _, name := range []string{"aws", "gcp"} {
		if got := readThrough(t, filepath.Join(rig.parent, name), "token"); got != "jwt-"+name {
			t.Fatalf("%s token = %q", name, got)
		}
	}
	if len(rig.masker.added) != 2 {
		t.Fatalf("masked %v", rig.masker.added)
	}
	if rig.asked != 1 || len(rig.clock.slept) != 0 {
		t.Fatalf("asked %d, slept %v", rig.asked, rig.clock.slept)
	}
}

// TestIdentityRefresher_MaskedBeforeWritten: a value reaches the masker
// before any file holds it.
func TestIdentityRefresher_MaskedBeforeWritten(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.answers = append(rig.answers, rig.issued(0, "aws"))
	rig.r.files["aws"].write = func(path string, data []byte) error {
		if len(rig.masker.added) == 0 {
			t.Errorf("%s was written before its value was masked", path)
		}
		return writeIdentityFile(path, data)
	}
	if f := rig.r.Setup(context.Background()); f != nil {
		t.Fatalf("Setup = %v", f)
	}
}

// TestIdentityRefresher_SetupRetries: an answer that says ask again is asked
// again after the one rule's delays, inside the setup budget, and fails
// with its reason once the budget is spent.
func TestIdentityRefresher_SetupRetries(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.answers = append(rig.answers,
		refused(connect.CodeUnavailable, "IDENTITY_UNAVAILABLE"),
		refused(connect.CodeFailedPrecondition, "ATTEMPT_NOT_HELD"),
		rig.issued(0, "aws"))
	if f := rig.r.Setup(context.Background()); f != nil {
		t.Fatalf("Setup = %v", f)
	}
	if rig.asked != 3 || len(rig.clock.slept) != 2 || rig.clock.slept[0] != 5*time.Second || rig.clock.slept[1] != 10*time.Second {
		t.Fatalf("asked %d, slept %v", rig.asked, rig.clock.slept)
	}

	rig = newRefreshRig(t, "aws")
	f := rig.r.Setup(context.Background())
	if f == nil || f.Reason != "IDENTITY_UNAVAILABLE" {
		t.Fatalf("an engine that never answers: %v", f)
	}
	var total time.Duration
	for _, d := range rig.clock.slept {
		total += d
	}
	if total > 2*time.Minute {
		t.Fatalf("slept %v past the setup budget", total)
	}
	if _, err := os.Lstat(filepath.Join(rig.parent, "aws", "current")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a failed setup wrote a token: %v", err)
	}
}

// TestIdentityRefresher_SetupEnds: an answer that ends the attempt is not
// asked again.
func TestIdentityRefresher_SetupEnds(t *testing.T) {
	for _, tc := range []struct {
		answer string
		want   string
	}{
		{"IDENTITY_ATTEMPTS_EXHAUSTED", "IDENTITY_ATTEMPTS_EXHAUSTED"},
		{"TASK_NOT_AS_ADMITTED", "IDENTITY_SPEC_SKEW"},
		{"IDENTITY_REVOKED", "IDENTITY_REVOKED"},
	} {
		rig := newRefreshRig(t, "aws")
		rig.answers = append(rig.answers, refused(connect.CodeFailedPrecondition, tc.answer))
		f := rig.r.Setup(context.Background())
		if f == nil || f.Reason != tc.want || rig.asked != 1 {
			t.Fatalf("%s: %v after %d asks", tc.answer, f, rig.asked)
		}
	}
}

// TestIdentityRefresher_ClockCheck: an issued_at more than the skew bound
// from the Orker's clock fails the attempt IDENTITY_CLOCK_SKEW with no file
// written and no second ask; within the bound it is noted and used.
func TestIdentityRefresher_ClockCheck(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.answers = append(rig.answers, rig.issued(3*time.Minute, "aws"))
	f := rig.r.Setup(context.Background())
	if f == nil || f.Reason != ReasonIdentityClockSkew || rig.asked != 1 {
		t.Fatalf("3 min off: %v after %d asks", f, rig.asked)
	}
	if _, err := os.Lstat(filepath.Join(rig.parent, "aws", "current")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a skewed token was written: %v", err)
	}
	if len(rig.masker.added) != 0 {
		t.Fatalf("a skewed token was masked as if used: %v", rig.masker.added)
	}

	rig = newRefreshRig(t, "aws")
	var noted []time.Duration
	rig.r.onSkew = func(d time.Duration) { noted = append(noted, d) }
	rig.answers = append(rig.answers, rig.issued(-45*time.Second, "aws"))
	if f := rig.r.Setup(context.Background()); f != nil {
		t.Fatalf("45 s off: %v", f)
	}
	if len(noted) != 1 || noted[0] != 45*time.Second {
		t.Fatalf("noted %v", noted)
	}
}

// TestIdentityRefresher_AnAnswerMissingAnIdentity: an answer that leaves out
// an identity the task declares is no answer: asked again, never written in
// part.
func TestIdentityRefresher_AnAnswerMissingAnIdentity(t *testing.T) {
	rig := newRefreshRig(t, "aws", "gcp")
	rig.answers = append(rig.answers, rig.issued(0, "aws"), rig.issued(0, "aws", "gcp"))
	if f := rig.r.Setup(context.Background()); f != nil {
		t.Fatalf("Setup = %v", f)
	}
	if rig.asked != 2 {
		t.Fatalf("asked %d", rig.asked)
	}
	if got, _ := os.Readlink(filepath.Join(rig.parent, "aws", "current")); got != ".v1" {
		t.Fatalf("the partial answer was written: current -> %q", got)
	}
}

// setUp runs a rig's setup and fails the test if it fails.
func (rig *refreshRig) setUp(t *testing.T, names ...string) {
	t.Helper()
	rig.answers = append(rig.answers, rig.issued(0, names...))
	if f := rig.r.Setup(context.Background()); f != nil {
		t.Fatalf("Setup = %v", f)
	}
}

// TestIdentityRefresher_RefreshStep: a refresh swaps every token, waits
// until the next refresh point, and keeps the replaced value masked until it
// has expired plus the grace.
func TestIdentityRefresher_RefreshStep(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.r.required = map[string]bool{"aws": true}
	rig.r.maskGrace = 5 * time.Minute
	rig.setUp(t, "aws")
	rig.clock.Sleep(context.Background(), 200*time.Second)
	rig.answers = append(rig.answers, rig.issued(0, "aws"))
	wait, stop := rig.r.refreshStep(context.Background())
	if stop != nil {
		t.Fatalf("stop = %v", stop)
	}
	if wait != 200*time.Second { // a third of 600 s, with a draw of 0
		t.Fatalf("wait = %v", wait)
	}
	if got, _ := os.Readlink(filepath.Join(rig.parent, "aws", "current")); got != ".v2" {
		t.Fatalf("current -> %q", got)
	}
	rig.r.dropDue()
	if len(rig.masker.dropped) != 0 {
		t.Fatalf("dropped %v before the old token expired", rig.masker.dropped)
	}
	rig.clock.Sleep(context.Background(), 400*time.Second+5*time.Minute)
	rig.r.dropDue()
	if len(rig.masker.dropped) != 1 || rig.masker.dropped[0] != "identity:aws:1" {
		t.Fatalf("dropped %v", rig.masker.dropped)
	}
}

// TestIdentityRefresher_RefreshRetries: an answer that says ask again is
// asked again by the one rule, and a success starts the count again; a local
// write fault is a failed refresh, retried as one.
func TestIdentityRefresher_RefreshRetries(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.r.required = map[string]bool{"aws": true}
	rig.setUp(t, "aws")
	rig.answers = append(rig.answers, refused(connect.CodeUnavailable, "IDENTITY_UNAVAILABLE"), refused(connect.CodeUnavailable, "IDENTITY_UNAVAILABLE"))
	for _, want := range []time.Duration{5 * time.Second, 10 * time.Second} {
		if wait, stop := rig.r.refreshStep(context.Background()); stop != nil || wait != want {
			t.Fatalf("wait %v, stop %v; want %v", wait, stop, want)
		}
	}
	rig.answers = append(rig.answers, rig.issued(0, "aws"))
	if _, stop := rig.r.refreshStep(context.Background()); stop != nil {
		t.Fatal(stop)
	}
	rig.r.files["aws"].write = func(string, []byte) error { return errors.New("no space left on device") }
	rig.answers = append(rig.answers, rig.issued(0, "aws"))
	if wait, stop := rig.r.refreshStep(context.Background()); stop != nil || wait != 5*time.Second {
		t.Fatalf("a write fault: wait %v, stop %v", wait, stop)
	}
}

// TestIdentityRefresher_RefreshRefusals: a refusal that ends the attempt
// stops it; IDENTITY_REVOKED stops a required identity's attempt and ends
// the refreshing of a best_effort one.
func TestIdentityRefresher_RefreshRefusals(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.r.required = map[string]bool{"aws": true}
	rig.setUp(t, "aws")
	rig.answers = append(rig.answers, refused(connect.CodeFailedPrecondition, "IDENTITY_ATTEMPT_EXPIRED"))
	if _, stop := rig.r.refreshStep(context.Background()); stop == nil || stop.Reason != "IDENTITY_ATTEMPT_EXPIRED" {
		t.Fatalf("expired attempt: %v", stop)
	}

	rig = newRefreshRig(t, "aws")
	rig.r.required = map[string]bool{"aws": true}
	rig.setUp(t, "aws")
	rig.answers = append(rig.answers, refused(connect.CodeFailedPrecondition, "IDENTITY_REVOKED"))
	if _, stop := rig.r.refreshStep(context.Background()); stop == nil || stop.Reason != "IDENTITY_REVOKED" {
		t.Fatalf("revoked, required: %v", stop)
	}

	rig = newRefreshRig(t, "aws")
	rig.r.required = map[string]bool{"aws": false}
	rig.setUp(t, "aws")
	rig.answers = append(rig.answers, refused(connect.CodeFailedPrecondition, "IDENTITY_REVOKED"))
	if wait, stop := rig.r.refreshStep(context.Background()); stop != nil || wait != 0 || !rig.r.done {
		t.Fatalf("revoked, best_effort: wait %v, stop %v, done %v", wait, stop, rig.r.done)
	}
}

// TestIdentityRefresher_Watchdog: a required identity whose token expired
// unrefreshed, by either clock, stops the attempt IDENTITY_REFRESH_FAILED;
// a best_effort one is noted once and the attempt runs on.
func TestIdentityRefresher_Watchdog(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.r.required = map[string]bool{"aws": true}
	rig.setUp(t, "aws")
	if f := rig.r.watchdog(); f != nil {
		t.Fatalf("fresh: %v", f)
	}
	rig.clock.boot += 600 * time.Second // the host slept: only the boot clock moved
	if f := rig.r.watchdog(); f == nil || f.Reason != ReasonIdentityRefreshFailed {
		t.Fatalf("expired by the boot clock: %v", f)
	}

	rig = newRefreshRig(t, "aws")
	rig.r.required = map[string]bool{"aws": true}
	rig.setUp(t, "aws")
	rig.clock.wall = rig.clock.wall.Add(600 * time.Second) // a paused VM: only the wall clock moved
	if f := rig.r.watchdog(); f == nil || f.Reason != ReasonIdentityRefreshFailed {
		t.Fatalf("expired by the wall clock: %v", f)
	}

	rig = newRefreshRig(t, "aws")
	rig.r.required = map[string]bool{"aws": false}
	var notes []string
	rig.r.onExpired = func(name string) { notes = append(notes, name) }
	rig.setUp(t, "aws")
	rig.clock.boot += 600 * time.Second
	for range 2 {
		if f := rig.r.watchdog(); f != nil {
			t.Fatalf("best_effort expired: %v", f)
		}
	}
	if len(notes) != 1 || notes[0] != "aws" {
		t.Fatalf("notes %v", notes)
	}
}

// TestIdentityRefresher_Run: an engine that stops answering after setup
// leaves the refresh loop asking again, and the watchdog, on its own
// ticker, stops the attempt once the required token has expired.
func TestIdentityRefresher_Run(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.r.required = map[string]bool{"aws": true}
	rig.setUp(t, "aws")
	var mu sync.Mutex
	mint := rig.r.mint
	rig.r.mint = func(ctx context.Context) (*orkstackv1.MintAttemptTokensResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		return mint(ctx)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if f := rig.r.Run(ctx); f == nil || f.Reason != ReasonIdentityRefreshFailed {
		t.Fatalf("Run = %v", f)
	}
}

// TestIdentityRefresher_RunEndsWithItsContext: an attempt that ends stops
// the loop and the watchdog, with no failure of their own.
func TestIdentityRefresher_RunEndsWithItsContext(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.setUp(t, "aws")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if f := rig.r.Run(ctx); f != nil {
		t.Fatalf("Run = %v", f)
	}
}

// TestIdentityRefresher_SkewAtARefreshIsRetried: a skewed answer at a
// refresh is a failed refresh, retried, while what is on disk is still good.
func TestIdentityRefresher_SkewAtARefreshIsRetried(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.r.required = map[string]bool{"aws": true}
	rig.setUp(t, "aws")
	rig.answers = append(rig.answers, rig.issued(5*time.Minute, "aws"))
	if wait, stop := rig.r.refreshStep(context.Background()); stop != nil || wait != 5*time.Second {
		t.Fatalf("wait %v, stop %v", wait, stop)
	}
	if !strings.Contains(rig.r.lastFailure, ReasonIdentityClockSkew) {
		t.Fatalf("last failure %q", rig.r.lastFailure)
	}
	rig.clock.boot += 600 * time.Second
	if f := rig.r.watchdog(); f == nil || !strings.Contains(f.Message, ReasonIdentityClockSkew) {
		t.Fatalf("the watchdog's failure does not say what the refreshes met: %v", f)
	}
}

// TestIdentityRefresher_LiveIdentities: what the heartbeat says of each
// identity: fresh after a refresh, retrying while refreshes fail, expired
// once what is on disk has; never a value.
func TestIdentityRefresher_LiveIdentities(t *testing.T) {
	rig := newRefreshRig(t, "aws", "gcp")
	rig.r.required = map[string]bool{"aws": true, "gcp": true}
	rig.setUp(t, "aws", "gcp")
	live := rig.r.liveIdentities()
	if len(live) != 2 || live[0].Name != "aws" || live[1].Name != "gcp" || live[0].State != task.HeartbeatIdentityFresh ||
		live[0].Kind != "identity" || live[0].ExpiresAt == "" || live[0].LastRefresh == "" {
		t.Fatalf("after setup: %+v", live)
	}
	rig.answers = append(rig.answers, refused(connect.CodeUnavailable, "IDENTITY_UNAVAILABLE"))
	rig.r.refreshStep(context.Background())
	if live := rig.r.liveIdentities(); live[0].State != task.HeartbeatIdentityRetrying {
		t.Fatalf("while refreshes fail: %+v", live)
	}
	rig.clock.boot += 600 * time.Second
	if live := rig.r.liveIdentities(); live[0].State != task.HeartbeatIdentityExpired {
		t.Fatalf("once expired: %+v", live)
	}
	for _, l := range rig.r.liveIdentities() {
		if strings.Contains(l.Name+l.LastRefresh+l.ExpiresAt+l.State, "jwt") {
			t.Fatalf("a value reached the heartbeat: %+v", l)
		}
	}
}

// TestIdentityRefresher_AttemptIdentities: the attempt's record says, for
// each identity, its audience, how many tokens it was given, the first and
// last jti, the last kid and when the last expires; never a token.
func TestIdentityRefresher_AttemptIdentities(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	rig.setUp(t, "aws")
	jti := 0
	issued := func() (*orkstackv1.MintAttemptTokensResponse, error) {
		jti++
		resp, _ := rig.issued(0, "aws")()
		resp.Tokens[0].Jti = fmt.Sprintf("j%d", jti+1)
		resp.Tokens[0].Kid = fmt.Sprintf("k%d", jti+1)
		return resp, nil
	}
	rig.answers = append(rig.answers, issued, issued)
	for range 2 {
		if _, stop := rig.r.refreshStep(context.Background()); stop != nil {
			t.Fatal(stop)
		}
	}
	got := rig.r.attemptIdentities()
	if len(got) != 1 {
		t.Fatalf("got %v", got)
	}
	a := got[0]
	if a.GetName() != "aws" || a.GetAudience() != "sts" || a.GetTokensIssued() != 3 || a.GetFirstJti() != "j-aws" ||
		a.GetLastJti() != "j3" || a.GetLastKid() != "k3" || !a.GetLastExpiresAt().IsValid() {
		t.Fatalf("record %v", a)
	}
}

// TestIdentityRefresher_TellsTheClockCheck: each answer's clock check is
// told: agreed for one within the bound, skewed for one beyond it.
func TestIdentityRefresher_TellsTheClockCheck(t *testing.T) {
	rig := newRefreshRig(t, "aws")
	var told []bool
	rig.r.onClock = func(agreed bool) { told = append(told, agreed) }
	rig.answers = append(rig.answers, rig.issued(3*time.Minute, "aws"))
	rig.r.Setup(context.Background())
	rig = newRefreshRig(t, "aws")
	rig.r.onClock = func(agreed bool) { told = append(told, agreed) }
	rig.setUp(t, "aws")
	if len(told) != 2 || told[0] || !told[1] {
		t.Fatalf("told %v", told)
	}
}
