package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const (
	mintedID  = "ork-01kyz0000000000000000000aa"
	claimedID = "ork-01kyz0000000000000000000bb"
)

// startWorld is offerWorld with a cluster that starts runs and the artifact
// store they read.
type startWorld struct {
	*offerWorld
	objects *artifacts.LocalStore
	starter *startingCluster
	body    []byte
}

// startingCluster is a cluster's starts: it refuses an id it holds, as
// Temporal does, and what it started can be read.
type startingCluster struct {
	engine.RunStarter
	starts *startsOf
	plans  map[string]engine.RunPlan
	inputs map[string]*orkstackv1.Payload
	// err is every start's answer; lost makes one that landed answer it.
	err  error
	lost bool
}

func (c *startingCluster) StartRun(_ context.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan engine.RunPlan) error {
	if c.err != nil && !c.lost {
		return c.err
	}
	if _, taken := c.starts.runs[runID]; taken {
		return fmt.Errorf("start run %s: %w", runID, engine.ErrRunIDTaken)
	}
	c.starts.runs[runID] = &engine.RunStartRecord{Orkflow: wf, Plan: plan}
	c.plans[runID], c.inputs[runID] = plan, input
	return c.err
}

func newStartWorld(t *testing.T) *startWorld {
	t.Helper()
	w := &startWorld{offerWorld: newOfferWorld(t), body: []byte(`{"id":"o-1"}`)}
	var err error
	if w.objects, err = artifacts.NewLocalStore(w.dir); err != nil {
		t.Fatal(err)
	}
	w.starter = &startingCluster{starts: w.starts, plans: map[string]engine.RunPlan{}, inputs: map[string]*orkstackv1.Payload{}}
	w.s.router = engine.NewSingleRouter(engine.Binding{Reader: w.starts, Starter: w.starter})
	w.s.staged = w.objects
	w.s.newRunID = func() string { return mintedID }
	w.s.artifacts = artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: w.dir}
	w.stage(t)
	return w
}

func (w *startWorld) digest() string {
	sum := sha256.Sum256(w.body)
	return hex.EncodeToString(sum[:])
}

func (w *startWorld) stage(t *testing.T) {
	t.Helper()
	key, err := artifacts.StagingKey("default", hostSource, offeredID, w.digest())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.records.PutStaged(context.Background(), key, w.body); err != nil {
		t.Fatal(err)
	}
}

func (w *startWorld) startRequest() *orkstackv1.StartMessageRunRequest {
	return &orkstackv1.StartMessageRunRequest{Instance: w.ref(4), Message: &orkstackv1.MessageEnvelope{Id: offeredID, Subject: "orders.paid"},
		Parts:     []*orkstackv1.UploadedPart{{Input: "order", Sha256: w.digest(), SizeBytes: int64(len(w.body))}},
		Extracted: map[string]*structpb.Value{"order_id": structpb.NewStringValue("o-1")}}
}

func (w *startWorld) start(t *testing.T, req *orkstackv1.StartMessageRunRequest) *orkstackv1.DoorAnswer {
	t.Helper()
	resp, err := w.s.StartMessageRun(asHost(hostOrker), connect.NewRequest(req))
	if err != nil {
		t.Fatal(err)
	}
	return answered(t, "StartMessageRun", resp.Msg.GetAnswer())
}

// messageStart is the start event of a run a message of identity started.
func messageStart(identity string) *engine.RunStartRecord {
	return &engine.RunStartRecord{Orkflow: &orkstackv1.Orkflow{Namespace: "default", Name: "deploy"},
		Plan: engine.RunPlan{Origin: engine.OriginFromProto(&orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_EVENT_SOURCE, Id: hostSource, Reference: identity})}}
}

// TestStartMessageRun_Accepts: the run is started under the id the claim
// names, with the message's values, its origin and its parts where a run
// reads them; the marker stands before the answer and what was staged is
// gone.
func TestStartMessageRun_Accepts(t *testing.T) {
	ctx := context.Background()
	w := newStartWorld(t)
	got := w.start(t, w.startRequest())
	if got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_ACCEPTED || got.GetRunId() != mintedID {
		t.Fatalf("%v", got)
	}
	if !w.started(t) {
		t.Fatal("accepted with no marker")
	}
	plan := w.starter.plans[mintedID]
	if o := plan.Origin; o.Kind != "RUN_ORIGIN_KIND_EVENT_SOURCE" || o.ID != hostSource || o.Reference != offeredID || o.Description != "orders" || o.Principal != "" {
		t.Fatalf("%+v", o)
	}
	if v := w.starter.inputs[mintedID].GetData().GetFields()["order_id"].GetStringValue(); v != "o-1" {
		t.Fatalf("%v", w.starter.inputs[mintedID])
	}
	if len(plan.InputArtifacts) != 1 || plan.ArtifactClaims == nil {
		t.Fatalf("%+v, %v", plan.InputArtifacts, plan.ArtifactClaims)
	}
	in := plan.InputArtifacts[0]
	if in.Name != "order" || in.RunID != mintedID || in.TaskID != "_inputs" || in.Artifact.Name != "order" || in.Artifact.SHA256 != w.digest() || in.Artifact.ContentType != "application/json" {
		t.Fatalf("%+v", in)
	}
	var read bytes.Buffer
	body, err := w.objects.Get(ctx, in.Artifact.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	if _, err := read.ReadFrom(body); err != nil || !bytes.Equal(read.Bytes(), w.body) {
		t.Fatalf("%q, %v", read.Bytes(), err)
	}
	if err := w.copyAgain(ctx); !errors.Is(err, artifacts.ErrStagedMissing) {
		t.Fatalf("what was staged is there: %v", err)
	}
	if len(w.starter.plans) != 1 {
		t.Fatalf("%d runs", len(w.starter.plans))
	}
	// The same message again is a duplicate of that run, and a retry of the
	// start whose answer was lost is accepted.
	if got := w.start(t, w.startRequest()); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_DUPLICATE || got.GetRunId() != mintedID {
		t.Fatalf("%v", got)
	}
	again := w.startRequest()
	again.RetryOfOwnStart = true
	if got := w.start(t, again); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_ACCEPTED || got.GetRunId() != mintedID {
		t.Fatalf("%v", got)
	}
}

// copyAgain copies the staged part, to see whether it is there.
func (w *startWorld) copyAgain(ctx context.Context) error {
	_, err := w.objects.CopyStaged(ctx, artifacts.StagedPart{Namespace: "default", SourceID: hostSource, Identity: offeredID, Digest: w.digest(), Size: int64(len(w.body))},
		artifacts.Provenance{Orkflow: "deploy", RunID: "ork-x", TaskID: "t"}, "n", "text/plain")
	return err
}

// TestStartMessageRun_ARefusalLeavesNothing: what prepare refuses is answered
// before an id is claimed and before anything is copied.
func TestStartMessageRun_ARefusalLeavesNothing(t *testing.T) {
	const reject, wait, stop, uncertain = orkstackv1.Disposition_DISPOSITION_REJECT, orkstackv1.Disposition_DISPOSITION_WAIT, orkstackv1.Disposition_DISPOSITION_STOP,
		orkstackv1.Disposition_DISPOSITION_UNCERTAIN
	for name, c := range map[string]struct {
		set    func(*testing.T, *startWorld, *orkstackv1.StartMessageRunRequest)
		want   orkstackv1.Disposition
		reason string
		cause  string
		code   string
	}{
		"a field's type": {func(_ *testing.T, _ *startWorld, r *orkstackv1.StartMessageRunRequest) {
			r.Extracted["order_id"] = structpb.NewBoolValue(true)
		}, reject, eventsources.ReasonMessageFieldType, "", ""},
		"a subject": {func(_ *testing.T, _ *startWorld, r *orkstackv1.StartMessageRunRequest) {
			r.Message.Subject = strings.Repeat("s", 2<<10)
		}, reject, eventsources.ReasonMessageTextInvalid, "", ""},
		"no identity": {func(_ *testing.T, _ *startWorld, r *orkstackv1.StartMessageRunRequest) { r.Message.Id = "" }, reject, eventsources.ReasonMessageIdentityInvalid, "", ""},
		"its Orkflow is gone": {func(t *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.save(t, func(src *orkstackv1.EventSource) { src.OrkflowName = "nothing" })
		}, stop, eventsources.ReasonPaused, "", "ORKFLOW_NOT_FOUND"},
		"the namespace on hold": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.s.pools.Namespaces = namespacesOf{&orkstackv1.Namespace{Name: "default", Hold: &orkstackv1.NamespaceHold{Reason: "x"}}}
		}, wait, "", waitAdmission, ""},
		"the Orkflow not read": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.s.orkflows = unreadOrkflows{w.s.orkflows, errors.New("503")}
		}, wait, "", waitOutage, ""},
		"the store refusing": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.s.orkflows = unreadOrkflows{w.s.orkflows, s3Status(403)}
		}, uncertain, "", "", codeStoreRefused},
		"the plan over its bound": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.s.staged = longRefs{w.objects}
		}, stop, eventsources.ReasonPaused, "", "PLAN_TOO_LARGE"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newStartWorld(t)
			req := w.startRequest()
			c.set(t, w, req)
			got := w.start(t, req)
			if got.GetDisposition() != c.want || got.GetReason() != c.reason || got.GetCause() != c.cause || got.GetRefusalCode() != c.code || got.GetRunId() != "" {
				t.Fatalf("%v; want %s %s %s %s", got, c.want, c.reason, c.cause, c.code)
			}
			for _, scope := range []string{"deploy", "nothing"} {
				if _, found, err := w.claims.Get(context.Background(), artifacts.ClaimScope{Namespace: "default", Source: "orders", Orkflow: scope}, offeredID); found || err != nil {
					t.Fatalf("a refused start claimed: %v", err)
				}
			}
			if len(w.starter.plans) != 0 {
				t.Fatal("a refused start started")
			}
			if err := w.copyAgain(context.Background()); err != nil {
				t.Fatalf("what was staged is kept for the next delivery: %v", err)
			}
		})
	}
}

// longRefs predicts references a plan cannot hold.
type longRefs struct{ *artifacts.LocalStore }

func (l longRefs) RefFor(p artifacts.Provenance, name, sha string, size int64, contentType string) artifacts.ArtifactRef {
	ref := l.LocalStore.RefFor(p, name, sha, size, contentType)
	ref.URI = strings.Repeat("x", engine.MaxPlanBytes)
	return ref
}

// TestStartMessageRun_OneRunPerMessage: whatever was left behind, and
// whatever the cluster answers, a message has one run, under the claimed id.
func TestStartMessageRun_OneRunPerMessage(t *testing.T) {
	const accepted, dup, wait, reject, uncertain = orkstackv1.Disposition_DISPOSITION_ACCEPTED, orkstackv1.Disposition_DISPOSITION_DUPLICATE, orkstackv1.Disposition_DISPOSITION_WAIT,
		orkstackv1.Disposition_DISPOSITION_REJECT, orkstackv1.Disposition_DISPOSITION_UNCERTAIN
	for name, c := range map[string]struct {
		set    func(*testing.T, *startWorld, *orkstackv1.StartMessageRunRequest)
		want   orkstackv1.Disposition
		reason string
		cause  string
		code   string
		run    string
		marked bool
	}{
		"a claim whose run never started": {func(t *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) { w.claim(t, claimedID, false) }, accepted, "", "", "", claimedID, true},
		"a claim whose run the cluster holds": {func(t *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.claim(t, claimedID, false)
			w.starts.runs[claimedID] = messageStart(offeredID)
		}, dup, "", "", "", claimedID, true},
		"the same, a retry of its own start": {func(t *testing.T, w *startWorld, r *orkstackv1.StartMessageRunRequest) {
			w.claim(t, claimedID, false)
			w.starts.runs[claimedID] = messageStart(offeredID)
			r.RetryOfOwnStart = true
		}, accepted, "", "", "", claimedID, true},
		"a retry of its own start, the run another's": {func(t *testing.T, w *startWorld, r *orkstackv1.StartMessageRunRequest) {
			w.claim(t, claimedID, true)
			w.starts.runs[claimedID] = messageStart("m-2")
			r.RetryOfOwnStart = true
		}, dup, "", "", "", claimedID, true},
		"a retry of its own start, the run not read": {func(t *testing.T, w *startWorld, r *orkstackv1.StartMessageRunRequest) {
			w.claim(t, claimedID, true)
			w.starts.err = errFault
			r.RetryOfOwnStart = true
		}, wait, "", waitOutage, "", "", true},
		"a retry of its own start, the run's record bad": {func(t *testing.T, w *startWorld, r *orkstackv1.StartMessageRunRequest) {
			w.claim(t, claimedID, true)
			w.starts.err = engine.ErrRunRecordUndecodable
			r.RetryOfOwnStart = true
		}, dup, "", "", "", claimedID, true},
		"a run the archive alone holds": {func(t *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.claim(t, claimedID, false)
			w.archive.runs[claimedID] = &orkstackv1.Run{Namespace: "default"}
		}, dup, "", "", "", claimedID, true},
		"an id another delivery started meanwhile": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.s.messageClaims = racedClaims{w.claims, w.scope()}
			w.s.router = engine.NewSingleRouter(engine.Binding{Reader: &lateStarts{w.starts}, Starter: w.starter})
			w.starts.runs[claimedID] = messageStart(offeredID)
		}, dup, "", "", "", claimedID, true},
		"an id a run of something else holds": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.starts.runs[mintedID] = startOf("default")
		}, reject, eventsources.ReasonMessageRunIDConflict, "", "", "", false},
		"an id another namespace's message holds": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.starts.runs[mintedID] = messageStart(offeredID)
			w.starts.runs[mintedID].Orkflow.Namespace = "payments"
		}, reject, eventsources.ReasonMessageRunIDConflict, "", "", "", false},
		"an id whose run cannot be read": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.starts.runs[mintedID] = messageStart(offeredID)
			w.starts.err = errFault
		}, wait, "", waitOutage, "", "", false},
		"a start whose answer was lost": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.starter.err, w.starter.lost = context.DeadlineExceeded, true
		}, wait, "", waitTimeout, "", "", false},
		"a cluster that refuses": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.starter.err = serviceerror.NewPermissionDenied("no", "")
		}, uncertain, "", "", codeClusterRefused, "", false},
		"a cluster that is down": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.starter.err = serviceerror.NewUnavailable("later")
		}, wait, "", waitOutage, "", "", false},
		"a marker that cannot be written": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.s.messageClaims = unmarkedClaims{w.claims}
		}, wait, "", waitOutage, "", "", false},
		"the same, at most once": {func(t *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.s.messageClaims = unmarkedClaims{w.claims}
			w.save(t, func(src *orkstackv1.EventSource) { src.Delivery = orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE })
		}, accepted, "", "", "", mintedID, false},
		"a claim that cannot be written": {func(_ *testing.T, w *startWorld, _ *orkstackv1.StartMessageRunRequest) {
			w.s.messageClaims = uncreatedClaims{w.claims}
		}, wait, "", waitOutage, "", "", false},
	} {
		t.Run(name, func(t *testing.T) {
			w := newStartWorld(t)
			req := w.startRequest()
			c.set(t, w, req)
			got := w.start(t, req)
			if got.GetDisposition() != c.want || got.GetReason() != c.reason || got.GetCause() != c.cause || got.GetRefusalCode() != c.code || got.GetRunId() != c.run {
				t.Fatalf("%v; want %s %s %s %s %s", got, c.want, c.reason, c.cause, c.code, c.run)
			}
			if _, found, err := w.claims.Get(context.Background(), w.scope(), offeredID); err != nil || (c.marked && (!found || !w.started(t))) {
				t.Fatalf("no marker: %v", err)
			}
			if len(w.starter.plans) > 1 {
				t.Fatalf("%d runs were started", len(w.starter.plans))
			}
		})
	}
}

// racedClaims is a claim another delivery creates between this one's read and
// its create.
type racedClaims struct {
	*artifacts.MessageClaimStore
	scope artifacts.ClaimScope
}

func (c racedClaims) Create(ctx context.Context, scope artifacts.ClaimScope, id, runID string) (artifacts.MessageClaim, error) {
	if _, err := c.MessageClaimStore.Create(ctx, c.scope, id, claimedID); err != nil {
		return artifacts.MessageClaim{}, err
	}
	return c.MessageClaimStore.Create(ctx, scope, id, runID)
}

// lateStarts is a cluster read through a name of its own.
type lateStarts struct{ *startsOf }

// uncreatedClaims is a claims store that cannot be written.
type uncreatedClaims struct{ *artifacts.MessageClaimStore }

func (uncreatedClaims) Create(context.Context, artifacts.ClaimScope, string, string) (artifacts.MessageClaim, error) {
	return artifacts.MessageClaim{}, errFault
}

// TestStartMessageRun_ALostStartIsFound: a start whose answer was lost, and
// one whose marker was not written, are found by the message's next try, with
// one run.
func TestStartMessageRun_ALostStartIsFound(t *testing.T) {
	for name, set := range map[string]func(*startWorld){
		"the answer": func(w *startWorld) { w.starter.err, w.starter.lost = errFault, true },
		"the marker": func(w *startWorld) { w.s.messageClaims = unmarkedClaims{w.claims} },
	} {
		w := newStartWorld(t)
		set(w)
		if got := w.start(t, w.startRequest()); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_WAIT {
			t.Fatalf("%s: %v", name, got)
		}
		w.starter.err, w.s.messageClaims = nil, w.claims
		w.s.newRunID = func() string { return "ork-01kyz0000000000000000000cc" }
		req := w.startRequest()
		req.RetryOfOwnStart = true
		if got := w.start(t, req); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_ACCEPTED || got.GetRunId() != mintedID {
			t.Fatalf("%s: %v", name, got)
		}
		if !w.started(t) || len(w.starter.plans) != 1 {
			t.Fatalf("%s: %d runs", name, len(w.starter.plans))
		}
	}
}

// TestStartMessageRun_UploadAgain: a part that is not there is asked for
// again, and one that is not what was said of it is removed first.
func TestStartMessageRun_UploadAgain(t *testing.T) {
	w := newStartWorld(t)
	if err := w.records.RemoveStaged(context.Background(), "default", hostSource, offeredID); err != nil {
		t.Fatal(err)
	}
	if got := w.start(t, w.startRequest()); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_UPLOAD_AGAIN || len(w.starter.plans) != 0 {
		t.Fatalf("%v", got)
	}
	w.stage(t)
	if got := w.start(t, w.startRequest()); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_ACCEPTED || got.GetRunId() != mintedID {
		t.Fatalf("%v", got)
	}

	w = newStartWorld(t)
	req := w.startRequest()
	req.Parts[0].SizeBytes++
	if got := w.start(t, req); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_UPLOAD_AGAIN {
		t.Fatalf("%v", got)
	}
	if err := w.copyAgain(context.Background()); !errors.Is(err, artifacts.ErrStagedMissing) {
		t.Fatalf("a part that is not what was said was left, where staging never writes over it: %v", err)
	}
	w.s.staged = failingCopies{w.objects}
	if got := w.start(t, w.startRequest()); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_WAIT || got.GetCause() != waitOutage {
		t.Fatalf("%v", got)
	}
}

// failingCopies is a store that does not copy.
type failingCopies struct{ *artifacts.LocalStore }

func (failingCopies) CopyStaged(context.Context, artifacts.StagedPart, artifacts.Provenance, string, string) (artifacts.ArtifactRef, error) {
	return artifacts.ArtifactRef{}, errFault
}

// TestStartMessageRun_AnOrkersWordIsChecked: what no honest Orker sends is no
// answer about the message.
func TestStartMessageRun_AnOrkersWordIsChecked(t *testing.T) {
	for name, edit := range map[string]func(*orkstackv1.StartMessageRunRequest){
		"no part":         func(r *orkstackv1.StartMessageRunRequest) { r.Parts = nil },
		"another input's": func(r *orkstackv1.StartMessageRunRequest) { r.Parts[0].Input = "other" },
		"one twice":       func(r *orkstackv1.StartMessageRunRequest) { r.Parts = append(r.Parts, r.Parts[0]) },
		"a digest":        func(r *orkstackv1.StartMessageRunRequest) { r.Parts[0].Sha256 = "../../x" },
		"a size":          func(r *orkstackv1.StartMessageRunRequest) { r.Parts[0].SizeBytes = -1 },
		"a size over the source's bound": func(r *orkstackv1.StartMessageRunRequest) {
			r.Parts[0].SizeBytes = eventsources.DefaultMaxMessageBytes + 1
		},
		"too many": func(r *orkstackv1.StartMessageRunRequest) {
			for i := range 17 {
				r.Parts = append(r.Parts, &orkstackv1.UploadedPart{Input: fmt.Sprint(i), Sha256: strings.Repeat("a", 64)})
			}
		},
		"a request over bound": func(r *orkstackv1.StartMessageRunRequest) {
			r.Message.Broker = map[string]string{"a": strings.Repeat("x", maxDoorRequest)}
		},
	} {
		w := newStartWorld(t)
		req := w.startRequest()
		edit(req)
		if _, err := w.s.StartMessageRun(asHost(hostOrker), connect.NewRequest(req)); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
	}
	w := newStartWorld(t)
	req := w.startRequest()
	req.Instance.JudgingRevision = 3
	if got := w.start(t, req); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_REFRESH || len(got.GetJudging().GetSpec()) == 0 {
		t.Fatalf("%v", got)
	}
}

// TestKeptPools: what was learned of a pool serves ten seconds, and a
// question that was not answered is asked again.
func TestKeptPools(t *testing.T) {
	now := time.Unix(1000, 0)
	k := newKeptPools(func() time.Time { return now })
	asked := &askedPools{}
	r := &engine.PoolResolver{Probe: asked, Fleet: asked}
	k.over(r)
	ctx := context.Background()
	ask := func() {
		_, _ = r.Probe.ObservePool(ctx, "q")
		_, _ = r.Fleet.ObserveOrkers(ctx)
		_, _ = r.Fleet.DescribeOrkerByID(ctx, "orker-a")
	}
	ask()
	ask()
	if asked.n != 3 {
		t.Fatalf("%d questions", asked.n)
	}
	now = now.Add(doorPoolAge)
	ask()
	if asked.n != 6 {
		t.Fatalf("%d questions after %s", asked.n, doorPoolAge)
	}
	now = now.Add(-time.Hour)
	ask()
	if asked.n != 9 {
		t.Fatalf("%d questions once the clock went back", asked.n)
	}
	now, asked.err = now.Add(2*time.Hour), errFault
	ask()
	ask()
	if asked.n != 15 {
		t.Fatalf("%d questions: one that was not answered was kept", asked.n)
	}
	for i := range maxKeptPools + 10 {
		asked.err = nil
		_, _ = r.Probe.ObservePool(ctx, fmt.Sprint(i))
	}
	if n := len(k.channels.kept); n > maxKeptPools {
		t.Fatalf("%d kept", n)
	}
	bare := &engine.PoolResolver{}
	k.over(bare)
	(*keptPools)(nil).over(r)
	if bare.Probe != nil || bare.Fleet != nil {
		t.Fatal("a resolver that asks nothing was given something to ask")
	}
}

type askedPools struct {
	n   int
	err error
}

func (a *askedPools) ObservePool(context.Context, string) (engine.PoolLiveness, error) {
	a.n++
	return engine.PoolLiveness{Orkers: 1}, a.err
}

func (a *askedPools) ObserveOrkers(context.Context) ([]engine.OrkerPresence, error) {
	a.n++
	return nil, a.err
}

func (a *askedPools) DescribeOrkerByID(context.Context, string) (*orkstackv1.DescribeOrkerResult, error) {
	a.n++
	return &orkstackv1.DescribeOrkerResult{}, a.err
}

// claimedInputs notes the claims made of a run's inputs.
type claimedInputs struct {
	runs []string
	err  error
}

func (c *claimedInputs) ClaimObjects(_ context.Context, runID string, _ []string) error {
	c.runs = append(c.runs, runID)
	return c.err
}

// TestSubmitRun_AgainOfAMessagesRun: a run a message started is run again
// with the artifacts it received, claimed for it before it starts, and that
// run again in turn; nothing else hands a run artifacts.
func TestSubmitRun_AgainOfAMessagesRun(t *testing.T) {
	ctx := context.Background()
	const again, third = "ork-01kyz0000000000000000000dd", "ork-01kyz0000000000000000000ee"
	newWorld := func(t *testing.T) (*startWorld, *claimedInputs) {
		w, claims := newStartWorld(t), &claimedInputs{}
		w.s.artifactStore, w.s.inputClaims = w.objects, engine.NewArtifactClaimActivities(w.objects, claims)
		if got := w.start(t, w.startRequest()); got.GetDisposition() != orkstackv1.Disposition_DISPOSITION_ACCEPTED {
			t.Fatalf("%v", got)
		}
		w.s.newRunID = func() string { return again }
		return w, claims
	}
	submit := func(w *startWorld, orkflow, source string) (*orkstackv1.Run, error) {
		resp, err := w.s.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{Namespace: "default", OrkflowName: orkflow, SourceRunId: source,
			Input: w.starter.inputs[mintedID]}))
		if err != nil {
			return nil, err
		}
		return resp.Msg.GetRun(), nil
	}

	w, claims := newWorld(t)
	for _, c := range [][2]string{{mintedID, again}, {again, third}} {
		w.s.newRunID = func() string { return c[1] }
		run, err := submit(w, "deploy", c[0])
		if err != nil || run.GetId() != c[1] {
			t.Fatalf("%v, %v", run, err)
		}
		plan := w.starter.plans[c[1]]
		if len(plan.InputArtifacts) != 1 || plan.InputArtifacts[0].Artifact.URI != w.starter.plans[mintedID].InputArtifacts[0].Artifact.URI || plan.ArtifactClaims == nil {
			t.Fatalf("%+v", plan.InputArtifacts)
		}
	}
	if !slices.Equal(claims.runs, []string{again, third}) {
		t.Fatalf("claimed for %v", claims.runs)
	}

	for name, c := range map[string]struct {
		set    func(*testing.T, *startWorld, *claimedInputs)
		ork    string
		code   connect.Code
		reason string
	}{
		"an object that is gone": {func(t *testing.T, w *startWorld, _ *claimedInputs) {
			if err := w.objects.Delete(ctx, w.starter.plans[mintedID].InputArtifacts[0].Artifact.URI); err != nil {
				t.Fatal(err)
			}
		}, "deploy", connect.CodeFailedPrecondition, engine.InputArtifactUnavailableCode},
		"a claim that cannot be written": {func(_ *testing.T, _ *startWorld, c *claimedInputs) { c.err = errFault }, "deploy", connect.CodeUnavailable, "input artifacts"},
		"a store that does not answer":   {func(_ *testing.T, w *startWorld, _ *claimedInputs) { w.s.artifactStore = unstatted{w.objects} }, "deploy", connect.CodeUnavailable, "order"},
		"another Orkflow": {func(t *testing.T, w *startWorld, _ *claimedInputs) {
			wf, err := w.s.orkflows.Get(ctx, "default", "deploy", "")
			if err != nil {
				t.Fatal(err)
			}
			wf = proto.Clone(wf).(*orkstackv1.Orkflow)
			wf.Name, wf.DefinitionDigest = "other", ""
			w.put(t, wf)
		}, "other", connect.CodeFailedPrecondition, engine.ArtifactInputUnsupportedCode},
		"a child run's": {func(_ *testing.T, w *startWorld, _ *claimedInputs) {
			w.starts.runs[mintedID].Plan.Origin.Kind = engine.RunOriginKindParentRun
		}, "deploy", connect.CodeFailedPrecondition, engine.ArtifactInputUnsupportedCode},
	} {
		t.Run(name, func(t *testing.T) {
			w, claims := newWorld(t)
			c.set(t, w, claims)
			_, err := submit(w, c.ork, mintedID)
			if connect.CodeOf(err) != c.code || !strings.Contains(err.Error(), c.reason) {
				t.Fatalf("%v; want %s %s", err, c.code, c.reason)
			}
			if len(w.starter.plans) != 1 {
				t.Fatal("a refused run was started")
			}
		})
	}

	// One the archive alone holds.
	w, _ = newWorld(t)
	plan := w.starter.plans[mintedID]
	delete(w.starts.runs, mintedID)
	w.archive.runs[mintedID] = &orkstackv1.Run{Namespace: "default", OrkflowName: "deploy", Origin: plan.Origin.Proto(),
		InputArtifacts: []*orkstackv1.NamedArtifact{plan.InputArtifacts[0].Proto()}}
	if _, err := submit(w, "deploy", mintedID); err != nil || len(w.starter.plans[again].InputArtifacts) != 1 {
		t.Fatalf("%v, %+v", err, w.starter.plans[again].InputArtifacts)
	}
}

// unstatted is a store that does not answer.
type unstatted struct{ artifacts.Store }

func (unstatted) Stat(context.Context, string) (artifacts.ArtifactRef, error) {
	return artifacts.ArtifactRef{}, errFault
}
