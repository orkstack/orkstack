package api_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"io/fs"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The admission record at submit (docs/TASK_IDENTITY.md, "The admission
// record"). Each test runs SubmitRun against a real local record store: the
// records it writes are read back from that store, and the files under its
// directory are what a test counts. The task identity gate of this build
// still refuses `identities:` at apply, so the definitions are put straight
// into the Orkflow store, as one applied once the gate lifts would be.

const admissionIssuer = "https://ork-id.admission.test"

// admissionWorld is a service with a task identity issuer and the identity
// record store under dir.
type admissionWorld struct {
	svc         *api.Service
	records     *artifacts.RecordStore
	dir         string
	starter     *executionStarter
	runContexts *artifacts.RunContextStore
}

// executionStarter is the unit tests' stand-in cluster, answering each
// start with the execution it opened, as engine.TemporalStarter does.
type executionStarter struct {
	fakeStarter
}

func (s *executionStarter) StartRunExecution(ctx context.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan engine.RunPlan) (string, error) {
	if err := s.StartRun(ctx, runID, wf, input, plan); err != nil {
		return "", err
	}
	return "exec-" + strings.TrimPrefix(runID, "ork-"), nil
}

type admissionOptions struct {
	claimer     api.IdempotencyClaimer
	records     api.AdmissionRecords
	dir         string
	attemptsMax int
	definitions []*orkstackv1.Orkflow
	grants      map[string]artifacts.Grant
	providers   provider.Set
	// trustDomain makes the issuer's tokens carry the SPIFFE form too.
	trustDomain string
	sessionTags map[string]artifacts.SessionTagging
	// fleet is the Orkers a submit asks; nil wires no inventory.
	fleet engine.OrkerFleet
	pools engine.PoolProbe
}

func newAdmissionWorld(t *testing.T, o admissionOptions) *admissionWorld {
	t.Helper()
	ctx := context.Background()
	if o.dir == "" {
		o.dir = t.TempDir()
	}
	records, err := artifacts.NewRecordStore(ctx, artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: o.dir})
	if err != nil {
		t.Fatal(err)
	}
	incarnations, err := artifacts.NewIncarnationStore(ctx, artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	// The built-in namespace's policy lets its tasks ask for the audience.
	if _, err := incarnations.SetNamespacePolicy(ctx, artifacts.DefaultNamespace, time.Unix(0, 0).UTC(),
		artifacts.NamespacePolicy{Audiences: []string{"sts.amazonaws.com"}, Grants: o.grants}, 0, artifacts.PolicyWriter{Principal: "admin"}); err != nil {
		t.Fatal(err)
	}
	orkflows := engine.NewMemoryOrkflowStore()
	for _, def := range o.definitions {
		if _, _, err := orkflows.Put(ctx, def, artifacts.HeadExpectation{}); err != nil {
			t.Fatal(err)
		}
	}
	var store api.AdmissionRecords = records
	if o.records != nil {
		store = o.records
	}
	if o.attemptsMax == 0 {
		o.attemptsMax = identity.DefaultAttemptsMax
	}
	starter := &executionStarter{}
	runContexts, err := artifacts.NewLocalRunContextStore(o.dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := api.NewService(api.Config{
		RunContexts:            runContexts,
		Orkflows:               orkflows,
		Runs:                   engine.NewRunStore(),
		Bus:                    engine.NewEventBus(),
		Starter:                starter,
		Idempotency:            o.claimer,
		Issuer:                 admissionTestIssuer(t, o.trustDomain),
		IdentityAdmissions:     store,
		IdentityUnscopedOrkers: identity.UnscopedReport,
		IdentityAttemptsMax:    o.attemptsMax,
		IdentityTokenTTL:       time.Hour,
		SessionTags:            o.sessionTags,
		IdentityCommitWait:     2 * time.Minute,
		Incarnations:           incarnations,
		NamespacePolicies:      incarnations,
		EngineBuild:            "build-test",
		Providers:              o.providers,
		Fleet:                  o.fleet,
		Pools:                  o.pools,
	})
	return &admissionWorld{svc: svc, records: records, dir: o.dir, starter: starter, runContexts: runContexts}
}

func admissionTestIssuer(t *testing.T, trustDomain string) *identity.Issuer {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jwt.NewSigner(k)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := artifacts.NewPublishedKeysStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	i, err := identity.New(identity.Config{URL: admissionIssuer, Keys: []*jwt.Signer{signer}, Algorithm: jwt.ES256,
		ActivationDelay: time.Hour, Engine: "e1", TrustDomain: trustDomain}, keys, nothingLive{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return i
}

// nothingLive serves no published key: nothing here signs.
type nothingLive struct{}

func (nothingLive) PublishedKeyIDs(context.Context) (map[string]bool, error) { return nil, nil }

// identityOrkflow is Orkflow deploy@1 of namespace default: a task that
// holds identity aws, one that holds none, and an awaited task.
func identityOrkflow() *orkstackv1.Orkflow {
	return digested(undigestedIdentityOrkflow())
}

// digested gives wf the digest a create gives it.
func digested(wf *orkstackv1.Orkflow) *orkstackv1.Orkflow {
	digest, err := dsl.DefinitionDigest(wf)
	if err != nil {
		panic(err) // a definition of this file, not an input
	}
	wf.DefinitionDigest = digest
	return wf
}

func undigestedIdentityOrkflow() *orkstackv1.Orkflow {
	pinned := &orkstackv1.ImageRef{Reference: "ghcr.io/acme/deploy:1", Pinned: "ghcr.io/acme/deploy@sha256:" + strings.Repeat("a", 64)}
	return &orkstackv1.Orkflow{Name: "deploy", Version: "1", Tasks: []*orkstackv1.Task{
		{Id: "apply", Image: pinned, Command: []string{"apply"}, Retry: &orkstackv1.RetryPolicy{MaximumAttempts: 3},
			Identities: []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts.amazonaws.com", TtlSeconds: 900}}},
		{Id: "report", Image: pinned, Needs: []string{"apply"}},
		{Id: "approve", Needs: []string{"report"}, Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}},
	}}
}

// composingOrkflows is Orkflow release@1, whose task ship composes
// child@1, whose task push holds identity aws.
func composingOrkflows() (parent, child *orkstackv1.Orkflow) {
	child = identityOrkflow()
	child.Name = "child"
	child.Tasks = child.Tasks[:1]
	child.Tasks[0].Id = "push"
	digested(child)
	pinned := &orkstackv1.ImageRef{Reference: "ghcr.io/acme/build:1", Pinned: "ghcr.io/acme/build@sha256:" + strings.Repeat("b", 64)}
	parent = digested(&orkstackv1.Orkflow{Name: "release", Version: "1", Tasks: []*orkstackv1.Task{
		{Id: "build", Image: pinned},
		{Id: "ship", Needs: []string{"build"}, Uses: "orkflow:child@1", Orkflow: &orkstackv1.OrkflowRef{Name: "child", Version: "1", DefinitionDigest: child.GetDefinitionDigest()}},
	}})
	return parent, child
}

func (w *admissionWorld) submit(t *testing.T, name, key string) (*orkstackv1.Run, error) {
	t.Helper()
	resp, err := w.svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: name, IdempotencyKey: key}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRun(), nil
}

// admissionFiles lists the admission records under the store's directory.
func (w *admissionWorld) admissionFiles(t *testing.T) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(w.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), "admission-") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestSubmitRun_AdmitsARunThatHoldsAnIdentity: the run is started with a
// plan naming its admission, the record is in the store under its id with
// every container task in its table, and the chain record names the
// execution the start opened and that digest.
func TestSubmitRun_AdmitsARunThatHoldsAnIdentity(t *testing.T) {
	ctx := context.Background()
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{identityOrkflow()}})
	run, err := w.submit(t, "deploy", "")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	plans := w.starter.startedPlans()
	if len(plans) != 1 || plans[0].Identity == nil {
		t.Fatalf("started plans %+v; want one naming its admission", plans)
	}
	ip := plans[0].Identity
	if ip.AttemptsExtra != identity.DefaultAttemptsExtra || ip.SpecVersion != task.IdentitySpecVersion || ip.Issuer != admissionIssuer || ip.TokenTTLSeconds != 3600 || ip.CommitWaitSeconds != 120 || len(ip.AdmissionSHA256) != 64 {
		t.Fatalf("the plan's identity = %+v", ip)
	}
	read, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, run.GetId(), ip.AdmissionSHA256)
	if err != nil || read.Answer != artifacts.RecordFound {
		t.Fatalf("the admission the plan names reads %+v, %v", read, err)
	}
	a := read.Value
	if a.Starter != (artifacts.Starter{Kind: artifacts.StarterEngine}) || a.RootRunID != "" || a.Issuer != admissionIssuer ||
		a.EngineBuild != "build-test" || a.TokenTTL != 3600 || a.NamespaceID == "" || a.OrkflowID == "" ||
		a.Origin.Kind != "submit" || "sha256:"+a.DefinitionDigest != identityOrkflow().GetDefinitionDigest() {
		t.Fatalf("admission = %+v", a)
	}
	wantSet := artifacts.AdmittedIdentity{Name: "aws", Audience: "sts.amazonaws.com", TTLSeconds: 900, Refresh: artifacts.RefreshRequired}
	if len(a.Tasks) != 2 || len(a.Sets) != 1 || a.Sets[0].Identities[0] != wantSet {
		t.Fatalf("tasks %+v, sets %+v; want apply and report, and one set", a.Tasks, a.Sets)
	}
	def := identityOrkflow()
	for _, tk := range def.GetTasks()[:2] {
		want, _, _ := task.IdentitySpecDigest(1, task.IdentitySpecOfTask(tk, nil))
		if got := a.Tasks[tk.GetId()]; got.SpecSHA256 != want || got.Pool != task.DefaultPool {
			t.Errorf("task %q: %+v; want spec %s at pool %s", tk.GetId(), got, want, task.DefaultPool)
		}
	}
	apply := a.Tasks["apply"]
	if apply.Set != 1 || apply.IdentitySlots != 3 || int64(apply.AttemptDeadline) != engine.AttemptDeadlineSeconds(def.GetTasks()[0], plans[0].TransferBudgetSeconds) {
		t.Errorf("apply = %+v; want set 1, 3 slots and the attempt's deadline", apply)
	}
	if report := a.Tasks["report"]; report.Set != 0 || report.IdentitySlots != 0 || report.AttemptDeadline != 0 {
		t.Errorf("report = %+v; a task that holds no identity has no slots", report)
	}
	chain, err := w.records.ReadChain(ctx, artifacts.DefaultNamespace, run.GetId())
	wantChain := artifacts.Chain{FirstExecutionID: "exec-" + strings.TrimPrefix(run.GetId(), "ork-"), AdmissionSHA256: ip.AdmissionSHA256}
	if err != nil || chain.Answer != artifacts.RecordFound || chain.Value != wantChain {
		t.Fatalf("chain = %+v, %v; want %+v", chain, err, wantChain)
	}
	if files := w.admissionFiles(t); len(files) != 1 {
		t.Fatalf("admission records %v; want one", files)
	}
}

// TestSubmitRun_AdmitsAFanOutsItems: the admission pins a fan-out's item maximum
// whether or not its task holds an identity, since the engine records every
// item's result and admits no item past it.
func TestSubmitRun_AdmitsAFanOutsItems(t *testing.T) {
	wf := undigestedIdentityOrkflow()
	wf.Tasks[0].Each = &orkstackv1.EachSpec{Over: "{{ .inputs.xs }}", As: "x", MaxItems: 4}
	wf.Tasks[1].Each = &orkstackv1.EachSpec{Over: "{{ .inputs.xs }}", As: "x", MaxItems: 3}
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{digested(wf)}})
	run, err := w.submit(t, "deploy", "")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	read, err := w.records.ReadAdmission(context.Background(), artifacts.DefaultNamespace, run.GetId(), w.starter.startedPlans()[0].Identity.AdmissionSHA256)
	if err != nil || read.Answer != artifacts.RecordFound {
		t.Fatalf("the admission reads %+v, %v", read, err)
	}
	if apply, report := read.Value.Tasks["apply"], read.Value.Tasks["report"]; apply.MaxItems != 4 || report.MaxItems != 3 || report.Set != 0 {
		t.Fatalf("apply %+v, report %+v; want 4 and 3 items", apply, report)
	}
}

// TestSubmitRun_AdmitsNothingForARunThatHoldsNone: a plain Orkflow's plan
// encodes as it did, and nothing is written.
func TestSubmitRun_AdmitsNothingForARunThatHoldsNone(t *testing.T) {
	plain := identityOrkflow()
	plain.Name = "plain"
	plain.Tasks[0].Identities = nil
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{plain}})
	run, err := w.submit(t, "plain", "")
	if err != nil {
		t.Fatal(err)
	}
	if plans := w.starter.startedPlans(); len(plans) != 1 || plans[0].Identity != nil {
		t.Fatalf("started plans %+v; want one with no identity", plans)
	}
	if files := w.admissionFiles(t); len(files) != 0 {
		t.Fatalf("admission records %v; want none", files)
	}
	if chain, err := w.records.ReadChain(context.Background(), artifacts.DefaultNamespace, run.GetId()); err != nil || chain.Answer != artifacts.RecordNone {
		t.Fatalf("chain = %+v, %v; want none", chain, err)
	}
}

// observedClaimer runs onClaim before each claim reaches the shared
// claimer: what was written by then, and a binding another replica makes
// just before this one claims.
type observedClaimer struct {
	*sharedClaimer
	onClaim func()
}

func (c *observedClaimer) Claim(ctx context.Context, key string, candidate engine.Claim) (engine.Claim, error) {
	if c.onClaim != nil {
		c.onClaim()
	}
	return c.sharedClaimer.Claim(ctx, key, candidate)
}

// TestSubmitRun_AKeyedSubmissionAdmitsBeforeTheClaim: the record is in
// the store when the key is claimed, and a submission told that another
// run holds the key deletes what its candidate created and starts nothing.
func TestSubmitRun_AKeyedSubmissionAdmitsBeforeTheClaim(t *testing.T) {
	t.Run("winner", func(t *testing.T) {
		claimer := &observedClaimer{sharedClaimer: newSharedClaimer()}
		w := newAdmissionWorld(t, admissionOptions{claimer: claimer, definitions: []*orkstackv1.Orkflow{identityOrkflow()}})
		atClaim := -1
		claimer.onClaim = func() { atClaim = len(w.admissionFiles(t)) }
		if _, err := w.submit(t, "deploy", "order-1"); err != nil {
			t.Fatal(err)
		}
		if atClaim != 1 {
			t.Fatalf("%d admission records when the key was claimed; want 1", atClaim)
		}
	})
	t.Run("loser", func(t *testing.T) {
		claimer := &observedClaimer{sharedClaimer: newSharedClaimer()}
		w := newAdmissionWorld(t, admissionOptions{claimer: claimer, definitions: []*orkstackv1.Orkflow{identityOrkflow()}})
		winner := &orkstackv1.Run{Id: "ork-winner", OrkflowName: "deploy", OrkflowVersion: "1", Status: orkstackv1.Status_STATUS_PENDING}
		atClaim := -1
		claimer.onClaim = func() {
			atClaim = len(w.admissionFiles(t))
			claimer.bind(claimKeyIn(artifacts.DefaultNamespace, "order-2"), winner)
		}
		_, _ = w.submit(t, "deploy", "order-2")
		if atClaim != 1 {
			t.Fatalf("%d admission records when the key was claimed; want 1", atClaim)
		}
		if files := w.admissionFiles(t); len(files) != 0 {
			t.Fatalf("the loser left %v", files)
		}
		if started := w.starter.startedRuns(); len(started) != 0 {
			t.Fatalf("the loser started %v", started)
		}
	})
}

// failingAdmissions is the real record store whose admission writes fail
// after failAfter of them: a store outage at one write, which a real store
// cannot be made to have on demand. Its deletes are the real store's,
// counted.
type failingAdmissions struct {
	*artifacts.RecordStore
	failAfter int
	deletes   int
}

func (f *failingAdmissions) WriteAdmission(ctx context.Context, a artifacts.Admission) (string, bool, error) {
	if f.failAfter == 0 {
		return "", false, errors.New("the store did not answer")
	}
	f.failAfter--
	return f.RecordStore.WriteAdmission(ctx, a)
}

func (f *failingAdmissions) DeleteAdmission(ctx context.Context, ns, runID, sha string) error {
	f.deletes++
	return f.RecordStore.DeleteAdmission(ctx, ns, runID, sha)
}

// TestSubmitRun_AFailedAdmissionWriteRefusesBeforeTheClaim: unavailable,
// with no key claimed, nothing started, and nothing left: the child's
// record, written before the run's own failed, is deleted, and so is the
// run's own, whose failed write may have landed.
func TestSubmitRun_AFailedAdmissionWriteRefusesBeforeTheClaim(t *testing.T) {
	dir := t.TempDir()
	real, err := artifacts.NewRecordStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	parent, child := composingOrkflows()
	faulty := &failingAdmissions{RecordStore: real, failAfter: 1}
	claimer := newSharedClaimer()
	w := newAdmissionWorld(t, admissionOptions{claimer: claimer, records: faulty, dir: dir, definitions: []*orkstackv1.Orkflow{child, parent}})
	_, err = w.submit(t, "release", "order-3")
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), "admission record") {
		t.Fatalf("submit = %v; want unavailable naming the admission record", err)
	}
	if proposed, _ := claimer.counts(); proposed != 0 {
		t.Fatalf("%d claims were made; a failed admission claims nothing", proposed)
	}
	if started := w.starter.startedRuns(); len(started) != 0 {
		t.Fatalf("started %v", started)
	}
	// The child's, which was created, and the run's own, whose failed write
	// may have landed all the same.
	if faulty.deletes != 2 {
		t.Fatalf("%d deletes; want the child's and the run's own", faulty.deletes)
	}
	if files := w.admissionFiles(t); len(files) != 0 {
		t.Fatalf("left %v", files)
	}
}

// TestSubmitRun_AdmitsEveryRunOfATree: a parent that holds no identity is
// admitted when its child holds one, and names the child run its composing
// task starts; the child's record names its parent's
// run, record and composing task and the root, and the child's plan names
// the child's record.
func TestSubmitRun_AdmitsEveryRunOfATree(t *testing.T) {
	ctx := context.Background()
	parent, child := composingOrkflows()
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{child, parent}})
	run, err := w.submit(t, "release", "")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	plan := w.starter.startedPlans()[0]
	if plan.Identity == nil {
		t.Fatal("the parent's plan names no admission")
	}
	root, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, run.GetId(), plan.Identity.AdmissionSHA256)
	if err != nil || root.Answer != artifacts.RecordFound || len(root.Value.Sets) != 0 || len(root.Value.Tasks) != 1 {
		t.Fatalf("the parent's admission reads %+v, %v; want build alone, with no set", root, err)
	}
	entry := plan.Children["ship"]
	if entry == nil || len(root.Value.Children) != 1 || root.Value.Children["ship"] != entry.RunID {
		t.Fatalf("the parent's admission names children %v; want ship's run", root.Value.Children)
	}
	if entry == nil || entry.RunID == "" || entry.Plan.Identity == nil {
		t.Fatalf("the child's plan entry = %+v", entry)
	}
	got, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, entry.RunID, entry.Plan.Identity.AdmissionSHA256)
	if err != nil || got.Answer != artifacts.RecordFound {
		t.Fatalf("the child's admission reads %+v, %v", got, err)
	}
	want := artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: run.GetId(), AdmissionSHA256: plan.Identity.AdmissionSHA256, TaskID: "ship"}
	if got.Value.Starter != want || got.Value.RootRunID != run.GetId() || got.Value.SetOf("push") == nil {
		t.Fatalf("the child's admission = %+v; want starter %+v and root %s", got.Value, want, run.GetId())
	}
	if chain, _ := w.records.ReadChain(ctx, artifacts.DefaultNamespace, entry.RunID); chain.Answer != artifacts.RecordNone {
		t.Fatalf("a child has a chain record: %+v", chain)
	}
}

// TestSubmitRun_AdmitsADefinitionAsItWasCreated: from a document through
// compile, the digest a create gives it and the store to SubmitRun, the
// admission holds the digest the stored version has. Only the identity is added to the compiled
// document, whose language still refuses it.
func TestSubmitRun_AdmitsADefinitionAsItWasCreated(t *testing.T) {
	ctx := context.Background()
	wf, err := dsl.Compile([]byte(`apiVersion: orkstack/v1
kind: Orkflow
name: deploy
version: "1"
tasks:
  - id: apply
    uses: image:ghcr.io/acme/deploy@sha256:` + strings.Repeat("a", 64) + `
    command:
      - apply
`))
	if err != nil {
		t.Fatal(err)
	}
	wf.Tasks[0].Identities = identityOrkflow().Tasks[0].Identities
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{digested(wf)}})
	run, err := w.submit(t, "deploy", "")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	got, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, run.GetId(), w.starter.startedPlans()[0].Identity.AdmissionSHA256)
	if err != nil || got.Answer != artifacts.RecordFound {
		t.Fatalf("the admission reads %+v, %v", got, err)
	}
	stored, err := w.svc.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: "deploy", Version: "1"}))
	if err != nil {
		t.Fatal(err)
	}
	if want := stored.Msg.GetOrkflow().GetDefinitionDigest(); want == "" || "sha256:"+got.Value.DefinitionDigest != want {
		t.Fatalf("admitted digest %q; the stored version's is %q", got.Value.DefinitionDigest, want)
	}
}

// TestSubmitRun_AdmitsASharedChildUnderItsOwner: a child run of an Orkflow
// another namespace shared is admitted as that namespace's, with its
// Orkflow's id, though it runs here. Admitted as this namespace's, its
// tokens would carry the subject of this namespace's own Orkflow of that
// name, which exists here beside it.
func TestSubmitRun_AdmitsASharedChildUnderItsOwner(t *testing.T) {
	ctx := context.Background()
	parent, own := composingOrkflows()
	shared, _ := proto.Clone(own).(*orkstackv1.Orkflow)
	shared.Namespace = "platform"
	shared.SharedWith = []*orkstackv1.OrkflowShare{{Namespace: "*"}}
	digested(shared)
	parent.Tasks[1].Uses = "orkflow:platform/child@1"
	parent.Tasks[1].Orkflow.Namespace, parent.Tasks[1].Orkflow.DefinitionDigest = "platform", shared.GetDefinitionDigest()
	digested(parent)
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{own, shared, parent}})
	if _, err := w.submit(t, "release", ""); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := w.submit(t, "child", ""); err != nil {
		t.Fatalf("submit this namespace's own child: %v", err)
	}
	plans := w.starter.startedPlans()
	entry := plans[0].Children["ship"]
	read := func(runID, sha string) artifacts.Admission {
		t.Helper()
		got, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, runID, sha)
		if err != nil || got.Answer != artifacts.RecordFound {
			t.Fatalf("the admission of %s reads %+v, %v", runID, got, err)
		}
		return got.Value
	}
	child := read(entry.RunID, entry.Plan.Identity.AdmissionSHA256)
	if child.Namespace != artifacts.DefaultNamespace || child.OrkflowNamespace != "platform" {
		t.Fatalf("the shared child is admitted in %q as %q's; want default, platform's", child.Namespace, child.OrkflowNamespace)
	}
	mine := read(w.starter.startedRuns()[1], plans[1].Identity.AdmissionSHA256)
	if mine.OrkflowNamespace != "" || mine.OrkflowID == "" || child.OrkflowID == "" || mine.OrkflowID == child.OrkflowID {
		t.Fatalf("the shared child's Orkflow id is %q, this namespace's own child's %q (owner %q)", child.OrkflowID, mine.OrkflowID, mine.OrkflowNamespace)
	}
	subject := func(a artifacts.Admission) any {
		t.Helper()
		claims, err := identity.BuildClaims(identity.ClaimsInput{Admission: a, TaskID: "push", Issuer: admissionIssuer,
			Audience: "sts.amazonaws.com", Attempt: 1, Identity: "aws", Now: time.Now(), Lifetime: time.Hour, JTI: "j"})
		if err != nil {
			t.Fatal(err)
		}
		return claims["sub"]
	}
	if subject(child) == subject(mine) {
		t.Fatalf("the shared child's token and this namespace's own child's have one subject, %v", subject(child))
	}
}

// TestSubmitRun_ASharedOrkflowsOwnChildIsItsOwners: two levels deep, every
// run is the namespace's that wrote its definition. platform/lib composes
// platform's child by its bare name and default's hook by its full one, and
// both namespaces hold an Orkflow of each name. The admission names the
// owner, and the run's context hides the root's principal and reference from
// every run at or below platform's. Its tokens name nobody either; a run of
// default's own Orkflow beside them names who submitted the tree.
func TestSubmitRun_ASharedOrkflowsOwnChildIsItsOwners(t *testing.T) {
	ctx := authn.WithPrincipal(authn.WithMode(context.Background(), authn.ModeEnforce), &authn.Principal{ID: "user:corp:alice"})
	everyone := []*orkstackv1.OrkflowShare{{Namespace: "*"}}
	in := func(ns, name string, tasks ...*orkstackv1.Task) *orkstackv1.Orkflow {
		_, child := composingOrkflows()
		child.Namespace, child.Name, child.SharedWith = ns, name, everyone
		child.Tasks[0].Command = []string{ns}
		if tasks != nil {
			child.Tasks = tasks
		}
		return digested(child)
	}
	mine, theirs := in("default", "child"), in("platform", "child")
	hook, theirHook := in("default", "hook"), in("platform", "hook")
	ref := func(ns string, to *orkstackv1.Orkflow) *orkstackv1.OrkflowRef {
		return &orkstackv1.OrkflowRef{Namespace: ns, Name: to.GetName(), Version: "1", DefinitionDigest: to.GetDefinitionDigest()}
	}
	lib := in("platform", "lib",
		&orkstackv1.Task{Id: "own", Uses: "orkflow:child@1", Orkflow: ref("", theirs)},
		&orkstackv1.Task{Id: "back", Uses: "orkflow:default/hook@1", Orkflow: ref("default", hook)})
	root := in("default", "release", &orkstackv1.Task{Id: "lib", Uses: "orkflow:platform/lib@1", Orkflow: ref("platform", lib)},
		&orkstackv1.Task{Id: "mine", Uses: "orkflow:child@1", Orkflow: ref("", mine)})
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{mine, theirs, hook, theirHook, lib, root}})
	resp, err := w.svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "release"}))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	run := resp.Msg.GetRun()
	plan := w.starter.startedPlans()[0]
	libEntry := plan.Children["lib"]
	ids := map[string]string{}
	for _, tc := range []struct {
		task, owner string
		def         *orkstackv1.Orkflow
		entry       *engine.ChildRunPlan
		submitter   any
	}{
		{"own", "platform", theirs, libEntry.Plan.Children["own"], nil},
		{"back", "", hook, libEntry.Plan.Children["back"], nil},
		{"mine", "", mine, plan.Children["mine"], "user:corp:alice"},
	} {
		entry := tc.entry
		got, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, entry.RunID, entry.Plan.Identity.AdmissionSHA256)
		if err != nil || got.Answer != artifacts.RecordFound {
			t.Fatalf("%s's admission reads %+v, %v", tc.task, got, err)
		}
		a := got.Value
		if a.Namespace != artifacts.DefaultNamespace || a.OrkflowNamespace != tc.owner || "sha256:"+a.DefinitionDigest != tc.def.GetDefinitionDigest() {
			t.Errorf("%s is admitted in %q as %q's with digest %s; want default, %q's, %s", tc.task, a.Namespace, a.OrkflowNamespace, a.DefinitionDigest, tc.owner, tc.def.GetDefinitionDigest())
		}
		ids[tc.task] = a.OrkflowID
		claims, err := identity.BuildClaims(identity.ClaimsInput{Admission: a, TaskID: "push", Issuer: admissionIssuer,
			Audience: "sts.amazonaws.com", Attempt: 1, Identity: "aws", Now: time.Now(), Lifetime: time.Hour, JTI: "j"})
		if err != nil {
			t.Fatal(err)
		}
		if claims["ork_submitted_by"] != tc.submitter || claims["ork_root_run_id"] != run.GetId() || claims["ork_origin"] != "submit" {
			t.Errorf("%s's token names submitter %v, root %v, origin %v; want %v, %s, submit", tc.task,
				claims["ork_submitted_by"], claims["ork_root_run_id"], claims["ork_origin"], tc.submitter, run.GetId())
		}
	}
	if ids["own"] == "" || ids["own"] == ids["mine"] || ids["own"] == ids["back"] {
		t.Errorf("Orkflow ids %v; want three", ids)
	}
	obj, err := w.runContexts.ReadRunContext(ctx, artifacts.DefaultNamespace, run.GetId(), plan.RunContextSHA256)
	if err != nil {
		t.Fatal(err)
	}
	if len(obj.Entries) != 5 {
		t.Fatalf("%d runs in the context; want release, lib, own, back and mine", len(obj.Entries))
	}
	for _, e := range obj.Entries[1:] {
		if want := e.ParentTaskID != "mine"; e.Shared != want {
			t.Errorf("the run of task %q has the root's principal and reference hidden: %v; want %v", e.ParentTaskID, e.Shared, want)
		}
	}
}

// TestSubmitRun_AnAdmissionHoldsWhatEachTaskFollows: the record names what
// each task follows, a child's names what its start follows, in its
// parent's run or, through a composing task with no need, in a run above,
// and only a child's names the tasks it completes.
func TestSubmitRun_AnAdmissionHoldsWhatEachTaskFollows(t *testing.T) {
	ctx := context.Background()
	parent, child := composingOrkflows()
	child.Tasks = append(child.Tasks, &orkstackv1.Task{Id: "verify", Image: child.Tasks[0].Image, Needs: []string{"push"}})
	mid := &orkstackv1.Orkflow{Name: "mid", Version: "1", DefinitionDigest: "sha256:" + strings.Repeat("f", 64), Tasks: []*orkstackv1.Task{
		{Id: "inner", Uses: "orkflow:child@1", Orkflow: &orkstackv1.OrkflowRef{Name: "child", Version: "1", DefinitionDigest: child.GetDefinitionDigest()}},
	}}
	parent.Tasks[1].Uses = "orkflow:mid@1"
	parent.Tasks[1].Orkflow = &orkstackv1.OrkflowRef{Name: "mid", Version: "1", DefinitionDigest: mid.GetDefinitionDigest()}
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{child, mid, parent}})
	run, err := w.submit(t, "release", "")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	read := func(runID string, plan engine.RunPlan) artifacts.Admission {
		t.Helper()
		got, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, runID, plan.Identity.AdmissionSHA256)
		if err != nil || got.Answer != artifacts.RecordFound {
			t.Fatalf("the admission of %s reads %+v, %v", runID, got, err)
		}
		return got.Value
	}
	plan := w.starter.startedPlans()[0]
	ship := plan.Children["ship"]
	inner := ship.Plan.Children["inner"]
	build := artifacts.FollowRef{TaskID: "build", Kind: artifacts.FollowContainer}
	above := build
	above.RunID = run.GetId()

	root := read(run.GetId(), plan)
	if !reflect.DeepEqual(root.Follows, map[string][]artifacts.FollowRef{"ship": {build}}) || root.StartFollows != nil || root.Completes != nil {
		t.Errorf("the root follows %v, its start %v, completes %v", root.Follows, root.StartFollows, root.Completes)
	}
	m := read(ship.RunID, ship.Plan)
	if m.Follows != nil || !slices.Equal(m.StartFollows, []artifacts.FollowRef{above}) || !slices.Equal(m.Completes, []string{"inner"}) {
		t.Errorf("mid follows %v, its start %v, completes %v", m.Follows, m.StartFollows, m.Completes)
	}
	c := read(inner.RunID, inner.Plan)
	if !reflect.DeepEqual(c.Follows, map[string][]artifacts.FollowRef{"verify": {{TaskID: "push", Kind: artifacts.FollowContainer}}}) ||
		!slices.Equal(c.StartFollows, []artifacts.FollowRef{above}) || !slices.Equal(c.Completes, []string{"push", "verify"}) {
		t.Errorf("the child follows %v, its start %v, completes %v", c.Follows, c.StartFollows, c.Completes)
	}
}

// TestSubmitRun_AnAdmissionHoldsWhereReceivedArtifactsComeFrom: each run
// below the root names, for each artifact it receives, the task that produced
// it and its run, through a run that only passes it on, for a shared child as
// for one of this namespace, and the tasks that name each.
func TestSubmitRun_AnAdmissionHoldsWhereReceivedArtifactsComeFrom(t *testing.T) {
	ctx := context.Background()
	parent, child := composingOrkflows()
	digest := func(d string) string { return "sha256:" + strings.Repeat(d, 64) }
	input := func(names ...string) *structpb.Struct {
		out := map[string]any{}
		for _, n := range names {
			out[n] = map[string]any{"type": "artifact", "required": true}
		}
		st, err := structpb.NewStruct(out)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	child.Namespace, child.SharedWith = "platform", []*orkstackv1.OrkflowShare{{Namespace: "*"}}
	child.Inputs = input("bundle", "notes")
	child.Tasks[0].InputArtifacts = []string{"bundle"}
	mid := &orkstackv1.Orkflow{Name: "mid", Version: "1", DefinitionDigest: digest("f"), Inputs: input("archive"), Tasks: []*orkstackv1.Task{
		{Id: "write", Image: child.Tasks[0].Image},
		{Id: "inner", Needs: []string{"write"}, Uses: "orkflow:platform/child@1", ChildArtifacts: []*orkstackv1.ChildArtifact{
			{Input: "bundle", TaskId: "_inputs", Name: "archive"}, {Input: "notes", TaskId: "write", Name: "notes.md"}},
			Orkflow: &orkstackv1.OrkflowRef{Name: "child", Version: "1", DefinitionDigest: child.GetDefinitionDigest(), Namespace: "platform"}},
	}}
	parent.Tasks[1].Uses = "orkflow:mid@1"
	parent.Tasks[1].ChildArtifacts = []*orkstackv1.ChildArtifact{{Input: "archive", TaskId: "build", Name: "app.tar"}}
	parent.Tasks[1].Orkflow = &orkstackv1.OrkflowRef{Name: "mid", Version: "1", DefinitionDigest: mid.GetDefinitionDigest()}
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{child, mid, parent}})
	run, err := w.submit(t, "release", "")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	plan := w.starter.startedPlans()[0]
	ship := plan.Children["ship"]
	inner := ship.Plan.Children["inner"]
	read := func(runID string, plan engine.RunPlan) artifacts.Admission {
		t.Helper()
		got, err := w.records.ReadAdmission(ctx, artifacts.DefaultNamespace, runID, plan.Identity.AdmissionSHA256)
		if err != nil || got.Answer != artifacts.RecordFound {
			t.Fatalf("the admission of %s reads %+v, %v", runID, got, err)
		}
		return got.Value
	}
	app := artifacts.StartArtifact{RunID: run.GetId(), TaskID: "build", Kind: artifacts.FollowContainer, Name: "app.tar"}
	if got := read(run.GetId(), plan).StartArtifacts; got != nil {
		t.Errorf("the root receives %v", got)
	}
	if got := read(ship.RunID, ship.Plan).StartArtifacts; !reflect.DeepEqual(got, map[string]artifacts.StartArtifact{"archive": app}) {
		t.Errorf("mid receives %v", got)
	}
	c := read(inner.RunID, inner.Plan)
	if want := (map[string]artifacts.StartArtifact{"bundle": app,
		"notes": {RunID: ship.RunID, TaskID: "write", Kind: artifacts.FollowContainer, Name: "notes.md"}}); !reflect.DeepEqual(c.StartArtifacts, want) {
		t.Errorf("the child receives %v; want %v", c.StartArtifacts, want)
	}
	if !slices.Equal(c.Tasks["push"].Inputs, []string{"bundle"}) || c.OrkflowNamespace != "platform" {
		t.Errorf("push names %v, owner %q", c.Tasks["push"].Inputs, c.OrkflowNamespace)
	}
}

// TestSubmitRun_RefusesATaskOverTheAttemptCap: IDENTITY_ATTEMPTS_OVER_LIMIT,
// naming the task, before anything is written.
func TestSubmitRun_RefusesATaskOverTheAttemptCap(t *testing.T) {
	w := newAdmissionWorld(t, admissionOptions{attemptsMax: 2, definitions: []*orkstackv1.Orkflow{identityOrkflow()}})
	_, err := w.submit(t, "deploy", "")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "IDENTITY_ATTEMPTS_OVER_LIMIT") || !strings.Contains(err.Error(), `"apply"`) {
		t.Fatalf("submit = %v", err)
	}
	if files := w.admissionFiles(t); len(files) != 0 || len(w.starter.startedRuns()) != 0 {
		t.Fatalf("wrote %v, started %v", files, w.starter.startedRuns())
	}
}

// TestPlanRun_NamesWhatEachTaskFollows: a plan of a run whose tree holds an
// identity names, for each task, the tasks whose record it needs before it
// starts; a plain run's plan names none.
func TestPlanRun_NamesWhatEachTaskFollows(t *testing.T) {
	plain := identityOrkflow()
	plain.Name = "plain"
	plain.Tasks[0].Identities = nil
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{identityOrkflow(), plain}})
	follows := func(name string) map[string][]string {
		resp, err := w.svc.PlanRun(context.Background(), connect.NewRequest(&orkstackv1.PlanRunRequest{OrkflowName: name}))
		if err != nil {
			t.Fatalf("plan %s: %v", name, err)
		}
		out := map[string][]string{}
		for _, task := range resp.Msg.GetTasks() {
			if len(task.GetFollows()) > 0 {
				out[task.GetTaskId()] = task.GetFollows()
			}
		}
		return out
	}
	got := follows("deploy")
	if len(got) != 2 || strings.Join(got["report"], ",") != "apply" || strings.Join(got["approve"], ",") != "report" {
		t.Fatalf("deploy's plan follows %v; want report on apply, approve on report", got)
	}
	if got := follows("plain"); len(got) != 0 {
		t.Fatalf("a plain run's plan follows %v", got)
	}
}

// TestSubmitRun_AnAdmittedRunWritesItsRunContext: an identity run is
// admitted before it starts, and its run-context object is written with its
// admission, under the id it starts with, so its tasks can read it.
func TestSubmitRun_AnAdmittedRunWritesItsRunContext(t *testing.T) {
	w := newAdmissionWorld(t, admissionOptions{definitions: []*orkstackv1.Orkflow{identityOrkflow()}})
	run, err := w.submit(t, "deploy", "")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	w.starter.mu.Lock()
	plans := append([]engine.RunPlan(nil), w.starter.plans...)
	w.starter.mu.Unlock()
	if len(plans) != 1 || plans[0].Identity == nil || plans[0].RunContextSHA256 == "" {
		t.Fatalf("started plans %+v; want one admitted and naming its run context", plans)
	}
	obj, err := w.runContexts.ReadRunContext(context.Background(), artifacts.DefaultNamespace, run.GetId(), plans[0].RunContextSHA256)
	if err != nil {
		t.Fatalf("the admitted run's context: %v", err)
	}
	if obj.Root.Orkflow.Name != "deploy" {
		t.Fatalf("run context root = %+v", obj.Root)
	}
}
