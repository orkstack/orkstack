package task

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// fakeOperator is a Temporal OperatorService in a map: what the namespace
// already indexes, and what a caller tried to add.
type fakeOperator struct {
	custom map[string]enumspb.IndexedValueType
	added  []string
	addErr error
	listEr error
	// listErrs are answered, one per call and in order, before listEr: a
	// namespace the cluster has registered and its operator service does not
	// see yet answers NotFound for a while, then answers.
	listErrs []error
	lists    int
	// addErrs are answered, one per call and in order, before addErr.
	addErrs []error
	// block makes every List call wait for its context to end, as a hung
	// frontend does.
	block bool
	// racer is what another replica registers while this one's registration
	// fails: it lands in custom even though addErr is returned.
	racer map[string]enumspb.IndexedValueType
}

func (f *fakeOperator) AddSearchAttributes(
	_ context.Context,
	in *operatorservice.AddSearchAttributesRequest,
	_ ...grpc.CallOption,
) (*operatorservice.AddSearchAttributesResponse, error) {
	for name := range in.GetSearchAttributes() {
		f.added = append(f.added, name)
	}
	if len(f.addErrs) > 0 {
		err := f.addErrs[0]
		f.addErrs = f.addErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if f.addErr != nil {
		for name, kind := range f.racer {
			if f.custom == nil {
				f.custom = map[string]enumspb.IndexedValueType{}
			}
			f.custom[name] = kind
		}
		return nil, f.addErr
	}
	if f.custom == nil {
		f.custom = map[string]enumspb.IndexedValueType{}
	}
	for name, kind := range in.GetSearchAttributes() {
		f.custom[name] = kind
	}
	return &operatorservice.AddSearchAttributesResponse{}, nil
}

func (f *fakeOperator) ListSearchAttributes(
	ctx context.Context,
	_ *operatorservice.ListSearchAttributesRequest,
	_ ...grpc.CallOption,
) (*operatorservice.ListSearchAttributesResponse, error) {
	f.lists++
	if f.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if len(f.listErrs) > 0 {
		err := f.listErrs[0]
		f.listErrs = f.listErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if f.listEr != nil {
		return nil, f.listEr
	}
	return &operatorservice.ListSearchAttributesResponse{CustomAttributes: f.custom}, nil
}

// A fresh namespace has no custom search attributes, so the first boot
// registers the run index. Without it, listing one namespace's live runs is a
// filter over another namespace's traffic.
func TestEnsureRunSearchAttributes_RegistersTheRunIndex(t *testing.T) {
	svc := &fakeOperator{}

	indexed, err := ensureRunSearchAttributes(context.Background(), svc, DefaultNamespace)
	must.OKf(t, err, "ensureRunSearchAttributes")
	if !indexed {
		t.Fatal("the registration succeeded but reported the index unusable")
	}
	if len(svc.added) != 1 || svc.added[0] != SearchAttrOrkflowNamespace {
		t.Fatalf("registered %v, want [%s]", svc.added, SearchAttrOrkflowNamespace)
	}
	if kind := svc.custom[SearchAttrOrkflowNamespace]; kind != enumspb.INDEXED_VALUE_TYPE_KEYWORD {
		t.Errorf("%s was registered as %v, want Keyword: a namespace is matched whole",
			SearchAttrOrkflowNamespace, kind)
	}
}

// Every replica runs this at boot, and so does every restart. An attribute
// that is already there is left alone rather than re-registered.
func TestEnsureRunSearchAttributes_LeavesAnExistingIndexAlone(t *testing.T) {
	svc := &fakeOperator{custom: map[string]enumspb.IndexedValueType{
		SearchAttrOrkflowNamespace: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}}

	indexed, err := ensureRunSearchAttributes(context.Background(), svc, DefaultNamespace)
	must.OKf(t, err, "ensureRunSearchAttributes")
	if !indexed {
		t.Fatal("an existing index reported unusable")
	}
	if len(svc.added) != 0 {
		t.Fatalf("re-registered %v on a namespace that already has the index", svc.added)
	}
}

// Two replicas booting at once race to register; losing the race is fine.
func TestEnsureRunSearchAttributes_ToleratesAConcurrentRegistration(t *testing.T) {
	svc := &fakeOperator{
		custom: map[string]enumspb.IndexedValueType{},
		addErr: serviceerror.NewAlreadyExists("already exists"),
	}
	// The winner's write lands between the two calls this one makes.
	svc.custom[SearchAttrOrkflowNamespace] = enumspb.INDEXED_VALUE_TYPE_KEYWORD

	indexed, err := ensureRunSearchAttributes(context.Background(), svc, DefaultNamespace)
	must.OKf(t, err, "ensureRunSearchAttributes")
	if !indexed {
		t.Fatal("a namespace that already has the index reported unusable")
	}
}

// A cluster that will not register it is a fact the caller has to be told, not
// a boot failure and not a silent yes: the engine still runs, and the reader
// walks the whole index instead of trusting a predicate the cluster cannot
// apply.
func TestEnsureRunSearchAttributes_ReportsAClusterThatRefuses(t *testing.T) {
	boom := errors.New("operator api is disabled")
	svc := &fakeOperator{custom: map[string]enumspb.IndexedValueType{}, addErr: boom}

	indexed, err := ensureRunSearchAttributes(context.Background(), svc, DefaultNamespace)
	if indexed {
		t.Fatal("a refused registration reported the index usable")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", err, boom)
	}
}

// TestEnsureRunRootSearchAttribute_IsItsOwnAnswer: a cluster that refuses the
// root attribute, as one at its limit of custom attributes does, keeps the
// origin attributes it already has; the answer names what was refused, and an
// attribute already there needs no registration.
func TestEnsureRunRootSearchAttribute_IsItsOwnAnswer(t *testing.T) {
	refused := &fakeOperator{
		custom: map[string]enumspb.IndexedValueType{
			SearchAttrOrkflowOriginKind: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			SearchAttrOrkflowOriginId:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
		addErr: errors.New("cannot have more than 10 search attribute of type Keyword"),
	}
	originIndexed, err := ensureRunOriginSearchAttributes(context.Background(), refused, DefaultNamespace)
	if err != nil || !originIndexed {
		t.Fatalf("origin index = %v, %v; want usable", originIndexed, err)
	}
	rootIndexed, err := ensureRunRootSearchAttribute(context.Background(), refused, DefaultNamespace)
	if rootIndexed || err == nil || !strings.Contains(err.Error(), SearchAttrOrkflowOriginRootRunId) {
		t.Fatalf("root index = %v, %v; want unusable with an error naming it", rootIndexed, err)
	}

	present := &fakeOperator{custom: map[string]enumspb.IndexedValueType{
		SearchAttrOrkflowOriginRootRunId: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}, addErr: errors.New("nothing should be registered")}
	if rootIndexed, err := ensureRunRootSearchAttribute(context.Background(), present, DefaultNamespace); err != nil || !rootIndexed {
		t.Fatalf("root index already present = %v, %v", rootIndexed, err)
	}
}

// The origin attributes carry their own answer. A cluster that already has
// OrkflowNamespace and now refuses the operator API keeps its namespace
// predicate while the origin index reports unusable; one that has all three
// reports both usable without registering anything; a fresh one registers
// both origin attributes and polls until both are present.
func TestEnsureRunOriginSearchAttributes_IsIndependentOfNamespace(t *testing.T) {
	t.Run("namespace present, registration refused", func(t *testing.T) {
		svc := &fakeOperator{
			custom: map[string]enumspb.IndexedValueType{SearchAttrOrkflowNamespace: enumspb.INDEXED_VALUE_TYPE_KEYWORD},
			addErr: errors.New("operator api is disabled"),
		}
		namespaceIndexed, err := ensureRunSearchAttributes(context.Background(), svc, DefaultNamespace)
		if err != nil || !namespaceIndexed {
			t.Fatalf("namespace index = %v, %v; want usable", namespaceIndexed, err)
		}
		originIndexed, err := ensureRunOriginSearchAttributes(context.Background(), svc, DefaultNamespace)
		if originIndexed || err == nil {
			t.Fatalf("origin index = %v, %v; want unusable with an error", originIndexed, err)
		}
		for _, name := range []string{SearchAttrOrkflowOriginKind, SearchAttrOrkflowOriginId} {
			if !strings.Contains(err.Error(), name) {
				t.Errorf("error %q does not name %s", err, name)
			}
		}
	})
	t.Run("all three present", func(t *testing.T) {
		svc := &fakeOperator{custom: map[string]enumspb.IndexedValueType{
			SearchAttrOrkflowNamespace:  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			SearchAttrOrkflowOriginKind: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			SearchAttrOrkflowOriginId:   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		}}
		namespaceIndexed, _ := ensureRunSearchAttributes(context.Background(), svc, DefaultNamespace)
		originIndexed, err := ensureRunOriginSearchAttributes(context.Background(), svc, DefaultNamespace)
		if !namespaceIndexed || !originIndexed || err != nil {
			t.Fatalf("namespace=%v origin=%v err=%v; want both usable", namespaceIndexed, originIndexed, err)
		}
		if len(svc.added) != 0 {
			t.Fatalf("registered %v on a namespace that has all three", svc.added)
		}
	})
	t.Run("one of the pair present registers only the other", func(t *testing.T) {
		svc := &fakeOperator{custom: map[string]enumspb.IndexedValueType{
			SearchAttrOrkflowOriginKind: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		}}
		originIndexed, err := ensureRunOriginSearchAttributes(context.Background(), svc, DefaultNamespace)
		if !originIndexed || err != nil {
			t.Fatalf("origin = %v, %v; want usable", originIndexed, err)
		}
		if len(svc.added) != 1 || svc.added[0] != SearchAttrOrkflowOriginId {
			t.Fatalf("registered %v, want only [%s]", svc.added, SearchAttrOrkflowOriginId)
		}
	})
	t.Run("fresh namespace registers both as Keyword", func(t *testing.T) {
		svc := &fakeOperator{}
		originIndexed, err := ensureRunOriginSearchAttributes(context.Background(), svc, DefaultNamespace)
		if !originIndexed || err != nil {
			t.Fatalf("origin = %v, %v; want usable", originIndexed, err)
		}
		for _, name := range []string{SearchAttrOrkflowOriginKind, SearchAttrOrkflowOriginId} {
			if svc.custom[name] != enumspb.INDEXED_VALUE_TYPE_KEYWORD {
				t.Errorf("%s registered as %v, want Keyword", name, svc.custom[name])
			}
		}
		if _, ok := svc.custom[SearchAttrOrkflowNamespace]; ok {
			t.Error("the origin registration must not register the namespace attribute")
		}
	})
}

// A namespace this engine just registered is not seen by the cluster's
// operator service at once: for a moment it answers NotFound. Two replicas
// booting a second apart must still reach one answer about the run index,
// because a replica that boots unindexed starts runs without the attribute and
// the indexed replica's scoped reads then miss them. Each boot waits until the
// namespace is visible, then registers, and both report the index usable.
func TestRunIndex_ReplicasBootingAroundANewNamespaceAgree(t *testing.T) {
	notFound := serviceerror.NewNamespaceNotFound("ork-new")
	cluster := &fakeOperator{listErrs: []error{notFound, notFound, notFound}}
	ctx := context.Background()

	boot := func() bool {
		t.Helper()
		must.OKf(t, waitNamespaceVisible(ctx, cluster, "ork-new", time.Minute, time.Millisecond),
			"waitNamespaceVisible")
		indexed, err := ensureRunSearchAttributes(ctx, cluster, "ork-new")
		must.OKf(t, err, "ensureRunSearchAttributes")
		return indexed
	}
	first, second := boot(), boot()
	if !first || !second {
		t.Fatalf("replica answers = %v, %v; want both indexed: a replica that boots "+
			"while its namespace is still invisible must not start runs unindexed", first, second)
	}
}

// A namespace that never becomes visible fails the boot, naming it, rather
// than leaving the engine up and unindexed.
func TestWaitNamespaceVisible_FailsWhenTheNamespaceNeverAppears(t *testing.T) {
	cluster := &fakeOperator{listEr: serviceerror.NewNamespaceNotFound("ork-gone")}
	err := waitNamespaceVisible(context.Background(), cluster, "ork-gone", 5*time.Millisecond, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "ork-gone") {
		t.Fatalf("err = %v, want a failure naming the namespace", err)
	}
	if cluster.lists < 2 {
		t.Errorf("asked %d times; want it to keep asking until the deadline", cluster.lists)
	}
}

// Any other answer than NotFound is not this wait's question: a cluster whose
// operator API is refused is reported by the registration, as before, and is
// not a boot failure.
func TestWaitNamespaceVisible_LeavesOtherRefusalsToTheRegistration(t *testing.T) {
	cluster := &fakeOperator{listEr: serviceerror.NewPermissionDenied("operator api is disabled", "")}
	if err := waitNamespaceVisible(context.Background(), cluster, "ork-x", time.Minute, time.Millisecond); err != nil {
		t.Fatalf("err = %v, want nil: only an invisible namespace is waited for", err)
	}
	if cluster.lists != 1 {
		t.Errorf("asked %d times; want one", cluster.lists)
	}
}

// Whether the run index is usable is the cluster's answer, not whether this
// replica's own registration call succeeded: a replica whose registration
// failed while another replica's landed reads the attribute back and agrees.
func TestEnsureRunSearchAttributes_AnswersFromTheClusterWhenItsRegistrationFails(t *testing.T) {
	cluster := &fakeOperator{
		addErr: serviceerror.NewUnavailable("frontend restarting"),
		racer:  map[string]enumspb.IndexedValueType{SearchAttrOrkflowNamespace: enumspb.INDEXED_VALUE_TYPE_KEYWORD},
	}
	indexed, err := ensureRunSearchAttributes(context.Background(), cluster, DefaultNamespace)
	if err != nil || !indexed {
		t.Fatalf("index = %v, %v; want usable: the cluster has the attribute", indexed, err)
	}
}

// fastVisibilityPoll makes the NotFound retries ask again at once.
func fastVisibilityPoll(t *testing.T) {
	t.Helper()
	previous := namespaceVisiblePoll
	namespaceVisiblePoll = time.Millisecond
	t.Cleanup(func() { namespaceVisiblePoll = previous })
}

// With several frontends, the one that answers the registration may not see
// the namespace yet although the one that answered the wait did. The
// registration keeps asking under the same deadline and the engine ends up
// indexed.
func TestEnsureRunSearchAttributes_RetriesANamespaceAFrontendDoesNotSeeYet(t *testing.T) {
	fastVisibilityPoll(t)
	cluster := &fakeOperator{addErrs: []error{serviceerror.NewNamespaceNotFound("ork-new")}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	must.OKf(t, waitNamespaceVisible(ctx, cluster, "ork-new", time.Minute, time.Millisecond), "waitNamespaceVisible")
	indexed, err := ensureRunSearchAttributes(ctx, cluster, "ork-new")
	if err != nil || !indexed {
		t.Fatalf("index = %v, %v; want usable after the frontend caught up", indexed, err)
	}
}

// A registration whose frontend never sees the namespace fails the boot with
// ErrNamespaceNotVisible, not the warning a refusal gets.
func TestEnsureRunSearchAttributes_FailsTheBootWhenTheNamespaceStaysInvisible(t *testing.T) {
	fastVisibilityPoll(t)
	cluster := &fakeOperator{addErr: serviceerror.NewNamespaceNotFound("ork-gone")}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := ensureRunSearchAttributes(ctx, cluster, "ork-gone")
	if !errors.Is(err, ErrNamespaceNotVisible) {
		t.Fatalf("err = %v, want ErrNamespaceNotVisible", err)
	}
}

// A call that hangs cannot carry the wait past its deadline: each call is
// bounded by what is left of it.
func TestWaitNamespaceVisible_AHungCallEndsAtTheDeadline(t *testing.T) {
	cluster := &fakeOperator{block: true}
	done := make(chan error, 1)
	go func() {
		done <- waitNamespaceVisible(context.Background(), cluster, "ork-x", 20*time.Millisecond, time.Millisecond)
	}()
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "ork-x") {
			t.Fatalf("err = %v, want the deadline, wrapped with the namespace", err)
		}
	case <-time.After(operatorCallTimeout):
		t.Fatal("the wait outlived its deadline behind a hung call")
	}
}
