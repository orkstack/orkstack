package task

import (
	"context"
	"errors"
	"strings"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"
)

// fakeOperator is a Temporal OperatorService in a map: what the namespace
// already indexes, and what a caller tried to add.
type fakeOperator struct {
	custom map[string]enumspb.IndexedValueType
	added  []string
	addErr error
	listEr error
}

func (f *fakeOperator) AddSearchAttributes(
	_ context.Context,
	in *operatorservice.AddSearchAttributesRequest,
	_ ...grpc.CallOption,
) (*operatorservice.AddSearchAttributesResponse, error) {
	for name := range in.GetSearchAttributes() {
		f.added = append(f.added, name)
	}
	if f.addErr != nil {
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
	_ context.Context,
	_ *operatorservice.ListSearchAttributesRequest,
	_ ...grpc.CallOption,
) (*operatorservice.ListSearchAttributesResponse, error) {
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
	if err != nil {
		t.Fatalf("ensureRunSearchAttributes: %v", err)
	}
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
	if err != nil {
		t.Fatalf("ensureRunSearchAttributes: %v", err)
	}
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
	if err != nil {
		t.Fatalf("ensureRunSearchAttributes: %v", err)
	}
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
