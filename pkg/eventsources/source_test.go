package eventsources

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

var limits = Limits{
	MaxMessageBytes: 1 << 20, MaxInputBytes: 16 << 10, MaxRate: 100, NamespaceRate: 200,
	MaxSources: 200, MaxPerNamespace: 20, MaxInstances: 4, NamespaceInstances: 20,
}

func validSource() *orkstackv1.EventSource {
	return &orkstackv1.EventSource{
		Namespace:   "payments",
		Name:        "order-created",
		OrkflowName: "handle-order",
		Uses:        "ghcr.io/acme/source@sha256:" + strings.Repeat("a", 64),
		Format:      orkstackv1.MessageFormat_MESSAGE_FORMAT_JSON,
		Match:       []*orkstackv1.MatchRule{{Pointer: "/status", Equals: structpb.NewStringValue("PAID")}},
		Map:         map[string]*orkstackv1.MapRule{"order_id": {Pointer: "/id"}},
	}
}

func TestSource_Validate(t *testing.T) {
	tests := []struct {
		name  string
		edit  func(*orkstackv1.EventSource)
		field string
	}{
		{"valid", func(*orkstackv1.EventSource) {}, ""},
		{"no name", func(s *orkstackv1.EventSource) { s.Name = "" }, "name"},
		{"bad name", func(s *orkstackv1.EventSource) { s.Name = "Order" }, "name"},
		{"bad namespace", func(s *orkstackv1.EventSource) { s.Namespace = "Default" }, "namespace"},
		{"namespace with space", func(s *orkstackv1.EventSource) { s.Namespace = "a " }, "namespace"},
		{"no orkflow", func(s *orkstackv1.EventSource) { s.OrkflowName = "" }, "orkflow"},
		{"another namespace's orkflow", func(s *orkstackv1.EventSource) { s.OrkflowName = "other/flow" }, "orkflow"},
		{"no uses", func(s *orkstackv1.EventSource) { s.Uses = "" }, "uses"},
		{"instances over", func(s *orkstackv1.EventSource) { s.Instances = 5 }, "instances"},
		{"instances negative", func(s *orkstackv1.EventSource) { s.Instances = -1 }, "instances"},
		{"concurrency over", func(s *orkstackv1.EventSource) { s.Concurrency = 65 }, "concurrency"},
		{"rate over", func(s *orkstackv1.EventSource) { s.Rate = 101 }, "rate"},
		{"max bytes over the deployment", func(s *orkstackv1.EventSource) { s.MaxBytes = 2 << 20 }, "max_bytes"},
		{"negative breaker", func(s *orkstackv1.EventSource) { s.RejectBreaker = proto.Int32(-1) }, "reject_breaker"},
		{"description over", func(s *orkstackv1.EventSource) { s.Description = strings.Repeat("d", 257) }, "description"},
		{"test with two selectors", func(s *orkstackv1.EventSource) { s.Match[0].Attribute = "x" }, "match[0]"},
		{"test with two tests", func(s *orkstackv1.EventSource) { s.Match[0].Exists = proto.Bool(true) }, "match[0]"},
		{"test with no test", func(s *orkstackv1.EventSource) { s.Match[0].Equals = nil }, "match[0]"},
		{"pointer on bytes", func(s *orkstackv1.EventSource) {
			s.Format = orkstackv1.MessageFormat_MESSAGE_FORMAT_BYTES
			s.Map = nil
		}, "match[0]"},
		{"field id is no test", func(s *orkstackv1.EventSource) {
			s.Match[0] = &orkstackv1.MatchRule{Field: orkstackv1.MessageField_MESSAGE_FIELD_ID, Exists: proto.Bool(true)}
		}, "match[0]"},
		{"literal not a scalar", func(s *orkstackv1.EventSource) {
			s.Match[0].Equals = structpb.NewListValue(&structpb.ListValue{})
		}, "match[0]"},
		{"literal too long", func(s *orkstackv1.EventSource) {
			s.Match[0].Equals = structpb.NewStringValue(strings.Repeat("x", 257))
		}, "match[0]"},
		{"identities", func(s *orkstackv1.EventSource) {
			s.Identities = []*orkstackv1.IdentityRef{{}}
		}, "identities"},
		{"secret with a grant", func(s *orkstackv1.EventSource) {
			s.Secrets = []*orkstackv1.SecretRef{{Name: "c", Source: "vault:x", Grant: "g"}}
		}, "secrets[0]"},
		{"secret with a bad name", func(s *orkstackv1.EventSource) {
			s.Secrets = []*orkstackv1.SecretRef{{Name: "a b", Source: "env:X"}}
		}, "secrets[0]"},
		{"secret named as a directory", func(s *orkstackv1.EventSource) {
			s.Secrets = []*orkstackv1.SecretRef{{Name: "..", Source: "env:X"}}
		}, "secrets[0]"},
		{"env with a bad name", func(s *orkstackv1.EventSource) { s.Env = map[string]string{"1X": "y"} }, "env"},
		{"bad delivery", func(s *orkstackv1.EventSource) { s.Delivery = 9 }, "delivery"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := validSource()
			tt.edit(src)
			err := Validate(src, limits)
			if tt.field == "" {
				if err != nil {
					t.Fatalf("Validate = %v", err)
				}
				return
			}
			var e *Error
			if !errors.As(err, &e) || e.Field != tt.field {
				t.Fatalf("Validate = %v, want a refusal at %s", err, tt.field)
			}
		})
	}
}

func TestSource_DocumentBounds(t *testing.T) {
	tests := []struct {
		name string
		edit func(*orkstackv1.EventSource)
	}{
		{"seventeen tests", func(s *orkstackv1.EventSource) {
			for len(s.Match) < 17 {
				s.Match = append(s.Match, &orkstackv1.MatchRule{Attribute: "a", Exists: proto.Bool(true)})
			}
		}},
		{"in over 32", func(s *orkstackv1.EventSource) {
			for i := 0; i < 33; i++ {
				s.Match[0].In = append(s.Match[0].In, structpb.NewNumberValue(float64(i)))
			}
			s.Match[0].Equals = nil
		}},
		{"65 maps", func(s *orkstackv1.EventSource) {
			for i := 0; len(s.Map) < 65; i++ {
				s.Map["i"+strings.Repeat("x", i)] = &orkstackv1.MapRule{Attribute: "a"}
			}
		}},
		{"with over 32 KiB", func(s *orkstackv1.EventSource) {
			s.With, _ = structpb.NewStruct(map[string]any{"k": strings.Repeat("v", 33<<10)})
		}},
		{"input over the bound", func(s *orkstackv1.EventSource) {
			data, _ := structpb.NewStruct(map[string]any{"k": strings.Repeat("v", 17<<10)})
			s.Input = &orkstackv1.Payload{Data: data}
		}},
		{"nine secrets", func(s *orkstackv1.EventSource) {
			for i := 0; i < 9; i++ {
				s.Secrets = append(s.Secrets, &orkstackv1.SecretRef{Name: "s" + strings.Repeat("x", i), Source: "env:X"})
			}
		}},
		{"document over 64 KiB", func(s *orkstackv1.EventSource) {
			s.Env = map[string]string{}
			for i := 0; i < 70; i++ {
				s.Env["K"+strings.Repeat("X", i)] = strings.Repeat("v", 1000)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := validSource()
			tt.edit(src)
			if err := Validate(src, limits); err == nil {
				t.Fatal("want a refusal over a bound")
			}
		})
	}
}

func TestApplyChange_RefusesImmutableFields(t *testing.T) {
	stored := validSource()
	stored.Id = "evs-0123456789abcdef"
	for _, tt := range []struct {
		name string
		edit func(*orkstackv1.EventSource)
		ok   bool
	}{
		{"description", func(s *orkstackv1.EventSource) { s.Description = "x" }, true},
		{"uses", func(s *orkstackv1.EventSource) { s.Uses = "other" }, true},
		{"name", func(s *orkstackv1.EventSource) { s.Name = "other" }, false},
		{"orkflow", func(s *orkstackv1.EventSource) { s.OrkflowName = "other" }, false},
	} {
		next := proto.Clone(stored).(*orkstackv1.EventSource)
		tt.edit(next)
		err := CheckImmutable(stored, next)
		if (err == nil) != tt.ok {
			t.Errorf("%s: CheckImmutable = %v", tt.name, err)
		}
	}
}

func TestChange_ServiceOrJudging(t *testing.T) {
	fields := (&orkstackv1.EventSource{}).ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		name := string(fields.Get(i).Name())
		if _, ok := fieldClass[name]; !ok {
			t.Errorf("field %s has no class: say whether a change to it is service, judging, neither, fixed or output", name)
		}
	}
	base := validSource()
	for _, tt := range []struct {
		edit             func(*orkstackv1.EventSource)
		service, judging bool
	}{
		{func(s *orkstackv1.EventSource) { s.Uses = "x" }, true, false},
		{func(s *orkstackv1.EventSource) { s.Pool = "p" }, true, false},
		{func(s *orkstackv1.EventSource) { s.Concurrency = 2 }, true, false},
		{func(s *orkstackv1.EventSource) { s.Env = map[string]string{"A": "b"} }, true, false},
		{func(s *orkstackv1.EventSource) { s.OrkflowVersion = "2" }, false, true},
		{func(s *orkstackv1.EventSource) { s.Rate = 5 }, false, true},
		{func(s *orkstackv1.EventSource) { s.Match = nil }, false, true},
		{func(s *orkstackv1.EventSource) { s.Description = "d" }, false, false},
		{func(s *orkstackv1.EventSource) { s.Instances = 3 }, false, false},
		{func(s *orkstackv1.EventSource) { s.Delivery = orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_LEAST_ONCE }, false, false},
		{func(s *orkstackv1.EventSource) { s.Rate = 10 }, false, false},
	} {
		next := proto.Clone(base).(*orkstackv1.EventSource)
		tt.edit(next)
		service, judging := Classify(base, next)
		if service != tt.service || judging != tt.judging {
			t.Errorf("change %+v: service=%v judging=%v, want %v %v", next, service, judging, tt.service, tt.judging)
		}
	}
}

func TestChangesWhatItSubmits(t *testing.T) {
	base := validSource()
	for _, tt := range []struct {
		edit func(*orkstackv1.EventSource)
		want bool
	}{
		{func(s *orkstackv1.EventSource) { s.OrkflowVersion = "2" }, true},
		{func(s *orkstackv1.EventSource) { s.Map = nil }, true},
		{func(s *orkstackv1.EventSource) { s.Uses = "x" }, false},
		{func(s *orkstackv1.EventSource) { s.With, _ = structpb.NewStruct(map[string]any{"a": 1}) }, false},
		{func(s *orkstackv1.EventSource) { s.Rate = 3 }, false},
	} {
		next := proto.Clone(base).(*orkstackv1.EventSource)
		tt.edit(next)
		if got := ChangesWhatItSubmits(base, next); got != tt.want {
			t.Errorf("%+v: %v, want %v", next, got, tt.want)
		}
	}
}

func TestSums_RateAndInstances(t *testing.T) {
	src := func(name string, instances, rate int32) *orkstackv1.EventSource {
		return &orkstackv1.EventSource{Name: name, Instances: instances, Rate: rate}
	}
	ns := []*orkstackv1.EventSource{src("a", 4, 20), src("b", 4, 0)}
	if err := CheckQuotas(ns, 2, src("c", 1, 10), limits); err != nil {
		t.Fatalf("within every sum: %v", err)
	}
	// 4×20 + 4×10 + 1×1 = 121 > 120
	err := CheckQuotas(ns, 2, src("c", 1, 1), Limits{MaxRate: 100, NamespaceRate: 120, MaxSources: 200, MaxPerNamespace: 20, MaxInstances: 4, NamespaceInstances: 20})
	var e *Error
	if !errors.As(err, &e) || e.Reason != ReasonLimit || !strings.Contains(e.Message, "121") {
		t.Fatalf("rate sum: %v", err)
	}
	if err := CheckQuotas(ns, 2, src("c", 4, 1), Limits{MaxRate: 100, NamespaceRate: 1000, MaxSources: 200, MaxPerNamespace: 20, MaxInstances: 4, NamespaceInstances: 11}); err == nil {
		t.Fatal("instance sum 12 over 11 must be refused")
	}
	if err := CheckQuotas(ns, 2, src("c", 1, 1), Limits{MaxRate: 100, NamespaceRate: 1000, MaxSources: 200, MaxPerNamespace: 2, MaxInstances: 4, NamespaceInstances: 20}); err == nil {
		t.Fatal("a third source over max_per_namespace 2 must be refused")
	}
	if err := CheckQuotas(ns, 200, src("c", 1, 1), limits); err == nil {
		t.Fatal("a source over max_sources must be refused")
	}
	// An update replaces itself in the sums: the namespace's list holds it.
	if err := CheckQuotas(append(ns, src("c", 1, 1)), 3, src("c", 2, 1), limits); err != nil {
		t.Fatalf("an update counts itself once: %v", err)
	}
}

func TestMemory_CoversAMessageInHand(t *testing.T) {
	const mib = 1 << 20
	// 2 × 4 lanes × 1 MiB + 32 MiB = 40 MiB.
	if err := CheckMemory(40*mib, 4, 1*mib); err != nil {
		t.Fatalf("exactly enough: %v", err)
	}
	err := CheckMemory(40*mib-1, 4, 1*mib)
	var e *Error
	if !errors.As(err, &e) || e.Field != "resources.memory" || !strings.Contains(e.Message, "2 × 4 × 1048576 + 33554432") {
		t.Fatalf("CheckMemory = %v, want the arithmetic", err)
	}
}

func TestNewID_Shape(t *testing.T) {
	id, err := NewID()
	if err != nil || !ValidID(id) {
		t.Fatalf("NewID = %q, %v", id, err)
	}
	for _, bad := range []string{"", "evs-", "evs-0123456789ABCDEF", "trg-0123456789abcdef", "evs-0123456789abcdef0"} {
		if ValidID(bad) {
			t.Errorf("ValidID(%q) = true", bad)
		}
	}
}
