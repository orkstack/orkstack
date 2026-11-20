package eventsources

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func specSource() *orkstackv1.EventSource {
	src := validSource()
	src.Id = "evs-0123456789abcdef"
	src.Image = "ghcr.io/acme/source@sha256:" + strings.Repeat("b", 64)
	src.With = &structpb.Struct{Fields: map[string]*structpb.Value{"stream": structpb.NewStringValue("ORDERS"), "tls": structpb.NewBoolValue(true)}}
	src.Env = map[string]string{"LOG": "debug"}
	src.Secrets = []*orkstackv1.SecretRef{{Name: "creds", Source: "env:NATS_CREDS"}}
	src.Resources = &orkstackv1.ResourceRequest{Cpu: "500m", Memory: "256Mi"}
	src.Concurrency = 4
	src.JudgingRevision = 7
	return src
}

var specImage = Image{Contracts: []int32{1}, Profiles: []int32{1}}

func TestServiceSpec_DigestStable(t *testing.T) {
	_, a, da, err := BuildServiceSpec(specSource(), specImage, Limits{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, b, db, err := BuildServiceSpec(proto.Clone(specSource()).(*orkstackv1.EventSource), specImage, Limits{}, 0)
	if err != nil || string(a) != string(b) || da != db {
		t.Fatalf("one source built two specs:\n%s\n%s", a, b)
	}
	changed := specSource()
	changed.Concurrency = 2
	if _, _, dc, _ := BuildServiceSpec(changed, specImage, Limits{}, 0); dc == da {
		t.Fatal("a service field changed and the digest did not")
	}
	judged := specSource()
	judged.Rate = 50
	judged.Match = nil
	if _, _, dj, _ := BuildServiceSpec(judged, specImage, Limits{}, 0); dj != da {
		t.Fatal("a judging field changed the service spec")
	}
	spec, err := ReadServiceSpec(a)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Delivery != "at-least-once" || spec.Concurrency != 4 || spec.Limits.MaxBodyBytes != DefaultMaxMessageBytes ||
		spec.Limits.AnswerSeconds != 30 || spec.Limits.HoldSeconds != 960 || string(spec.With) != `{"stream":"ORDERS","tls":true}` ||
		spec.Secrets[0] != (SpecSecret{Name: "creds", Source: "env:NATS_CREDS"}) {
		t.Fatalf("spec = %+v", spec)
	}
}

func TestHoldSeconds_IsOneTryTwoParksAndAMinute(t *testing.T) {
	for _, tc := range []struct{ park, want int }{{0, 960}, {300, 960}, {60, 480}, {600, 1560}} {
		if got := HoldSeconds(tc.park); got != tc.want {
			t.Errorf("HoldSeconds(%d) = %d; want %d", tc.park, got, tc.want)
		}
	}
}

func TestReadServiceSpec_StrictUnknownFormatRefused(t *testing.T) {
	_, body, _, err := BuildServiceSpec(specSource(), specImage, Limits{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"format 2":      strings.Replace(string(body), `"format":1`, `"format":2`, 1),
		"unknown field": strings.Replace(string(body), `"format":1`, `"format":1,"later":true`, 1),
		"trailing":      string(body) + "{}",
		"not JSON":      "spec",
		"no source id":  strings.Replace(string(body), `"id":"evs-0123456789abcdef"`, `"id":"x"`, 1),
	} {
		if _, err := ReadServiceSpec([]byte(raw)); !errors.Is(err, ErrSpecUnreadable) {
			t.Errorf("%s: err = %v; want ErrSpecUnreadable", name, err)
		}
	}
}

func TestJudgingSpec_CarriesOnlyJudgingFields(t *testing.T) {
	src := specSource()
	_, body, digest, err := BuildJudgingSpec(src, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	spec, got, err := ReadJudgingSpec(body)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Revision != 7 || got.GetJudgingRevision() != 7 || got.GetName() != src.GetName() || got.GetOrkflowName() != src.GetOrkflowName() ||
		len(got.GetMatch()) != 1 || got.GetRate() != DefaultRate {
		t.Fatalf("judging source = %v", got)
	}
	for _, leak := range []string{"ORDERS", "env:NATS_CREDS", "256Mi", "debug", src.GetImage()} {
		if strings.Contains(string(body), leak) {
			t.Fatalf("the judging spec carries a service field (%s): %s", leak, body)
		}
	}
	service := specSource()
	service.Concurrency = 1
	if _, _, d2, _ := BuildJudgingSpec(service, Limits{}); d2 != digest {
		t.Fatal("a service field changed the judging spec")
	}
	stray := strings.Replace(string(body), `"name":"order-created"`, `"name":"order-created","uses":"x"`, 1)
	if _, _, err := ReadJudgingSpec([]byte(stray)); !errors.Is(err, ErrSpecUnreadable) || !strings.Contains(err.Error(), "uses") {
		t.Fatalf("a judging spec carrying uses: %v", err)
	}
	if _, _, err := ReadJudgingSpec([]byte(strings.Replace(string(body), `"format":1`, `"format":9`, 1))); !errors.Is(err, ErrSpecUnreadable) {
		t.Fatalf("format 9: %v", err)
	}
}

func FuzzServiceSpecRecord(f *testing.F) {
	_, body, _, _ := BuildServiceSpec(specSource(), specImage, Limits{}, 0)
	f.Add(body)
	f.Add([]byte(`{"format":1}`))
	f.Add([]byte(`{"format":2,"eventSource":{"id":"evs-0123456789abcdef","name":"a"}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		spec, err := ReadServiceSpec(raw)
		if err != nil {
			if !errors.Is(err, ErrSpecUnreadable) {
				t.Fatalf("an unreadable spec answered %v", err)
			}
			return
		}
		if spec.Format != SpecFormat || !ValidID(spec.Source.ID) || !ValidName(spec.Source.Name) {
			t.Fatalf("a spec that reads has %+v", spec)
		}
		again, _, err := encodeSpec(spec)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ReadServiceSpec(again); err != nil {
			t.Fatalf("a spec that reads does not read again once written: %v", err)
		}
	})
}

func FuzzJudgingSpecRecord(f *testing.F) {
	_, body, _, _ := BuildJudgingSpec(specSource(), Limits{})
	f.Add(body)
	f.Add([]byte(`{"format":1,"revision":1,"source":{"id":"evs-0123456789abcdef","name":"a","uses":"x"}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		_, src, err := ReadJudgingSpec(raw)
		if err != nil {
			if !errors.Is(err, ErrSpecUnreadable) {
				t.Fatalf("an unreadable spec answered %v", err)
			}
			return
		}
		if src.GetUses() != "" || src.GetWith() != nil || len(src.GetSecrets()) != 0 || len(src.GetEnv()) != 0 || src.GetImage() != "" {
			t.Fatalf("a judging spec that reads carries a service field: %v", src)
		}
	})
}
