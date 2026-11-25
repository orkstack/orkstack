package payloadcrypt

import (
	"bytes"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

// In a sealed run the workflow face seals every payload it writes unless the
// value is marked Plain (the user's decision, round 20, E1, 2026-11-20): a
// site nobody marked is sealed, never plaintext.
func TestConverter_SealsByDefaultInASealedRun(t *testing.T) {
	inner := converter.GetDefaultDataConverter()
	scope := NewScope(testKey(t, 4), RunContext("ork-1"))
	face := &Converter{inner: inner, run: &KeyState{scope: scope}}

	unmarked, err := face.ToPayload("a value no site marked")
	if err != nil || !IsSealed(unmarked) || bytes.Contains(unmarked.Data, []byte("no site marked")) {
		t.Fatalf("an unmarked value in a sealed run = %v, %v; want it sealed", unmarked, err)
	}
	if opened, err := scope.Open(unmarked); err != nil || !bytes.Contains(opened.Data, []byte("no site marked")) {
		t.Fatalf("the default seal does not open for the run: %v", err)
	}
	plain, err := face.ToPayload(Plain("a memo value"))
	want, _ := inner.ToPayload("a memo value")
	if err != nil || IsSealed(plain) || !proto.Equal(plain, want) {
		t.Fatalf("a Plain value in a sealed run = %v, %v; want it as the SDK writes it", plain, err)
	}
	raw := converter.NewRawValue(want)
	if got, err := face.ToPayload(raw); err != nil || !proto.Equal(got, want) {
		t.Fatalf("a raw value was rewritten: %v, %v", got, err)
	}
	ps, err := face.ToPayloads("x", Plain("y"))
	if err != nil || !IsSealed(ps.Payloads[0]) || IsSealed(ps.Payloads[1]) {
		t.Fatalf("ToPayloads = %v, %v", ps, err)
	}

	// A run that is not sealed writes everything, Plain or not, as before.
	open := &Converter{inner: inner, run: &KeyState{}}
	for _, v := range []any{"x", Plain("x")} {
		got, err := open.ToPayload(v)
		if err != nil || !proto.Equal(got, mustPayload(t, inner, "x")) {
			t.Fatalf("an unsealed run wrote %v, %v", got, err)
		}
	}
}

func mustPayload(t *testing.T, dc converter.DataConverter, v any) *commonpb.Payload {
	t.Helper()
	p, err := dc.ToPayload(v)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// A Plain mark holding another mark is refused, never written as an empty
// object.
func TestConverter_RefusesNestedPlain(t *testing.T) {
	scope := NewScope(testKey(t, 4), RunContext("ork-1"))
	c := NewConverter(converter.GetDefaultDataConverter(), nil)
	for _, v := range []any{plainValue{v: Seal(scope, "x")}, plainValue{v: PlainBinary(nil)}} {
		if _, err := c.ToPayload(v); err == nil {
			t.Errorf("%T was written", v)
		}
	}
}
