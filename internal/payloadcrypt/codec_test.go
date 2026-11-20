package payloadcrypt

import (
	"bytes"
	"errors"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

func TestCodec_EncodesMarkedAndDecodesSealed(t *testing.T) {
	k := testKey(t, 6)
	keys := func(c Context) (*Key, error) {
		if c == RunContext("ork-1") {
			return k, nil
		}
		return nil, errors.New("no key for " + c.String())
	}
	codec := NewCodec(keys)
	plain := jsonPayload(`"plain"`)
	marked := Mark(jsonPayload(`"secret"`), RunContext("ork-1"))
	already, _ := SealPayload(k, RunContext("ork-1"), jsonPayload(`"already"`))

	enc, err := codec.Encode([]*commonpb.Payload{plain, marked, already})
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(enc[0], plain) {
		t.Error("an unmarked payload changed")
	}
	if !IsSealed(enc[1]) || bytes.Contains(enc[1].Data, []byte("secret")) {
		t.Error("a marked payload was not sealed")
	}
	if _, ok := enc[1].Metadata[metaMark]; ok {
		t.Error("the mark reached a sealed payload")
	}
	if !proto.Equal(enc[2], already) {
		t.Error("an already sealed payload was touched")
	}

	dec, err := codec.Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(dec[0], plain) || string(dec[1].Data) != `"secret"` || string(dec[2].Data) != `"already"` {
		t.Fatalf("decoded %v", dec)
	}
}

// A marked payload with no key fails closed: it is never written plaintext.
func TestCodec_FailsClosed(t *testing.T) {
	codec := NewCodec(func(Context) (*Key, error) { return nil, errors.New("away") })
	if _, err := codec.Encode([]*commonpb.Payload{Mark(jsonPayload(`1`), RunContext("x"))}); err == nil {
		t.Fatal("Encode wrote a marked payload without a key")
	}
	bad := Mark(jsonPayload(`1`), RunContext("x"))
	bad.Metadata[metaMark] = []byte("tenant:x")
	if _, err := NewCodec(func(Context) (*Key, error) { return testKey(t, 1), nil }).Encode([]*commonpb.Payload{bad}); err == nil {
		t.Fatal("Encode accepted a mark of no known kind")
	}
	sealed, _ := SealPayload(testKey(t, 1), RunContext("x"), jsonPayload(`1`))
	if _, err := codec.Decode([]*commonpb.Payload{sealed}); err == nil {
		t.Fatal("Decode passed a sealed payload it could not open")
	}
}
