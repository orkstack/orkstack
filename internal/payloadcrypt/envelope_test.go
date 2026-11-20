package payloadcrypt

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

func TestInfo_IsLengthPrefixed(t *testing.T) {
	got, err := Info("ab", "", "c")
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0, 2, 'a', 'b', 0, 0, 0, 1, 'c'}
	if !bytes.Equal(got, want) {
		t.Fatalf("Info = %v, want %v", got, want)
	}
	// "a"‖"bc" and "ab"‖"c" are one input under a plain concatenation; never here.
	x, _ := Info("a", "bc")
	y, _ := Info("ab", "c")
	if bytes.Equal(x, y) {
		t.Fatal("Info(a,bc) == Info(ab,c)")
	}
	if _, err := Info(strings.Repeat("x", 65536)); err == nil {
		t.Fatal("a field of 65,536 bytes was accepted")
	}
	if _, err := Info(strings.Repeat("x", 65535)); err != nil {
		t.Fatalf("a field of 65,535 bytes was refused: %v", err)
	}
}

func TestContext_ParseAndString(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Context
		ok   bool
	}{
		{"run:ork-abc", RunContext("ork-abc"), true},
		{"schedule:ns/daily", ScheduleContext("ns/daily"), true},
		{"run:", Context{}, false},
		{"tenant:x", Context{}, false},
		{"run", Context{}, false},
		{"", Context{}, false},
	} {
		got, err := ParseContext(tc.in)
		if tc.ok != (err == nil) || got != tc.want {
			t.Errorf("ParseContext(%q) = %v, %v; want %v ok=%v", tc.in, got, err, tc.want, tc.ok)
		}
		if tc.ok && got.String() != tc.in {
			t.Errorf("String() = %q, want %q", got.String(), tc.in)
		}
	}
}

func jsonPayload(data string) *commonpb.Payload {
	return &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(data)}
}

func TestSealPayload_RoundTrip(t *testing.T) {
	k := testKey(t, 9)
	in := jsonPayload(`{"secret":"canary-123"}`)
	sealed, err := SealPayload(k, RunContext("ork-1"), in)
	if err != nil {
		t.Fatal(err)
	}
	if !IsSealed(sealed) {
		t.Fatal("IsSealed(sealed) = false")
	}
	if got := string(sealed.Metadata[MetaEncoding]); got != Encoding {
		t.Errorf("encoding = %q", got)
	}
	if got := string(sealed.Metadata[MetaContext]); got != "run:ork-1" {
		t.Errorf("ork-ctx = %q", got)
	}
	if got := string(sealed.Metadata[MetaVersion]); got != EnvelopeVersion {
		t.Errorf("ork-v = %q", got)
	}
	if bytes.Contains(sealed.Data, []byte("canary-123")) {
		t.Fatal("sealed data holds the plaintext")
	}
	out, err := OpenPayload(k, RunContext("ork-1"), sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("opened %v, want %v", out, in)
	}
	again, _ := SealPayload(k, RunContext("ork-1"), in)
	if bytes.Equal(again.Data, sealed.Data) {
		t.Error("two seals share a nonce")
	}
}

// Every open says what it expects; anything else is ErrUnreadable.
func TestOpenPayload_RefusesWhatItDoesNotExpect(t *testing.T) {
	k, other := testKey(t, 9), testKey(t, 10)
	sealed, err := SealPayload(k, RunContext("ork-1"), jsonPayload(`1`))
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(f func(p *commonpb.Payload)) *commonpb.Payload {
		c := proto.Clone(sealed).(*commonpb.Payload)
		f(c)
		return c
	}
	cases := map[string]struct {
		key  *Key
		want Context
		p    *commonpb.Payload
	}{
		"another run's context":   {k, RunContext("ork-2"), sealed},
		"a schedule's context":    {k, ScheduleContext("ork-1"), sealed},
		"another key":             {other, RunContext("ork-1"), sealed},
		"a changed bit":           {k, RunContext("ork-1"), mutate(func(p *commonpb.Payload) { p.Data[len(p.Data)-1] ^= 1 })},
		"a truncated tag":         {k, RunContext("ork-1"), mutate(func(p *commonpb.Payload) { p.Data = p.Data[:len(p.Data)-1] })},
		"no nonce":                {k, RunContext("ork-1"), mutate(func(p *commonpb.Payload) { p.Data = p.Data[:5] })},
		"metadata names another":  {k, RunContext("ork-1"), mutate(func(p *commonpb.Payload) { p.Metadata[MetaContext] = []byte("run:ork-2") })},
		"an unknown version":      {k, RunContext("ork-1"), mutate(func(p *commonpb.Payload) { p.Metadata[MetaVersion] = []byte("2") })},
		"no version":              {k, RunContext("ork-1"), mutate(func(p *commonpb.Payload) { delete(p.Metadata, MetaVersion) })},
		"not sealed":              {k, RunContext("ork-1"), jsonPayload(`1`)},
		"nil":                     {k, RunContext("ork-1"), nil},
		"a nil key":               {nil, RunContext("ork-1"), sealed},
		"an extra metadata entry": {k, RunContext("ork-1"), mutate(func(p *commonpb.Payload) { p.Metadata["x"] = []byte("y") })},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := OpenPayload(tc.key, tc.want, tc.p); !errors.Is(err, ErrUnreadable) {
				t.Fatalf("OpenPayload = %v, want ErrUnreadable", err)
			}
		})
	}
}

// MaxOverhead holds over every payload shape OrkStack sends, from empty to
// the 256 KiB bound, and never encodes bytes as text.
func TestSealPayload_MaxOverhead(t *testing.T) {
	k := testKey(t, 1)
	long := strings.Repeat("r", MaxContextID-len("ork-"))
	for _, n := range []int{0, 1, 100, 4096, 128 << 10, 256 << 10} {
		in := &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte("json/protobuf"), "messageType": []byte("orkstack.v1.ExecuteTaskInput")},
			Data:     bytes.Repeat([]byte{'x'}, n),
		}
		sealed, err := SealPayload(k, RunContext("ork-"+long), in)
		if err != nil {
			t.Fatal(err)
		}
		if over := proto.Size(sealed) - proto.Size(in); over > MaxOverhead {
			t.Errorf("payload of %d bytes grew by %d, more than MaxOverhead %d", n, over, MaxOverhead)
		}
	}
}

func TestSealPayload_RefusesAContextOverItsBound(t *testing.T) {
	if _, err := SealPayload(testKey(t, 1), ScheduleContext(strings.Repeat("s", MaxContextID+1)), jsonPayload(`1`)); err == nil {
		t.Fatal("a context id over MaxContextID was sealed for")
	}
}

func TestSealPayload_RefusesSealingTwice(t *testing.T) {
	k := testKey(t, 1)
	sealed, _ := SealPayload(k, RunContext("ork-1"), jsonPayload(`1`))
	if _, err := SealPayload(k, RunContext("ork-1"), sealed); err == nil {
		t.Fatal("a sealed payload was sealed again")
	}
}
