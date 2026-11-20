package payloadcrypt

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

// FuzzSealedPayload holds the envelope's properties: a payload sealed for a
// context opens only for that context and key; any change to its bytes or
// metadata, a truncation, another context or an unknown version is refused,
// never opened and never a panic.
func FuzzSealedPayload(f *testing.F) {
	key, _ := NewKey(bytes.Repeat([]byte{3}, KeySize))
	// A fixed nonce, so every fuzz worker process builds the same seed.
	sealed, _ := sealPayload(key, RunContext("ork-1"), jsonPayload(`{"a":1}`), bytes.NewReader(make([]byte, nonceSize)))
	good, _ := proto.Marshal(sealed)
	f.Add(good, "run:ork-1", 0, byte(0))
	f.Add(good, "run:ork-1", 20, byte(1))
	f.Add(good, "run:ork-2", 0, byte(0))
	f.Add(good, "schedule:ork-1", 0, byte(0))
	f.Add(good[:len(good)-3], "run:ork-1", 0, byte(0))
	f.Add([]byte{}, "run:ork-1", 0, byte(0))
	f.Fuzz(func(t *testing.T, raw []byte, want string, at int, flip byte) {
		c, err := ParseContext(want)
		if err != nil {
			return
		}
		var p commonpb.Payload
		if err := proto.Unmarshal(raw, &p); err != nil {
			return
		}
		if len(p.Data) > 0 && at >= 0 {
			p.Data[at%len(p.Data)] ^= flip
		}
		out, err := OpenPayload(key, c, &p)
		if err != nil {
			if !errors.Is(err, ErrUnreadable) {
				t.Fatalf("an error that is not ErrUnreadable: %v", err)
			}
			return
		}
		// It opened: it must be exactly the original, for the original context.
		if c != RunContext("ork-1") || !proto.Equal(&p, sealed) || string(out.Data) != `{"a":1}` {
			t.Fatalf("opened something that was not sealed for %v: %v", c, &p)
		}
	})
}

// FuzzRunKeyWrap holds the memo's wrap: a changed field, another run's wrap,
// a truncated one or an unknown version never opens, and parsing never
// panics.
func FuzzRunKeyWrap(f *testing.F) {
	ek, _ := NewKey(bytes.Repeat([]byte{1}, KeySize))
	keys, _ := NewEngineKeys(ek)
	tree, memo, _ := NewRunTree(context.Background(), NewStaticDataKeys("pk_f", ek), ek, "ns", "ns_1", "ork-root")
	good, _ := json.Marshal(memo)
	f.Add(good, "")
	f.Add(bytes.Replace(good, []byte("ork-root"), []byte("ork-other"), 1), "")
	f.Add(bytes.Replace(good, []byte(`"v":1`), []byte(`"v":9`), 1), "")
	f.Add(good[:len(good)/2], "")
	f.Add([]byte(`{}`), "sched")
	f.Fuzz(func(t *testing.T, raw []byte, stamp string) {
		m, err := ParseKeyMemo(raw)
		if err != nil {
			if !errors.Is(err, ErrKeyless) {
				t.Fatalf("parse error that is not ErrKeyless: %v", err)
			}
			return
		}
		k, err := m.Open(keys, stamp)
		if err != nil {
			if !errors.Is(err, ErrKeyless) && !errors.Is(err, ErrEngineKeyMissing) {
				t.Fatalf("open error of no known kind: %v", err)
			}
			return
		}
		// It opened: it is the memo wrapped (each worker wraps with its own
		// nonce, so the wrap itself is not compared), and its key is the tree's.
		wantMemo := memo
		m.Wrap, wantMemo.Wrap = nil, nil
		got, _ := json.Marshal(m)
		want, _ := json.Marshal(wantMemo)
		if !bytes.Equal(got, want) || k.ID() != tree.ID() {
			t.Fatalf("a memo that is not the one wrapped opened: %s", got)
		}
	})
}

// FuzzKeyInfoEncoding holds §4's encoding: two different lists of fields
// never encode to the same bytes, and a field too long is refused.
func FuzzKeyInfoEncoding(f *testing.F) {
	f.Add("a", "bc", "ab", "c")
	f.Add("", "x", "x", "")
	f.Add("run", "ork-1", "run", "ork-1")
	f.Fuzz(func(t *testing.T, a, b, c, d string) {
		x, errX := Info(a, b)
		y, errY := Info(c, d)
		if errX != nil || errY != nil {
			if len(a) <= 65535 && len(b) <= 65535 && errX != nil {
				t.Fatalf("a field within bounds refused: %v", errX)
			}
			return
		}
		if bytes.Equal(x, y) != (a == c && b == d) {
			t.Fatalf("Info(%q,%q) vs Info(%q,%q): equal=%v", a, b, c, d, bytes.Equal(x, y))
		}
		z, _ := Info(a, b, "")
		if bytes.Equal(x, z) {
			t.Fatal("a list and the same list with one more empty field encode alike")
		}
	})
}
