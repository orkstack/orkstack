package payloadcrypt

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testKey(t testing.TB, fill byte) *Key {
	t.Helper()
	k, err := NewKey(bytes.Repeat([]byte{fill}, KeySize))
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}
	return k
}

func TestKey_IDIsFirst16HexOfSHA256(t *testing.T) {
	raw := bytes.Repeat([]byte{7}, KeySize)
	k, err := NewKey(raw)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if want := hex.EncodeToString(sum[:])[:16]; k.ID() != want {
		t.Fatalf("ID = %q, want %q", k.ID(), want)
	}
}

func TestKey_RefusesWrongSize(t *testing.T) {
	for _, n := range []int{0, 16, 31, 33, 64} {
		if _, err := NewKey(make([]byte, n)); err == nil {
			t.Errorf("NewKey(%d bytes) accepted", n)
		}
	}
}

// Every printed form of a key is its id: a %v, a %+v, a %#v, a %s, a %q, a
// %x or a logged struct holding it never shows its bytes.
func TestKey_EveryPrintedFormIsItsID(t *testing.T) {
	k := testKey(t, 0xAB)
	holder := struct {
		Name string
		K    *Key
		V    Key
	}{"x", k, *k}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d"} {
		for _, v := range []any{k, *k, holder, &holder} {
			out := fmt.Sprintf(verb, v)
			if strings.Contains(strings.ToLower(out), "abab") || strings.Contains(out, "171") {
				t.Errorf("Sprintf(%s, %T) leaks key bytes: %s", verb, v, out)
			}
		}
	}
	if got := fmt.Sprint(k); !strings.Contains(got, k.ID()) {
		t.Errorf("Sprint(key) = %q, want it to name the id %q", got, k.ID())
	}
}

func TestLoadKeyFile(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good")
	if err := os.WriteFile(good, bytes.Repeat([]byte{1}, KeySize), 0o600); err != nil {
		t.Fatal(err)
	}
	short := filepath.Join(dir, "short")
	if err := os.WriteFile(short, []byte("too short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKeyFile(good); err != nil {
		t.Errorf("LoadKeyFile(good): %v", err)
	}
	if _, err := LoadKeyFile(short); err == nil {
		t.Error("LoadKeyFile(short) accepted a 9-byte file")
	}
	if _, err := LoadKeyFile(dir); err == nil {
		t.Error("LoadKeyFile(a directory) accepted")
	}
	if _, err := NewKey(make([]byte, KeySize)); err == nil {
		t.Error("an all-zero key was accepted")
	}
	if _, err := LoadKeyFile(filepath.Join(dir, "missing")); err == nil {
		t.Error("LoadKeyFile(missing) accepted")
	}
}

func TestLoadEngineKeys(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, fill byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, bytes.Repeat([]byte{fill}, KeySize), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	a, b := write("a", 1), write("b", 2)
	keys, err := LoadEngineKeys([]string{a, b})
	if err != nil {
		t.Fatal(err)
	}
	ids := keys.IDs()
	if len(ids) != 2 || ids[0] != testKey(t, 1).ID() || ids[1] != testKey(t, 2).ID() {
		t.Fatalf("IDs = %v, want the files' ids in list order", ids)
	}
	if _, ok := keys.Get(ids[1]); !ok {
		t.Error("Get(second id) missing")
	}
	if _, ok := keys.Get("0000000000000000"); ok {
		t.Error("Get(unknown id) found a key")
	}
	if _, err := LoadEngineKeys([]string{a, write("a2", 1)}); err == nil {
		t.Error("two files holding one key were accepted")
	}
	if _, err := LoadEngineKeys(nil); err == nil {
		t.Error("an empty list was accepted")
	}
}

// A key wraps nothing until every engine seen lists it: the first of this
// engine's list that every peer lists is chosen, and none is an error.
func TestEngineKeys_WrapKey(t *testing.T) {
	k1, k2, k3 := testKey(t, 1), testKey(t, 2), testKey(t, 3)
	own, err := NewEngineKeys(k1, k2, k3)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		peers [][]string
		want  *Key
	}{
		{"no peers: own first", nil, k1},
		{"every peer has the first", [][]string{{k1.ID(), k2.ID()}, {k1.ID()}}, k1},
		{"one peer lacks the first", [][]string{{k2.ID(), k1.ID()}, {k2.ID()}}, k2},
		{"only the last is common", [][]string{{k3.ID()}, {k1.ID(), k3.ID()}}, k3},
		{"none common", [][]string{{k1.ID()}, {k2.ID()}}, nil},
		{"a peer that lists nothing", [][]string{{k1.ID()}, {}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := own.WrapKey(tc.peers)
			if tc.want == nil {
				if !errors.Is(err, ErrNoCommonEngineKey) {
					t.Fatalf("WrapKey = %v, %v; want ErrNoCommonEngineKey", got, err)
				}
				return
			}
			if err != nil || got.ID() != tc.want.ID() {
				t.Fatalf("WrapKey = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}
