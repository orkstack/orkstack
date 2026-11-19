package payloadcrypt

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"strings"
	"testing"
)

func testRunMemo(t *testing.T, engine *Key) (*Key, KeyMemo) {
	t.Helper()
	tree, memo, err := NewRunTree(context.Background(), NewStaticDataKeys("pk_test", testKey(t, 0x42)), engine, "payments", "ns_01", "ork-root")
	if err != nil {
		t.Fatal(err)
	}
	return tree, memo
}

func TestDerive_TreeAndScheduleKeysDiffer(t *testing.T) {
	dk := testKey(t, 0x42)
	a, _ := DeriveRunTreeKey(dk, "x")
	b, _ := DeriveScheduleKey(dk, "x")
	c, _ := DeriveRunTreeKey(dk, "y")
	a2, _ := DeriveRunTreeKey(dk, "x")
	if a.ID() == b.ID() || a.ID() == c.ID() {
		t.Fatal("a run tree and a schedule, or two trees, share a key")
	}
	if a.ID() != a2.ID() {
		t.Fatal("derivation is not deterministic")
	}
}

func TestKeyMemo_RoundTrip(t *testing.T) {
	ek := testKey(t, 1)
	keys, _ := NewEngineKeys(ek)
	tree, memo := testRunMemo(t, ek)
	if memo.Version != 1 || memo.Kind != KindRun || memo.EngineKeyID != ek.ID() || memo.DataKeyID != "pk_test" ||
		memo.Namespace != "payments" || memo.NamespaceID != "ns_01" || memo.RootRunID != "ork-root" {
		t.Fatalf("memo = %+v", memo)
	}
	got, err := memo.Open(keys, "")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID() != tree.ID() {
		t.Fatal("unwrapped another key")
	}
	raw, err := json.Marshal(memo)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > 300 {
		t.Errorf("memo field is %d bytes; the design budgets about 150 plus its binding", len(raw))
	}
	back, err := ParseKeyMemo(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := back.Open(keys, ""); err != nil {
		t.Fatal(err)
	}
}

// A memo changed in any field it is bound to opens nothing; a miss is told
// apart from a keyless memo.
func TestKeyMemo_BindingAndMiss(t *testing.T) {
	ek := testKey(t, 1)
	keys, _ := NewEngineKeys(ek)
	_, memo := testRunMemo(t, ek)
	change := map[string]func(m *KeyMemo){
		"namespace":    func(m *KeyMemo) { m.Namespace = "other" },
		"namespace id": func(m *KeyMemo) { m.NamespaceID = "ns_02" },
		"root run":     func(m *KeyMemo) { m.RootRunID = "ork-other" },
		"data key":     func(m *KeyMemo) { m.DataKeyID = "pk_other" },
		"kind":         func(m *KeyMemo) { m.Kind = KindSchedule },
		"a wrap bit":   func(m *KeyMemo) { m.Wrap = append([]byte(nil), m.Wrap...); m.Wrap[3] ^= 1 },
		"a cut wrap":   func(m *KeyMemo) { m.Wrap = m.Wrap[:len(m.Wrap)-1] },
		"version":      func(m *KeyMemo) { m.Version = 2 },
	}
	for name, f := range change {
		t.Run(name, func(t *testing.T) {
			m := memo
			f(&m)
			if _, err := m.Open(keys, ""); !errors.Is(err, ErrKeyless) {
				t.Fatalf("Open = %v, want ErrKeyless", err)
			}
		})
	}
	t.Run("an engine key this process lacks", func(t *testing.T) {
		other, _ := NewEngineKeys(testKey(t, 2))
		if _, err := memo.Open(other, ""); !errors.Is(err, ErrEngineKeyMissing) {
			t.Fatalf("Open = %v, want ErrEngineKeyMissing", err)
		}
	})
	t.Run("a nil key set is a miss", func(t *testing.T) {
		if _, err := memo.Open(nil, ""); !errors.Is(err, ErrEngineKeyMissing) {
			t.Fatalf("Open = %v, want ErrEngineKeyMissing", err)
		}
	})
}

// A schedule's wrap is bound to the id the cluster stamped, never to one its
// memo names.
func TestKeyMemo_ScheduleBoundToStamp(t *testing.T) {
	ek := testKey(t, 1)
	keys, _ := NewEngineKeys(ek)
	key, memo, err := NewSchedule(context.Background(), NewStaticDataKeys("pk_s", testKey(t, 5)), ek, "payments", "ns_01", "payments/daily")
	if err != nil {
		t.Fatal(err)
	}
	if memo.RootRunID != "" || memo.Kind != KindSchedule {
		t.Fatalf("memo = %+v", memo)
	}
	got, err := memo.Open(keys, "payments/daily")
	if err != nil || got.ID() != key.ID() {
		t.Fatalf("Open(own stamp) = %v, %v", got, err)
	}
	if _, err := memo.Open(keys, "payments/weekly"); !errors.Is(err, ErrKeyless) {
		t.Fatalf("Open(another schedule's stamp) = %v, want ErrKeyless", err)
	}
	if _, err := memo.Open(keys, ""); !errors.Is(err, ErrKeyless) {
		t.Fatalf("Open(no stamp) = %v, want ErrKeyless", err)
	}
}

// The longest schedule id the cluster holds binds a wrap.
func TestKeyMemo_LongestScheduleID(t *testing.T) {
	ek := testKey(t, 1)
	id := "ork-schedule/" + strings.Repeat("n", 200) + "/" + strings.Repeat("s", 200)
	key, memo, err := NewSchedule(context.Background(), NewStaticDataKeys("pk_s", testKey(t, 5)), ek, "ns", "ns_01", id)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := memo.Open(mustEngineKeys(t, ek), id); err != nil || got.ID() != key.ID() {
		t.Fatalf("Open = %v, %v", got, err)
	}
}

func TestParseKeyMemo_Strict(t *testing.T) {
	ek := testKey(t, 1)
	_, memo := testRunMemo(t, ek)
	good, _ := json.Marshal(memo)
	for name, raw := range map[string][]byte{
		"empty":         nil,
		"not json":      []byte("{"),
		"unknown field": bytes.Replace(good, []byte(`{`), []byte(`{"extra":1,`), 1),
		"trailing data": append(append([]byte(nil), good...), []byte(` {}`)...),
		"version 2":     bytes.Replace(good, []byte(`"v":1`), []byte(`"v":2`), 1),
		"bad engine id": bytes.Replace(good, []byte(ek.ID()), []byte("ZZZZ"), 1),
		"unknown kind":  bytes.Replace(good, []byte(`"k":"run"`), []byte(`"k":"tenant"`), 1),
	} {
		if _, err := ParseKeyMemo(raw); !errors.Is(err, ErrKeyless) {
			t.Errorf("%s: ParseKeyMemo = %v, want ErrKeyless", name, err)
		}
	}
}

type failingDataKeys struct{}

func (failingDataKeys) DataKey(context.Context, string) (string, *Key, error) {
	return "", nil, errors.New("provider away")
}

func TestNewRunTree_NilKeys(t *testing.T) {
	if _, _, err := NewRunTree(context.Background(), NewStaticDataKeys("pk", nil), testKey(t, 1), "ns", "ns_01", "ork-1"); err == nil {
		t.Fatal("NewRunTree accepted a nil data key")
	}
	if _, _, err := NewSchedule(context.Background(), NewStaticDataKeys("pk", testKey(t, 2)), nil, "ns", "ns_01", "s"); err == nil {
		t.Fatal("NewSchedule accepted a nil engines' key")
	}
	if _, err := DeriveRunTreeKey(nil, "x"); err == nil {
		t.Fatal("DeriveRunTreeKey accepted a nil key")
	}
	_, _, err := NewRunTree(context.Background(), NewStaticDataKeys("pk", testKey(t, 2)), testKey(t, 1), "", "ns_01", "ork-1")
	if err == nil || errors.Is(err, ErrKeyless) {
		t.Fatalf("an empty namespace at submit = %v, want a plain error, not a run's keyless start", err)
	}
}

func TestNewRunTree_DataKeyFailure(t *testing.T) {
	if _, _, err := NewRunTree(context.Background(), failingDataKeys{}, testKey(t, 1), "ns", "ns_01", "ork-1"); err == nil {
		t.Fatal("NewRunTree succeeded without a data key")
	}
}
