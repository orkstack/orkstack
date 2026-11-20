package servicehost

import (
	"errors"
	"strings"
	"testing"
	"time"
)

var sourceVocabulary = Vocabulary{
	Reasons:  map[string]bool{"OBJECT_MISSING": true, "OBJECT_INVALID": true, "NO_DEAD_LETTER": true, "MESSAGE_LIMIT": true},
	Warnings: map[string]bool{"DELIVERY_LIMIT_LOW": true, "PULL_LIMIT_LOW": true, "MESSAGE_LIMIT_ABOVE_BOUND": true},
	Block:    "broker",
}

func TestReadStatus(t *testing.T) {
	for _, tc := range []struct {
		name, raw, want string
	}{
		{name: "ready", raw: `{"schemaVersion":1,"checkSeq":7,"ready":true,"fatal":false,"check":{"ok":true,"reason":"","field":"","warnings":["DELIVERY_LIMIT_LOW"]},"broker":{"pending":12},"later":1}`},
		{name: "unhealthy with a profile reason", raw: `{"schemaVersion":1,"ready":true,"check":{"ok":false,"reason":"OBJECT_MISSING"}}`},
		{name: "general reason", raw: `{"schemaVersion":1,"check":{"ok":false,"reason":"UNREACHABLE"}}`},
		{name: "newer version", raw: `{"schemaVersion":2,"check":{"ok":true}}`, want: "schemaVersion 2"},
		{name: "no check", raw: `{"schemaVersion":1}`, want: "no check"},
		{name: "ok with a reason", raw: `{"schemaVersion":1,"check":{"ok":true,"reason":"UNREACHABLE"}}`, want: "disagree"},
		{name: "not ok without a reason", raw: `{"schemaVersion":1,"check":{"ok":false}}`, want: "disagree"},
		{name: "a reason of its own", raw: `{"schemaVersion":1,"check":{"ok":false,"reason":"I_FEEL_BAD"}}`, want: "not a word"},
		{name: "a warning of its own", raw: `{"schemaVersion":1,"check":{"ok":true,"warnings":["x"]}}`, want: "not a word"},
		{name: "field too long", raw: `{"schemaVersion":1,"check":{"ok":false,"reason":"CONFIG_INVALID","field":"` + strings.Repeat("f", 65) + `"}}`, want: "printable"},
		{name: "field with a control", raw: `{"schemaVersion":1,"check":{"ok":false,"reason":"CONFIG_INVALID","field":"a\u001bb"}}`, want: "printable"},
		{name: "negative checkSeq", raw: `{"schemaVersion":1,"checkSeq":-1,"check":{"ok":true}}`, want: "checkSeq"},
		{name: "ready not a boolean", raw: `{"schemaVersion":1,"ready":"yes","check":{"ok":true}}`, want: "shape"},
		{name: "not an object", raw: `[]`, want: "JSON object"},
		{name: "duplicate key", raw: `{"schemaVersion":1,"schemaVersion":1,"check":{"ok":true}}`, want: "JSON object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := ReadStatus([]byte(tc.raw), sourceVocabulary)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var v *ViolationError
			if !errors.As(err, &v) || !strings.Contains(v.Reason, tc.want) {
				t.Fatalf("%+v, %v; want a violation containing %q", s, err, tc.want)
			}
		})
	}
	s, _ := ReadStatus([]byte(`{"schemaVersion":1,"checkSeq":7,"ready":true,"check":{"ok":true},"broker":{"pending":12}}`), sourceVocabulary)
	if s.CheckSeq != 7 || !s.Ready || string(s.Block) != `{"pending":12}` {
		t.Fatalf("status %+v", s)
	}
}

func TestStatus_ReadWhetherOrNotReady(t *testing.T) {
	w := NewStatusWatch(t0, 60, 30)
	unhealthy, _ := ReadStatus([]byte(`{"schemaVersion":1,"ready":false,"check":{"ok":false,"reason":"UNREACHABLE"}}`), sourceVocabulary)
	w.Observe(unhealthy, t0.Add(time.Second), true)
	if h := w.Health(t0.Add(2 * time.Second)); h != HealthUnhealthy {
		t.Fatalf("a container up and unhealthy reads %v, not as not ready", h)
	}
}

func TestStatus_Lifecycle(t *testing.T) {
	ready, _ := ReadStatus([]byte(`{"schemaVersion":1,"ready":true,"check":{"ok":true}}`), sourceVocabulary)
	notReady, _ := ReadStatus([]byte(`{"schemaVersion":1,"ready":false,"check":{"ok":true}}`), sourceVocabulary)
	fatal, _ := ReadStatus([]byte(`{"schemaVersion":1,"fatal":true,"check":{"ok":false,"reason":"CONFIG_INVALID","field":"stream"}}`), sourceVocabulary)

	w := NewStatusWatch(t0, 60, 30)
	if w.Health(t0.Add(59*time.Second)) != HealthStarting || w.Health(t0.Add(61*time.Second)) != HealthNeverReported {
		t.Fatal("no status within startupSeconds")
	}
	w.Observe(notReady, t0.Add(5*time.Second), true)
	if w.Health(t0.Add(6*time.Second)) != HealthNotReady {
		t.Fatal("not ready")
	}
	w.Observe(ready, t0.Add(10*time.Second), true)
	if w.Health(t0.Add(11*time.Second)) != HealthReady {
		t.Fatal("ready")
	}
	w.Observe(ready, t0.Add(80*time.Second), false)
	if h := w.Health(t0.Add(101 * time.Second)); h != HealthNoVerdict {
		t.Fatalf("a status not rewritten for three intervals reads %v, want no verdict", h)
	}
	w.Observe(ready, t0.Add(102*time.Second), true)
	if w.Health(t0.Add(103*time.Second)) != HealthReady {
		t.Fatal("a rewritten status is fresh again")
	}
	w.Observe(fatal, t0.Add(104*time.Second), true)
	if w.Health(t0.Add(105*time.Second)) != HealthFatal {
		t.Fatal("fatal")
	}
}

func FuzzServiceStatus(f *testing.F) {
	for _, seed := range []string{
		`{"schemaVersion":1,"checkSeq":7,"ready":true,"check":{"ok":true,"warnings":["DELIVERY_LIMIT_LOW"]},"broker":{}}`,
		`{"schemaVersion":1,"check":{"ok":false,"reason":"CONFIG_INVALID","field":"x"}}`,
		`{"schemaVersion":3}`, `{}`, `null`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		s, err := ReadStatus(raw, sourceVocabulary)
		if err != nil {
			var v *ViolationError
			if !errors.As(err, &v) {
				t.Fatalf("a bad status is not a violation: %v", err)
			}
			return
		}
		if s.OK != (s.Reason == "") || len(s.Field) > maxStatusField || !printable(s.Field) || len(s.Warnings) > maxWarnings {
			t.Fatalf("a status that reads breaks a rule: %+v", s)
		}
		if s.Reason != "" && !generalReason(s.Reason) && !sourceVocabulary.Reasons[s.Reason] {
			t.Fatalf("a reason outside the vocabulary: %q", s.Reason)
		}
	})
}
