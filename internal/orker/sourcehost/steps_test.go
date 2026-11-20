package sourcehost

import (
	"errors"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
)

const specMessage = `{
  "schemaVersion": 1, "seq": 42, "id": "jetstream:ORDERS:48213:1762853400123456789", "bodyBytes": 4,
  "subject": "orders.eu.created", "attributes": {"Ce-Type": ["com.acme.order.created"]}, "binaryAttributes": {"k": "AAE="},
  "publishedAt": "2026-11-11T09:30:00Z", "deliveryCount": 1, "group": "", "broker": {"stream": "ORDERS", "streamSequence": "48213"},
  "later": true
}`

func wantViolation(t *testing.T, err error, part string) {
	t.Helper()
	var v *servicehost.ViolationError
	if !errors.As(err, &v) || !strings.Contains(v.Reason, part) {
		t.Fatalf("err = %v; want a violation containing %q", err, part)
	}
}

func TestReadMessage(t *testing.T) {
	m, err := ReadMessage([]byte(specMessage))
	if err != nil {
		t.Fatal(err)
	}
	if m.Seq != 42 || m.ID == "" || m.BodyBytes != 4 || m.Oversize || m.DeliveryCount != 1 || string(m.Binary["k"]) != "\x00\x01" ||
		m.PublishedAt.IsZero() || m.Broker["stream"] != "ORDERS" || !m.BodyMatches([]byte("body")) || m.BodyMatches([]byte("bod")) {
		t.Fatalf("message %+v", m)
	}
	over, err := ReadMessage([]byte(`{"schemaVersion":1,"seq":1,"id":"m","oversizeBytes":5000000}`))
	if err != nil || !over.Oversize || over.OversizeBytes != 5000000 || over.BodyMatches(nil) {
		t.Fatalf("oversize %+v, %v", over, err)
	}
	for name, tc := range map[string]struct{ raw, want string }{
		"no id":           {`{"schemaVersion":1,"seq":1,"bodyBytes":1}`, "no id"},
		"neither size":    {`{"schemaVersion":1,"seq":1,"id":"m"}`, "exactly one"},
		"both sizes":      {`{"schemaVersion":1,"seq":1,"id":"m","bodyBytes":1,"oversizeBytes":2}`, "exactly one"},
		"negative size":   {`{"schemaVersion":1,"seq":1,"id":"m","bodyBytes":-1}`, "below 0"},
		"negative count":  {`{"schemaVersion":1,"seq":1,"id":"m","bodyBytes":1,"deliveryCount":-2}`, "deliveryCount"},
		"bad base64":      {`{"schemaVersion":1,"seq":1,"id":"m","bodyBytes":1,"binaryAttributes":{"k":"*"}}`, "base64"},
		"bad time":        {`{"schemaVersion":1,"seq":1,"id":"m","bodyBytes":1,"publishedAt":"yesterday"}`, "RFC 3339"},
		"broker too big":  {`{"schemaVersion":1,"seq":1,"id":"m","bodyBytes":1,"broker":{"a":"` + strings.Repeat("x", 1100) + `"}}`, "broker facts over"},
		"attribute shape": {`{"schemaVersion":1,"seq":1,"id":"m","bodyBytes":1,"attributes":{"a":"x"}}`, "shape"},
		"no seq":          {`{"schemaVersion":1,"id":"m","bodyBytes":1}`, "no seq"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ReadMessage([]byte(tc.raw))
			wantViolation(t, err, tc.want)
		})
	}
}

func TestReadEmptyAndSettled(t *testing.T) {
	if e, err := ReadEmpty([]byte(`{"schemaVersion":1,"seq":42}`)); err != nil || e.Error != nil || e.Seq != 42 {
		t.Fatalf("quiet poll %+v, %v", e, err)
	}
	if e, err := ReadEmpty([]byte(`{"schemaVersion":1,"seq":42,"error":{"kind":"local","code":"ENOSPC"}}`)); err != nil || e.Error.Kind != KindLocal {
		t.Fatalf("local %+v, %v", e, err)
	}
	_, err := ReadEmpty([]byte(`{"schemaVersion":1,"seq":1,"error":{"kind":"hold_expired"}}`))
	wantViolation(t, err, "kind")
	if s, err := ReadSettled([]byte(`{"schemaVersion":1,"seq":42,"ok":true}`)); err != nil || !s.OK {
		t.Fatalf("settled %+v, %v", s, err)
	}
	if s, err := ReadSettled([]byte(`{"schemaVersion":1,"seq":42,"ok":false,"error":{"kind":"hold_expired","code":""}}`)); err != nil || s.Error.Kind != KindHoldExpired {
		t.Fatalf("hold expired %+v, %v", s, err)
	}
	for name, raw := range map[string]string{
		"ok with an error":      `{"schemaVersion":1,"seq":1,"ok":true,"error":{"kind":"refused"}}`,
		"not ok without one":    `{"schemaVersion":1,"seq":1,"ok":false}`,
		"no ok":                 `{"schemaVersion":1,"seq":1}`,
		"a code with a control": `{"schemaVersion":1,"seq":1,"ok":false,"error":{"kind":"refused","code":"a\u001bb"}}`,
		"a code too long":       `{"schemaVersion":1,"seq":1,"ok":false,"error":{"kind":"refused","code":"` + strings.Repeat("c", 65) + `"}}`,
		"local is not settled":  `{"schemaVersion":1,"seq":1,"ok":false,"error":{"kind":"local"}}`,
	} {
		if _, err := ReadSettled([]byte(raw)); err == nil {
			t.Errorf("%s: read", name)
		}
	}
}

func TestSafeWord(t *testing.T) {
	if got := SafeWord("ok\x1b[31mred\xff", 64); got != "ok?[31mred?" {
		t.Fatalf("SafeWord = %q", got)
	}
	if got := SafeWord(strings.Repeat("w", 100), 8); len(got) != 8 {
		t.Fatalf("SafeWord bound: %q", got)
	}
}

func FuzzSourceMessage(f *testing.F) {
	f.Add([]byte(specMessage))
	f.Add([]byte(`{"schemaVersion":1,"seq":1,"id":"m","oversizeBytes":5}`))
	f.Add([]byte(`{"schemaVersion":1,"seq":1,"id":"","bodyBytes":0}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		m, err := ReadMessage(raw)
		if err != nil {
			var v *servicehost.ViolationError
			if !errors.As(err, &v) {
				t.Fatalf("a bad message.json is not a violation: %v", err)
			}
			return
		}
		if m.ID == "" || m.Seq < 1 || m.DeliveryCount < 0 || m.BodyBytes < 0 || m.OversizeBytes < 0 || len(m.Broker) > maxBrokerFacts {
			t.Fatalf("a message that reads breaks a rule: %+v", m)
		}
	})
}
