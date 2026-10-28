package webhooks_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

// FuzzSignAndVerify: a signature is the receiver-verifiable HMAC of exactly
// the bytes it covers. It verifies for its own secret and body, with or
// without the algorithm prefix, and for no other secret, body or signature
// text; an empty secret signs nothing and verifies nothing.
func FuzzSignAndVerify(f *testing.F) {
	f.Add("s3cr3t", []byte(`{"runId":"ork-1-1","status":"SUCCEEDED"}`), "other", []byte(`{"runId":"ork-1-1","status":"FAILED"}`), "")
	f.Add("", []byte("body"), "", []byte("body"), "")
	f.Add("k", []byte(nil), "k", []byte{0}, "sha256=")
	f.Add("k", []byte("b"), "k", []byte("b"), "sha256=sha256=")
	f.Add("\xff\x00", []byte{0xff}, "\xff", []byte{0xff}, "SHA256=00")
	f.Fuzz(func(t *testing.T, secret string, body []byte, otherSecret string, otherBody []byte, junk string) {
		signature := webhooks.Sign(secret, body)
		if secret == "" {
			if signature != "" || webhooks.Verify(secret, body, signature) || webhooks.Verify(secret, body, junk) {
				t.Fatalf("an empty secret signed or verified: %q", signature)
			}
			return
		}
		raw := strings.TrimPrefix(signature, webhooks.SignaturePrefix)
		if !strings.HasPrefix(signature, webhooks.SignaturePrefix) || len(raw) != sha256.Size*2 {
			t.Fatalf("Sign = %q, want sha256= and %d hex characters", signature, sha256.Size*2)
		}
		if _, err := hex.DecodeString(raw); err != nil || raw != strings.ToLower(raw) {
			t.Fatalf("signature %q is not lower-case hex", signature)
		}
		if again := webhooks.Sign(secret, body); again != signature {
			t.Fatalf("signing is not deterministic: %q then %q", signature, again)
		}
		if !webhooks.Verify(secret, body, signature) || !webhooks.Verify(secret, body, raw) {
			t.Fatalf("the signature %q of %q did not verify, prefixed or bare", signature, body)
		}
		if webhooks.Verify(secret, body, "") {
			t.Fatalf("an empty signature verified")
		}
		if !bytes.Equal(body, otherBody) && webhooks.Verify(secret, otherBody, signature) {
			t.Fatalf("a signature verified a different body")
		}
		// HMAC zero-pads a key shorter than its block, so two secrets that
		// differ only in trailing NUL bytes are one key: a property of the
		// primitive, not something a receiver can tell apart.
		sameKey := len(secret) <= 64 && len(otherSecret) <= 64 &&
			strings.TrimRight(secret, "\x00") == strings.TrimRight(otherSecret, "\x00")
		if secret != otherSecret && !sameKey && webhooks.Verify(otherSecret, body, signature) {
			t.Fatalf("a signature verified under a different secret")
		}
		// The text accepted is the signature, optionally without its prefix,
		// and nothing else: any other text verifies only if it is one of them.
		if junk != signature && junk != raw && webhooks.Verify(secret, body, junk) {
			t.Fatalf("Verify accepted %q, which is neither %q nor %q", junk, signature, raw)
		}
	})
}

// FuzzDeliveryBody: the signed body is one JSON document in the contract's
// field order, carrying the delivery's coordinates, the timestamp as RFC 3339
// in UTC, task fields only when set, and the run's JSON unchanged in meaning;
// the same delivery always renders the same bytes (a retry's signature is
// the first attempt's), and a run that is not JSON is an error, not a body.
func FuzzDeliveryBody(f *testing.F) {
	run := `{"id":"ork-1755302400123456789-1","status":"SUCCEEDED","note":"<a & b>"}`
	f.Add("ork-1-1", "hello-ork", "SUCCEEDED", "", int32(0), int64(1755302400), int32(0), run)
	f.Add("ork-1-1", "hello-ork", "FAILED", "extract", int32(2), int64(1755302400), int32(123456789), `{}`)
	f.Add("", "", "", "", int32(0), int64(0), int32(0), `null`)
	f.Add("r", "o", "s", "t", int32(-1), int64(-1), int32(5), `[1,2,{"a":[]}]`)
	f.Add("r\xff", "o\u2028", "s", "t\x00", int32(0), int64(253402300799), int32(999999999), `1e400`)
	f.Add("r", "o", "s", "", int32(0), int64(1), int32(0), `{"a":1`)
	f.Add("r", "o", "s", "", int32(0), int64(1), int32(0), "")
	f.Fuzz(func(t *testing.T, runID, orkflow, status, task string, attempt int32, sec int64, nanos int32, runJSON string) {
		if nanos < 0 || nanos > 999_999_999 || sec < 0 || sec > 253402300799 {
			return // an instant RFC 3339 spells with four digits
		}
		delivery := webhooks.Delivery{
			ID: "dlv-1", Event: webhooks.EventRunSucceeded, RunID: runID, OrkflowName: orkflow, Status: status,
			Timestamp: time.Unix(sec, int64(nanos)), TaskID: task, Attempt: attempt,
		}
		if runJSON != "" {
			delivery.Run = json.RawMessage(runJSON)
		}
		body, err := delivery.Body()
		if runJSON != "" && !json.Valid([]byte(runJSON)) {
			if err == nil {
				t.Fatalf("Body accepted a run that is not JSON: %q", runJSON)
			}
			return
		}
		if err != nil {
			t.Fatalf("Body(run %q): %v", runJSON, err)
		}
		if again, _ := delivery.Body(); !bytes.Equal(again, body) {
			t.Fatalf("Body is not deterministic: %s then %s", body, again)
		}
		if !json.Valid(body) {
			t.Fatalf("Body is not JSON: %s", body)
		}

		// Field order: runId, orkflowName, status, timestamp, [taskId], [attempt], run.
		decoder := json.NewDecoder(bytes.NewReader(body))
		if _, err := decoder.Token(); err != nil {
			t.Fatal(err)
		}
		var keys []string
		fields := map[string]json.RawMessage{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				t.Fatal(err)
			}
			var value json.RawMessage
			if err := decoder.Decode(&value); err != nil {
				t.Fatal(err)
			}
			keys = append(keys, keyToken.(string))
			fields[keyToken.(string)] = value
		}
		wantKeys := []string{"runId", "orkflowName", "status", "timestamp"}
		if task != "" {
			wantKeys = append(wantKeys, "taskId")
		}
		if attempt != 0 {
			wantKeys = append(wantKeys, "attempt")
		}
		wantKeys = append(wantKeys, "run")
		if !reflect.DeepEqual(keys, wantKeys) {
			t.Fatalf("body fields %v, want %v: %s", keys, wantKeys, body)
		}

		for key, want := range map[string]string{"runId": runID, "orkflowName": orkflow, "status": status, "taskId": task} {
			raw, present := fields[key]
			if !present || !utf8.ValidString(want) {
				continue
			}
			var got string
			if err := json.Unmarshal(raw, &got); err != nil || got != want {
				t.Fatalf("%s = %s, want %q", key, raw, want)
			}
		}
		var stamp string
		if err := json.Unmarshal(fields["timestamp"], &stamp); err != nil {
			t.Fatal(err)
		}
		parsed, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil || !parsed.Equal(time.Unix(sec, int64(nanos))) || parsed.Location() != time.UTC {
			t.Fatalf("timestamp %q: %v, want the UTC instant %d.%09d", stamp, err, sec, nanos)
		}
		if raw, present := fields["attempt"]; present && string(raw) != strconv.Itoa(int(attempt)) {
			t.Fatalf("attempt = %s, want %d", raw, attempt)
		}
		wantRun := runJSON
		if wantRun == "" {
			wantRun = "null"
		}
		if !sameJSON(t, wantRun, string(fields["run"])) {
			t.Fatalf("run = %s, want the run %s unchanged in meaning", fields["run"], wantRun)
		}
	})
}

// sameJSON reports whether two JSON texts decode to the same value, numbers
// compared by their text.
func sameJSON(t *testing.T, a, b string) bool {
	t.Helper()
	decode := func(text string) any {
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			t.Fatalf("decode %q: %v", text, err)
		}
		return value
	}
	return reflect.DeepEqual(decode(a), decode(b))
}

// FuzzDispatcher_WhatTheReceiverReadsVerifiesAndIsWhatTheResultRecords sends
// real deliveries to a real receiver: the body it reads is the body Body
// renders, its signature verifies under the subscription's secret (and is
// absent for a subscription with none), the headers carry the delivery's
// coordinates, and the result's digest and length are those of the bytes it
// read.
func FuzzDispatcher_WhatTheReceiverReadsVerifiesAndIsWhatTheResultRecords(f *testing.F) {
	var mu sync.Mutex
	var gotBody []byte
	var gotHeader http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotBody, gotHeader = body, r.Header.Clone()
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	f.Cleanup(server.Close)
	dispatcher := webhooks.NewDispatcher(webhooks.DispatcherOptions{MaxRetries: -1})

	f.Add("s3cr3t", "wh-0123456789abcdef", "dlv-1", "ork-1-1", "", "redeliver-1", "hello-ork", "FAILED", int32(0), `{"id":"x"}`)
	f.Add("", "wh-1", "dlv-2", "ork-2-2", "extract", "", "o", "s", int32(3), `{"a":[1,2]}`)
	f.Add("k", "w", "d", "r", "t", "x", "\u00e9", "\u00fc", int32(-1), `"text"`)
	f.Fuzz(func(t *testing.T, secret, hookID, deliveryID, runID, task, redelivery, orkflow, status string, attempt int32, runJSON string) {
		for _, header := range []string{hookID, deliveryID, runID, task, redelivery} {
			for i := 0; i < len(header); i++ {
				if c := header[i]; c < 0x20 || c == 0x7f || c >= 0x80 {
					return // not a header value a sender can put on the wire
				}
			}
			if header != strings.TrimSpace(header) {
				return // the receiver's reader trims the optional whitespace around a value
			}
		}
		if !json.Valid([]byte(runJSON)) {
			return
		}
		delivery := webhooks.Delivery{
			ID: deliveryID, Event: webhooks.EventTaskFailed, RunID: runID, OrkflowName: orkflow, Status: status,
			Timestamp: time.Unix(1755302400, 5).UTC(), TaskID: task, Attempt: attempt, Run: json.RawMessage(runJSON),
			RedeliveryOf: redelivery,
		}
		hook := webhooks.Webhook{ID: hookID, URL: server.URL, Secret: secret}
		result, err := dispatcher.Deliver(context.Background(), hook, delivery)
		if err != nil || !result.Delivered || len(result.Attempts) != 1 || result.LastAttempt().StatusCode != http.StatusNoContent {
			t.Fatalf("Deliver = %+v, %v", result, err)
		}
		want, err := delivery.Body()
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		body, header := gotBody, gotHeader
		mu.Unlock()
		if !bytes.Equal(body, want) {
			t.Fatalf("the receiver read %s, Body renders %s", body, want)
		}
		signature := header.Get(webhooks.SignatureHeader)
		switch {
		case secret == "" && signature != "":
			t.Fatalf("an unsigned subscription sent signature %q", signature)
		case secret != "" && !webhooks.Verify(secret, body, signature):
			t.Fatalf("the receiver could not verify %q over %s", signature, body)
		case result.Signature != signature:
			t.Fatalf("result records signature %q, the receiver read %q", result.Signature, signature)
		}
		digest := sha256.Sum256(body)
		if result.BodySHA256 != hex.EncodeToString(digest[:]) || result.BodyBytes != len(body) {
			t.Fatalf("result records %s over %d bytes, the receiver read %x over %d", result.BodySHA256, result.BodyBytes, digest, len(body))
		}
		for name, wantValue := range map[string]string{
			webhooks.EventHeader: string(webhooks.EventTaskFailed), webhooks.DeliveryHeader: deliveryID, webhooks.WebhookHeader: hookID,
			webhooks.RunHeader: runID, webhooks.TaskHeader: task, webhooks.RedeliveryHeader: redelivery,
			webhooks.AttemptHeader: "1", webhooks.TimestampHeader: "2025-08-16T00:00:00.000000005Z",
		} {
			if got := header.Get(name); got != wantValue {
				t.Fatalf("header %s = %q, want %q", name, got, wantValue)
			}
			// The task and redelivery headers are sent only when there is a
			// task or a redelivery; every other header is always sent.
			optional := name == webhooks.TaskHeader || name == webhooks.RedeliveryHeader
			if _, present := header[http.CanonicalHeaderKey(name)]; present != (!optional || wantValue != "") {
				t.Fatalf("header %s present=%v for value %q", name, present, wantValue)
			}
		}
	})
}

// FuzzValidateURL: an accepted URL is what the caller wrote without its
// surrounding whitespace, is a fixed point of the validation, and is one a
// delivery can build a request for; a refused one says why.
func FuzzValidateURL(f *testing.F) {
	for _, seed := range []string{
		"https://example.test/hooks", " http://example.test ", "", "   ", "example.test/hooks", "ftp://example.test", "https://",
		"http://:80/", "http://user:pass@host/x?y=z#frag", "HTTPS://EXAMPLE.TEST", "http://[::1]:8080/", "http://[::1", "http://exa mple.test",
		"http://host/\x00", "https://host/\n", "http:///path", "//host/x", "https:host", "http://%zz/", "http://host:99999999/",
		"\u00a0https://host/\u00a0", "http://\xff/",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		got, err := webhooks.ValidateURL(raw)
		if err != nil {
			if got != "" || !strings.HasPrefix(err.Error(), "webhooks: ") {
				t.Fatalf("ValidateURL(%q) = %q, %v", raw, got, err)
			}
			return
		}
		if got != strings.TrimSpace(raw) || got == "" {
			t.Fatalf("ValidateURL(%q) = %q, want the text without its surrounding space", raw, got)
		}
		again, err := webhooks.ValidateURL(got)
		if err != nil || again != got {
			t.Fatalf("ValidateURL(%q) = %q is not a fixed point: %q, %v", raw, got, again, err)
		}
		parsed, err := url.Parse(got)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			t.Fatalf("ValidateURL(%q) accepted %q, which is not an absolute http(s) URL: %v", raw, got, err)
		}
		if _, err := http.NewRequestWithContext(context.Background(), http.MethodPost, got, nil); err != nil {
			t.Fatalf("ValidateURL(%q) accepted %q, which no delivery can build a request for: %v", raw, got, err)
		}
	})
}

// FuzzParseEvent: an accepted spelling is one of the subscribable events,
// parsing it again gives the same event, the documented variants (case, the
// proto prefix) read as the same event, and no spelling outside ASCII does.
func FuzzParseEvent(f *testing.F) {
	for _, seed := range []string{
		"RUN_SUCCEEDED", "run_succeeded", "EVENT_TYPE_RUN_SUCCEEDED", " event_type_run_succeeded ", "TASK_LOG", "", "EVENT_TYPE_",
		"RUN-SUCCEEDED", "run_\u017fucceeded", "RUN_STARTED\u00a0", "TASK_AWAITING", "event_type_event_type_run_failed",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		event, err := webhooks.ParseEvent(raw)
		if err != nil {
			if event != "" || !strings.Contains(err.Error(), "RUN_SUCCEEDED") {
				t.Fatalf("ParseEvent(%q) = %q, %v; a refusal names the valid events", raw, event, err)
			}
			return
		}
		if !contains(webhooks.KnownEvents(), event) {
			t.Fatalf("ParseEvent(%q) = %q, which is not a subscribable event", raw, event)
		}
		for _, again := range []string{string(event), "EVENT_TYPE_" + string(event), strings.ToLower(string(event))} {
			if got, err := webhooks.ParseEvent(again); err != nil || got != event {
				t.Fatalf("ParseEvent(%q) = %q, %v; want %q", again, got, err, event)
			}
		}
		for i := 0; i < len(strings.TrimSpace(raw)); i++ {
			if strings.TrimSpace(raw)[i] >= utf8.RuneSelf {
				t.Fatalf("ParseEvent(%q) = %q: a spelling outside ASCII is not an event name", raw, event)
			}
		}
	})
}

func contains(events []webhooks.Event, event webhooks.Event) bool {
	for _, known := range events {
		if known == event {
			return true
		}
	}
	return false
}

// slugRule is AGENTS.md §7's name rule as a byte loop.
func slugRule(s string) bool {
	if s == "" || len(s) > 200 || s[0] < 'a' || s[0] > 'z' {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// FuzzScopeAndName: a scope field or a name is accepted exactly when it is
// empty (where empty means every) or a slug, a name additionally needs its
// namespace, and a task scope needs a task event to be consulted at all.
func FuzzScopeAndName(f *testing.F) {
	f.Add("analytics", "nightly-etl", "extract", "ci", uint8(1))
	f.Add("", "", "", "", uint8(0))
	f.Add("Analytics", "-x", "a b", "N", uint8(2))
	f.Add("a", "b", "c", "d", uint8(3))
	f.Add(strings.Repeat("a", 201), "", "", strings.Repeat("a", 201), uint8(1))
	f.Fuzz(func(t *testing.T, namespace, orkflow, task, name string, kind uint8) {
		events := [][]webhooks.Event{nil, {webhooks.EventRunFailed}, {webhooks.EventTaskSucceeded}, {webhooks.EventRunFailed, webhooks.EventTaskFailed}}[kind%4]
		fieldsOK := (namespace == "" || slugRule(namespace)) && (orkflow == "" || slugRule(orkflow)) && (task == "" || slugRule(task))
		hasTaskEvent := false
		for _, event := range events {
			hasTaskEvent = hasTaskEvent || event == webhooks.EventTaskSucceeded || event == webhooks.EventTaskFailed
		}
		want := fieldsOK && (task == "" || hasTaskEvent)
		if err := webhooks.ValidateScope(namespace, orkflow, task, events); (err == nil) != want {
			t.Fatalf("ValidateScope(%q, %q, %q, %v) = %v, want accepted=%v", namespace, orkflow, task, events, err, want)
		}
		wantName := (name == "" || slugRule(name)) && (name == "" || namespace != "")
		if err := webhooks.ValidateName(name, namespace); (err == nil) != wantName {
			t.Fatalf("ValidateName(%q, %q) = %v, want accepted=%v", name, namespace, err, wantName)
		}
	})
}
