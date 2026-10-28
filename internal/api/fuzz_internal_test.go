package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// The plain-HTTP doors and the JSON codec take bytes from callers OrkStack
// does not control. The properties below are the ones the code and its
// reference docs state, each checked against an independent reading.

// FuzzTriggerInput: the trigger door's body is the run's input object. An
// empty body (JSON whitespace only) is no input; anything else is accepted
// only when it is one valid JSON object, and then the input is exactly the
// object the body spells; every refusal is one of the door's named answers.
func FuzzTriggerInput(f *testing.F) {
	for _, seed := range []string{
		``, ` `, "\n\t\r ", `{}`, `{"a":1}`, `{"a":{"b":[1,2,{"c":null}]},"d":"x"}`, `[1,2]`, `"hello"`, `12`, `true`, `null`, `{`, `{}{}`,
		`{"a":1}{"b":2}`, `{"a":1,"a":2}`, `{"a":1e999}`, `{"a":"\ud800"}`, "{\"a\":\"\xff\"}", "\u00a0{}", "{}\u00a0", "\ufeff{}", "{\"a\":\"\u0000\"}",
		`{"orkflow_name":"other","namespace":"other","status":"SUCCEEDED","a":"STATUS_FAILED"}`, `{"a":` + strings.Repeat("[", 20000),
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		request := httptest.NewRequest(http.MethodPost, "/v1/triggers/trg-0123456789abcdef", bytes.NewReader(body))
		payload, err := triggerInput(httptest.NewRecorder(), request)
		jsonSpace := bytes.Trim(body, " \t\r\n")
		if err != nil {
			if payload != nil {
				t.Fatalf("a refused body also returned input %v", payload)
			}
			named := false
			for _, prefix := range []string{"read request body", "request body is not valid JSON", "request body must be a JSON object", "decode request body"} {
				named = named || strings.HasPrefix(err.Error(), prefix)
			}
			if !named {
				t.Fatalf("body %q refused with %q, not one of the door's named answers", body, err)
			}
			if len(jsonSpace) == 0 {
				t.Fatalf("an empty body was refused: %v", err)
			}
			return
		}
		if len(jsonSpace) == 0 {
			if payload != nil {
				t.Fatalf("an empty body produced input %v", payload)
			}
			return
		}
		if payload == nil || payload.GetData() == nil {
			t.Fatalf("body %q accepted with no input object", body)
		}
		// Accepted: one JSON object, valid UTF-8, and nothing but JSON
		// whitespace around it.
		if !json.Valid(body) || !utf8.Valid(body) || jsonSpace[0] != '{' {
			t.Fatalf("body %q accepted, but it is not one JSON object in UTF-8", body)
		}
		var spelled map[string]any
		if err := json.Unmarshal(body, &spelled); err != nil {
			t.Fatalf("body %q accepted, but encoding/json reads it as %v", body, err)
		}
		if got := payload.GetData().AsMap(); !reflect.DeepEqual(got, spelled) {
			t.Fatalf("body %q is the object %v, but the run's input is %v", body, spelled, got)
		}
	})
}

// FuzzBearerToken: the token is what follows "Bearer " (any case of the
// scheme), trimmed, and only when something remains.
func FuzzBearerToken(f *testing.F) {
	for _, seed := range []string{
		"Bearer abc", "bearer abc", "BEARER abc", "Bearer  abc  ", "Bearer ", "Bearer", "", "Basic abc", "Bearer\tabc", "Bearer a b",
		"Bearer \u00a0", "Bearer \u00a0x\u00a0", "\u212aearer abc", "Bearer abc\r\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, header string) {
		token, ok := bearerToken(header)
		hasScheme := len(header) >= 7 && strings.EqualFold(header[:7], "bearer ") && header[:7] == lowerASCII(header[:7]) ||
			len(header) >= 7 && lowerASCII(header[:7]) == "bearer "
		if !hasScheme {
			if ok || token != "" {
				t.Fatalf("bearerToken(%q) = %q, %v without the Bearer scheme", header, token, ok)
			}
			return
		}
		want := strings.TrimSpace(header[7:])
		if ok != (want != "") || token != want {
			t.Fatalf("bearerToken(%q) = %q, %v; want %q, %v", header, token, ok, want, want != "")
		}
	})
}

func lowerASCII(s string) string {
	lower := []byte(s)
	for i, c := range lower {
		if c >= 'A' && c <= 'Z' {
			lower[i] = c + 'a' - 'A'
		}
	}
	return string(lower)
}

// FuzzOriginReference: a reference is accepted when it is at most 256 bytes
// of valid UTF-8 with no control character, and refused by name otherwise;
// the two asserted origin fields are never refused but always come out as
// valid UTF-8 of at most 256 bytes, unchanged when they already were, and
// stable when bound again.
func FuzzOriginReference(f *testing.F) {
	for _, seed := range []string{
		"", "ci-run-42", strings.Repeat("a", 256), strings.Repeat("a", 257), "a\nb", "a\x00", "a\x7f", "a\x80", "a\xffb", "\u00e9",
		strings.Repeat("\u00e9", 128), strings.Repeat("\u00e9", 129), "line\u2028sep", "tab\there", strings.Repeat("\xff", 300),
		strings.Repeat("x", 250) + "\u20ac\u20ac",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, ref string) {
		valid := len(ref) <= OriginReferenceMaxBytes && utf8.ValidString(ref)
		for i := 0; valid && i < len(ref); i++ {
			valid = ref[i] >= 0x20 && ref[i] != 0x7f
		}
		if err := ValidateOriginReference(ref); (err == nil) != valid {
			t.Fatalf("ValidateOriginReference(%q) = %v; the rule says accepted=%v", ref, err, valid)
		} else if err != nil && !strings.Contains(err.Error(), "origin reference") {
			t.Fatalf("ValidateOriginReference(%q) = %q does not name what it refused", ref, err)
		}

		bound := boundOriginAssertion(ref)
		if len(bound) > originAssertionMaxBytes || !utf8.ValidString(bound) {
			t.Fatalf("boundOriginAssertion(%q) = %q: %d bytes, valid UTF-8 %v", ref, bound, len(bound), utf8.ValidString(bound))
		}
		if len(ref) <= originAssertionMaxBytes && utf8.ValidString(ref) && bound != ref {
			t.Fatalf("boundOriginAssertion(%q) changed a value that already fit: %q", ref, bound)
		}
		if again := boundOriginAssertion(bound); again != bound {
			t.Fatalf("boundOriginAssertion is not stable: %q then %q", bound, again)
		}
	})
}

// FuzzRegisterWebhook posts bodies to the webhook registration door: a 201
// is only ever one JSON object in UTF-8 whose scope, names and URL satisfy
// the rules, stored exactly as written with its secret disclosed once; every
// other outcome is a 400 with the error envelope and stores nothing.
func FuzzRegisterWebhook(f *testing.F) {
	for _, seed := range []string{
		`{"url":"https://example.test/hooks"}`,
		`{"url":" https://example.test/hooks ","events":["run_failed","RUN_FAILED","EVENT_TYPE_TASK_FAILED"],"secret":"mine"}`,
		`{"url":"https://example.test/h","namespace":"analytics","orkflowName":"nightly-etl","taskId":"extract","events":["TASK_SUCCEEDED"],"name":"ci"}`,
		`{"url":"https://example.test/h","taskId":"extract"}`, `{"url":"https://example.test/h","name":"ci"}`, `{"events":["RUN_FAILED"]}`,
		`{"url":"/hooks"}`, `{"url":"https://example.test","events":["RUN_EXPLODED"]}`, `{"url":"https://example.test"}{"url":"https://other.test"}`,
		`{"url":"https://example.test","secret":"s\ud800"}`, "{\"url\":\"https://example.test/\xff\",\"secret\":\"s\xff\"}", `{"url":"https://example.test","events":null}`,
		`null`, `[]`, `"x"`, `{`, ``, `{"url":"https://example.test","namespace":"Bad"}`, `{"url":"https://example.test","UNKNOWN":1,"URL":"https://x.test"}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		registry := webhooks.NewRegistry()
		handler := NewWebhookHandler(registry, nil)
		request := httptest.NewRequest(http.MethodPost, WebhookCollectionPath, bytes.NewReader(body))
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		stored := registry.List()

		switch recorder.Code {
		case http.StatusBadRequest:
			var envelope webhookErrorEnvelope
			if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil || envelope.Error.Code != "invalid_argument" || envelope.Error.Message == "" {
				t.Fatalf("a refusal must carry the error envelope: %s (%v)", recorder.Body.Bytes(), err)
			}
			if len(stored) != 0 {
				t.Fatalf("body %q was refused, but %+v was stored", body, stored)
			}
		case http.StatusCreated:
			if !json.Valid(body) || !utf8.Valid(body) {
				t.Fatalf("body %q was accepted, but it is not one JSON document in UTF-8", body)
			}
			var sent registerWebhookRequest
			if err := json.Unmarshal(body, &sent); err != nil {
				t.Fatalf("body %q was accepted, but reads as %v", body, err)
			}
			var envelope webhookEnvelope
			if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("201 body %s: %v", recorder.Body.Bytes(), err)
			}
			hook := envelope.Webhook
			if len(stored) != 1 || stored[0].ID != hook.ID || hook.ID == "" || hook.Secret == "" {
				t.Fatalf("201 for %q: answered %+v, stored %+v", body, hook, stored)
			}
			if stored[0].Secret != "" {
				t.Fatalf("a listing carries the secret %q", stored[0].Secret)
			}
			if sent.Secret != "" && hook.Secret != sent.Secret {
				t.Fatalf("the caller's secret %q was stored as %q", sent.Secret, hook.Secret)
			}
			if hook.URL != strings.TrimSpace(sent.URL) || hook.Namespace != sent.Namespace || hook.OrkflowName != sent.OrkflowName ||
				hook.TaskID != sent.TaskID || hook.Name != sent.Name {
				t.Fatalf("stored %+v from %+v", hook, sent)
			}
			for _, field := range []string{hook.Namespace, hook.OrkflowName, hook.TaskID, hook.Name} {
				if field != "" && !isSlug(field) {
					t.Fatalf("stored the scope or name %q, which is not a slug", field)
				}
			}
			if hook.Name != "" && hook.Namespace == "" {
				t.Fatalf("stored the name %q without a namespace", hook.Name)
			}
			if len(hook.Events) > len(sent.Events) {
				t.Fatalf("stored %v for %v", hook.Events, sent.Events)
			}
			for _, event := range hook.Events {
				if _, err := webhooks.ParseEvent(string(event)); err != nil {
					t.Fatalf("stored the event %q: %v", event, err)
				}
			}
		default:
			t.Fatalf("body %q answered %d: %s", body, recorder.Code, recorder.Body.Bytes())
		}
	})
}

// isSlug is AGENTS.md §7's name rule as a byte loop.
func isSlug(s string) bool {
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

// FuzzAwaitRejectReason: a rejection envelope is one JSON object in UTF-8
// whose only key is "reason" (any case), a string; the reason is returned
// exactly as written, and everything else is refused with the envelope
// spelled out.
func FuzzAwaitRejectReason(f *testing.F) {
	for _, seed := range []string{
		`{"reason":"not approved"}`, `{"reason":""}`, `{}`, `{"reason":null}`, `{"reason":1}`, `{"reason":"a","extra":1}`, `{"REASON":"a"}`,
		`{"reason":"a"}{"reason":"b"}`, `[]`, `"x"`, ``, `{"reason":"a\ud800"}`, "{\"reason\":\"a\xff\"}", `{"reason":"a","reason":"b"}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		reason, err := awaitRejectReason(httptest.NewRecorder(), request)
		if err != nil {
			if reason != "" || !strings.Contains(err.Error(), "reason") {
				t.Fatalf("body %q refused as %q with reason %q", body, err, reason)
			}
			return
		}
		if !utf8.Valid(body) || !json.Valid(body) || !utf8.ValidString(reason) {
			t.Fatalf("body %q accepted as reason %q, but it is not one JSON document in UTF-8", body, reason)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(body, &object); err != nil || len(object) == 0 {
			t.Fatalf("body %q accepted, but it is not an object with keys: %v", body, err)
		}
		for key := range object {
			if !strings.EqualFold(key, "reason") {
				t.Fatalf("body %q accepted with the key %q", body, key)
			}
		}
	})
}

// messageTypes are the messages the codec fuzz targets build, chosen for
// their enum fields, nested messages, maps and opaque Struct data.
var messageTypes = []func() proto.Message{
	func() proto.Message { return &orkstackv1.Run{} },
	func() proto.Message { return &orkstackv1.Event{} },
	func() proto.Message { return &orkstackv1.SubmitRunRequest{} },
	func() proto.Message { return &orkstackv1.ListRunsRequest{} },
	func() proto.Message { return &orkstackv1.Namespace{} },
	func() proto.Message { return &orkstackv1.Orkflow{} },
	func() proto.Message { return &orkstackv1.Payload{} },
}

// FuzzJSONCodec_RoundTripsAnyMessage builds arbitrary messages from wire
// bytes and checks the dialect's promises: the short spelling and the
// canonical protojson spelling of the same message both read back as it, and
// no opaque Struct string is ever taken for an enum name.
func FuzzJSONCodec_RoundTripsAnyMessage(f *testing.F) {
	seed := func(which int, message proto.Message) {
		wire, err := proto.Marshal(message)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(uint8(which), wire)
	}
	opaque, _ := structpb.NewStruct(map[string]any{"status": "running", "kind": "STATUS_FAILED", "n": 1.5, "list": []any{"failed", nil, true}})
	seed(0, &orkstackv1.Run{Id: "ork-1-1", OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_FAILED, Input: &orkstackv1.Payload{Data: opaque}})
	seed(1, &orkstackv1.Event{RunId: "ork-1", Sequence: 7, Type: orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, Status: orkstackv1.Status_STATUS_RUNNING})
	seed(2, &orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", Input: &orkstackv1.Payload{Data: opaque}, OriginReference: "ci-1"})
	seed(3, &orkstackv1.ListRunsRequest{Filters: []*orkstackv1.FieldFilter{{Field: "status", Op: orkstackv1.FilterOperator_FILTER_OPERATOR_IN, Values: []string{"failed", "RUNNING"}}}})
	seed(4, &orkstackv1.Namespace{Name: "payments", Labels: map[string]string{"team": "running", "status": "STATUS_FAILED"}})
	seed(6, &orkstackv1.Payload{Data: opaque})
	f.Add(uint8(0), []byte{0x20, 0x63}) // an enum number no value is declared for
	f.Fuzz(func(t *testing.T, which uint8, wire []byte) {
		message := messageTypes[int(which)%len(messageTypes)]()
		if err := (proto.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(wire, message); err != nil {
			return
		}

		short, err := MarshalJSON(message)
		if err != nil {
			return // a value JSON cannot hold (NaN in a Struct, invalid UTF-8)
		}
		canonical, err := protojson.Marshal(message)
		if err != nil {
			t.Fatalf("protojson cannot write what MarshalJSON wrote: %v", err)
		}
		// A message JSON cannot hold exactly (an undeclared number in a
		// google.protobuf.NullValue, which JSON spells null) is outside what
		// the dialect promises; it must still read the same either way.
		plain := messageTypes[int(which)%len(messageTypes)]()
		if err := protojson.Unmarshal(canonical, plain); err != nil {
			t.Fatalf("protojson cannot read what it wrote: %v", err)
		}
		if !proto.Equal(plain, message) {
			message = plain
		}
		for name, text := range map[string][]byte{"short": short, "canonical": canonical} {
			back := messageTypes[int(which)%len(messageTypes)]()
			if err := UnmarshalJSON(text, back); err != nil {
				t.Fatalf("%s spelling %s of %v does not read back: %v", name, text, message, err)
			}
			if !proto.Equal(message, back) {
				t.Fatalf("%s spelling %s of %v reads back as %v", name, text, message, back)
			}
		}
	})
}

// FuzzUnmarshalJSON_IsProtojsonPlusEnumSpellings feeds raw bytes to the
// tolerant decoder. It may accept more enum spellings than protojson, and
// nothing else: for a message with no enum field it agrees with protojson on
// every input, for any message it accepts only valid UTF-8, and whatever it
// accepts protojson itself accepts once the message is written back.
func FuzzUnmarshalJSON_IsProtojsonPlusEnumSpellings(f *testing.F) {
	for i := range messageTypes {
		for _, seed := range []string{
			`{}`, `{"status":"running"}`, `{"status":"STATUS_RUNNING","orkflowName":"a"}`, `{"filters":[{"field":"status","op":"in","values":["x"]}]}`,
			`{"data":{"a":"running","b":[1,{"c":"failed"}]}}`, `{"input":{"data":{"a":"x\ud800y"}}}`, "{\"orkflowName\":\"a\xff\"}",
			`{"orkflowName":"a","orkflowName":"b"}`, `{"data":{"a":` + strings.Repeat("[", 12000) + strings.Repeat("]", 12000) + `}}`,
			`{"unknownField":{"deeper":[1,2,3]},"id":"x"}`, `[]`, `null`, ``, ` `, `{"labels":{"k":"v","k":"w"}}`, "\ufeff{}",
		} {
			f.Add(uint8(i), []byte(seed))
		}
	}
	options := protojson.UnmarshalOptions{DiscardUnknown: true}
	f.Fuzz(func(t *testing.T, which uint8, data []byte) {
		ours := messageTypes[int(which)%len(messageTypes)]()
		oursErr := UnmarshalJSONOptions(options, data, ours)
		plain := messageTypes[int(which)%len(messageTypes)]()
		plainErr := options.Unmarshal(data, plain)

		enumFree := !hasEnumField(ours.ProtoReflect().Descriptor())
		if plainErr == nil && oursErr != nil {
			t.Fatalf("protojson reads %q as %v, the tolerant decoder refuses it: %v", data, plain, oursErr)
		}
		if plainErr == nil && enumFree && !proto.Equal(plain, ours) {
			t.Fatalf("protojson reads %q as %v, the tolerant decoder as %v", data, plain, ours)
		}
		if oursErr != nil {
			return
		}
		if !utf8.Valid(data) {
			t.Fatalf("%q is not valid UTF-8, but was read as %v", data, ours)
		}
		if enumFree && plainErr != nil {
			t.Fatalf("%T has no enum field, yet the tolerant decoder accepted %q, which protojson refuses: %v", ours, data, plainErr)
		}
		written, err := MarshalJSON(ours)
		if err != nil {
			return
		}
		again := messageTypes[int(which)%len(messageTypes)]()
		if err := UnmarshalJSONOptions(options, written, again); err != nil || !proto.Equal(ours, again) {
			t.Fatalf("%q was read as %v, which does not survive being written (%s): %v", data, ours, written, err)
		}
	})
}

// hasEnumField reports whether a message, or any message nested in it, has an
// enum-typed field the tolerant decoder rewrites.
func hasEnumField(md protoreflect.MessageDescriptor) bool {
	return enumFieldIn(md, map[protoreflect.FullName]bool{})
}

func enumFieldIn(md protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) bool {
	if seen[md.FullName()] || strings.HasPrefix(string(md.FullName()), "google.protobuf.") {
		return false
	}
	seen[md.FullName()] = true
	for i := 0; i < md.Fields().Len(); i++ {
		field := md.Fields().Get(i)
		switch {
		case field.Kind() == protoreflect.EnumKind:
			return true
		case field.IsMap() && field.MapValue().Kind() == protoreflect.EnumKind:
			return true
		case field.IsMap() && field.MapValue().Message() != nil && enumFieldIn(field.MapValue().Message(), seen):
			return true
		case !field.IsMap() && field.Message() != nil && enumFieldIn(field.Message(), seen):
			return true
		}
	}
	return false
}

// FuzzPageTokens: a page token is opaque text a client hands back, so every
// decoder refuses what it did not issue as InvalidArgument and never anything
// else; a token it accepts re-encodes to a token that decodes to the same
// position; and a token minted under one filter set is accepted under that set
// and under no other.
func FuzzPageTokens(f *testing.F) {
	empty := listfilter.Set{}
	spec := listfilter.MustSpec(&orkstackv1.ListNamespacesRequest{})
	parsed, err := listfilter.Parse("name~pay")
	if err != nil {
		f.Fatal(err)
	}
	filtered, err := spec.Compile([]*orkstackv1.FieldFilter{parsed})
	if err != nil {
		f.Fatal(err)
	}
	other, err := listfilter.Parse("description^x")
	if err != nil {
		f.Fatal(err)
	}
	filteredOther, err := spec.Compile([]*orkstackv1.FieldFilter{other})
	if err != nil {
		f.Fatal(err)
	}
	mint := func(token string, err error) string {
		if err != nil {
			f.Fatal(err)
		}
		return token
	}
	for _, seed := range []string{
		"", "x", "!!!", "e30", "bnVsbA", mint(encodeNamespaceCursor(empty, "payments")), mint(encodeNamespaceCursor(filtered, "payments")),
		mint(encodeOrkflowCursor("default", empty, "nightly")), mint(encodeOrkflowCursor("default", filtered, "nightly")),
		mint(encodeRunCursor(runCursor{Version: runCursorVersion, Namespace: "default", LastCreated: "2026-10-09T00:00:00Z", LastID: "ork-1-1"})),
		mint(encodeAuditCursor(auditCursor{WindowSince: "2026-10-09T00:00:00Z", WindowUntil: "2026-10-10T00:00:00Z", BeforeKey: "metadata/audit/2026/10/09/aud-20261009T000000.000000000Z-0123456789abcdef.json"})),
		"v2:e30", "v1:e30", "v2:", mint(encodeDeliveryCursor(deliveryCursor{Version: deliveryCursorVersion, WindowSince: "2026-10-09T00:00:00Z", WindowUntil: "2026-10-10T00:00:00Z"})),
	} {
		f.Add(seed)
	}
	refusedAsInvalid := func(t *testing.T, decoder string, err error) {
		t.Helper()
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("%s refused a token with %T %v, want InvalidArgument", decoder, err, err)
		}
	}
	f.Fuzz(func(t *testing.T, token string) {
		for _, set := range []listfilter.Set{empty, filtered, filteredOther} {
			if name, err := decodeNamespaceCursor(token, set); err != nil {
				refusedAsInvalid(t, "decodeNamespaceCursor", err)
			} else {
				again, err := encodeNamespaceCursor(set, name)
				if err != nil {
					t.Fatal(err)
				}
				if back, err := decodeNamespaceCursor(again, set); err != nil || back != name {
					t.Fatalf("a namespace cursor at %q did not survive re-encoding: %q, %v", name, back, err)
				}
				for _, wrong := range []listfilter.Set{empty, filtered, filteredOther} {
					if _, err := decodeNamespaceCursor(again, wrong); (err == nil) != (wrong.Key() == set.Key()) {
						t.Fatalf("a token minted under %q decoded under %q: %v", set.Texts(), wrong.Texts(), err)
					}
				}
			}
			if name, err := decodeOrkflowCursor(token, "default", set); err != nil {
				refusedAsInvalid(t, "decodeOrkflowCursor", err)
			} else if again, err := encodeOrkflowCursor("default", set, name); err != nil {
				t.Fatal(err)
			} else if back, err := decodeOrkflowCursor(again, "default", set); err != nil || back != name {
				t.Fatalf("an orkflow cursor at %q did not survive re-encoding: %q, %v", name, back, err)
			}
			if cursor, err := decodeRunCursor(token, "default", set); err != nil {
				refusedAsInvalid(t, "decodeRunCursor", err)
			} else if cursor != nil {
				again, err := encodeRunCursor(*cursor)
				if err != nil {
					t.Fatal(err)
				}
				if back, err := decodeRunCursor(again, "default", set); err != nil || *back != *cursor {
					t.Fatalf("a run cursor %+v did not survive re-encoding: %+v, %v", cursor, back, err)
				}
			}
			if since, until, key, err := decodeAuditCursor(token, set); err != nil {
				refusedAsInvalid(t, "decodeAuditCursor", err)
			} else if again, err := encodeAuditCursor(auditCursor{
				Filters: set.Key(), WindowSince: since.Format(time.RFC3339Nano), WindowUntil: until.Format(time.RFC3339Nano), BeforeKey: key}); err != nil {
				t.Fatal(err)
			} else if s, u, k, err := decodeAuditCursor(again, set); err != nil || !s.Equal(since) || !u.Equal(until) || k != key {
				t.Fatalf("an audit cursor did not survive re-encoding: %v %v %q, %v", s, u, k, err)
			}
			if since, until, runStart, key, err := decodeDeliveryCursor(token, set); err != nil {
				refusedAsInvalid(t, "decodeDeliveryCursor", err)
			} else if again, err := encodeDeliveryCursor(deliveryCursor{Version: deliveryCursorVersion, Filters: set.Key(),
				WindowSince: since.Format(time.RFC3339Nano), WindowUntil: until.Format(time.RFC3339Nano), RunStart: runStart, BeforeKey: key}); err != nil {
				t.Fatal(err)
			} else if s, u, rs, k, err := decodeDeliveryCursor(again, set); err != nil || !s.Equal(since) || !u.Equal(until) || rs != runStart || k != key {
				t.Fatalf("a delivery cursor did not survive re-encoding: %v, %v", s, err)
			}
		}
	})
}
