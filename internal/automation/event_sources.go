package automation

import (
	stdjson "encoding/json/v2"
	"errors"
	"fmt"
	"strings"

	"go.yaml.in/yaml/v4"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// The `event_sources` section (docs/AUTOMATION.md, "event_sources"): one
// entry a source, keyed by its name. An entry is the source as a person
// writes it; the engine's checks run when its step is applied.

// eventSourceKeys are an entry's keys, in the order of the docs table. An
// entry has no `namespace` key: every source of a document is in the
// document's namespace.
var eventSourceKeys = []string{
	"orkflow", "version", "description", "uses", "with", "secrets", "identities", "credentials", "env",
	"pool", "instances", "resources", "delivery", "concurrency", "rate", "format", "max_bytes",
	"reject_breaker", "match", "input", "map", "artifacts",
}

// kindEventSource is the section's object kind.
const kindEventSource = orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_EVENT_SOURCE

// EventSourceEntry is one `event_sources` entry: the source it declares,
// in the document's namespace, and the keys the entry wrote.
type EventSourceEntry struct {
	Source *orkstackv1.EventSource
	Keys   []string
}

// eventSourceJSONName is each key's field in the API's JSON, where it
// differs from the key.
var eventSourceJSONName = map[string]string{
	"orkflow": "orkflowName", "version": "orkflowVersion", "max_bytes": "maxBytes", "reject_breaker": "rejectBreaker",
}

// The words a document spells an enum with.
var (
	deliveryWords = map[string]string{"at-least-once": "DELIVERY_CLASS_AT_LEAST_ONCE", "at-most-once": "DELIVERY_CLASS_AT_MOST_ONCE"}
	formatWords   = map[string]string{"bytes": "MESSAGE_FORMAT_BYTES", "json": "MESSAGE_FORMAT_JSON"}
	fieldWords    = map[string]string{"subject": "MESSAGE_FIELD_SUBJECT", "id": "MESSAGE_FIELD_ID", "published_at": "MESSAGE_FIELD_PUBLISHED_AT", "group": "MESSAGE_FIELD_GROUP"}
	partWords     = map[string]string{"body": "MESSAGE_PART_BODY", "metadata": "MESSAGE_PART_METADATA"}
)

// planLimits are the widest bounds a document can be held to without the
// deployment's settings, which the engine applies when the step runs.
var planLimits = eventsources.Limits{MaxMessageBytes: eventsources.MaxMessageBytesCeiling,
	MaxInputBytes: eventsources.MaxInputBytesCeiling, MaxRate: 1 << 20, MaxInstances: 1 << 10}

// eventSource reads one entry. Every value is read through the strict
// mapping (docs/JSON.md): anchors, aliases, merge keys and tags are refused
// anywhere in it, and so is a plain scalar YAML readers resolve
// differently.
func (p *parser) eventSource(namespace, name string, node *yaml.Node) (EventSourceEntry, bool) {
	field := "event_sources." + name
	before := len(p.issues)
	present := map[string]bool{}
	refuseNamespace := func(key *yaml.Node) bool {
		if key.Value != "namespace" {
			return false
		}
		p.errorAt(field+".namespace", key, "an event source entry has no namespace key: every source of the document is in the document's namespace (%s)", namespace)
		return true
	}
	p.mapping(field, node, eventSourceKeys, refuseNamespace, func(key string, _, _ *yaml.Node) { present[key] = true })
	if len(p.issues) > before {
		return EventSourceEntry{}, false
	}
	src, err := EventSourceFromNode(node, field)
	if err != nil {
		p.sourceError(field, node, err)
		return EventSourceEntry{}, false
	}
	src.Namespace, src.Name = namespace, name
	if src.GetOrkflowName() == "" {
		p.errorAt(field+".orkflow", node, "event source %q needs an orkflow: the Orkflow each message starts", name)
	}
	if src.GetUses() == "" {
		p.errorAt(field+".uses", node, "event source %q needs uses: the source image that reads its broker", name)
	}
	if len(p.issues) == before {
		if err := eventsources.Validate(src, planLimits); err != nil {
			var e *eventsources.Error
			if errors.As(err, &e) && e.Field != "" {
				p.errorAt(joinField(field, e.Field), node, "%s", e.Message)
			} else {
				p.errorAt(field, node, "%v", err)
			}
		}
	}
	entry := EventSourceEntry{Source: src}
	for _, key := range eventSourceKeys {
		if present[key] {
			entry.Keys = append(entry.Keys, key)
		}
	}
	return entry, len(p.issues) == before
}

func (p *parser) sourceError(field string, node *yaml.Node, err error) {
	var refused *dsl.AuthoredDataError
	if errors.As(err, &refused) {
		p.issues = append(p.issues, errorIssue(refused.Path, fmt.Sprintf("line %d: %s", refused.Line, refused.Reason)))
		return
	}
	p.errorAt(field, node, "%v", err)
}

// EventSourceFromNode reads a source's YAML (an entry's keys, as a mapping
// node) through the strict mapping into the API's message. path names the
// node in errors.
func EventSourceFromNode(node *yaml.Node, path string) (*orkstackv1.EventSource, error) {
	value, err := dsl.AuthoredJSON(node, path)
	if err != nil {
		return nil, err
	}
	doc, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("an event source is a mapping")
	}
	wire := map[string]any{}
	for key, v := range doc {
		at := joinField(path, key)
		if v == nil {
			return nil, &dsl.AuthoredDataError{Path: at, Line: lineOf(node, key),
				Reason: fmt.Sprintf("%s is null: write a value, or remove the key", key)}
		}
		switch key {
		case "delivery":
			v, err = enumWord(at, v, deliveryWords)
		case "format":
			v, err = enumWord(at, v, formatWords)
		case "input":
			v = map[string]any{"data": v}
		case "match":
			v, err = matchRules(at, v)
		case "map":
			v, err = mapRules(at, v)
		case "artifacts":
			v, err = enumMap(at, v, partWords)
		}
		if err != nil {
			return nil, err
		}
		name := key
		if json, ok := eventSourceJSONName[key]; ok {
			name = json
		}
		// Each key is decoded alone first, so a refusal names its key.
		one, err := stdjson.Marshal(map[string]any{name: v})
		if err == nil {
			err = protojson.Unmarshal(one, &orkstackv1.EventSource{})
		}
		if err != nil {
			return nil, &dsl.AuthoredDataError{Path: at, Line: lineOf(node, key), Reason: key + ": " + cleanProtoError(err)}
		}
		wire[name] = v
	}
	raw, err := stdjson.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	src := &orkstackv1.EventSource{}
	if err := protojson.Unmarshal(raw, src); err != nil {
		return nil, fmt.Errorf("%s: %s", path, cleanProtoError(err))
	}
	return src, nil
}

// lineOf is the line of a mapping's key, 0 when it has none.
func lineOf(node *yaml.Node, key string) int {
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i].Line
		}
	}
	return node.Line
}

// cleanProtoError keeps a decoder's sentence without the decoder's own
// prefix.
func cleanProtoError(err error) string {
	msg := err.Error()
	if i := strings.Index(msg, "):"); i >= 0 {
		return strings.TrimSpace(msg[i+2:])
	}
	return msg
}

func enumWord(at string, v any, words map[string]string) (any, error) {
	s, ok := v.(string)
	if !ok || words[s] == "" {
		return nil, fmt.Errorf("%s: want one of %s", at, joinKeys(sortedKeys(words)))
	}
	return words[s], nil
}

func enumMap(at string, v any, words map[string]string) (any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: want a mapping of input: %s", at, strings.Join(sortedKeys(words), " or "))
	}
	out := map[string]any{}
	for k, w := range m {
		word, err := enumWord(at+"."+k, w, words)
		if err != nil {
			return nil, err
		}
		out[k] = word
	}
	return out, nil
}

func selectorWire(at string, m map[string]any) error {
	if f, ok := m["field"]; ok {
		word, err := enumWord(at+".field", f, fieldWords)
		if err != nil {
			return err
		}
		m["field"] = word
	}
	return nil
}

func matchRules(at string, v any) (any, error) {
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s: want a list of tests", at)
	}
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s[%d]: a test is a mapping of one selector and one test", at, i)
		}
		if err := selectorWire(fmt.Sprintf("%s[%d]", at, i), m); err != nil {
			return nil, err
		}
	}
	return list, nil
}

func mapRules(at string, v any) (any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: want a mapping of input: selector", at)
	}
	for name, rule := range m {
		r, ok := rule.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s.%s: a selector is a mapping with one of pointer, attribute and field", at, name)
		}
		if err := selectorWire(at+"."+name, r); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// EventSourceFile is what a kind: EventSource document holds: one source,
// and the namespace and name it names, if it names them.
type EventSourceFile struct {
	Source       *orkstackv1.EventSource
	Namespace    string
	HasNamespace bool
	Name         string // "" when the document names none
}

// eventSourceFileHeader names the two lines a kind: EventSource document
// begins with.
var eventSourceFileHeader = fmt.Sprintf("%q and %q", "apiVersion: "+dsl.APIVersion, "kind: "+dsl.KindEventSource)

// ParseEventSourceFile reads the document `orkctl event-source
// create|update|evaluate --file` names: a kind: EventSource document, whose
// header is apiVersion: orkstack/v1 and kind: EventSource, with an optional
// namespace and name beside an event_sources entry's keys. It is as strict
// as a document's entry (the strict mapping, docs/EVENT_SOURCES.md).
// It is the one reader of that file.
func ParseEventSourceFile(source []byte) (EventSourceFile, error) {
	if line, found := dsl.SecondDocumentLine(source); found {
		return EventSourceFile{}, fmt.Errorf("line %d: %s", line, dsl.SecondDocumentMessage("event source document"))
	}
	var root yaml.Node
	if err := yaml.Unmarshal(source, &root); err != nil {
		return EventSourceFile{}, fmt.Errorf("the document is not valid YAML: %w", err)
	}
	if len(root.Content) == 0 {
		return EventSourceFile{}, fmt.Errorf("the document is empty: an event source document begins with %s", eventSourceFileHeader)
	}
	body := root.Content[0]
	if body.Kind != yaml.MappingNode {
		return EventSourceFile{}, fmt.Errorf("an event source document is a mapping that begins with %s, got %s",
			eventSourceFileHeader, dsl.DescribeNode(body))
	}
	if alias := findAlias(body); alias != nil {
		return EventSourceFile{}, fmt.Errorf("line %d: anchors, aliases and merge keys are not supported in an event source document", alias.Line)
	}
	// Every top-level key is a plain string, as in every authored document:
	// a tag or anchor on a key could make one reader see a header another
	// does not.
	for i := 0; i+1 < len(body.Content); i += 2 {
		if key := body.Content[i]; !dsl.PlainTopLevelKey(key) {
			return EventSourceFile{}, fmt.Errorf("line %d: a top-level key must be a plain string, not %s: write each key out, "+
				"with no merge key (<<), alias, anchor or tag", key.Line, dsl.DescribeTopLevelKey(key))
		}
	}
	if err := eventSourceFileKind(body); err != nil {
		return EventSourceFile{}, err
	}
	allowed := map[string]bool{"apiVersion": true, "kind": true, "namespace": true, "name": true}
	for _, k := range eventSourceKeys {
		allowed[k] = true
	}
	var out EventSourceFile
	entry := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	seen := map[string]bool{}
	for i := 0; i+1 < len(body.Content); i += 2 {
		key, value := body.Content[i], body.Content[i+1]
		if !allowed[key.Value] {
			return EventSourceFile{}, fmt.Errorf("line %d: unknown key %q; allowed: apiVersion, kind, namespace, name, %s",
				key.Line, key.Value, joinKeys(eventSourceKeys))
		}
		if seen[key.Value] {
			return EventSourceFile{}, fmt.Errorf("line %d: %s is written twice", key.Line, key.Value)
		}
		seen[key.Value] = true
		switch key.Value {
		case "apiVersion", "kind":
			continue
		case "namespace":
			if !dsl.IsStringScalar(value) || dsl.ValidSlug("namespace", value.Value) != nil {
				return EventSourceFile{}, fmt.Errorf("line %d: namespace is a namespace's name; leave the key out to take -n", key.Line)
			}
			out.Namespace, out.HasNamespace = value.Value, true
			continue
		case "name":
			if !dsl.IsStringScalar(value) || !eventsources.ValidName(value.Value) {
				return EventSourceFile{}, fmt.Errorf("line %d: name is the event source's name (%s); leave the key out to take the command's", key.Line, dsl.SlugPattern())
			}
			out.Name = value.Value
			continue
		}
		entry.Content = append(entry.Content, key, value)
	}
	src, err := EventSourceFromNode(entry, "")
	if err != nil {
		return EventSourceFile{}, err
	}
	src.Name = out.Name
	out.Source = src
	return out, nil
}

// eventSourceFileKind is rule 45's header check for kind: EventSource: the
// two lines, read off the root mapping's first occurrences, each refusal
// naming the line to write, and a document of another kind named for what
// it is.
func eventSourceFileKind(body *yaml.Node) error {
	var apiVersion, kind *yaml.Node
	for i := 0; i+1 < len(body.Content); i += 2 {
		switch body.Content[i].Value {
		case "apiVersion":
			if apiVersion == nil {
				apiVersion = body.Content[i+1]
			}
		case "kind":
			if kind == nil {
				kind = body.Content[i+1]
			}
		}
	}
	switch {
	case apiVersion == nil && kind == nil:
		return fmt.Errorf("the document has no apiVersion or kind: an event source document begins with %s; add them at the top, after any leading comments",
			eventSourceFileHeader)
	case apiVersion == nil:
		return fmt.Errorf("line %d: apiVersion is required: add the line %q above %q", kind.Line, "apiVersion: "+dsl.APIVersion, "kind: "+dsl.KindEventSource)
	case kind == nil:
		return fmt.Errorf("line %d: kind is required: add the line %q below %q", apiVersion.Line, "kind: "+dsl.KindEventSource, "apiVersion: "+dsl.APIVersion)
	case !dsl.IsStringScalar(apiVersion):
		return fmt.Errorf("line %d: apiVersion must be the string %s, got %s", apiVersion.Line, dsl.APIVersion, dsl.DescribeNode(apiVersion))
	case apiVersion.Value != dsl.APIVersion:
		return fmt.Errorf("line %d: apiVersion %q is not supported: this release reads apiVersion: %s", apiVersion.Line, apiVersion.Value, dsl.APIVersion)
	case !dsl.IsStringScalar(kind):
		return fmt.Errorf("line %d: kind must be the string %s, got %s", kind.Line, dsl.KindEventSource, dsl.DescribeNode(kind))
	case kind.Value == dsl.KindOrkflow:
		return fmt.Errorf("line %d: kind: %s is an Orkflow, not an event source: create it with orkctl orkflow create; an event source document says kind: %s",
			kind.Line, dsl.KindOrkflow, dsl.KindEventSource)
	case kind.Value == dsl.KindAutomation:
		return fmt.Errorf("line %d: kind: %s is an automation document, not an event source: apply it with orkctl automation apply; "+
			"an event source document says kind: %s", kind.Line, dsl.KindAutomation, dsl.KindEventSource)
	case kind.Value == dsl.KindActivityInterface, kind.Value == dsl.KindServiceInterface:
		return fmt.Errorf("line %d: kind: %s is an image's interface, not an event source: pack it with orkctl activity interface pack; "+
			"an event source document says kind: %s", kind.Line, kind.Value, dsl.KindEventSource)
	case kind.Value != dsl.KindEventSource:
		return fmt.Errorf("line %d: kind %q is not an event source: an event source document says kind: %s", kind.Line, kind.Value, dsl.KindEventSource)
	}
	return nil
}

// ---------------------------------------------------------------------------
// planning
// ---------------------------------------------------------------------------

// userFields are what an entry writes of a source: what a plan compares.
var userFields = []string{
	"orkflow_version", "description", "uses", "with", "secrets", "identities", "credentials", "env",
	"pool", "instances", "resources", "delivery", "concurrency", "rate", "format", "max_bytes",
	"reject_breaker", "match", "input", "map", "artifacts",
}

// sourceField is the field a document key names, and fieldKey the reverse.
func sourceField(key string) string {
	switch key {
	case "version":
		return "orkflow_version"
	case "orkflow":
		return "orkflow_name"
	}
	return key
}

func fieldKey(field string) string {
	if field == "orkflow_version" {
		return "version"
	}
	return field
}

func (p *planner) eventSources() {
	live := map[string]*orkstackv1.EventSource{}
	for _, src := range p.live.EventSources {
		live[src.GetName()] = src
	}
	var creates []string
	for _, name := range sortedKeys(p.doc.EventSources) {
		entry := p.doc.EventSources[name]
		field := "event_sources." + name
		desired := proto.Clone(entry.Source).(*orkstackv1.EventSource)
		desired.Namespace = p.namespace
		cur, exists := live[name]
		if !exists {
			step := &orkstackv1.AutomationStep{Action: actionCreate, Kind: kindEventSource, Name: name,
				Summary: sourceSummary(desired),
				Request: &orkstackv1.AutomationStep_CreateEventSource{CreateEventSource: &orkstackv1.CreateEventSourceRequest{EventSource: desired}}}
			for _, key := range entry.Keys {
				step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{Field: key, After: renderSourceField(desired, sourceField(key))})
			}
			p.steps = append(p.steps, step)
			creates = append(creates, name)
			continue
		}
		if cur.GetOrkflowName() != desired.GetOrkflowName() {
			p.issues = append(p.issues, errorIssue(field+".orkflow", fmt.Sprintf(
				"event source %s binds %s; the Orkflow a source binds is fixed at creation — give the entry a new name (and --prune the old one), "+
					"or delete it first with `orkctl event-source delete %s -n %s`", name, cur.GetOrkflowName(), name, p.namespace)))
			continue
		}
		step := &orkstackv1.AutomationStep{Action: actionUnchanged, Kind: kindEventSource, Name: name, Id: cur.GetId(), Summary: sourceSummary(desired)}
		for _, f := range userFields {
			before, after := renderSourceField(cur, f), renderSourceField(desired, f)
			if before != after {
				step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{Field: fieldKey(f), Before: before, After: after})
			}
		}
		if cur.GetPause() != nil {
			step.Notes = append(step.Notes, "paused: "+cur.GetPause().GetReason()+operationalNote)
		}
		if len(step.Changes) > 0 {
			step.Action = actionUpdate
			desired.Etag = cur.GetEtag()
			step.Request = &orkstackv1.AutomationStep_UpdateEventSource{UpdateEventSource: &orkstackv1.UpdateEventSourceRequest{EventSource: desired}}
		}
		p.steps = append(p.steps, step)
	}
	var gone []*orkstackv1.EventSource
	for _, name := range sortedKeys(live) {
		if _, declared := p.doc.EventSources[name]; declared {
			continue
		}
		cur := live[name]
		gone = append(gone, cur)
		step := &orkstackv1.AutomationStep{Action: actionUndeclared, Kind: kindEventSource, Name: name, Id: cur.GetId(), Summary: sourceSummary(cur)}
		if p.opts.Prune {
			// A delete names its source by the id this plan read, and says
			// what the source held, as before-values (docs/AUTOMATION.md).
			step.Action = actionDelete
			step.Changes = []*orkstackv1.AutomationFieldChange{
				{Field: "pool", Before: renderSourceField(cur, "pool")},
				{Field: "instances", Before: renderSourceField(eventsources.Effective(cur, eventsources.Limits{}), "instances")},
			}
			step.Request = &orkstackv1.AutomationStep_DeleteEventSource{DeleteEventSource: &orkstackv1.DeleteEventSourceRequest{
				Namespace: p.namespace, Name: name, Id: cur.GetId()}}
		}
		p.steps = append(p.steps, step)
	}
	for _, name := range creates {
		desired := p.doc.EventSources[name].Source
		for _, old := range gone {
			if old.GetOrkflowName() == desired.GetOrkflowName() && old.GetUses() == desired.GetUses() {
				p.issues = append(p.issues, warningIssue("event_sources."+name, fmt.Sprintf(
					"event source %s matches undeclared source %s; a renamed source is a new source, with no record of the messages %s accepted",
					name, old.GetName(), old.GetName())))
			}
		}
	}
}

func sourceSummary(src *orkstackv1.EventSource) string {
	version := src.GetOrkflowVersion()
	if version == "" {
		version = currentLabel
	}
	return fmt.Sprintf("%s@%s from %s", src.GetOrkflowName(), version, src.GetUses())
}

// renderSourceField renders one field of a source for a plan's change: the
// value its document key writes, as one line of JSON; "" when unset.
func renderSourceField(src *orkstackv1.EventSource, field string) string {
	only := &orkstackv1.EventSource{}
	fd := only.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(field))
	if fd == nil || !src.ProtoReflect().Has(fd) {
		return ""
	}
	only.ProtoReflect().Set(fd, src.ProtoReflect().Get(fd))
	node, err := EventSourceNode(only)
	if err != nil || len(node.Content) < 2 {
		return ""
	}
	var out any
	if err := node.Content[1].Decode(&out); err != nil {
		return node.Content[1].Value
	}
	raw, err := stdjson.Marshal(out, stdjson.Deterministic(true))
	if err != nil {
		return node.Content[1].Value
	}
	return string(raw)
}

// ---------------------------------------------------------------------------
// export
// ---------------------------------------------------------------------------

// EventSourceNode renders a stored source as an entry: the keys it has, in
// the docs table's order, block style, and the strings a YAML reader could
// read as something else quoted. It writes neither the binder nor the
// consent, nor anything else that is output only.
func EventSourceNode(src *orkstackv1.EventSource) (*yaml.Node, error) {
	view := proto.Clone(src).(*orkstackv1.EventSource)
	view.Id, view.Namespace, view.Name, view.Image, view.Etag = "", "", "", "", ""
	view.Paused, view.Pause = false, nil
	view.CreatedAt, view.UpdatedAt, view.CreatedBy, view.UpdatedBy = nil, nil, "", ""
	view.BoundBy, view.BoundByVerified, view.ConsentStale, view.AsksFor, view.Consented = "", false, false, nil, nil
	view.ServiceRevision, view.JudgingRevision = 0, 0
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(view)
	if err != nil {
		return nil, err
	}
	var wire map[string]any
	if err := stdjson.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	out := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for _, key := range eventSourceKeys {
		v, ok := wire[sourceField(key)]
		if !ok {
			continue
		}
		switch key {
		case "max_bytes":
			if s, ok := v.(string); ok {
				var n float64
				if _, err := fmt.Sscan(s, &n); err == nil {
					v = n
				}
			}
		case "delivery":
			v = wordOf(v, deliveryWords)
		case "format":
			v = wordOf(v, formatWords)
		case "input":
			if m, ok := v.(map[string]any); ok {
				v = m["data"]
			}
		case "match":
			for _, rule := range asList(v) {
				fieldWordOf(rule)
			}
		case "map":
			for _, rule := range asMap(v) {
				fieldWordOf(rule)
			}
		case "artifacts":
			m := asMap(v)
			for k, part := range m {
				m[k] = wordOf(part, partWords)
			}
		}
		out.Content = append(out.Content, plainNode(key), dsl.AuthoredNode(v))
	}
	return out, nil
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func fieldWordOf(rule any) {
	if m, ok := rule.(map[string]any); ok {
		if f, ok := m["field"]; ok {
			m["field"] = wordOf(f, fieldWords)
		}
	}
}

func wordOf(v any, words map[string]string) any {
	for word, enum := range words {
		if enum == v {
			return word
		}
	}
	return v
}
