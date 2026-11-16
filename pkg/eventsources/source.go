// Package eventsources holds an event source's document as OrkStack judges
// it, with no I/O: its bounds and rules, which of its fields restart its
// containers and which change only how a message is judged, the quotas and
// sums of a namespace, the filter and the mapping, and the streaming JSON
// reader both the engine and an Orker read a message's body with
// (docs/EVENT_SOURCES.md).
//
// The domain type is the API's own message, orkstackv1.EventSource: the
// store keeps it as written, and Effective reads it with its defaults.
package eventsources

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The bounds of a source's document, applied by every write and by every
// evaluation (docs/EVENT_SOURCES.md, "Bounds on a source").
const (
	MaxDocumentBytes    = 64 << 10
	MaxMatchTests       = 16
	MaxInValues         = 32
	MaxLiteralBytes     = 256
	MaxMapEntries       = 64
	MaxWithBytes        = 32 << 10
	MaxSecrets          = 8
	MaxIdentities       = 8
	MaxCredentials      = 8
	MaxDescriptionBytes = 256
	MaxConcurrency      = 64
	MaxUsesBytes        = 512
	MaxEnvEntries       = 64
	// DefaultRate is a source's rate when it sets none.
	DefaultRate = 10
	// MemoryMargin is what an instance's memory must cover beside the
	// messages it holds.
	MemoryMargin = 32 << 20
	// MaxNameBytes is the name rule's bound.
	MaxNameBytes = 200
)

// Limits are the deployment's event_sources.* settings a write is held to.
type Limits struct {
	MaxMessageBytes    int64
	MaxInputBytes      int
	MaxRate            int32
	NamespaceRate      int
	MaxSources         int
	MaxPerNamespace    int
	MaxInstances       int32
	NamespaceInstances int
}

var (
	namePattern    = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	idPattern      = regexp.MustCompile(`^evs-[0-9a-f]{16}$`)
	secretPattern  = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
	envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// ValidName is the name rule every boundary applies (^[a-z][a-z0-9-]*$, at
// most 200 bytes). Validated, never sanitized.
func ValidName(s string) bool { return len(s) <= MaxNameBytes && namePattern.MatchString(s) }

// IDPrefix begins every source's id.
const IDPrefix = "evs-"

// NewID draws a source's id: evs- and 16 hexadecimal digits.
func NewID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("eventsources: draw an id: %w", err)
	}
	return IDPrefix + hex.EncodeToString(b[:]), nil
}

// ValidID reports whether s is shaped as NewID draws one.
func ValidID(s string) bool { return idPattern.MatchString(s) }

func refuse(field, format string, args ...any) *Error {
	return &Error{Reason: ReasonInvalid, Field: field, Message: fmt.Sprintf(format, args...)}
}

// Effective returns a copy of src with its defaults written in: one
// instance, one lane, a rate of 10, at-least-once, bytes, and the
// deployment's body bound. The stored document keeps what was written.
func Effective(src *orkstackv1.EventSource, l Limits) *orkstackv1.EventSource {
	out := proto.Clone(src).(*orkstackv1.EventSource)
	if out.Instances == 0 {
		out.Instances = 1
	}
	if out.Concurrency == 0 {
		out.Concurrency = 1
	}
	if out.Rate == 0 {
		out.Rate = DefaultRate
	}
	if out.Delivery == orkstackv1.DeliveryClass_DELIVERY_CLASS_UNSPECIFIED {
		out.Delivery = orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_LEAST_ONCE
	}
	if out.Format == orkstackv1.MessageFormat_MESSAGE_FORMAT_UNSPECIFIED {
		out.Format = orkstackv1.MessageFormat_MESSAGE_FORMAT_BYTES
	}
	if out.MaxBytes == 0 {
		out.MaxBytes = l.MaxMessageBytes
		if out.MaxBytes == 0 {
			out.MaxBytes = DefaultMaxMessageBytes
		}
	}
	return out
}

// Validate holds a source's document to its bounds and its own rules: what
// can be refused before anything is read. Its checks against the Orkflow,
// the image, the namespace and the quotas follow it.
func Validate(src *orkstackv1.EventSource, l Limits) error {
	if src == nil {
		return refuse("event_source", "a source is required")
	}
	if src.GetNamespace() != "" && !ValidName(src.GetNamespace()) {
		return refuse("namespace", "a namespace is a slug (^[a-z][a-z0-9-]*$, at most %d bytes)", MaxNameBytes)
	}
	if !ValidName(src.GetName()) {
		return refuse("name", "a source's name is a slug (^[a-z][a-z0-9-]*$, at most %d bytes)", MaxNameBytes)
	}
	if strings.Contains(src.GetOrkflowName(), "/") {
		return refuse("orkflow", "a source binds an Orkflow of its own namespace; <namespace>/<name> is refused")
	}
	if !ValidName(src.GetOrkflowName()) {
		return refuse("orkflow", "an Orkflow's name is a slug (^[a-z][a-z0-9-]*$, at most %d bytes)", MaxNameBytes)
	}
	if len(src.GetDescription()) > MaxDescriptionBytes || !utf8.ValidString(src.GetDescription()) {
		return refuse("description", "a description is text of at most %d bytes", MaxDescriptionBytes)
	}
	if src.GetUses() == "" {
		return refuse("uses", "uses names a source image: a first-party name or a registry reference")
	}
	if len(src.GetUses()) > MaxUsesBytes || strings.ContainsAny(src.GetUses(), " \t\n\r") {
		return refuse("uses", "uses is one reference of at most %d bytes", MaxUsesBytes)
	}
	if err := validateNumbers(src, l); err != nil {
		return err
	}
	if err := validateMatch(src); err != nil {
		return err
	}
	if len(src.GetMap()) > MaxMapEntries {
		return refuse("map", "%d entries; at most %d", len(src.GetMap()), MaxMapEntries)
	}
	for _, name := range sortedKeys(src.GetMap()) {
		rule := src.GetMap()[name]
		if err := checkSelector("map."+name, rule.GetAttribute(), rule.GetPointer(), rule.GetField(), src.GetFormat()); err != nil {
			return err
		}
	}
	if size := structBytes(src.GetWith()); size > MaxWithBytes {
		return refuse("with", "%d bytes; at most %d", size, MaxWithBytes)
	}
	maxInput := l.MaxInputBytes
	if maxInput <= 0 {
		maxInput = DefaultMaxInputBytes
	}
	if size := structBytes(src.GetInput().GetData()); size > maxInput {
		return refuse("input", "%d bytes; event_sources.max_input_bytes is %d", size, maxInput)
	}
	if err := validateGrants(src); err != nil {
		return err
	}
	if len(src.GetEnv()) > MaxEnvEntries {
		return refuse("env", "%d entries; at most %d", len(src.GetEnv()), MaxEnvEntries)
	}
	for _, k := range sortedKeys(src.GetEnv()) {
		if !envNamePattern.MatchString(k) || !utf8.ValidString(src.GetEnv()[k]) {
			return refuse("env", "an environment variable is named ^[A-Za-z_][A-Za-z0-9_]*$ and holds text")
		}
	}
	if size := DocumentBytes(src); size > MaxDocumentBytes {
		return refuse("event_source", "the document is %d bytes; at most %d", size, MaxDocumentBytes)
	}
	return nil
}

func validateNumbers(src *orkstackv1.EventSource, l Limits) error {
	maxInstances := l.MaxInstances
	if maxInstances <= 0 {
		maxInstances = 1
	}
	if n := src.GetInstances(); n < 0 || n > maxInstances {
		return refuse("instances", "%d; a source runs 1 to event_sources.max_instances (%d)", n, maxInstances)
	}
	if n := src.GetConcurrency(); n < 0 || n > MaxConcurrency {
		return refuse("concurrency", "%d; 1 to %d", n, MaxConcurrency)
	}
	maxRate := l.MaxRate
	if maxRate <= 0 {
		maxRate = DefaultRate
	}
	if n := src.GetRate(); n < 0 || n > maxRate {
		return refuse("rate", "%d; 1 to event_sources.max_rate (%d) messages a second an instance", n, maxRate)
	}
	maxBytes := l.MaxMessageBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxMessageBytes
	}
	if n := src.GetMaxBytes(); n < 0 || n > maxBytes {
		return refuse("max_bytes", "%d; at most event_sources.max_message_bytes (%d)", n, maxBytes)
	}
	if src.RejectBreaker != nil && src.GetRejectBreaker() < 0 {
		return refuse("reject_breaker", "0 turns it off; a count is positive")
	}
	if _, ok := orkstackv1.DeliveryClass_name[int32(src.GetDelivery())]; !ok {
		return refuse("delivery", "at-least-once or at-most-once")
	}
	if _, ok := orkstackv1.MessageFormat_name[int32(src.GetFormat())]; !ok {
		return refuse("format", "bytes or json")
	}
	return nil
}

func validateMatch(src *orkstackv1.EventSource) error {
	if len(src.GetMatch()) > MaxMatchTests {
		return refuse("match", "%d tests; at most %d", len(src.GetMatch()), MaxMatchTests)
	}
	for i, rule := range src.GetMatch() {
		field := fmt.Sprintf("match[%d]", i)
		if rule == nil {
			return refuse(field, "a test is one selector and one test")
		}
		if err := checkSelector(field, rule.GetAttribute(), rule.GetPointer(), rule.GetField(), src.GetFormat()); err != nil {
			err.Reason = ReasonInvalid
			return err
		}
		if f := rule.GetField(); f != orkstackv1.MessageField_MESSAGE_FIELD_UNSPECIFIED && f != orkstackv1.MessageField_MESSAGE_FIELD_SUBJECT {
			return refuse(field, "subject is the one field a test reads")
		}
		tests := 0
		if rule.GetEquals() != nil {
			tests++
		}
		if len(rule.GetIn()) > 0 {
			tests++
		}
		if rule.Exists != nil {
			tests++
		}
		if tests != 1 {
			return refuse(field, "a test is exactly one of equals, in and exists")
		}
		if len(rule.GetIn()) > MaxInValues {
			return refuse(field, "in holds %d values; at most %d", len(rule.GetIn()), MaxInValues)
		}
		literals := rule.GetIn()
		if rule.GetEquals() != nil {
			literals = []*structpb.Value{rule.GetEquals()}
		}
		for _, lit := range literals {
			if err := checkLiteral(lit); err != nil {
				return refuse(field, "%s", err)
			}
		}
	}
	return nil
}

// checkLiteral holds a test's literal to a JSON scalar of at most 256 bytes.
func checkLiteral(v *structpb.Value) error {
	switch k := v.GetKind().(type) {
	case *structpb.Value_StringValue:
		if len(k.StringValue) > MaxLiteralBytes {
			return fmt.Errorf("a literal is at most %d bytes", MaxLiteralBytes)
		}
	case *structpb.Value_NumberValue, *structpb.Value_BoolValue, *structpb.Value_NullValue:
	default:
		return fmt.Errorf("a literal is a string, a number, true, false or null")
	}
	return nil
}

// validateGrants holds secrets, identities and credentials to a task's
// rules. A source's identities, credentials and grant-backed secrets need
// task identity, which this release does not have: they are refused by
// name.
func validateGrants(src *orkstackv1.EventSource) error {
	if len(src.GetSecrets()) > MaxSecrets {
		return refuse("secrets", "%d; at most %d", len(src.GetSecrets()), MaxSecrets)
	}
	if len(src.GetIdentities()) > MaxIdentities {
		return refuse("identities", "%d; at most %d", len(src.GetIdentities()), MaxIdentities)
	}
	if len(src.GetCredentials()) > MaxCredentials {
		return refuse("credentials", "%d; at most %d", len(src.GetCredentials()), MaxCredentials)
	}
	if len(src.GetIdentities()) > 0 {
		return &Error{Reason: ReasonIdentityUnsupported, Field: "identities",
			Message: "a source's own identities need task identity, which this release does not have"}
	}
	if len(src.GetCredentials()) > 0 {
		return &Error{Reason: ReasonIdentityUnsupported, Field: "credentials",
			Message: "a source's own credentials need task identity, which this release does not have"}
	}
	names := map[string]bool{}
	for i, s := range src.GetSecrets() {
		field := fmt.Sprintf("secrets[%d]", i)
		if !secretPattern.MatchString(s.GetName()) {
			return refuse(field, "a secret's name is ^[A-Za-z0-9._-]{1,128}$")
		}
		if names[s.GetName()] {
			return refuse(field, "secret %q is declared twice", s.GetName())
		}
		names[s.GetName()] = true
		if s.GetSource() == "" {
			return refuse(field, "a secret names its source, such as env:NAME or file:/path")
		}
		if s.GetGrant() != "" || s.GetCiphertext() != "" {
			return &Error{Reason: ReasonIdentityUnsupported, Field: field,
				Message: "a secret through a grant needs task identity, which this release does not have"}
		}
	}
	return nil
}

// structBytes is a Struct's size as JSON.
func structBytes(s *structpb.Struct) int {
	if s == nil {
		return 0
	}
	b, err := protojson.Marshal(s)
	if err != nil {
		return 0
	}
	return len(b)
}

// DocumentBytes is a source's size as the JSON its document is stored as.
func DocumentBytes(src *orkstackv1.EventSource) int {
	b, err := protojson.Marshal(src)
	if err != nil {
		return 0
	}
	return len(b)
}

// CheckMemory holds an instance's memory to what a message in hand needs: a
// lane's file is replaced by rename, so the one before it is still there
// when the next is put in place, and every lane may hold one.
func CheckMemory(memoryBytes int64, concurrency int32, maxBytes int64) error {
	if concurrency <= 0 {
		concurrency = 1
	}
	need := 2*int64(concurrency)*maxBytes + MemoryMargin
	if memoryBytes < need {
		return &Error{Reason: ReasonInvalid, Field: "resources.memory", Message: fmt.Sprintf(
			"%d bytes does not cover the messages an instance holds: 2 × %d × %d + %d = %d",
			memoryBytes, concurrency, maxBytes, int64(MemoryMargin), need)}
	}
	return nil
}

// A field's class (docs/EVENT_SOURCES.md, "Which changes restart a
// source"): a change to a service field replaces the source's containers;
// a change to a judging field changes only how a message is judged.
const (
	classFixed   = "fixed"
	classService = "service"
	classJudging = "judging"
	classNeither = "neither"
	classOutput  = "output"
)

// fieldClass gives every field of EventSource its class.
// TestChange_ServiceOrJudging fails on a field added without one.
var fieldClass = map[string]string{
	"id": classOutput, "namespace": classFixed, "name": classFixed, "orkflow_name": classFixed,
	"orkflow_version": classJudging, "description": classNeither,
	"uses": classService, "image": classOutput, "with": classService, "pool": classService,
	"instances": classNeither, "resources": classService, "delivery": classService,
	"concurrency": classService, "format": classJudging, "max_bytes": classJudging,
	"match": classJudging, "input": classJudging, "map": classJudging, "artifacts": classJudging,
	"rate": classJudging, "reject_breaker": classJudging,
	"paused": classOutput, "pause": classOutput, "created_at": classOutput, "updated_at": classOutput,
	"etag": classOutput, "created_by": classOutput, "updated_by": classOutput,
	"bound_by": classOutput, "bound_by_verified": classOutput, "consent_stale": classOutput,
	"asks_for": classOutput, "consented": classOutput,
	"secrets": classService, "identities": classService, "credentials": classService, "env": classService,
	"service_revision": classOutput, "judging_revision": classOutput,
}

// submitFields are what a source submits: a change to one makes whoever
// writes it the binder.
var submitFields = map[string]bool{"orkflow_version": true, "input": true, "map": true, "artifacts": true}

// Classify reports whether next differs from prev in a service field and
// in a judging field, each read with its defaults, so writing a default out
// changes nothing.
func Classify(prev, next *orkstackv1.EventSource) (service, judging bool) {
	a, b := Effective(prev, Limits{}), Effective(next, Limits{})
	for _, name := range changedFields(a, b) {
		switch fieldClass[name] {
		case classService:
			service = true
		case classJudging:
			judging = true
		}
	}
	return service, judging
}

// ChangesWhatItSubmits reports whether next changes the Orkflow version, the
// constants, the mapping or the artifacts.
func ChangesWhatItSubmits(prev, next *orkstackv1.EventSource) bool {
	for _, name := range changedFields(prev, next) {
		if submitFields[name] {
			return true
		}
	}
	return false
}

func changedFields(a, b *orkstackv1.EventSource) []string {
	ra, rb := a.ProtoReflect(), b.ProtoReflect()
	fields := ra.Descriptor().Fields()
	var out []string
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !equalField(ra, rb, fd) {
			out = append(out, string(fd.Name()))
		}
	}
	return out
}

func equalField(a, b protoreflect.Message, fd protoreflect.FieldDescriptor) bool {
	if a.Has(fd) != b.Has(fd) {
		return false
	}
	if !a.Has(fd) {
		return true
	}
	x := a.Interface().(*orkstackv1.EventSource)
	y := b.Interface().(*orkstackv1.EventSource)
	cx := &orkstackv1.EventSource{}
	cy := &orkstackv1.EventSource{}
	cx.ProtoReflect().Set(fd, x.ProtoReflect().Get(fd))
	cy.ProtoReflect().Set(fd, y.ProtoReflect().Get(fd))
	return proto.Equal(cx, cy)
}

// CheckImmutable refuses a change to what a source is fixed to at creation:
// its name and the Orkflow it binds (its namespace is checked where the
// request is decided).
func CheckImmutable(stored, next *orkstackv1.EventSource) error {
	if next.GetName() != stored.GetName() {
		return refuse("name", "a source's name is fixed at creation; create another source instead")
	}
	if next.GetOrkflowName() != stored.GetOrkflowName() {
		return refuse("orkflow", "the Orkflow a source binds is fixed at creation; create another source instead")
	}
	return nil
}

// CheckQuotas holds a write to the namespace's quotas and sums and the
// deployment's count (docs/EVENT_SOURCES.md, "Quotas"). namespace is every
// source of next's namespace as stored, next itself included when it is
// an update; deployment is how many sources the deployment holds.
func CheckQuotas(namespace []*orkstackv1.EventSource, deployment int, next *orkstackv1.EventSource, l Limits) error {
	isNew := true
	count, instances, rate := 0, 0, 0
	for _, s := range namespace {
		if s.GetName() == next.GetName() {
			isNew = false
			continue
		}
		e := Effective(s, l)
		count++
		instances += int(e.GetInstances())
		rate += int(e.GetInstances()) * int(e.GetRate())
	}
	e := Effective(next, l)
	count++
	instances += int(e.GetInstances())
	rate += int(e.GetInstances()) * int(e.GetRate())
	limit := func(format string, args ...any) error {
		return &Error{Reason: ReasonLimit, Message: fmt.Sprintf(format, args...)}
	}
	if isNew && l.MaxSources > 0 && deployment+1 > l.MaxSources {
		return limit("the deployment holds %d sources; event_sources.max_sources is %d", deployment, l.MaxSources)
	}
	if l.MaxPerNamespace > 0 && count > l.MaxPerNamespace {
		return limit("the namespace would hold %d sources; event_sources.max_per_namespace is %d", count, l.MaxPerNamespace)
	}
	if l.NamespaceInstances > 0 && instances > l.NamespaceInstances {
		return limit("the namespace's sources would run %d instances; event_sources.namespace_instances is %d", instances, l.NamespaceInstances)
	}
	if l.NamespaceRate > 0 && rate > l.NamespaceRate {
		return limit("the namespace's rate × instances would sum to %d; event_sources.namespace_rate is %d", rate, l.NamespaceRate)
	}
	return nil
}
