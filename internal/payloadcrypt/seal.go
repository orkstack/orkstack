package payloadcrypt

import (
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

// Scope is a run's (or a schedule's) key with the context its payloads are
// sealed for. It prints its context and its key's id only.
type Scope struct {
	key *Key
	ctx Context
}

// NewScope pairs a key with the context it seals for.
func NewScope(key *Key, c Context) *Scope { return &Scope{key: key, ctx: c} }

// unsealedScope is the one scope of every run that is not sealed.
var unsealedScope = &Scope{}

// Unsealed is the scope of a run that is not sealed: Seal hands back the
// value itself, and WithKey writes the error as the SDK would. It is told
// apart from a nil scope, a key that was lost, which a sealing process
// withholds.
func Unsealed() *Scope { return unsealedScope }

func (s *Scope) unsealed() bool { return s == unsealedScope }

// Sealed reports whether s holds a key.
func (s *Scope) Sealed() bool { return s != nil && s.key != nil }

// For is the scope of another context under the same key: a child run of the
// tree, whose start and result are sealed for the child's own id. A scope
// that holds no key stays as it is.
func (s *Scope) For(c Context) *Scope {
	if !s.Sealed() {
		return s
	}
	return &Scope{key: s.key, ctx: c}
}

// Context is what the scope seals for and expects.
func (s *Scope) Context() Context { return s.ctx }

// String names the context and the key's id.
func (s *Scope) String() string {
	if !s.Sealed() {
		return "payloadcrypt.Scope(none)"
	}
	return "payloadcrypt.Scope(" + s.ctx.String() + ", " + s.key.ID() + ")"
}

// Format prints String for every verb.
func (s *Scope) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(s.String())) }

// SealPayload is p sealed for the scope's own context; for Unsealed() it is p
// itself. A nil scope, or one that holds no key, is a key that was lost:
// refused, never written in plaintext.
func (s *Scope) SealPayload(p *commonpb.Payload) (*commonpb.Payload, error) {
	if s.unsealed() {
		return p, nil
	}
	if !s.Sealed() {
		return nil, errors.New("payloadcrypt: a payload sealed under no key: its run's key was lost")
	}
	return SealPayload(s.key, s.ctx, p)
}

// Open decrypts p for the scope's own context.
func (s *Scope) Open(p *commonpb.Payload) (*commonpb.Payload, error) {
	if !s.Sealed() {
		return nil, unreadable("no key")
	}
	return OpenPayload(s.key, s.ctx, p)
}

// sealedValue is a value the Converter writes sealed.
type sealedValue struct {
	s *Scope
	v any
}

// Seal marks v to be written sealed under s. For a run that is not sealed
// (Unsealed()) it is v itself, so the SDK converts what it always converted.
//
// Never Seal a side effect's, a mutable side effect's or a marker's value:
// the SDK reads those back with the process's root converter, which opens
// nothing, so a sealed one would never read again.
//
// A nil scope, or one that holds no key, is a key that was lost: its value is
// refused when written, never written in plaintext.
func Seal(s *Scope, v any) any {
	if s.unsealed() {
		return v
	}
	return sealedValue{s: s, v: v}
}

// plainValue is a value the Converter writes as it is, in a sealed run too.
type plainValue struct{ v any }

// Plain marks v never to be sealed: a memo field, or what a reader with no
// run key must read (payload-encryption.md §2). In a sealed run's workflow
// code every other value is sealed under the run's key by default (the
// user's decision, round 20, E1, 2026-11-20); in any other run it is written
// exactly as v. A raw value is carried as it is, so it is returned as it is.
func Plain(v any) any {
	switch v.(type) {
	case converter.RawValue, plainValue:
		return v
	}
	return plainValue{v: v}
}

// PlainIn is v marked Plain where s is a sealed run's scope, and v itself
// in any other run, so a run that is not sealed hands the SDK exactly what it
// always did, whatever converter reads it.
func PlainIn(s *Scope, v any) any {
	if !s.Sealed() {
		return v
	}
	return Plain(v)
}

// Unplain is the value a Plain mark holds, or v itself.
func Unplain(v any) any {
	if p, ok := v.(plainValue); ok {
		return p.v
	}
	return v
}

// plainBinary is a message the Converter writes as binary proto.
type plainBinary struct{ m proto.Message }

// PlainBinary is m, never sealed, written in the SDK's binary proto
// encoding, in which a bytes field is its bytes: a fire's preparation that
// carries its run's start already sealed.
func PlainBinary(m proto.Message) any { return plainBinary{m: m} }

// metaMark marks a payload the Codec is to seal, naming the context. It
// never reaches the cluster: Encode removes it, or fails.
const metaMark = "ork-seal"

// Mark is a copy of p that the Codec seals for c.
func Mark(p *commonpb.Payload, c Context) *commonpb.Payload {
	m := proto.Clone(p).(*commonpb.Payload)
	if m.Metadata == nil {
		m.Metadata = map[string][]byte{}
	}
	m.Metadata[metaMark] = []byte(c.String())
	return m
}

var _ interface {
	Encode([]*commonpb.Payload) ([]*commonpb.Payload, error)
	Decode([]*commonpb.Payload) ([]*commonpb.Payload, error)
} = (*Codec)(nil)

// Codec seals exactly the payloads that carry the mark and opens exactly
// those whose encoding is Encoding; it touches nothing else. keys answers
// the key of a context; on the workflow side it is the run's own, on an
// Orker its cache of the run trees it holds attempts in (P20b).
type Codec struct {
	keys func(Context) (*Key, error)
}

// NewCodec is a codec over keys.
func NewCodec(keys func(Context) (*Key, error)) *Codec { return &Codec{keys: keys} }

func (c *Codec) keyFor(ctx Context) (*Key, error) {
	if c == nil || c.keys == nil {
		return nil, errors.New("payloadcrypt: no key source")
	}
	k, err := c.keys(ctx)
	if err != nil {
		return nil, err
	}
	if k == nil {
		return nil, fmt.Errorf("payloadcrypt: no key for %s", ctx)
	}
	return k, nil
}

// Encode seals every marked payload, failing closed: a marked payload is
// never written plaintext.
func (c *Codec) Encode(ps []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(ps))
	for i, p := range ps {
		raw, marked := p.GetMetadata()[metaMark]
		if !marked {
			out[i] = p
			continue
		}
		ctx, err := ParseContext(string(raw))
		if err != nil {
			return nil, fmt.Errorf("payloadcrypt: encode payload %d: %w", i, err)
		}
		k, err := c.keyFor(ctx)
		if err != nil {
			return nil, fmt.Errorf("payloadcrypt: encode payload %d: %w", i, err)
		}
		bare := proto.Clone(p).(*commonpb.Payload)
		delete(bare.Metadata, metaMark)
		sealed, err := SealPayload(k, ctx, bare)
		if err != nil {
			return nil, fmt.Errorf("payloadcrypt: encode payload %d: %w", i, err)
		}
		out[i] = sealed
	}
	return out, nil
}

// Decode opens every sealed payload for the context it names; a sealed
// payload that cannot be opened is an error, never passed on.
func (c *Codec) Decode(ps []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(ps))
	for i, p := range ps {
		if !IsSealed(p) {
			out[i] = p
			continue
		}
		ctx, err := ParseContext(string(p.GetMetadata()[MetaContext]))
		if err != nil {
			return nil, unreadable("payload %d: %v", i, err)
		}
		k, err := c.keyFor(ctx)
		if err != nil {
			return nil, fmt.Errorf("payloadcrypt: decode payload %d: %w", i, err)
		}
		opened, err := OpenPayload(k, ctx, p)
		if err != nil {
			return nil, fmt.Errorf("payloadcrypt: decode payload %d: %w", i, err)
		}
		out[i] = opened
	}
	return out, nil
}
