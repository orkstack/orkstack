package payloadcrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

// The envelope of a sealed payload, version 1.
const (
	// Encoding is a sealed payload's encoding.
	Encoding = "binary/ork-encrypted"
	// MetaEncoding is the SDK's own metadata key for a payload's encoding.
	MetaEncoding = "encoding"
	// MetaContext names what the payload was sealed for: "run:<id>" or
	// "schedule:<id>". It is informative; the open checks the context it
	// expects, through the AAD.
	MetaContext = "ork-ctx"
	// MetaVersion is the envelope's version.
	MetaVersion = "ork-v"
	// EnvelopeVersion is the only version this release reads or writes.
	EnvelopeVersion = "1"

	// MaxOverhead bounds how many bytes sealing adds to one payload, for
	// every context this package accepts (TestSealPayload_MaxOverhead).
	MaxOverhead = 512

	// MaxContextID bounds a context's id, so MaxOverhead holds. A run id is
	// 30 bytes; the longest schedule id the cluster holds is
	// "ork-schedule/" (13), a namespace (200), "/" and a name (200).
	MaxContextID = 414

	payloadLabel = "ork-payload-v1"
	// failureLabel separates a failure's sealed text from a sealed value in
	// the AAD, so neither opens in the other's place.
	failureLabel = "ork-failure-v1"
	nonceSize    = 12
	tagSize      = 16
)

// ErrUnreadable is the answer for a payload that does not open as what its
// reader expects: altered, sealed for another context or under another key,
// of an unknown version, or not sealed where it must be. It is decided from
// the bytes alone, so it is the same on every engine and every replay.
var ErrUnreadable = errors.New("PAYLOAD_UNREADABLE")

func unreadable(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrUnreadable}, args...)...)
}

// Info is the one encoding of every HKDF info and every AAD of this design:
// for each field in order, its length as two bytes big-endian, then its
// bytes. Two different lists never encode alike. A field longer than 65,535
// bytes is refused.
func Info(fields ...string) ([]byte, error) {
	n := 0
	for _, f := range fields {
		if len(f) > 0xFFFF {
			return nil, fmt.Errorf("payloadcrypt: a field of %d bytes is longer than 65,535", len(f))
		}
		n += 2 + len(f)
	}
	out := make([]byte, 0, n)
	for _, f := range fields {
		out = binary.BigEndian.AppendUint16(out, uint16(len(f)))
		out = append(out, f...)
	}
	return out, nil
}

// The kinds of context a payload is sealed for: a closed set.
const (
	KindRun      = "run"
	KindSchedule = "schedule"
)

// Context is what a payload is sealed for: a run's id, or a schedule's id as
// the cluster holds it.
type Context struct {
	Kind string
	ID   string
}

// RunContext is the context of everything of run id.
func RunContext(id string) Context { return Context{Kind: KindRun, ID: id} }

// ScheduleContext is the context of a schedule's stored action and its
// fire's own arguments.
func ScheduleContext(id string) Context { return Context{Kind: KindSchedule, ID: id} }

// String is the context as "kind:id".
func (c Context) String() string { return c.Kind + ":" + c.ID }

func (c Context) valid() error {
	if c.Kind != KindRun && c.Kind != KindSchedule {
		return fmt.Errorf("a context of no known kind (%d bytes)", len(c.Kind))
	}
	if c.ID == "" || len(c.ID) > MaxContextID {
		return fmt.Errorf("a context id of %d bytes", len(c.ID))
	}
	return nil
}

// ParseContext reads "kind:id" of a known kind.
func ParseContext(s string) (Context, error) {
	kind, id, ok := strings.Cut(s, ":")
	c := Context{Kind: kind, ID: id}
	if !ok {
		return Context{}, errors.New("payloadcrypt: a context is kind:id")
	}
	if err := c.valid(); err != nil {
		return Context{}, fmt.Errorf("payloadcrypt: %w", err)
	}
	return c, nil
}

func (c Context) aad(label string) ([]byte, error) { return Info(label, c.Kind, c.ID) }

// IsSealed reports whether p is marked sealed.
func IsSealed(p *commonpb.Payload) bool {
	return p != nil && string(p.GetMetadata()[MetaEncoding]) == Encoding
}

func gcmOf(k *Key) (cipher.AEAD, error) {
	block, err := aes.NewCipher(k.b[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// SealPayload encrypts p for c under key, with a random nonce.
func SealPayload(key *Key, c Context, p *commonpb.Payload) (*commonpb.Payload, error) {
	return sealWith(payloadLabel, key, c, p, rand.Reader)
}

func sealPayload(key *Key, c Context, p *commonpb.Payload, random io.Reader) (*commonpb.Payload, error) {
	return sealWith(payloadLabel, key, c, p, random)
}

func sealWith(label string, key *Key, c Context, p *commonpb.Payload, random io.Reader) (*commonpb.Payload, error) {
	if key == nil {
		return nil, errors.New("payloadcrypt: seal without a key")
	}
	if p == nil {
		return nil, errors.New("payloadcrypt: seal of no payload")
	}
	if IsSealed(p) {
		return nil, errors.New("payloadcrypt: the payload is sealed already")
	}
	if err := c.valid(); err != nil {
		return nil, fmt.Errorf("payloadcrypt: %w", err)
	}
	plain, err := proto.MarshalOptions{Deterministic: true}.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("payloadcrypt: marshal payload: %w", err)
	}
	defer clear(plain)
	aad, err := c.aad(label)
	if err != nil {
		return nil, err
	}
	aead, err := gcmOf(key)
	if err != nil {
		return nil, err
	}
	data := make([]byte, nonceSize, nonceSize+len(plain)+tagSize)
	if _, err := io.ReadFull(random, data); err != nil {
		return nil, fmt.Errorf("payloadcrypt: nonce: %w", err)
	}
	data = aead.Seal(data, data[:nonceSize], plain, aad)
	return &commonpb.Payload{
		Metadata: map[string][]byte{
			MetaEncoding: []byte(Encoding),
			MetaContext:  []byte(c.String()),
			MetaVersion:  []byte(EnvelopeVersion),
		},
		Data: data,
	}, nil
}

// OpenPayload decrypts p, which must have been sealed for want under key.
// Every failure is ErrUnreadable.
func OpenPayload(key *Key, want Context, p *commonpb.Payload) (*commonpb.Payload, error) {
	return openWith(payloadLabel, key, want, p)
}

func openWith(label string, key *Key, want Context, p *commonpb.Payload) (*commonpb.Payload, error) {
	if key == nil {
		return nil, unreadable("no key")
	}
	if !IsSealed(p) {
		return nil, unreadable("the payload is not sealed")
	}
	md := p.GetMetadata()
	if len(md) != 3 {
		return nil, unreadable("the envelope has %d metadata entries", len(md))
	}
	if string(md[MetaVersion]) != EnvelopeVersion {
		return nil, unreadable("an envelope of an unknown version (%d bytes)", len(md[MetaVersion]))
	}
	if err := want.valid(); err != nil {
		return nil, unreadable("%v", err)
	}
	if string(md[MetaContext]) != want.String() {
		return nil, unreadable("sealed for another context")
	}
	data := p.GetData()
	if len(data) < nonceSize+tagSize {
		return nil, unreadable("the envelope is %d bytes", len(data))
	}
	aad, err := want.aad(label)
	if err != nil {
		return nil, unreadable("%v", err)
	}
	aead, err := gcmOf(key)
	if err != nil {
		return nil, unreadable("%v", err)
	}
	plain, err := aead.Open(nil, data[:nonceSize], data[nonceSize:], aad)
	if err != nil {
		return nil, unreadable("the payload does not open")
	}
	defer clear(plain)
	var out commonpb.Payload
	if err := proto.Unmarshal(plain, &out); err != nil {
		return nil, unreadable("the opened payload does not parse")
	}
	if IsSealed(&out) {
		return nil, unreadable("a sealed payload inside a sealed payload")
	}
	return &out, nil
}
